package stationcluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

type backupTestControl struct {
	state         ControlState
	applied       uint64
	err           error
	advanceDuring bool
}

func (control *backupTestControl) State(context.Context) (ControlState, error) {
	if control.err != nil {
		return ControlState{}, control.err
	}
	if control.advanceDuring {
		control.applied++
	}
	return control.state.Clone(), nil
}

func (control *backupTestControl) Status() (uint64, uint64, error) {
	return 1, control.applied, nil
}

type backupTestPartition struct {
	core          *stationcore.Core
	lead          uint64
	applied       uint64
	barrierErr    error
	advanceDuring bool
	transferAfter bool
	barriers      int
	statusCalls   int
}

func (partition *backupTestPartition) readBarrier(context.Context) error {
	partition.barriers++
	return partition.barrierErr
}

func (partition *backupTestPartition) status() (uint64, uint64, error) {
	partition.statusCalls++
	lead := partition.lead
	if lead == 0 {
		lead = 1
	}
	if partition.transferAfter && partition.statusCalls > 1 {
		lead = lead%3 + 1
	}
	return lead, partition.applied, nil
}

func (partition *backupTestPartition) createSnapshot(root string, binding CoreSnapshotBinding, chunkBytes int) (CoreSnapshotManifest, error) {
	manifest, err := CreateCoreSnapshot(partition.core, root, binding, chunkBytes)
	if partition.advanceDuring {
		partition.applied++
	}
	return manifest, err
}

func TestCoordinatedBackupPublishesOnlyCompleteBarrierSet(t *testing.T) {
	core := openBackupTestCore(t)
	defer core.Close()
	state := backupTestControlState(t)
	partitions := backupTestPartitions(core, 19)
	partitions[23].(*backupTestPartition).barrierErr = ErrNotLeader
	destination := filepath.Join(t.TempDir(), "partial")
	_, err := createCoordinatedBackup(context.Background(), destination, 1, &backupTestControl{state: state, applied: 13}, partitions, MinCoreSnapshotChunkBytes)
	if !errors.Is(err, ErrBackupIncomplete) {
		t.Fatalf("partial backup error = %v", err)
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("partial backup became selectable: %v", err)
	}
	if partitions[24].(*backupTestPartition).barriers != 0 {
		t.Fatal("backup continued after the first failed partition")
	}

	partitions = backupTestPartitions(core, 19)
	partitions[7].(*backupTestPartition).advanceDuring = true
	destination = filepath.Join(t.TempDir(), "moving")
	if _, err := createCoordinatedBackup(context.Background(), destination, 1, &backupTestControl{state: state, applied: 13}, partitions, MinCoreSnapshotChunkBytes); !errors.Is(err, ErrBackupIncomplete) {
		t.Fatalf("moving applied index error = %v", err)
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("moving backup became selectable: %v", err)
	}

	partitions = backupTestPartitions(core, 19)
	partitions[11].(*backupTestPartition).transferAfter = true
	destination = filepath.Join(t.TempDir(), "leader-transfer")
	if _, err := createCoordinatedBackup(context.Background(), destination, 0, &backupTestControl{state: state, applied: 13}, partitions, MinCoreSnapshotChunkBytes); !errors.Is(err, ErrBackupIncomplete) {
		t.Fatalf("leader transfer error = %v", err)
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("leader-transfer backup became selectable: %v", err)
	}
}

func TestCoordinatedBackupAcceptsDistributedPartitionLeaders(t *testing.T) {
	core := openBackupTestCore(t)
	defer core.Close()
	state := backupTestControlState(t)
	partitions := backupTestPartitions(core, 23)
	for partition := range partitions {
		partitions[partition].(*backupTestPartition).lead = uint64(partition%3 + 1)
	}
	destination := filepath.Join(t.TempDir(), "distributed-leaders")
	manifest, err := createCoordinatedBackup(context.Background(), destination, 0, &backupTestControl{state: state, applied: 17}, partitions, MinCoreSnapshotChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[uint64]bool{}
	for _, partition := range manifest.Partitions {
		seen[partition.SourceMemberID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("distributed source count=%d", len(seen))
	}
}

func TestCoordinatedBackupQuorumLossAndRPOZeroBarrier(t *testing.T) {
	core := openBackupTestCore(t)
	defer core.Close()
	state := backupTestControlState(t)
	root := filepath.Join(t.TempDir(), "quorum-loss")
	_, err := createCoordinatedBackup(context.Background(), root, 1, &backupTestControl{state: state, applied: 13, err: ErrNotLeader}, backupTestPartitions(core, 21), MinCoreSnapshotChunkBytes)
	if !errors.Is(err, ErrBackupIncomplete) {
		t.Fatalf("control quorum loss error = %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("quorum-loss backup became selectable: %v", err)
	}
	root = filepath.Join(t.TempDir(), "moving-control")
	_, err = createCoordinatedBackup(context.Background(), root, 1, &backupTestControl{state: state, applied: 13, advanceDuring: true}, backupTestPartitions(core, 21), MinCoreSnapshotChunkBytes)
	if !errors.Is(err, ErrBackupIncomplete) {
		t.Fatalf("moving control barrier error = %v", err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("moving-control backup became selectable: %v", err)
	}

	partitions := backupTestPartitions(core, 21)
	root = filepath.Join(t.TempDir(), "surviving-quorum")
	manifest, err := createCoordinatedBackup(context.Background(), root, 1, &backupTestControl{state: state, applied: 13}, partitions, MinCoreSnapshotChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.ControlApplied != 13 || len(manifest.Partitions) != PartitionCount {
		t.Fatalf("manifest barrier metadata = %+v", manifest)
	}
	for index, entry := range manifest.Partitions {
		if entry.AppliedIndex != 21 || partitions[index].(*backupTestPartition).barriers != 1 {
			t.Fatalf("partition %d did not bind the surviving-quorum barrier", index)
		}
	}
}

func TestClusterBackupRestorePreflightTamperEpochAndFreshRoot(t *testing.T) {
	core := openBackupTestCore(t)
	defer core.Close()
	state := backupTestControlState(t)
	backupRoot := filepath.Join(t.TempDir(), "backup")
	manifest, err := createCoordinatedBackup(context.Background(), backupRoot, 1, &backupTestControl{state: state, applied: 31}, backupTestPartitions(core, 37), MinCoreSnapshotChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	expected := RestorePreflight{
		ClusterID: state.ClusterID, PlacementEpoch: state.PlacementEpoch,
		MinCommandVersion: 1, MinStoreVersion: StoreVersion, MaxCommandVersion: 2, MaxStoreVersion: StoreVersion,
	}
	restoreRoot := filepath.Join(t.TempDir(), "isolated")
	receipt, err := RestoreClusterBackup(backupRoot, restoreRoot, expected)
	if err != nil {
		t.Fatal(err)
	}
	if receipt.ClientAdmission || receipt.PartitionCount != PartitionCount || receipt.BackupDigest != manifest.DigestSHA256 {
		t.Fatalf("restore receipt = %+v", receipt)
	}
	for partition := uint16(0); partition < PartitionCount; partition++ {
		root := filepath.Join(restoreRoot, formatBackupPartition(partition))
		restored, err := stationcore.Open(stationcore.Config{DataRoot: root, ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, backupTestClock{})
		if err != nil {
			t.Fatalf("open restored partition %d: %v", partition, err)
		}
		if err := restored.Close(); err != nil {
			t.Fatal(err)
		}
	}

	wrongEpoch := expected
	wrongEpoch.PlacementEpoch++
	if _, err := VerifyClusterBackup(backupRoot, wrongEpoch); !errors.Is(err, ErrInvalidBackup) {
		t.Fatalf("wrong epoch error = %v", err)
	}
	existing := t.TempDir()
	if _, err := RestoreClusterBackup(backupRoot, existing, expected); !errors.Is(err, ErrInvalidBackup) {
		t.Fatalf("existing restore root error = %v", err)
	}
	if _, err := RestoreClusterBackup(backupRoot, filepath.Join(backupRoot, "nested"), expected); !errors.Is(err, ErrInvalidBackup) {
		t.Fatalf("overlapping restore root error = %v", err)
	}

	first := manifest.Partitions[0]
	coreManifest, err := readCoreSnapshotManifest(filepath.Join(backupRoot, first.Artifact))
	if err != nil {
		t.Fatal(err)
	}
	chunk := filepath.Join(backupRoot, first.Artifact, snapshotChunkName(coreManifest.Chunks[0].Index))
	file, err := os.OpenFile(chunk, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteAt([]byte{0xff}, 0); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyClusterBackup(backupRoot, expected); !errors.Is(err, ErrInvalidBackup) {
		t.Fatalf("tamper error = %v", err)
	}
}

func backupTestControlState(t *testing.T) ControlState {
	t.Helper()
	machine, err := OpenControlMachine(ControlBootstrap{
		Root: filepath.Join(t.TempDir(), "control"), ClusterID: "cluster-backup-contract", PlacementEpoch: 9,
		MinCommandVersion: 1, MinStoreVersion: StoreVersion,
		Members: []ControlMember{
			controlTestMember(1, "zone-a", 1, StoreVersion),
			controlTestMember(2, "zone-b", 1, StoreVersion),
			controlTestMember(3, "zone-c", 1, StoreVersion),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	state := machine.Snapshot()
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}
	return state
}

type backupTestCores struct {
	cores [PartitionCount]*stationcore.Core
	pool  *stationcore.StoragePool
}

func openBackupTestCore(t *testing.T) *backupTestCores {
	t.Helper()
	set := &backupTestCores{pool: stationcore.NewStoragePool()}
	root := t.TempDir()
	for partition := uint16(0); partition < PartitionCount; partition++ {
		core, err := stationcore.Open(stationcore.Config{
			DataRoot: filepath.Join(root, formatBackupPartition(partition)), ProfileID: stationcore.ProfileSingleNodeGeneralV1,
			StorageMode: stationcore.StorageModePartitionOne, Partition: partition, StoragePool: set.pool,
		}, backupTestClock{})
		if err != nil {
			set.Close()
			t.Fatal(err)
		}
		set.cores[partition] = core
	}
	return set
}

func (set *backupTestCores) Close() {
	for _, core := range set.cores {
		if core != nil {
			_ = core.Close()
		}
	}
	_ = set.pool.Close()
}

func backupTestPartitions(set *backupTestCores, applied uint64) []backupPartition {
	partitions := make([]backupPartition, PartitionCount)
	for index := range partitions {
		partitions[index] = &backupTestPartition{core: set.cores[index], applied: applied}
	}
	return partitions
}

func formatBackupPartition(partition uint16) string {
	return "partition-" + string([]byte{'0' + byte(partition/10), '0' + byte(partition%10)})
}

type backupTestClock struct{}

func (backupTestClock) NowUnixMs() int64            { return 1_800_000_000_000 }
func (backupTestClock) MonotonicNow() time.Duration { return time.Second }
