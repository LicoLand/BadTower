//go:build station_cluster_acceptance

package acceptance

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcluster"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

// TestStationClusterLifecycle is deliberately non-empty and exact-name
// addressable. The default focused profile is expected to close in minutes;
// long load and soak profiles remain separate explicit runs.
func TestStationClusterLifecycle(t *testing.T) {
	profile := loadClusterCapacityProfile(t)
	started := time.Now()
	driver := newClusterLifecycleDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(profile.Focused.MaximumDurationSeconds)*time.Second)
	defer cancel()
	operator := driver.campaignAndOperator(ctx)
	if _, err := operator.Bootstrap(ctx); err != nil {
		t.Fatal(err)
	}

	hotNamespace := clusterNamespaceForPartition(t, 0)
	coldNamespace := clusterNamespaceForPartition(t, 1)
	hotAddress := driver.callReserve(ctx, []int{0, 1, 2}, stationcore.ReserveInput{NamespaceID: hotNamespace, RequestID: "focused-hot-reserve", TTLMs: 3_600_000})
	mutations := uint64(1)
	largePayload := make([]byte, 360<<10)
	for index := range largePayload {
		largePayload[index] = byte(index % 251)
	}
	backupRoot := filepath.Join(t.TempDir(), "backup")
	manifest, err := operator.Backup(ctx, backupRoot, stationcluster.MinCoreSnapshotChunkBytes)
	if err != nil {
		t.Fatalf("distributed coordinated backup failed closed: %v", err)
	}
	expected := stationcluster.RestorePreflight{ClusterID: driver.clusterID, PlacementEpoch: manifest.PlacementEpoch, MinCommandVersion: 1, MinStoreVersion: stationcluster.StoreVersion, MaxCommandVersion: 2, MaxStoreVersion: stationcluster.StoreVersion + 1}
	isolatedRoot := filepath.Join(t.TempDir(), "isolated")
	if _, err = operator.RestorePreflight(backupRoot, isolatedRoot, expected); err != nil {
		t.Fatal(err)
	}
	receipt, err := stationcluster.RestoreClusterBackup(backupRoot, isolatedRoot, expected)
	if err != nil || receipt.ClientAdmission || receipt.PartitionCount != stationcluster.PartitionCount {
		t.Fatalf("isolated restore admission=%t partitions=%d err=%v", receipt.ClientAdmission, receipt.PartitionCount, err)
	}
	var committed stationcore.SubmitOutput
	var committedInput stationcore.SubmitInput
	for index := 0; index < 35; index++ {
		payload := []byte{byte(index)}
		if index < 3 {
			payload = largePayload
		}
		requestID := fmt.Sprintf("focused-hot-submit-%03d", index)
		input := stationcore.SubmitInput{NamespaceID: hotNamespace, RequestID: requestID, AddressID: hotAddress.AddressID, Payload: payload, TTLMs: 3_600_000}
		output := driver.callSubmit(ctx, []int{0, 1, 2}, input)
		mutations++
		if index == 34 {
			committed = output
			committedInput = input
			duplicate := driver.callSubmit(ctx, []int{0, 1, 2}, input)
			if duplicate.UnitID != output.UnitID {
				t.Fatal("idempotent duplicate changed the committed unit")
			}
		}
	}
	if _, err := operator.Join(ctx, stationcluster.ControlMember{ID: 4, FaultDomain: "fd-4", Incarnation: 1, CommandVersion: 1, StoreVersion: stationcluster.StoreVersion}); err != nil {
		t.Fatal(err)
	}
	var caughtUp bool
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(25 * time.Millisecond) {
		if _, err := operator.MarkCaughtUp(ctx, 4, 0); err == nil {
			caughtUp = true
			break
		}
	}
	if !caughtUp {
		t.Fatal("stream snapshot learner did not catch up")
	}
	status, err := operator.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := operator.UpgradePreflight(ctx, stationcluster.UpgradePreflight{Current: stationcluster.ClusterVersion{Command: 1, Store: stationcluster.StoreVersion}, Next: stationcluster.ClusterVersion{Command: 2, Store: stationcluster.StoreVersion + 1}, LeaderMemberIDs: []uint64{status.LeaderMemberID}})
	if err != nil || len(steps) != 4 || steps[0].Stage != stationcluster.UpgradeCanaryLearner {
		t.Fatalf("upgrade preflight steps=%d err=%v", len(steps), err)
	}
	if _, err = operator.Promote(ctx, 4); err != nil {
		t.Fatal(err)
	}
	if _, err = operator.Drain(ctx, 3); err != nil {
		t.Fatal(err)
	}
	if _, err = operator.Rebalance(ctx, 3, 4); err != nil {
		t.Fatal(err)
	}
	operator, err = stationcluster.NewOperator(driver.nodes[int(status.LeaderMemberID)-1], driver.nodes[int(status.LeaderMemberID)-1].PartitionMembership(), 1, stationcluster.StoreVersion)
	if err != nil {
		t.Fatal(err)
	}
	for phase := 0; phase < 2; phase++ {
		if _, err = operator.Rebalance(ctx, 3, 4); err != nil {
			t.Fatal(err)
		}
	}

	coldAddress := driver.callReserve(ctx, []int{0, 1, 2, 3}, stationcore.ReserveInput{NamespaceID: coldNamespace, RequestID: "focused-cold-reserve", TTLMs: 3_600_000})
	mutations++
	coldUnit := driver.callSubmit(ctx, []int{0, 1, 2, 3}, stationcore.SubmitInput{NamespaceID: coldNamespace, RequestID: "focused-cold-submit", AddressID: coldAddress.AddressID, Payload: []byte("cold"), TTLMs: 3_600_000})
	mutations++
	coldClaim := driver.callClaim(ctx, []int{0, 1, 2, 3}, stationcore.ClaimInput{NamespaceID: coldNamespace, RequestID: "focused-cold-claim", AddressID: coldAddress.AddressID, MaxUnits: 1, MaxBytes: 1024, VisibilityTimeoutMs: 30_000})
	mutations++
	if coldClaim.Units[0].UnitID != coldUnit.UnitID {
		t.Fatal("cold namespace did not make fair progress")
	}
	if _, err = driver.nodes[0].Router().Settle(ctx, stationcore.SettleInput{NamespaceID: coldNamespace, RequestID: "focused-cold-settle", ClaimID: coldClaim.ClaimID, ClaimEpoch: coldClaim.ClaimEpoch, Entries: []stationcore.SettleEntry{{UnitID: coldUnit.UnitID, Action: stationcore.SettleComplete}}}); err != nil {
		// Node one is the deterministic campaign target; retry through the
		// remaining replicas if leadership moved during the prior operation.
		settled := false
		for _, index := range []int{1, 2, 3} {
			if _, retryErr := driver.nodes[index].Router().Settle(ctx, stationcore.SettleInput{NamespaceID: coldNamespace, RequestID: "focused-cold-settle", ClaimID: coldClaim.ClaimID, ClaimEpoch: coldClaim.ClaimEpoch, Entries: []stationcore.SettleEntry{{UnitID: coldUnit.UnitID, Action: stationcore.SettleComplete}}}); retryErr == nil {
				settled = true
				break
			}
		}
		if !settled {
			t.Fatal("cold namespace settlement failed")
		}
	}
	mutations++

	placement, err := (stationcluster.Topology{ClusterID: driver.clusterID, Epoch: 7, Members: []stationcluster.Member{{ID: 1, FaultDomain: "a", Incarnation: 1}, {ID: 2, FaultDomain: "b", Incarnation: 1}, {ID: 3, FaultDomain: "c", Incarnation: 1}}}).Placements()
	if err != nil || !errors.Is(placement[0].Accepts(placement[0].Replicas[0], 6), stationcluster.ErrStaleEpoch) {
		t.Fatal("stale ownership epoch was accepted")
	}
	assertStaleSnapshotRejected(t, driver.clock, driver.clusterID)

	if status.LeaderMemberID == 0 || status.LeaderMemberID > 3 {
		t.Fatal("focused gate did not observe a bootstrap voter leader")
	}
	failedIndex := int(status.LeaderMemberID) - 1
	if err := driver.nodes[failedIndex].Close(); err != nil {
		t.Fatal(err)
	}
	driver.nodes[failedIndex] = nil
	active := make([]int, 0, 3)
	for index := range 4 {
		if index != failedIndex {
			active = append(active, index)
		}
	}
	failoverStarted := time.Now()
	replayed := driver.callSubmit(ctx, active, committedInput)
	failoverRTO := time.Since(failoverStarted)
	if replayed.UnitID != committed.UnitID {
		t.Fatal("acknowledged write was lost with a surviving quorum")
	}
	mutations++

	evidence := clusterLifecycleEvidence{Schema: clusterEvidenceSchema, ProfileID: profile.ProfileID, Mode: "focused", MembersObserved: 4, Partitions: stationcluster.PartitionCount, NativeMutations: mutations, ColdNamespaceCompletions: 1, RebalanceMoves: 1, BackupPartitions: uint16(len(manifest.Partitions)), RestoredPartitions: receipt.PartitionCount, CommittedWriteLoss: 0, FailoverRTOMillis: failoverRTO.Milliseconds(), Scenarios: append([]string(nil), profile.Focused.RequiredScenarios...)}
	if err := evidence.validate(profile); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > time.Duration(profile.Focused.MaximumDurationSeconds)*time.Second {
		t.Fatalf("focused lifecycle exceeded profile duration: %s", elapsed)
	}
	emitClusterLifecycleEvidence(t, evidence)
}

func TestStationClusterLifecycleLoad(t *testing.T) {
	profile := loadClusterCapacityProfile(t)
	if os.Getenv(profile.Load.OptInEnvironment) != "1" {
		t.Skip("cluster load profile is an explicit long-running gate; no evidence was generated")
	}
	t.Skip("cluster load execution is declared but intentionally not claimed by this focused source closure")
}

func TestStationClusterLifecycleSoak(t *testing.T) {
	profile := loadClusterCapacityProfile(t)
	if os.Getenv(profile.Soak.OptInEnvironment) != "1" {
		t.Skip("cluster soak profile is an explicit long-running gate; no evidence was generated")
	}
	t.Skip("cluster soak execution is declared but intentionally not claimed by this focused source closure")
}

func clusterNamespaceForPartition(t *testing.T, partition uint16) string {
	t.Helper()
	for candidate := 1; candidate < 1_000_000; candidate++ {
		namespace := fmt.Sprintf("ns_%022d", candidate)
		if stationcore.PartitionFor(namespace) == partition {
			return namespace
		}
	}
	t.Fatal("namespace partition fixture not found")
	return ""
}

func assertStaleSnapshotRejected(t *testing.T, clock stationcore.Clock, clusterID string) {
	t.Helper()
	pool := stationcore.NewStoragePool()
	core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: 0, StoragePool: pool}, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = core.Close()
		_ = pool.Close()
	})
	root := filepath.Join(t.TempDir(), "snapshot")
	manifest, err := stationcluster.CreateCoreSnapshot(core, root, stationcluster.CoreSnapshotBinding{ClusterID: clusterID, GroupID: 1, SourceMemberID: 1, Partition: 0, OwnershipEpoch: 1, AppliedIndex: 2}, stationcluster.MinCoreSnapshotChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stationcluster.RestoreCoreSnapshot(root, stationcluster.CoreSnapshotExpectation{ClusterID: clusterID, GroupID: 1, SourceMemberID: 1, Partition: 0, OwnershipEpoch: 1, CurrentAppliedIndex: manifest.AppliedIndex}, filepath.Join(t.TempDir(), "restore"))
	if !errors.Is(err, stationcluster.ErrStaleSnapshot) {
		t.Fatalf("stale snapshot error=%v", err)
	}
}
