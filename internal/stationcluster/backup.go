package stationcluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"
)

const (
	ClusterBackupFormat        = "badtower.station-cluster.backup.v2"
	IsolatedRestoreFormat      = "badtower.station-cluster.isolated-restore.v2"
	defaultBackupSnapshotChunk = 4 << 20
	maxClusterBackupManifest   = 2 << 20
)

var (
	ErrBackupIncomplete = errors.New("cluster backup is incomplete")
	ErrInvalidBackup    = errors.New("invalid cluster backup")
)

type ClusterBackupPartition struct {
	Partition      uint16 `json:"partition"`
	GroupID        uint64 `json:"groupId"`
	SourceMemberID uint64 `json:"sourceMemberId"`
	OwnershipEpoch uint64 `json:"ownershipEpoch"`
	AppliedIndex   uint64 `json:"appliedIndex"`
	Artifact       string `json:"artifact"`
	ArtifactDigest string `json:"artifactDigestSha256"`
}

// ClusterBackupManifest is published only after the control ReadIndex and all
// partition ReadIndex/checkpoint barriers have completed. DigestSHA256 binds
// the manifest metadata; each entry separately binds its CoreSnapshot bytes.
type ClusterBackupManifest struct {
	Format            string                   `json:"format"`
	ClusterID         string                   `json:"clusterId"`
	PlacementEpoch    uint64                   `json:"placementEpoch"`
	ControlGroupID    uint64                   `json:"controlGroupId"`
	ControlApplied    uint64                   `json:"controlAppliedIndex"`
	MinCommandVersion uint32                   `json:"minCommandVersion"`
	MinStoreVersion   uint32                   `json:"minStoreVersion"`
	ControlState      ControlState             `json:"controlState"`
	Partitions        []ClusterBackupPartition `json:"partitions"`
	DigestSHA256      string                   `json:"digestSha256"`
}

type IsolatedRestoreReceipt struct {
	Format          string `json:"format"`
	ClusterID       string `json:"clusterId"`
	PlacementEpoch  uint64 `json:"placementEpoch"`
	PartitionCount  uint16 `json:"partitionCount"`
	BackupDigest    string `json:"backupDigestSha256"`
	ElapsedMillis   int64  `json:"elapsedMillis"`
	ClientAdmission bool   `json:"clientAdmission"`
}

type RestorePreflight struct {
	ClusterID         string
	PlacementEpoch    uint64
	MinCommandVersion uint32
	MinStoreVersion   uint32
	MaxCommandVersion uint32
	MaxStoreVersion   uint32
}

type backupControl interface {
	State(context.Context) (ControlState, error)
	Status() (lead, applied uint64, fatal error)
}

type backupPartition interface {
	readBarrier(context.Context) error
	status() (lead, applied uint64, fatal error)
	createSnapshot(string, CoreSnapshotBinding, int) (CoreSnapshotManifest, error)
}

type nodeBackupSession struct {
	node           *Node
	membership     *nodePartitionMembership
	backupID       uint64
	expectedEpoch  uint64
	commandVersion uint32
	storeVersion   uint32
}

func (session *nodeBackupSession) authorizePartition(ctx context.Context, partition uint16, sourceMemberID uint64) (uint64, error) {
	if session == nil || session.node == nil || session.node.control == nil {
		return 0, ErrBackupIncomplete
	}
	state, err := session.node.control.Propose(ctx, ControlCommand{
		CommandVersion: session.commandVersion,
		StoreVersion:   session.storeVersion,
		Kind:           ControlAuthorizeBackup,
		ExpectedEpoch:  session.expectedEpoch,
		MemberID:       session.node.id,
		Partition:      partition,
		ToMemberID:     sourceMemberID,
		BackupID:       session.backupID,
	})
	if err != nil || state.ActiveBackup == nil || state.ActiveBackup.Partition != partition || state.ActiveBackup.SourceMemberID != sourceMemberID {
		return 0, ErrBackupIncomplete
	}
	session.backupID = state.ActiveBackup.BackupID
	_, controlApplied, fatal := session.node.control.Status()
	if fatal != nil || controlApplied == 0 {
		return 0, ErrBackupIncomplete
	}
	if err := session.membership.waitControlApplied(ctx, sourceMemberID, controlApplied); err != nil {
		return 0, ErrBackupIncomplete
	}
	return session.backupID, nil
}

func (session *nodeBackupSession) complete() {
	if session == nil || session.node == nil || session.node.control == nil || session.backupID == 0 {
		return
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	state, err := session.node.control.State(cleanupCtx)
	if err != nil || state.ActiveBackup == nil || state.ActiveBackup.BackupID != session.backupID {
		return
	}
	_, _ = session.node.control.Propose(cleanupCtx, ControlCommand{
		CommandVersion: state.MinCommandVersion,
		StoreVersion:   state.MinStoreVersion,
		Kind:           ControlCompleteBackup,
		ExpectedEpoch:  state.PlacementEpoch,
		MemberID:       session.node.id,
		BackupID:       session.backupID,
	})
}

type nodeBackupPartition struct {
	session   *nodeBackupSession
	partition uint16
}

func (partition nodeBackupPartition) readBarrier(ctx context.Context) error {
	leader, status, err := partition.session.membership.leader(ctx, partition.partition, ^uint64(0))
	if err != nil {
		return err
	}
	backupID, err := partition.session.authorizePartition(ctx, partition.partition, leader)
	if err != nil {
		return err
	}
	return partition.session.membership.backupReadBarrier(ctx, partition.partition, leader, status.epoch, backupID)
}

func (partition nodeBackupPartition) status() (uint64, uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), peerRequestTimeout)
	defer cancel()
	leader, status, err := partition.session.membership.leader(ctx, partition.partition, ^uint64(0))
	return leader, status.applied, err
}

func (partition nodeBackupPartition) createSnapshot(root string, binding CoreSnapshotBinding, chunkBytes int) (CoreSnapshotManifest, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if partition.session.backupID == 0 {
		return CoreSnapshotManifest{}, ErrBackupIncomplete
	}
	return partition.session.membership.fetchBackupSnapshot(ctx, partition.partition, binding, partition.session.backupID, chunkBytes, root)
}

// CreateBackup creates a coordinated backup from the control leader. Each
// partition is authorized in control Raft, read-barriered, and streamed from
// its independently discovered leader; leadership never needs to be collected
// on the coordinator. A leader/applied-index change fails the whole unpublished
// attempt closed.
func (node *Node) CreateBackup(ctx context.Context, artifactRoot string, chunkBytes int) (ClusterBackupManifest, error) {
	if node == nil || node.control == nil {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	lead, _, fatal := node.control.Status()
	if fatal != nil || lead != node.id {
		return ClusterBackupManifest{}, ErrNotLeader
	}
	state, err := node.control.State(ctx)
	if err != nil {
		return ClusterBackupManifest{}, errors.Join(ErrBackupIncomplete, err)
	}
	if state.ActiveBackup != nil {
		if _, err := node.control.Propose(ctx, ControlCommand{
			CommandVersion: state.MinCommandVersion,
			StoreVersion:   state.MinStoreVersion,
			Kind:           ControlCompleteBackup,
			ExpectedEpoch:  state.PlacementEpoch,
			MemberID:       node.id,
		}); err != nil {
			return ClusterBackupManifest{}, errors.Join(ErrBackupIncomplete, err)
		}
		state, err = node.control.State(ctx)
		if err != nil {
			return ClusterBackupManifest{}, errors.Join(ErrBackupIncomplete, err)
		}
	}
	membership := &nodePartitionMembership{node: node}
	session := &nodeBackupSession{
		node:           node,
		membership:     membership,
		expectedEpoch:  state.PlacementEpoch,
		commandVersion: state.MinCommandVersion,
		storeVersion:   state.MinStoreVersion,
	}
	defer session.complete()
	partitions := make([]backupPartition, PartitionCount)
	for partition := uint16(0); partition < PartitionCount; partition++ {
		partitions[partition] = nodeBackupPartition{session: session, partition: partition}
	}
	return createCoordinatedBackup(ctx, artifactRoot, 0, node.control, partitions, chunkBytes)
}

func createCoordinatedBackup(ctx context.Context, artifactRoot string, sourceMemberID uint64, control backupControl, partitions []backupPartition, chunkBytes int) (ClusterBackupManifest, error) {
	if control == nil || len(partitions) != PartitionCount || !filepath.IsAbs(artifactRoot) {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	if chunkBytes == 0 {
		chunkBytes = defaultBackupSnapshotChunk
	}
	if chunkBytes < MinCoreSnapshotChunkBytes || chunkBytes > MaxCoreSnapshotChunkBytes {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	if err := requireAbsentAbsoluteRoot(artifactRoot); err != nil {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	_, controlAppliedBefore, fatal := control.Status()
	if fatal != nil || controlAppliedBefore == 0 {
		return ClusterBackupManifest{}, ErrBackupIncomplete
	}
	state, err := control.State(ctx)
	if err != nil {
		return ClusterBackupManifest{}, errors.Join(ErrBackupIncomplete, err)
	}
	_, controlAppliedAfter, fatal := control.Status()
	if fatal != nil || controlAppliedAfter != controlAppliedBefore || state.Validate() != nil {
		return ClusterBackupManifest{}, ErrBackupIncomplete
	}

	parent := filepath.Dir(filepath.Clean(artifactRoot))
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return ClusterBackupManifest{}, err
	}
	staging, err := os.MkdirTemp(parent, ".badtower-cluster-backup-")
	if err != nil {
		return ClusterBackupManifest{}, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()

	manifest := ClusterBackupManifest{
		Format: ClusterBackupFormat, ClusterID: state.ClusterID, PlacementEpoch: state.PlacementEpoch,
		ControlGroupID: ControlGroupID, ControlApplied: controlAppliedBefore,
		MinCommandVersion: state.MinCommandVersion, MinStoreVersion: state.MinStoreVersion,
		ControlState: state.Clone(), Partitions: make([]ClusterBackupPartition, 0, PartitionCount),
	}
	for partitionID := uint16(0); partitionID < PartitionCount; partitionID++ {
		partition := partitions[partitionID]
		if partition == nil || partition.readBarrier(ctx) != nil {
			return ClusterBackupManifest{}, ErrBackupIncomplete
		}
		leaderBefore, appliedBefore, fatal := partition.status()
		if fatal != nil || leaderBefore == 0 || appliedBefore == 0 {
			return ClusterBackupManifest{}, ErrBackupIncomplete
		}
		placement := state.Placements[partitionID]
		partitionSource := sourceMemberID
		if partitionSource == 0 {
			partitionSource = leaderBefore
		}
		if partitionSource != leaderBefore || !slices.Contains(placement.Replicas[:], partitionSource) {
			return ClusterBackupManifest{}, ErrBackupIncomplete
		}
		artifact := fmt.Sprintf("partition-%02d", partitionID)
		coreManifest, err := partition.createSnapshot(filepath.Join(staging, artifact), CoreSnapshotBinding{
			ClusterID: state.ClusterID, GroupID: uint64(partitionID) + 1, SourceMemberID: partitionSource,
			Partition: partitionID, OwnershipEpoch: placement.Epoch, AppliedIndex: appliedBefore,
		}, chunkBytes)
		if err != nil {
			return ClusterBackupManifest{}, errors.Join(ErrBackupIncomplete, err)
		}
		leaderAfter, appliedAfter, fatal := partition.status()
		if fatal != nil || leaderAfter != leaderBefore || appliedAfter != appliedBefore || coreManifest.AppliedIndex != appliedBefore {
			return ClusterBackupManifest{}, ErrBackupIncomplete
		}
		manifest.Partitions = append(manifest.Partitions, ClusterBackupPartition{
			Partition: partitionID, GroupID: uint64(partitionID) + 1, SourceMemberID: partitionSource,
			OwnershipEpoch: placement.Epoch, AppliedIndex: appliedBefore, Artifact: artifact,
			ArtifactDigest: coreManifest.DigestSHA256,
		})
	}
	digest, raw, err := marshalClusterBackupManifest(manifest)
	if err != nil {
		return ClusterBackupManifest{}, err
	}
	manifest.DigestSHA256 = digest
	raw, err = json.Marshal(manifest)
	if err != nil || len(raw) > maxClusterBackupManifest {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	if err := writeSnapshotManifest(filepath.Join(staging, "manifest.json"), raw); err != nil || syncSnapshotDir(staging) != nil {
		return ClusterBackupManifest{}, ErrBackupIncomplete
	}
	if err := os.Rename(staging, artifactRoot); err != nil {
		return ClusterBackupManifest{}, err
	}
	if err := syncSnapshotDir(parent); err != nil {
		_ = os.RemoveAll(artifactRoot)
		_ = syncSnapshotDir(parent)
		return ClusterBackupManifest{}, err
	}
	published = true
	return manifest, nil
}

// RestoreClusterBackup verifies the complete closed manifest before creating
// any destination state. It restores only into a fresh isolated root and
// deliberately emits ClientAdmission=false; activation is an external step.
func RestoreClusterBackup(backupRoot, isolatedRoot string, expected RestorePreflight) (IsolatedRestoreReceipt, error) {
	started := time.Now()
	if snapshotRootsOverlap(backupRoot, isolatedRoot) || !filepath.IsAbs(isolatedRoot) {
		return IsolatedRestoreReceipt{}, ErrInvalidBackup
	}
	manifest, err := VerifyClusterBackup(backupRoot, expected)
	if err != nil {
		return IsolatedRestoreReceipt{}, err
	}
	if err := requireAbsentAbsoluteRoot(isolatedRoot); err != nil {
		return IsolatedRestoreReceipt{}, ErrInvalidBackup
	}
	parent := filepath.Dir(filepath.Clean(isolatedRoot))
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return IsolatedRestoreReceipt{}, err
	}
	staging, err := os.MkdirTemp(parent, ".badtower-cluster-restore-")
	if err != nil {
		return IsolatedRestoreReceipt{}, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(staging)
		}
	}()
	for _, partition := range manifest.Partitions {
		_, err := RestoreCoreSnapshot(filepath.Join(backupRoot, partition.Artifact), CoreSnapshotExpectation{
			ClusterID: manifest.ClusterID, GroupID: partition.GroupID, SourceMemberID: partition.SourceMemberID,
			Partition: partition.Partition, OwnershipEpoch: partition.OwnershipEpoch, CurrentAppliedIndex: 0,
		}, filepath.Join(staging, partition.Artifact))
		if err != nil {
			return IsolatedRestoreReceipt{}, errors.Join(ErrInvalidBackup, err)
		}
	}
	receipt := IsolatedRestoreReceipt{
		Format: IsolatedRestoreFormat, ClusterID: manifest.ClusterID, PlacementEpoch: manifest.PlacementEpoch,
		PartitionCount: PartitionCount, BackupDigest: manifest.DigestSHA256,
		ElapsedMillis: time.Since(started).Milliseconds(), ClientAdmission: false,
	}
	raw, err := json.Marshal(receipt)
	if err != nil || writeSnapshotManifest(filepath.Join(staging, "restore.json"), raw) != nil || syncSnapshotDir(staging) != nil {
		return IsolatedRestoreReceipt{}, ErrInvalidBackup
	}
	if err := os.Rename(staging, isolatedRoot); err != nil {
		return IsolatedRestoreReceipt{}, err
	}
	if err := syncSnapshotDir(parent); err != nil {
		_ = os.RemoveAll(isolatedRoot)
		_ = syncSnapshotDir(parent)
		return IsolatedRestoreReceipt{}, err
	}
	complete = true
	return receipt, nil
}

func VerifyClusterBackup(root string, expected RestorePreflight) (ClusterBackupManifest, error) {
	manifest, err := readClusterBackupManifest(root)
	if err != nil || validateRestorePreflight(manifest, expected) != nil {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != PartitionCount+1 {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	for _, partition := range manifest.Partitions {
		artifactRoot := filepath.Join(root, partition.Artifact)
		coreManifest, err := readCoreSnapshotManifest(artifactRoot)
		if err != nil || verifySnapshotArtifact(artifactRoot, coreManifest) != nil ||
			coreManifest.ClusterID != manifest.ClusterID || coreManifest.GroupID != partition.GroupID ||
			coreManifest.SourceMemberID != partition.SourceMemberID || coreManifest.Partition != partition.Partition ||
			coreManifest.OwnershipEpoch != partition.OwnershipEpoch || coreManifest.AppliedIndex != partition.AppliedIndex ||
			coreManifest.DigestSHA256 != partition.ArtifactDigest {
			return ClusterBackupManifest{}, ErrInvalidBackup
		}
	}
	return manifest, nil
}

func readClusterBackupManifest(root string) (ClusterBackupManifest, error) {
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil || len(raw) == 0 || len(raw) > maxClusterBackupManifest {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest ClusterBackupManifest
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateClusterBackupManifest(manifest) != nil {
		return ClusterBackupManifest{}, ErrInvalidBackup
	}
	return manifest, nil
}

func validateClusterBackupManifest(manifest ClusterBackupManifest) error {
	if manifest.Format != ClusterBackupFormat || manifest.ControlGroupID != ControlGroupID || manifest.ControlApplied == 0 ||
		manifest.ClusterID == "" || manifest.ClusterID != manifest.ControlState.ClusterID || manifest.PlacementEpoch != manifest.ControlState.PlacementEpoch ||
		manifest.MinCommandVersion != manifest.ControlState.MinCommandVersion || manifest.MinStoreVersion != manifest.ControlState.MinStoreVersion ||
		manifest.ControlState.Validate() != nil || len(manifest.Partitions) != PartitionCount || len(manifest.DigestSHA256) != sha256.Size*2 {
		return ErrInvalidBackup
	}
	for index, partition := range manifest.Partitions {
		placement := manifest.ControlState.Placements[index]
		if partition.Partition != uint16(index) || partition.GroupID != uint64(index)+1 || partition.SourceMemberID == 0 ||
			partition.OwnershipEpoch != placement.Epoch || partition.AppliedIndex == 0 || partition.Artifact != fmt.Sprintf("partition-%02d", index) ||
			len(partition.ArtifactDigest) != sha256.Size*2 || !slices.Contains(placement.Replicas[:], partition.SourceMemberID) {
			return ErrInvalidBackup
		}
		if _, err := hex.DecodeString(partition.ArtifactDigest); err != nil {
			return ErrInvalidBackup
		}
	}
	want, _, err := marshalClusterBackupManifest(manifest)
	if err != nil || want != manifest.DigestSHA256 {
		return ErrInvalidBackup
	}
	return nil
}

func marshalClusterBackupManifest(manifest ClusterBackupManifest) (string, []byte, error) {
	manifest.DigestSHA256 = ""
	raw, err := json.Marshal(manifest)
	if err != nil || len(raw) > maxClusterBackupManifest {
		return "", nil, ErrInvalidBackup
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), raw, nil
}

func validateRestorePreflight(manifest ClusterBackupManifest, expected RestorePreflight) error {
	if expected.ClusterID == "" || manifest.ClusterID != expected.ClusterID || expected.PlacementEpoch == 0 || manifest.PlacementEpoch != expected.PlacementEpoch ||
		expected.MinCommandVersion == 0 || expected.MinStoreVersion == 0 || expected.MaxCommandVersion < expected.MinCommandVersion || expected.MaxStoreVersion < expected.MinStoreVersion ||
		manifest.MinCommandVersion < expected.MinCommandVersion || manifest.MinCommandVersion > expected.MaxCommandVersion ||
		manifest.MinStoreVersion < expected.MinStoreVersion || manifest.MinStoreVersion > expected.MaxStoreVersion {
		return ErrInvalidBackup
	}
	return nil
}

func copyVerifiedSnapshotArtifact(source, destination string) (CoreSnapshotManifest, error) {
	manifest, err := readCoreSnapshotManifest(source)
	if err != nil || verifySnapshotArtifact(source, manifest) != nil || requireAbsentAbsoluteRoot(destination) != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return CoreSnapshotManifest{}, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(destination)
		}
	}()
	buffer := make([]byte, 64<<10)
	for _, chunk := range manifest.Chunks {
		sourceFile, openErr := os.Open(filepath.Join(source, snapshotChunkName(chunk.Index)))
		if openErr != nil {
			return CoreSnapshotManifest{}, openErr
		}
		destinationFile, createErr := os.OpenFile(filepath.Join(destination, snapshotChunkName(chunk.Index)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if createErr != nil {
			_ = sourceFile.Close()
			return CoreSnapshotManifest{}, createErr
		}
		_, copyErr := io.CopyBuffer(destinationFile, sourceFile, buffer)
		sourceCloseErr := sourceFile.Close()
		syncErr := destinationFile.Sync()
		destinationCloseErr := destinationFile.Close()
		if copyErr != nil || sourceCloseErr != nil || syncErr != nil || destinationCloseErr != nil {
			return CoreSnapshotManifest{}, ErrInvalidSnapshot
		}
	}
	manifestRaw, err := os.ReadFile(filepath.Join(source, "manifest.json"))
	if err != nil || writeSnapshotManifest(filepath.Join(destination, "manifest.json"), manifestRaw) != nil || syncSnapshotDir(destination) != nil || verifySnapshotArtifact(destination, manifest) != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	complete = true
	return manifest, nil
}
