package stationcore

import (
	"os"
	"path/filepath"
	"strings"
)

type snapshotInstallFault uint8

const (
	snapshotInstallNoFault snapshotInstallFault = iota
	snapshotInstallFaultAfterClose
	snapshotInstallFaultAfterSwap
)

// SnapshotCheckpoint describes one consistent, independently openable Core
// checkpoint. The checkpoint is a storage contract only; it carries no Raft
// metadata and does not imply log compaction or snapshot installation.
type SnapshotCheckpoint struct {
	StoreIdentity string
	DigestSHA256  string
}

// OwnsOnlyPartition reports the closed physical topology used by Cluster
// snapshot binding. InstanceSharded is never a partition snapshot source.
func (core *Core) OwnsOnlyPartition(partition uint16) bool {
	return core != nil && core.config.StorageMode == StorageModePartitionOne && core.config.Partition == partition
}

// VerifySnapshotCheckpointTopology verifies both the recursive store and its
// exact physical topology without creating or migrating state.
func VerifySnapshotCheckpointTopology(root string, mode StorageMode, partition uint16) error {
	expected, err := expectedStoreManifest(mode, partition)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil || verifyStoreRootManifest(root, entries, expected) != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	return verifyStoreRoot(root)
}

// ExportSnapshotCheckpoint flushes the live Core through Pebble's checkpoint
// primitive into a fresh directory. Mutations and the checkpoint boundary are
// serialized by the store lock, so the exported tree represents one
// consistent Core state.
func (core *Core) ExportSnapshotCheckpoint(destination string) (SnapshotCheckpoint, error) {
	return core.exportSnapshotCheckpoint(destination, 0)
}

// ExportSnapshotCheckpointBounded is the cluster snapshot form. It rejects a
// checkpoint before level verification or hashing when its regular-file bytes
// exceed maxBytes; zero is invalid so callers must make the admission bound
// explicit.
func (core *Core) ExportSnapshotCheckpointBounded(destination string, maxBytes uint64) (SnapshotCheckpoint, error) {
	if maxBytes == 0 {
		return SnapshotCheckpoint{}, fail(ReasonInvalidRequest, false, 0)
	}
	return core.exportSnapshotCheckpoint(destination, maxBytes)
}

func (core *Core) exportSnapshotCheckpoint(destination string, maxBytes uint64) (SnapshotCheckpoint, error) {
	if err := core.rejectBackupRestoreTargets(destination, ""); err != nil {
		return SnapshotCheckpoint{}, err
	}
	if err := requireFreshRoot(destination); err != nil {
		return SnapshotCheckpoint{}, err
	}
	if err := core.store.checkpointTo(destination); err != nil {
		return SnapshotCheckpoint{}, err
	}
	if maxBytes > 0 {
		if _, err := storeTreeBytes(destination, maxBytes); err != nil {
			_ = os.RemoveAll(destination)
			return SnapshotCheckpoint{}, err
		}
	}
	if err := verifyStoreRoot(destination); err != nil {
		_ = os.RemoveAll(destination)
		return SnapshotCheckpoint{}, err
	}
	digest, err := storeTreeDigest(destination)
	if err != nil {
		_ = os.RemoveAll(destination)
		return SnapshotCheckpoint{}, fail(ReasonStorageUnavailable, true, 100)
	}
	return SnapshotCheckpoint{StoreIdentity: StoreIdentity, DigestSHA256: digest}, nil
}

// RestoreSnapshotCheckpoint copies a verified Core checkpoint into a fresh,
// isolated root. Existing or nested roots are rejected; failed imports leave
// no partially restored destination.
func RestoreSnapshotCheckpoint(checkpointRoot, isolatedRoot string) error {
	if checkpointRoot == "" || isolatedRoot == "" {
		return fail(ReasonInvalidRequest, false, 0)
	}
	source, err := filepath.Abs(checkpointRoot)
	if err != nil {
		return fail(ReasonInvalidRequest, false, 0)
	}
	destination, err := filepath.Abs(isolatedRoot)
	if err != nil || source == destination || pathsOverlap(source, destination) {
		return fail(ReasonInvalidRequest, false, 0)
	}
	if err := validateSnapshotCheckpointTree(source); err != nil {
		return err
	}
	if err := requireFreshRoot(destination); err != nil {
		return err
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(destination)
		}
	}()
	if err := copyStoreTree(source, destination); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if err := verifyStoreRoot(destination); err != nil {
		return err
	}
	ok = true
	return nil
}

// InstallSnapshotCheckpoint atomically replaces this Core's durable store
// with a verified checkpoint while preserving the Core object used by native
// callers. The caller must serialize replicated mutation application; the
// RaftMember run loop provides that ownership. Candidate validation and all
// copying happen before the live Pebble store is closed.
func (core *Core) InstallSnapshotCheckpoint(checkpointRoot string) error {
	return core.installSnapshotCheckpoint(checkpointRoot, snapshotInstallNoFault)
}

func (core *Core) installSnapshotCheckpoint(checkpointRoot string, injected snapshotInstallFault) error {
	liveRoot, err := filepath.Abs(core.config.DataRoot)
	if err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	parent := filepath.Dir(liveRoot)
	stagingRoot, err := os.MkdirTemp(parent, ".badtower-core-install-")
	if err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if err := os.Remove(stagingRoot); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	defer os.RemoveAll(stagingRoot)
	if err := RestoreSnapshotCheckpoint(checkpointRoot, stagingRoot); err != nil {
		return err
	}

	candidateConfig := core.config
	candidateConfig.DataRoot = stagingRoot
	candidate, err := Open(candidateConfig, core.clock)
	if err != nil {
		return err
	}
	candidateWatermark := candidate.watermark
	candidateClockSafe := candidate.clockSafe
	candidateCapacity := candidate.capacity
	if err := candidate.Close(); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}

	rollbackRoot, err := os.MkdirTemp(parent, ".badtower-core-rollback-")
	if err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if err := os.Remove(rollbackRoot); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	defer os.RemoveAll(rollbackRoot)

	core.mu.Lock()
	defer core.mu.Unlock()
	core.store.mu.Lock()
	defer core.store.mu.Unlock()
	if !core.store.opened {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if err := core.store.closeResourcesLocked(); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if injected == snapshotInstallFaultAfterClose {
		return core.reopenLiveStoreLocked(liveRoot)
	}
	if err := os.Rename(liveRoot, rollbackRoot); err != nil {
		return core.reopenLiveStoreLocked(liveRoot)
	}
	if err := os.Rename(stagingRoot, liveRoot); err != nil {
		_ = os.Rename(rollbackRoot, liveRoot)
		return core.reopenLiveStoreLocked(liveRoot)
	}
	if injected == snapshotInstallFaultAfterSwap {
		_ = os.Rename(liveRoot, stagingRoot)
		_ = os.Rename(rollbackRoot, liveRoot)
		return core.reopenLiveStoreLocked(liveRoot)
	}
	installedConfig := core.config
	installedConfig.DataRoot = liveRoot
	installed, err := openConfiguredStore(installedConfig)
	if err != nil {
		_ = os.Rename(liveRoot, stagingRoot)
		_ = os.Rename(rollbackRoot, liveRoot)
		if rollbackErr := core.reopenLiveStoreLocked(liveRoot); rollbackErr != nil {
			return rollbackErr
		}
		return err
	}
	core.store.adoptLocked(installed)
	core.watermark = candidateWatermark
	core.clockSafe = candidateClockSafe
	replaceCapacityTracker(core.capacity, candidateCapacity)
	return nil
}

func replaceCapacityTracker(destination, source *capacityTracker) {
	destination.mu.Lock()
	defer destination.mu.Unlock()
	source.mu.Lock()
	defer source.mu.Unlock()
	destination.limits = source.limits
	destination.globalPressure = source.globalPressure
	destination.nsPressure = source.nsPressure
	destination.counters = source.counters
	destination.station = source.station
	destination.stationPressure = source.stationPressure
}

func (core *Core) reopenLiveStoreLocked(liveRoot string) error {
	config := core.config
	config.DataRoot = liveRoot
	reopened, err := openConfiguredStore(config)
	if err != nil {
		return err
	}
	core.store.adoptLocked(reopened)
	return fail(ReasonStorageUnavailable, true, 100)
}

func validateSnapshotCheckpointTree(root string) error {
	return verifyStoreRoot(root)
}

func requireFreshRoot(root string) error {
	if root == "" || !filepath.IsAbs(root) {
		return fail(ReasonInvalidRequest, false, 0)
	}
	_, err := os.Stat(root)
	if err == nil {
		return fail(ReasonInvalidRequest, false, 0)
	}
	if !os.IsNotExist(err) {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	return nil
}

func pathsOverlap(left, right string) bool {
	rel, err := filepath.Rel(left, right)
	if err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))) {
		return true
	}
	rel, err = filepath.Rel(right, left)
	return err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))))
}
