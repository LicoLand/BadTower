package stationcluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net/http"
	"strconv"
	"time"
)

type peerPartitionStatus struct{ lead, applied, epoch uint64 }

type nodePartitionMembership struct{ node *Node }

func (node *Node) PartitionMembership() PartitionMembership {
	return &nodePartitionMembership{node: node}
}

func (membership *nodePartitionMembership) status(ctx context.Context, partition uint16, memberID uint64) (peerPartitionStatus, error) {
	return membership.groupStatus(ctx, uint64(partition)+1, memberID)
}

func (membership *nodePartitionMembership) groupStatus(ctx context.Context, groupID, memberID uint64) (peerPartitionStatus, error) {
	target := membership.node.peerClient.targets[memberID]
	if target == nil {
		return peerPartitionStatus{}, ErrMemberUnknown
	}
	endpoint := *target
	endpoint.Path = nodeOperatorPathPrefix + strconv.FormatUint(groupID, 10) + "/status"
	requestCtx, cancel := context.WithTimeout(ctx, peerRequestTimeout)
	defer cancel()
	request, _ := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	response, err := membership.node.peerClient.do(memberID, request)
	if err != nil {
		return peerPartitionStatus{}, ErrMemberStopped
	}
	defer response.Body.Close()
	raw, readErr := io.ReadAll(io.LimitReader(response.Body, 25))
	if readErr != nil || response.StatusCode != http.StatusOK || len(raw) != 24 {
		return peerPartitionStatus{}, ErrMemberStopped
	}
	return peerPartitionStatus{lead: binary.BigEndian.Uint64(raw[:8]), applied: binary.BigEndian.Uint64(raw[8:16]), epoch: binary.BigEndian.Uint64(raw[16:24])}, nil
}

func (node *Node) controlCaughtUpIndex(ctx context.Context, memberID uint64) (uint64, error) {
	membership := &nodePartitionMembership{node: node}
	leaderStatus, err := membership.groupStatus(ctx, ControlGroupID, node.control.ID())
	if err != nil || leaderStatus.lead != node.control.ID() {
		return 0, ErrNotLeader
	}
	target, err := membership.groupStatus(ctx, ControlGroupID, memberID)
	if err != nil || target.applied < leaderStatus.applied {
		return 0, ErrControlOrder
	}
	return target.applied, nil
}

func (membership *nodePartitionMembership) leader(ctx context.Context, partition uint16, epoch uint64) (uint64, peerPartitionStatus, error) {
	for _, configured := range membership.node.config.Members {
		status, err := membership.status(ctx, partition, configured.ID)
		if err == nil && status.lead == configured.ID && epoch >= status.epoch {
			return configured.ID, status, nil
		}
	}
	return 0, peerPartitionStatus{}, ErrNotLeader
}

func (membership *nodePartitionMembership) waitControlApplied(ctx context.Context, memberID, minApplied uint64) error {
	if memberID == 0 || minApplied == 0 || memberID == membership.node.id {
		return nil
	}
	waitCtx, cancel := context.WithTimeout(ctx, peerRequestTimeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		status, err := membership.groupStatus(waitCtx, ControlGroupID, memberID)
		if err == nil && status.applied >= minApplied {
			return nil
		}
		select {
		case <-waitCtx.Done():
			return ErrControlOrder
		case <-ticker.C:
		}
	}
}

func (membership *nodePartitionMembership) backupReadBarrier(ctx context.Context, partition uint16, leaderID, epoch, backupID uint64) error {
	if partition >= PartitionCount || leaderID == 0 || epoch == 0 || backupID == 0 {
		return ErrBackupIncomplete
	}
	target := membership.node.peerClient.targets[leaderID]
	if target == nil {
		return ErrMemberUnknown
	}
	endpoint := *target
	endpoint.Path = nodeOperatorPathPrefix + strconv.FormatUint(uint64(partition)+1, 10) + "/backup-barrier"
	var raw [16]byte
	binary.BigEndian.PutUint64(raw[0:8], epoch)
	binary.BigEndian.PutUint64(raw[8:16], backupID)
	requestCtx, cancel := context.WithTimeout(ctx, peerRequestTimeout)
	defer cancel()
	request, _ := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint.String(), bytes.NewReader(raw[:]))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.ContentLength = int64(len(raw))
	response, err := membership.node.peerClient.do(leaderID, request)
	if err != nil {
		return ErrBackupIncomplete
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	if response.StatusCode != http.StatusNoContent {
		return ErrBackupIncomplete
	}
	return nil
}

func (membership *nodePartitionMembership) fetchBackupSnapshot(ctx context.Context, partition uint16, binding CoreSnapshotBinding, backupID uint64, chunkBytes int, destination string) (CoreSnapshotManifest, error) {
	if partition >= PartitionCount || binding.Partition != partition || binding.SourceMemberID == 0 || backupID == 0 || chunkBytes < MinCoreSnapshotChunkBytes || chunkBytes > MaxCoreSnapshotChunkBytes {
		return CoreSnapshotManifest{}, ErrBackupIncomplete
	}
	target := membership.node.peerClient.targets[binding.SourceMemberID]
	if target == nil || membership.node.snapshotStore == nil {
		return CoreSnapshotManifest{}, ErrBackupIncomplete
	}
	endpoint := *target
	endpoint.Path = nodeOperatorPathPrefix + strconv.FormatUint(uint64(partition)+1, 10) + "/backup-snapshot"
	var raw [28]byte
	binary.BigEndian.PutUint64(raw[0:8], binding.OwnershipEpoch)
	binary.BigEndian.PutUint64(raw[8:16], binding.AppliedIndex)
	binary.BigEndian.PutUint32(raw[16:20], uint32(chunkBytes))
	binary.BigEndian.PutUint64(raw[20:28], backupID)
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(raw[:]))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.ContentLength = int64(len(raw))
	response, err := membership.node.peerClient.do(binding.SourceMemberID, request)
	if err != nil {
		return CoreSnapshotManifest{}, ErrBackupIncomplete
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != streamSnapshotContentType {
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
		return CoreSnapshotManifest{}, ErrBackupIncomplete
	}
	ref, err := membership.node.snapshotStore.receive(ctx, response.Body, streamSnapshotReceiveExpectation{
		ClusterID:      binding.ClusterID,
		GroupID:        binding.GroupID,
		Partition:      binding.Partition,
		SourceMemberID: binding.SourceMemberID,
		AppliedIndex:   binding.AppliedIndex,
		OwnershipEpoch: binding.OwnershipEpoch,
	})
	if err != nil || ref.ClusterID != binding.ClusterID || ref.GroupID != binding.GroupID || ref.Partition != binding.Partition || ref.OwnershipEpoch != binding.OwnershipEpoch || ref.AppliedIndex != binding.AppliedIndex {
		return CoreSnapshotManifest{}, ErrBackupIncomplete
	}
	source, err := membership.node.snapshotStore.artifact(ref, true)
	if err != nil {
		return CoreSnapshotManifest{}, ErrBackupIncomplete
	}
	manifest, err := copyVerifiedSnapshotArtifact(source, destination)
	if err == nil {
		membership.node.snapshotStore.prune(ref, true)
	}
	return manifest, err
}

func (membership *nodePartitionMembership) mutate(ctx context.Context, partition uint16, action string, targetID, epoch uint64) error {
	leaderID, _, err := membership.leader(ctx, partition, epoch)
	if err != nil {
		return err
	}
	if _, controlApplied, fatal := membership.node.control.Status(); fatal == nil && controlApplied > 0 {
		if err := membership.waitControlApplied(ctx, leaderID, controlApplied); err != nil {
			return err
		}
	}
	target := membership.node.peerClient.targets[leaderID]
	endpoint := *target
	endpoint.Path = nodeOperatorPathPrefix + strconv.FormatUint(uint64(partition)+1, 10) + "/" + action + "/" + strconv.FormatUint(targetID, 10)
	var raw [8]byte
	binary.BigEndian.PutUint64(raw[:], epoch)
	requestCtx, cancel := context.WithTimeout(ctx, peerRequestTimeout)
	defer cancel()
	request, _ := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint.String(), bytes.NewReader(raw[:]))
	request.Header.Set("Content-Type", "application/octet-stream")
	request.ContentLength = 8
	response, err := membership.node.peerClient.do(leaderID, request)
	if err != nil {
		return ErrMemberStopped
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	if response.StatusCode != http.StatusNoContent {
		return ErrMemberStopped
	}
	return nil
}

func (membership *nodePartitionMembership) InstallLearnerSnapshot(ctx context.Context, partition uint16, to, epoch uint64) error {
	return membership.mutate(ctx, partition, "add-learner", to, epoch)
}
func (membership *nodePartitionMembership) WaitLearnerCaughtUp(ctx context.Context, partition uint16, to, epoch uint64) (uint64, error) {
	_, leader, err := membership.leader(ctx, partition, epoch)
	if err != nil {
		return 0, err
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		target, err := membership.status(ctx, partition, to)
		if err == nil && target.applied >= leader.applied {
			return target.applied, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}
func (membership *nodePartitionMembership) EnterJoint(ctx context.Context, partition uint16, _, to, epoch uint64) error {
	return membership.mutate(ctx, partition, "enter-joint", to, epoch)
}
func (membership *nodePartitionMembership) MoveLeadershipOff(ctx context.Context, partition uint16, from, to, epoch uint64) error {
	leader, _, err := membership.leader(ctx, partition, epoch)
	if err != nil || leader != from {
		return err
	}
	return membership.mutate(ctx, partition, "transfer", to, epoch)
}
func (membership *nodePartitionMembership) LeaveJoint(ctx context.Context, partition uint16, epoch uint64) error {
	return membership.mutate(ctx, partition, "leave-joint", 0, epoch)
}
func (membership *nodePartitionMembership) RemoveReplica(ctx context.Context, partition uint16, from, epoch uint64) error {
	return membership.mutate(ctx, partition, "remove", from, epoch)
}
