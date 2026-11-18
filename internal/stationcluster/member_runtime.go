package stationcluster

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	defaultTickInterval          = 100 * time.Millisecond
	peerQueueDepth               = 256
	proposalQueueDepth           = 64
	maxPendingProposals          = 64
	maxPendingReads              = 64
	raftSnapshotIntervalEntries  = 32
	raftSnapshotChunkBytes       = 64 << 10
	maxRaftSnapshotArtifactBytes = 768 << 10
	maxRaftSnapshotDataBytes     = maxSizePerMessage - 128<<10
)

var (
	ErrNotLeader        = errors.New("cluster member is not leader")
	ErrMemberStopped    = errors.New("cluster member stopped")
	ErrPeerBackpressure = errors.New("cluster peer queue is full")
)

// PeerTransport is the only network capability available to the consensus
// loop. Implementations must authenticate peers and bound message bytes.
type PeerTransport interface {
	Send(context.Context, *raftpb.Message) error
}

type RaftMemberConfig struct {
	ClusterID        string
	GroupID          uint64
	ID               uint64
	Partition        uint16
	OwnershipEpoch   uint64
	PeerIDs          []uint64
	BootstrapPeerIDs []uint64
	RaftRoot         string
	Core             *stationcore.Core
	Transport        PeerTransport
	TickInterval     time.Duration
	// Join starts a fresh unbootstrapped RawNode that can only become active
	// after an existing leader commits ConfChangeAddLearnerNode for its ID.
	Join          bool
	SnapshotStore *streamSnapshotStore
}

type proposalRequest struct {
	id     uint64
	ctx    context.Context
	data   []byte
	digest [32]byte
	result chan proposalResult
}

type cancelRequest struct {
	id  uint64
	ack chan struct{}
}

type readRequest struct {
	id     uint64
	ctx    context.Context
	token  [8]byte
	result chan error
}

type raftMembershipAction uint8

const (
	raftAddLearner raftMembershipAction = iota + 1
	raftEnterJoint
	raftLeaveJoint
	raftRemoveMember
)

type raftMembershipRequest struct {
	id       uint64
	ctx      context.Context
	action   raftMembershipAction
	memberID uint64
	result   chan error
}

type raftMembershipContext struct {
	RequestID uint64               `json:"requestId"`
	Action    raftMembershipAction `json:"action"`
	MemberID  uint64               `json:"memberId,omitempty"`
}

type leadershipRequest struct {
	ctx        context.Context
	fromID     uint64
	transferee uint64
	result     chan error
}

type pendingProposal struct {
	digest [32]byte
	result chan proposalResult
}

type pendingRead struct {
	result chan error
	index  uint64
}

type proposalResult struct {
	result stationcore.CommandResult
	err    error
}

type appliedResult struct {
	digest [32]byte
	result stationcore.CommandResult
	err    error
}

// RaftMember is one durable, bounded etcd/raft state-machine member. One
// goroutine exclusively owns RawNode. Ready processing persists HardState,
// Entries, and Snapshot before peer sends; committed commands are then applied
// to the native Core before the applied index is synchronized.
type RaftMember struct {
	id            uint64
	clusterID     string
	groupID       uint64
	core          *stationcore.Core
	transport     PeerTransport
	log           *raftLog
	node          *raft.RawNode
	storage       *raft.MemoryStorage
	partition     uint16
	epoch         uint64
	peerIDs       map[uint64]struct{}
	snapshotStore *streamSnapshotStore
	snapshotIndex uint64

	proposals       chan proposalRequest
	proposalCancels chan cancelRequest
	reads           chan readRequest
	memberships     chan raftMembershipRequest
	leadership      chan leadershipRequest
	readCancels     chan cancelRequest
	campaigns       chan chan error
	incoming        chan *raftpb.Message
	stop            chan struct{}
	done            chan struct{}
	closeOnce       sync.Once
	requestID       atomic.Uint64

	mu             sync.RWMutex
	lead           uint64
	applied        uint64
	fatal          error
	confState      *raftpb.ConfState
	bootstrapPeers []uint64
	joining        bool
}

func OpenRaftMember(config RaftMemberConfig) (*RaftMember, error) {
	if config.ID == 0 || config.ClusterID == "" || config.GroupID == 0 || config.Partition >= PartitionCount || config.OwnershipEpoch == 0 || config.Core == nil || config.Transport == nil || len(config.PeerIDs) < MinMembers || len(config.PeerIDs) > MaxMembers || !filepath.IsAbs(config.RaftRoot) {
		return nil, ErrInvalidTopology
	}
	seen := make(map[uint64]struct{}, len(config.PeerIDs))
	bootstrapPeerIDs := config.BootstrapPeerIDs
	if len(bootstrapPeerIDs) == 0 {
		bootstrapPeerIDs = config.PeerIDs
	}
	peers := make([]raft.Peer, len(bootstrapPeerIDs))
	memberPresent := false
	for _, id := range config.PeerIDs {
		if id == 0 {
			return nil, ErrInvalidTopology
		}
		if _, exists := seen[id]; exists {
			return nil, ErrInvalidTopology
		}
		seen[id] = struct{}{}
		memberPresent = memberPresent || id == config.ID
	}
	for index, id := range bootstrapPeerIDs {
		peers[index] = raft.Peer{ID: id}
	}
	if !memberPresent {
		return nil, ErrInvalidTopology
	}
	if !config.Join {
		if len(bootstrapPeerIDs) < MinMembers || len(bootstrapPeerIDs) > MaxMembers || !slices.Contains(bootstrapPeerIDs, config.ID) {
			return nil, ErrInvalidTopology
		}
		for _, id := range bootstrapPeerIDs {
			if _, ok := seen[id]; !ok {
				return nil, ErrInvalidTopology
			}
		}
	}
	tick := config.TickInterval
	if tick == 0 {
		tick = defaultTickInterval
	}
	if tick < 10*time.Millisecond || tick > time.Second {
		return nil, ErrInvalidTopology
	}
	log, recovered, err := openRaftLog(config.RaftRoot, raftLogBinding{
		clusterID: config.ClusterID, memberID: config.ID, groupID: config.GroupID,
		partition: config.Partition, ownershipEpoch: config.OwnershipEpoch,
	})
	if err != nil {
		return nil, err
	}
	if recovered.snapshot != nil {
		if err := installRaftCoreSnapshot(config.Core, recovered.snapshot, config, seen, 0); err != nil {
			_ = log.close()
			return nil, err
		}
	}
	node, err := raft.NewRawNode(&raft.Config{
		ID: config.ID, ElectionTick: 10, HeartbeatTick: 1,
		Storage: recovered.storage, Applied: recovered.applied,
		MaxSizePerMsg: maxSizePerMessage, MaxCommittedSizePerReady: maxSizePerMessage,
		MaxUncommittedEntriesSize: maxUncommittedEntriesBytes,
		MaxInflightMsgs:           maxInflightMessages, MaxInflightBytes: maxInflightBytes,
		CheckQuorum: true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe,
		DisableProposalForwarding: true, StepDownOnRemoval: true,
		Logger: consensusLogger{},
	})
	if err != nil {
		_ = log.close()
		return nil, err
	}
	if recovered.fresh && !config.Join {
		if err := node.Bootstrap(peers); err != nil {
			_ = log.close()
			return nil, err
		}
	}
	bootstrapCopy := make([]uint64, 0, len(peers))
	for _, peer := range peers {
		bootstrapCopy = append(bootstrapCopy, peer.ID)
	}
	member := &RaftMember{
		id: config.ID, clusterID: config.ClusterID, groupID: config.GroupID, partition: config.Partition,
		epoch: config.OwnershipEpoch, peerIDs: seen, core: config.Core, transport: config.Transport, snapshotStore: config.SnapshotStore,
		log: log, node: node, storage: recovered.storage,
		proposals:       make(chan proposalRequest, proposalQueueDepth),
		proposalCancels: make(chan cancelRequest, maxPendingProposals),
		reads:           make(chan readRequest, maxPendingReads),
		memberships:     make(chan raftMembershipRequest, maxPendingProposals),
		leadership:      make(chan leadershipRequest, 1),
		readCancels:     make(chan cancelRequest, maxPendingReads),
		campaigns:       make(chan chan error, 1),
		incoming:        make(chan *raftpb.Message, peerQueueDepth),
		stop:            make(chan struct{}), done: make(chan struct{}), applied: recovered.applied,
		bootstrapPeers: bootstrapCopy,
		joining:        config.Join,
	}
	if recovered.confState != nil {
		member.confState = proto.Clone(recovered.confState).(*raftpb.ConfState)
	}
	if recovered.snapshot != nil {
		member.snapshotIndex = recovered.snapshot.GetMetadata().GetIndex()
	}
	go member.run(tick, recovered.confState)
	return member, nil
}

func (member *RaftMember) ID() uint64        { return member.id }
func (member *RaftMember) ClusterID() string { return member.clusterID }
func (member *RaftMember) GroupID() uint64   { return member.groupID }

func (member *RaftMember) ConfState() *raftpb.ConfState {
	member.mu.RLock()
	defer member.mu.RUnlock()
	if member.confState == nil {
		return nil
	}
	return proto.Clone(member.confState).(*raftpb.ConfState)
}

func (member *RaftMember) setConfState(confState *raftpb.ConfState) {
	if confState == nil {
		return
	}
	cloned := proto.Clone(confState).(*raftpb.ConfState)
	member.mu.Lock()
	member.confState = cloned
	member.mu.Unlock()
}

func (member *RaftMember) Status() (lead, applied uint64, fatal error) {
	member.mu.RLock()
	defer member.mu.RUnlock()
	return member.lead, member.applied, member.fatal
}

func (member *RaftMember) Campaign(ctx context.Context) error {
	result := make(chan error, 1)
	select {
	case member.campaigns <- result:
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
}

func (member *RaftMember) Step(ctx context.Context, message *raftpb.Message) error {
	if message == nil || message.GetTo() != member.id {
		return ErrInvalidTopology
	}
	if proto.Size(message) > maxPeerMessageBytes {
		return ErrProposalTooLarge
	}
	copyMessage := proto.Clone(message).(*raftpb.Message)
	select {
	case member.incoming <- copyMessage:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	default:
		return ErrPeerBackpressure
	}
}

func (member *RaftMember) Propose(ctx context.Context, command stationcore.Command) (stationcore.CommandResult, error) {
	if err := command.Validate(); err != nil {
		return stationcore.CommandResult{}, err
	}
	raw, err := json.Marshal(command)
	if err != nil || len(raw) > maxProposalBytes {
		return stationcore.CommandResult{}, ErrProposalTooLarge
	}
	id := member.requestID.Add(1)
	request := proposalRequest{id: id, ctx: ctx, data: raw, digest: sha256.Sum256(raw), result: make(chan proposalResult, 1)}
	select {
	case member.proposals <- request:
	case <-ctx.Done():
		return stationcore.CommandResult{}, ctx.Err()
	case <-member.done:
		return stationcore.CommandResult{}, ErrMemberStopped
	}
	select {
	case result := <-request.result:
		return result.result, result.err
	case <-ctx.Done():
		member.cancelProposal(id)
		return stationcore.CommandResult{}, ctx.Err()
	case <-member.done:
		return stationcore.CommandResult{}, ErrMemberStopped
	}
}

func (member *RaftMember) ReadBarrier(ctx context.Context) error {
	id := member.requestID.Add(1)
	request := readRequest{id: id, ctx: ctx, result: make(chan error, 1)}
	binary.BigEndian.PutUint64(request.token[:], id)
	select {
	case member.reads <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		member.cancelRead(id)
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
}

func (member *RaftMember) AddLearner(ctx context.Context, memberID uint64) error {
	return member.proposeMembership(ctx, raftAddLearner, memberID)
}

func (member *RaftMember) EnterJoint(ctx context.Context, memberID uint64) error {
	return member.proposeMembership(ctx, raftEnterJoint, memberID)
}

func (member *RaftMember) LeaveJoint(ctx context.Context) error {
	return member.proposeMembership(ctx, raftLeaveJoint, 0)
}

func (member *RaftMember) RemoveMember(ctx context.Context, memberID uint64) error {
	return member.proposeMembership(ctx, raftRemoveMember, memberID)
}

func (member *RaftMember) proposeMembership(ctx context.Context, action raftMembershipAction, memberID uint64) error {
	if (action != raftLeaveJoint && memberID == 0) || action < raftAddLearner || action > raftRemoveMember {
		return ErrInvalidTopology
	}
	id := member.requestID.Add(1)
	request := raftMembershipRequest{id: id, ctx: ctx, action: action, memberID: memberID, result: make(chan error, 1)}
	select {
	case member.memberships <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
	select {
	case err := <-request.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
}

func (member *RaftMember) TransferLeadership(ctx context.Context, fromID, transferee uint64) error {
	if fromID == 0 || transferee == 0 || fromID == transferee {
		return ErrInvalidTopology
	}
	request := leadershipRequest{ctx: ctx, fromID: fromID, transferee: transferee, result: make(chan error, 1)}
	select {
	case member.leadership <- request:
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
	select {
	case err := <-request.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		lead, _, fatal := member.Status()
		if fatal != nil {
			return fatal
		}
		if lead != 0 && lead != fromID {
			return nil
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-member.done:
			return ErrMemberStopped
		}
	}
}

func (member *RaftMember) cancelProposal(id uint64) { member.cancel(member.proposalCancels, id) }
func (member *RaftMember) cancelRead(id uint64)     { member.cancel(member.readCancels, id) }

func (member *RaftMember) cancel(queue chan cancelRequest, id uint64) {
	ack := make(chan struct{})
	select {
	case queue <- cancelRequest{id: id, ack: ack}:
	case <-member.done:
		return
	}
	select {
	case <-ack:
	case <-member.done:
	}
}

func (member *RaftMember) Close() error {
	member.closeOnce.Do(func() { close(member.stop) })
	<-member.done
	return member.log.close()
}

func (member *RaftMember) run(tickInterval time.Duration, confState *raftpb.ConfState) {
	defer close(member.done)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	pending := make(map[uint64]pendingProposal, maxPendingProposals)
	byDigest := make(map[[32]byte][]uint64, maxPendingProposals)
	pendingReads := make(map[uint64]pendingRead, maxPendingReads)
	pendingMemberships := make(map[uint64]chan error, maxPendingProposals)
	for {
		select {
		case <-member.stop:
			member.failPending(pending, pendingReads, ErrMemberStopped)
			failMemberships(pendingMemberships, ErrMemberStopped)
			return
		case <-ticker.C:
			member.node.Tick()
		case result := <-member.campaigns:
			result <- member.node.Campaign()
		case message := <-member.incoming:
			if !isAllowedRaftNetworkMessage(message.GetType()) {
				break
			}
			if err := member.node.Step(message); err != nil && !isIgnorableRaftStepError(err) {
				member.stopWithError(pending, pendingReads, err)
				return
			}
		case cancellation := <-member.proposalCancels:
			member.removePendingProposal(pending, byDigest, cancellation.id)
			close(cancellation.ack)
		case cancellation := <-member.readCancels:
			delete(pendingReads, cancellation.id)
			close(cancellation.ack)
		case request := <-member.proposals:
			if request.ctx.Err() != nil {
				request.result <- proposalResult{err: request.ctx.Err()}
				break
			}
			status := member.node.BasicStatus()
			if status.Lead != member.id || status.RaftState != raft.StateLeader {
				request.result <- proposalResult{err: ErrNotLeader}
				break
			}
			if len(pending) >= maxPendingProposals {
				request.result <- proposalResult{err: ErrPeerBackpressure}
				break
			}
			pending[request.id] = pendingProposal{digest: request.digest, result: request.result}
			byDigest[request.digest] = append(byDigest[request.digest], request.id)
			if err := member.node.Propose(request.data); err != nil {
				member.completePendingProposal(pending, byDigest, request.id, proposalResult{err: err})
			}
		case request := <-member.reads:
			if request.ctx.Err() != nil {
				request.result <- request.ctx.Err()
				break
			}
			status := member.node.BasicStatus()
			if status.Lead != member.id || status.RaftState != raft.StateLeader {
				request.result <- ErrNotLeader
				break
			}
			if len(pendingReads) >= maxPendingReads {
				request.result <- ErrPeerBackpressure
				break
			}
			pendingReads[request.id] = pendingRead{result: request.result}
			member.node.ReadIndex(request.token[:])
		case request := <-member.memberships:
			if request.ctx.Err() != nil {
				request.result <- request.ctx.Err()
				break
			}
			status := member.node.BasicStatus()
			if status.Lead != member.id || status.RaftState != raft.StateLeader {
				request.result <- ErrNotLeader
				break
			}
			if len(pendingMemberships) >= maxPendingProposals {
				request.result <- ErrPeerBackpressure
				break
			}
			raw, err := json.Marshal(raftMembershipContext{RequestID: request.id, Action: request.action, MemberID: request.memberID})
			if err != nil || len(raw) > 1024 {
				request.result <- ErrInvalidTopology
				break
			}
			pendingMemberships[request.id] = request.result
			if err := proposeRaftMembership(member.node, request.action, request.memberID, raw); err != nil {
				delete(pendingMemberships, request.id)
				request.result <- err
			}
		case request := <-member.leadership:
			if request.ctx.Err() != nil {
				request.result <- request.ctx.Err()
				break
			}
			status := member.node.BasicStatus()
			if status.Lead != request.fromID || status.Lead != member.id || status.RaftState != raft.StateLeader {
				request.result <- ErrNotLeader
				break
			}
			member.node.TransferLeader(request.transferee)
			request.result <- nil
		}
		if err := member.processReady(&confState, pending, byDigest, pendingReads, pendingMemberships); err != nil {
			member.stopWithError(pending, pendingReads, err)
			failMemberships(pendingMemberships, err)
			return
		}
	}
}

func (member *RaftMember) processReady(confState **raftpb.ConfState, pending map[uint64]pendingProposal, byDigest map[[32]byte][]uint64, pendingReads map[uint64]pendingRead, pendingMemberships map[uint64]chan error) error {
	if confState == nil || *confState == nil {
		return ErrInvalidTopology
	}
	for member.node.HasReady() {
		ready := member.node.Ready()
		if ready.Snapshot != nil && !raft.IsEmptySnap(ready.Snapshot) {
			if err := member.installReadySnapshot(ready.Snapshot); err != nil {
				return err
			}
		}
		if err := member.log.persist(ready); err != nil {
			return err
		}
		if ready.Snapshot != nil && !raft.IsEmptySnap(ready.Snapshot) && member.snapshotStore != nil {
			if reference, streamed, err := decodeStreamSnapshotReference(ready.Snapshot.Data); streamed && err == nil {
				member.snapshotStore.prune(reference, true)
			}
		}
		if ready.Snapshot != nil && !raft.IsEmptySnap(ready.Snapshot) {
			if err := member.storage.ApplySnapshot(ready.Snapshot); err != nil {
				return err
			}
			if snapshotConf := ready.Snapshot.GetMetadata().GetConfState(); snapshotConf != nil {
				*confState = proto.Clone(snapshotConf).(*raftpb.ConfState)
				member.setConfState(*confState)
			}
			member.setApplied(ready.Snapshot.GetMetadata().GetIndex())
			member.snapshotIndex = ready.Snapshot.GetMetadata().GetIndex()
		}
		if len(ready.Entries) > 0 {
			if err := member.storage.Append(ready.Entries); err != nil {
				return err
			}
		}
		if ready.HardState != nil && !raft.IsEmptyHardState(ready.HardState) {
			if err := member.storage.SetHardState(ready.HardState); err != nil {
				return err
			}
		}
		for _, message := range ready.Messages {
			err := member.transport.Send(context.Background(), message)
			if message.GetType() == raftpb.MessageType_MsgSnap {
				status := raft.SnapshotFinish
				if err != nil {
					status = raft.SnapshotFailure
				}
				member.node.ReportSnapshot(message.GetTo(), status)
			}
		}
		applied := make([]appliedResult, 0, len(ready.CommittedEntries))
		lastApplied := member.currentApplied()
		var lastAppliedTerm uint64
		configurationChanged := false
		for _, entry := range ready.CommittedEntries {
			if entry.GetIndex() <= lastApplied {
				continue
			}
			switch entry.GetType() {
			case raftpb.EntryType_EntryNormal:
				if len(entry.Data) > 0 {
					var command stationcore.Command
					if err := json.Unmarshal(entry.Data, &command); err != nil {
						return fmt.Errorf("decode committed command: %w", err)
					}
					result, err := member.core.ApplyCommand(context.Background(), command)
					applied = append(applied, appliedResult{digest: sha256.Sum256(entry.Data), result: result, err: err})
				}
			case raftpb.EntryType_EntryConfChange:
				var change raftpb.ConfChange
				if err := proto.Unmarshal(entry.Data, &change); err != nil {
					return err
				}
				if next := member.node.ApplyConfChange(&change); next != nil {
					*confState = proto.Clone(next).(*raftpb.ConfState)
					member.setConfState(*confState)
				}
				configurationChanged = true
				completeRaftMembership(change.Context, pendingMemberships)
			case raftpb.EntryType_EntryConfChangeV2:
				var change raftpb.ConfChangeV2
				if err := proto.Unmarshal(entry.Data, &change); err != nil {
					return err
				}
				if next := member.node.ApplyConfChange(&change); next != nil {
					*confState = proto.Clone(next).(*raftpb.ConfState)
					member.setConfState(*confState)
				}
				configurationChanged = true
				completeRaftMembership(change.Context, pendingMemberships)
			}
			lastApplied = entry.GetIndex()
			lastAppliedTerm = entry.GetTerm()
		}
		if lastApplied > member.currentApplied() {
			if err := member.log.markApplied(lastApplied, lastAppliedTerm, *confState); err != nil {
				return err
			}
			member.setApplied(lastApplied)
			if configurationChanged {
				// A learner must never receive a pre-membership snapshot whose
				// ConfState omits it. Force one snapshot at each committed
				// configuration boundary before the old compacted snapshot can
				// be selected for catch-up.
				member.snapshotIndex = 0
			}
			if err := member.maybeCreateSnapshot(lastApplied, lastAppliedTerm, *confState); err != nil {
				return err
			}
			for _, result := range applied {
				member.completeCommittedProposal(pending, byDigest, result.digest, proposalResult{result: result.result, err: result.err})
			}
		}
		for _, readState := range ready.ReadStates {
			if len(readState.RequestCtx) != 8 {
				continue
			}
			id := binary.BigEndian.Uint64(readState.RequestCtx)
			if current, ok := pendingReads[id]; ok {
				current.index = readState.Index
				pendingReads[id] = current
			}
		}
		member.completeAppliedReads(pendingReads)
		member.node.Advance(ready)
		status := member.node.BasicStatus()
		member.mu.Lock()
		member.lead = status.Lead
		member.mu.Unlock()
	}
	return nil
}

func (member *RaftMember) installReadySnapshot(snapshot *raftpb.Snapshot) error {
	config := RaftMemberConfig{
		ClusterID: member.clusterID, GroupID: member.groupID, ID: member.id,
		Partition: member.partition, OwnershipEpoch: member.epoch, SnapshotStore: member.snapshotStore,
	}
	return installRaftCoreSnapshot(member.core, snapshot, config, member.peerIDs, member.currentApplied())
}

func installRaftCoreSnapshot(core *stationcore.Core, snapshot *raftpb.Snapshot, config RaftMemberConfig, peers map[uint64]struct{}, currentApplied uint64) error {
	if core == nil || snapshot == nil || raft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 || snapshot.GetMetadata().GetIndex() <= currentApplied {
		return ErrInvalidSnapshot
	}
	if reference, streamed, referenceErr := decodeStreamSnapshotReference(snapshot.Data); streamed {
		if referenceErr != nil || config.SnapshotStore == nil || reference.ClusterID != config.ClusterID || reference.GroupID != config.GroupID || reference.Partition != config.Partition || reference.OwnershipEpoch != config.OwnershipEpoch || reference.AppliedIndex != snapshot.GetMetadata().GetIndex() {
			return ErrInvalidSnapshot
		}
		if _, allowed := peers[reference.SourceMemberID]; !allowed {
			return ErrInvalidSnapshot
		}
		artifactRoot, err := config.SnapshotStore.artifact(reference, true)
		if err != nil {
			return err
		}
		_, err = installCoreSnapshot(core, artifactRoot, CoreSnapshotExpectation{ClusterID: config.ClusterID, GroupID: config.GroupID, SourceMemberID: reference.SourceMemberID, Partition: config.Partition, OwnershipEpoch: config.OwnershipEpoch, CurrentAppliedIndex: currentApplied})
		return err
	}
	artifactRoot, err := os.MkdirTemp("", "badtower-raft-snapshot-")
	if err != nil {
		return err
	}
	if err := os.Remove(artifactRoot); err != nil {
		return err
	}
	defer os.RemoveAll(artifactRoot)
	manifest, err := unmarshalCoreSnapshotArtifact(snapshot.Data, artifactRoot, maxRaftSnapshotDataBytes)
	if err != nil || manifest.AppliedIndex != snapshot.GetMetadata().GetIndex() {
		return ErrInvalidSnapshot
	}
	if _, allowed := peers[manifest.SourceMemberID]; !allowed {
		return ErrInvalidSnapshot
	}
	_, err = installCoreSnapshot(core, artifactRoot, CoreSnapshotExpectation{
		ClusterID: config.ClusterID, GroupID: config.GroupID, SourceMemberID: manifest.SourceMemberID,
		Partition: config.Partition, OwnershipEpoch: config.OwnershipEpoch, CurrentAppliedIndex: currentApplied,
	})
	return err
}

func (member *RaftMember) maybeCreateSnapshot(applied, term uint64, confState *raftpb.ConfState) error {
	if applied <= member.snapshotIndex || applied-member.snapshotIndex < raftSnapshotIntervalEntries {
		return nil
	}
	if term == 0 || confState == nil {
		return ErrInvalidSnapshot
	}
	artifactRoot, err := os.MkdirTemp("", "badtower-raft-snapshot-create-")
	if err != nil {
		return err
	}
	if err := os.Remove(artifactRoot); err != nil {
		return err
	}
	maxArtifactBytes := uint64(maxRaftSnapshotArtifactBytes)
	chunkBytes := raftSnapshotChunkBytes
	streamCandidate := member.snapshotStore != nil
	if streamCandidate {
		artifactRoot, err = os.MkdirTemp(filepath.Join(member.snapshotStore.root, "source"), ".building-")
		if err != nil {
			return err
		}
		if err = os.Remove(artifactRoot); err != nil {
			return err
		}
		maxArtifactBytes = MaxCoreSnapshotBytes
		chunkBytes = MaxCoreSnapshotChunkBytes
	}
	cleanupArtifact := true
	defer func() {
		if cleanupArtifact {
			_ = os.RemoveAll(artifactRoot)
		}
	}()
	_, err = createCoreSnapshot(member.core, artifactRoot, CoreSnapshotBinding{
		ClusterID: member.clusterID, GroupID: member.groupID, SourceMemberID: member.id,
		Partition: member.partition, OwnershipEpoch: member.epoch, AppliedIndex: applied,
	}, chunkBytes, maxArtifactBytes)
	if err != nil {
		return err
	}
	manifest, err := readCoreSnapshotManifest(artifactRoot)
	if err != nil || manifest.AppliedIndex != applied {
		return ErrInvalidSnapshot
	}
	var data []byte
	var streamReference *streamSnapshotReference
	if streamCandidate && manifest.TotalBytes > maxRaftSnapshotArtifactBytes {
		reference := referenceForManifest(manifest)
		streamReference = &reference
		finalRoot := member.snapshotStore.sourceRoot(reference)
		if _, existingErr := member.snapshotStore.artifact(reference, false); existingErr == nil {
			_ = os.RemoveAll(artifactRoot)
			artifactRoot = finalRoot
		} else {
			if requireAbsentAbsoluteRoot(finalRoot) != nil {
				return ErrInvalidSnapshot
			}
			if err = os.Rename(artifactRoot, finalRoot); err != nil {
				return err
			}
			if syncSnapshotDir(filepath.Dir(finalRoot)) != nil {
				return ErrInvalidSnapshot
			}
			artifactRoot = finalRoot
		}
		data, err = encodeStreamSnapshotReference(reference)
		cleanupArtifact = false
	} else {
		data, manifest, err = marshalCoreSnapshotArtifact(artifactRoot, maxRaftSnapshotDataBytes)
	}
	if err != nil || manifest.AppliedIndex != applied {
		return ErrInvalidSnapshot
	}
	snapshot, err := member.storage.CreateSnapshot(applied, confState, data)
	if err != nil {
		return err
	}
	if snapshot.GetMetadata().GetTerm() != term {
		return ErrInvalidSnapshot
	}
	if err := member.log.persistLocalSnapshot(snapshot); err != nil {
		return err
	}
	if streamReference != nil {
		member.snapshotStore.prune(*streamReference, false)
	}
	if err := member.storage.Compact(applied); err != nil && !errors.Is(err, raft.ErrCompacted) {
		return err
	}
	member.snapshotIndex = applied
	return nil
}

func (member *RaftMember) currentApplied() uint64 {
	member.mu.RLock()
	defer member.mu.RUnlock()
	return member.applied
}

func (member *RaftMember) setApplied(index uint64) {
	member.mu.Lock()
	member.applied = index
	member.mu.Unlock()
}

func (member *RaftMember) completeCommittedProposal(pending map[uint64]pendingProposal, byDigest map[[32]byte][]uint64, digest [32]byte, result proposalResult) {
	ids := byDigest[digest]
	if len(ids) == 0 {
		return
	}
	id := ids[0]
	member.completePendingProposal(pending, byDigest, id, result)
}

func (member *RaftMember) completePendingProposal(pending map[uint64]pendingProposal, byDigest map[[32]byte][]uint64, id uint64, result proposalResult) {
	current, ok := pending[id]
	if !ok {
		return
	}
	current.result <- result
	member.removePendingProposal(pending, byDigest, id)

}

func (member *RaftMember) removePendingProposal(pending map[uint64]pendingProposal, byDigest map[[32]byte][]uint64, id uint64) {
	current, ok := pending[id]
	if !ok {
		return
	}
	delete(pending, id)
	ids := byDigest[current.digest]
	for index, candidate := range ids {
		if candidate == id {
			ids = append(ids[:index], ids[index+1:]...)
			break
		}
	}
	if len(ids) == 0 {
		delete(byDigest, current.digest)
	} else {
		byDigest[current.digest] = ids
	}
}

func (member *RaftMember) completeAppliedReads(pending map[uint64]pendingRead) {
	applied := member.currentApplied()
	for id, read := range pending {
		if read.index != 0 && applied >= read.index {
			read.result <- nil
			delete(pending, id)
		}
	}
}

func (member *RaftMember) failPending(proposals map[uint64]pendingProposal, reads map[uint64]pendingRead, err error) {
	for id, proposal := range proposals {
		proposal.result <- proposalResult{err: err}
		delete(proposals, id)
	}
	for id, read := range reads {
		read.result <- err
		delete(reads, id)
	}
}

func (member *RaftMember) stopWithError(pending map[uint64]pendingProposal, reads map[uint64]pendingRead, err error) {
	member.mu.Lock()
	member.fatal = err
	member.mu.Unlock()
	member.failPending(pending, reads, err)
}

func proposeRaftMembership(node *raft.RawNode, action raftMembershipAction, memberID uint64, raw []byte) error {
	switch action {
	case raftAddLearner:
		changeType := raftpb.ConfChangeType_ConfChangeAddLearnerNode
		return node.ProposeConfChange(&raftpb.ConfChange{Type: &changeType, NodeId: &memberID, Context: raw})
	case raftEnterJoint:
		transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointExplicit
		changeType := raftpb.ConfChangeType_ConfChangeAddNode
		return node.ProposeConfChange(&raftpb.ConfChangeV2{Transition: &transition, Changes: []*raftpb.ConfChangeSingle{{Type: &changeType, NodeId: &memberID}}, Context: raw})
	case raftLeaveJoint:
		return node.ProposeConfChange(&raftpb.ConfChangeV2{Context: raw})
	case raftRemoveMember:
		changeType := raftpb.ConfChangeType_ConfChangeRemoveNode
		return node.ProposeConfChange(&raftpb.ConfChange{Type: &changeType, NodeId: &memberID, Context: raw})
	default:
		return ErrInvalidTopology
	}
}

func completeRaftMembership(raw []byte, pending map[uint64]chan error) {
	if len(raw) == 0 || len(raw) > 1024 {
		return
	}
	var value raftMembershipContext
	if json.Unmarshal(raw, &value) != nil || value.RequestID == 0 {
		return
	}
	if result, ok := pending[value.RequestID]; ok {
		result <- nil
		delete(pending, value.RequestID)
	}
}

func failMemberships(pending map[uint64]chan error, err error) {
	for id, result := range pending {
		result <- err
		delete(pending, id)
	}
}
