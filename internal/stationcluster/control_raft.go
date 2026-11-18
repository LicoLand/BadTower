package stationcluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	maxControlEntryBytes = maxControlCommandBytes + 1024
	controlQueueDepth    = 64
	maxControlPending    = 64
)

type ControlMembershipAction string

const (
	ControlRaftAddLearner   ControlMembershipAction = "add-learner"
	ControlRaftEnterJoint   ControlMembershipAction = "enter-joint"
	ControlRaftLeaveJoint   ControlMembershipAction = "leave-joint"
	ControlRaftRemoveMember ControlMembershipAction = "remove-member"
)

type ControlRaftConfig struct {
	ClusterID      string
	GroupID        uint64
	ID             uint64
	InitialPeerIDs []uint64
	Bootstrap      bool
	RaftRoot       string
	Machine        *ControlMachine
	Transport      PeerTransport
	TickInterval   time.Duration
}

// ControlRaftNode is the minimum production-node surface. Process assembly,
// peer listeners, and routing remain outside this contract.
type ControlRaftNode interface {
	ID() uint64
	Status() (lead, applied uint64, fatal error)
	Campaign(context.Context) error
	Step(context.Context, *raftpb.Message) error
	Propose(context.Context, ControlCommand) (ControlState, error)
	ProposeMembership(context.Context, ControlMembershipAction, uint64) error
	ReadBarrier(context.Context) error
	State(context.Context) (ControlState, error)
	Close() error
}

type controlEntry struct {
	RequestID uint64          `json:"requestId"`
	Command   json.RawMessage `json:"command"`
}

type controlMembershipContext struct {
	RequestID uint64                  `json:"requestId"`
	Action    ControlMembershipAction `json:"action"`
	MemberID  uint64                  `json:"memberId,omitempty"`
}

type controlProposalRequest struct {
	id     uint64
	ctx    context.Context
	raw    []byte
	result chan controlProposalResult
}

type controlProposalResult struct {
	state ControlState
	err   error
}

type controlMembershipRequest struct {
	id       uint64
	ctx      context.Context
	action   ControlMembershipAction
	memberID uint64
	result   chan error
}

type controlReadRequest struct {
	id     uint64
	ctx    context.Context
	token  [8]byte
	result chan error
}

type controlPendingRead struct {
	index  uint64
	result chan error
}

type controlCancel struct {
	id  uint64
	ack chan struct{}
}

type DurableControlMember struct {
	id        uint64
	machine   *ControlMachine
	transport PeerTransport
	log       *raftLog
	node      *raft.RawNode
	storage   *raft.MemoryStorage

	proposals   chan controlProposalRequest
	memberships chan controlMembershipRequest
	reads       chan controlReadRequest
	cancels     chan controlCancel
	readCancels chan controlCancel
	incoming    chan *raftpb.Message
	campaigns   chan chan error
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
	requestID   atomic.Uint64

	mu             sync.RWMutex
	lead           uint64
	applied        uint64
	fatal          error
	confState      *raftpb.ConfState
	bootstrapPeers []uint64
}

var _ ControlRaftNode = (*DurableControlMember)(nil)

func OpenDurableControlMember(config ControlRaftConfig) (*DurableControlMember, error) {
	if config.ClusterID == "" || config.GroupID == 0 || config.ID == 0 || config.Machine == nil || config.Transport == nil || !filepath.IsAbs(config.RaftRoot) {
		return nil, ErrInvalidTopology
	}
	if config.Machine.Snapshot().ClusterID != config.ClusterID {
		return nil, ErrInvalidTopology
	}
	peers := make([]raft.Peer, len(config.InitialPeerIDs))
	if config.Bootstrap {
		if len(peers) < MinMembers || len(peers) > MaxMembers {
			return nil, ErrInvalidTopology
		}
		seen := map[uint64]struct{}{}
		present := false
		for index, id := range config.InitialPeerIDs {
			if id == 0 {
				return nil, ErrInvalidTopology
			}
			if _, duplicate := seen[id]; duplicate {
				return nil, ErrInvalidTopology
			}
			seen[id] = struct{}{}
			peers[index] = raft.Peer{ID: id}
			present = present || id == config.ID
		}
		if !present {
			return nil, ErrInvalidTopology
		}
	} else if len(peers) != 0 {
		return nil, ErrInvalidTopology
	}
	tick := config.TickInterval
	if tick == 0 {
		tick = defaultTickInterval
	}
	if tick < 10*time.Millisecond || tick > time.Second {
		return nil, ErrInvalidTopology
	}
	// The control group has one fixed storage identity. Its placement state
	// carries mutable ownership epochs; the log root itself must not be rebound
	// whenever that state advances.
	log, recovered, err := openRaftLog(config.RaftRoot, raftLogBinding{
		clusterID: config.ClusterID, memberID: config.ID, groupID: config.GroupID,
		partition: 0, ownershipEpoch: 1,
	})
	if err != nil {
		return nil, err
	}
	node, err := raft.NewRawNode(&raft.Config{
		ID: config.ID, ElectionTick: 10, HeartbeatTick: 1, Storage: recovered.storage, Applied: recovered.applied,
		MaxSizePerMsg: maxSizePerMessage, MaxCommittedSizePerReady: maxSizePerMessage,
		MaxUncommittedEntriesSize: maxUncommittedEntriesBytes, MaxInflightMsgs: maxInflightMessages, MaxInflightBytes: maxInflightBytes,
		CheckQuorum: true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe,
		DisableProposalForwarding: true, StepDownOnRemoval: true, Logger: consensusLogger{},
	})
	if err != nil {
		_ = log.close()
		return nil, err
	}
	if recovered.fresh && config.Bootstrap {
		if err := node.Bootstrap(peers); err != nil {
			_ = log.close()
			return nil, err
		}
	}
	member := &DurableControlMember{
		id: config.ID, machine: config.Machine, transport: config.Transport, log: log, node: node, storage: recovered.storage,
		proposals: make(chan controlProposalRequest, controlQueueDepth), memberships: make(chan controlMembershipRequest, controlQueueDepth),
		reads: make(chan controlReadRequest, controlQueueDepth), cancels: make(chan controlCancel, maxControlPending), readCancels: make(chan controlCancel, maxControlPending),
		incoming: make(chan *raftpb.Message, peerQueueDepth), campaigns: make(chan chan error, 1), stop: make(chan struct{}), done: make(chan struct{}), applied: recovered.applied,
		bootstrapPeers: slices.Clone(config.InitialPeerIDs),
	}
	if recovered.confState != nil {
		member.confState = proto.Clone(recovered.confState).(*raftpb.ConfState)
	}
	go member.run(tick, recovered.confState)
	return member, nil
}

func (member *DurableControlMember) ID() uint64 { return member.id }

func (member *DurableControlMember) ConfState() *raftpb.ConfState {
	member.mu.RLock()
	defer member.mu.RUnlock()
	if member.confState == nil {
		return nil
	}
	return proto.Clone(member.confState).(*raftpb.ConfState)
}

func (member *DurableControlMember) setConfState(confState *raftpb.ConfState) {
	if confState == nil {
		return
	}
	cloned := proto.Clone(confState).(*raftpb.ConfState)
	member.mu.Lock()
	member.confState = cloned
	member.mu.Unlock()
}

func (member *DurableControlMember) Status() (lead, applied uint64, fatal error) {
	member.mu.RLock()
	defer member.mu.RUnlock()
	return member.lead, member.applied, member.fatal
}

func (member *DurableControlMember) Campaign(ctx context.Context) error {
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

func (member *DurableControlMember) Step(ctx context.Context, message *raftpb.Message) error {
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

func (member *DurableControlMember) Propose(ctx context.Context, command ControlCommand) (ControlState, error) {
	commandRaw, err := EncodeControlCommand(command)
	if err != nil {
		return ControlState{}, err
	}
	id := member.requestID.Add(1)
	raw, err := json.Marshal(controlEntry{RequestID: id, Command: commandRaw})
	if err != nil || len(raw) > maxControlEntryBytes {
		return ControlState{}, ErrControlOrder
	}
	request := controlProposalRequest{id: id, ctx: ctx, raw: raw, result: make(chan controlProposalResult, 1)}
	select {
	case member.proposals <- request:
	case <-ctx.Done():
		return ControlState{}, ctx.Err()
	case <-member.done:
		return ControlState{}, ErrMemberStopped
	}
	select {
	case result := <-request.result:
		return result.state, result.err
	case <-ctx.Done():
		member.cancel(member.cancels, id)
		return ControlState{}, ctx.Err()
	case <-member.done:
		return ControlState{}, ErrMemberStopped
	}
}

func (member *DurableControlMember) ProposeMembership(ctx context.Context, action ControlMembershipAction, memberID uint64) error {
	if action != ControlRaftAddLearner && action != ControlRaftEnterJoint && action != ControlRaftLeaveJoint && action != ControlRaftRemoveMember {
		return ErrControlOrder
	}
	if action != ControlRaftLeaveJoint && memberID == 0 {
		return ErrControlOrder
	}
	id := member.requestID.Add(1)
	request := controlMembershipRequest{id: id, ctx: ctx, action: action, memberID: memberID, result: make(chan error, 1)}
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
		member.cancel(member.cancels, id)
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
}

func (member *DurableControlMember) ReadBarrier(ctx context.Context) error {
	id := member.requestID.Add(1)
	request := controlReadRequest{id: id, ctx: ctx, result: make(chan error, 1)}
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
		member.cancel(member.readCancels, id)
		return ctx.Err()
	case <-member.done:
		return ErrMemberStopped
	}
}

func (member *DurableControlMember) State(ctx context.Context) (ControlState, error) {
	if err := member.ReadBarrier(ctx); err != nil {
		return ControlState{}, err
	}
	return member.machine.Snapshot(), nil
}

func (member *DurableControlMember) cancel(queue chan controlCancel, id uint64) {
	ack := make(chan struct{})
	select {
	case queue <- controlCancel{id: id, ack: ack}:
	case <-member.done:
		return
	}
	select {
	case <-ack:
	case <-member.done:
	}
}

func (member *DurableControlMember) Close() error {
	member.closeOnce.Do(func() { close(member.stop) })
	<-member.done
	return member.log.close()
}

func decodeControlEntry(raw []byte) (controlEntry, error) {
	if len(raw) == 0 || len(raw) > maxControlEntryBytes {
		return controlEntry{}, ErrControlOrder
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var entry controlEntry
	if err := decoder.Decode(&entry); err != nil || entry.RequestID == 0 || len(entry.Command) == 0 {
		return controlEntry{}, ErrControlOrder
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return controlEntry{}, ErrControlOrder
	}
	return entry, nil
}

func membershipContext(request controlMembershipRequest) ([]byte, error) {
	raw, err := json.Marshal(controlMembershipContext{RequestID: request.id, Action: request.action, MemberID: request.memberID})
	if err != nil || len(raw) > 1024 {
		return nil, ErrControlOrder
	}
	return raw, nil
}

func decodeMembershipContext(raw []byte) (controlMembershipContext, error) {
	if len(raw) == 0 || len(raw) > 1024 {
		return controlMembershipContext{}, ErrControlOrder
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value controlMembershipContext
	if err := decoder.Decode(&value); err != nil || value.RequestID == 0 {
		return controlMembershipContext{}, ErrControlOrder
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return controlMembershipContext{}, ErrControlOrder
	}
	return value, nil
}

func confContains(values []uint64, id uint64) bool { return slices.Contains(values, id) }

func (member *DurableControlMember) setStatus(lead, applied uint64, fatal error) {
	member.mu.Lock()
	member.lead, member.applied = lead, applied
	if fatal != nil {
		member.fatal = fatal
	}
	member.mu.Unlock()
}

func controlError(message string, err error) error {
	return fmt.Errorf("control raft %s: %w", message, err)
}
