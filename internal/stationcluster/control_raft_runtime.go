package stationcluster

import (
	"context"
	"encoding/binary"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func (member *DurableControlMember) run(tickInterval time.Duration, confState *raftpb.ConfState) {
	defer close(member.done)
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()
	pending := make(map[uint64]chan controlProposalResult, maxControlPending)
	pendingMembership := make(map[uint64]chan error, maxControlPending)
	pendingReads := make(map[uint64]controlPendingRead, maxControlPending)
	for {
		select {
		case <-member.stop:
			failControlPending(pending, pendingMembership, pendingReads, ErrMemberStopped)
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
				member.stopWithError(pending, pendingMembership, pendingReads, err)
				return
			}
		case cancellation := <-member.cancels:
			delete(pending, cancellation.id)
			delete(pendingMembership, cancellation.id)
			close(cancellation.ack)
		case cancellation := <-member.readCancels:
			delete(pendingReads, cancellation.id)
			close(cancellation.ack)
		case request := <-member.proposals:
			if request.ctx.Err() != nil {
				request.result <- controlProposalResult{err: request.ctx.Err()}
				break
			}
			if !member.isLeader() {
				request.result <- controlProposalResult{err: ErrNotLeader}
				break
			}
			if len(pending)+len(pendingMembership) >= maxControlPending {
				request.result <- controlProposalResult{err: ErrPeerBackpressure}
				break
			}
			entry, err := decodeControlEntry(request.raw)
			if err != nil {
				request.result <- controlProposalResult{err: err}
				break
			}
			command, err := DecodeControlCommand(entry.Command)
			if err != nil || validateControlBeforeProposal(member.machine.Snapshot(), confState, command) != nil {
				if err == nil {
					err = ErrControlOrder
				}
				request.result <- controlProposalResult{err: err}
				break
			}
			pending[request.id] = request.result
			if err := member.node.Propose(request.raw); err != nil {
				delete(pending, request.id)
				request.result <- controlProposalResult{err: err}
			}
		case request := <-member.memberships:
			if request.ctx.Err() != nil {
				request.result <- request.ctx.Err()
				break
			}
			if !member.isLeader() {
				request.result <- ErrNotLeader
				break
			}
			if len(pending)+len(pendingMembership) >= maxControlPending {
				request.result <- ErrPeerBackpressure
				break
			}
			if err := validateMembership(member.machine.Snapshot(), confState, request.action, request.memberID); err != nil {
				request.result <- err
				break
			}
			raw, err := membershipContext(request)
			if err != nil {
				request.result <- err
				break
			}
			pendingMembership[request.id] = request.result
			if err := proposeMembership(member.node, request.action, request.memberID, raw); err != nil {
				delete(pendingMembership, request.id)
				request.result <- err
			}
		case request := <-member.reads:
			if request.ctx.Err() != nil {
				request.result <- request.ctx.Err()
				break
			}
			if !member.isLeader() {
				request.result <- ErrNotLeader
				break
			}
			if len(pendingReads) >= maxControlPending {
				request.result <- ErrPeerBackpressure
				break
			}
			pendingReads[request.id] = controlPendingRead{result: request.result}
			member.node.ReadIndex(request.token[:])
		}
		if err := member.processReady(&confState, pending, pendingMembership, pendingReads); err != nil {
			member.stopWithError(pending, pendingMembership, pendingReads, err)
			return
		}
	}
}

func (member *DurableControlMember) isLeader() bool {
	status := member.node.BasicStatus()
	return status.Lead == member.id && status.RaftState == raft.StateLeader
}

func proposeMembership(node *raft.RawNode, action ControlMembershipAction, memberID uint64, raw []byte) error {
	switch action {
	case ControlRaftAddLearner:
		changeType := raftpb.ConfChangeType_ConfChangeAddLearnerNode
		return node.ProposeConfChange(&raftpb.ConfChange{Type: &changeType, NodeId: &memberID, Context: raw})
	case ControlRaftRemoveMember:
		changeType := raftpb.ConfChangeType_ConfChangeRemoveNode
		return node.ProposeConfChange(&raftpb.ConfChange{Type: &changeType, NodeId: &memberID, Context: raw})
	case ControlRaftEnterJoint:
		transition := raftpb.ConfChangeTransition_ConfChangeTransitionJointExplicit
		changeType := raftpb.ConfChangeType_ConfChangeAddNode
		return node.ProposeConfChange(&raftpb.ConfChangeV2{Transition: &transition, Changes: []*raftpb.ConfChangeSingle{{Type: &changeType, NodeId: &memberID}}, Context: raw})
	case ControlRaftLeaveJoint:
		return node.ProposeConfChange(&raftpb.ConfChangeV2{Context: raw})
	default:
		return ErrControlOrder
	}
}

func validateControlBeforeProposal(state ControlState, conf *raftpb.ConfState, command ControlCommand) error {
	if conf == nil {
		return ErrControlOrder
	}
	member := controlMember(&state, command.MemberID)
	switch command.Kind {
	case ControlJoinLearner:
		if confContains(conf.Voters, command.Member.ID) || confContains(conf.Learners, command.Member.ID) {
			return ErrControlOrder
		}
	case ControlMarkCaughtUp:
		if !confContains(conf.Learners, command.MemberID) {
			return ErrControlOrder
		}
	case ControlBeginJointPromote:
		if !confContains(conf.Learners, command.MemberID) {
			return ErrControlOrder
		}
	case ControlCommitPromote:
		if !confContains(conf.Voters, command.MemberID) || confContains(conf.Learners, command.MemberID) || len(conf.VotersOutgoing) != 0 {
			return ErrControlOrder
		}
	case ControlRemoveMember:
		if member != nil && (confContains(conf.Voters, command.MemberID) || confContains(conf.Learners, command.MemberID)) {
			return ErrControlOrder
		}
	}
	return nil
}

func validateMembership(state ControlState, conf *raftpb.ConfState, action ControlMembershipAction, memberID uint64) error {
	if conf == nil {
		return ErrControlOrder
	}
	member := controlMember(&state, memberID)
	switch action {
	case ControlRaftAddLearner:
		if member == nil || member.Role != ControlRoleLearner || member.Phase != ControlPhaseJoining || confContains(conf.Voters, memberID) || confContains(conf.Learners, memberID) {
			return ErrControlOrder
		}
	case ControlRaftEnterJoint:
		if member == nil || member.Role != ControlRoleLearner || member.Phase != ControlPhaseJoint || !confContains(conf.Learners, memberID) || len(conf.VotersOutgoing) != 0 {
			return ErrControlOrder
		}
	case ControlRaftLeaveJoint:
		if len(conf.VotersOutgoing) == 0 {
			return ErrControlOrder
		}
	case ControlRaftRemoveMember:
		if member == nil || !member.Draining || stateMemberOwnsPartition(state, memberID) || (!confContains(conf.Voters, memberID) && !confContains(conf.Learners, memberID)) {
			return ErrControlOrder
		}
		next := state.Clone()
		if err := applyRemoveMember(&next, memberID); err != nil {
			return err
		}
		sortControlState(&next)
		if err := next.Validate(); err != nil {
			return err
		}
	default:
		return ErrControlOrder
	}
	return nil
}

func stateMemberOwnsPartition(state ControlState, memberID uint64) bool {
	for _, placement := range state.Placements {
		if confContains(placement.Replicas[:], memberID) {
			return true
		}
	}
	return false
}

func (member *DurableControlMember) processReady(confState **raftpb.ConfState, pending map[uint64]chan controlProposalResult, pendingMembership map[uint64]chan error, pendingReads map[uint64]controlPendingRead) error {
	if confState == nil || *confState == nil {
		return ErrInvalidTopology
	}
	for member.node.HasReady() {
		ready := member.node.Ready()
		if err := member.log.persist(ready); err != nil {
			return err
		}
		if ready.Snapshot != nil && !raft.IsEmptySnap(ready.Snapshot) {
			if err := member.storage.ApplySnapshot(ready.Snapshot); err != nil {
				return err
			}
			if snapshotConf := ready.Snapshot.GetMetadata().GetConfState(); snapshotConf != nil {
				*confState = proto.Clone(snapshotConf).(*raftpb.ConfState)
				member.setConfState(*confState)
			}
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
		lastApplied := member.currentApplied()
		lastTerm := uint64(0)
		type completedControlProposal struct {
			waiter chan controlProposalResult
			result controlProposalResult
		}
		var completedProposals []completedControlProposal
		for _, entry := range ready.CommittedEntries {
			if entry.GetIndex() <= lastApplied {
				continue
			}
			switch entry.GetType() {
			case raftpb.EntryType_EntryNormal:
				if len(entry.Data) > 0 {
					controlEntry, err := decodeControlEntry(entry.Data)
					if err != nil {
						return controlError("decode command", err)
					}
					command, err := DecodeControlCommand(controlEntry.Command)
					if err != nil || validateControlBeforeProposal(member.machine.Snapshot(), *confState, command) != nil {
						return ErrControlOrder
					}
					state, err := member.machine.ApplyBytes(controlEntry.Command)
					if err != nil {
						if waiter := pending[controlEntry.RequestID]; waiter != nil {
							waiter <- controlProposalResult{state: state, err: err}
							delete(pending, controlEntry.RequestID)
						}
						return controlError("apply command", err)
					}
					if waiter := pending[controlEntry.RequestID]; waiter != nil {
						completedProposals = append(completedProposals, completedControlProposal{waiter: waiter, result: controlProposalResult{state: state}})
						delete(pending, controlEntry.RequestID)
					}
				}
			case raftpb.EntryType_EntryConfChange:
				var change raftpb.ConfChange
				if err := proto.Unmarshal(entry.Data, &change); err != nil {
					return err
				}
				if err := member.applyMembershipV1(confState, &change, pendingMembership); err != nil {
					return err
				}
			case raftpb.EntryType_EntryConfChangeV2:
				var change raftpb.ConfChangeV2
				if err := proto.Unmarshal(entry.Data, &change); err != nil {
					return err
				}
				if err := member.applyMembershipV2(confState, &change, pendingMembership); err != nil {
					return err
				}
			}
			lastApplied, lastTerm = entry.GetIndex(), entry.GetTerm()
		}
		if lastApplied > member.currentApplied() {
			if err := member.log.markApplied(lastApplied, lastTerm, *confState); err != nil {
				for _, completed := range completedProposals {
					completed.waiter <- controlProposalResult{err: err}
				}
				return err
			}
			member.setConfState(*confState)
			member.setApplied(lastApplied)
		}
		for _, completed := range completedProposals {
			completed.waiter <- completed.result
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
		applied := member.currentApplied()
		for id, read := range pendingReads {
			if read.index != 0 && applied >= read.index {
				read.result <- nil
				delete(pendingReads, id)
			}
		}
		member.node.Advance(ready)
		status := member.node.BasicStatus()
		member.setStatus(status.Lead, member.currentApplied(), nil)
	}
	return nil
}

func (member *DurableControlMember) applyMembershipV1(confState **raftpb.ConfState, change *raftpb.ConfChange, pending map[uint64]chan error) error {
	if len(change.Context) == 0 {
		if next := member.node.ApplyConfChange(change); next != nil {
			*confState = proto.Clone(next).(*raftpb.ConfState)
		}
		return nil
	}
	contextValue, err := decodeMembershipContext(change.Context)
	if err != nil || validateMembership(member.machine.Snapshot(), *confState, contextValue.Action, contextValue.MemberID) != nil {
		return ErrControlOrder
	}
	if next := member.node.ApplyConfChange(change); next != nil {
		*confState = proto.Clone(next).(*raftpb.ConfState)
	}
	if !membershipApplied(*confState, contextValue.Action, contextValue.MemberID) {
		return ErrControlOrder
	}
	if waiter := pending[contextValue.RequestID]; waiter != nil {
		waiter <- nil
		delete(pending, contextValue.RequestID)
	}
	return nil
}

func (member *DurableControlMember) applyMembershipV2(confState **raftpb.ConfState, change *raftpb.ConfChangeV2, pending map[uint64]chan error) error {
	if len(change.Context) == 0 {
		if next := member.node.ApplyConfChange(change); next != nil {
			*confState = proto.Clone(next).(*raftpb.ConfState)
		}
		return nil
	}
	contextValue, err := decodeMembershipContext(change.Context)
	if err != nil || validateMembership(member.machine.Snapshot(), *confState, contextValue.Action, contextValue.MemberID) != nil {
		return ErrControlOrder
	}
	if next := member.node.ApplyConfChange(change); next != nil {
		*confState = proto.Clone(next).(*raftpb.ConfState)
	}
	if !membershipApplied(*confState, contextValue.Action, contextValue.MemberID) {
		return ErrControlOrder
	}
	if waiter := pending[contextValue.RequestID]; waiter != nil {
		waiter <- nil
		delete(pending, contextValue.RequestID)
	}
	return nil
}

func membershipApplied(conf *raftpb.ConfState, action ControlMembershipAction, memberID uint64) bool {
	if conf == nil {
		return false
	}
	switch action {
	case ControlRaftAddLearner:
		return confContains(conf.Learners, memberID)
	case ControlRaftEnterJoint:
		return confContains(conf.Voters, memberID) && len(conf.VotersOutgoing) > 0
	case ControlRaftLeaveJoint:
		return len(conf.VotersOutgoing) == 0
	case ControlRaftRemoveMember:
		return !confContains(conf.Voters, memberID) && !confContains(conf.Learners, memberID)
	default:
		return false
	}
}

func (member *DurableControlMember) currentApplied() uint64 {
	member.mu.RLock()
	defer member.mu.RUnlock()
	return member.applied
}
func (member *DurableControlMember) setApplied(index uint64) {
	member.mu.Lock()
	member.applied = index
	member.mu.Unlock()
}

func failControlPending(proposals map[uint64]chan controlProposalResult, memberships map[uint64]chan error, reads map[uint64]controlPendingRead, err error) {
	for id, waiter := range proposals {
		waiter <- controlProposalResult{err: err}
		delete(proposals, id)
	}
	for id, waiter := range memberships {
		waiter <- err
		delete(memberships, id)
	}
	for id, waiter := range reads {
		waiter.result <- err
		delete(reads, id)
	}
}

func (member *DurableControlMember) stopWithError(proposals map[uint64]chan controlProposalResult, memberships map[uint64]chan error, reads map[uint64]controlPendingRead, err error) {
	member.setStatus(0, member.currentApplied(), err)
	failControlPending(proposals, memberships, reads, err)
}
