package stationcluster

import (
	"context"
	"errors"
	"path/filepath"
	"time"
)

var (
	ErrOperatorUnavailable = errors.New("cluster operator unavailable")
	ErrOperatorPreflight   = errors.New("cluster operator preflight rejected")
)

type ClusterStatus struct {
	MemberID       uint64
	LeaderMemberID uint64
	AppliedIndex   uint64
	PlacementEpoch uint64
	MemberCount    uint32
	ActiveMove     *PartitionMove
}

// Operator is the closed mutation surface used by the local cluster service.
// It exposes no peer URLs, roots, credentials, payloads, or raw errors.
type Operator struct {
	node           *Node
	control        ControlRaftNode
	rebalance      *RebalanceExecutor
	commandVersion uint32
	storeVersion   uint32
}

func NewOperator(node *Node, data PartitionMembership, commandVersion, storeVersion uint32) (*Operator, error) {
	if node == nil || node.control == nil || storeVersion != StoreVersion {
		return nil, ErrOperatorUnavailable
	}
	executor, err := NewRebalanceExecutor(node.control, data, commandVersion, storeVersion)
	if err != nil {
		return nil, err
	}
	return &Operator{node: node, control: node.control, rebalance: executor, commandVersion: commandVersion, storeVersion: storeVersion}, nil
}

// NewOperatorWithControl supports an independently assembled local operator
// process while preserving exactly the same control-Raft contract.
func NewOperatorWithControl(control ControlRaftNode, data PartitionMembership, commandVersion, storeVersion uint32) (*Operator, error) {
	if storeVersion != StoreVersion {
		return nil, ErrOperatorPreflight
	}
	executor, err := NewRebalanceExecutor(control, data, commandVersion, storeVersion)
	if err != nil {
		return nil, err
	}
	return &Operator{control: control, rebalance: executor, commandVersion: commandVersion, storeVersion: storeVersion}, nil
}

func (operator *Operator) Status(ctx context.Context) (ClusterStatus, error) {
	state, err := operator.control.State(ctx)
	if err != nil {
		return ClusterStatus{}, err
	}
	lead, applied, fatal := operator.control.Status()
	if fatal != nil {
		return ClusterStatus{}, ErrOperatorUnavailable
	}
	status := ClusterStatus{MemberID: operator.control.ID(), LeaderMemberID: lead, AppliedIndex: applied, PlacementEpoch: state.PlacementEpoch, MemberCount: uint32(len(state.Members))}
	if state.ActiveMove != nil {
		move := *state.ActiveMove
		status.ActiveMove = &move
	}
	return status, nil
}

func (operator *Operator) command(state ControlState, kind ControlCommandKind) ControlCommand {
	return ControlCommand{CommandVersion: operator.commandVersion, StoreVersion: operator.storeVersion, Kind: kind, ExpectedEpoch: state.PlacementEpoch}
}

func (operator *Operator) Bootstrap(ctx context.Context) (ControlState, error) {
	state, err := operator.control.State(ctx)
	if err != nil || state.Validate() != nil || len(state.Members) < MinMembers {
		return ControlState{}, ErrOperatorPreflight
	}
	return state, nil
}

func (operator *Operator) Join(ctx context.Context, member ControlMember) (ControlState, error) {
	state, err := operator.control.State(ctx)
	if err != nil {
		return ControlState{}, err
	}
	command := operator.command(state, ControlJoinLearner)
	command.Member = member
	state, err = operator.control.Propose(ctx, command)
	if err != nil {
		return ControlState{}, err
	}
	if err := operator.control.ProposeMembership(ctx, ControlRaftAddLearner, member.ID); err != nil {
		return ControlState{}, err
	}
	return state, nil
}

func (operator *Operator) MarkCaughtUp(ctx context.Context, memberID, appliedIndex uint64) (ControlState, error) {
	if operator.node != nil {
		observed, err := operator.node.controlCaughtUpIndex(ctx, memberID)
		if err != nil {
			return ControlState{}, err
		}
		appliedIndex = observed
	}
	state, err := operator.control.State(ctx)
	if err != nil {
		return ControlState{}, err
	}
	command := operator.command(state, ControlMarkCaughtUp)
	command.MemberID, command.CaughtUpIndex = memberID, appliedIndex
	return operator.control.Propose(ctx, command)
}

func (operator *Operator) Promote(ctx context.Context, memberID uint64) (ControlState, error) {
	state, err := operator.control.State(ctx)
	if err != nil {
		return ControlState{}, err
	}
	command := operator.command(state, ControlBeginJointPromote)
	command.MemberID = memberID
	state, err = operator.control.Propose(ctx, command)
	if err != nil {
		return ControlState{}, err
	}
	if err = operator.control.ProposeMembership(ctx, ControlRaftEnterJoint, memberID); err != nil {
		return ControlState{}, err
	}
	if err = operator.control.ProposeMembership(ctx, ControlRaftLeaveJoint, 0); err != nil {
		return ControlState{}, err
	}
	command = operator.command(state, ControlCommitPromote)
	command.MemberID = memberID
	return operator.control.Propose(ctx, command)
}

func (operator *Operator) Drain(ctx context.Context, memberID uint64) (ControlState, error) {
	state, err := operator.control.State(ctx)
	if err != nil {
		return ControlState{}, err
	}
	command := operator.command(state, ControlDrainMember)
	command.MemberID = memberID
	return operator.control.Propose(ctx, command)
}

func (operator *Operator) Replace(ctx context.Context, oldMemberID uint64, replacement ControlMember) (ControlState, error) {
	if _, err := operator.Join(ctx, replacement); err != nil {
		return ControlState{}, err
	}
	return operator.Drain(ctx, oldMemberID)
}

func (operator *Operator) Remove(ctx context.Context, memberID uint64) (ControlState, error) {
	state, err := operator.control.State(ctx)
	if err != nil {
		return ControlState{}, err
	}
	if err := operator.control.ProposeMembership(ctx, ControlRaftRemoveMember, memberID); err != nil {
		return ControlState{}, err
	}
	command := operator.command(state, ControlRemoveMember)
	command.MemberID = memberID
	return operator.control.Propose(ctx, command)
}

func (operator *Operator) Rebalance(ctx context.Context, drainingID, replacementID uint64) (*PartitionMove, error) {
	return operator.rebalance.ExecuteDrainMove(ctx, drainingID, replacementID)
}

func (operator *Operator) Backup(ctx context.Context, artifactRoot string, chunkBytes int) (ClusterBackupManifest, error) {
	if operator.node == nil || !filepath.IsAbs(artifactRoot) {
		return ClusterBackupManifest{}, ErrOperatorPreflight
	}
	for attempt := 0; attempt < 3; attempt++ {
		manifest, err := operator.node.CreateBackup(ctx, artifactRoot, chunkBytes)
		if err == nil {
			return manifest, nil
		}
		if !errors.Is(err, ErrBackupIncomplete) || attempt == 2 {
			return ClusterBackupManifest{}, err
		}
		timer := time.NewTimer(time.Duration(attempt+1) * 25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ClusterBackupManifest{}, ctx.Err()
		case <-timer.C:
		}
	}
	return ClusterBackupManifest{}, ErrBackupIncomplete
}

func (operator *Operator) RestorePreflight(backupRoot, isolatedRoot string, expected RestorePreflight) (ClusterBackupManifest, error) {
	if !filepath.IsAbs(backupRoot) || !filepath.IsAbs(isolatedRoot) || snapshotRootsOverlap(backupRoot, isolatedRoot) || requireAbsentAbsoluteRoot(isolatedRoot) != nil {
		return ClusterBackupManifest{}, ErrOperatorPreflight
	}
	return VerifyClusterBackup(backupRoot, expected)
}

func (operator *Operator) UpgradePreflight(ctx context.Context, preflight UpgradePreflight) ([]UpgradeStep, error) {
	state, err := operator.control.State(ctx)
	if err != nil {
		return nil, err
	}
	return PlanRollingUpgrade(state, preflight)
}
