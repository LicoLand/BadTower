package stationcluster

import (
	"context"
	"errors"
	"time"
)

var ErrRebalanceUnavailable = errors.New("cluster rebalance execution unavailable")

// PartitionMembership is the production data-group membership boundary. Each
// method returns only after the named Raft operation is committed (and, for
// catch-up, applied). Implementations must be idempotent because the
// replicated control phase is the recovery cursor.
type PartitionMembership interface {
	InstallLearnerSnapshot(context.Context, uint16, uint64, uint64) error
	WaitLearnerCaughtUp(context.Context, uint16, uint64, uint64) (uint64, error)
	EnterJoint(context.Context, uint16, uint64, uint64, uint64) error
	MoveLeadershipOff(context.Context, uint16, uint64, uint64, uint64) error
	LeaveJoint(context.Context, uint16, uint64) error
	RemoveReplica(context.Context, uint16, uint64, uint64) error
}

// RebalanceExecutor executes one replicated placement move at a time. It
// never advances a control phase before the corresponding data-group action
// succeeds, so restart resumes without claiming an unexecuted ConfChange.
type RebalanceExecutor struct {
	control        ControlRaftNode
	data           PartitionMembership
	commandVersion uint32
	storeVersion   uint32
}

func NewRebalanceExecutor(control ControlRaftNode, data PartitionMembership, commandVersion, storeVersion uint32) (*RebalanceExecutor, error) {
	if control == nil || data == nil || !validControlVersion(commandVersion) || !validControlVersion(storeVersion) {
		return nil, ErrRebalanceUnavailable
	}
	return &RebalanceExecutor{control: control, data: data, commandVersion: commandVersion, storeVersion: storeVersion}, nil
}

// ExecuteDrainMove plans (when necessary) and advances exactly one partition
// move. A nil result means the draining member no longer owns a partition.
func (executor *RebalanceExecutor) ExecuteDrainMove(ctx context.Context, drainingID, replacementID uint64) (*PartitionMove, error) {
	state, err := executor.control.State(ctx)
	if err != nil {
		return nil, err
	}
	if state.ActiveMove == nil {
		command, ok, err := state.NextDrainMove(drainingID, replacementID, executor.commandVersion, executor.storeVersion)
		if err != nil || !ok {
			return nil, err
		}
		state, err = executor.control.Propose(ctx, command)
		if err != nil {
			return nil, err
		}
	}
	move := *state.ActiveMove
	if move.FromMemberID != drainingID || move.ToMemberID != replacementID || move.ExpectedEpoch != state.PlacementEpoch {
		return nil, ErrControlConflict
	}
	dataEpoch := state.Placements[move.Partition].Epoch
	command := func(kind ControlCommandKind) ControlCommand {
		return ControlCommand{CommandVersion: executor.commandVersion, StoreVersion: executor.storeVersion, Kind: kind,
			ExpectedEpoch: move.ExpectedEpoch, Partition: move.Partition, FromMemberID: move.FromMemberID, ToMemberID: move.ToMemberID}
	}
	switch move.Phase {
	case MovePhasePlanned:
		if err := executor.data.InstallLearnerSnapshot(ctx, move.Partition, move.ToMemberID, dataEpoch); err != nil {
			return &move, err
		}
		if _, err := executor.data.WaitLearnerCaughtUp(ctx, move.Partition, move.ToMemberID, dataEpoch); err != nil {
			return &move, err
		}
		state, err = executor.control.Propose(ctx, command(ControlMoveCaughtUp))
	case MovePhaseCaughtUp:
		if err := executor.data.EnterJoint(ctx, move.Partition, move.FromMemberID, move.ToMemberID, dataEpoch); err != nil {
			return &move, err
		}
		state, err = executor.control.Propose(ctx, command(ControlBeginMoveJoint))
	case MovePhaseJoint:
		if err := executor.data.MoveLeadershipOff(ctx, move.Partition, move.FromMemberID, move.ToMemberID, dataEpoch); err != nil {
			return &move, err
		}
		if err := executor.data.LeaveJoint(ctx, move.Partition, dataEpoch); err != nil {
			return &move, err
		}
		if err := executor.data.RemoveReplica(ctx, move.Partition, move.FromMemberID, dataEpoch); err != nil {
			return &move, err
		}
		state, err = executor.control.Propose(ctx, command(ControlCommitMove))
	default:
		return &move, ErrControlOrder
	}
	if err != nil {
		return &move, err
	}
	if state.ActiveMove != nil {
		result := *state.ActiveMove
		return &result, nil
	}
	if state.LastMove != nil && sameMoveIdentity(*state.LastMove, move) {
		result := *state.LastMove
		return &result, nil
	}
	return nil, ErrControlConflict
}

// PartitionGroupAccess resolves the current local view of a partition leader
// and one replica's durable applied index. A process assembler supplies this
// from its authenticated peer registry; the operator never learns peer URLs.
type PartitionGroupAccess interface {
	Leader(uint16) (*RaftMember, error)
	Applied(uint16, uint64) (uint64, error)
}

// RaftPartitionMembership is the production PartitionMembership adapter. Its
// methods execute real etcd/raft ConfChange requests through RaftMember's
// single-owner loop; no control phase can be advanced from observation alone.
type RaftPartitionMembership struct{ Groups PartitionGroupAccess }

func (membership RaftPartitionMembership) leader(partition uint16, epoch uint64) (*RaftMember, error) {
	if membership.Groups == nil || partition >= PartitionCount || epoch == 0 {
		return nil, ErrRebalanceUnavailable
	}
	leader, err := membership.Groups.Leader(partition)
	if err != nil || leader == nil || leader.partition != partition || epoch < leader.epoch {
		return nil, ErrStaleEpoch
	}
	lead, _, fatal := leader.Status()
	if fatal != nil || lead != leader.ID() {
		return nil, ErrNotLeader
	}
	return leader, nil
}

func (membership RaftPartitionMembership) InstallLearnerSnapshot(ctx context.Context, partition uint16, toMemberID, epoch uint64) error {
	leader, err := membership.leader(partition, epoch)
	if err != nil {
		return err
	}
	return leader.AddLearner(ctx, toMemberID)
}

func (membership RaftPartitionMembership) WaitLearnerCaughtUp(ctx context.Context, partition uint16, toMemberID, epoch uint64) (uint64, error) {
	leader, err := membership.leader(partition, epoch)
	if err != nil {
		return 0, err
	}
	if err := leader.ReadBarrier(ctx); err != nil {
		return 0, err
	}
	_, target, fatal := leader.Status()
	if fatal != nil || target == 0 {
		return 0, ErrRebalanceUnavailable
	}
	for {
		applied, err := membership.Groups.Applied(partition, toMemberID)
		if err == nil && applied >= target {
			return applied, nil
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (membership RaftPartitionMembership) EnterJoint(ctx context.Context, partition uint16, _, toMemberID, epoch uint64) error {
	leader, err := membership.leader(partition, epoch)
	if err != nil {
		return err
	}
	return leader.EnterJoint(ctx, toMemberID)
}

func (membership RaftPartitionMembership) MoveLeadershipOff(ctx context.Context, partition uint16, fromMemberID, toMemberID, epoch uint64) error {
	leader, err := membership.leader(partition, epoch)
	if err != nil {
		return err
	}
	lead, _, _ := leader.Status()
	if lead != fromMemberID {
		return nil
	}
	return leader.TransferLeadership(ctx, fromMemberID, toMemberID)
}

func (membership RaftPartitionMembership) LeaveJoint(ctx context.Context, partition uint16, epoch uint64) error {
	leader, err := membership.leader(partition, epoch)
	if err != nil {
		return err
	}
	return leader.LeaveJoint(ctx)
}

func (membership RaftPartitionMembership) RemoveReplica(ctx context.Context, partition uint16, fromMemberID, epoch uint64) error {
	leader, err := membership.leader(partition, epoch)
	if err != nil {
		return err
	}
	return leader.RemoveMember(ctx, fromMemberID)
}
