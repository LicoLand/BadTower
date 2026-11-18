package stationcluster

import "slices"

func applyPlanMove(state *ControlState, command ControlCommand) error {
	requested := PartitionMove{
		Partition: command.Partition, FromMemberID: command.FromMemberID, ToMemberID: command.ToMemberID,
		ExpectedEpoch: command.ExpectedEpoch, Phase: MovePhasePlanned,
	}
	if state.ActiveMove != nil {
		if *state.ActiveMove == requested {
			return nil
		}
		return ErrMoveInProgress
	}
	if state.LastMove != nil && sameMoveIdentity(*state.LastMove, requested) && state.LastMove.ExpectedEpoch == requested.ExpectedEpoch {
		return nil
	}
	if err := state.validateMove(requested, false); err != nil {
		return err
	}
	placement := state.Placements[requested.Partition]
	if placement.Epoch > requested.ExpectedEpoch || !slices.Contains(placement.Replicas[:], requested.FromMemberID) || slices.Contains(placement.Replicas[:], requested.ToMemberID) {
		return ErrControlConflict
	}
	from := controlMember(state, requested.FromMemberID)
	to := controlMember(state, requested.ToMemberID)
	if from == nil || to == nil || !from.Draining || to.Draining || to.Role != ControlRoleVoter || to.Phase != ControlPhaseStable {
		return ErrControlOrder
	}
	for _, replicaID := range placement.Replicas {
		if replicaID == requested.FromMemberID {
			continue
		}
		replica := controlMember(state, replicaID)
		if replica == nil || replica.FaultDomain == to.FaultDomain {
			return ErrInvalidTopology
		}
	}
	state.ActiveMove = &requested
	return nil
}

func applyMovePhase(state *ControlState, command ControlCommand, required, next PartitionMovePhase) error {
	if state.ActiveMove == nil {
		if state.LastMove != nil && sameMoveCommand(*state.LastMove, command) {
			return nil
		}
		return ErrControlOrder
	}
	if !sameMoveCommand(*state.ActiveMove, command) {
		return ErrControlConflict
	}
	if state.ActiveMove.Phase == next {
		return nil
	}
	if state.ActiveMove.Phase != required {
		return ErrControlOrder
	}
	state.ActiveMove.Phase = next
	return nil
}

func applyCommitMove(state *ControlState, command ControlCommand) error {
	if state.ActiveMove == nil {
		if state.LastMove != nil && sameMoveCommand(*state.LastMove, command) && state.LastMove.Phase == MovePhaseCommitted {
			return nil
		}
		return ErrControlOrder
	}
	move := *state.ActiveMove
	if !sameMoveCommand(move, command) {
		return ErrControlConflict
	}
	if move.Phase != MovePhaseJoint {
		return ErrControlOrder
	}
	placement := &state.Placements[move.Partition]
	replaced := false
	for index, memberID := range placement.Replicas {
		if memberID == move.FromMemberID {
			placement.Replicas[index] = move.ToMemberID
			replaced = true
			break
		}
	}
	if !replaced {
		return ErrControlConflict
	}
	state.PlacementEpoch++
	placement.Epoch = state.PlacementEpoch
	move.Phase = MovePhaseCommitted
	state.LastMove = &move
	state.ActiveMove = nil
	state.ActiveBackup = nil
	return nil
}

func sameMoveCommand(move PartitionMove, command ControlCommand) bool {
	return move.Partition == command.Partition && move.FromMemberID == command.FromMemberID && move.ToMemberID == command.ToMemberID && move.ExpectedEpoch == command.ExpectedEpoch
}

func sameMoveIdentity(left, right PartitionMove) bool {
	return left.Partition == right.Partition && left.FromMemberID == right.FromMemberID && left.ToMemberID == right.ToMemberID
}

// NextDrainMove returns at most one deterministic partition move. It is a
// pure planner: the returned command changes no state until replicated and
// applied by the external control group.
func (state ControlState) NextDrainMove(memberID, replacementID uint64, commandVersion, storeVersion uint32) (ControlCommand, bool, error) {
	if err := state.Validate(); err != nil {
		return ControlCommand{}, false, err
	}
	if state.ActiveMove != nil {
		return ControlCommand{}, false, ErrMoveInProgress
	}
	from, to := controlMember(&state, memberID), controlMember(&state, replacementID)
	if from == nil || to == nil || !from.Draining || to.Draining || to.Role != ControlRoleVoter {
		return ControlCommand{}, false, ErrControlOrder
	}
	for _, placement := range state.Placements {
		if slices.Contains(placement.Replicas[:], memberID) && !slices.Contains(placement.Replicas[:], replacementID) {
			return ControlCommand{
				CommandVersion: commandVersion, StoreVersion: storeVersion, Kind: ControlPlanMove,
				ExpectedEpoch: state.PlacementEpoch, Partition: placement.Partition,
				FromMemberID: memberID, ToMemberID: replacementID,
			}, true, nil
		}
	}
	return ControlCommand{}, false, nil
}
