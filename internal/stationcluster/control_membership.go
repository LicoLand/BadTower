package stationcluster

import "slices"

func applyJoinLearner(state *ControlState, member ControlMember) error {
	if slices.Contains(state.RetiredMemberIDs, member.ID) {
		return ErrControlConflict
	}
	if existing := memberIndex(state.Members, member.ID); existing >= 0 {
		current := state.Members[existing]
		member.Role, member.Phase, member.CaughtUpIndex, member.Draining = current.Role, current.Phase, current.CaughtUpIndex, current.Draining
		if current == member {
			return nil
		}
		return ErrControlConflict
	}
	if len(state.Members) >= MaxMembers || member.ID == 0 || member.FaultDomain == "" || member.Incarnation == 0 || member.CommandVersion < state.MinCommandVersion || member.StoreVersion < state.MinStoreVersion {
		return ErrInvalidTopology
	}
	member.Role, member.Phase = ControlRoleLearner, ControlPhaseJoining
	member.CaughtUpIndex, member.Draining = 0, false
	state.Members = append(state.Members, member)
	return nil
}

func applyMarkCaughtUp(state *ControlState, memberID, index uint64) error {
	member := controlMember(state, memberID)
	if member == nil || member.Role != ControlRoleLearner || index == 0 {
		return ErrControlOrder
	}
	if member.Phase == ControlPhaseCaughtUp || member.Phase == ControlPhaseJoint {
		if index < member.CaughtUpIndex {
			return ErrControlOrder
		}
		member.CaughtUpIndex = index
		return nil
	}
	if member.Phase != ControlPhaseJoining {
		return ErrControlOrder
	}
	member.CaughtUpIndex, member.Phase = index, ControlPhaseCaughtUp
	return nil
}

func applyBeginJointPromote(state *ControlState, memberID uint64) error {
	member := controlMember(state, memberID)
	if member == nil || member.Role != ControlRoleLearner {
		return ErrControlOrder
	}
	if member.Phase == ControlPhaseJoint {
		return nil
	}
	if member.Phase != ControlPhaseCaughtUp || member.CaughtUpIndex == 0 {
		return ErrControlOrder
	}
	member.Phase = ControlPhaseJoint
	return nil
}

func applyCommitPromote(state *ControlState, memberID uint64) error {
	member := controlMember(state, memberID)
	if member == nil {
		return ErrControlOrder
	}
	if member.Role == ControlRoleVoter && member.Phase == ControlPhaseStable {
		return nil
	}
	if member.Role != ControlRoleLearner || member.Phase != ControlPhaseJoint {
		return ErrControlOrder
	}
	member.Role, member.Phase = ControlRoleVoter, ControlPhaseStable
	return nil
}

func applyDrain(state *ControlState, memberID uint64, drain bool) error {
	member := controlMember(state, memberID)
	if member == nil {
		return ErrControlOrder
	}
	if member.Draining == drain {
		return nil
	}
	member.Draining = drain
	if drain {
		state.DrainIntents = append(state.DrainIntents, DrainIntent{MemberID: memberID})
	} else {
		for index, intent := range state.DrainIntents {
			if intent.MemberID == memberID {
				state.DrainIntents = append(state.DrainIntents[:index], state.DrainIntents[index+1:]...)
				break
			}
		}
	}
	return nil
}

func applyRemoveMember(state *ControlState, memberID uint64) error {
	index := memberIndex(state.Members, memberID)
	if index < 0 {
		return nil
	}
	if !state.Members[index].Draining {
		return ErrControlOrder
	}
	for _, placement := range state.Placements {
		if slices.Contains(placement.Replicas[:], memberID) {
			return ErrMemberStillOwner
		}
	}
	if state.ActiveMove != nil && (state.ActiveMove.FromMemberID == memberID || state.ActiveMove.ToMemberID == memberID) {
		return ErrMoveInProgress
	}
	if state.ActiveBackup != nil && (state.ActiveBackup.CoordinatorMemberID == memberID || state.ActiveBackup.SourceMemberID == memberID) {
		state.ActiveBackup = nil
	}
	state.Members = append(state.Members[:index], state.Members[index+1:]...)
	if !slices.Contains(state.RetiredMemberIDs, memberID) {
		state.RetiredMemberIDs = append(state.RetiredMemberIDs, memberID)
	}
	for intentIndex, intent := range state.DrainIntents {
		if intent.MemberID == memberID {
			state.DrainIntents = append(state.DrainIntents[:intentIndex], state.DrainIntents[intentIndex+1:]...)
			break
		}
	}
	return nil
}

func applyUpdateVersion(state *ControlState, memberID uint64, commandVersion, storeVersion uint32) error {
	member := controlMember(state, memberID)
	if member == nil || !validControlVersion(commandVersion) || !validControlVersion(storeVersion) {
		return ErrControlOrder
	}
	if commandVersion < member.CommandVersion || storeVersion < member.StoreVersion {
		return ErrVersionRollback
	}
	member.CommandVersion, member.StoreVersion = commandVersion, storeVersion
	return nil
}

func applyVersionFloor(state *ControlState, commandVersion, storeVersion uint32) error {
	if !validControlVersion(commandVersion) || !validControlVersion(storeVersion) || commandVersion < state.MinCommandVersion || storeVersion < state.MinStoreVersion {
		return ErrVersionRollback
	}
	for _, member := range state.Members {
		if member.CommandVersion < commandVersion || member.StoreVersion < storeVersion {
			return ErrControlOrder
		}
	}
	state.MinCommandVersion, state.MinStoreVersion = commandVersion, storeVersion
	return nil
}

func controlMember(state *ControlState, memberID uint64) *ControlMember {
	index := memberIndex(state.Members, memberID)
	if index < 0 {
		return nil
	}
	return &state.Members[index]
}
