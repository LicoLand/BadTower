package stationcluster

import (
	"errors"
	"slices"
	"sort"
)

const (
	ControlStateVersion        = 1
	MaxControlVersion   uint32 = 16
)

var (
	ErrControlConflict  = errors.New("cluster control state conflict")
	ErrControlOrder     = errors.New("cluster control lifecycle order violation")
	ErrVersionRollback  = errors.New("cluster control version rollback rejected")
	ErrMoveInProgress   = errors.New("cluster partition move already in progress")
	ErrMemberStillOwner = errors.New("cluster member still owns partitions")
)

type ControlMemberRole string

const (
	ControlRoleLearner ControlMemberRole = "learner"
	ControlRoleVoter   ControlMemberRole = "voter"
)

type ControlMemberPhase string

const (
	ControlPhaseJoining  ControlMemberPhase = "joining"
	ControlPhaseCaughtUp ControlMemberPhase = "caught-up"
	ControlPhaseJoint    ControlMemberPhase = "joint"
	ControlPhaseStable   ControlMemberPhase = "stable"
)

type ControlMember struct {
	ID             uint64             `json:"id"`
	FaultDomain    string             `json:"faultDomain"`
	Incarnation    uint64             `json:"incarnation"`
	CommandVersion uint32             `json:"commandVersion"`
	StoreVersion   uint32             `json:"storeVersion"`
	Role           ControlMemberRole  `json:"role"`
	Phase          ControlMemberPhase `json:"phase"`
	CaughtUpIndex  uint64             `json:"caughtUpIndex,omitempty"`
	Draining       bool               `json:"draining,omitempty"`
}

type ControlPlacement struct {
	Partition uint16                    `json:"partition"`
	Epoch     uint64                    `json:"epoch"`
	Replicas  [ReplicationFactor]uint64 `json:"replicas"`
}

type PartitionMovePhase string

const (
	MovePhasePlanned   PartitionMovePhase = "planned"
	MovePhaseCaughtUp  PartitionMovePhase = "caught-up"
	MovePhaseJoint     PartitionMovePhase = "joint"
	MovePhaseCommitted PartitionMovePhase = "committed"
)

type PartitionMove struct {
	Partition     uint16             `json:"partition"`
	FromMemberID  uint64             `json:"fromMemberId"`
	ToMemberID    uint64             `json:"toMemberId"`
	ExpectedEpoch uint64             `json:"expectedEpoch"`
	Phase         PartitionMovePhase `json:"phase"`
}

type DrainIntent struct {
	MemberID uint64 `json:"memberId"`
}

type BackupAuthorization struct {
	BackupID            uint64 `json:"backupId"`
	CoordinatorMemberID uint64 `json:"coordinatorMemberId"`
	PlacementEpoch      uint64 `json:"placementEpoch"`
	Partition           uint16 `json:"partition"`
	SourceMemberID      uint64 `json:"sourceMemberId"`
}

// ControlState is a deterministic snapshot suitable for replication by one
// external control Raft group. It contains no timers, local observations, or
// consensus implementation.
type ControlState struct {
	Version           uint32               `json:"version"`
	ClusterID         string               `json:"clusterId"`
	PlacementEpoch    uint64               `json:"placementEpoch"`
	MinCommandVersion uint32               `json:"minCommandVersion"`
	MinStoreVersion   uint32               `json:"minStoreVersion"`
	Members           []ControlMember      `json:"members"`
	RetiredMemberIDs  []uint64             `json:"retiredMemberIds,omitempty"`
	Placements        []ControlPlacement   `json:"placements"`
	DrainIntents      []DrainIntent        `json:"drainIntents,omitempty"`
	ActiveMove        *PartitionMove       `json:"activeMove,omitempty"`
	LastMove          *PartitionMove       `json:"lastMove,omitempty"`
	BackupSequence    uint64               `json:"backupSequence,omitempty"`
	ActiveBackup      *BackupAuthorization `json:"activeBackup,omitempty"`
}

func (state ControlState) Clone() ControlState {
	clone := state
	clone.Members = slices.Clone(state.Members)
	clone.RetiredMemberIDs = slices.Clone(state.RetiredMemberIDs)
	clone.Placements = slices.Clone(state.Placements)
	clone.DrainIntents = slices.Clone(state.DrainIntents)
	if state.ActiveMove != nil {
		copyMove := *state.ActiveMove
		clone.ActiveMove = &copyMove
	}
	if state.LastMove != nil {
		copyMove := *state.LastMove
		clone.LastMove = &copyMove
	}
	if state.ActiveBackup != nil {
		copyBackup := *state.ActiveBackup
		clone.ActiveBackup = &copyBackup
	}
	return clone
}

func (state ControlState) Validate() error {
	if state.Version != ControlStateVersion || state.ClusterID == "" || len(state.ClusterID) > 128 || state.PlacementEpoch == 0 || !validControlVersion(state.MinCommandVersion) || !validControlVersion(state.MinStoreVersion) {
		return ErrInvalidTopology
	}
	if len(state.Members) < MinMembers || len(state.Members) > MaxMembers || len(state.Placements) != PartitionCount {
		return ErrInvalidTopology
	}
	memberIDs := make(map[uint64]ControlMember, len(state.Members))
	voters := make(map[uint64]ControlMember, len(state.Members))
	domains := make(map[string]struct{}, ReplicationFactor)
	lastID := uint64(0)
	for index, member := range state.Members {
		if member.ID == 0 || member.FaultDomain == "" || member.Incarnation == 0 || !validControlVersion(member.CommandVersion) || !validControlVersion(member.StoreVersion) || member.CommandVersion < state.MinCommandVersion || member.StoreVersion < state.MinStoreVersion || index > 0 && member.ID <= lastID {
			return ErrInvalidTopology
		}
		if member.Role != ControlRoleLearner && member.Role != ControlRoleVoter {
			return ErrInvalidTopology
		}
		if member.Role == ControlRoleLearner && member.Phase != ControlPhaseJoining && member.Phase != ControlPhaseCaughtUp && member.Phase != ControlPhaseJoint {
			return ErrInvalidTopology
		}
		if member.Role == ControlRoleVoter && member.Phase != ControlPhaseStable {
			return ErrInvalidTopology
		}
		lastID = member.ID
		memberIDs[member.ID] = member
		if member.Role == ControlRoleVoter {
			voters[member.ID] = member
			domains[member.FaultDomain] = struct{}{}
		}
	}
	if len(voters) < MinMembers || len(domains) < ReplicationFactor {
		return ErrInvalidTopology
	}
	lastRetired := uint64(0)
	for index, retiredID := range state.RetiredMemberIDs {
		if retiredID == 0 || (index > 0 && retiredID <= lastRetired) {
			return ErrInvalidTopology
		}
		if _, active := memberIDs[retiredID]; active {
			return ErrInvalidTopology
		}
		lastRetired = retiredID
	}
	for index, placement := range state.Placements {
		if placement.Partition != uint16(index) || placement.Epoch == 0 || placement.Epoch > state.PlacementEpoch {
			return ErrInvalidTopology
		}
		seen := map[uint64]struct{}{}
		placementDomains := map[string]struct{}{}
		for _, id := range placement.Replicas {
			member, ok := voters[id]
			if !ok {
				return ErrInvalidTopology
			}
			if _, duplicate := seen[id]; duplicate {
				return ErrInvalidTopology
			}
			seen[id] = struct{}{}
			if _, duplicate := placementDomains[member.FaultDomain]; duplicate {
				return ErrInvalidTopology
			}
			placementDomains[member.FaultDomain] = struct{}{}
		}
	}
	intentIDs := make(map[uint64]struct{}, len(state.DrainIntents))
	lastID = 0
	for index, intent := range state.DrainIntents {
		member, ok := memberIDs[intent.MemberID]
		if !ok || !member.Draining || index > 0 && intent.MemberID <= lastID {
			return ErrInvalidTopology
		}
		intentIDs[intent.MemberID] = struct{}{}
		lastID = intent.MemberID
	}
	for _, member := range state.Members {
		_, hasIntent := intentIDs[member.ID]
		if member.Draining != hasIntent {
			return ErrInvalidTopology
		}
	}
	if state.ActiveMove != nil {
		if err := state.validateMove(*state.ActiveMove, false); err != nil {
			return err
		}
		placement := state.Placements[state.ActiveMove.Partition]
		from, fromOK := memberIDs[state.ActiveMove.FromMemberID]
		to, toOK := memberIDs[state.ActiveMove.ToMemberID]
		if !fromOK || !toOK || !from.Draining || to.Draining || to.Role != ControlRoleVoter || to.Phase != ControlPhaseStable || !slices.Contains(placement.Replicas[:], from.ID) || slices.Contains(placement.Replicas[:], to.ID) {
			return ErrInvalidTopology
		}
	}
	if state.LastMove != nil && state.LastMove.Phase != MovePhaseCommitted {
		return ErrInvalidTopology
	}
	if state.ActiveBackup != nil {
		backup := state.ActiveBackup
		coordinator, coordOK := voters[backup.CoordinatorMemberID]
		if backup.BackupID == 0 || backup.BackupID > state.BackupSequence || !coordOK || coordinator.Phase != ControlPhaseStable || backup.PlacementEpoch != state.PlacementEpoch || backup.Partition >= PartitionCount || !slices.Contains(state.Placements[backup.Partition].Replicas[:], backup.SourceMemberID) {
			return ErrInvalidTopology
		}
	}
	return nil
}

func (state ControlState) validateMove(move PartitionMove, committed bool) error {
	if move.Partition >= PartitionCount || move.FromMemberID == 0 || move.ToMemberID == 0 || move.FromMemberID == move.ToMemberID || move.ExpectedEpoch == 0 {
		return ErrInvalidTopology
	}
	if !committed && move.ExpectedEpoch != state.PlacementEpoch {
		return ErrStaleEpoch
	}
	if move.Phase != MovePhasePlanned && move.Phase != MovePhaseCaughtUp && move.Phase != MovePhaseJoint {
		return ErrInvalidTopology
	}
	return nil
}

func validControlVersion(version uint32) bool { return version > 0 && version <= MaxControlVersion }

func sortControlState(state *ControlState) {
	sort.Slice(state.Members, func(i, j int) bool { return state.Members[i].ID < state.Members[j].ID })
	slices.Sort(state.RetiredMemberIDs)
	sort.Slice(state.DrainIntents, func(i, j int) bool { return state.DrainIntents[i].MemberID < state.DrainIntents[j].MemberID })
}

func memberIndex(members []ControlMember, id uint64) int {
	index, found := slices.BinarySearchFunc(members, id, func(member ControlMember, target uint64) int {
		if member.ID < target {
			return -1
		}
		if member.ID > target {
			return 1
		}
		return 0
	})
	if !found {
		return -1
	}
	return index
}
