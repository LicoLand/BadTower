package stationcluster

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"sync"
)

const maxControlCommandBytes = 64 << 10

type ControlCommandKind string

const (
	ControlJoinLearner       ControlCommandKind = "join-learner"
	ControlMarkCaughtUp      ControlCommandKind = "mark-caught-up"
	ControlBeginJointPromote ControlCommandKind = "begin-joint-promote"
	ControlCommitPromote     ControlCommandKind = "commit-promote"
	ControlDrainMember       ControlCommandKind = "drain-member"
	ControlCancelDrain       ControlCommandKind = "cancel-drain"
	ControlRemoveMember      ControlCommandKind = "remove-member"
	ControlUpdateVersion     ControlCommandKind = "update-member-version"
	ControlSetVersionFloor   ControlCommandKind = "set-version-floor"
	ControlPlanMove          ControlCommandKind = "plan-partition-move"
	ControlMoveCaughtUp      ControlCommandKind = "partition-move-caught-up"
	ControlBeginMoveJoint    ControlCommandKind = "begin-partition-move-joint"
	ControlCommitMove        ControlCommandKind = "commit-partition-move"
	ControlAuthorizeBackup   ControlCommandKind = "authorize-backup-partition"
	ControlCompleteBackup    ControlCommandKind = "complete-backup"
)

type ControlCommand struct {
	CommandVersion uint32             `json:"commandVersion"`
	StoreVersion   uint32             `json:"storeVersion"`
	Kind           ControlCommandKind `json:"kind"`
	ExpectedEpoch  uint64             `json:"expectedEpoch"`
	Member         ControlMember      `json:"member,omitempty"`
	MemberID       uint64             `json:"memberId,omitempty"`
	CaughtUpIndex  uint64             `json:"caughtUpIndex,omitempty"`
	MinCommand     uint32             `json:"minCommandVersion,omitempty"`
	MinStore       uint32             `json:"minStoreVersion,omitempty"`
	Partition      uint16             `json:"partition,omitempty"`
	FromMemberID   uint64             `json:"fromMemberId,omitempty"`
	ToMemberID     uint64             `json:"toMemberId,omitempty"`
	BackupID       uint64             `json:"backupId,omitempty"`
}

type ControlBootstrap struct {
	Root              string
	ClusterID         string
	PlacementEpoch    uint64
	MinCommandVersion uint32
	MinStoreVersion   uint32
	Members           []ControlMember
}

// ControlMachine atomically applies already-ordered commands. The caller is
// responsible for replicating command bytes through the production control
// Raft group; this type deliberately contains no election or transport path.
type ControlMachine struct {
	mu    sync.RWMutex
	store *controlStore
	state ControlState
}

func OpenControlMachine(config ControlBootstrap) (*ControlMachine, error) {
	store, snapshot, fresh, err := openControlStore(config.Root, config.ClusterID)
	if err != nil {
		return nil, err
	}
	if !fresh {
		if err := snapshot.Validate(); err != nil {
			_ = store.close()
			return nil, err
		}
		return &ControlMachine{store: store, state: snapshot}, nil
	}
	if config.PlacementEpoch == 0 || !validControlVersion(config.MinCommandVersion) || !validControlVersion(config.MinStoreVersion) {
		_ = store.close()
		return nil, ErrInvalidTopology
	}
	members := slices.Clone(config.Members)
	for index := range members {
		members[index].Role = ControlRoleVoter
		members[index].Phase = ControlPhaseStable
		members[index].CaughtUpIndex = 0
		members[index].Draining = false
	}
	sortControlState(&ControlState{Members: members})
	topologyMembers := make([]Member, len(members))
	for index, member := range members {
		topologyMembers[index] = Member{ID: member.ID, FaultDomain: member.FaultDomain, Incarnation: member.Incarnation}
	}
	placements, err := (Topology{ClusterID: config.ClusterID, Epoch: config.PlacementEpoch, Members: topologyMembers}).Placements()
	if err != nil {
		_ = store.close()
		return nil, err
	}
	state := ControlState{
		Version: ControlStateVersion, ClusterID: config.ClusterID, PlacementEpoch: config.PlacementEpoch,
		MinCommandVersion: config.MinCommandVersion, MinStoreVersion: config.MinStoreVersion,
		Members: members, Placements: make([]ControlPlacement, len(placements)),
	}
	for index, placement := range placements {
		state.Placements[index] = ControlPlacement{Partition: placement.Partition, Epoch: placement.Epoch, Replicas: placement.Replicas}
	}
	if err := state.Validate(); err != nil {
		_ = store.close()
		return nil, err
	}
	if err := store.save(state); err != nil {
		_ = store.close()
		return nil, err
	}
	return &ControlMachine{store: store, state: state}, nil
}

func (machine *ControlMachine) Snapshot() ControlState {
	machine.mu.RLock()
	defer machine.mu.RUnlock()
	return machine.state.Clone()
}

func (machine *ControlMachine) Apply(command ControlCommand) (ControlState, error) {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	if !validControlVersion(command.CommandVersion) || !validControlVersion(command.StoreVersion) || command.CommandVersion < machine.state.MinCommandVersion || command.StoreVersion < machine.state.MinStoreVersion {
		return ControlState{}, ErrVersionRollback
	}
	if command.ExpectedEpoch != machine.state.PlacementEpoch {
		if machine.state.LastMove != nil && sameMoveCommand(*machine.state.LastMove, command) {
			return machine.state.Clone(), nil
		}
		return ControlState{}, ErrStaleEpoch
	}
	next := machine.state.Clone()
	if err := applyControlCommand(&next, command); err != nil {
		return ControlState{}, err
	}
	sortControlState(&next)
	if err := next.Validate(); err != nil {
		return ControlState{}, err
	}
	if reflect.DeepEqual(next, machine.state) {
		return machine.state.Clone(), nil
	}
	if err := machine.store.save(next); err != nil {
		return ControlState{}, err
	}
	machine.state = next
	return next.Clone(), nil
}

// ApplyBytes is the production control-Raft state-machine boundary. A caller
// may replicate EncodeControlCommand output verbatim and apply committed bytes
// here; decoding is closed and bounded.
func (machine *ControlMachine) ApplyBytes(raw []byte) (ControlState, error) {
	command, err := DecodeControlCommand(raw)
	if err != nil {
		return ControlState{}, err
	}
	return machine.Apply(command)
}

func EncodeControlCommand(command ControlCommand) ([]byte, error) {
	if !validControlVersion(command.CommandVersion) || !validControlVersion(command.StoreVersion) || command.Kind == "" || command.ExpectedEpoch == 0 {
		return nil, ErrControlOrder
	}
	raw, err := json.Marshal(command)
	if err != nil || len(raw) > maxControlCommandBytes {
		return nil, ErrControlOrder
	}
	return raw, nil
}

func DecodeControlCommand(raw []byte) (ControlCommand, error) {
	if len(raw) == 0 || len(raw) > maxControlCommandBytes {
		return ControlCommand{}, ErrControlOrder
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var command ControlCommand
	if err := decoder.Decode(&command); err != nil {
		return ControlCommand{}, ErrControlOrder
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ControlCommand{}, ErrControlOrder
	}
	if !validControlVersion(command.CommandVersion) || !validControlVersion(command.StoreVersion) || command.Kind == "" || command.ExpectedEpoch == 0 {
		return ControlCommand{}, ErrControlOrder
	}
	return command, nil
}

func (machine *ControlMachine) Close() error {
	machine.mu.Lock()
	defer machine.mu.Unlock()
	return machine.store.close()
}

func applyControlCommand(state *ControlState, command ControlCommand) error {
	switch command.Kind {
	case ControlJoinLearner:
		return applyJoinLearner(state, command.Member)
	case ControlMarkCaughtUp:
		return applyMarkCaughtUp(state, command.MemberID, command.CaughtUpIndex)
	case ControlBeginJointPromote:
		return applyBeginJointPromote(state, command.MemberID)
	case ControlCommitPromote:
		return applyCommitPromote(state, command.MemberID)
	case ControlDrainMember:
		return applyDrain(state, command.MemberID, true)
	case ControlCancelDrain:
		return applyDrain(state, command.MemberID, false)
	case ControlRemoveMember:
		return applyRemoveMember(state, command.MemberID)
	case ControlUpdateVersion:
		return applyUpdateVersion(state, command.MemberID, command.Member.CommandVersion, command.Member.StoreVersion)
	case ControlSetVersionFloor:
		return applyVersionFloor(state, command.MinCommand, command.MinStore)
	case ControlPlanMove:
		return applyPlanMove(state, command)
	case ControlMoveCaughtUp:
		return applyMovePhase(state, command, MovePhasePlanned, MovePhaseCaughtUp)
	case ControlBeginMoveJoint:
		return applyMovePhase(state, command, MovePhaseCaughtUp, MovePhaseJoint)
	case ControlCommitMove:
		return applyCommitMove(state, command)
	case ControlAuthorizeBackup:
		return applyAuthorizeBackup(state, command)
	case ControlCompleteBackup:
		return applyCompleteBackup(state, command)
	default:
		return ErrControlOrder
	}
}

func applyAuthorizeBackup(state *ControlState, command ControlCommand) error {
	if command.Partition >= PartitionCount || command.MemberID == 0 || command.ToMemberID == 0 {
		return ErrControlOrder
	}
	coordinator := controlMember(state, command.MemberID)
	if coordinator == nil || coordinator.Role != ControlRoleVoter || coordinator.Phase != ControlPhaseStable {
		return ErrControlOrder
	}
	if !slices.Contains(state.Placements[command.Partition].Replicas[:], command.ToMemberID) {
		return ErrControlConflict
	}
	if command.BackupID == 0 {
		if state.ActiveBackup != nil {
			return ErrControlConflict
		}
		state.BackupSequence++
		state.ActiveBackup = &BackupAuthorization{
			BackupID:            state.BackupSequence,
			CoordinatorMemberID: command.MemberID,
			PlacementEpoch:      state.PlacementEpoch,
			Partition:           command.Partition,
			SourceMemberID:      command.ToMemberID,
		}
		return nil
	}
	if state.ActiveBackup == nil || state.ActiveBackup.BackupID != command.BackupID || state.ActiveBackup.CoordinatorMemberID != command.MemberID || state.ActiveBackup.PlacementEpoch != state.PlacementEpoch {
		return ErrControlConflict
	}
	state.ActiveBackup.Partition = command.Partition
	state.ActiveBackup.SourceMemberID = command.ToMemberID
	return nil
}

func applyCompleteBackup(state *ControlState, command ControlCommand) error {
	if state.ActiveBackup == nil {
		return nil
	}
	if command.BackupID != 0 && state.ActiveBackup.BackupID != command.BackupID {
		return ErrControlConflict
	}
	if command.MemberID != 0 && state.ActiveBackup.CoordinatorMemberID != command.MemberID && command.BackupID != 0 {
		return ErrControlConflict
	}
	state.ActiveBackup = nil
	return nil
}
