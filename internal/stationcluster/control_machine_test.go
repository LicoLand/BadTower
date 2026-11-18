package stationcluster

import (
	"errors"
	"testing"
)

func TestControlLifecyclePersistsAndRecoversBoundedReplacement(t *testing.T) {
	root := t.TempDir()
	bootstrap := ControlBootstrap{
		Root: root, ClusterID: "cluster-control-lifecycle", PlacementEpoch: 7,
		MinCommandVersion: 1, MinStoreVersion: 1,
		Members: []ControlMember{
			controlTestMember(1, "zone-a", 2, 2),
			controlTestMember(2, "zone-b", 2, 2),
			controlTestMember(3, "zone-c", 2, 2),
		},
	}
	machine, err := OpenControlMachine(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(command ControlCommand) ControlState {
		t.Helper()
		state, applyErr := machine.Apply(command)
		if applyErr != nil {
			t.Fatalf("apply %s: %v", command.Kind, applyErr)
		}
		return state
	}
	command := func(kind ControlCommandKind) ControlCommand {
		return ControlCommand{CommandVersion: 2, StoreVersion: 2, Kind: kind, ExpectedEpoch: machine.Snapshot().PlacementEpoch}
	}

	floor := command(ControlSetVersionFloor)
	floor.MinCommand, floor.MinStore = 2, 2
	apply(floor)
	rollback := ControlCommand{CommandVersion: 1, StoreVersion: 2, Kind: ControlDrainMember, ExpectedEpoch: 7, MemberID: 1}
	if _, err := machine.Apply(rollback); !errors.Is(err, ErrVersionRollback) {
		t.Fatalf("rollback err=%v", err)
	}

	join := command(ControlJoinLearner)
	join.Member = controlTestMember(4, "zone-d", 2, 2)
	rawJoin, err := EncodeControlCommand(join)
	if err != nil {
		t.Fatal(err)
	}
	state, err := machine.ApplyBytes(rawJoin)
	if err != nil {
		t.Fatal(err)
	}
	state = apply(join)
	if member := controlMember(&state, 4); member == nil || member.Role != ControlRoleLearner || member.Phase != ControlPhaseJoining {
		t.Fatalf("joined member=%+v", member)
	}
	weak := join
	weak.Member.FaultDomain = "zone-x"
	if _, err := machine.Apply(weak); !errors.Is(err, ErrControlConflict) {
		t.Fatalf("join conflict err=%v", err)
	}
	weak = command(ControlJoinLearner)
	weak.Member = controlTestMember(5, "zone-e", 1, 2)
	if _, err := machine.Apply(weak); !errors.Is(err, ErrInvalidTopology) {
		t.Fatalf("weak learner err=%v", err)
	}

	caught := command(ControlMarkCaughtUp)
	caught.MemberID, caught.CaughtUpIndex = 4, 100
	apply(caught)
	apply(caught)
	joint := command(ControlBeginJointPromote)
	joint.MemberID = 4
	apply(joint)
	apply(joint)
	promote := command(ControlCommitPromote)
	promote.MemberID = 4
	apply(promote)
	apply(promote)
	drain := command(ControlDrainMember)
	drain.MemberID = 1
	apply(drain)
	apply(drain)

	// Stop after the first move reaches caught-up. Reopening must recover the
	// exact active step and continue without constructing another move.
	state = machine.Snapshot()
	move, ok, err := state.NextDrainMove(1, 4, 2, 2)
	if err != nil || !ok {
		t.Fatalf("first move ok=%v err=%v", ok, err)
	}
	apply(move)
	caughtMove := move
	caughtMove.Kind = ControlMoveCaughtUp
	apply(caughtMove)
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}

	machine, err = OpenControlMachine(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	state = machine.Snapshot()
	if state.ActiveMove == nil || state.ActiveMove.Phase != MovePhaseCaughtUp {
		t.Fatalf("recovered move=%+v", state.ActiveMove)
	}
	jointMove := caughtMove
	jointMove.Kind = ControlBeginMoveJoint
	apply(jointMove)
	commitMove := caughtMove
	commitMove.Kind = ControlCommitMove
	state = apply(commitMove)
	// A replicated duplicate is idempotent even though commit advanced the
	// ownership epoch beyond the command's expected fence.
	state = apply(commitMove)
	if state.PlacementEpoch != 8 {
		t.Fatalf("duplicate advanced epoch=%d", state.PlacementEpoch)
	}

	for {
		move, ok, err = state.NextDrainMove(1, 4, 2, 2)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			break
		}
		apply(move)
		move.Kind = ControlMoveCaughtUp
		apply(move)
		move.Kind = ControlBeginMoveJoint
		apply(move)
		move.Kind = ControlCommitMove
		state = apply(move)
	}
	if state.ActiveMove != nil {
		t.Fatal("move remained active")
	}
	for _, placement := range state.Placements {
		for _, memberID := range placement.Replicas {
			if memberID == 1 {
				t.Fatalf("partition %d retained drained member", placement.Partition)
			}
		}
	}
	remove := command(ControlRemoveMember)
	remove.MemberID = 1
	state = apply(remove)
	state = apply(remove)
	if len(state.Members) != 3 || memberIndex(state.Members, 1) >= 0 {
		t.Fatalf("members=%+v", state.Members)
	}
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}

	machine, err = OpenControlMachine(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	defer machine.Close()
	recovered := machine.Snapshot()
	if recovered.MinCommandVersion != 2 || recovered.MinStoreVersion != 2 || recovered.PlacementEpoch != state.PlacementEpoch || len(recovered.Members) != 3 {
		t.Fatalf("recovered state=%+v", recovered)
	}
	rejoin := ControlCommand{
		CommandVersion: 2, StoreVersion: 2, Kind: ControlJoinLearner, ExpectedEpoch: recovered.PlacementEpoch,
		Member: ControlMember{ID: 1, FaultDomain: "zone-a", Incarnation: 2, CommandVersion: 2, StoreVersion: 2},
	}
	if _, err := machine.Apply(rejoin); !errors.Is(err, ErrControlConflict) {
		t.Fatalf("reusing retired member ID err=%v, want ErrControlConflict", err)
	}
}

func TestControlRejectsEpochOrderAndForeignStore(t *testing.T) {
	root := t.TempDir()
	bootstrap := ControlBootstrap{
		Root: root, ClusterID: "cluster-control-negative", PlacementEpoch: 3,
		MinCommandVersion: 1, MinStoreVersion: 1,
		Members: []ControlMember{
			controlTestMember(1, "zone-a", 1, 1),
			controlTestMember(2, "zone-b", 1, 1),
			controlTestMember(3, "zone-c", 1, 1),
		},
	}
	machine, err := OpenControlMachine(bootstrap)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := machine.Apply(ControlCommand{CommandVersion: 1, StoreVersion: 1, Kind: ControlDrainMember, ExpectedEpoch: 2, MemberID: 1}); !errors.Is(err, ErrStaleEpoch) {
		t.Fatalf("stale epoch err=%v", err)
	}
	if _, err := machine.Apply(ControlCommand{CommandVersion: 1, StoreVersion: 1, Kind: ControlBeginJointPromote, ExpectedEpoch: 3, MemberID: 1}); !errors.Is(err, ErrControlOrder) {
		t.Fatalf("invalid promotion order err=%v", err)
	}
	if err := machine.Close(); err != nil {
		t.Fatal(err)
	}
	foreign := bootstrap
	foreign.ClusterID = "cluster-control-foreign"
	if opened, err := OpenControlMachine(foreign); !errors.Is(err, ErrInvalidTopology) {
		if opened != nil {
			_ = opened.Close()
		}
		t.Fatalf("foreign root err=%v", err)
	}
}

func controlTestMember(id uint64, domain string, commandVersion, storeVersion uint32) ControlMember {
	return ControlMember{ID: id, FaultDomain: domain, Incarnation: 1, CommandVersion: commandVersion, StoreVersion: storeVersion}
}
