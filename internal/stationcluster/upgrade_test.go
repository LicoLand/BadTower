package stationcluster

import (
	"errors"
	"testing"
)

func TestRollingUpgradeRequiresCanaryFollowersThenLeaders(t *testing.T) {
	state := upgradeTestState(t)
	current := ClusterVersion{Command: 1, Store: StoreVersion}
	next := ClusterVersion{Command: 2, Store: StoreVersion + 1}
	plan, err := PlanRollingUpgrade(state, UpgradePreflight{
		Current: current, Next: next,
		LeaderMemberIDs: []uint64{1},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := []uint64{4, 2, 3, 1}
	wantStages := []UpgradeStage{UpgradeCanaryLearner, UpgradeFollower, UpgradeFollower, UpgradeLeader}
	if len(plan) != len(wantIDs) {
		t.Fatalf("upgrade steps = %+v", plan)
	}
	for index, step := range plan {
		if step.Sequence != uint16(index+1) || step.MemberID != wantIDs[index] || step.Stage != wantStages[index] || step.From != current || step.To != next {
			t.Fatalf("step %d = %+v", index, step)
		}
	}
}

func TestRollingUpgradeRejectsQuorumLossAndNonAdjacentVersion(t *testing.T) {
	state := upgradeTestState(t)
	_, err := PlanRollingUpgrade(state, UpgradePreflight{
		Current: ClusterVersion{Command: 1, Store: StoreVersion}, Next: ClusterVersion{Command: 2, Store: StoreVersion + 1},
		LeaderMemberIDs: []uint64{1}, UnavailableMemberIDs: []uint64{2},
	})
	if !errors.Is(err, ErrUpgradeQuorum) {
		t.Fatalf("quorum guard error = %v", err)
	}
	_, err = PlanRollingUpgrade(state, UpgradePreflight{
		Current: ClusterVersion{Command: 1, Store: StoreVersion}, Next: ClusterVersion{Command: 3, Store: StoreVersion + 2},
		LeaderMemberIDs: []uint64{1},
	})
	if !errors.Is(err, ErrUpgradeAdmission) {
		t.Fatalf("N/N+2 admission error = %v", err)
	}
	state.Members[0].CommandVersion = 3
	state.Members[0].StoreVersion = StoreVersion + 2
	_, err = PlanRollingUpgrade(state, UpgradePreflight{
		Current: ClusterVersion{Command: 1, Store: StoreVersion}, Next: ClusterVersion{Command: 2, Store: StoreVersion + 1},
		LeaderMemberIDs: []uint64{1},
	})
	if !errors.Is(err, ErrUpgradeAdmission) {
		t.Fatalf("foreign member version error = %v", err)
	}
}

func TestVersionFloorRequiresCompletedRolloutAndRejectsOldRollback(t *testing.T) {
	state := upgradeTestState(t)
	target := ClusterVersion{Command: 2, Store: StoreVersion + 1}
	if _, err := PreflightVersionFloorAdvance(state, target); !errors.Is(err, ErrUpgradeOrder) {
		t.Fatalf("early floor error = %v", err)
	}
	if err := PreflightRollback(state, ClusterVersion{Command: 1, Store: StoreVersion}, 2); err != nil {
		t.Fatalf("rollback before floor: %v", err)
	}
	for index := range state.Members {
		state.Members[index].CommandVersion = 2
		state.Members[index].StoreVersion = target.Store
	}
	command, err := PreflightVersionFloorAdvance(state, target)
	if err != nil {
		t.Fatal(err)
	}
	if command.Kind != ControlSetVersionFloor || command.MinCommand != target.Command || command.MinStore != target.Store || command.ExpectedEpoch != state.PlacementEpoch {
		t.Fatalf("floor command = %+v", command)
	}
	state.MinCommandVersion = 2
	state.MinStoreVersion = target.Store
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := PreflightRollback(state, ClusterVersion{Command: 1, Store: StoreVersion}, 2); !errors.Is(err, ErrVersionRollback) {
		t.Fatalf("old rollback error = %v", err)
	}
	if err := PreflightRollback(state, target, 2); err != nil {
		t.Fatalf("N+1 rollback after floor: %v", err)
	}
}

func upgradeTestState(t *testing.T) ControlState {
	t.Helper()
	state := backupTestControlState(t)
	state.Members = append(state.Members, ControlMember{
		ID: 4, FaultDomain: "zone-d", Incarnation: 1, CommandVersion: 1, StoreVersion: StoreVersion,
		Role: ControlRoleLearner, Phase: ControlPhaseCaughtUp, CaughtUpIndex: 50,
	})
	sortControlState(&state)
	if err := state.Validate(); err != nil {
		t.Fatal(err)
	}
	return state
}
