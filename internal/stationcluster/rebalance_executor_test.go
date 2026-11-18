package stationcluster

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
	"go.etcd.io/raft/v3/raftpb"
)

type machineControlNode struct{ machine *ControlMachine }

func (node machineControlNode) ID() uint64                                  { return 1 }
func (node machineControlNode) Status() (uint64, uint64, error)             { return 1, 1, nil }
func (node machineControlNode) Campaign(context.Context) error              { return nil }
func (node machineControlNode) Step(context.Context, *raftpb.Message) error { return nil }
func (node machineControlNode) Propose(_ context.Context, command ControlCommand) (ControlState, error) {
	return node.machine.Apply(command)
}
func (node machineControlNode) ProposeMembership(context.Context, ControlMembershipAction, uint64) error {
	return nil
}
func (node machineControlNode) ReadBarrier(context.Context) error { return nil }
func (node machineControlNode) State(context.Context) (ControlState, error) {
	return node.machine.Snapshot(), nil
}
func (node machineControlNode) Close() error { return nil }

type interruptingMembership struct {
	actions   []string
	failJoint bool
}

func (membership *interruptingMembership) InstallLearnerSnapshot(context.Context, uint16, uint64, uint64) error {
	membership.actions = append(membership.actions, "snapshot")
	return nil
}
func (membership *interruptingMembership) WaitLearnerCaughtUp(context.Context, uint16, uint64, uint64) (uint64, error) {
	membership.actions = append(membership.actions, "caught-up")
	return 10, nil
}
func (membership *interruptingMembership) EnterJoint(context.Context, uint16, uint64, uint64, uint64) error {
	membership.actions = append(membership.actions, "joint")
	if membership.failJoint {
		membership.failJoint = false
		return errors.New("interrupted")
	}
	return nil
}
func (membership *interruptingMembership) MoveLeadershipOff(context.Context, uint16, uint64, uint64, uint64) error {
	membership.actions = append(membership.actions, "leadership-off")
	return nil
}
func (membership *interruptingMembership) LeaveJoint(context.Context, uint16, uint64) error {
	membership.actions = append(membership.actions, "leave-joint")
	return nil
}
func (membership *interruptingMembership) RemoveReplica(context.Context, uint16, uint64, uint64) error {
	membership.actions = append(membership.actions, "remove")
	return nil
}

func TestRaftMemberDynamicLearnerJointTransferAndRemove(t *testing.T) {
	transport := newMemoryPeerTransport()
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	initial := []uint64{1, 2, 3}
	all := []uint64{1, 2, 3, 4}
	namespace := "ns_DDDDDDDDDDDDDDDDDDDDDD"
	partition := stationcore.PartitionFor(namespace)
	groupID := uint64(partition) + 1
	cores := map[uint64]*stationcore.Core{}
	members := map[uint64]*RaftMember{}
	open := func(id uint64, peers []uint64, join bool) {
		t.Helper()
		core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
		if err != nil {
			t.Fatal(err)
		}
		member, err := OpenRaftMember(RaftMemberConfig{ClusterID: "cluster-dynamic-data", GroupID: groupID, ID: id,
			Partition: partition, OwnershipEpoch: 7, PeerIDs: peers, RaftRoot: t.TempDir(), Core: core,
			Transport: transport, TickInterval: 10 * time.Millisecond, Join: join})
		if err != nil {
			_ = core.Close()
			t.Fatal(err)
		}
		cores[id], members[id] = core, member
		transport.set(id, member)
	}
	for _, id := range initial {
		open(id, initial, false)
	}
	t.Cleanup(func() {
		for id, member := range members {
			transport.set(id, nil)
			if member != nil {
				_ = member.Close()
			}
			if cores[id] != nil {
				_ = cores[id].Close()
			}
		}
	})
	if err := members[1].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	leaderID := awaitLeader(t, members, 1)
	for index := 0; index < raftSnapshotIntervalEntries+4; index++ {
		command, err := cores[leaderID].PrepareCommand(stationcore.OperationReserve, stationcore.ReserveInput{
			NamespaceID: namespace, RequestID: fmt.Sprintf("rq_dynamic%015d", index), TTLMs: 60_000,
		}, 7)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := members[leaderID].Propose(context.Background(), command); err != nil {
			t.Fatal(err)
		}
	}

	// Member 4 starts with no bootstrapped configuration. The committed learner
	// ConfChange activates it and the compacted leader sends the real Core
	// snapshot plus bounded log tail.
	open(4, all, true)
	if err := members[leaderID].AddLearner(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	awaitRaftAppliedAtLeast(t, members[4], members[leaderID].currentApplied())
	if got := cores[4].ObservationSnapshot().Aggregate.Addresses; got != raftSnapshotIntervalEntries+4 {
		t.Fatalf("learner address count=%d", got)
	}
	if err := members[leaderID].EnterJoint(context.Background(), 4); err != nil {
		t.Fatal(err)
	}
	if err := members[leaderID].TransferLeadership(context.Background(), leaderID, 4); err != nil {
		t.Fatal(err)
	}
	awaitLeader(t, members, 4)
	if err := members[4].LeaveJoint(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := members[4].RemoveMember(context.Background(), leaderID); err != nil {
		t.Fatal(err)
	}
	if err := members[4].ReadBarrier(context.Background()); err != nil {
		t.Fatalf("post-remove quorum read: %v", err)
	}
}

func awaitRaftAppliedAtLeast(t *testing.T, member *RaftMember, target uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, applied, fatal := member.Status()
		if fatal != nil {
			t.Fatal(fatal)
		}
		if applied >= target {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("learner did not catch up")
}

func TestRebalanceExecutorPersistsPhaseAndResumesAfterInterruption(t *testing.T) {
	machine, err := OpenControlMachine(ControlBootstrap{Root: t.TempDir(), ClusterID: "cluster-rebalance-resume", PlacementEpoch: 3,
		MinCommandVersion: 1, MinStoreVersion: 1, Members: []ControlMember{
			controlTestMember(1, "zone-a", 1, 1), controlTestMember(2, "zone-b", 1, 1),
			controlTestMember(3, "zone-c", 1, 1), controlTestMember(4, "zone-d", 1, 1),
		}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = machine.Close() })
	control := machineControlNode{machine: machine}
	state := machine.Snapshot()
	drain := ControlCommand{CommandVersion: 1, StoreVersion: 1, Kind: ControlDrainMember, ExpectedEpoch: state.PlacementEpoch, MemberID: 1}
	if _, err := machine.Apply(drain); err != nil {
		t.Fatal(err)
	}
	membership := &interruptingMembership{failJoint: true}
	executor, err := NewRebalanceExecutor(control, membership, 1, 1)
	if err != nil {
		t.Fatal(err)
	}

	// First call performs snapshot/catch-up and persists caught-up. The next
	// call is interrupted before joint consensus, so the replicated phase must
	// remain caught-up. A fresh executor then resumes at joint without replaying
	// snapshot installation.
	move, err := executor.ExecuteDrainMove(context.Background(), 1, 4)
	if err != nil || move.Phase != MovePhaseCaughtUp {
		t.Fatalf("caught-up move=%+v err=%v", move, err)
	}
	if _, err = executor.ExecuteDrainMove(context.Background(), 1, 4); err == nil {
		t.Fatal("expected interruption")
	}
	if got := machine.Snapshot().ActiveMove.Phase; got != MovePhaseCaughtUp {
		t.Fatalf("phase after interruption=%s", got)
	}
	executor, _ = NewRebalanceExecutor(control, membership, 1, 1)
	move, err = executor.ExecuteDrainMove(context.Background(), 1, 4)
	if err != nil || move.Phase != MovePhaseJoint {
		t.Fatalf("joint move=%+v err=%v", move, err)
	}
	move, err = executor.ExecuteDrainMove(context.Background(), 1, 4)
	if err != nil || move.Phase != MovePhaseCommitted {
		t.Fatalf("committed move=%+v err=%v", move, err)
	}
	want := []string{"snapshot", "caught-up", "joint", "joint", "leadership-off", "leave-joint", "remove"}
	if !reflect.DeepEqual(membership.actions, want) {
		t.Fatalf("actions=%v", membership.actions)
	}
}
