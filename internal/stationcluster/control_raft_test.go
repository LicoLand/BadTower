package stationcluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type controlMemoryTransport struct {
	mu      sync.RWMutex
	members map[uint64]*DurableControlMember
}

func newControlMemoryTransport() *controlMemoryTransport {
	return &controlMemoryTransport{members: make(map[uint64]*DurableControlMember)}
}

func (transport *controlMemoryTransport) Send(ctx context.Context, message *raftpb.Message) error {
	transport.mu.RLock()
	target := transport.members[message.GetTo()]
	transport.mu.RUnlock()
	if target == nil {
		return ErrMemberStopped
	}
	return target.Step(ctx, proto.Clone(message).(*raftpb.Message))
}

func (transport *controlMemoryTransport) set(id uint64, member *DurableControlMember) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if member == nil {
		delete(transport.members, id)
	} else {
		transport.members[id] = member
	}
}

func TestDurableControlRaftMembershipRecoveryFailoverAndQuorum(t *testing.T) {
	transport := newControlMemoryTransport()
	initialIDs := []uint64{1, 2, 3}
	bootstrap := func(root string) ControlBootstrap {
		return ControlBootstrap{
			Root: root, ClusterID: "cluster-durable-control", PlacementEpoch: 11,
			MinCommandVersion: 1, MinStoreVersion: 1,
			Members: []ControlMember{
				controlTestMember(1, "zone-a", 2, 2),
				controlTestMember(2, "zone-b", 2, 2),
				controlTestMember(3, "zone-c", 2, 2),
			},
		}
	}
	machineRoots := map[uint64]string{}
	raftRoots := map[uint64]string{}
	machines := map[uint64]*ControlMachine{}
	members := map[uint64]*DurableControlMember{}
	open := func(id uint64, initial bool) {
		t.Helper()
		if machineRoots[id] == "" {
			machineRoots[id] = t.TempDir()
			raftRoots[id] = t.TempDir()
		}
		machine, err := OpenControlMachine(bootstrap(machineRoots[id]))
		if err != nil {
			t.Fatalf("open machine %d: %v", id, err)
		}
		config := ControlRaftConfig{
			ClusterID: "cluster-durable-control", GroupID: 9001, ID: id, Bootstrap: initial,
			RaftRoot: raftRoots[id], Machine: machine, Transport: transport, TickInterval: 10 * time.Millisecond,
		}
		if initial {
			config.InitialPeerIDs = initialIDs
		}
		member, err := OpenDurableControlMember(config)
		if err != nil {
			_ = machine.Close()
			t.Fatalf("open raft member %d: %v", id, err)
		}
		machines[id], members[id] = machine, member
		transport.set(id, member)
	}
	closeMember := func(id uint64) {
		t.Helper()
		transport.set(id, nil)
		if members[id] != nil {
			if err := members[id].Close(); err != nil {
				t.Fatal(err)
			}
			members[id] = nil
		}
		if machines[id] != nil {
			if err := machines[id].Close(); err != nil {
				t.Fatal(err)
			}
			machines[id] = nil
		}
	}
	for _, id := range initialIDs {
		open(id, true)
	}
	t.Cleanup(func() {
		for id := range members {
			transport.set(id, nil)
			if members[id] != nil {
				_ = members[id].Close()
			}
			if machines[id] != nil {
				_ = machines[id].Close()
			}
		}
	})

	if err := members[1].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	leader := awaitControlLeader(t, members, 1)
	if err := members[leader].ReadBarrier(context.Background()); err != nil {
		t.Fatal(err)
	}

	command := func(kind ControlCommandKind) ControlCommand {
		return ControlCommand{CommandVersion: 2, StoreVersion: 2, Kind: kind, ExpectedEpoch: machines[leader].Snapshot().PlacementEpoch}
	}
	join := command(ControlJoinLearner)
	join.Member = controlTestMember(4, "zone-d", 2, 2)
	if _, err := members[leader].Propose(context.Background(), join); err != nil {
		t.Fatal(err)
	}
	open(4, false)
	if err := members[leader].ProposeMembership(context.Background(), ControlRaftAddLearner, 4); err != nil {
		t.Fatal(err)
	}
	awaitControlApplied(t, members, 4, leader)
	awaitControlMemberPhase(t, machines[4], 4, ControlRoleLearner, ControlPhaseJoining)

	caught := command(ControlMarkCaughtUp)
	caught.MemberID, caught.CaughtUpIndex = 4, members[leader].currentApplied()
	if _, err := members[leader].Propose(context.Background(), caught); err != nil {
		t.Fatal(err)
	}
	joint := command(ControlBeginJointPromote)
	joint.MemberID = 4
	if _, err := members[leader].Propose(context.Background(), joint); err != nil {
		t.Fatal(err)
	}
	if err := members[leader].ProposeMembership(context.Background(), ControlRaftEnterJoint, 4); err != nil {
		t.Fatal(err)
	}
	// Restart one old voter while the real Raft configuration is joint. Its
	// durable ConfState and control phase must recover before the leader leaves
	// joint consensus.
	restartID := uint64(2)
	if restartID == leader {
		restartID = 3
	}
	closeMember(restartID)
	open(restartID, true)
	awaitControlApplied(t, members, restartID, leader)
	if err := members[leader].ProposeMembership(context.Background(), ControlRaftLeaveJoint, 0); err != nil {
		t.Fatal(err)
	}
	promote := command(ControlCommitPromote)
	promote.MemberID = 4
	if _, err := members[leader].Propose(context.Background(), promote); err != nil {
		t.Fatal(err)
	}
	awaitControlMemberPhase(t, machines[4], 4, ControlRoleVoter, ControlPhaseStable)

	// The recovered voter participates in the post-joint quorum and the leader
	// can serve a linearizable control-state read.
	if _, err := members[leader].State(context.Background()); err != nil {
		t.Fatal(err)
	}

	drain := command(ControlDrainMember)
	drain.MemberID = 4
	if _, err := members[leader].Propose(context.Background(), drain); err != nil {
		t.Fatal(err)
	}
	if err := members[leader].ProposeMembership(context.Background(), ControlRaftRemoveMember, 4); err != nil {
		t.Fatal(err)
	}
	remove := command(ControlRemoveMember)
	remove.MemberID = 4
	if _, err := members[leader].Propose(context.Background(), remove); err != nil {
		t.Fatal(err)
	}
	awaitControlAbsent(t, machines, []uint64{1, 2, 3}, 4)
	closeMember(4)

	oldLeader := leader
	closeMember(oldLeader)
	for id, candidate := range members {
		if candidate != nil {
			_ = candidate.Campaign(context.Background())
			leader = awaitControlLeader(t, members, id)
			break
		}
	}
	if leader == oldLeader {
		t.Fatal("stopped control leader remained elected")
	}
	if err := members[leader].ReadBarrier(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Stop one more voter. The sole survivor may reject immediately as a
	// follower or time out an uncommitted proposal, but it must never apply it.
	for id, candidate := range members {
		if candidate != nil && id != leader {
			closeMember(id)
			break
		}
	}
	before := machines[leader].Snapshot()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	failed := command(ControlCancelDrain)
	failed.MemberID = 1
	_, err := members[leader].Propose(ctx, failed)
	if err == nil {
		t.Fatal("minority control proposal committed")
	}
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrNotLeader) {
		t.Fatalf("minority err=%v", err)
	}
	after := machines[leader].Snapshot()
	if before.PlacementEpoch != after.PlacementEpoch || len(before.DrainIntents) != len(after.DrainIntents) {
		t.Fatal("minority changed control state")
	}
}

func awaitControlLeader(t *testing.T, members map[uint64]*DurableControlMember, preferred uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if preferred != 0 && members[preferred] != nil {
			lead, _, fatal := members[preferred].Status()
			if fatal != nil {
				t.Fatal(fatal)
			}
			if lead == preferred {
				return preferred
			}
		}
		for id, member := range members {
			if member == nil {
				continue
			}
			lead, _, fatal := member.Status()
			if fatal != nil {
				t.Fatal(fatal)
			}
			if lead == id {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("control leader election timed out")
	return 0
}

func awaitControlApplied(t *testing.T, members map[uint64]*DurableControlMember, target, reference uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, targetApplied, targetFatal := members[target].Status()
		_, referenceApplied, referenceFatal := members[reference].Status()
		if targetFatal != nil {
			t.Fatal(targetFatal)
		}
		if referenceFatal != nil {
			t.Fatal(referenceFatal)
		}
		if targetApplied >= referenceApplied {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("control member did not catch up")
}

func awaitControlMemberPhase(t *testing.T, machine *ControlMachine, id uint64, role ControlMemberRole, phase ControlMemberPhase) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		state := machine.Snapshot()
		member := controlMember(&state, id)
		if member != nil && member.Role == role && member.Phase == phase {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("control member phase did not converge")
}

func awaitControlAbsent(t *testing.T, machines map[uint64]*ControlMachine, ids []uint64, absent uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range ids {
			if machines[id] == nil {
				continue
			}
			state := machines[id].Snapshot()
			if controlMember(&state, absent) != nil {
				all = false
				break
			}
		}
		if all {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("removed control member remained in voter state")
}
