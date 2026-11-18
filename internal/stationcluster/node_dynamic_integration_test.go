package stationcluster

import (
	"context"
	"crypto/tls"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestFourNodeTLSOperatorJoinPromoteAndExecutePartitionMove(t *testing.T) {
	ca, caKey, roots := newNodeTestCA(t)
	listeners := make([]net.Listener, 4)
	registry := make([]NodeMemberConfig, 4)
	for index := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[index] = listener
		registry[index] = NodeMemberConfig{ID: uint64(index + 1), FaultDomain: string(rune('a' + index)), PeerURL: "https://" + listener.Addr().String()}
	}
	bootstrap := append([]NodeMemberConfig(nil), registry[:3]...)
	nodes := make([]*Node, 4)
	for index := range nodes {
		id := uint64(index + 1)
		certificate := newNodeTestCertificate(t, ca, caKey, id, "cluster-four-node")
		serverTLS, _ := PeerServerTLSConfig(certificate, roots)
		clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, NextProtos: []string{"h2"}}
		root := t.TempDir()
		config := NodeConfig{Schema: NodeConfigSchema, ClusterID: "cluster-four-node", MemberID: id, OwnershipEpoch: 9, PeerListen: listeners[index].Addr().String(), CoreRoot: filepath.Join(root, "core"), RaftRoot: filepath.Join(root, "raft"), TLS: NodeTLSFiles{CertificateFile: filepath.Join(root, "cert"), PrivateKeyFile: filepath.Join(root, "key"), CAFile: filepath.Join(root, "ca")}, Members: registry, BootstrapMembers: bootstrap, Join: index == 3}
		node, err := OpenNode(config, serverTLS, clientTLS, clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatalf("open %d: %v", id, err)
		}
		nodes[index] = node
	}
	for index, node := range nodes {
		go func(node *Node, listener net.Listener) { _ = node.ServePeer(listener) }(node, listeners[index])
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	_ = nodes[0].control.Campaign(ctx)
	controlLeader := awaitNodeControlLeader(t, nodes[:3], 0)
	operator, err := NewOperator(nodes[controlLeader-1], nodes[controlLeader-1].PartitionMembership(), 1, StoreVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = operator.Join(ctx, ControlMember{ID: 4, FaultDomain: "d", Incarnation: 1, CommandVersion: 1, StoreVersion: StoreVersion}); err != nil {
		t.Fatal(err)
	}
	awaitControlApplied(t, map[uint64]*DurableControlMember{1: nodes[0].control.(*DurableControlMember), 2: nodes[1].control.(*DurableControlMember), 3: nodes[2].control.(*DurableControlMember), 4: nodes[3].control.(*DurableControlMember)}, 4, controlLeader)
	_, applied, fatal := nodes[3].control.Status()
	if fatal != nil || applied == 0 {
		t.Fatalf("join applied=%d fatal=%v", applied, fatal)
	}
	if _, err = operator.MarkCaughtUp(ctx, 4, applied); err != nil {
		t.Fatal(err)
	}
	if _, err = operator.Promote(ctx, 4); err != nil {
		t.Fatal(err)
	}
	drainingID := uint64(1)
	if drainingID == controlLeader {
		drainingID = 2
	}
	if _, err = operator.Drain(ctx, drainingID); err != nil {
		t.Fatal(err)
	}
	restartedExecutor := false
	for {
		state, stateErr := nodes[controlLeader-1].control.State(ctx)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		var partition uint16
		var replacement uint64
		found := false
		for _, placement := range state.Placements {
			hasDraining := false
			present := map[uint64]bool{}
			for _, id := range placement.Replicas {
				present[id] = true
				hasDraining = hasDraining || id == drainingID
			}
			if !hasDraining {
				continue
			}
			for candidate := uint64(1); candidate <= 4; candidate++ {
				if candidate != drainingID && !present[candidate] {
					partition, replacement, found = placement.Partition, candidate, true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			break
		}
		for phase := 0; phase < 3; phase++ {
			move, moveErr := operator.Rebalance(ctx, drainingID, replacement)
			if moveErr != nil {
				t.Fatalf("partition %d phase %d: %v", partition, phase, moveErr)
			}
			if move == nil {
				t.Fatal("missing move")
			}
			if !restartedExecutor && phase == 0 {
				operator, err = NewOperator(nodes[controlLeader-1], nodes[controlLeader-1].PartitionMembership(), 1, StoreVersion)
				if err != nil {
					t.Fatal(err)
				}
				restartedExecutor = true
			}
		}
	}
	state, err := operator.Remove(ctx, drainingID)
	if err != nil {
		t.Fatal(err)
	}
	if controlMember(&state, drainingID) != nil {
		t.Fatal("drained member remained in control state")
	}
}
