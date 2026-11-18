package stationcluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPeerOperatorAuthorizesCommittedLeaderAndExactOperations(t *testing.T) {
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
	certs := make([]tls.Certificate, 4)
	nodes := make([]*Node, 3)
	for index := 0; index < 4; index++ {
		id := uint64(index + 1)
		certs[index] = newNodeTestCertificate(t, ca, caKey, id, "cluster-peer-auth")
	}
	for index := range nodes {
		id := uint64(index + 1)
		serverTLS, err := PeerServerTLSConfig(certs[index], roots)
		if err != nil {
			t.Fatal(err)
		}
		clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certs[index]}, RootCAs: roots, NextProtos: []string{"h2"}}
		root := t.TempDir()
		config := NodeConfig{
			Schema: NodeConfigSchema, ClusterID: "cluster-peer-auth", MemberID: id, OwnershipEpoch: 7,
			PeerListen: listeners[index].Addr().String(), CoreRoot: filepath.Join(root, "core"), RaftRoot: filepath.Join(root, "raft"),
			TLS:              NodeTLSFiles{CertificateFile: filepath.Join(root, "cert"), PrivateKeyFile: filepath.Join(root, "key"), CAFile: filepath.Join(root, "ca")},
			Members:          registry,
			BootstrapMembers: bootstrap,
		}
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
		_ = listeners[3].Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_ = nodes[0].control.Campaign(ctx)
	controlLeader := awaitNodeControlLeader(t, nodes, 0)
	_ = nodes[0].members[0].Campaign(ctx)
	partLeader := awaitNodePartitionLeader(t, nodes, 0)

	followerID := uint64(1)
	if followerID == controlLeader {
		followerID = 2
	}
	targetAddr := "https://" + listeners[partLeader-1].Addr().String()

	clientFor := func(cert tls.Certificate) *http.Client {
		return &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
					Certificates: []tls.Certificate{cert}, RootCAs: roots, NextProtos: []string{"h2"},
				},
				ForceAttemptHTTP2: true,
			},
			CheckRedirect: rejectPeerRedirect,
		}
	}
	postOperator := func(client *http.Client, path string, body []byte) int {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetAddr+path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.ContentLength = int64(len(body))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("POST %s: %v", path, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	var epochBody [8]byte
	binary.BigEndian.PutUint64(epochBody[:], 7)
	var backupBarrierBody [16]byte
	binary.BigEndian.PutUint64(backupBarrierBody[0:8], 7)
	binary.BigEndian.PutUint64(backupBarrierBody[8:16], 1)
	var backupSnapBody [28]byte
	binary.BigEndian.PutUint64(backupSnapBody[0:8], 7)
	binary.BigEndian.PutUint64(backupSnapBody[8:16], 1)
	binary.BigEndian.PutUint32(backupSnapBody[16:20], MinCoreSnapshotChunkBytes)
	binary.BigEndian.PutUint64(backupSnapBody[20:28], 1)

	postRaft := func(client *http.Client, groupID uint64, msg *raftpb.Message) int {
		raw, err := proto.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetAddr+nodePeerPathPrefix+strconv.FormatUint(groupID, 10), bytes.NewReader(raw))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", peerMessageContentType)
		req.ContentLength = int64(len(raw))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("POST raft group %d: %v", groupID, err)
		}
		defer resp.Body.Close()
		return resp.StatusCode
	}

	// 0. Unadmitted configured member (member 4) must be rejected at Raft ingress (both control and partition),
	// and local Raft message types (MsgHup) from even a valid voter must be rejected without crashing the member.
	unadmittedClient := clientFor(certs[3])
	if code := postRaft(unadmittedClient, ControlGroupID, &raftpb.Message{Type: raftpb.MessageType_MsgVote.Enum(), From: proto.Uint64(4), To: proto.Uint64(partLeader), Term: proto.Uint64(99)}); code != http.StatusForbidden {
		t.Fatalf("unadmitted control MsgVote status = %d, want 403", code)
	}
	if code := postRaft(unadmittedClient, 1, &raftpb.Message{Type: raftpb.MessageType_MsgApp.Enum(), From: proto.Uint64(4), To: proto.Uint64(partLeader), Term: proto.Uint64(99)}); code != http.StatusForbidden {
		t.Fatalf("unadmitted partition MsgApp status = %d, want 403", code)
	}
	voterClient := clientFor(certs[followerID-1])
	if code := postRaft(voterClient, ControlGroupID, &raftpb.Message{Type: raftpb.MessageType_MsgHup.Enum(), From: proto.Uint64(followerID), To: proto.Uint64(partLeader)}); code != http.StatusForbidden {
		t.Fatalf("voter control MsgHup status = %d, want 403", code)
	}
	if code := postRaft(voterClient, 1, &raftpb.Message{Type: raftpb.MessageType_MsgHup.Enum(), From: proto.Uint64(followerID), To: proto.Uint64(partLeader)}); code != http.StatusForbidden {
		t.Fatalf("voter partition MsgHup status = %d, want 403", code)
	}
	if _, _, fatal := nodes[partLeader-1].control.Status(); fatal != nil {
		t.Fatalf("control member died after rejected Raft ingress: %v", fatal)
	}
	if _, _, fatal := nodes[partLeader-1].members[0].Status(); fatal != nil {
		t.Fatalf("partition member died after rejected Raft ingress: %v", fatal)
	}

	// 1. Unadmitted configured member (member 4) must be rejected everywhere.
	if code := postOperator(unadmittedClient, nodeOperatorPathPrefix+"1/add-learner/4", epochBody[:]); code != http.StatusForbidden {
		t.Fatalf("unadmitted add-learner status = %d, want 403", code)
	}
	if code := postOperator(unadmittedClient, nodeOperatorPathPrefix+"1/backup-barrier", backupBarrierBody[:]); code != http.StatusForbidden {
		t.Fatalf("unadmitted backup-barrier status = %d, want 403", code)
	}
	if code := postOperator(unadmittedClient, nodeOperatorPathPrefix+"1/backup-snapshot", backupSnapBody[:]); code != http.StatusForbidden {
		t.Fatalf("unadmitted backup-snapshot status = %d, want 403", code)
	}

	// 2. Non-leader committed voter must be rejected.
	followerClient := clientFor(certs[followerID-1])
	if code := postOperator(followerClient, nodeOperatorPathPrefix+"1/add-learner/4", epochBody[:]); code != http.StatusForbidden {
		t.Fatalf("follower add-learner status = %d, want 403", code)
	}
	if code := postOperator(followerClient, nodeOperatorPathPrefix+"1/backup-barrier", backupBarrierBody[:]); code != http.StatusForbidden {
		t.Fatalf("follower backup-barrier status = %d, want 403", code)
	}

	// 3. Control leader without a committed ActiveMove or ActiveBackup must be rejected.
	leaderClient := clientFor(certs[controlLeader-1])
	if code := postOperator(leaderClient, nodeOperatorPathPrefix+"1/add-learner/4", epochBody[:]); code != http.StatusForbidden {
		t.Fatalf("leader add-learner without active move status = %d, want 403", code)
	}
	if code := postOperator(leaderClient, nodeOperatorPathPrefix+"1/backup-barrier", backupBarrierBody[:]); code != http.StatusForbidden {
		t.Fatalf("leader backup-barrier without active backup status = %d, want 403", code)
	}
	if code := postOperator(leaderClient, nodeOperatorPathPrefix+"1/backup-snapshot", backupSnapBody[:]); code != http.StatusForbidden {
		t.Fatalf("leader backup-snapshot without active backup status = %d, want 403", code)
	}

	// 4. Authorize partition 0 backup: partition 0 passes barrier, wrong partition (1) and wrong backupID (99) are rejected.
	authState, err := nodes[controlLeader-1].control.Propose(ctx, ControlCommand{
		CommandVersion: 1, StoreVersion: StoreVersion, Kind: ControlAuthorizeBackup,
		ExpectedEpoch: 7, MemberID: controlLeader, Partition: 0, ToMemberID: partLeader,
	})
	if err != nil || authState.ActiveBackup == nil {
		t.Fatalf("authorize backup: state=%+v err=%v", authState, err)
	}
	_, applied, _ := nodes[controlLeader-1].control.Status()
	if err := (&nodePartitionMembership{node: nodes[controlLeader-1]}).waitControlApplied(ctx, partLeader, applied); err != nil {
		t.Fatalf("waitControlApplied: %v", err)
	}
	binary.BigEndian.PutUint64(backupBarrierBody[8:16], authState.ActiveBackup.BackupID)
	if code := postOperator(leaderClient, nodeOperatorPathPrefix+"1/backup-barrier", backupBarrierBody[:]); code != http.StatusNoContent {
		t.Fatalf("authorized partition 0 backup-barrier status = %d, want 204", code)
	}
	// Wrong partition (group 2 = partition 1) must be forbidden.
	if code := postOperator(leaderClient, nodeOperatorPathPrefix+strconv.Itoa(2)+"/backup-barrier", backupBarrierBody[:]); code != http.StatusForbidden {
		t.Fatalf("wrong partition backup-barrier status = %d, want 403", code)
	}
	// Wrong backupID must be forbidden.
	binary.BigEndian.PutUint64(backupBarrierBody[8:16], authState.ActiveBackup.BackupID+99)
	if code := postOperator(leaderClient, nodeOperatorPathPrefix+"1/backup-barrier", backupBarrierBody[:]); code != http.StatusForbidden {
		t.Fatalf("wrong backupID backup-barrier status = %d, want 403", code)
	}
	// Complete backup and verify access is immediately revoked.
	if _, err := nodes[controlLeader-1].control.Propose(ctx, ControlCommand{
		CommandVersion: 1, StoreVersion: StoreVersion, Kind: ControlCompleteBackup,
		ExpectedEpoch: 7, MemberID: controlLeader, BackupID: authState.ActiveBackup.BackupID,
	}); err != nil {
		t.Fatalf("complete backup: %v", err)
	}
	_, applied, _ = nodes[controlLeader-1].control.Status()
	if err := (&nodePartitionMembership{node: nodes[controlLeader-1]}).waitControlApplied(ctx, partLeader, applied); err != nil {
		t.Fatalf("waitControlApplied after complete: %v", err)
	}
	binary.BigEndian.PutUint64(backupBarrierBody[8:16], authState.ActiveBackup.BackupID)
	if code := postOperator(leaderClient, nodeOperatorPathPrefix+"1/backup-barrier", backupBarrierBody[:]); code != http.StatusForbidden {
		t.Fatalf("completed backup-barrier status = %d, want 403", code)
	}

	// 5. Full coordinated CreateBackup from control leader must succeed end-to-end.
	for partition := uint16(1); partition < PartitionCount; partition++ {
		if err := nodes[0].members[partition].Campaign(ctx); err != nil {
			t.Fatal(err)
		}
	}
	for partition := uint16(0); partition < PartitionCount; partition++ {
		awaitNodePartitionLeader(t, nodes, partition)
	}
	backupCtx, backupCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer backupCancel()
	manifest, err := nodes[controlLeader-1].CreateBackup(backupCtx, filepath.Join(t.TempDir(), "full-backup"), MinCoreSnapshotChunkBytes)
	if err != nil {
		t.Fatalf("CreateBackup failed: %v", err)
	}
	if len(manifest.Partitions) != PartitionCount {
		t.Fatalf("CreateBackup partitions = %d", len(manifest.Partitions))
	}
}
