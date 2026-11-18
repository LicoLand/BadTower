package stationcluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/nativeapi"
	"github.com/LicoLand/BadTower/internal/stationcore"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var _ nativeapi.Station = (*MemberStation)(nil)

type memoryPeerTransport struct {
	mu      sync.RWMutex
	members map[uint64]*RaftMember
	drop    bool
}

func newMemoryPeerTransport() *memoryPeerTransport {
	return &memoryPeerTransport{members: make(map[uint64]*RaftMember)}
}

func (transport *memoryPeerTransport) Send(ctx context.Context, message *raftpb.Message) error {
	transport.mu.RLock()
	target := transport.members[message.GetTo()]
	drop := transport.drop
	transport.mu.RUnlock()
	if target == nil || drop {
		return ErrMemberStopped
	}
	return target.Step(ctx, proto.Clone(message).(*raftpb.Message))
}

func (transport *memoryPeerTransport) setDrop(drop bool) {
	transport.mu.Lock()
	transport.drop = drop
	transport.mu.Unlock()
}

func (transport *memoryPeerTransport) set(id uint64, member *RaftMember) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if member == nil {
		delete(transport.members, id)
		return
	}
	transport.members[id] = member
}

func TestRaftMemberPersistsReplicatesFailsOverAndReplays(t *testing.T) {
	transport := newMemoryPeerTransport()
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	ids := []uint64{1, 2, 3}
	cores := make(map[uint64]*stationcore.Core, len(ids))
	raftRoots := make(map[uint64]string, len(ids))
	members := make(map[uint64]*RaftMember, len(ids))
	for _, id := range ids {
		core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1}, clock)
		if err != nil {
			t.Fatal(err)
		}
		cores[id] = core
		raftRoots[id] = t.TempDir()
		member, err := OpenRaftMember(RaftMemberConfig{
			ClusterID: "cluster-member-runtime", GroupID: 1,
			Partition: 0, OwnershipEpoch: 1,
			ID: id, PeerIDs: ids, RaftRoot: raftRoots[id], Core: core,
			Transport: transport, TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		members[id] = member
		transport.set(id, member)
	}
	t.Cleanup(func() {
		for id, member := range members {
			if member != nil {
				_ = member.Close()
				transport.set(id, nil)
			}
		}
		for _, core := range cores {
			_ = core.Close()
		}
	})

	if err := members[1].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	leader := awaitLeader(t, members, 1)
	namespace := "ns_RRRRRRRRRRRRRRRRRRRRRR"
	reserve := stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_memberReserve00001", TTLMs: 3_600_000}
	command, err := cores[leader].PrepareCommand(stationcore.OperationReserve, reserve, 1)
	if err != nil {
		t.Fatal(err)
	}
	result, err := members[leader].Propose(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	var reserved stationcore.ReserveOutput
	if err := json.Unmarshal(result.Payload, &reserved); err != nil || reserved.AddressID == "" {
		t.Fatalf("reserve=%s err=%v", result.Payload, err)
	}
	awaitUsage(t, cores, ids, namespace, 1, 0)
	if err := members[leader].ReadBarrier(context.Background()); err != nil {
		t.Fatalf("leader read barrier: %v", err)
	}
	for _, id := range ids {
		if id != leader {
			if err := members[id].ReadBarrier(context.Background()); err != ErrNotLeader {
				t.Fatalf("follower read barrier err=%v", err)
			}
			break
		}
	}

	longClaim := stationcore.ClaimInput{
		NamespaceID: namespace, RequestID: "rq_memberClaim0000001", AddressID: reserved.AddressID,
		MaxUnits: 1, MaxBytes: 1024, VisibilityTimeoutMs: 30_000, WaitTimeoutMs: 20_000,
	}
	command, err = cores[leader].PrepareCommand(stationcore.OperationClaim, longClaim, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	claimResult, err := members[leader].Propose(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	var emptyClaim stationcore.ClaimOutput
	if err := json.Unmarshal(claimResult.Payload, &emptyClaim); err != nil || len(emptyClaim.Units) != 0 {
		t.Fatalf("empty claim=%s err=%v", claimResult.Payload, err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("replicated long claim blocked apply for %s", elapsed)
	}

	// A stopped follower misses one committed command. Reopening from its
	// durable Raft root replays only the missing entry into its durable Core.
	lagging := uint64(3)
	if lagging == leader {
		lagging = 2
	}
	if err := members[lagging].Close(); err != nil {
		t.Fatal(err)
	}
	transport.set(lagging, nil)
	members[lagging] = nil
	submit := stationcore.SubmitInput{
		NamespaceID: namespace, RequestID: "rq_memberSubmit000001", AddressID: reserved.AddressID,
		Payload: []byte("opaque"), TTLMs: 300_000,
	}
	command, err = cores[leader].PrepareCommand(stationcore.OperationSubmit, submit, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := members[leader].Propose(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	// Restart the entire surviving quorum before the lagging member returns.
	// The newly elected leader must retain committed entries on disk and repair
	// the old follower; an index-only synthetic snapshot would fail this step.
	for _, id := range ids {
		if id == lagging {
			continue
		}
		if err := members[id].Close(); err != nil {
			t.Fatal(err)
		}
		transport.set(id, nil)
		members[id] = nil
	}
	for _, id := range ids {
		if id == lagging {
			continue
		}
		restarted, err := OpenRaftMember(RaftMemberConfig{
			ClusterID: "cluster-member-runtime", GroupID: 1,
			Partition: 0, OwnershipEpoch: 1,
			ID: id, PeerIDs: ids, RaftRoot: raftRoots[id], Core: cores[id],
			Transport: transport, TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		members[id] = restarted
		transport.set(id, restarted)
	}
	for _, id := range ids {
		if members[id] != nil {
			_ = members[id].Campaign(context.Background())
			break
		}
	}
	leader = awaitLeader(t, members, 0)
	restarted, err := OpenRaftMember(RaftMemberConfig{
		ClusterID: "cluster-member-runtime", GroupID: 1,
		Partition: 0, OwnershipEpoch: 1,
		ID: lagging, PeerIDs: ids, RaftRoot: raftRoots[lagging], Core: cores[lagging],
		Transport: transport, TickInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	members[lagging] = restarted
	transport.set(lagging, restarted)
	awaitUsage(t, cores, ids, namespace, 1, 1)

	// Losing the leader leaves two persisted voters. They elect a replacement;
	// the old leader can later reopen and converge without accepting minority work.
	if err := members[leader].Close(); err != nil {
		t.Fatal(err)
	}
	transport.set(leader, nil)
	members[leader] = nil
	for _, id := range ids {
		if members[id] != nil {
			_ = members[id].Campaign(context.Background())
			break
		}
	}
	newLeader := awaitLeader(t, members, 0)
	if newLeader == leader {
		t.Fatal("stopped member remained leader")
	}
}

func TestRaftMemberCancellationCleansBoundedPendingWaiters(t *testing.T) {
	transport := newMemoryPeerTransport()
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	ids := []uint64{1, 2, 3}
	members := make(map[uint64]*RaftMember, len(ids))
	cores := make(map[uint64]*stationcore.Core, len(ids))
	for _, id := range ids {
		core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1}, clock)
		if err != nil {
			t.Fatal(err)
		}
		cores[id] = core
		member, err := OpenRaftMember(RaftMemberConfig{
			ClusterID: "cluster-cancel", GroupID: 1, ID: id, PeerIDs: ids,
			Partition: 0, OwnershipEpoch: 1,
			RaftRoot: t.TempDir(), Core: core, Transport: transport, TickInterval: 100 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		members[id] = member
		transport.set(id, member)
	}
	t.Cleanup(func() {
		transport.setDrop(false)
		for id, member := range members {
			_ = member.Close()
			transport.set(id, nil)
			_ = cores[id].Close()
		}
	})
	if err := members[1].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	leader := awaitLeader(t, members, 1)
	command, err := cores[leader].PrepareCommand(stationcore.OperationReserve, stationcore.ReserveInput{
		NamespaceID: "ns_CCCCCCCCCCCCCCCCCCCCCC", RequestID: "rq_cancelPending0000001", TTLMs: 60_000,
	}, 1)
	if err != nil {
		t.Fatal(err)
	}
	transport.setDrop(true)
	for index := 0; index < maxPendingProposals+8; index++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		_, proposeErr := members[leader].Propose(ctx, command)
		cancel()
		if proposeErr != context.DeadlineExceeded {
			t.Fatalf("proposal %d err=%v", index, proposeErr)
		}
	}
}

func TestRaftMemberEmptyReplicaRecoversFromCoreSnapshotAndBoundedTail(t *testing.T) {
	transport := newMemoryPeerTransport()
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	ids := []uint64{1, 2, 3}
	namespace := "ns_KKKKKKKKKKKKKKKKKKKKKK"
	partition := stationcore.PartitionFor(namespace)
	groupID := uint64(partition) + 1
	cores := make(map[uint64]*stationcore.Core, len(ids))
	members := make(map[uint64]*RaftMember, len(ids))
	raftRoots := make(map[uint64]string, len(ids))
	for _, id := range ids {
		core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
		if err != nil {
			t.Fatal(err)
		}
		cores[id] = core
		raftRoots[id] = t.TempDir()
		member, err := OpenRaftMember(RaftMemberConfig{
			ClusterID: "cluster-snapshot-recovery", GroupID: groupID, Partition: partition, OwnershipEpoch: 4,
			ID: id, PeerIDs: ids, RaftRoot: raftRoots[id], Core: core,
			Transport: transport, TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		members[id] = member
		transport.set(id, member)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			if members[id] != nil {
				_ = members[id].Close()
			}
			transport.set(id, nil)
			if cores[id] != nil {
				_ = cores[id].Close()
			}
		}
	})

	if err := members[1].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	leader := awaitLeader(t, members, 1)
	lagging := uint64(3)
	if lagging == leader {
		lagging = 2
	}
	if err := members[lagging].Close(); err != nil {
		t.Fatal(err)
	}
	members[lagging] = nil
	transport.set(lagging, nil)
	if err := cores[lagging].Close(); err != nil {
		t.Fatal(err)
	}
	emptyCore, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
	if err != nil {
		t.Fatal(err)
	}
	cores[lagging] = emptyCore

	const addressCount = 40
	for index := 0; index < addressCount; index++ {
		input := stationcore.ReserveInput{
			NamespaceID: namespace, RequestID: fmt.Sprintf("rq_snap%018d", index), TTLMs: 3_600_000,
		}
		command, err := cores[leader].PrepareCommand(stationcore.OperationReserve, input, 4)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := members[leader].Propose(context.Background(), command); err != nil {
			t.Fatalf("proposal %d: %v", index, err)
		}
	}

	recovered, err := OpenRaftMember(RaftMemberConfig{
		ClusterID: "cluster-snapshot-recovery", GroupID: groupID, Partition: partition, OwnershipEpoch: 4,
		ID: lagging, PeerIDs: ids, RaftRoot: raftRoots[lagging], Core: cores[lagging],
		Transport: transport, TickInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	members[lagging] = recovered
	transport.set(lagging, recovered)
	awaitAddressUsage(t, cores, ids, namespace, addressCount)

	for _, id := range ids {
		if err := members[id].Close(); err != nil {
			t.Fatal(err)
		}
		members[id] = nil
		transport.set(id, nil)
	}
	for _, id := range ids {
		restarted, err := OpenRaftMember(RaftMemberConfig{
			ClusterID: "cluster-snapshot-recovery", GroupID: groupID, Partition: partition, OwnershipEpoch: 4,
			ID: id, PeerIDs: ids, RaftRoot: raftRoots[id], Core: cores[id],
			Transport: transport, TickInterval: 10 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("restart member %d: %v", id, err)
		}
		members[id] = restarted
		transport.set(id, restarted)
	}
	if err := members[1].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	leader = awaitLeader(t, members, 0)
	awaitAddressUsage(t, cores, ids, namespace, addressCount)
	replayInput := stationcore.ReserveInput{
		NamespaceID: namespace, RequestID: fmt.Sprintf("rq_snap%018d", 0), TTLMs: 3_600_000,
	}
	replayCommand, err := cores[leader].PrepareCommand(stationcore.OperationReserve, replayInput, 4)
	if err != nil {
		t.Fatal(err)
	}
	replayResult, err := members[leader].Propose(context.Background(), replayCommand)
	if err != nil {
		t.Fatal(err)
	}
	var replayed stationcore.ReserveOutput
	if err := json.Unmarshal(replayResult.Payload, &replayed); err != nil || !replayed.Created || replayed.AddressID == "" {
		t.Fatalf("snapshot-restored idempotency differs: output=%s err=%v", replayResult.Payload, err)
	}
	awaitAddressUsage(t, cores, ids, namespace, addressCount)
	for _, id := range ids {
		count, err := members[id].log.entryCount()
		if err != nil {
			t.Fatal(err)
		}
		if count >= raftSnapshotIntervalEntries {
			t.Fatalf("member %d retained %d entries, ceiling is %d", id, count, raftSnapshotIntervalEntries-1)
		}
	}
}

func awaitAddressUsage(t *testing.T, cores map[uint64]*stationcore.Core, ids []uint64, namespace string, addresses uint64) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		complete := true
		for _, id := range ids {
			capacity, err := cores[id].GetCapacity(context.Background(), stationcore.GetCapacityInput{NamespaceID: namespace})
			if err != nil || capacity.NamespaceUsage.Addresses != addresses {
				complete = false
				break
			}
		}
		if complete {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("snapshot-restored address usage did not converge")
}

func TestRaftCoreSnapshotInstallRejectsStaleForeignAndCorruptData(t *testing.T) {
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	namespace := "ns_LLLLLLLLLLLLLLLLLLLLLL"
	partition := stationcore.PartitionFor(namespace)
	groupID := uint64(partition) + 1
	source, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Reserve(context.Background(), stationcore.ReserveInput{
		NamespaceID: namespace, RequestID: "rq_raftSnapshotInstall001", TTLMs: 3_600_000,
	}); err != nil {
		t.Fatal(err)
	}
	artifactRoot := filepath.Join(t.TempDir(), "artifact")
	if _, err := createCoreSnapshot(source, artifactRoot, CoreSnapshotBinding{
		ClusterID: "cluster-install-negative", GroupID: groupID, SourceMemberID: 1,
		Partition: partition, OwnershipEpoch: 3, AppliedIndex: 40,
	}, raftSnapshotChunkBytes, maxRaftSnapshotArtifactBytes); err != nil {
		t.Fatal(err)
	}
	data, _, err := marshalCoreSnapshotArtifact(artifactRoot, maxRaftSnapshotDataBytes)
	if err != nil {
		t.Fatal(err)
	}
	term, index := uint64(2), uint64(40)
	confState := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	makeSnapshot := func(payload []byte, snapshotIndex uint64) *raftpb.Snapshot {
		return &raftpb.Snapshot{Data: payload, Metadata: &raftpb.SnapshotMetadata{Index: &snapshotIndex, Term: &term, ConfState: confState}}
	}
	config := RaftMemberConfig{ClusterID: "cluster-install-negative", GroupID: groupID, ID: 2, Partition: partition, OwnershipEpoch: 3}
	wrongPartition := (partition + 1) % PartitionCount
	peers := map[uint64]struct{}{1: {}, 2: {}, 3: {}}

	tests := []struct {
		name    string
		payload []byte
		index   uint64
		config  RaftMemberConfig
		peers   map[uint64]struct{}
		applied uint64
	}{
		{name: "stale", payload: data, index: index, config: config, peers: peers, applied: index},
		{name: "metadata-index-mismatch", payload: data, index: index + 1, config: config, peers: peers},
		{name: "foreign-source-member", payload: data, index: index, config: config, peers: map[uint64]struct{}{2: {}, 3: {}}},
		{name: "wrong-group", payload: data, index: index, config: RaftMemberConfig{ClusterID: config.ClusterID, GroupID: groupID + 1, ID: 2, Partition: partition, OwnershipEpoch: 3}, peers: peers},
		{name: "wrong-partition", payload: data, index: index, config: RaftMemberConfig{ClusterID: config.ClusterID, GroupID: groupID, ID: 2, Partition: wrongPartition, OwnershipEpoch: 3}, peers: peers},
		{name: "wrong-epoch", payload: data, index: index, config: RaftMemberConfig{ClusterID: config.ClusterID, GroupID: groupID, ID: 2, Partition: partition, OwnershipEpoch: 4}, peers: peers},
	}
	corrupt := append([]byte(nil), data...)
	corrupt[len(corrupt)-1] ^= 0x80
	tests = append(tests, struct {
		name    string
		payload []byte
		index   uint64
		config  RaftMemberConfig
		peers   map[uint64]struct{}
		applied uint64
	}{name: "corrupt", payload: corrupt, index: index, config: config, peers: peers})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			if err := installRaftCoreSnapshot(target, makeSnapshot(test.payload, test.index), test.config, test.peers, test.applied); err == nil {
				t.Fatal("invalid Raft snapshot accepted")
			}
			capacity, err := target.GetCapacity(context.Background(), stationcore.GetCapacityInput{NamespaceID: namespace})
			if err != nil || capacity.NamespaceUsage.Addresses != 0 {
				t.Fatalf("invalid snapshot changed Core: usage=%+v err=%v", capacity.NamespaceUsage, err)
			}
		})
	}
}

func TestStepRejectsOversizedMessageBeforeQueue(t *testing.T) {
	member := &RaftMember{id: 1, done: make(chan struct{}), incoming: make(chan *raftpb.Message, 1)}
	message := &raftpb.Message{From: new(uint64(2)), To: new(uint64(1)), Context: make([]byte, maxPeerMessageBytes+1)}
	if err := member.Step(context.Background(), message); err != ErrProposalTooLarge {
		t.Fatalf("oversized Step err=%v", err)
	}
	if len(member.incoming) != 0 {
		t.Fatal("oversized message reached queue")
	}
}

func TestPeerTransportRequiresMTLSHTTP2AndBoundsBeforeStep(t *testing.T) {
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	transport := newMemoryPeerTransport()
	member, err := OpenRaftMember(RaftMemberConfig{
		ClusterID: "cluster-peer-transport", GroupID: 1,
		Partition: 0, OwnershipEpoch: 1,
		ID: 1, PeerIDs: []uint64{1, 2, 3}, RaftRoot: t.TempDir(), Core: core,
		Transport: transport, TickInterval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer member.Close()
	transport.set(1, member)

	message := &raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: new(uint64(2)), To: new(uint64(1))}
	raw, err := proto.Marshal(message)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "https://member.test"+peerMessagePath, bytes.NewReader(raw))
	request.ProtoMajor = 2
	request.ContentLength = int64(len(raw))
	request.Header.Set("Content-Type", peerMessageContentType)
	request.TLS = &tls.ConnectionState{
		Version: tls.VersionTLS13, NegotiatedProtocol: "h2",
		PeerCertificates: []*x509.Certificate{{DNSNames: []string{PeerCertificateDNSName("cluster-peer-transport", 2)}}}, VerifiedChains: [][]*x509.Certificate{{{}}},
	}
	recorder := httptest.NewRecorder()
	NewPeerHandler(member).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("valid peer status=%d", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "https://member.test"+peerMessagePath, bytes.NewReader(raw))
	request.ProtoMajor = 2
	request.ContentLength = int64(len(raw))
	request.Header.Set("Content-Type", peerMessageContentType)
	request.TLS = &tls.ConnectionState{
		Version: tls.VersionTLS13, NegotiatedProtocol: "h2",
		PeerCertificates: []*x509.Certificate{{DNSNames: []string{PeerCertificateDNSName("cluster-peer-transport", 3)}}}, VerifiedChains: [][]*x509.Certificate{{{}}},
	}
	recorder = httptest.NewRecorder()
	NewPeerHandler(member).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("wrong peer identity status=%d", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "https://member.test"+peerMessagePath, bytes.NewReader(raw))
	request.ProtoMajor = 2
	request.ContentLength = int64(len(raw))
	request.Header.Set("Content-Type", peerMessageContentType)
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS12, NegotiatedProtocol: "h2"}
	recorder = httptest.NewRecorder()
	NewPeerHandler(member).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("weak peer carrier status=%d", recorder.Code)
	}

	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{{Certificate: [][]byte{{1}}}},
		RootCAs:      x509.NewCertPool(), NextProtos: []string{"h2"},
	}
	httpTransport, err := NewHTTPPeerTransport(map[uint64]string{
		1: "https://member-1.test", 2: "https://member-2.test", 3: "https://member-3.test",
	}, clientTLS)
	if err != nil {
		t.Fatal(err)
	}
	httpTransport.CloseIdleConnections()
	clientTLS.MinVersion = tls.VersionTLS12
	if _, err := NewHTTPPeerTransport(map[uint64]string{
		1: "https://member-1.test", 2: "https://member-2.test", 3: "https://member-3.test",
	}, clientTLS); err != ErrInvalidTopology {
		t.Fatalf("weak client TLS err=%v", err)
	}
}

func awaitLeader(t *testing.T, members map[uint64]*RaftMember, preferred uint64) uint64 {
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
	t.Fatal("leader election timed out")
	return 0
}

func awaitUsage(t *testing.T, cores map[uint64]*stationcore.Core, ids []uint64, namespace string, addresses, units uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		matched := true
		for _, id := range ids {
			capacity, err := cores[id].GetCapacity(context.Background(), stationcore.GetCapacityInput{NamespaceID: namespace})
			if err != nil || capacity.NamespaceUsage.Addresses != addresses || capacity.NamespaceUsage.Units != units {
				matched = false
				break
			}
		}
		if matched {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("usage did not converge to addresses=%d units=%d", addresses, units)
}
