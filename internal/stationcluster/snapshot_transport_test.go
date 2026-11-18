package stationcluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

type streamMemoryTransport struct {
	mu      sync.RWMutex
	members map[uint64]*RaftMember
	stores  map[uint64]*streamSnapshotStore
}

func (transport *streamMemoryTransport) Send(ctx context.Context, message *raftpb.Message) error {
	transport.mu.RLock()
	target := transport.members[message.GetTo()]
	sourceStore := transport.stores[message.GetFrom()]
	targetStore := transport.stores[message.GetTo()]
	transport.mu.RUnlock()
	if target == nil {
		return ErrMemberStopped
	}
	copyMessage := proto.Clone(message).(*raftpb.Message)
	if copyMessage.GetType() == raftpb.MessageType_MsgSnap && copyMessage.Snapshot != nil {
		ref, streamed, err := decodeStreamSnapshotReference(copyMessage.Snapshot.Data)
		if err != nil {
			return err
		}
		if streamed {
			var wire bytes.Buffer
			if err = sourceStore.writeTo(ctx, &wire, ref); err != nil {
				return err
			}
			if _, err = targetStore.receive(ctx, &wire, streamSnapshotReceiveExpectation{
				ClusterID:      ref.ClusterID,
				GroupID:        ref.GroupID,
				Partition:      ref.Partition,
				SourceMemberID: message.GetFrom(),
				AppliedIndex:   ref.AppliedIndex,
				OwnershipEpoch: ref.OwnershipEpoch,
			}); err != nil {
				return err
			}
		}
	}
	return target.Step(ctx, copyMessage)
}
func (transport *streamMemoryTransport) set(id uint64, member *RaftMember, store *streamSnapshotStore) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if member == nil {
		delete(transport.members, id)
	} else {
		transport.members[id] = member
	}
	if store != nil {
		transport.stores[id] = store
	}
}

func TestStreamSnapshotOverOneMiBRecoversFollowerTailAndRestart(t *testing.T) {
	transport := &streamMemoryTransport{members: map[uint64]*RaftMember{}, stores: map[uint64]*streamSnapshotStore{}}
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	ids := []uint64{1, 2, 3}
	namespace := "ns_SSSSSSSSSSSSSSSSSSSSSS"
	partition := stationcore.PartitionFor(namespace)
	groupID := uint64(partition) + 1
	members := map[uint64]*RaftMember{}
	cores := map[uint64]*stationcore.Core{}
	roots := map[uint64]string{}
	stores := map[uint64]*streamSnapshotStore{}
	for _, id := range ids {
		core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
		if err != nil {
			t.Fatal(err)
		}
		root := t.TempDir()
		store, err := openStreamSnapshotStore(root)
		if err != nil {
			t.Fatal(err)
		}
		member, err := OpenRaftMember(RaftMemberConfig{ClusterID: "cluster-stream-large", GroupID: groupID, ID: id, Partition: partition, OwnershipEpoch: 3, PeerIDs: ids, RaftRoot: filepath.Join(root, "raft"), Core: core, Transport: transport, TickInterval: 10 * time.Millisecond, SnapshotStore: store})
		if err != nil {
			t.Fatal(err)
		}
		cores[id], roots[id], stores[id], members[id] = core, root, store, member
		transport.set(id, member, store)
	}
	t.Cleanup(func() {
		for _, id := range ids {
			if members[id] != nil {
				_ = members[id].Close()
			}
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
	if leader == 3 {
		lagging = 2
	}
	if err := members[lagging].Close(); err != nil {
		t.Fatal(err)
	}
	members[lagging] = nil
	transport.set(lagging, nil, stores[lagging])
	reserve, err := cores[leader].PrepareCommand(stationcore.OperationReserve, stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_streamReserve000001", TTLMs: 3_600_000}, 3)
	if err != nil {
		t.Fatal(err)
	}
	result, err := members[leader].Propose(context.Background(), reserve)
	if err != nil {
		t.Fatal(err)
	}
	var reserved stationcore.ReserveOutput
	if json.Unmarshal(result.Payload, &reserved) != nil {
		t.Fatal("reserve decode")
	}
	for index := 0; index < 3; index++ {
		payload := make([]byte, 400<<10)
		if _, err = rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		submit, prepareErr := cores[leader].PrepareCommand(stationcore.OperationSubmit, stationcore.SubmitInput{NamespaceID: namespace, RequestID: fmt.Sprintf("rq_streamSubmit%010d", index), AddressID: reserved.AddressID, Payload: payload, TTLMs: 3_600_000}, 3)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		if _, err = members[leader].Propose(context.Background(), submit); err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < raftSnapshotIntervalEntries+4; index++ {
		command, prepareErr := cores[leader].PrepareCommand(stationcore.OperationReserve, stationcore.ReserveInput{NamespaceID: namespace, RequestID: fmt.Sprintf("rq_stream%017d", index), TTLMs: 3_600_000}, 3)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		if _, err = members[leader].Propose(context.Background(), command); err != nil {
			t.Fatal(err)
		}
	}
	leaderSnapshot, snapshotErr := members[leader].storage.Snapshot()
	if snapshotErr != nil {
		t.Fatal(snapshotErr)
	}
	reference, streamed, referenceErr := decodeStreamSnapshotReference(leaderSnapshot.Data)
	if referenceErr != nil || !streamed || reference.TotalBytes <= 1<<20 {
		t.Fatalf("large snapshot reference=%+v streamed=%t err=%v", reference, streamed, referenceErr)
	}
	if err = cores[lagging].Close(); err != nil {
		t.Fatal(err)
	}
	fresh, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
	if err != nil {
		t.Fatal(err)
	}
	cores[lagging] = fresh
	restarted, err := OpenRaftMember(RaftMemberConfig{ClusterID: "cluster-stream-large", GroupID: groupID, ID: lagging, Partition: partition, OwnershipEpoch: 3, PeerIDs: ids, RaftRoot: filepath.Join(roots[lagging], "raft"), Core: fresh, Transport: transport, TickInterval: 10 * time.Millisecond, SnapshotStore: stores[lagging]})
	if err != nil {
		t.Fatal(err)
	}
	members[lagging] = restarted
	transport.set(lagging, restarted, stores[lagging])
	awaitUsage(t, cores, ids, namespace, uint64(raftSnapshotIntervalEntries+5), 3)
	if err = restarted.Close(); err != nil {
		t.Fatal(err)
	}
	members[lagging] = nil
	transport.set(lagging, nil, stores[lagging])
	restarted, err = OpenRaftMember(RaftMemberConfig{ClusterID: "cluster-stream-large", GroupID: groupID, ID: lagging, Partition: partition, OwnershipEpoch: 3, PeerIDs: ids, RaftRoot: filepath.Join(roots[lagging], "raft"), Core: fresh, Transport: transport, TickInterval: 10 * time.Millisecond, SnapshotStore: stores[lagging]})
	if err != nil {
		t.Fatal(err)
	}
	members[lagging] = restarted
	transport.set(lagging, restarted, stores[lagging])
	awaitUsage(t, cores, ids, namespace, uint64(raftSnapshotIntervalEntries+5), 3)
}

func TestStreamSnapshotRejectsTamperTruncateReorderDuplicateAndOverLimit(t *testing.T) {
	source, _ := openStreamSnapshotStore(t.TempDir())
	target, _ := openStreamSnapshotStore(t.TempDir())
	ref, wire := streamSnapshotFixture(t, source)
	tests := map[string][]byte{"truncate": wire[:len(wire)-1], "duplicate": append(append([]byte{}, wire...), 0), "tamper": append([]byte{}, wire...)}
	tests["tamper"][len(tests["tamper"])-1] ^= 0xff
	envelopeSize := int(binary.BigEndian.Uint32(wire[:4]))
	reordered := append([]byte{}, wire...)
	binary.BigEndian.PutUint64(reordered[4+envelopeSize+4:4+envelopeSize+12], 1)
	tests["reorder"] = reordered
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			fresh, _ := openStreamSnapshotStore(t.TempDir())
			if _, err := fresh.receive(context.Background(), bytes.NewReader(raw), streamSnapshotReceiveExpectation{
				ClusterID:      ref.ClusterID,
				GroupID:        ref.GroupID,
				Partition:      ref.Partition,
				SourceMemberID: ref.SourceMemberID,
				AppliedIndex:   ref.AppliedIndex,
				OwnershipEpoch: ref.OwnershipEpoch,
			}); err == nil {
				t.Fatal("invalid stream accepted")
			}
		})
	}
	over := ref
	over.TotalBytes = MaxCoreSnapshotBytes + 1
	if _, err := encodeStreamSnapshotReference(over); err == nil {
		t.Fatal("over-limit reference accepted")
	}
	_ = target
}

func TestStreamSnapshotPeerRouteBindsMTLSSource(t *testing.T) {
	source, _ := openStreamSnapshotStore(t.TempDir())
	target, _ := openStreamSnapshotStore(t.TempDir())
	ref, wire := streamSnapshotFixture(t, source)
	ca, caKey, _ := newNodeTestCA(t)
	certificate := newNodeTestCertificate(t, ca, caKey, 1, ref.ClusterID)
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	controlDB, err := OpenControlMachine(ControlBootstrap{
		Root: filepath.Join(t.TempDir(), "control"), ClusterID: ref.ClusterID,
		PlacementEpoch: ref.OwnershipEpoch, MinCommandVersion: 1, MinStoreVersion: StoreVersion,
		Members: []ControlMember{
			{ID: 1, FaultDomain: "a", Incarnation: 1, CommandVersion: 1, StoreVersion: StoreVersion},
			{ID: 2, FaultDomain: "b", Incarnation: 1, CommandVersion: 1, StoreVersion: StoreVersion},
			{ID: 3, FaultDomain: "c", Incarnation: 1, CommandVersion: 1, StoreVersion: StoreVersion},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	node := &Node{id: 2, clusterID: ref.ClusterID, controlDB: controlDB, config: NodeConfig{Members: []NodeMemberConfig{{ID: 1}, {ID: 2}, {ID: 3}}}, snapshotStore: target}
	server := &nodePeerServer{node: node}
	request := httptest.NewRequest(http.MethodPost, streamSnapshotPathPrefix+"1/"+strconv.FormatUint(ref.AppliedIndex, 10), bytes.NewReader(wire))
	request.ProtoMajor = 2
	request.Header.Set("Content-Type", streamSnapshotContentType)
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: "h2", PeerCertificates: []*x509.Certificate{leaf}, VerifiedChains: [][]*x509.Certificate{{leaf}}}
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("status=%d", response.Code)
	}
	if _, err = target.artifact(ref, true); err != nil {
		t.Fatal(err)
	}
	wrongCertificate := newNodeTestCertificate(t, ca, caKey, 2, ref.ClusterID)
	wrongLeaf, _ := x509.ParseCertificate(wrongCertificate.Certificate[0])
	wrong := httptest.NewRequest(http.MethodPost, streamSnapshotPathPrefix+"1/"+strconv.FormatUint(ref.AppliedIndex, 10), bytes.NewReader(wire))
	wrong.ProtoMajor = 2
	wrong.Header.Set("Content-Type", streamSnapshotContentType)
	wrong.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: "h2", PeerCertificates: []*x509.Certificate{wrongLeaf}, VerifiedChains: [][]*x509.Certificate{{wrongLeaf}}}
	wrongResponse := httptest.NewRecorder()
	server.ServeHTTP(wrongResponse, wrong)
	if wrongResponse.Code == http.StatusNoContent {
		t.Fatal("wrong source certificate accepted")
	}
}

func TestStreamSnapshotPeerRouteRejectsSlowloris(t *testing.T) {
	ca, caKey, roots := newNodeTestCA(t)
	sourceCertificate := newNodeTestCertificate(t, ca, caKey, 1, "cluster-stream-slow")
	serverCertificate := newNodeTestCertificate(t, ca, caKey, 2, "cluster-stream-slow")
	serverTLS, err := PeerServerTLSConfig(serverCertificate, roots)
	if err != nil {
		t.Fatal(err)
	}
	store, _ := openStreamSnapshotStore(t.TempDir())
	handler := &nodePeerServer{node: &Node{id: 2, clusterID: "cluster-stream-slow", config: NodeConfig{Members: []NodeMemberConfig{{ID: 1}, {ID: 2}, {ID: 3}}}, snapshotStore: store}}
	tlsServer := httptest.NewUnstartedServer(handler)
	tlsServer.EnableHTTP2 = true
	tlsServer.TLS = serverTLS
	tlsServer.StartTLS()
	defer tlsServer.Close()
	clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{sourceCertificate}, RootCAs: roots, NextProtos: []string{"h2"}}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: clientTLS, ForceAttemptHTTP2: true}}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	go func() { _, _ = writer.Write([]byte{0}); time.Sleep(7 * time.Second); _ = writer.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, tlsServer.URL+streamSnapshotPathPrefix+"1/1", reader)
	request.Header.Set("Content-Type", streamSnapshotContentType)
	started := time.Now()
	response, requestErr := client.Do(request)
	if response != nil {
		_ = response.Body.Close()
	}
	if requestErr == nil && response.StatusCode == http.StatusNoContent {
		t.Fatal("slow stream accepted")
	}
	if time.Since(started) >= 8*time.Second {
		t.Fatal("slow stream was not cut off by idle deadline")
	}
}

func TestStreamSnapshotAdmissionAndStaleFence(t *testing.T) {
	source, _ := openStreamSnapshotStore(t.TempDir())
	target, _ := openStreamSnapshotStore(t.TempDir())
	ref, wire := streamSnapshotFixture(t, source)
	expected := streamSnapshotReceiveExpectation{
		ClusterID:      ref.ClusterID,
		GroupID:        ref.GroupID,
		Partition:      ref.Partition,
		SourceMemberID: ref.SourceMemberID,
		AppliedIndex:   ref.AppliedIndex,
		OwnershipEpoch: ref.OwnershipEpoch,
	}
	// Mismatched group/partition/epoch envelope must fail before writing or occupying any partition slot.
	wrongGroup := expected
	wrongGroup.GroupID = 2
	wrongGroup.Partition = 1
	if _, err := target.receive(context.Background(), bytes.NewReader(wire), wrongGroup); err != ErrInvalidSnapshot {
		t.Fatalf("mismatched group err=%v, want ErrInvalidSnapshot", err)
	}
	if entries, err := os.ReadDir(filepath.Join(target.root, "received")); err != nil || len(entries) != 0 {
		t.Fatalf("rejected mismatched snapshot persisted entries=%d err=%v", len(entries), err)
	}
	target.admission <- struct{}{}
	target.admission <- struct{}{}
	if _, err := target.receive(context.Background(), bytes.NewReader(wire), expected); err != ErrPeerBackpressure {
		t.Fatalf("admission err=%v", err)
	}
	<-target.admission
	<-target.admission
	if _, err := target.receive(context.Background(), bytes.NewReader(wire), expected); err != nil {
		t.Fatal(err)
	}
	data, _ := encodeStreamSnapshotReference(ref)
	snapshot := &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: &ref.AppliedIndex, Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
	core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: ref.Partition}, clusterClock{now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	config := RaftMemberConfig{ClusterID: ref.ClusterID, GroupID: ref.GroupID, Partition: ref.Partition, OwnershipEpoch: ref.OwnershipEpoch, SnapshotStore: target}
	if err = installRaftCoreSnapshot(core, snapshot, config, map[uint64]struct{}{1: {}}, ref.AppliedIndex); err == nil {
		t.Fatal("stale snapshot accepted")
	}
}

func streamSnapshotFixture(t *testing.T, store *streamSnapshotStore) (streamSnapshotReference, []byte) {
	t.Helper()
	core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: 0}, clusterClock{now: time.Now()})
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	binding := CoreSnapshotBinding{ClusterID: "cluster-stream-codec", GroupID: 1, SourceMemberID: 1, Partition: 0, OwnershipEpoch: 1, AppliedIndex: 2}
	placeholder := streamSnapshotReference{Format: streamSnapshotFormat, ClusterID: binding.ClusterID, GroupID: 1, SourceMemberID: 1, Partition: 0, OwnershipEpoch: 1, AppliedIndex: 2, TotalBytes: 1, DigestSHA256: strings.Repeat("0", 64)}
	root := store.sourceRoot(placeholder)
	manifest, err := createCoreSnapshot(core, root, binding, MaxCoreSnapshotChunkBytes, MaxCoreSnapshotBytes)
	if err != nil {
		t.Fatal(err)
	}
	ref := referenceForManifest(manifest)
	var wire bytes.Buffer
	if err = store.writeTo(context.Background(), &wire, ref); err != nil {
		t.Fatal(err)
	}
	return ref, wire.Bytes()
}
