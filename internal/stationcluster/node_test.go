package stationcluster

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/nativeapi"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

var _ nativeapi.Station = (*Router)(nil)

func TestStableRouterErrorPreservesRetryAndStationFailure(t *testing.T) {
	err := stableRouterError(ErrNotLeader)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("expected not-leader identity, got %v", err)
	}
	failure, ok := stationcore.IsFailure(err)
	if !ok {
		t.Fatalf("expected station failure, got %v", err)
	}
	if failure.Reason != stationcore.ReasonStorageUnavailable || !failure.Retryable || failure.RetryAfterMs != 100 {
		t.Fatalf("unexpected retry contract: %+v", failure)
	}
}

func TestThreeNodeLoopbackTLSNativeLifecycleFailoverAndMinority(t *testing.T) {
	ca, caKey, roots := newNodeTestCA(t)
	listeners := make([]net.Listener, MinMembers)
	for index := range listeners {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[index] = listener
	}
	members := make([]NodeMemberConfig, MinMembers)
	for index := range members {
		members[index] = NodeMemberConfig{ID: uint64(index + 1), FaultDomain: "zone-" + string(rune('a'+index)), PeerURL: "https://" + listeners[index].Addr().String()}
	}
	nodes := make([]*Node, MinMembers)
	serveErrors := make(chan error, MinMembers)
	for index := range nodes {
		memberID := uint64(index + 1)
		certificate := newNodeTestCertificate(t, ca, caKey, memberID, "cluster-node-test")
		serverTLS, err := PeerServerTLSConfig(certificate, roots)
		if err != nil {
			t.Fatal(err)
		}
		clientTLS := &tls.Config{
			MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
			Certificates: []tls.Certificate{certificate}, RootCAs: roots, NextProtos: []string{"h2"},
		}
		root := t.TempDir()
		config := NodeConfig{
			Schema: NodeConfigSchema, ClusterID: "cluster-node-test", MemberID: memberID, OwnershipEpoch: 5,
			PeerListen: listeners[index].Addr().String(), CoreRoot: filepath.Join(root, "core"), RaftRoot: filepath.Join(root, "raft"),
			TLS:     NodeTLSFiles{CertificateFile: filepath.Join(root, "unused-cert"), PrivateKeyFile: filepath.Join(root, "unused-key"), CAFile: filepath.Join(root, "unused-ca")},
			Members: members,
		}
		node, err := OpenNode(config, serverTLS, clientTLS, clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)})
		if err != nil {
			t.Fatalf("open node %d: %v", memberID, err)
		}
		nodes[index] = node
	}
	for index, node := range nodes {
		listener := listeners[index]
		go func() { serveErrors <- node.ServePeer(listener) }()
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			if node != nil {
				_ = node.Close()
			}
		}
	})

	namespace := "ns_NNNNNNNNNNNNNNNNNNNNNN"
	partition := stationcore.PartitionFor(namespace)
	if err := nodes[0].control.Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	controlLeader := awaitNodeControlLeader(t, nodes, 0)
	if err := nodes[0].members[partition].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	leader := awaitNodePartitionLeader(t, nodes, partition)
	router := nodes[leader-1].Router()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	reserved, err := router.Reserve(ctx, stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_nodeReserve00000001", TTLMs: 3_600_000})
	if err != nil {
		t.Fatal(err)
	}
	submitted, err := router.Submit(ctx, stationcore.SubmitInput{
		NamespaceID: namespace, RequestID: "rq_nodeSubmit000000001", AddressID: reserved.AddressID,
		Payload: []byte("opaque-node"), TTLMs: 300_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := router.Claim(ctx, stationcore.ClaimInput{
		NamespaceID: namespace, RequestID: "rq_nodeClaim0000000001", AddressID: reserved.AddressID,
		MaxUnits: 1, MaxBytes: 1024, VisibilityTimeoutMs: 30_000,
	})
	if err != nil || len(claimed.Units) != 1 || claimed.Units[0].UnitID != submitted.UnitID {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	if _, err := router.Settle(ctx, stationcore.SettleInput{
		NamespaceID: namespace, RequestID: "rq_nodeSettle000000001", ClaimID: claimed.ClaimID, ClaimEpoch: claimed.ClaimEpoch,
		Entries: []stationcore.SettleEntry{{UnitID: submitted.UnitID, Action: stationcore.SettleComplete}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := router.GetCapacity(ctx, stationcore.GetCapacityInput{NamespaceID: namespace}); err != nil {
		t.Fatal(err)
	}
	stopped := leader - 1
	if err := nodes[stopped].Close(); err != nil {
		t.Fatal(err)
	}
	nodes[stopped] = nil
	candidate := 0
	for index, node := range nodes {
		if node != nil {
			candidate = index
			break
		}
	}
	if err := nodes[candidate].members[partition].Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := nodes[candidate].control.Campaign(context.Background()); err != nil {
		t.Fatal(err)
	}
	newLeader := awaitNodePartitionLeaderExcept(t, nodes, partition, leader)
	if newLeader == leader {
		t.Fatal("stopped node remained leader")
	}
	failoverRouter := nodes[newLeader-1].Router()
	if _, err := failoverRouter.GetCapacity(ctx, stationcore.GetCapacityInput{NamespaceID: namespace}); err != nil {
		t.Fatal(err)
	}
	forbiddenControlLeader := uint64(0)
	if controlLeader == leader {
		forbiddenControlLeader = controlLeader
	}
	newControlLeader := awaitNodeControlLeader(t, nodes, forbiddenControlLeader)
	controlState, err := nodes[newControlLeader-1].control.State(ctx)
	if err != nil || controlState.ClusterID != "cluster-node-test" || len(controlState.Placements) != PartitionCount {
		t.Fatalf("control failover state=%+v err=%v", controlState, err)
	}

	for index, node := range nodes {
		if node != nil && uint64(index+1) != newLeader {
			if err := node.Close(); err != nil {
				t.Fatal(err)
			}
			nodes[index] = nil
		}
	}
	minorityCtx, minorityCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer minorityCancel()
	_, err = failoverRouter.Reserve(minorityCtx, stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_nodeMinority0000001", TTLMs: 3_600_000})
	if err == nil {
		t.Fatal("minority mutation committed")
	}
	if _, err := nodes[newLeader-1].control.State(minorityCtx); err == nil {
		t.Fatal("minority control read passed a quorum barrier")
	}
}

func awaitNodeControlLeader(t *testing.T, nodes []*Node, forbidden uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leader uint64
		consistent := true
		for _, node := range nodes {
			if node == nil {
				continue
			}
			candidate, _, fatal := node.control.Status()
			if fatal != nil || candidate == 0 {
				consistent = false
				break
			}
			if leader == 0 {
				leader = candidate
			} else if leader != candidate {
				consistent = false
				break
			}
		}
		if consistent && leader != 0 && leader != forbidden {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("control leader was not elected")
	return 0
}

func awaitNodePartitionLeader(t *testing.T, nodes []*Node, partition uint16) uint64 {
	return awaitNodePartitionLeaderExcept(t, nodes, partition, 0)
}

func awaitNodePartitionLeaderExcept(t *testing.T, nodes []*Node, partition uint16, forbidden uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var leader uint64
		consistent := true
		for _, node := range nodes {
			if node == nil {
				continue
			}
			candidate, _, fatal := node.members[partition].Status()
			if fatal != nil || candidate == 0 {
				consistent = false
				break
			}
			if leader == 0 {
				leader = candidate
			} else if leader != candidate {
				consistent = false
				break
			}
		}
		if consistent && leader != 0 && leader != forbidden {
			return leader
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("partition leader election timed out")
	return 0
}

func newNodeTestCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, *x509.CertPool) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "node-test-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	raw, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.ParseCertificate(raw)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(certificate)
	return certificate, privateKey, roots
}

func newNodeTestCertificate(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, memberID uint64, clusterID string) tls.Certificate {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: new(big.Int).SetUint64(memberID + 10), Subject: pkix.Name{CommonName: "node-peer"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		DNSNames: []string{PeerCertificateDNSName(clusterID, memberID)}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	privateRaw, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: raw}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateRaw}))
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

func TestLoadNodeConfigIsStrictAndRequiresAbsoluteIndependentRoots(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "node.json")
	raw := `{"schema":"badtower.station-cluster.node.v1","clusterId":"cluster-config","memberId":1,"ownershipEpoch":1,"peerListen":"127.0.0.1:9001","coreRoot":"` + filepath.Join(root, "core") + `","raftRoot":"` + filepath.Join(root, "raft") + `","tls":{"certificateFile":"` + filepath.Join(root, "cert") + `","privateKeyFile":"` + filepath.Join(root, "key") + `","caFile":"` + filepath.Join(root, "ca") + `"},"members":[{"id":1,"faultDomain":"a","peerUrl":"https://127.0.0.1:9001"},{"id":2,"faultDomain":"b","peerUrl":"https://127.0.0.1:9002"},{"id":3,"faultDomain":"c","peerUrl":"https://127.0.0.1:9003"}]}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadNodeConfig(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(raw[:len(raw)-1]+`,"unknown":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadNodeConfig(path); err == nil {
		t.Fatal("unknown config field accepted")
	}
}
