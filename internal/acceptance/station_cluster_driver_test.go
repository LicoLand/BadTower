//go:build station_cluster_acceptance

package acceptance

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcluster"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

var (
	errInvalidClusterProfile  = errors.New("invalid cluster capacity profile")
	errInvalidClusterEvidence = errors.New("invalid cluster lifecycle evidence")
)

type clusterCapacityProfile struct {
	Schema    string `json:"schema"`
	ProfileID string `json:"profileId"`
	Status    string `json:"status"`
	Topology  struct {
		BootstrapMembers  int `json:"bootstrapMembers"`
		MaximumMembers    int `json:"maximumMembers"`
		FaultDomains      int `json:"faultDomains"`
		Partitions        int `json:"partitions"`
		ReplicationFactor int `json:"replicationFactor"`
	} `json:"topology"`
	ConsensusBounds struct {
		HeartbeatTicks            int `json:"heartbeatTicks"`
		ElectionTicks             int `json:"electionTicks"`
		TickMilliseconds          int `json:"tickMilliseconds"`
		MaxProposalBytes          int `json:"maxProposalBytes"`
		MaxMessageBytes           int `json:"maxMessageBytes"`
		MaxUncommittedEntryBytes  int `json:"maxUncommittedEntryBytes"`
		MaxInflightMessages       int `json:"maxInflightMessages"`
		MaxInflightBytes          int `json:"maxInflightBytes"`
		PeerQueueDepth            int `json:"peerQueueDepth"`
		ProposalQueueDepth        int `json:"proposalQueueDepth"`
		SnapshotStreamChunkBytes  int `json:"snapshotStreamChunkBytes"`
		SnapshotStreamMaxBytes    int `json:"snapshotStreamMaxBytes"`
		SnapshotStreamConcurrency int `json:"snapshotStreamConcurrency"`
	} `json:"consensusBounds"`
	Carrier struct {
		Transport            string `json:"transport"`
		HTTPMajor            int    `json:"httpMajor"`
		TLSMinimum           string `json:"tlsMinimum"`
		TLSMaximum           string `json:"tlsMaximum"`
		MutualAuthentication bool   `json:"mutualAuthentication"`
	} `json:"carrier"`
	Focused struct {
		EnabledByDefault                bool     `json:"enabledByDefault"`
		MaximumDurationSeconds          int      `json:"maximumDurationSeconds"`
		MinimumNativeMutations          int      `json:"minimumNativeMutations"`
		MinimumColdNamespaceCompletions int      `json:"minimumColdNamespaceCompletions"`
		MaximumFailoverRTOMilliseconds  int      `json:"maximumFailoverRtoMilliseconds"`
		CommittedWriteRPO               int      `json:"committedWriteRpo"`
		RequiredScenarios               []string `json:"requiredScenarios"`
	} `json:"focused"`
	Load struct {
		Run                      string  `json:"run"`
		OptInEnvironment         string  `json:"optInEnvironment"`
		DurationMinutes          int     `json:"durationMinutes"`
		Clients                  int     `json:"clients"`
		Namespaces               int     `json:"namespaces"`
		HotPartitionTrafficShare float64 `json:"hotPartitionTrafficShare"`
		PayloadKiB               []int   `json:"payloadKiB"`
	} `json:"load"`
	Soak struct {
		Run              string   `json:"run"`
		OptInEnvironment string   `json:"optInEnvironment"`
		DurationMinutes  int      `json:"durationMinutes"`
		TargetLoadShare  float64  `json:"targetLoadShare"`
		Faults           []string `json:"faults"`
	} `json:"soak"`
	Evidence struct {
		Schema              string `json:"schema"`
		Privacy             string `json:"privacy"`
		FocusedRun          string `json:"focusedRun"`
		LoadRun             string `json:"loadRun"`
		SoakRun             string `json:"soakRun"`
		FormalCapacityClaim bool   `json:"formalCapacityClaim"`
	} `json:"evidence"`
}

func loadClusterCapacityProfile(t *testing.T) clusterCapacityProfile {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "docs", "capacity", "cluster-general-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var profile clusterCapacityProfile
	if decoder.Decode(&profile) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateClusterCapacityProfile(profile) != nil {
		t.Fatal(errInvalidClusterProfile)
	}
	return profile
}

func validateClusterCapacityProfile(profile clusterCapacityProfile) error {
	if profile.Schema != "badtower.cluster-capacity-profile.v1" || profile.ProfileID != "cluster-general-v1" || profile.Status != "candidate" ||
		profile.Topology.BootstrapMembers != 3 || profile.Topology.MaximumMembers != 4 || profile.Topology.FaultDomains != 4 || profile.Topology.Partitions != stationcluster.PartitionCount || profile.Topology.ReplicationFactor != 3 ||
		profile.ConsensusBounds.HeartbeatTicks != 1 || profile.ConsensusBounds.ElectionTicks != 10 || profile.ConsensusBounds.TickMilliseconds != 100 ||
		profile.ConsensusBounds.MaxProposalBytes != 2<<20 || profile.ConsensusBounds.MaxMessageBytes != 1<<20 || profile.ConsensusBounds.MaxUncommittedEntryBytes != 8<<20 ||
		profile.ConsensusBounds.MaxInflightMessages != 64 || profile.ConsensusBounds.MaxInflightBytes != 8<<20 || profile.ConsensusBounds.PeerQueueDepth != 256 || profile.ConsensusBounds.ProposalQueueDepth != 64 ||
		profile.ConsensusBounds.SnapshotStreamChunkBytes != 64<<10 || profile.ConsensusBounds.SnapshotStreamMaxBytes != 1<<30 || profile.ConsensusBounds.SnapshotStreamConcurrency != 2 ||
		profile.Carrier.Transport != "https" || profile.Carrier.HTTPMajor != 2 || profile.Carrier.TLSMinimum != "1.3" || profile.Carrier.TLSMaximum != "1.3" || !profile.Carrier.MutualAuthentication ||
		!profile.Focused.EnabledByDefault || profile.Focused.MaximumDurationSeconds < 300 || profile.Focused.MaximumDurationSeconds > 600 || profile.Focused.MinimumNativeMutations < 32 || profile.Focused.MinimumColdNamespaceCompletions < 1 || profile.Focused.MaximumFailoverRTOMilliseconds <= 0 || profile.Focused.CommittedWriteRPO != 0 || len(profile.Focused.RequiredScenarios) < 10 ||
		profile.Load.Run != "not-run" || profile.Load.OptInEnvironment != "BADTOWER_CLUSTER_LOAD" || profile.Load.DurationMinutes < 30 || profile.Load.Clients <= 0 || profile.Load.Namespaces != stationcluster.PartitionCount || profile.Load.HotPartitionTrafficShare <= 0 || profile.Load.HotPartitionTrafficShare >= 1 || len(profile.Load.PayloadKiB) == 0 ||
		profile.Soak.Run != "not-run" || profile.Soak.OptInEnvironment != "BADTOWER_CLUSTER_SOAK" || profile.Soak.DurationMinutes < 120 || profile.Soak.TargetLoadShare <= 0 || profile.Soak.TargetLoadShare >= 1 || len(profile.Soak.Faults) < 7 ||
		profile.Evidence.Schema != clusterEvidenceSchema || profile.Evidence.Privacy != "aggregate-only" || profile.Evidence.FocusedRun != "source-gate" || profile.Evidence.LoadRun != "not-run" || profile.Evidence.SoakRun != "not-run" || profile.Evidence.FormalCapacityClaim {
		return errInvalidClusterProfile
	}
	return nil
}

type clusterLifecycleDriver struct {
	t         *testing.T
	clusterID string
	clock     clusterAcceptanceClock
	nodes     []*stationcluster.Node
	listeners []net.Listener
	configs   []stationcluster.NodeConfig
}

func newClusterLifecycleDriver(t *testing.T) *clusterLifecycleDriver {
	t.Helper()
	const clusterID = "cluster-acceptance-v1"
	ca, caKey, roots := newClusterAcceptanceCA(t)
	driver := &clusterLifecycleDriver{t: t, clusterID: clusterID, clock: clusterAcceptanceClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}}
	registry := make([]stationcluster.NodeMemberConfig, 4)
	for index := range registry {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		driver.listeners = append(driver.listeners, listener)
		registry[index] = stationcluster.NodeMemberConfig{ID: uint64(index + 1), FaultDomain: fmt.Sprintf("fd-%d", index+1), PeerURL: "https://" + listener.Addr().String()}
	}
	bootstrap := append([]stationcluster.NodeMemberConfig(nil), registry[:3]...)
	for index := range registry {
		root := t.TempDir()
		certificate := newClusterAcceptanceCertificate(t, ca, caKey, uint64(index+1), clusterID)
		serverTLS, err := stationcluster.PeerServerTLSConfig(certificate, roots)
		if err != nil {
			t.Fatal(err)
		}
		clientTLS := &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate}, RootCAs: roots, NextProtos: []string{"h2"}}
		config := stationcluster.NodeConfig{Schema: stationcluster.NodeConfigSchema, ClusterID: clusterID, MemberID: uint64(index + 1), OwnershipEpoch: 1, PeerListen: driver.listeners[index].Addr().String(), CoreRoot: filepath.Join(root, "core"), RaftRoot: filepath.Join(root, "raft"), TLS: stationcluster.NodeTLSFiles{CertificateFile: filepath.Join(root, "cert"), PrivateKeyFile: filepath.Join(root, "key"), CAFile: filepath.Join(root, "ca")}, Members: registry, BootstrapMembers: bootstrap, Join: index == 3}
		node, err := stationcluster.OpenNode(config, serverTLS, clientTLS, driver.clock)
		if err != nil {
			t.Fatalf("open cluster member failed: %v", err)
		}
		driver.nodes = append(driver.nodes, node)
		driver.configs = append(driver.configs, config)
	}
	for index, node := range driver.nodes {
		go func(node *stationcluster.Node, listener net.Listener) { _ = node.ServePeer(listener) }(node, driver.listeners[index])
	}
	t.Cleanup(driver.close)
	return driver
}

func (driver *clusterLifecycleDriver) close() {
	for _, node := range driver.nodes {
		if node != nil {
			_ = node.Close()
		}
	}
	for _, listener := range driver.listeners {
		_ = listener.Close()
	}
}

func (driver *clusterLifecycleDriver) campaignAndOperator(ctx context.Context) *stationcluster.Operator {
	driver.t.Helper()
	if err := driver.nodes[0].CampaignAll(ctx); err != nil {
		driver.t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, node := range driver.nodes[:3] {
			operator, err := stationcluster.NewOperator(node, node.PartitionMembership(), 1, stationcluster.StoreVersion)
			if err != nil {
				continue
			}
			status, err := operator.Status(ctx)
			if err == nil && status.LeaderMemberID == node.ID() && status.AppliedIndex > 0 {
				return operator
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	driver.t.Fatal("control leader election timed out")
	return nil
}

func (driver *clusterLifecycleDriver) callReserve(ctx context.Context, active []int, input stationcore.ReserveInput) stationcore.ReserveOutput {
	driver.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, index := range active {
			if driver.nodes[index] == nil {
				continue
			}
			if output, err := driver.nodes[index].Router().Reserve(ctx, input); err == nil {
				return output
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	driver.t.Fatal("reserve did not reach a quorum leader")
	return stationcore.ReserveOutput{}
}

func (driver *clusterLifecycleDriver) callSubmit(ctx context.Context, active []int, input stationcore.SubmitInput) stationcore.SubmitOutput {
	driver.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, index := range active {
			if driver.nodes[index] == nil {
				continue
			}
			if output, err := driver.nodes[index].Router().Submit(ctx, input); err == nil {
				return output
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	driver.t.Fatal("submit did not reach a quorum leader")
	return stationcore.SubmitOutput{}
}

func (driver *clusterLifecycleDriver) callClaim(ctx context.Context, active []int, input stationcore.ClaimInput) stationcore.ClaimOutput {
	driver.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, index := range active {
			if driver.nodes[index] == nil {
				continue
			}
			if output, err := driver.nodes[index].Router().Claim(ctx, input); err == nil && len(output.Units) > 0 {
				return output
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	driver.t.Fatal("claim did not recover through surviving quorum")
	return stationcore.ClaimOutput{}
}

func newClusterAcceptanceCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey, *x509.CertPool) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "cluster-acceptance-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
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

func newClusterAcceptanceCertificate(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, memberID uint64, clusterID string) tls.Certificate {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: new(big.Int).SetUint64(memberID + 10), Subject: pkix.Name{CommonName: "cluster-peer"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{stationcluster.PeerCertificateDNSName(clusterID, memberID)}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature}
	raw, err := x509.CreateCertificate(rand.Reader, template, ca, publicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	certificate := tls.Certificate{Certificate: [][]byte{raw}, PrivateKey: privateKey}
	return certificate
}
