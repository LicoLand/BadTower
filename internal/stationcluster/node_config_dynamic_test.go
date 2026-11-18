package stationcluster

import (
	"path/filepath"
	"testing"
)

func TestNodeConfigAdmitsExplicitFourthLearnerWithoutChangingBootstrapSet(t *testing.T) {
	root := t.TempDir()
	bootstrap := []NodeMemberConfig{{ID: 1, FaultDomain: "a", PeerURL: "https://127.0.0.1:9101"}, {ID: 2, FaultDomain: "b", PeerURL: "https://127.0.0.1:9102"}, {ID: 3, FaultDomain: "c", PeerURL: "https://127.0.0.1:9103"}}
	registry := append(append([]NodeMemberConfig{}, bootstrap...), NodeMemberConfig{ID: 4, FaultDomain: "d", PeerURL: "https://127.0.0.1:9104"})
	config := NodeConfig{Schema: NodeConfigSchema, ClusterID: "cluster-dynamic-config", MemberID: 4, OwnershipEpoch: 1, PeerListen: "127.0.0.1:9104",
		CoreRoot: filepath.Join(root, "core"), RaftRoot: filepath.Join(root, "raft"), Join: true,
		TLS:     NodeTLSFiles{CertificateFile: filepath.Join(root, "cert"), PrivateKeyFile: filepath.Join(root, "key"), CAFile: filepath.Join(root, "ca")},
		Members: registry, BootstrapMembers: bootstrap}
	validated, err := ValidateNodeConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	if !validated.Join || len(validated.Members) != 4 || len(validated.BootstrapMembers) != 3 {
		t.Fatalf("validated=%+v", validated)
	}
	config.Join = false
	if _, err := ValidateNodeConfig(config); err == nil {
		t.Fatal("non-founding member bootstrapped from static registry")
	}
}
