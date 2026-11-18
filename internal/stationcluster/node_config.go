package stationcluster

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
)

const NodeConfigSchema = "badtower.station-cluster.node.v1"

type NodeMemberConfig struct {
	ID          uint64 `json:"id"`
	FaultDomain string `json:"faultDomain"`
	PeerURL     string `json:"peerUrl"`
}

type NodeTLSFiles struct {
	CertificateFile string `json:"certificateFile"`
	PrivateKeyFile  string `json:"privateKeyFile"`
	CAFile          string `json:"caFile"`
}

type NodeOperatorConfig struct {
	Enabled          bool   `json:"enabled"`
	SocketPath       string `json:"socketPath,omitempty"`
	CredentialSHA256 string `json:"credentialSha256,omitempty"`
}

type NodeConfig struct {
	Schema           string              `json:"schema"`
	ClusterID        string              `json:"clusterId"`
	MemberID         uint64              `json:"memberId"`
	OwnershipEpoch   uint64              `json:"ownershipEpoch"`
	PeerListen       string              `json:"peerListen"`
	CoreRoot         string              `json:"coreRoot"`
	RaftRoot         string              `json:"raftRoot"`
	TLS              NodeTLSFiles        `json:"tls"`
	Members          []NodeMemberConfig  `json:"members"`
	BootstrapMembers []NodeMemberConfig  `json:"bootstrapMembers,omitempty"`
	Join             bool                `json:"join,omitempty"`
	Operator         *NodeOperatorConfig `json:"operator,omitempty"`
}

func LoadNodeConfig(path string) (NodeConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil || len(raw) == 0 || len(raw) > 1<<20 {
		return NodeConfig{}, ErrInvalidTopology
	}
	if rejectNodeDuplicateJSONKeys(raw) != nil {
		return NodeConfig{}, ErrInvalidTopology
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var config NodeConfig
	if decoder.Decode(&config) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return NodeConfig{}, ErrInvalidTopology
	}
	return ValidateNodeConfig(config)
}

func ValidateNodeConfig(config NodeConfig) (NodeConfig, error) {
	if config.Schema != NodeConfigSchema || !validSnapshotClusterID(config.ClusterID) || config.MemberID == 0 || config.OwnershipEpoch == 0 || len(config.Members) < MinMembers || len(config.Members) > MaxMembers {
		return NodeConfig{}, ErrInvalidTopology
	}
	host, port, err := net.SplitHostPort(config.PeerListen)
	portNumber, portErr := strconv.ParseUint(port, 10, 16)
	if err != nil || portErr != nil || host == "" || portNumber == 0 {
		return NodeConfig{}, ErrInvalidTopology
	}
	for _, root := range []string{config.CoreRoot, config.RaftRoot, config.TLS.CertificateFile, config.TLS.PrivateKeyFile, config.TLS.CAFile} {
		if root == "" || !filepath.IsAbs(root) {
			return NodeConfig{}, ErrInvalidTopology
		}
	}
	if snapshotRootsOverlap(config.CoreRoot, config.RaftRoot) {
		return NodeConfig{}, ErrInvalidTopology
	}
	if config.Operator != nil {
		if !config.Operator.Enabled {
			if config.Operator.SocketPath != "" || config.Operator.CredentialSHA256 != "" {
				return NodeConfig{}, ErrInvalidTopology
			}
		} else {
			raw, err := base64.RawURLEncoding.DecodeString(config.Operator.CredentialSHA256)
			if !filepath.IsAbs(config.Operator.SocketPath) || len(raw) != sha256.Size || err != nil {
				return NodeConfig{}, ErrInvalidTopology
			}
		}
	}
	bootstrap := config.BootstrapMembers
	if len(bootstrap) == 0 {
		if len(config.Members) != MinMembers || config.Join {
			return NodeConfig{}, ErrInvalidTopology
		}
		bootstrap = slices.Clone(config.Members)
	}
	if len(bootstrap) != MinMembers {
		return NodeConfig{}, ErrInvalidTopology
	}
	topology := Topology{ClusterID: config.ClusterID, Epoch: config.OwnershipEpoch, Members: make([]Member, 0, MinMembers)}
	present := false
	registry := make(map[uint64]NodeMemberConfig, len(config.Members))
	for _, member := range config.Members {
		if member.ID == config.MemberID {
			present = true
		}
		if member.PeerURL == "" || !strings.HasPrefix(member.PeerURL, "https://") {
			return NodeConfig{}, ErrInvalidTopology
		}
		if _, duplicate := registry[member.ID]; duplicate {
			return NodeConfig{}, ErrInvalidTopology
		}
		registry[member.ID] = member
	}
	for _, member := range bootstrap {
		registered, ok := registry[member.ID]
		if !ok || registered.FaultDomain != member.FaultDomain || registered.PeerURL != member.PeerURL {
			return NodeConfig{}, ErrInvalidTopology
		}
		topology.Members = append(topology.Members, Member{ID: member.ID, FaultDomain: member.FaultDomain, Incarnation: 1})
	}
	if !present || topology.Validate() != nil || (!config.Join && !slices.ContainsFunc(bootstrap, func(member NodeMemberConfig) bool { return member.ID == config.MemberID })) {
		return NodeConfig{}, ErrInvalidTopology
	}
	return config, nil
}

func rejectNodeDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	return walkNodeJSONValue(decoder)
}

func walkNodeJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrInvalidTopology
			}
			if _, exists := seen[key]; exists {
				return ErrInvalidTopology
			}
			seen[key] = struct{}{}
			if err := walkNodeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := walkNodeJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return ErrInvalidTopology
	}
}

func LoadNodeTLS(config NodeConfig) (*tls.Config, *tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(config.TLS.CertificateFile, config.TLS.PrivateKeyFile)
	if err != nil {
		return nil, nil, ErrInvalidTopology
	}
	if len(certificate.Certificate) == 0 {
		return nil, nil, ErrInvalidTopology
	}
	leaf, err := x509.ParseCertificate(certificate.Certificate[0])
	if err != nil || !peerCertificateMatches(leaf, config.ClusterID, config.MemberID) {
		return nil, nil, ErrInvalidTopology
	}
	caPEM, err := os.ReadFile(config.TLS.CAFile)
	if err != nil || len(caPEM) > 1<<20 {
		return nil, nil, ErrInvalidTopology
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, nil, ErrInvalidTopology
	}
	serverTLS, err := PeerServerTLSConfig(certificate, roots)
	if err != nil {
		return nil, nil, err
	}
	clientTLS := &tls.Config{
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate}, RootCAs: roots,
		NextProtos: []string{"h2"},
	}
	return serverTLS, clientTLS, nil
}

type nodeClock struct{ start time.Time }

func (clock nodeClock) NowUnixMs() int64            { return time.Now().UTC().UnixMilli() }
func (clock nodeClock) MonotonicNow() time.Duration { return time.Since(clock.start) }
