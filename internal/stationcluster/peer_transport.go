package stationcluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	peerMessagePath        = "/badtower.cluster.v1/raft/message"
	peerMessageContentType = "application/x-protobuf"
	peerRequestTimeout     = 2 * time.Second
	maxPeerMessageBytes    = maxSizePerMessage + 64*1024
)

type HTTPPeerTransport struct {
	targets map[uint64]*url.URL
	client  *http.Client
}

func rejectPeerRedirect(_ *http.Request, _ []*http.Request) error {
	return errors.New("peer redirects are not allowed")
}

// NewHTTPPeerTransport constructs the production peer sender. It accepts only
// HTTPS targets and a client-certificate TLS 1.3 configuration with verified
// roots and HTTP/2 ALPN.
func NewHTTPPeerTransport(targets map[uint64]string, tlsConfig *tls.Config) (*HTTPPeerTransport, error) {
	if len(targets) < MinMembers || len(targets) > MaxMembers || !validPeerClientTLS(tlsConfig) {
		return nil, ErrInvalidTopology
	}
	parsed := make(map[uint64]*url.URL, len(targets))
	for id, raw := range targets {
		target, err := url.Parse(raw)
		if err != nil || id == 0 || target.Scheme != "https" || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") {
			return nil, ErrInvalidTopology
		}
		copyTarget := *target
		copyTarget.Path = peerMessagePath
		parsed[id] = &copyTarget
	}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig.Clone(), ForceAttemptHTTP2: true,
		DisableCompression: true, MaxIdleConns: len(parsed) * 2,
		MaxIdleConnsPerHost: 2, MaxConnsPerHost: 4,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: peerRequestTimeout,
		ResponseHeaderTimeout: peerRequestTimeout,
	}
	return &HTTPPeerTransport{targets: parsed, client: &http.Client{Transport: transport, CheckRedirect: rejectPeerRedirect}}, nil
}

func (transport *HTTPPeerTransport) Send(ctx context.Context, message *raftpb.Message) error {
	if message == nil || message.GetFrom() == 0 || message.GetTo() == 0 {
		return ErrInvalidTopology
	}
	target := transport.targets[message.GetTo()]
	if target == nil {
		return ErrMemberUnknown
	}
	raw, err := proto.Marshal(message)
	if err != nil || len(raw) == 0 || len(raw) > maxPeerMessageBytes {
		return ErrProposalTooLarge
	}
	requestContext, cancel := context.WithTimeout(ctx, peerRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestContext, http.MethodPost, target.String(), bytes.NewReader(raw))
	if err != nil {
		return ErrInvalidTopology
	}
	request.Header.Set("Content-Type", peerMessageContentType)
	request.Header.Set("Accept", "")
	request.ContentLength = int64(len(raw))
	response, err := transport.client.Do(request)
	if err != nil {
		return errors.New("peer transport unavailable")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("peer transport status %d", response.StatusCode)
	}
	return nil
}

func (transport *HTTPPeerTransport) CloseIdleConnections() { transport.client.CloseIdleConnections() }

// NewPeerHandler validates the authenticated HTTP/2/TLS 1.3 carrier before a
// protobuf allocation reaches the bounded member queue.
func NewPeerHandler(member *RaftMember) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if member == nil || request.Method != http.MethodPost || request.URL.Path != peerMessagePath || request.URL.RawQuery != "" || request.URL.Fragment != "" || request.ProtoMajor != 2 || request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || request.TLS.NegotiatedProtocol != "h2" || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 {
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		if request.Header.Get("Content-Type") != peerMessageContentType || request.ContentLength <= 0 || request.ContentLength > maxPeerMessageBytes || len(request.TransferEncoding) != 0 || len(request.Trailer) != 0 {
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(request.Body, maxPeerMessageBytes+1))
		if err != nil || int64(len(raw)) != request.ContentLength || len(raw) > maxPeerMessageBytes {
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		var message raftpb.Message
		if proto.Unmarshal(raw, &message) != nil || message.GetFrom() == 0 || message.GetTo() != member.ID() {
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		if !peerCertificateMatches(request.TLS.PeerCertificates[0], member.ClusterID(), message.GetFrom()) {
			http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		if !authorizeStandaloneRaftIngress(member, &message) {
			http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		stepContext, cancel := context.WithTimeout(request.Context(), peerRequestTimeout)
		defer cancel()
		if err := member.Step(stepContext, &message); err != nil {
			http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	})
}

func authorizeStandaloneRaftIngress(member *RaftMember, message *raftpb.Message) bool {
	if member == nil || message == nil || message.GetFrom() == 0 || message.GetFrom() == member.ID() || !isAllowedRaftNetworkMessage(message.GetType()) {
		return false
	}
	conf := member.ConfState()
	if confHasMembers(conf) {
		return raftMessageAllowedForRole(roleInConfState(conf, message.GetFrom()), message.GetType())
	}
	if !member.joining {
		if confContains(member.bootstrapPeers, message.GetFrom()) {
			return raftMessageAllowedForRole(raftPeerVoter, message.GetType())
		}
		return false
	}
	if confContains(member.bootstrapPeers, message.GetFrom()) {
		return raftMessageAllowedForRole(raftPeerBootstrapLeader, message.GetType())
	}
	return false
}

// PeerCertificateDNSName is the single certificate identity form accepted by
// the peer receiver. The cluster identifier is hashed so a certificate or
// authentication failure never discloses the operator's topology label.
func PeerCertificateDNSName(clusterID string, memberID uint64) string {
	sum := sha256.Sum256([]byte("badtower.cluster.peer.v1\x00" + clusterID))
	return fmt.Sprintf("member-%d.%x.cluster.badtower.invalid", memberID, sum[:16])
}

func peerCertificateMatches(certificate *x509.Certificate, clusterID string, memberID uint64) bool {
	if certificate == nil || clusterID == "" || memberID == 0 {
		return false
	}
	expected := PeerCertificateDNSName(clusterID, memberID)
	for _, name := range certificate.DNSNames {
		if name == expected {
			return true
		}
	}
	return false
}

func validPeerClientTLS(config *tls.Config) bool {
	if config == nil || config.MinVersion != tls.VersionTLS13 || config.MaxVersion != tls.VersionTLS13 || config.RootCAs == nil || config.InsecureSkipVerify || len(config.Certificates) != 1 {
		return false
	}
	return len(config.NextProtos) == 1 && config.NextProtos[0] == "h2"
}

// PeerServerTLSConfig creates the exact server-side half of the mTLS
// boundary. Callers retain certificate loading and rotation authority.
func PeerServerTLSConfig(certificate tls.Certificate, clientRoots *x509.CertPool) (*tls.Config, error) {
	if len(certificate.Certificate) == 0 || clientRoots == nil {
		return nil, ErrInvalidTopology
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate}, ClientCAs: clientRoots,
		ClientAuth: tls.RequireAndVerifyClientCert, NextProtos: []string{"h2"},
	}, nil
}
