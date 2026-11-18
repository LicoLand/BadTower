package stationcluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/net/netutil"
	"google.golang.org/protobuf/proto"
)

const (
	nodePeerPathPrefix     = "/badtower.cluster.v1/group/"
	nodeOperatorPathPrefix = "/badtower.cluster.v1/operator/"
	nodePeerMaxConnections = 128
	ControlGroupID         = uint64(PartitionCount) + 1
)

type nodePeerClient struct {
	targets       map[uint64]*url.URL
	clients       map[uint64]*http.Client
	snapshotStore *streamSnapshotStore
}

func newNodePeerClient(clusterID string, members []NodeMemberConfig, tlsConfig *tls.Config, snapshotStore *streamSnapshotStore) (*nodePeerClient, error) {
	if clusterID == "" || !validPeerClientTLS(tlsConfig) || len(members) < MinMembers || len(members) > MaxMembers {
		return nil, ErrInvalidTopology
	}
	targets := make(map[uint64]*url.URL, len(members))
	clients := make(map[uint64]*http.Client, len(members))
	for _, member := range members {
		target, err := url.Parse(member.PeerURL)
		if err != nil || target.Scheme != "https" || target.Host == "" || target.User != nil || target.RawQuery != "" || target.Fragment != "" || (target.Path != "" && target.Path != "/") {
			return nil, ErrInvalidTopology
		}
		targets[member.ID] = target
		expectedMemberID := member.ID
		memberTLS := tlsConfig.Clone()
		baseVerify := memberTLS.VerifyConnection
		memberTLS.VerifyConnection = func(state tls.ConnectionState) error {
			if baseVerify != nil {
				if err := baseVerify(state); err != nil {
					return err
				}
			}
			if len(state.PeerCertificates) == 0 || !peerCertificateMatches(state.PeerCertificates[0], clusterID, expectedMemberID) {
				return ErrMemberUnknown
			}
			return nil
		}
		transport := &http.Transport{
			TLSClientConfig: memberTLS, ForceAttemptHTTP2: true, DisableCompression: true,
			MaxIdleConns: 8, MaxIdleConnsPerHost: 8, MaxConnsPerHost: 16,
			IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: peerRequestTimeout,
			ResponseHeaderTimeout: peerRequestTimeout,
		}
		clients[member.ID] = &http.Client{Transport: transport, CheckRedirect: rejectPeerRedirect}
	}
	return &nodePeerClient{targets: targets, clients: clients, snapshotStore: snapshotStore}, nil
}

func (client *nodePeerClient) do(memberID uint64, request *http.Request) (*http.Response, error) {
	peerHTTP := client.clients[memberID]
	if peerHTTP == nil {
		return nil, ErrMemberUnknown
	}
	return peerHTTP.Do(request)
}

func (client *nodePeerClient) group(groupID uint64) PeerTransport {
	return groupPeerTransport{client: client, groupID: groupID}
}

func (client *nodePeerClient) close() {
	for _, peerHTTP := range client.clients {
		peerHTTP.CloseIdleConnections()
	}
}

type groupPeerTransport struct {
	client  *nodePeerClient
	groupID uint64
}

func (transport groupPeerTransport) Send(ctx context.Context, message *raftpb.Message) error {
	if message == nil || transport.groupID == 0 || transport.groupID > ControlGroupID {
		return ErrInvalidTopology
	}
	if message.GetType() == raftpb.MessageType_MsgSnap && message.Snapshot != nil {
		if reference, streamed, err := decodeStreamSnapshotReference(message.Snapshot.Data); streamed {
			if err != nil || reference.GroupID != transport.groupID {
				return ErrInvalidSnapshot
			}
			if err := transport.client.sendStreamSnapshot(ctx, message.GetTo(), reference); err != nil {
				return err
			}
		}
	}
	target := transport.client.targets[message.GetTo()]
	if target == nil {
		return ErrMemberUnknown
	}
	raw, err := proto.Marshal(message)
	if err != nil || len(raw) == 0 || len(raw) > maxPeerMessageBytes {
		return ErrProposalTooLarge
	}
	endpoint := *target
	endpoint.Path = nodePeerPathPrefix + strconv.FormatUint(transport.groupID, 10)
	requestCtx, cancel := context.WithTimeout(ctx, peerRequestTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint.String(), bytes.NewReader(raw))
	if err != nil {
		return ErrInvalidTopology
	}
	request.Header.Set("Content-Type", peerMessageContentType)
	request.ContentLength = int64(len(raw))
	response, err := transport.client.do(message.GetTo(), request)
	if err != nil {
		return ErrMemberStopped
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	if response.StatusCode != http.StatusNoContent {
		return ErrMemberStopped
	}
	return nil
}

type nodePeerServer struct {
	node *Node
	mu   sync.Mutex
	http *http.Server
}

func (server *nodePeerServer) serve(listener net.Listener, tlsConfig *tls.Config) error {
	if listener == nil || tlsConfig == nil {
		return ErrInvalidTopology
	}
	httpServer := &http.Server{
		Handler: server, TLSConfig: tlsConfig.Clone(), ReadHeaderTimeout: 2 * time.Second,
		IdleTimeout:    30 * time.Second,
		MaxHeaderBytes: 8 << 10, ErrorLog: log.New(io.Discard, "", 0),
	}
	server.mu.Lock()
	server.http = httpServer
	server.mu.Unlock()
	err := httpServer.Serve(tls.NewListener(netutil.LimitListener(listener, nodePeerMaxConnections), tlsConfig.Clone()))
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

func (server *nodePeerServer) close(ctx context.Context) error {
	server.mu.Lock()
	httpServer := server.http
	server.mu.Unlock()
	if httpServer == nil {
		return nil
	}
	return httpServer.Shutdown(ctx)
}

func (server *nodePeerServer) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	if strings.HasPrefix(request.URL.Path, streamSnapshotPathPrefix) {
		server.serveStreamSnapshot(response, request)
		return
	}
	controller := http.NewResponseController(response)
	_ = controller.SetReadDeadline(time.Now().Add(3 * time.Second))
	_ = controller.SetWriteDeadline(time.Now().Add(3 * time.Second))
	if strings.HasPrefix(request.URL.Path, nodeOperatorPathPrefix) {
		server.serveOperatorPeer(response, request)
		return
	}
	groupText := strings.TrimPrefix(request.URL.Path, nodePeerPathPrefix)
	groupID, parseErr := strconv.ParseUint(groupText, 10, 64)
	if parseErr != nil || groupID == 0 || groupID > ControlGroupID || request.URL.Path != nodePeerPathPrefix+groupText || request.Method != http.MethodPost || request.ProtoMajor != 2 || request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || request.TLS.NegotiatedProtocol != "h2" || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 || request.Header.Get("Content-Type") != peerMessageContentType || request.ContentLength <= 0 || request.ContentLength > maxPeerMessageBytes {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, maxPeerMessageBytes+1))
	if err != nil || int64(len(raw)) != request.ContentLength || len(raw) > maxPeerMessageBytes {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	var message raftpb.Message
	if proto.Unmarshal(raw, &message) != nil || message.GetFrom() == 0 || message.GetTo() != server.node.id || !peerCertificateMatches(request.TLS.PeerCertificates[0], server.node.clusterID, message.GetFrom()) {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	if !server.authorizeRaftIngress(groupID, &message) {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	var member interface {
		Step(context.Context, *raftpb.Message) error
	}
	if groupID == ControlGroupID {
		member = server.node.control
	} else {
		member = server.node.members[groupID-1]
	}
	if member == nil || member.Step(request.Context(), &message) != nil {
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (server *nodePeerServer) serveStreamSnapshot(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost || request.ProtoMajor != 2 || request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || request.TLS.NegotiatedProtocol != "h2" || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 || request.Header.Get("Content-Type") != streamSnapshotContentType || request.URL.RawQuery != "" || request.URL.Fragment != "" || server.node.snapshotStore == nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, streamSnapshotPathPrefix), "/")
	if len(parts) != 2 {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	groupID, groupErr := strconv.ParseUint(parts[0], 10, 64)
	index, indexErr := strconv.ParseUint(parts[1], 10, 64)
	if groupErr != nil || indexErr != nil || groupID == 0 || groupID > PartitionCount || index == 0 {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	caller, controlState, ok := server.authenticatedCommittedMember(request.TLS.PeerCertificates[0])
	partition := uint16(groupID - 1)
	if !ok || caller.Role != ControlRoleVoter || caller.Phase != ControlPhaseStable || !slices.Contains(controlState.Placements[partition].Replicas[:], caller.ID) {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	controller := http.NewResponseController(response)
	expected := streamSnapshotReceiveExpectation{
		ClusterID:      server.node.clusterID,
		GroupID:        groupID,
		Partition:      partition,
		SourceMemberID: caller.ID,
		AppliedIndex:   index,
		OwnershipEpoch: controlState.Placements[partition].Epoch,
	}
	ref, err := server.node.snapshotStore.receive(request.Context(), &snapshotDeadlineReader{reader: request.Body, controller: controller}, expected)
	if err != nil || ref.GroupID != groupID || ref.AppliedIndex != index || ref.ClusterID != server.node.clusterID {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	_ = controller.SetWriteDeadline(time.Now().Add(3 * time.Second))
	response.WriteHeader(http.StatusNoContent)
}

func (server *nodePeerServer) serveOperatorPeer(response http.ResponseWriter, request *http.Request) {
	if request.ProtoMajor != 2 || request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || request.TLS.NegotiatedProtocol != "h2" || len(request.TLS.VerifiedChains) == 0 || len(request.TLS.PeerCertificates) == 0 {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	caller, controlState, ok := server.authorizeControlLeader(request.TLS.PeerCertificates[0])
	if !ok {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, nodeOperatorPathPrefix), "/")
	if len(parts) < 2 {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	groupID, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || groupID == 0 || groupID > ControlGroupID {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	var statusMember interface {
		Status() (uint64, uint64, error)
	}
	var member *RaftMember
	if groupID == ControlGroupID {
		statusMember = server.node.control
	} else {
		member = server.node.members[groupID-1]
		statusMember = member
	}
	if statusMember == nil {
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	if len(parts) == 2 && parts[1] == "status" && request.Method == http.MethodGet {
		lead, applied, fatal := statusMember.Status()
		if fatal != nil {
			http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		var raw [24]byte
		binary.BigEndian.PutUint64(raw[0:8], lead)
		binary.BigEndian.PutUint64(raw[8:16], applied)
		if member != nil {
			binary.BigEndian.PutUint64(raw[16:24], member.epoch)
		}
		response.Header().Set("Content-Type", "application/octet-stream")
		response.Header().Set("Content-Length", "24")
		_, _ = response.Write(raw[:])
		return
	}
	if member == nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	partition := uint16(groupID - 1)
	if len(parts) == 2 && parts[1] == "backup-barrier" {
		if request.Method != http.MethodPost || request.ContentLength != 16 || request.Header.Get("Content-Type") != "application/octet-stream" {
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		raw, readErr := io.ReadAll(io.LimitReader(request.Body, 17))
		if readErr != nil || len(raw) != 16 {
			http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
			return
		}
		epoch := binary.BigEndian.Uint64(raw[0:8])
		backupID := binary.BigEndian.Uint64(raw[8:16])
		if !server.authorizeBackupOperation(controlState, caller.ID, partition, epoch, backupID, member.epoch) {
			http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
			return
		}
		leadBefore, _, fatal := member.Status()
		if fatal != nil || leadBefore != server.node.id || member.ReadBarrier(request.Context()) != nil {
			http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		leadAfter, _, fatal := member.Status()
		if fatal != nil || leadAfter != server.node.id {
			http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
		return
	}
	if len(parts) == 2 && parts[1] == "backup-snapshot" {
		server.serveBackupSnapshot(response, request, caller.ID, controlState, groupID, member)
		return
	}
	if len(parts) != 3 || request.Method != http.MethodPost || request.ContentLength != 8 || request.Header.Get("Content-Type") != "application/octet-stream" {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	targetID, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, 9))
	if err != nil || len(raw) != 8 {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	epoch := binary.BigEndian.Uint64(raw)
	if epoch < member.epoch {
		http.Error(response, http.StatusText(http.StatusConflict), http.StatusConflict)
		return
	}
	if !server.authorizeMoveOperation(controlState, partition, parts[1], targetID, epoch) {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	switch parts[1] {
	case "add-learner":
		err = member.AddLearner(request.Context(), targetID)
	case "enter-joint":
		err = member.EnterJoint(request.Context(), targetID)
	case "transfer":
		lead, _, _ := member.Status()
		err = member.TransferLeadership(request.Context(), lead, targetID)
	case "leave-joint":
		err = member.LeaveJoint(request.Context())
	case "remove":
		err = member.RemoveMember(request.Context(), targetID)
	default:
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	response.WriteHeader(http.StatusNoContent)
}

func (server *nodePeerServer) serveBackupSnapshot(response http.ResponseWriter, request *http.Request, callerID uint64, controlState ControlState, groupID uint64, member *RaftMember) {
	if request.Method != http.MethodPost || request.ContentLength != 28 || request.Header.Get("Content-Type") != "application/octet-stream" || server.node.snapshotStore == nil || groupID == 0 || groupID > PartitionCount {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, 29))
	if err != nil || len(raw) != 28 {
		http.Error(response, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	epoch := binary.BigEndian.Uint64(raw[0:8])
	applied := binary.BigEndian.Uint64(raw[8:16])
	chunkBytes := int(binary.BigEndian.Uint32(raw[16:20]))
	backupID := binary.BigEndian.Uint64(raw[20:28])
	partition := uint16(groupID - 1)
	if !server.authorizeBackupOperation(controlState, callerID, partition, epoch, backupID, member.epoch) {
		http.Error(response, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}
	lead, currentApplied, fatal := member.Status()
	if applied == 0 || chunkBytes < MinCoreSnapshotChunkBytes || chunkBytes > MaxCoreSnapshotChunkBytes || fatal != nil || lead != server.node.id || currentApplied != applied || member.ReadBarrier(request.Context()) != nil {
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	lead, currentApplied, fatal = member.Status()
	if fatal != nil || lead != server.node.id || currentApplied != applied {
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	ref, err := server.node.prepareBackupStreamSnapshot(partition, epoch, applied, chunkBytes)
	if err != nil {
		http.Error(response, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	controller := http.NewResponseController(response)
	_ = controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	response.Header().Set("Content-Type", streamSnapshotContentType)
	response.WriteHeader(http.StatusOK)
	if server.node.snapshotStore.writeTo(request.Context(), &snapshotDeadlineWriter{writer: response, controller: controller}, ref) == nil {
		server.node.snapshotStore.prune(ref, false)
	}
}

func (node *Node) prepareBackupStreamSnapshot(partition uint16, epoch, applied uint64, chunkBytes int) (streamSnapshotReference, error) {
	if node == nil || partition >= PartitionCount || node.cores[partition] == nil || node.snapshotStore == nil {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	parent := filepath.Join(node.snapshotStore.root, "source")
	staging, err := os.MkdirTemp(parent, ".building-")
	if err != nil {
		return streamSnapshotReference{}, err
	}
	if err = os.Remove(staging); err != nil {
		return streamSnapshotReference{}, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(staging)
		}
	}()
	manifest, err := createCoreSnapshot(node.cores[partition], staging, CoreSnapshotBinding{ClusterID: node.clusterID, GroupID: uint64(partition) + 1, SourceMemberID: node.id, Partition: partition, OwnershipEpoch: epoch, AppliedIndex: applied}, chunkBytes, MaxCoreSnapshotBytes)
	if err != nil {
		return streamSnapshotReference{}, err
	}
	ref := referenceForManifest(manifest)
	final := node.snapshotStore.sourceRoot(ref)
	if _, existingErr := node.snapshotStore.artifact(ref, false); existingErr == nil {
		_ = os.RemoveAll(staging)
		return ref, nil
	}
	if requireAbsentAbsoluteRoot(final) != nil || os.Rename(staging, final) != nil || syncSnapshotDir(parent) != nil {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	cleanup = false
	return ref, nil
}

type raftPeerRole uint8

const (
	raftPeerDenied raftPeerRole = iota
	raftPeerLearner
	raftPeerBootstrapLeader
	raftPeerVoter
)

func isAllowedRaftNetworkMessage(msgType raftpb.MessageType) bool {
	switch msgType {
	case raftpb.MessageType_MsgProp,
		raftpb.MessageType_MsgApp,
		raftpb.MessageType_MsgAppResp,
		raftpb.MessageType_MsgVote,
		raftpb.MessageType_MsgVoteResp,
		raftpb.MessageType_MsgSnap,
		raftpb.MessageType_MsgHeartbeat,
		raftpb.MessageType_MsgHeartbeatResp,
		raftpb.MessageType_MsgTransferLeader,
		raftpb.MessageType_MsgTimeoutNow,
		raftpb.MessageType_MsgReadIndex,
		raftpb.MessageType_MsgReadIndexResp,
		raftpb.MessageType_MsgPreVote,
		raftpb.MessageType_MsgPreVoteResp:
		return true
	default:
		return false
	}
}

func isIgnorableRaftStepError(err error) bool {
	return errors.Is(err, raft.ErrStepPeerNotFound) ||
		errors.Is(err, raft.ErrStepLocalMsg) ||
		errors.Is(err, raft.ErrProposalDropped)
}

func raftMessageAllowedForRole(role raftPeerRole, msgType raftpb.MessageType) bool {
	switch role {
	case raftPeerVoter:
		return isAllowedRaftNetworkMessage(msgType)
	case raftPeerLearner:
		return msgType == raftpb.MessageType_MsgAppResp || msgType == raftpb.MessageType_MsgHeartbeatResp
	case raftPeerBootstrapLeader:
		return msgType == raftpb.MessageType_MsgApp || msgType == raftpb.MessageType_MsgHeartbeat || msgType == raftpb.MessageType_MsgSnap
	default:
		return false
	}
}

func confHasMembers(conf *raftpb.ConfState) bool {
	return conf != nil && (len(conf.Voters) > 0 || len(conf.VotersOutgoing) > 0 || len(conf.Learners) > 0 || len(conf.LearnersNext) > 0)
}

func roleInConfState(conf *raftpb.ConfState, peerID uint64) raftPeerRole {
	if conf == nil {
		return raftPeerDenied
	}
	if confContains(conf.Voters, peerID) || confContains(conf.VotersOutgoing, peerID) {
		return raftPeerVoter
	}
	if confContains(conf.Learners, peerID) || confContains(conf.LearnersNext, peerID) {
		return raftPeerLearner
	}
	return raftPeerDenied
}

func (server *nodePeerServer) authorizeRaftIngress(groupID uint64, message *raftpb.Message) bool {
	if server == nil || server.node == nil || server.node.controlDB == nil || message == nil || !isAllowedRaftNetworkMessage(message.GetType()) {
		return false
	}
	fromID := message.GetFrom()
	if fromID == 0 || fromID == server.node.id {
		return false
	}
	state := server.node.controlDB.Snapshot()
	if state.ClusterID != server.node.clusterID || len(state.Placements) != PartitionCount {
		return false
	}
	if groupID == ControlGroupID {
		control, ok := server.node.control.(*DurableControlMember)
		if !ok || control == nil {
			return false
		}
		conf := control.ConfState()
		if confHasMembers(conf) {
			return raftMessageAllowedForRole(roleInConfState(conf, fromID), message.GetType())
		}
		if !server.node.config.Join {
			if confContains(control.bootstrapPeers, fromID) {
				return raftMessageAllowedForRole(raftPeerVoter, message.GetType())
			}
			return false
		}
		sender := controlMember(&state, fromID)
		if sender != nil && sender.Role == ControlRoleVoter && sender.Phase == ControlPhaseStable {
			return raftMessageAllowedForRole(raftPeerBootstrapLeader, message.GetType())
		}
		return false
	}
	if groupID == 0 || groupID > PartitionCount {
		return false
	}
	partition := uint16(groupID - 1)
	member := server.node.members[partition]
	if member == nil {
		return false
	}
	sender := controlMember(&state, fromID)
	if sender == nil {
		return false
	}
	conf := member.ConfState()
	if confHasMembers(conf) {
		return raftMessageAllowedForRole(roleInConfState(conf, fromID), message.GetType())
	}
	placement := state.Placements[partition]
	if !server.node.config.Join {
		if slices.Contains(placement.Replicas[:], fromID) {
			return raftMessageAllowedForRole(raftPeerVoter, message.GetType())
		}
		return false
	}
	if sender.Role == ControlRoleVoter && sender.Phase == ControlPhaseStable && slices.Contains(placement.Replicas[:], fromID) {
		return raftMessageAllowedForRole(raftPeerBootstrapLeader, message.GetType())
	}
	return false
}

func (server *nodePeerServer) authenticatedCommittedMember(certificate *x509.Certificate) (ControlMember, ControlState, bool) {
	if server == nil || server.node == nil || server.node.controlDB == nil || certificate == nil {
		return ControlMember{}, ControlState{}, false
	}
	state := server.node.controlDB.Snapshot()
	if state.ClusterID != server.node.clusterID || len(state.Placements) != PartitionCount {
		return ControlMember{}, ControlState{}, false
	}
	for _, member := range state.Members {
		if peerCertificateMatches(certificate, server.node.clusterID, member.ID) {
			return member, state, true
		}
	}
	return ControlMember{}, ControlState{}, false
}

func (server *nodePeerServer) authorizeControlLeader(certificate *x509.Certificate) (ControlMember, ControlState, bool) {
	caller, state, ok := server.authenticatedCommittedMember(certificate)
	if !ok || caller.Role != ControlRoleVoter || caller.Phase != ControlPhaseStable || server.node.control == nil {
		return ControlMember{}, ControlState{}, false
	}
	controlLead, _, fatal := server.node.control.Status()
	if fatal != nil || controlLead == 0 || controlLead != caller.ID {
		return ControlMember{}, ControlState{}, false
	}
	return caller, state, true
}

func (server *nodePeerServer) authorizeBackupOperation(state ControlState, callerID uint64, partition uint16, epoch, backupID, memberEpoch uint64) bool {
	if partition >= PartitionCount || backupID == 0 || epoch < memberEpoch || state.ActiveBackup == nil {
		return false
	}
	backup := state.ActiveBackup
	placement := state.Placements[partition]
	return backup.BackupID == backupID &&
		backup.CoordinatorMemberID == callerID &&
		backup.PlacementEpoch == state.PlacementEpoch &&
		backup.Partition == partition &&
		backup.SourceMemberID == server.node.id &&
		epoch == placement.Epoch &&
		slices.Contains(placement.Replicas[:], server.node.id)
}

func (server *nodePeerServer) authorizeMoveOperation(state ControlState, partition uint16, action string, targetID, epoch uint64) bool {
	if partition >= PartitionCount || state.ActiveMove == nil {
		return false
	}
	move := state.ActiveMove
	if move.Partition != partition || move.ExpectedEpoch != state.PlacementEpoch {
		return false
	}
	placement := state.Placements[partition]
	if epoch != placement.Epoch {
		return false
	}
	inPlacement := slices.Contains(placement.Replicas[:], server.node.id)
	switch action {
	case "add-learner":
		return inPlacement && move.Phase == MovePhasePlanned && targetID == move.ToMemberID
	case "enter-joint":
		return inPlacement && move.Phase == MovePhaseCaughtUp && targetID == move.ToMemberID
	case "transfer":
		return inPlacement && move.Phase == MovePhaseJoint && targetID == move.ToMemberID
	case "leave-joint":
		return (inPlacement || server.node.id == move.ToMemberID) && move.Phase == MovePhaseJoint && targetID == 0
	case "remove":
		return (inPlacement || server.node.id == move.ToMemberID) && move.Phase == MovePhaseJoint && targetID == move.FromMemberID
	default:
		return false
	}
}
