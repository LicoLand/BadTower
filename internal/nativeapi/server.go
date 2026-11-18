package nativeapi

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	"github.com/LicoLand/BadTower/internal/stationcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

const (
	maxReceiveBytes = 1_114_112
	maxSendBytes    = 4_325_376
)

// LifecycleState is the native API process lifecycle (C-12).
type LifecycleState int32

const (
	LifecycleStarting LifecycleState = iota + 1
	LifecycleRunning
	LifecycleDraining
	LifecycleStopped
)

// Server exposes the native Station contract and auxiliary listeners.
type Server struct {
	core        Station
	config      Config
	auth        *Authenticator
	lifecycle   atomic.Int32
	dataServer  *grpc.Server
	health      *HealthServer
	metrics     *MetricsServer
	operator    *OperatorServer
	serveMu     sync.Mutex
	inFlight    atomic.Int64
	connections atomic.Int64
}

// NewServer constructs one native API server.
func NewServer(core Station, config Config, auth *Authenticator) *Server {
	server := &Server{core: core, config: config, auth: auth}
	server.lifecycle.Store(int32(LifecycleStarting))
	server.health = NewHealthServer(server)
	server.metrics = NewMetricsServer(server)
	server.operator = NewOperatorServer(server)
	return server
}

// Lifecycle returns the current lifecycle state.
func (server *Server) Lifecycle() LifecycleState {
	return LifecycleState(server.lifecycle.Load())
}

// BeginDrain transitions to DRAINING, rejects new mutation RPCs, and invokes
// core.BeginDrain so the data plane rejects admissions with the same reason.
// Both transitions are idempotent.
func (server *Server) BeginDrain() {
	if server.core != nil {
		server.core.BeginDrain()
	}
	for {
		state := server.lifecycle.Load()
		if LifecycleState(state) == LifecycleDraining || LifecycleState(state) == LifecycleStopped {
			return
		}
		if server.lifecycle.CompareAndSwap(state, int32(LifecycleDraining)) {
			return
		}
	}
}

// SetLifecycleRunning marks the server ready for startup completion.
func (server *Server) SetLifecycleRunning() bool {
	if !server.lifecycle.CompareAndSwap(int32(LifecycleStarting), int32(LifecycleRunning)) {
		return false
	}
	server.health.SetStartup(true)
	return true
}

// MarkStopped closes the process lifecycle. A later startup callback cannot
// revive a draining or stopped server because RUNNING is a one-way CAS from
// STARTING only.
func (server *Server) MarkStopped() {
	server.lifecycle.Store(int32(LifecycleStopped))
}

// Readiness derives one closed readiness state from process lifecycle and the
// Station Core clock/lifecycle check. It is the shared source for both the
// operator projection and grpc.health readiness.
func (server *Server) Readiness() (bool, stationcore.Reason) {
	switch server.Lifecycle() {
	case LifecycleStarting:
		return false, stationcore.Reason(ReadinessReasonStarting)
	case LifecycleRunning:
		if server.core == nil {
			return false, stationcore.Reason(ReadinessReasonStopped)
		}
		return server.core.Ready()
	case LifecycleDraining:
		return false, stationcore.ReasonDraining
	default:
		return false, stationcore.Reason(ReadinessReasonStopped)
	}
}

// ServeData serves the authenticated Station gRPC listener until ctx ends.
func (server *Server) ServeData(ctx context.Context, listener net.Listener, tlsConfig *tls.Config) error {
	options := []grpc.ServerOption{
		grpc.MaxConcurrentStreams(64),
		grpc.MaxRecvMsgSize(maxReceiveBytes),
		grpc.MaxSendMsgSize(maxSendBytes),
		grpc.KeepaliveParams(keepalive.ServerParameters{MaxConnectionIdle: 2 * time.Minute}),
		grpc.UnaryInterceptor(server.unaryInterceptor),
	}
	if tlsConfig != nil {
		options = append(options, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}
	dataServer := grpc.NewServer(options...)
	stationv1.RegisterStationServer(dataServer, &grpcStation{server: server})
	server.serveMu.Lock()
	server.dataServer = dataServer
	server.serveMu.Unlock()

	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		dataServer.GracefulStop()
		close(done)
	}()
	err := dataServer.Serve(listener)
	server.serveMu.Lock()
	if server.dataServer == dataServer {
		server.dataServer = nil
	}
	server.serveMu.Unlock()
	if ctx.Err() != nil {
		<-done
	}
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}

// ServeHealth serves grpc.health.v1 on one listener.
func (server *Server) ServeHealth(ctx context.Context, listener net.Listener) error {
	return server.health.Serve(ctx, listener)
}

// ServeMetrics serves GET /metrics on one listener.
func (server *Server) ServeMetrics(ctx context.Context, listener net.Listener) error {
	return server.metrics.Serve(ctx, listener)
}

// ServeOperator serves the strict operator overview listener until ctx ends.
// It fails fast when the operator block is absent or disabled.
func (server *Server) ServeOperator(ctx context.Context, listener net.Listener) error {
	if server.operator == nil || !server.operator.enabled {
		return errors.New("operator listener is disabled")
	}
	return server.operator.Serve(ctx, listener)
}

// GracefulStop stops the data gRPC server.
func (server *Server) GracefulStop() {
	server.serveMu.Lock()
	dataServer := server.dataServer
	server.serveMu.Unlock()
	if dataServer != nil {
		dataServer.GracefulStop()
	}
}

// HardStop force-closes every process listener after the bounded graceful
// budget. It never waits for handlers and is idempotent.
func (server *Server) HardStop() {
	server.serveMu.Lock()
	dataServer := server.dataServer
	server.serveMu.Unlock()
	if dataServer != nil {
		dataServer.Stop()
	}
	server.health.HardStop()
	server.metrics.Close()
	server.operator.Close()
}

func (server *Server) unaryInterceptor(
	ctx context.Context,
	request any,
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (any, error) {
	server.inFlight.Add(1)
	defer server.inFlight.Add(-1)
	if p, ok := peer.FromContext(ctx); ok && p != nil {
		server.connections.Add(1)
		defer server.connections.Add(-1)
	}
	_ = info
	return handler(ctx, request)
}

type grpcStation struct {
	stationv1.UnimplementedStationServer
	server *Server
}

func (grpc *grpcStation) Reserve(ctx context.Context, req *stationv1.ReserveRequest) (*stationv1.ReserveResponse, error) {
	if err := requireDeadline(hasDeadline(ctx, 5*time.Second)); err != nil {
		return nil, mapError(err)
	}
	if err := grpc.server.rejectIfDraining("Reserve"); err != nil {
		return nil, mapError(err)
	}
	namespace, err := grpc.server.namespaceFromContext(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	response, err := grpc.server.reserveProto(ctx, namespace, req)
	return response, mapError(err)
}

func (grpc *grpcStation) Submit(ctx context.Context, req *stationv1.SubmitRequest) (*stationv1.SubmitResponse, error) {
	if err := requireDeadline(hasDeadline(ctx, 5*time.Second)); err != nil {
		return nil, mapError(err)
	}
	if err := grpc.server.rejectIfDraining("Submit"); err != nil {
		return nil, mapError(err)
	}
	namespace, err := grpc.server.namespaceFromContext(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	response, err := grpc.server.submitProto(ctx, namespace, req)
	return response, mapError(err)
}

func (grpc *grpcStation) Claim(ctx context.Context, req *stationv1.ClaimRequest) (*stationv1.ClaimResponse, error) {
	if err := requireDeadline(hasDeadline(ctx, 30*time.Second)); err != nil {
		return nil, mapError(err)
	}
	if err := grpc.server.rejectIfDraining("Claim"); err != nil {
		return nil, mapError(err)
	}
	namespace, err := grpc.server.namespaceFromContext(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	response, err := grpc.server.claimProto(ctx, namespace, req)
	return response, mapError(err)
}

func (grpc *grpcStation) Settle(ctx context.Context, req *stationv1.SettleRequest) (*stationv1.SettleResponse, error) {
	if err := requireDeadline(hasDeadline(ctx, 5*time.Second)); err != nil {
		return nil, mapError(err)
	}
	namespace, err := grpc.server.namespaceFromContext(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	response, err := grpc.server.settleProto(ctx, namespace, req)
	return response, mapError(err)
}

func (grpc *grpcStation) GetCapacity(ctx context.Context, req *stationv1.GetCapacityRequest) (*stationv1.GetCapacityResponse, error) {
	if err := requireDeadline(hasDeadline(ctx, 5*time.Second)); err != nil {
		return nil, mapError(err)
	}
	namespace, err := grpc.server.namespaceFromContext(ctx)
	if err != nil {
		return nil, mapError(err)
	}
	if grpc.server.core == nil {
		return nil, mapError(stationFailure{Reason: "INTERNAL"})
	}
	limits := grpc.server.core.ProfileLimits()
	pressure, usage := grpc.server.core.NamespacePressure(namespace)
	return &stationv1.GetCapacityResponse{
		ProfileId:      grpc.server.config.Profile,
		Pressure:       mapPressure(pressure),
		NamespaceUsage: mapResourceVector(usage),
		NamespaceLimit: profileLimitVector(limits),
	}, nil
}

func (server *Server) namespaceFromContext(ctx context.Context) (string, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", stationFailure{Reason: "UNAUTHENTICATED"}
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return "", stationFailure{Reason: "UNAUTHENTICATED"}
	}
	const prefix = "Bearer "
	token := values[0]
	if len(token) <= len(prefix) || token[:len(prefix)] != prefix {
		return "", stationFailure{Reason: "UNAUTHENTICATED"}
	}
	return server.auth.NamespaceForBearer(token[len(prefix):])
}

func (server *Server) rejectIfDraining(method string) error {
	if server.Lifecycle() != LifecycleDraining {
		return nil
	}
	switch method {
	case "Reserve", "Submit", "Claim":
		return stationFailure{Reason: "DRAINING", Retryable: true, RetryAfterMs: 100}
	default:
		return nil
	}
}

func (server *Server) reserveProto(ctx context.Context, namespace string, req *stationv1.ReserveRequest) (*stationv1.ReserveResponse, error) {
	if err := requireNamespace(server, namespace); err != nil {
		return nil, err
	}
	if server.core == nil {
		return nil, stationFailure{Reason: "INTERNAL"}
	}
	addressID := ""
	if req.AddressId != nil {
		addressID = *req.AddressId
	}
	out, err := server.core.Reserve(ctx, stationcore.ReserveInput{
		NamespaceID: namespace,
		RequestID:   req.RequestId,
		AddressID:   addressID,
		TTLMs:       req.TtlMs,
	})
	if err != nil {
		return nil, err
	}
	return &stationv1.ReserveResponse{
		AddressId:       out.AddressID,
		ExpiresAtUnixMs: out.ExpiresAtUnixMs,
		Created:         out.Created,
	}, nil
}

func (server *Server) submitProto(ctx context.Context, namespace string, req *stationv1.SubmitRequest) (*stationv1.SubmitResponse, error) {
	if err := requireNamespace(server, namespace); err != nil {
		return nil, err
	}
	if server.core == nil {
		return nil, stationFailure{Reason: "INTERNAL"}
	}
	out, err := server.core.Submit(ctx, stationcore.SubmitInput{
		NamespaceID: namespace,
		RequestID:   req.RequestId,
		AddressID:   req.AddressId,
		Payload:     req.Payload,
		TTLMs:       req.TtlMs,
	})
	if err != nil {
		return nil, err
	}
	return &stationv1.SubmitResponse{
		UnitId:           out.UnitID,
		AcceptedAtUnixMs: out.AcceptedAtUnixMs,
		ExpiresAtUnixMs:  out.ExpiresAtUnixMs,
	}, nil
}

func (server *Server) claimProto(ctx context.Context, namespace string, req *stationv1.ClaimRequest) (*stationv1.ClaimResponse, error) {
	if err := requireNamespace(server, namespace); err != nil {
		return nil, err
	}
	if server.core == nil {
		return nil, stationFailure{Reason: "INTERNAL"}
	}
	out, err := server.core.Claim(ctx, stationcore.ClaimInput{
		NamespaceID:         namespace,
		RequestID:           req.RequestId,
		AddressID:           req.AddressId,
		MaxUnits:            req.MaxUnits,
		MaxBytes:            req.MaxBytes,
		VisibilityTimeoutMs: req.VisibilityTimeoutMs,
		WaitTimeoutMs:       req.WaitTimeoutMs,
	})
	if err != nil {
		return nil, err
	}
	response := &stationv1.ClaimResponse{
		ClaimEpoch:           out.ClaimEpoch,
		ClaimExpiresAtUnixMs: out.ClaimExpiresAtUnixMs,
	}
	if out.ClaimID != "" {
		response.ClaimId = &out.ClaimID
	}
	for _, unit := range out.Units {
		response.Units = append(response.Units, &stationv1.ClaimedUnit{
			UnitId:           unit.UnitID,
			Payload:          unit.Payload,
			AcceptedAtUnixMs: unit.AcceptedAtUnixMs,
			ExpiresAtUnixMs:  unit.ExpiresAtUnixMs,
			DeliveryAttempt:  unit.DeliveryAttempt,
		})
	}
	return response, nil
}

func (server *Server) settleProto(ctx context.Context, namespace string, req *stationv1.SettleRequest) (*stationv1.SettleResponse, error) {
	if err := requireNamespace(server, namespace); err != nil {
		return nil, err
	}
	if server.core == nil {
		return nil, stationFailure{Reason: "INTERNAL"}
	}
	entries := make([]stationcore.SettleEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, stationcore.SettleEntry{
			UnitID:      entry.UnitId,
			Action:      mapSettleAction(entry.Action),
			DelayMs:     entry.DelayMs,
			ExtensionMs: entry.ExtensionMs,
		})
	}
	out, err := server.core.Settle(ctx, stationcore.SettleInput{
		NamespaceID: namespace,
		RequestID:   req.RequestId,
		ClaimID:     req.ClaimId,
		ClaimEpoch:  req.ClaimEpoch,
		Entries:     entries,
	})
	if err != nil {
		return nil, err
	}
	response := &stationv1.SettleResponse{}
	for _, unit := range out.Units {
		response.Units = append(response.Units, &stationv1.SettledUnit{
			UnitId:            unit.UnitID,
			Outcome:           mapSettleOutcome(unit.Outcome),
			EffectiveAtUnixMs: unit.EffectiveAtUnixMs,
		})
	}
	return response, nil
}

func mapSettleAction(action stationv1.SettleAction) stationcore.SettleAction {
	switch action {
	case stationv1.SettleAction_SETTLE_ACTION_COMPLETE:
		return stationcore.SettleComplete
	case stationv1.SettleAction_SETTLE_ACTION_RELEASE:
		return stationcore.SettleRelease
	case stationv1.SettleAction_SETTLE_ACTION_EXTEND:
		return stationcore.SettleExtend
	default:
		return 0
	}
}

func profileLimitVector(limits stationcore.ProfileLimits) *stationv1.ResourceVector {
	return &stationv1.ResourceVector{
		Addresses:     limits.MaxAddressesPerNamespace,
		Units:         limits.MaxUnitsPerNamespace,
		RetainedBytes: limits.MaxRetainedBytesPerNamespace,
		ActiveClaims:  limits.MaxActiveClaimsPerNamespace,
	}
}

func hasDeadline(ctx context.Context, max time.Duration) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return false
	}
	return time.Until(deadline) <= max+time.Second
}

// OpenListener opens one TCP or Unix listener from transport config.
func OpenListener(config TransportConfig) (net.Listener, error) {
	switch config.Mode {
	case "tcp":
		return net.Listen("tcp", config.Listen)
	case "unix":
		if err := prepareUnixSocketPath(config.SocketPath); err != nil {
			return nil, err
		}
		listener, err := net.Listen("unix", config.SocketPath)
		if err != nil {
			return nil, err
		}
		if err := os.Chmod(config.SocketPath, 0o600); err != nil {
			_ = listener.Close()
			return nil, err
		}
		return listener, nil
	default:
		return nil, fmt.Errorf("unsupported transport mode %q", config.Mode)
	}
}

func validateTrustedUnixSocketDir(socketPath string, createMissing bool) (string, error) {
	cleanPath := filepath.Clean(socketPath)
	if socketPath == "" || !filepath.IsAbs(cleanPath) {
		return "", fmt.Errorf("unix socket path must be absolute")
	}
	dir := filepath.Dir(cleanPath)
	dirInfo, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) && createMissing {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return "", err
		}
		dirInfo, err = os.Lstat(dir)
	}
	if err != nil {
		return "", err
	}
	if !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("unix socket parent is not a directory")
	}
	if dirInfo.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("unix socket parent directory must be owner-only (mode 0700)")
	}
	dirStat, ok := dirInfo.Sys().(*syscall.Stat_t)
	if !ok || dirStat.Uid != uint32(os.Geteuid()) {
		return "", fmt.Errorf("unix socket parent directory must be owned by current user")
	}
	return cleanPath, nil
}

func prepareUnixSocketPath(socketPath string) error {
	cleanPath, err := validateTrustedUnixSocketDir(socketPath, true)
	if err != nil {
		return err
	}
	info, err := os.Lstat(cleanPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode().Type() != os.ModeSocket {
		return fmt.Errorf("refusing to remove non-socket path %q", cleanPath)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return fmt.Errorf("refusing to remove socket not owned by current user")
	}
	conn, dialErr := net.DialTimeout("unix", cleanPath, 200*time.Millisecond)
	if dialErr == nil {
		_ = conn.Close()
		return fmt.Errorf("unix socket %q is actively listening", cleanPath)
	}
	if !errors.Is(dialErr, syscall.ECONNREFUSED) {
		return fmt.Errorf("refusing to remove unverified socket %q: %w", cleanPath, dialErr)
	}
	return os.Remove(cleanPath)
}

// VerifyUnixSocketPath checks that socketPath resides in a trusted owner-only
// directory and refers to an owner-only Unix domain socket owned by the current user.
func VerifyUnixSocketPath(socketPath string) (string, error) {
	cleanPath, err := validateTrustedUnixSocketDir(socketPath, false)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(cleanPath)
	if err != nil {
		return "", err
	}
	if info.Mode().Type() != os.ModeSocket {
		return "", fmt.Errorf("path %q is not a unix socket", cleanPath)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("unix socket %q must be owner-only (mode 0600)", cleanPath)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return "", fmt.Errorf("unix socket %q must be owned by current user", cleanPath)
	}
	return cleanPath, nil
}

// DialVerifiedUnix validates the socket directory, socket metadata, and OS
// peer credentials before returning the connected Unix socket to a caller.
func DialVerifiedUnix(ctx context.Context, socketPath string) (net.Conn, error) {
	cleanPath, err := VerifyUnixSocketPath(socketPath)
	if err != nil {
		return nil, err
	}
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", cleanPath)
	if err != nil {
		return nil, err
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("unexpected non-unix connection")
	}
	if err := verifyUnixPeerUID(unixConn, uint32(os.Geteuid())); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// OpenTCPListener opens one TCP listener for health or metrics.
func OpenTCPListener(listen string) (net.Listener, error) {
	return net.Listen("tcp", listen)
}

// LoadTLSConfig loads one TLS 1.3 server configuration.
func LoadTLSConfig(certificateFile, privateKeyFile string) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certificateFile, privateKeyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion:   tls.VersionTLS13,
		NextProtos:   []string{"h2"},
	}, nil
}

// Direct request types for ctl and focused tests.

type ReserveRequest struct {
	RequestID string
	AddressID string
	TTLMs     uint64
}

type ReserveResponse struct {
	AddressID       string
	ExpiresAtUnixMs int64
	Created         bool
}

type SubmitRequest struct {
	RequestID string
	AddressID string
	Payload   []byte
	TTLMs     uint64
}

type SubmitResponse struct {
	UnitID           string
	AcceptedAtUnixMs int64
	ExpiresAtUnixMs  int64
}

type ClaimRequest struct {
	RequestID           string
	AddressID           string
	MaxUnits            uint32
	MaxBytes            uint64
	VisibilityTimeoutMs uint64
	WaitTimeoutMs       uint64
}

type ClaimResponse struct {
	ClaimID              string
	ClaimEpoch           uint64
	ClaimExpiresAtUnixMs int64
	Units                []ClaimedUnit
}

type ClaimedUnit struct {
	UnitID           string
	Payload          []byte
	AcceptedAtUnixMs int64
	ExpiresAtUnixMs  int64
	DeliveryAttempt  uint32
}

type SettleEntry struct {
	UnitID      string
	Action      int
	DelayMs     uint64
	ExtensionMs uint64
}

type SettleRequest struct {
	RequestID  string
	ClaimID    string
	ClaimEpoch uint64
	Entries    []SettleEntry
}

type SettledUnit struct {
	UnitID            string
	Outcome           string
	EffectiveAtUnixMs int64
}

type SettleResponse struct {
	Units []SettledUnit
}

func (server *Server) Reserve(ctx context.Context, namespace string, req ReserveRequest) (ReserveResponse, error) {
	if err := server.rejectIfDraining("Reserve"); err != nil {
		return ReserveResponse{}, err
	}
	if err := requireNamespace(server, namespace); err != nil {
		return ReserveResponse{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return ReserveResponse{}, stationFailure{Reason: "DEADLINE_REQUIRED"}
	}
	if server.core == nil {
		return ReserveResponse{}, stationFailure{Reason: "INTERNAL"}
	}
	out, err := server.core.Reserve(ctx, stationcore.ReserveInput{
		NamespaceID: namespace,
		RequestID:   req.RequestID,
		AddressID:   req.AddressID,
		TTLMs:       req.TTLMs,
	})
	if err != nil {
		return ReserveResponse{}, err
	}
	return ReserveResponse{AddressID: out.AddressID, ExpiresAtUnixMs: out.ExpiresAtUnixMs, Created: out.Created}, nil
}

func (server *Server) Submit(ctx context.Context, namespace string, req SubmitRequest) (SubmitResponse, error) {
	if err := server.rejectIfDraining("Submit"); err != nil {
		return SubmitResponse{}, err
	}
	if err := requireNamespace(server, namespace); err != nil {
		return SubmitResponse{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return SubmitResponse{}, stationFailure{Reason: "DEADLINE_REQUIRED"}
	}
	if server.core == nil {
		return SubmitResponse{}, stationFailure{Reason: "INTERNAL"}
	}
	out, err := server.core.Submit(ctx, stationcore.SubmitInput{
		NamespaceID: namespace,
		RequestID:   req.RequestID,
		AddressID:   req.AddressID,
		Payload:     req.Payload,
		TTLMs:       req.TTLMs,
	})
	if err != nil {
		return SubmitResponse{}, err
	}
	return SubmitResponse{UnitID: out.UnitID, AcceptedAtUnixMs: out.AcceptedAtUnixMs, ExpiresAtUnixMs: out.ExpiresAtUnixMs}, nil
}

func (server *Server) Claim(ctx context.Context, namespace string, req ClaimRequest) (ClaimResponse, error) {
	if err := server.rejectIfDraining("Claim"); err != nil {
		return ClaimResponse{}, err
	}
	if err := requireNamespace(server, namespace); err != nil {
		return ClaimResponse{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return ClaimResponse{}, stationFailure{Reason: "DEADLINE_REQUIRED"}
	}
	if server.core == nil {
		return ClaimResponse{}, stationFailure{Reason: "INTERNAL"}
	}
	out, err := server.core.Claim(ctx, stationcore.ClaimInput{
		NamespaceID:         namespace,
		RequestID:           req.RequestID,
		AddressID:           req.AddressID,
		MaxUnits:            req.MaxUnits,
		MaxBytes:            req.MaxBytes,
		VisibilityTimeoutMs: req.VisibilityTimeoutMs,
		WaitTimeoutMs:       req.WaitTimeoutMs,
	})
	if err != nil {
		return ClaimResponse{}, err
	}
	units := make([]ClaimedUnit, 0, len(out.Units))
	for _, unit := range out.Units {
		units = append(units, ClaimedUnit{
			UnitID: unit.UnitID, Payload: unit.Payload,
			AcceptedAtUnixMs: unit.AcceptedAtUnixMs, ExpiresAtUnixMs: unit.ExpiresAtUnixMs,
			DeliveryAttempt: unit.DeliveryAttempt,
		})
	}
	return ClaimResponse{
		ClaimID: out.ClaimID, ClaimEpoch: out.ClaimEpoch,
		ClaimExpiresAtUnixMs: out.ClaimExpiresAtUnixMs, Units: units,
	}, nil
}

func (server *Server) Settle(ctx context.Context, namespace string, req SettleRequest) (SettleResponse, error) {
	if err := requireNamespace(server, namespace); err != nil {
		return SettleResponse{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return SettleResponse{}, stationFailure{Reason: "DEADLINE_REQUIRED"}
	}
	if server.core == nil {
		return SettleResponse{}, stationFailure{Reason: "INTERNAL"}
	}
	entries := make([]stationcore.SettleEntry, 0, len(req.Entries))
	for _, entry := range req.Entries {
		entries = append(entries, stationcore.SettleEntry{
			UnitID: entry.UnitID, Action: stationcore.SettleAction(entry.Action),
			DelayMs: entry.DelayMs, ExtensionMs: entry.ExtensionMs,
		})
	}
	out, err := server.core.Settle(ctx, stationcore.SettleInput{
		NamespaceID: namespace, RequestID: req.RequestID,
		ClaimID: req.ClaimID, ClaimEpoch: req.ClaimEpoch, Entries: entries,
	})
	if err != nil {
		return SettleResponse{}, err
	}
	units := make([]SettledUnit, 0, len(out.Units))
	for _, unit := range out.Units {
		units = append(units, SettledUnit{UnitID: unit.UnitID, Outcome: unit.Outcome, EffectiveAtUnixMs: unit.EffectiveAtUnixMs})
	}
	return SettleResponse{Units: units}, nil
}
