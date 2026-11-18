package nativeapi

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	"github.com/LicoLand/BadTower/internal/stationcore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func openTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	namespace := "ns_WWWWWWWWWWWWWWWWWWWWWW"
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  t.TempDir(),
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, testClock{t: time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("Open core: %v", err)
	}
	t.Cleanup(func() { _ = core.Close() })
	auth := &Authenticator{snapshot: AuthSnapshot{
		NamespaceByDigest: map[string]string{
			"test-token-digest-AAAAAAAAAAAAAAAAAAAAAAAAAAA": namespace,
		},
	}}
	server := NewServer(core, Config{
		Schema:  ConfigSchema,
		Profile: ProfileSingleNode,
	}, auth)
	return server, namespace
}

type testClock struct{ t time.Time }

func (clock testClock) NowUnixMs() int64 { return clock.t.UnixMilli() }

func (clock testClock) MonotonicNow() time.Duration { return 0 }

func TestReserveSubmitClaimSettleUnaryFlow(t *testing.T) {
	t.Parallel()
	server, namespace := openTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reserved, err := server.Reserve(ctx, namespace, ReserveRequest{
		RequestID: "rq_apiFlowReserve0001",
		TTLMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	submitted, err := server.Submit(ctx, namespace, SubmitRequest{
		RequestID: "rq_apiFlowSubmit00001",
		AddressID: reserved.AddressID,
		Payload:   []byte("native"),
		TTLMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	claimed, err := server.Claim(ctx, namespace, ClaimRequest{
		RequestID:           "rq_apiFlowClaim000001",
		AddressID:           reserved.AddressID,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claimed.Units) != 1 || claimed.Units[0].UnitID != submitted.UnitID {
		t.Fatalf("Claim payload mismatch: %#v", claimed)
	}

	settled, err := server.Settle(ctx, namespace, SettleRequest{
		RequestID:  "rq_apiFlowSettle0001",
		ClaimID:    claimed.ClaimID,
		ClaimEpoch: claimed.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: submitted.UnitID,
			Action: 1,
		}},
	})
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(settled.Units) != 1 {
		t.Fatalf("Settle units = %d", len(settled.Units))
	}
}

func TestAuthMissingBearerFailsClosed(t *testing.T) {
	t.Parallel()
	server, _ := openTestServer(t)
	_, err := server.Reserve(context.Background(), "", ReserveRequest{
		RequestID: "rq_apiAuthMissing001",
		TTLMs:     60_000,
	})
	if !isFailureReason(err, "UNAUTHENTICATED") {
		t.Fatalf("missing auth: err=%v", err)
	}
}

func TestAuthUnknownTokenDoesNotRevealNamespace(t *testing.T) {
	t.Parallel()
	server, _ := openTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := server.Submit(ctx, "ns_unknownNamespace000000", SubmitRequest{
		RequestID: "rq_apiAuthUnknown001",
		AddressID: "sa_unknownAddress00000000",
		Payload:   []byte("x"),
		TTLMs:     60_000,
	})
	if !isFailureReason(err, "UNAUTHENTICATED") && !isFailureReason(err, "PERMISSION_DENIED") {
		t.Fatalf("unknown token must fail closed without existence leak: err=%v", err)
	}
}

func TestDeadlineRequiredWithoutClientDeadline(t *testing.T) {
	t.Parallel()
	server, namespace := openTestServer(t)
	ctx := context.Background()
	_, err := server.Claim(ctx, namespace, ClaimRequest{
		RequestID:           "rq_apiDeadlineMissing1",
		AddressID:           "sa_placeholder00000000000",
		MaxUnits:            1,
		MaxBytes:            1024,
		VisibilityTimeoutMs: 30_000,
		WaitTimeoutMs:       1000,
	})
	if !isFailureReason(err, "DEADLINE_REQUIRED") {
		t.Fatalf("Claim without deadline: err=%v", err)
	}
}

func TestClaimEmptyWaitReturnsSuccessWithoutClaim(t *testing.T) {
	t.Parallel()
	server, namespace := openTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reserved, err := server.Reserve(ctx, namespace, ReserveRequest{
		RequestID: "rq_apiEmptyWaitRes001",
		TTLMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	claimed, err := server.Claim(ctx, namespace, ClaimRequest{
		RequestID:           "rq_apiEmptyWaitClm001",
		AddressID:           reserved.AddressID,
		MaxUnits:            1,
		MaxBytes:            1024,
		VisibilityTimeoutMs: 30_000,
		WaitTimeoutMs:       50,
	})
	if err != nil {
		t.Fatalf("empty wait Claim: %v", err)
	}
	if claimed.ClaimID != "" || len(claimed.Units) != 0 {
		t.Fatalf("empty wait must succeed without claim: %#v", claimed)
	}
}

func TestDrainRejectsNewReserveSubmitClaim(t *testing.T) {
	t.Parallel()
	server, namespace := openTestServer(t)
	server.BeginDrain()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := server.Reserve(ctx, namespace, ReserveRequest{
		RequestID: "rq_apiDrainReserve01",
		TTLMs:     60_000,
	})
	if !isFailureReason(err, "DRAINING") {
		t.Fatalf("Reserve during drain: err=%v", err)
	}
	if server.Lifecycle() != LifecycleDraining {
		t.Fatalf("lifecycle = %v, want DRAINING", server.Lifecycle())
	}
}

func TestFailureMappingNeverEchoesIdentifiers(t *testing.T) {
	t.Parallel()
	server, namespace := openTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sensitive := "sa_sensitiveAddress000000"
	_, err := server.Submit(ctx, namespace, SubmitRequest{
		RequestID: "rq_apiFailureEcho001",
		AddressID: sensitive,
		Payload:   []byte("x"),
		TTLMs:     60_000,
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	if contains(err.Error(), sensitive) {
		t.Fatalf("failure echoed identifier: %q", err)
	}
}

// grpcFixture owns one in-process server and generated client over a Unix
// socket, exercising the real generated binding (C-02, REQ-015).
type grpcFixture struct {
	server    *Server
	client    stationv1.StationClient
	conn      *grpc.ClientConn
	namespace string
	token     string
	cancel    context.CancelFunc
}

// shortSocketDir returns a short dedicated temp directory for unix socket
// paths. t.TempDir() embeds the full test name (up to ~45 chars) plus a
// numeric suffix, which pushes a socket path past the platform sun_path limit
// (~104 bytes on macOS) under the parallel suite. A "bt" prefix plus a short
// numeric suffix keeps the worst-case path well under 90 bytes.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "bt")
	if err != nil {
		t.Fatalf("short socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func startGRPCFixture(t *testing.T) *grpcFixture {
	t.Helper()
	dir := t.TempDir()
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  dir,
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, testClock{t: time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("Open core: %v", err)
	}
	namespace := "ns_aaaaaaaaaaaaaaaaaaaaaa"
	tokenMaterial := make([]byte, 32)
	if _, err := rand.Read(tokenMaterial); err != nil {
		t.Fatalf("token material: %v", err)
	}
	token, digest, err := GenerateToken(tokenMaterial)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	config := Config{
		Schema:  ConfigSchema,
		Profile: ProfileSingleNode,
		Transport: TransportConfig{
			Mode:       "unix",
			SocketPath: filepath.Join(shortSocketDir(t), "station.sock"),
		},
		Health:  ListenerConfig{Listen: "127.0.0.1:0"},
		Metrics: ListenerConfig{Listen: "127.0.0.1:0"},
		Namespaces: []NamespaceConfig{{
			ID: namespace,
			Credentials: []CredentialConfig{{
				SHA256:    digest,
				NotBefore: time.Now().Add(-time.Hour),
				NotAfter:  time.Now().Add(24 * time.Hour),
			}},
		}},
	}
	auth := NewAuthenticator(config)
	server := NewServer(core, Config{Schema: ConfigSchema, Profile: ProfileSingleNode}, auth)
	server.SetLifecycleRunning()
	listener, err := OpenListener(config.Transport)
	if err != nil {
		t.Fatalf("OpenListener: %v", err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	go func() { _ = server.ServeData(serveCtx, listener, nil) }()
	conn, err := grpc.NewClient(
		"unix://"+config.Transport.SocketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = conn.Close()
		_ = core.Close()
	})
	return &grpcFixture{
		server:    server,
		client:    stationv1.NewStationClient(conn),
		conn:      conn,
		namespace: namespace,
		token:     token,
		cancel:    cancel,
	}
}

func (fixture *grpcFixture) authed(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+fixture.token)), cancel
}

func TestGRPCReserveSubmitClaimSettleRoundTrip(t *testing.T) {
	t.Parallel()
	fixture := startGRPCFixture(t)

	ctx, cancel := fixture.authed(5 * time.Second)
	defer cancel()
	reserved, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
		RequestId: "rq_grpcFlowReserve01",
		TtlMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if reserved.AddressId == "" || !reserved.Created || reserved.ExpiresAtUnixMs <= 0 {
		t.Fatalf("unexpected Reserve response: %#v", reserved)
	}

	submitted, err := fixture.client.Submit(ctx, &stationv1.SubmitRequest{
		RequestId: "rq_grpcFlowSubmit01",
		AddressId: reserved.AddressId,
		Payload:   []byte("native round trip"),
		TtlMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if submitted.UnitId == "" || submitted.AcceptedAtUnixMs <= 0 {
		t.Fatalf("unexpected Submit response: %#v", submitted)
	}

	claimCtx, claimCancel := fixture.authed(30 * time.Second)
	defer claimCancel()
	claimed, err := fixture.client.Claim(claimCtx, &stationv1.ClaimRequest{
		RequestId:           "rq_grpcFlowClaim01",
		AddressId:           reserved.AddressId,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claimed.ClaimId == nil || claimed.ClaimEpoch == 0 || len(claimed.Units) != 1 {
		t.Fatalf("unexpected Claim response: %#v", claimed)
	}
	if claimed.Units[0].UnitId != submitted.UnitId || string(claimed.Units[0].Payload) != "native round trip" {
		t.Fatalf("claimed unit mismatch: %#v", claimed.Units[0])
	}

	settled, err := fixture.client.Settle(ctx, &stationv1.SettleRequest{
		RequestId:  "rq_grpcFlowSettle01",
		ClaimId:    *claimed.ClaimId,
		ClaimEpoch: claimed.ClaimEpoch,
		Entries: []*stationv1.SettleEntry{{
			UnitId: submitted.UnitId,
			Action: stationv1.SettleAction_SETTLE_ACTION_COMPLETE,
		}},
	})
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(settled.Units) != 1 ||
		settled.Units[0].Outcome != stationv1.SettleOutcome_SETTLE_OUTCOME_COMPLETED {
		t.Fatalf("unexpected Settle response: %#v", settled)
	}
}

func TestGRPCClosedFailureTaxonomy(t *testing.T) {
	t.Parallel()
	fixture := startGRPCFixture(t)

	ctx, cancel := fixture.authed(5 * time.Second)
	defer cancel()
	reserved, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
		RequestId: "rq_taxReserve0002",
		TtlMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	submitted, err := fixture.client.Submit(ctx, &stationv1.SubmitRequest{
		RequestId: "rq_taxSubmit000001",
		AddressId: reserved.AddressId,
		Payload:   []byte("x"),
		TtlMs:     60_000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	cases := []struct {
		name     string
		call     func() error
		wantCode codes.Code
		wantErr  string
	}{
		{
			name: "missing deadline",
			call: func() error {
				_, err := fixture.client.Reserve(context.Background(), &stationv1.ReserveRequest{
					RequestId: "rq_taxDeadline001",
					TtlMs:     60_000,
				})
				return err
			},
			wantCode: codes.InvalidArgument,
			wantErr:  "DEADLINE_REQUIRED",
		},
		{
			name: "missing bearer",
			call: func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
					RequestId: "rq_taxAuthMissing1",
					TtlMs:     60_000,
				})
				return err
			},
			wantCode: codes.Unauthenticated,
			wantErr:  "UNAUTHENTICATED",
		},
		{
			name: "unknown bearer",
			call: func() error {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer synthetic-unknown-token"))
				_, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
					RequestId: "rq_taxAuthUnknown01",
					TtlMs:     60_000,
				})
				return err
			},
			wantCode: codes.Unauthenticated,
			wantErr:  "UNAUTHENTICATED",
		},
		{
			name: "address not found",
			call: func() error {
				ctx, cancel := fixture.authed(5 * time.Second)
				defer cancel()
				_, err := fixture.client.Submit(ctx, &stationv1.SubmitRequest{
					RequestId: "rq_taxNotFound001",
					AddressId: "sa_aaaaaaaaaaaaaaaaaaaaab",
					Payload:   []byte("x"),
					TtlMs:     60_000,
				})
				return err
			},
			wantCode: codes.NotFound,
			wantErr:  "ADDRESS_NOT_FOUND",
		},
		{
			name: "idempotency conflict",
			call: func() error {
				ctx, cancel := fixture.authed(5 * time.Second)
				defer cancel()
				if _, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
					RequestId: "rq_taxConflict001",
					TtlMs:     86_400_000,
				}); err != nil {
					return err
				}
				_, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
					RequestId: "rq_taxConflict001",
					TtlMs:     3_600_000,
				})
				return err
			},
			wantCode: codes.AlreadyExists,
			wantErr:  "IDEMPOTENCY_CONFLICT",
		},
		{
			name: "stale claim epoch",
			call: func() error {
				ctx, cancel := fixture.authed(5 * time.Second)
				defer cancel()
				claimCtx, claimCancel := fixture.authed(30 * time.Second)
				defer claimCancel()
				claimed, err := fixture.client.Claim(claimCtx, &stationv1.ClaimRequest{
					RequestId:           "rq_taxClaim000001",
					AddressId:           reserved.AddressId,
					MaxUnits:            1,
					MaxBytes:            1 << 20,
					VisibilityTimeoutMs: 30_000,
				})
				if err != nil {
					return err
				}
				_, err = fixture.client.Settle(ctx, &stationv1.SettleRequest{
					RequestId:  "rq_taxStaleSettle1",
					ClaimId:    *claimed.ClaimId,
					ClaimEpoch: claimed.ClaimEpoch + 1,
					Entries: []*stationv1.SettleEntry{{
						UnitId: submitted.UnitId,
						Action: stationv1.SettleAction_SETTLE_ACTION_COMPLETE,
					}},
				})
				return err
			},
			wantCode: codes.Aborted,
			wantErr:  "CLAIM_STALE",
		},
		{
			name: "oversized payload",
			call: func() error {
				ctx, cancel := fixture.authed(5 * time.Second)
				defer cancel()
				_, err := fixture.client.Submit(ctx, &stationv1.SubmitRequest{
					RequestId: "rq_taxOversize001",
					AddressId: reserved.AddressId,
					Payload:   make([]byte, 1<<20+1),
					TtlMs:     60_000,
				})
				return err
			},
			wantCode: codes.InvalidArgument,
			wantErr:  "INVALID_REQUEST",
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			err := testCase.call()
			if err == nil {
				t.Fatalf("expected %s", testCase.wantErr)
			}
			st, ok := status.FromError(err)
			if !ok {
				t.Fatalf("non-status error: %v", err)
			}
			if st.Code() != testCase.wantCode {
				t.Fatalf("code = %s, want %s (msg %q)", st.Code(), testCase.wantCode, st.Message())
			}
			if st.Message() != testCase.wantErr {
				t.Fatalf("message = %q, want fixed reason %q", st.Message(), testCase.wantErr)
			}
			var detail *stationv1.StationFailure
			for _, value := range st.Details() {
				if failure, ok := value.(*stationv1.StationFailure); ok {
					detail = failure
				}
			}
			if detail == nil || detail.Reason != testCase.wantErr {
				t.Fatalf("missing closed StationFailure detail: %#v", st.Details())
			}
		})
	}
}

func TestGRPCUnknownMethodReturnsUnimplemented(t *testing.T) {
	t.Parallel()
	fixture := startGRPCFixture(t)
	ctx, cancel := fixture.authed(5 * time.Second)
	defer cancel()
	err := fixture.conn.Invoke(ctx, "/badtower.station.v1.Station/NotAnRpc",
		&stationv1.ReserveRequest{}, &stationv1.ReserveResponse{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("unknown method: err=%v", err)
	}
}

func TestGRPCGetCapacityReturnsOwnAggregateOnly(t *testing.T) {
	t.Parallel()
	fixture := startGRPCFixture(t)
	ctx, cancel := fixture.authed(5 * time.Second)
	defer cancel()

	reserved, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
		RequestId: "rq_capacityReserve01",
		TtlMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := fixture.client.Submit(ctx, &stationv1.SubmitRequest{
		RequestId: "rq_capacitySubmit01",
		AddressId: reserved.AddressId,
		Payload:   []byte("c"),
		TtlMs:     60_000,
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	claimCtx, claimCancel := fixture.authed(30 * time.Second)
	defer claimCancel()
	claimed, err := fixture.client.Claim(claimCtx, &stationv1.ClaimRequest{
		RequestId:           "rq_capacityClaim001",
		AddressId:           reserved.AddressId,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	capacity, err := fixture.client.GetCapacity(ctx, &stationv1.GetCapacityRequest{})
	if err != nil {
		t.Fatalf("GetCapacity: %v", err)
	}
	if capacity.ProfileId != "single-node-general-v1" {
		t.Fatalf("profile = %q", capacity.ProfileId)
	}
	if capacity.Pressure != stationv1.Pressure_PRESSURE_NORMAL || capacity.RetryAfterMs != 0 {
		t.Fatalf("unexpected pressure state: %#v", capacity)
	}
	usage := capacity.NamespaceUsage
	if usage == nil || usage.Addresses < 1 || usage.Units < 1 || usage.ActiveClaims < 1 {
		t.Fatalf("usage must reflect only this namespace: %#v", usage)
	}
	limit := capacity.NamespaceLimit
	if limit == nil || limit.Addresses < usage.Addresses || limit.Units < usage.Units {
		t.Fatalf("limits must cover own usage: %#v", limit)
	}
	if capacity.NamespaceUsage.IdempotencyRecords == 0 {
		t.Fatalf("idempotency accounting missing: %#v", usage)
	}

	if _, err := fixture.client.Settle(ctx, &stationv1.SettleRequest{
		RequestId:  "rq_capacitySettle01",
		ClaimId:    *claimed.ClaimId,
		ClaimEpoch: claimed.ClaimEpoch,
		Entries: []*stationv1.SettleEntry{{
			UnitId: claimed.Units[0].UnitId,
			Action: stationv1.SettleAction_SETTLE_ACTION_COMPLETE,
		}},
	}); err != nil {
		t.Fatalf("Settle: %v", err)
	}
	after, err := fixture.client.GetCapacity(ctx, &stationv1.GetCapacityRequest{})
	if err != nil {
		t.Fatalf("GetCapacity after settle: %v", err)
	}
	if after.NamespaceUsage.ActiveClaims != 0 || after.NamespaceUsage.Units != 0 {
		t.Fatalf("settled units and claim must release counters: %#v", after.NamespaceUsage)
	}
}

func TestDrainKeepsSettleAndGetCapacityAvailable(t *testing.T) {
	t.Parallel()
	fixture := startGRPCFixture(t)
	ctx, cancel := fixture.authed(5 * time.Second)
	defer cancel()
	reserved, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
		RequestId: "rq_drainReserve0001",
		TtlMs:     86_400_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := fixture.client.Submit(ctx, &stationv1.SubmitRequest{
		RequestId: "rq_drainSubmit00001",
		AddressId: reserved.AddressId,
		Payload:   []byte("drain"),
		TtlMs:     60_000,
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	claimCtx, claimCancel := fixture.authed(30 * time.Second)
	defer claimCancel()
	claimed, err := fixture.client.Claim(claimCtx, &stationv1.ClaimRequest{
		RequestId:           "rq_drainClaim000001",
		AddressId:           reserved.AddressId,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	fixture.server.BeginDrain()

	if _, err := fixture.client.Reserve(ctx, &stationv1.ReserveRequest{
		RequestId: "rq_drainBlockedRes1",
		TtlMs:     60_000,
	}); err == nil {
		t.Fatal("Reserve must fail during drain")
	}
	settled, err := fixture.client.Settle(ctx, &stationv1.SettleRequest{
		RequestId:  "rq_drainSettle00001",
		ClaimId:    *claimed.ClaimId,
		ClaimEpoch: claimed.ClaimEpoch,
		Entries: []*stationv1.SettleEntry{{
			UnitId: claimed.Units[0].UnitId,
			Action: stationv1.SettleAction_SETTLE_ACTION_COMPLETE,
		}},
	})
	if err != nil {
		t.Fatalf("Settle must drain during shutdown: %v", err)
	}
	if len(settled.Units) != 1 {
		t.Fatalf("drain settle units = %d", len(settled.Units))
	}
	if _, err := fixture.client.GetCapacity(ctx, &stationv1.GetCapacityRequest{}); err != nil {
		t.Fatalf("GetCapacity must drain during shutdown: %v", err)
	}
}

func TestHealthClosedServicesAndWatchUnimplemented(t *testing.T) {
	t.Parallel()
	fixture := startGRPCFixture(t)
	healthServer := NewHealthServer(fixture.server)
	// The test owns a separate registry; simulate startup completion.
	healthServer.SetStartup(true)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("health listener: %v", err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = healthServer.Serve(serveCtx, listener) }()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("health dial: %v", err)
	}
	defer conn.Close()
	client := healthpb.NewHealthClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, service := range []string{"", HealthServiceStartup, HealthServiceLiveness, HealthServiceReadiness} {
		response, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: service})
		if err != nil {
			t.Fatalf("Check(%q): %v", service, err)
		}
		if response.Status != healthpb.HealthCheckResponse_SERVING {
			t.Fatalf("Check(%q) = %s, want SERVING", service, response.Status)
		}
	}

	fixture.server.BeginDrain()
	// Readiness is derived from the shared process/core state while
	// startup/liveness stay SERVING.
	readiness, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: HealthServiceReadiness})
	if err != nil {
		t.Fatalf("readiness after drain: %v", err)
	}
	if readiness.Status != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("readiness must fall before drain: %s", readiness.Status)
	}
	liveness, err := client.Check(ctx, &healthpb.HealthCheckRequest{Service: HealthServiceLiveness})
	if err != nil {
		t.Fatalf("liveness after drain: %v", err)
	}
	if liveness.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("liveness must survive drain: %s", liveness.Status)
	}

	err = conn.Invoke(ctx, "/grpc.health.v1.Health/Watch",
		&healthpb.HealthCheckRequest{}, &healthpb.HealthCheckResponse{})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unimplemented {
		t.Fatalf("Watch must return UNIMPLEMENTED: %v", err)
	}

	noDeadline, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{Service: HealthServiceLiveness})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.InvalidArgument || st.Message() != "DEADLINE_REQUIRED" {
		t.Fatalf("health without deadline: err=%v noDeadline=%v", err, noDeadline)
	}
}

func TestTLSConfigEnforcesTLS13AndALPNH2(t *testing.T) {
	t.Parallel()
	certPath, keyPath := syntheticServerCertificate(t)
	config, err := LoadTLSConfig(certPath, keyPath)
	if err != nil {
		t.Fatalf("LoadTLSConfig: %v", err)
	}
	if config.MinVersion != tls.VersionTLS13 {
		t.Fatalf("MinVersion = %#x, want TLS 1.3", config.MinVersion)
	}
	if !slices.Equal(config.NextProtos, []string{"h2"}) {
		t.Fatalf("NextProtos = %v, want [h2]", config.NextProtos)
	}
	if len(config.Certificates) != 1 {
		t.Fatalf("certificates = %d", len(config.Certificates))
	}
	if _, err := LoadTLSConfig(filepath.Join(t.TempDir(), "missing.pem"), keyPath); err == nil {
		t.Fatal("missing certificate file must fail")
	}
}

func TestUnixSocketMode0600(t *testing.T) {
	t.Parallel()
	socketPath := filepath.Join(shortSocketDir(t), "station.sock")
	listener, err := OpenListener(TransportConfig{Mode: "unix", SocketPath: socketPath})
	if err != nil {
		t.Fatalf("OpenListener: %v", err)
	}
	defer listener.Close()
	info, err := os.Stat(socketPath)
	if err != nil {
		t.Fatalf("socket stat: %v", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("unix socket mode = %o, want 0600", info.Mode().Perm())
	}
	if _, err := OpenListener(TransportConfig{Mode: "unsupported"}); err == nil {
		t.Fatal("unsupported transport mode must fail")
	}
}

func TestFailureProjectionFixedReasonOnly(t *testing.T) {
	t.Parallel()
	server, namespace := openTestServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sensitive := "sa_sensitiveIdentifier000"
	_, err := server.Submit(ctx, namespace, SubmitRequest{
		RequestID: "rq_projectSensitive001",
		AddressID: sensitive,
		Payload:   []byte("x"),
		TTLMs:     60_000,
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	if strings.Contains(err.Error(), sensitive) {
		t.Fatalf("failure echoed identifier: %q", err)
	}
	if failure, ok := stationcore.IsFailure(err); !ok || failure.Reason != stationcore.ReasonAddressNotFound {
		t.Fatalf("failure must be the fixed reason ADDRESS_NOT_FOUND: %v", err)
	}

	wrapped := errors.New("sensitive-local-context")
	mapped := mapError(wrapped)
	st, ok := status.FromError(mapped)
	if !ok || st.Code() != codes.Internal || st.Message() != "INTERNAL" {
		t.Fatalf("unknown failure must project fixed INTERNAL: %v", mapped)
	}
	if strings.Contains(mapped.Error(), "sensitive") {
		t.Fatalf("projection leaked context: %q", mapped)
	}
}

func syntheticServerCertificate(t *testing.T) (string, string) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey, privateKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certPath, keyPath
}

func isFailureReason(err error, reason string) bool {
	if err == nil {
		return false
	}
	var failure stationFailure
	if errors.As(err, &failure) {
		return failure.Reason == reason
	}
	if failure, ok := stationcore.IsFailure(err); ok {
		return string(failure.Reason) == reason
	}
	return false
}

func contains(value, fragment string) bool {
	return len(fragment) > 0 && len(value) >= len(fragment) && searchSubstring(value, fragment)
}

func searchSubstring(value, fragment string) bool {
	for index := 0; index+len(fragment) <= len(value); index++ {
		if value[index:index+len(fragment)] == fragment {
			return true
		}
	}
	return false
}

func TestOpenListenerUnixPreservesNonSocketsAndActiveSockets(t *testing.T) {
	dir, err := os.MkdirTemp("", "bt-sock-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	// 1. Regular file must not be removed.
	regularPath := filepath.Join(dir, "regular.sock")
	if err := os.WriteFile(regularPath, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if listener, err := OpenListener(TransportConfig{Mode: "unix", SocketPath: regularPath}); err == nil {
		_ = listener.Close()
		t.Fatal("OpenListener accepted regular file path")
	}
	if raw, err := os.ReadFile(regularPath); err != nil || string(raw) != "keep" {
		t.Fatalf("regular file altered or removed: raw=%q err=%v", string(raw), err)
	}

	// 2. Symlink must not be removed.
	linkPath := filepath.Join(dir, "link.sock")
	if err := os.Symlink(regularPath, linkPath); err != nil {
		t.Fatal(err)
	}
	if listener, err := OpenListener(TransportConfig{Mode: "unix", SocketPath: linkPath}); err == nil {
		_ = listener.Close()
		t.Fatal("OpenListener accepted symlink path")
	}
	if _, err := os.Lstat(linkPath); err != nil {
		t.Fatalf("symlink was removed: %v", err)
	}

	// 3. Active Unix socket must not be replaced or removed.
	sockPath := filepath.Join(dir, "active.sock")
	active, err := OpenListener(TransportConfig{Mode: "unix", SocketPath: sockPath})
	if err != nil {
		t.Fatal(err)
	}
	if duplicate, err := OpenListener(TransportConfig{Mode: "unix", SocketPath: sockPath}); err == nil {
		_ = duplicate.Close()
		_ = active.Close()
		t.Fatal("OpenListener replaced active unix socket")
	}
	_ = active.Close()

	// 4. Verified stale owner-owned Unix socket (closed without unlinking) must be cleanly reclaimed.
	stalePath := filepath.Join(dir, "stale.sock")
	rawListener, err := net.Listen("unix", stalePath)
	if err != nil {
		t.Fatal(err)
	}
	rawListener.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := rawListener.Close(); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := OpenListener(TransportConfig{Mode: "unix", SocketPath: stalePath})
	if err != nil {
		t.Fatalf("OpenListener failed to reclaim verified stale socket: %v", err)
	}
	defer reclaimed.Close()

	// 5. Verified Unix client dial succeeds against owner-only socket and rejects permissive dirs/sockets.
	conn, err := DialVerifiedUnix(context.Background(), stalePath)
	if err != nil {
		t.Fatalf("DialVerifiedUnix failed on trusted owner-only socket: %v", err)
	}
	_ = conn.Close()

	permissiveDir, err := os.MkdirTemp("", "bt-perm-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(permissiveDir) })
	if err := os.Chmod(permissiveDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if listener, err := OpenListener(TransportConfig{Mode: "unix", SocketPath: filepath.Join(permissiveDir, "p.sock")}); err == nil {
		_ = listener.Close()
		t.Fatal("OpenListener accepted group/world-accessible socket directory")
	}
	if _, err := DialVerifiedUnix(context.Background(), filepath.Join(permissiveDir, "p.sock")); err == nil {
		t.Fatal("DialVerifiedUnix accepted group/world-accessible socket directory")
	}
}

func TestConfigAndAuthenticatorRejectCrossNamespaceDigestCollision(t *testing.T) {
	now := time.Now().UTC()
	var raw [32]byte
	raw[0] = 0x42
	_, digest, err := GenerateToken(raw[:])
	if err != nil {
		t.Fatal(err)
	}
	cred := CredentialConfig{SHA256: digest, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
	cfg := Config{
		Schema:   ConfigSchema,
		Profile:  "single-node-general-v1",
		DataRoot: t.TempDir(),
		Transport: TransportConfig{
			Mode:       "unix",
			SocketPath: filepath.Join(t.TempDir(), "s.sock"),
		},
		Namespaces: []NamespaceConfig{
			{ID: "ns_AAAAAAAAAAAAAAAAAAAAAA", Credentials: []CredentialConfig{cred}},
			{ID: "ns_BBBBBBBBBBBBBBBBBBBBBB", Credentials: []CredentialConfig{cred}},
		},
	}
	if _, err := ValidateConfig(cfg); err == nil {
		t.Fatal("ValidateConfig accepted duplicate credential digest across namespaces")
	}
	auth := NewAuthenticator(Config{})
	if err := auth.Reload(cfg); err == nil {
		t.Fatal("Authenticator.Reload accepted duplicate credential digest across namespaces")
	}

	limit := uint64(10)
	cfg.Namespaces = []NamespaceConfig{
		{ID: "ns_AAAAAAAAAAAAAAAAAAAAAA", Credentials: []CredentialConfig{cred}, Limits: &NamespaceLimits{Addresses: &limit}},
	}
	if _, err := ValidateConfig(cfg); err == nil {
		t.Fatal("ValidateConfig accepted unsupported per-namespace limits override")
	}
}
