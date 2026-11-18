package licoarccandidate

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	"google.golang.org/grpc"
)

func mintTestBearer(t *testing.T) string {
	t.Helper()
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(raw[:])
}

type fakeStationClient struct {
	mu             sync.Mutex
	reserveCalls   int
	submitCalls    int
	claimCalls     int
	settleCalls    int
	deadlineMisses int
	requestIDs     map[string][]string
	packet         []byte
	submitStarted  chan struct{}
	submitRelease  chan struct{}
}

type maliciousStationClient struct{ fakeStationClient }

func (*maliciousStationClient) Reserve(context.Context, *stationv1.ReserveRequest, ...grpc.CallOption) (*stationv1.ReserveResponse, error) {
	return &stationv1.ReserveResponse{AddressId: "foreign", ExpiresAtUnixMs: 1}, nil
}

func (client *fakeStationClient) Reserve(ctx context.Context, request *stationv1.ReserveRequest, _ ...grpc.CallOption) (*stationv1.ReserveResponse, error) {
	client.mu.Lock()
	client.reserveCalls++
	client.recordDeadlineLocked(ctx, nativeMutationTimeout)
	client.recordRequestIDLocked("reserve", request.GetRequestId())
	client.mu.Unlock()
	return &stationv1.ReserveResponse{AddressId: "sa_AAAAAAAAAAAAAAAAAAAAAA", ExpiresAtUnixMs: 1}, nil
}

func (client *fakeStationClient) Submit(ctx context.Context, request *stationv1.SubmitRequest, _ ...grpc.CallOption) (*stationv1.SubmitResponse, error) {
	client.mu.Lock()
	client.submitCalls++
	client.recordDeadlineLocked(ctx, nativeMutationTimeout)
	client.recordRequestIDLocked("submit", request.GetRequestId())
	client.packet = append([]byte(nil), request.GetPayload()...)
	started, release := client.submitStarted, client.submitRelease
	client.mu.Unlock()
	if started != nil {
		select {
		case started <- struct{}{}:
		default:
		}
	}
	if release != nil {
		<-release
	}
	return &stationv1.SubmitResponse{UnitId: "tu_AAAAAAAAAAAAAAAAAAAAAA", AcceptedAtUnixMs: 1, ExpiresAtUnixMs: 2}, nil
}

func (client *fakeStationClient) Claim(ctx context.Context, request *stationv1.ClaimRequest, _ ...grpc.CallOption) (*stationv1.ClaimResponse, error) {
	client.mu.Lock()
	client.claimCalls++
	client.recordDeadlineLocked(ctx, nativeClaimTimeout)
	client.recordRequestIDLocked("claim", request.GetRequestId())
	packet := append([]byte(nil), client.packet...)
	client.mu.Unlock()
	claimID := "dc_AAAAAAAAAAAAAAAAAAAAAA"
	return &stationv1.ClaimResponse{ClaimId: &claimID, ClaimEpoch: 1, ClaimExpiresAtUnixMs: 2_000_000_000_000, Units: []*stationv1.ClaimedUnit{{UnitId: "tu_AAAAAAAAAAAAAAAAAAAAAA", Payload: packet, AcceptedAtUnixMs: 1, ExpiresAtUnixMs: 2_000_000_000_000, DeliveryAttempt: 1}}}, nil
}

func (client *fakeStationClient) Settle(ctx context.Context, request *stationv1.SettleRequest, _ ...grpc.CallOption) (*stationv1.SettleResponse, error) {
	client.mu.Lock()
	client.settleCalls++
	client.recordDeadlineLocked(ctx, nativeMutationTimeout)
	client.recordRequestIDLocked("settle", request.GetRequestId())
	client.mu.Unlock()
	return &stationv1.SettleResponse{Units: []*stationv1.SettledUnit{{UnitId: "tu_AAAAAAAAAAAAAAAAAAAAAA", Outcome: stationv1.SettleOutcome_SETTLE_OUTCOME_COMPLETED}}}, nil
}

func (client *fakeStationClient) recordDeadlineLocked(ctx context.Context, maximum time.Duration) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > maximum+time.Second {
		client.deadlineMisses++
	}
}

func (client *fakeStationClient) recordRequestIDLocked(operation, requestID string) {
	if client.requestIDs == nil {
		client.requestIDs = make(map[string][]string)
	}
	client.requestIDs[operation] = append(client.requestIDs[operation], requestID)
}

func TestCandidateOperationsClaimRetrieveSettle(t *testing.T) {
	t.Parallel()
	handler, client := openTestHandler(t)
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	reserveBody := mustEncode(t, map[uint64]any{0: bytes.Repeat([]byte{1}, 32), 1: bytes.Repeat([]byte{2}, 32), 2: profileDigest, 3: uint64(HandleAsync)})
	reserveResponse := httptest.NewRecorder()
	handler.ServeHTTP(reserveResponse, carrierRequest(t, "/v1/reserve", controlMediaType, reserveBody, "10000000000000000000000000000000"))
	var reserveControl map[uint64]any
	if err := strictDecoder.Unmarshal(reserveResponse.Body.Bytes(), &reserveControl); err != nil {
		t.Fatal(err)
	}
	var handle [32]byte
	copy(handle[:], reserveControl[uint64(1)].([]byte))

	packet := []byte{0xde, 0xad, 0xbe, 0xef}
	submitResponse := httptest.NewRecorder()
	handler.ServeHTTP(submitResponse, carrierRequest(t, "/v1/handles/"+tokenString(handle)+"/submit", packetMediaType, packet, "20000000000000000000000000000000"))
	claimBody := mustEncode(t, map[uint64]any{0: uint64(1)})
	claimResponse := httptest.NewRecorder()
	handler.ServeHTTP(claimResponse, carrierRequest(t, "/v1/handles/"+tokenString(handle)+"/claim", controlMediaType, claimBody, "30000000000000000000000000000000"))
	if claimResponse.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%x", claimResponse.Code, claimResponse.Body.Bytes())
	}
	var claimControl map[uint64]any
	if err := strictDecoder.Unmarshal(claimResponse.Body.Bytes(), &claimControl); err != nil {
		t.Fatal(err)
	}
	claimRaw := claimControl[uint64(1)].([]byte)
	claimEpoch, ok := uintValue(claimControl[uint64(2)])
	if !ok {
		t.Fatal("claim epoch missing")
	}
	claimedItems := claimControl[uint64(3)].([]any)
	claimedMap := claimedItems[0].(map[any]any)
	itemRaw := claimedMap[uint64(0)].([]byte)

	retrieveBody := mustEncode(t, map[uint64]any{0: claimRaw, 1: claimEpoch, 2: itemRaw})
	retrieveResponse := httptest.NewRecorder()
	handler.ServeHTTP(retrieveResponse, carrierRequest(t, "/v1/handles/"+tokenString(handle)+"/retrieve", controlMediaType, retrieveBody, "40000000000000000000000000000000"))
	if retrieveResponse.Code != http.StatusOK || retrieveResponse.Header().Get("Content-Type") != packetMediaType || !bytes.Equal(retrieveResponse.Body.Bytes(), packet) {
		t.Fatalf("retrieve status=%d type=%s body=%x", retrieveResponse.Code, retrieveResponse.Header().Get("Content-Type"), retrieveResponse.Body.Bytes())
	}

	settleBody := mustEncode(t, map[uint64]any{0: claimRaw, 1: claimEpoch, 2: []any{map[uint64]any{0: itemRaw, 1: uint64(SettleComplete)}}})
	settleResponse := httptest.NewRecorder()
	handler.ServeHTTP(settleResponse, carrierRequest(t, "/v1/handles/"+tokenString(handle)+"/settle", controlMediaType, settleBody, "50000000000000000000000000000000"))
	if settleResponse.Code != http.StatusOK {
		t.Fatalf("settle status=%d body=%x", settleResponse.Code, settleResponse.Body.Bytes())
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.claimCalls != 1 || client.settleCalls != 1 || client.deadlineMisses != 0 {
		t.Fatalf("native claim=%d settle=%d deadline_misses=%d", client.claimCalls, client.settleCalls, client.deadlineMisses)
	}
}

func (client *fakeStationClient) GetCapacity(context.Context, *stationv1.GetCapacityRequest, ...grpc.CallOption) (*stationv1.GetCapacityResponse, error) {
	return &stationv1.GetCapacityResponse{}, nil
}

func TestCandidateCarrierReserveSubmitAndIdempotentReplay(t *testing.T) {
	t.Parallel()
	handler, client := openTestHandler(t)
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	reserveBody := mustEncode(t, map[uint64]any{
		0: bytes.Repeat([]byte{1}, 32), 1: bytes.Repeat([]byte{2}, 32), 2: profileDigest, 3: uint64(HandleAsync),
	})
	reserve := carrierRequest(t, "/v1/reserve", controlMediaType, reserveBody, "00112233445566778899aabbccddeeff")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, reserve)
	if response.Code != http.StatusCreated || response.Header().Get("LicoArc-Outcome") != "accepted" {
		t.Fatalf("reserve status=%d headers=%v body=%x", response.Code, response.Header(), response.Body.Bytes())
	}
	var reserveControl map[uint64]any
	if err := strictDecoder.Unmarshal(response.Body.Bytes(), &reserveControl); err != nil {
		t.Fatal(err)
	}
	handleBytes, ok := reserveControl[uint64(1)].([]byte)
	if !ok || len(handleBytes) != 32 {
		t.Fatalf("reserve handle=%#v", reserveControl[uint64(1)])
	}
	var handle [32]byte
	copy(handle[:], handleBytes)

	packet := []byte{0xaa, 0xbb, 0xcc}
	submit := carrierRequest(t, "/v1/handles/"+tokenString(handle)+"/submit", packetMediaType, packet, "11112222333344445555666677778888")
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, submit)
	if first.Code != http.StatusAccepted {
		t.Fatalf("submit status=%d body=%x", first.Code, first.Body.Bytes())
	}
	replay := carrierRequest(t, submit.URL.Path, packetMediaType, packet, "11112222333344445555666677778888")
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, replay)
	if second.Code != first.Code || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
		t.Fatal("idempotent replay changed response")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reserveCalls != 1 || client.submitCalls != 1 || !bytes.Equal(client.packet, packet) {
		t.Fatalf("native calls reserve=%d submit=%d packet=%x", client.reserveCalls, client.submitCalls, client.packet)
	}
}

func TestCandidateCarrierRejectsBeforeStateChange(t *testing.T) {
	t.Parallel()
	handler, client := openTestHandler(t)
	body := mustEncode(t, map[uint64]any{0: bytes.Repeat([]byte{1}, 32), 1: bytes.Repeat([]byte{2}, 32), 2: bytes.Repeat([]byte{3}, 32), 3: uint64(0)})
	tests := []struct {
		name   string
		mutate func(*http.Request)
	}{
		{name: "http1", mutate: func(request *http.Request) { request.ProtoMajor = 1 }},
		{name: "tls12", mutate: func(request *http.Request) { request.TLS.Version = tls.VersionTLS12 }},
		{name: "query", mutate: func(request *http.Request) { request.URL.RawQuery = "x=1" }},
		{name: "wrong media", mutate: func(request *http.Request) { request.Header.Set("Content-Type", "application/json") }},
		{name: "noncanonical length", mutate: func(request *http.Request) { request.Header.Set("Content-Length", "0"+strconv.Itoa(len(body))) }},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := carrierRequest(t, "/v1/reserve", controlMediaType, body, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"+strconv.Itoa(index))
			test.mutate(request)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest || response.Header().Get("LicoArc-Outcome") != "rejected" {
				t.Fatalf("status=%d headers=%v", response.Code, response.Header())
			}
		})
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reserveCalls != 0 {
		t.Fatalf("malformed carrier reached native Station %d times", client.reserveCalls)
	}
}

func TestCandidateCarrierRunsOverRealHTTP2TLS13(t *testing.T) {
	t.Parallel()
	handler, client := openTestHandler(t)
	server := httptest.NewUnstartedServer(handler)
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, NextProtos: []string{"h2"}}
	server.StartTLS()
	defer server.Close()

	body := mustEncode(t, map[uint64]any{0: bytes.Repeat([]byte{1}, 32), 1: bytes.Repeat([]byte{2}, 32)})
	request, err := http.NewRequest(http.MethodPost, server.URL+"/v1/affiliate", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", controlMediaType)
	request.Header.Set("LicoArc-Operation-Id", "0123456789abcdef0123456789abcdef")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 || response.TLS == nil || response.TLS.Version != tls.VersionTLS13 || response.StatusCode != http.StatusCreated {
		t.Fatalf("proto=%s tls=%#v status=%d", response.Proto, response.TLS, response.StatusCode)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reserveCalls != 0 || client.submitCalls != 0 {
		t.Fatal("AFFILIATE unexpectedly used native mutation API")
	}
}

func TestCandidateCrashRecoveryReplaysPersistedOperation(t *testing.T) {
	t.Parallel()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil || len(publicKey) != ed25519.PublicKeySize {
		t.Fatal(err)
	}
	client := &fakeStationClient{}
	stateRoot := t.TempDir()
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	newHandler := func() (*Handler, *Adapter) {
		adapter, err := NewAdapter(client, Config{
			NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: stateRoot,
			Signer: privateKey, StationDescriptorDigest: filledDigest(1), DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		admitTestAffiliation(t, adapter, filledDigest(1), filledDigest(2))
		handler, err := NewHandler(adapter)
		if err != nil {
			t.Fatal(err)
		}
		return handler, adapter
	}
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	body := mustEncode(t, map[uint64]any{0: bytes.Repeat([]byte{1}, 32), 1: bytes.Repeat([]byte{2}, 32), 2: profileDigest, 3: uint64(HandleAsync)})
	requestID := "99998888777766665555444433332222"

	firstHandler, firstAdapter := newHandler()
	first := httptest.NewRecorder()
	firstHandler.ServeHTTP(first, carrierRequest(t, "/v1/reserve", controlMediaType, body, requestID))
	if first.Code != http.StatusCreated {
		t.Fatalf("first status=%d", first.Code)
	}
	if err := firstAdapter.Close(); err != nil {
		t.Fatal(err)
	}

	secondHandler, secondAdapter := newHandler()
	defer secondAdapter.Close()
	second := httptest.NewRecorder()
	secondHandler.ServeHTTP(second, carrierRequest(t, "/v1/reserve", controlMediaType, body, requestID))
	if second.Code != first.Code || !bytes.Equal(second.Body.Bytes(), first.Body.Bytes()) {
		t.Fatal("restart replay changed exact response")
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reserveCalls != 1 {
		t.Fatalf("restart replay called native Reserve %d times", client.reserveCalls)
	}
}

func TestCandidateReserveCrashBeforeHTTPRecordKeepsOneHandle(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{}
	stateRoot := t.TempDir()
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	config := Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: stateRoot,
		Signer: privateKey, StationDescriptorDigest: [32]byte{1}, DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
	}
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	var digest [32]byte
	copy(digest[:], profileDigest)
	request := ReserveRequest{
		StationDescriptorDigest: [32]byte{1}, AffiliationCommitment: [32]byte{2},
		TransportProfileDigest: digest, HandleClass: HandleAsync,
	}
	operationID := [16]byte{9}
	firstAdapter, err := NewAdapter(client, config)
	if err != nil {
		t.Fatal(err)
	}
	admitTestAffiliation(t, firstAdapter, request.StationDescriptorDigest, request.AffiliationCommitment)
	firstHandle, firstUntil, firstSignature, err := firstAdapter.Reserve(context.Background(), operationID, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstAdapter.Close(); err != nil {
		t.Fatal(err)
	}
	secondAdapter, err := NewAdapter(client, config)
	if err != nil {
		t.Fatal(err)
	}
	defer secondAdapter.Close()
	secondHandle, secondUntil, secondSignature, err := secondAdapter.Reserve(context.Background(), operationID, request)
	if err != nil {
		t.Fatal(err)
	}
	if firstHandle != secondHandle || firstUntil != secondUntil || !bytes.Equal(firstSignature, secondSignature) {
		t.Fatal("reserve recovery changed exact result")
	}
	request.HandleClass = HandleFirstContact
	if _, _, _, err := secondAdapter.Reserve(context.Background(), operationID, request); !errors.Is(err, ErrInvalidControl) {
		t.Fatalf("conflicting recovered reserve err=%v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reserveCalls != 1 {
		t.Fatalf("native reserve calls=%d", client.reserveCalls)
	}
}

func TestCandidateReserveRequiresLiveMatchingAffiliation(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(client, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: [32]byte{1}, DescriptorNotAfter: uint64(now.Add(2 * time.Hour).Unix()), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	var profile [32]byte
	copy(profile[:], profileDigest)
	request := ReserveRequest{
		StationDescriptorDigest: [32]byte{1}, AffiliationCommitment: [32]byte{2},
		TransportProfileDigest: profile, HandleClass: HandleAsync,
	}
	wrongDescriptor := request
	wrongDescriptor.StationDescriptorDigest[0] ^= 1
	if _, _, err := adapter.Affiliate(context.Background(), [16]byte{0}, AffiliateRequest{
		StationDescriptorDigest: wrongDescriptor.StationDescriptorDigest,
		AffiliationCommitment:   request.AffiliationCommitment,
	}); !errors.Is(err, ErrDescriptorUnavailable) {
		t.Fatalf("affiliate with unconfigured descriptor err=%v", err)
	}
	if _, _, _, err := adapter.Reserve(context.Background(), [16]byte{1}, request); !errors.Is(err, ErrHandleUnavailable) {
		t.Fatalf("reserve without affiliation err=%v", err)
	}
	admitTestAffiliation(t, adapter, request.StationDescriptorDigest, request.AffiliationCommitment)
	mismatch := request
	mismatch.AffiliationCommitment[0] ^= 1
	if _, _, _, err := adapter.Reserve(context.Background(), [16]byte{2}, mismatch); !errors.Is(err, ErrHandleUnavailable) {
		t.Fatalf("reserve with mismatched affiliation err=%v", err)
	}
	mismatch = request
	mismatch.TransportProfileDigest[0] ^= 1
	if _, _, _, err := adapter.Reserve(context.Background(), [16]byte{2}, mismatch); !errors.Is(err, ErrUnsupportedProfile) {
		t.Fatalf("reserve with unsupported profile err=%v", err)
	}
	if _, _, _, err := adapter.Reserve(context.Background(), [16]byte{3}, request); err != nil {
		t.Fatal(err)
	}
	now = now.Add((StorageWindowSeconds + 1) * time.Second)
	if _, _, _, err := adapter.Reserve(context.Background(), [16]byte{4}, request); !errors.Is(err, ErrHandleUnavailable) {
		t.Fatalf("reserve with expired affiliation err=%v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.reserveCalls != 1 {
		t.Fatalf("native reserve calls=%d", client.reserveCalls)
	}
}

func TestCandidateStatementsBindExactConfiguredTuples(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	descriptor := [32]byte{1}
	commitment := [32]byte{2}
	adapter, err := NewAdapter(client, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: descriptor,
		DescriptorNotAfter: uint64(now.Add(2 * time.Hour).Unix()), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()

	affiliationNotAfter, affiliationSignature, err := adapter.Affiliate(context.Background(), [16]byte{1}, AffiliateRequest{
		StationDescriptorDigest: descriptor, AffiliationCommitment: commitment,
	})
	if err != nil {
		t.Fatal(err)
	}
	affiliationMessage := append([]byte("licoarc.station-affiliation.v1\x00"), descriptor[:]...)
	affiliationMessage = append(affiliationMessage, commitment[:]...)
	affiliationMessage = appendUint64(affiliationMessage, affiliationNotAfter)
	if !ed25519.Verify(publicKey, affiliationMessage, affiliationSignature) {
		t.Fatal("AFFILIATE signature does not bind the exact descriptor/commitment/notAfter tuple")
	}

	now = now.Add(30 * time.Minute)
	profileRaw, _ := hex.DecodeString(TransportSourceDigest)
	var profile [32]byte
	copy(profile[:], profileRaw)
	handle, serviceUntil, routeSignature, err := adapter.Reserve(context.Background(), [16]byte{2}, ReserveRequest{
		StationDescriptorDigest: descriptor, AffiliationCommitment: commitment,
		TransportProfileDigest: profile, HandleClass: HandleAsync,
	})
	if err != nil {
		t.Fatal(err)
	}
	if serviceUntil != affiliationNotAfter {
		t.Fatalf("serviceUntil=%d exceeds matched affiliationNotAfter=%d", serviceUntil, affiliationNotAfter)
	}
	routeMessage := append([]byte("licoarc.station-route-service.v1\x00"), descriptor[:]...)
	routeMessage = append(routeMessage, commitment[:]...)
	routeMessage = append(routeMessage, profile[:]...)
	routeMessage = append(routeMessage, handle[:]...)
	routeMessage = appendUint64(routeMessage, serviceUntil)
	if !ed25519.Verify(publicKey, routeMessage, routeSignature) {
		t.Fatal("RESERVE signature does not bind the exact route-service tuple")
	}
}

func TestCandidateNativeOperationGenerationSeparatesReusedWindow(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(client, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: [32]byte{1}, DescriptorNotAfter: uint64(now.Add(2 * time.Hour).Unix()), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	var profile [32]byte
	copy(profile[:], profileDigest)
	request := ReserveRequest{
		StationDescriptorDigest: [32]byte{1}, AffiliationCommitment: [32]byte{2},
		TransportProfileDigest: profile, HandleClass: HandleAsync,
	}
	admitTestAffiliation(t, adapter, request.StationDescriptorDigest, request.AffiliationCommitment)
	operationID := [16]byte{9}
	if _, _, _, err := adapter.Reserve(context.Background(), operationID, request); err != nil {
		t.Fatal(err)
	}
	now = now.Add((IdempotencyWindowSeconds + 1) * time.Second)
	if err := adapter.pruneExpired(); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := adapter.Reserve(context.Background(), operationID, request); err != nil {
		t.Fatal(err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	requestIDs := client.requestIDs["reserve"]
	if len(requestIDs) != 2 || requestIDs[0] == requestIDs[1] || !validNativeID(requestIDs[0], "rq_") || !validNativeID(requestIDs[1], "rq_") {
		t.Fatalf("native reserve request IDs=%v", requestIDs)
	}
}

func TestCandidateSettleCrashBeforeHTTPRecordReplaysAfterRestart(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{}
	stateRoot := t.TempDir()
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	config := Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: stateRoot,
		Signer: privateKey, StationDescriptorDigest: [32]byte{1}, DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
	}
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	var profile [32]byte
	copy(profile[:], profileDigest)
	first, err := NewAdapter(client, config)
	if err != nil {
		t.Fatal(err)
	}
	admitTestAffiliation(t, first, [32]byte{1}, [32]byte{2})
	handle, _, _, err := first.Reserve(context.Background(), [16]byte{1}, ReserveRequest{
		StationDescriptorDigest: [32]byte{1}, AffiliationCommitment: [32]byte{2},
		TransportProfileDigest: profile, HandleClass: HandleAsync,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Submit(context.Background(), [16]byte{2}, handle, []byte("opaque")); err != nil {
		t.Fatal(err)
	}
	claimID, epoch, items, err := first.Claim(context.Background(), [16]byte{3}, handle, ClaimRequest{MaxItems: 1})
	if err != nil || len(items) != 1 {
		t.Fatalf("claim items=%d err=%v", len(items), err)
	}
	request := SettleRequest{ClaimID: claimID, ClaimEpoch: epoch, Settlements: []Settlement{{ItemID: items[0].ItemID, Action: SettleComplete}}}
	operationID := [16]byte{4}
	beforeCrash, err := first.Settle(context.Background(), operationID, handle, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := NewAdapter(client, config)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	afterRestart, err := second.Settle(context.Background(), operationID, handle, request)
	if err != nil || len(afterRestart) != len(beforeCrash) || afterRestart[0] != beforeCrash[0] {
		t.Fatalf("settle replay before=%#v after=%#v err=%v", beforeCrash, afterRestart, err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	requestIDs := client.requestIDs["settle"]
	if len(requestIDs) != 2 || requestIDs[0] != requestIDs[1] {
		t.Fatalf("settle crash replay request IDs=%v", requestIDs)
	}
}

func TestCandidateStateRejectsForeignRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(root+"/foreign.txt", []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewAdapter(&fakeStationClient{}, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: root,
		Signer: privateKey, StationDescriptorDigest: filledDigest(1), DescriptorNotAfter: uint64(time.Now().Add(time.Hour).Unix()),
	})
	if err == nil {
		t.Fatal("foreign state root accepted")
	}
}

func TestCandidateStateBindsDescriptorAndSigner(t *testing.T) {
	_, firstSigner, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	_, secondSigner, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	config := Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: firstSigner, StationDescriptorDigest: [32]byte{1},
		DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
	}
	adapter, err := NewAdapter(&fakeStationClient{}, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}

	changed := config
	changed.Signer = secondSigner
	if adapter, err := NewAdapter(&fakeStationClient{}, changed); err == nil {
		_ = adapter.Close()
		t.Fatal("state root accepted a different Station signer")
	}
	changed = config
	changed.StationDescriptorDigest[0] ^= 1
	if adapter, err := NewAdapter(&fakeStationClient{}, changed); err == nil {
		_ = adapter.Close()
		t.Fatal("state root accepted a different Station descriptor")
	}
}

func TestUntrustedStationResponseFailsAmbiguousWithoutHandle(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(&maliciousStationClient{}, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: filledDigest(1), DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	admitTestAffiliation(t, adapter, filledDigest(1), filledDigest(2))
	defer adapter.Close()
	handler, err := NewHandler(adapter)
	if err != nil {
		t.Fatal(err)
	}
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	body := mustEncode(t, map[uint64]any{
		0: bytes.Repeat([]byte{1}, 32), 1: bytes.Repeat([]byte{2}, 32), 2: profileDigest, 3: uint64(HandleAsync),
	})
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, carrierRequest(t, "/v1/reserve", controlMediaType, body, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	if response.Code != http.StatusGatewayTimeout || response.Header().Get("LicoArc-Outcome") != "ambiguous" || response.Header().Get("LicoArc-Failure-Class") != "internalUnavailable" {
		t.Fatalf("status=%d headers=%v", response.Code, response.Header())
	}
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	if len(adapter.handles) != 0 {
		t.Fatal("untrusted Station response created a handle")
	}
}

func TestCandidateFirstContactSerializesByHandle(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{submitStarted: make(chan struct{}, 1), submitRelease: make(chan struct{})}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(client, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: [32]byte{1}, DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	var digest [32]byte
	copy(digest[:], profileDigest)
	admitTestAffiliation(t, adapter, [32]byte{1}, [32]byte{2})
	handle, _, _, err := adapter.Reserve(context.Background(), [16]byte{1}, ReserveRequest{
		StationDescriptorDigest: [32]byte{1}, AffiliationCommitment: [32]byte{2},
		TransportProfileDigest: digest, HandleClass: HandleFirstContact,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	go func() { firstDone <- adapter.Submit(context.Background(), [16]byte{2}, handle, []byte("first")) }()
	<-client.submitStarted
	secondDone := make(chan error, 1)
	go func() { secondDone <- adapter.Submit(context.Background(), [16]byte{3}, handle, []byte("second")) }()
	select {
	case err := <-secondDone:
		t.Fatalf("second submit passed handle lock early: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(client.submitRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; !errors.Is(err, ErrHandleUnavailable) {
		t.Fatalf("second submit err=%v", err)
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.submitCalls != 1 {
		t.Fatalf("native submit calls=%d", client.submitCalls)
	}
}

func TestCandidateHandlerEnforcesConcurrentAndQueuedBudgets(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{submitStarted: make(chan struct{}, 2), submitRelease: make(chan struct{})}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(client, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: filledDigest(1), DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
		MaxConcurrentOperations: 1, MaxQueuedOperations: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	admitTestAffiliation(t, adapter, filledDigest(1), filledDigest(2))
	defer adapter.Close()
	handler, err := NewHandler(adapter)
	if err != nil {
		t.Fatal(err)
	}
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	reserveBody := mustEncode(t, map[uint64]any{0: bytes.Repeat([]byte{1}, 32), 1: bytes.Repeat([]byte{2}, 32), 2: profileDigest, 3: uint64(HandleAsync)})
	reserveResponse := httptest.NewRecorder()
	handler.ServeHTTP(reserveResponse, carrierRequest(t, "/v1/reserve", controlMediaType, reserveBody, "10000000000000000000000000000000"))
	var reserveControl map[uint64]any
	if err := strictDecoder.Unmarshal(reserveResponse.Body.Bytes(), &reserveControl); err != nil {
		t.Fatal(err)
	}
	var handle [32]byte
	copy(handle[:], reserveControl[uint64(1)].([]byte))
	path := "/v1/handles/" + tokenString(handle) + "/submit"
	call := func(id string) chan *httptest.ResponseRecorder {
		done := make(chan *httptest.ResponseRecorder, 1)
		go func() {
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, carrierRequest(t, path, packetMediaType, []byte("packet"), id))
			done <- response
		}()
		return done
	}
	first := call("20000000000000000000000000000000")
	<-client.submitStarted
	second := call("30000000000000000000000000000000")
	deadline := time.Now().Add(time.Second)
	for len(handler.admitted) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	third := httptest.NewRecorder()
	handler.ServeHTTP(third, carrierRequest(t, path, packetMediaType, []byte("packet"), "40000000000000000000000000000000"))
	if third.Code != http.StatusTooManyRequests || third.Header().Get("LicoArc-Failure-Class") != "rateLimited" {
		t.Fatalf("third status=%d headers=%v", third.Code, third.Header())
	}
	close(client.submitRelease)
	if response := <-first; response.Code != http.StatusAccepted {
		t.Fatalf("first status=%d", response.Code)
	}
	if response := <-second; response.Code != http.StatusAccepted {
		t.Fatalf("second status=%d", response.Code)
	}
}

func TestCandidateStateByteCeilingFailsClosed(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(&fakeStationClient{}, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: [32]byte{1}, DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
		MaxStateBytes: 800,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	var digest [32]byte
	copy(digest[:], profileDigest)
	admitTestAffiliation(t, adapter, [32]byte{1}, [32]byte{2})
	if _, _, _, err := adapter.Reserve(context.Background(), [16]byte{1}, ReserveRequest{
		StationDescriptorDigest: [32]byte{1}, AffiliationCommitment: [32]byte{2},
		TransportProfileDigest: digest, HandleClass: HandleAsync,
	}); !errors.Is(err, ErrStateCapacity) {
		t.Fatalf("state ceiling err=%v", err)
	}
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	if len(adapter.handles) != 0 {
		t.Fatal("state ceiling admitted a handle")
	}
}

func TestCandidateDeadlineIndexPrunesBoundedState(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(&fakeStationClient{}, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t), StateRoot: t.TempDir(),
		Signer: privateKey, StationDescriptorDigest: [32]byte{1}, DescriptorNotAfter: uint64(now.Add(2 * time.Hour).Unix()), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	profileDigest, _ := hex.DecodeString(TransportSourceDigest)
	var digest [32]byte
	copy(digest[:], profileDigest)
	request := ReserveRequest{
		StationDescriptorDigest: [32]byte{1}, AffiliationCommitment: [32]byte{2},
		TransportProfileDigest: digest, HandleClass: HandleAsync,
	}
	admitTestAffiliation(t, adapter, request.StationDescriptorDigest, request.AffiliationCommitment)
	operationID := [16]byte{7}
	handle, _, _, err := adapter.Reserve(context.Background(), operationID, request)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add((IdempotencyWindowSeconds + 1) * time.Second)
	if err := adapter.pruneExpired(); err != nil {
		t.Fatal(err)
	}
	if _, found, err := adapter.store.getReservation(operationID, reserveFingerprint(request)); err != nil || found {
		t.Fatalf("expired reservation found=%v err=%v", found, err)
	}
	adapter.mu.RLock()
	_, handlePresent := adapter.handles[handle]
	adapter.mu.RUnlock()
	if !handlePresent {
		t.Fatal("live handle pruned with short operation window")
	}
	now = now.Add(StorageWindowSeconds * time.Second)
	if err := adapter.pruneExpired(); err != nil {
		t.Fatal(err)
	}
	adapter.mu.RLock()
	_, handlePresent = adapter.handles[handle]
	adapter.mu.RUnlock()
	if handlePresent {
		t.Fatal("expired handle retained")
	}
}

func openTestHandler(t *testing.T) (*Handler, *fakeStationClient) {
	t.Helper()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeStationClient{}
	now := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	adapter, err := NewAdapter(client, Config{
		NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: mintTestBearer(t),
		StateRoot: t.TempDir(),
		Signer:    privateKey, StationDescriptorDigest: filledDigest(1),
		DescriptorNotAfter: uint64(now.Add(time.Hour).Unix()), Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	admitTestAffiliation(t, adapter, filledDigest(1), filledDigest(2))
	t.Cleanup(func() { _ = adapter.Close() })
	handler, err := NewHandler(adapter)
	if err != nil {
		t.Fatal(err)
	}
	return handler, client
}

func admitTestAffiliation(t *testing.T, adapter *Adapter, descriptor, commitment [32]byte) {
	t.Helper()
	if _, _, err := adapter.Affiliate(context.Background(), [16]byte{0xfe}, AffiliateRequest{
		StationDescriptorDigest: descriptor, AffiliationCommitment: commitment,
	}); err != nil {
		t.Fatal(err)
	}
}

func filledDigest(value byte) [32]byte {
	var digest [32]byte
	for index := range digest {
		digest[index] = value
	}
	return digest
}

func carrierRequest(t *testing.T, path, mediaType string, body []byte, operationID string) *http.Request {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "https://station.invalid"+path, io.NopCloser(bytes.NewReader(body)))
	request.Proto = "HTTP/2.0"
	request.ProtoMajor = 2
	request.ProtoMinor = 0
	request.TLS = &tls.ConnectionState{Version: tls.VersionTLS13, NegotiatedProtocol: "h2"}
	request.ContentLength = int64(len(body))
	request.Header.Set("Content-Length", strconv.Itoa(len(body)))
	request.Header.Set("Content-Type", mediaType)
	request.Header.Set("LicoArc-Operation-Id", operationID)
	return request
}

func TestRetrieveAndSettleRejectForeignHandleBeforeAndAfterRestart(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_760_000_000, 0).UTC()
	descriptor := filledDigest(0x31)
	commitment := filledDigest(0x32)
	transportProfile, err := hex.DecodeString(TransportSourceDigest)
	if err != nil {
		t.Fatal(err)
	}
	var transportDigest [32]byte
	copy(transportDigest[:], transportProfile)
	root := t.TempDir()
	bearer := mintTestBearer(t)
	station := &fakeStationClient{packet: []byte("protected-packet")}
	openAdapter := func() *Adapter {
		adapter, openErr := NewAdapter(station, Config{
			NativeNamespace: "ns_AAAAAAAAAAAAAAAAAAAAAA", NativeBearer: bearer, StateRoot: root,
			Signer: append(ed25519.PrivateKey(nil), privateKey...), StationDescriptorDigest: descriptor,
			DescriptorNotAfter: uint64(now.Add(24 * time.Hour).Unix()),
			Now:                func() time.Time { return now },
		})
		if openErr != nil {
			t.Fatal(openErr)
		}
		return adapter
	}
	adapter := openAdapter()
	admitTestAffiliation(t, adapter, descriptor, commitment)
	handleA, _, _, err := adapter.Reserve(context.Background(), [16]byte{0x01}, ReserveRequest{
		StationDescriptorDigest: descriptor, AffiliationCommitment: commitment,
		TransportProfileDigest: transportDigest, HandleClass: HandleAsync,
	})
	if err != nil {
		t.Fatal(err)
	}
	handleB, _, _, err := adapter.Reserve(context.Background(), [16]byte{0x02}, ReserveRequest{
		StationDescriptorDigest: descriptor, AffiliationCommitment: commitment,
		TransportProfileDigest: transportDigest, HandleClass: HandleAsync,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimID, claimEpoch, items, err := adapter.Claim(context.Background(), [16]byte{0x03}, handleA, ClaimRequest{MaxItems: 1})
	if err != nil || len(items) != 1 {
		t.Fatalf("claim=%+v err=%v", items, err)
	}
	if _, err := adapter.Retrieve(context.Background(), handleB, RetrieveRequest{ClaimID: claimID, ClaimEpoch: claimEpoch, ItemID: items[0].ItemID}); !errors.Is(err, ErrClaimUnavailable) {
		t.Fatalf("foreign handle Retrieve err=%v, want ErrClaimUnavailable", err)
	}
	if _, err := adapter.Settle(context.Background(), [16]byte{0x04}, handleB, SettleRequest{
		ClaimID: claimID, ClaimEpoch: claimEpoch,
		Settlements: []Settlement{{ItemID: items[0].ItemID, Action: SettleComplete}},
	}); !errors.Is(err, ErrClaimUnavailable) {
		t.Fatalf("foreign handle Settle err=%v, want ErrClaimUnavailable", err)
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	// Verify handle-to-claim binding survives restart.
	restarted := openAdapter()
	defer restarted.Close()
	if _, err := restarted.Retrieve(context.Background(), handleB, RetrieveRequest{ClaimID: claimID, ClaimEpoch: claimEpoch, ItemID: items[0].ItemID}); !errors.Is(err, ErrClaimUnavailable) {
		t.Fatalf("post-restart foreign handle Retrieve err=%v, want ErrClaimUnavailable", err)
	}
	if _, err := restarted.Settle(context.Background(), [16]byte{0x05}, handleB, SettleRequest{
		ClaimID: claimID, ClaimEpoch: claimEpoch,
		Settlements: []Settlement{{ItemID: items[0].ItemID, Action: SettleComplete}},
	}); !errors.Is(err, ErrClaimUnavailable) {
		t.Fatalf("post-restart foreign handle Settle err=%v, want ErrClaimUnavailable", err)
	}
	packet, err := restarted.Retrieve(context.Background(), handleA, RetrieveRequest{ClaimID: claimID, ClaimEpoch: claimEpoch, ItemID: items[0].ItemID})
	if err != nil || string(packet) != "protected-packet" {
		t.Fatalf("owner handle Retrieve packet=%q err=%v", string(packet), err)
	}
}
