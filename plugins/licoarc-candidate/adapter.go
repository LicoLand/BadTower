package licoarccandidate

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"strings"
	"sync"
	"time"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	nativeMutationTimeout = 5 * time.Second
	nativeClaimTimeout    = 30 * time.Second
)

var (
	ErrHandleUnavailable     = errors.New("LicoArc handle unavailable")
	ErrClaimUnavailable      = errors.New("LicoArc claim unavailable")
	ErrDescriptorUnavailable = errors.New("LicoArc Station descriptor unavailable")
	ErrUnsupportedProfile    = errors.New("LicoArc transport profile unsupported")
	ErrUntrustedStation      = errors.New("native Station response invalid")
	ErrStateCapacity         = errors.New("plugin local state capacity unavailable")
)

type Config struct {
	NativeNamespace         string
	NativeBearer            string
	StateRoot               string
	Signer                  ed25519.PrivateKey
	StationDescriptorDigest [32]byte
	DescriptorNotAfter      uint64
	MaxConcurrentOperations uint32
	MaxQueuedOperations     uint32
	MaxStateBytes           uint64
	Now                     func() time.Time
}

type handleState struct {
	NativeAddressID  string
	Class            HandleClass
	ServiceUntil     uint64
	Consumed         bool
	PendingOperation string
}

type claimState struct {
	Handle        [32]byte
	NativeClaimID string
	NativeEpoch   uint64
	ExpiresAtMS   int64
	Items         map[[32]byte]claimItem
}

type claimItem struct {
	NativeUnitID string
	Packet       []byte
}

type Adapter struct {
	client      extension.StationClient
	config      Config
	mu          sync.RWMutex
	handles     map[[32]byte]handleState
	claims      map[[32]byte]claimState
	store       *stateStore
	handleLanes [64]sync.Mutex
}

func NewAdapter(client extension.StationClient, config Config) (*Adapter, error) {
	if client == nil || config.NativeNamespace == "" || config.NativeBearer == "" || config.StateRoot == "" || len(config.Signer) != ed25519.PrivateKeySize || config.StationDescriptorDigest == ([32]byte{}) || config.DescriptorNotAfter == 0 {
		return nil, ErrInvalidControl
	}
	config.Signer = append(ed25519.PrivateKey(nil), config.Signer...)
	if config.Now == nil {
		config.Now = time.Now
	}
	if config.MaxConcurrentOperations == 0 {
		config.MaxConcurrentOperations = MaxConcurrentOperations
	}
	if config.MaxQueuedOperations == 0 {
		config.MaxQueuedOperations = MaxQueuedOperations
	}
	if config.MaxStateBytes == 0 {
		config.MaxStateBytes = MaxPluginStateBytes
	}
	if config.MaxConcurrentOperations > MaxConcurrentOperations || config.MaxQueuedOperations > MaxQueuedOperations || config.MaxStateBytes > MaxPluginStateBytes {
		return nil, ErrInvalidControl
	}
	store, err := openStateStore(config.StateRoot, config.MaxStateBytes, adapterStateBinding(config))
	if err != nil {
		return nil, err
	}
	adapter := &Adapter{client: client, config: config, handles: make(map[[32]byte]handleState), claims: make(map[[32]byte]claimState), store: store}
	if err := store.loadState(adapter.handles, adapter.claims); err != nil {
		_ = store.close()
		return nil, err
	}
	return adapter, nil
}

func (adapter *Adapter) Close() error {
	err := adapter.store.close()
	clear(adapter.config.Signer)
	return err
}

func (adapter *Adapter) Affiliate(_ context.Context, operationID [16]byte, request AffiliateRequest) (uint64, []byte, error) {
	if request.StationDescriptorDigest != adapter.config.StationDescriptorDigest {
		return 0, nil, ErrDescriptorUnavailable
	}
	nowTime := adapter.config.Now()
	now := uint64(nowTime.Unix())
	if now >= adapter.config.DescriptorNotAfter {
		return 0, nil, ErrHandleUnavailable
	}
	fingerprint := affiliationFingerprint(request.StationDescriptorDigest, request.AffiliationCommitment)
	notAfter, found, err := adapter.store.getAffiliationOperation(operationID, fingerprint, nowTime.UnixMilli())
	if err != nil {
		return 0, nil, err
	}
	if !found {
		notAfter, found, err = adapter.store.getLiveAffiliation(request.StationDescriptorDigest, request.AffiliationCommitment, now)
		if err != nil {
			return 0, nil, err
		}
		if !found {
			notAfter = minUint64(now+StorageWindowSeconds, adapter.config.DescriptorNotAfter)
		}
		replayUntil := nowTime.Add(IdempotencyWindowSeconds * time.Second).UnixMilli()
		if affiliationLimit := int64(notAfter) * 1000; replayUntil > affiliationLimit {
			replayUntil = affiliationLimit
		}
		if err := adapter.store.putAffiliation(operationID, fingerprint, request, notAfter, replayUntil); err != nil {
			return 0, nil, err
		}
	}
	return notAfter, adapter.signAffiliation(request, notAfter), nil
}

func (adapter *Adapter) signAffiliation(request AffiliateRequest, notAfter uint64) []byte {
	message := make([]byte, 0, 32+32+8+64)
	message = append(message, []byte("licoarc.station-affiliation.v1\x00")...)
	message = append(message, request.StationDescriptorDigest[:]...)
	message = append(message, request.AffiliationCommitment[:]...)
	message = appendUint64(message, notAfter)
	return ed25519.Sign(adapter.config.Signer, message)
}

func (adapter *Adapter) Reserve(ctx context.Context, operationID [16]byte, request ReserveRequest) ([32]byte, uint64, []byte, error) {
	var zero [32]byte
	if request.StationDescriptorDigest != adapter.config.StationDescriptorDigest {
		return zero, 0, nil, ErrDescriptorUnavailable
	}
	nowTime := adapter.config.Now()
	affiliationNotAfter, live, err := adapter.store.getLiveAffiliation(request.StationDescriptorDigest, request.AffiliationCommitment, uint64(nowTime.Unix()))
	if err != nil {
		return zero, 0, nil, err
	}
	if !live {
		return zero, 0, nil, ErrHandleUnavailable
	}
	expectedProfile, _ := hex.DecodeString(TransportSourceDigest)
	if !equal32(request.TransportProfileDigest, expectedProfile) {
		return zero, 0, nil, ErrUnsupportedProfile
	}
	fingerprint := reserveFingerprint(request)
	if existing, found, err := adapter.store.getReservation(operationID, fingerprint); err != nil {
		return zero, 0, nil, err
	} else if found {
		adapter.mu.RLock()
		state, ok := adapter.handles[existing]
		adapter.mu.RUnlock()
		if !ok || state.Class != request.HandleClass {
			return zero, 0, nil, ErrInvalidControl
		}
		return adapter.reserveResult(request, existing, state.ServiceUntil, expectedProfile)
	}
	nativeRequestID, err := adapter.store.nativeRequestID("reserve", operationID, fingerprint, nowTime)
	if err != nil {
		return zero, 0, nil, err
	}
	nativeContext, cancel := adapter.nativeContext(ctx, nativeMutationTimeout)
	defer cancel()
	native, err := adapter.client.Reserve(nativeContext, &stationv1.ReserveRequest{
		RequestId: nativeRequestID, TtlMs: StorageWindowSeconds * 1000,
	})
	if err != nil {
		return zero, 0, nil, err
	}
	if !validNativeID(native.GetAddressId(), "sa_") || native.GetExpiresAtUnixMs() <= 0 {
		return zero, 0, nil, ErrUntrustedStation
	}
	handle, err := randomToken()
	if err != nil {
		return zero, 0, nil, err
	}
	now := uint64(nowTime.Unix())
	serviceUntil := minUint64(minUint64(now+StorageWindowSeconds, adapter.config.DescriptorNotAfter), affiliationNotAfter)
	state := handleState{NativeAddressID: native.GetAddressId(), Class: request.HandleClass, ServiceUntil: serviceUntil}
	if err := adapter.store.putReservedHandle(operationID, fingerprint, handle, state, nowTime.Add(IdempotencyWindowSeconds*time.Second).UnixMilli()); err != nil {
		return zero, 0, nil, err
	}
	adapter.mu.Lock()
	adapter.handles[handle] = state
	adapter.mu.Unlock()
	return adapter.reserveResult(request, handle, serviceUntil, expectedProfile)
}

func (adapter *Adapter) reserveResult(request ReserveRequest, handle [32]byte, serviceUntil uint64, expectedProfile []byte) ([32]byte, uint64, []byte, error) {
	if request.HandleClass == HandleFirstContact {
		return handle, 0, nil, nil
	}
	message := make([]byte, 0, 160)
	message = append(message, []byte("licoarc.station-route-service.v1\x00")...)
	message = append(message, request.StationDescriptorDigest[:]...)
	message = append(message, request.AffiliationCommitment[:]...)
	message = append(message, expectedProfile...)
	message = append(message, handle[:]...)
	message = appendUint64(message, serviceUntil)
	return handle, serviceUntil, ed25519.Sign(adapter.config.Signer, message), nil
}

func reserveFingerprint(request ReserveRequest) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("badtower.licoarc-candidate.reserve.v1\x00"))
	hash.Write(request.StationDescriptorDigest[:])
	hash.Write(request.AffiliationCommitment[:])
	hash.Write(request.TransportProfileDigest[:])
	hash.Write([]byte{byte(request.HandleClass)})
	var fingerprint [32]byte
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint
}

func (adapter *Adapter) Submit(ctx context.Context, operationID [16]byte, handle [32]byte, packet []byte) error {
	if len(packet) == 0 || len(packet) > MaxPacketBytes {
		return ErrInvalidControl
	}
	lane := &adapter.handleLanes[handle[0]&63]
	lane.Lock()
	defer lane.Unlock()
	adapter.mu.RLock()
	state, ok := adapter.handles[handle]
	adapter.mu.RUnlock()
	if !ok || state.Consumed || uint64(adapter.config.Now().Unix()) >= state.ServiceUntil {
		return ErrHandleUnavailable
	}
	operation := hex.EncodeToString(operationID[:])
	if state.Class == HandleFirstContact {
		if state.PendingOperation != "" && state.PendingOperation != operation {
			return ErrHandleUnavailable
		}
		if state.PendingOperation == "" {
			state.PendingOperation = operation
			if err := adapter.store.putHandle(handle, state); err != nil {
				return err
			}
			adapter.mu.Lock()
			adapter.handles[handle] = state
			adapter.mu.Unlock()
		}
	}
	fingerprint := nativeOperationFingerprint("submit", handle[:], packet)
	nativeRequestID, err := adapter.store.nativeRequestID("submit", operationID, fingerprint, adapter.config.Now())
	if err != nil {
		return err
	}
	nativeContext, cancel := adapter.nativeContext(ctx, nativeMutationTimeout)
	defer cancel()
	native, err := adapter.client.Submit(nativeContext, &stationv1.SubmitRequest{
		RequestId: nativeRequestID, AddressId: state.NativeAddressID,
		Payload: append([]byte(nil), packet...), TtlMs: StorageWindowSeconds * 1000,
	})
	if err != nil {
		return err
	}
	if !validNativeID(native.GetUnitId(), "tu_") || native.GetAcceptedAtUnixMs() <= 0 || native.GetExpiresAtUnixMs() <= native.GetAcceptedAtUnixMs() {
		return ErrUntrustedStation
	}
	if state.Class == HandleFirstContact {
		adapter.mu.Lock()
		state = adapter.handles[handle]
		state.Consumed = true
		state.PendingOperation = ""
		if err := adapter.store.putHandle(handle, state); err != nil {
			adapter.mu.Unlock()
			return err
		}
		adapter.handles[handle] = state
		adapter.mu.Unlock()
	}
	return nil
}

func (adapter *Adapter) Claim(ctx context.Context, operationID [16]byte, handle [32]byte, request ClaimRequest) ([32]byte, uint64, []ClaimedItem, error) {
	var zero [32]byte
	adapter.mu.RLock()
	state, ok := adapter.handles[handle]
	adapter.mu.RUnlock()
	if !ok || state.Consumed || state.Class != HandleAsync || uint64(adapter.config.Now().Unix()) >= state.ServiceUntil {
		return zero, 0, nil, ErrHandleUnavailable
	}
	maxItems := appendUint64(nil, request.MaxItems)
	fingerprint := nativeOperationFingerprint("claim", handle[:], maxItems)
	nativeRequestID, err := adapter.store.nativeRequestID("claim", operationID, fingerprint, adapter.config.Now())
	if err != nil {
		return zero, 0, nil, err
	}
	nativeContext, cancel := adapter.nativeContext(ctx, nativeClaimTimeout)
	defer cancel()
	native, err := adapter.client.Claim(nativeContext, &stationv1.ClaimRequest{
		RequestId: nativeRequestID, AddressId: state.NativeAddressID,
		MaxUnits: uint32(request.MaxItems), MaxBytes: MaxClaimBytes,
		VisibilityTimeoutMs: ClaimWindowSeconds * 1000,
	})
	if err != nil {
		return zero, 0, nil, err
	}
	if len(native.GetUnits()) == 0 {
		if native.ClaimId != nil || native.GetClaimEpoch() != 0 {
			return zero, 0, nil, ErrUntrustedStation
		}
		return zero, 0, nil, nil
	}
	if native.ClaimId == nil || !validNativeID(native.GetClaimId(), "dc_") || native.GetClaimEpoch() == 0 || native.GetClaimExpiresAtUnixMs() <= 0 || len(native.GetUnits()) > int(request.MaxItems) || len(native.GetUnits()) > MaxClaimItems {
		return zero, 0, nil, ErrUntrustedStation
	}
	claimID := derivedToken("claim", handle[:], []byte(native.GetClaimId()))
	stateClaim := claimState{Handle: handle, NativeClaimID: native.GetClaimId(), NativeEpoch: native.GetClaimEpoch(), ExpiresAtMS: native.GetClaimExpiresAtUnixMs(), Items: make(map[[32]byte]claimItem, len(native.GetUnits()))}
	items := make([]ClaimedItem, len(native.GetUnits()))
	totalBytes := 0
	seenNative := make(map[string]struct{}, len(native.GetUnits()))
	for index, unit := range native.GetUnits() {
		if !validNativeID(unit.GetUnitId(), "tu_") || len(unit.GetPayload()) == 0 || len(unit.GetPayload()) > MaxPacketBytes || unit.GetAcceptedAtUnixMs() <= 0 || unit.GetExpiresAtUnixMs() <= unit.GetAcceptedAtUnixMs() || unit.GetDeliveryAttempt() == 0 {
			return zero, 0, nil, ErrUntrustedStation
		}
		if _, duplicate := seenNative[unit.GetUnitId()]; duplicate {
			return zero, 0, nil, ErrUntrustedStation
		}
		seenNative[unit.GetUnitId()] = struct{}{}
		totalBytes += len(unit.GetPayload())
		if totalBytes > MaxClaimBytes {
			return zero, 0, nil, ErrUntrustedStation
		}
		itemID := derivedToken("item", claimID[:], []byte(unit.GetUnitId()))
		packet := append([]byte(nil), unit.GetPayload()...)
		stateClaim.Items[itemID] = claimItem{NativeUnitID: unit.GetUnitId(), Packet: packet}
		items[index] = ClaimedItem{ItemID: itemID, ProtectedPacket: packet}
	}
	adapter.mu.Lock()
	if err := adapter.store.putClaim(claimID, stateClaim); err != nil {
		adapter.mu.Unlock()
		return zero, 0, nil, err
	}
	adapter.claims[claimID] = stateClaim
	adapter.mu.Unlock()
	return claimID, native.GetClaimEpoch(), items, nil
}

func (adapter *Adapter) Retrieve(_ context.Context, handle [32]byte, request RetrieveRequest) ([]byte, error) {
	adapter.mu.RLock()
	defer adapter.mu.RUnlock()
	if _, ok := adapter.handles[handle]; !ok {
		return nil, ErrHandleUnavailable
	}
	claim, ok := adapter.claims[request.ClaimID]
	if !ok || claim.Handle != handle || claim.NativeEpoch != request.ClaimEpoch || adapter.config.Now().UnixMilli() >= claim.ExpiresAtMS {
		return nil, ErrClaimUnavailable
	}
	item, ok := claim.Items[request.ItemID]
	if !ok {
		return nil, ErrClaimUnavailable
	}
	return append([]byte(nil), item.Packet...), nil
}

func (adapter *Adapter) Settle(ctx context.Context, operationID [16]byte, handle [32]byte, request SettleRequest) ([]SettlementResult, error) {
	adapter.mu.RLock()
	_, handleOK := adapter.handles[handle]
	claim, claimOK := adapter.claims[request.ClaimID]
	adapter.mu.RUnlock()
	if !handleOK {
		return nil, ErrHandleUnavailable
	}
	if !claimOK || claim.Handle != handle || claim.NativeEpoch != request.ClaimEpoch || adapter.config.Now().UnixMilli() >= claim.ExpiresAtMS {
		return nil, ErrClaimUnavailable
	}
	entries := make([]*stationv1.SettleEntry, len(request.Settlements))
	for index, settlement := range request.Settlements {
		item, ok := claim.Items[settlement.ItemID]
		if !ok {
			return nil, ErrClaimUnavailable
		}
		action := stationv1.SettleAction_SETTLE_ACTION_COMPLETE
		if settlement.Action == SettleRelease {
			action = stationv1.SettleAction_SETTLE_ACTION_RELEASE
		}
		entries[index] = &stationv1.SettleEntry{UnitId: item.NativeUnitID, Action: action}
	}
	fingerprintParts := make([][]byte, 0, 3+len(request.Settlements)*2)
	fingerprintParts = append(fingerprintParts, handle[:], request.ClaimID[:], appendUint64(nil, request.ClaimEpoch))
	for _, settlement := range request.Settlements {
		fingerprintParts = append(fingerprintParts, settlement.ItemID[:], []byte{byte(settlement.Action)})
	}
	fingerprint := nativeOperationFingerprint("settle", fingerprintParts...)
	nativeRequestID, err := adapter.store.nativeRequestID("settle", operationID, fingerprint, adapter.config.Now())
	if err != nil {
		return nil, err
	}
	nativeContext, cancel := adapter.nativeContext(ctx, nativeMutationTimeout)
	defer cancel()
	native, err := adapter.client.Settle(nativeContext, &stationv1.SettleRequest{
		RequestId: nativeRequestID, ClaimId: claim.NativeClaimID,
		ClaimEpoch: claim.NativeEpoch, Entries: entries,
	})
	if err != nil {
		return nil, err
	}
	if len(native.GetUnits()) != len(entries) {
		return nil, ErrUntrustedStation
	}
	results := make([]SettlementResult, len(native.GetUnits()))
	byNative := make(map[string][32]byte, len(claim.Items))
	for itemID, item := range claim.Items {
		byNative[item.NativeUnitID] = itemID
	}
	for index, unit := range native.GetUnits() {
		itemID, known := byNative[unit.GetUnitId()]
		if !known {
			return nil, ErrUntrustedStation
		}
		delete(byNative, unit.GetUnitId())
		var outcome SettleOutcome
		switch unit.GetOutcome() {
		case stationv1.SettleOutcome_SETTLE_OUTCOME_COMPLETED:
			outcome = SettledCompleted
		case stationv1.SettleOutcome_SETTLE_OUTCOME_RELEASED:
			outcome = SettledReleased
		case stationv1.SettleOutcome_SETTLE_OUTCOME_ATTEMPT_LIMIT:
			outcome = SettledAttemptLimit
		default:
			return nil, ErrUntrustedStation
		}
		results[index] = SettlementResult{ItemID: itemID, Outcome: outcome}
	}
	// Retain the bounded claim projection until its existing deadline. If the
	// process stops after Native Settle commits but before the HTTP response is
	// persisted, the same operation can replay through Native idempotency and
	// reconstruct the exact accepted response after restart.
	return results, nil
}

func (adapter *Adapter) pruneExpired() error {
	targets, err := adapter.store.pruneExpired(adapter.config.Now().UnixMilli(), 64)
	if err != nil {
		return err
	}
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	for _, target := range targets {
		switch {
		case strings.HasPrefix(target, "handle/"):
			if token, ok := parseToken(strings.TrimPrefix(target, "handle/")); ok {
				delete(adapter.handles, token)
			}
		case strings.HasPrefix(target, "claim/"):
			if token, ok := parseToken(strings.TrimPrefix(target, "claim/")); ok {
				delete(adapter.claims, token)
			}
		}
	}
	return nil
}

func (adapter *Adapter) nativeContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	bounded, cancel := context.WithTimeout(ctx, timeout)
	return metadata.AppendToOutgoingContext(bounded, "authorization", "Bearer "+adapter.config.NativeBearer), cancel
}

func affiliationFingerprint(descriptor, commitment [32]byte) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("badtower.licoarc-candidate.affiliation.v1\x00"))
	hash.Write(descriptor[:])
	hash.Write(commitment[:])
	var fingerprint [32]byte
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint
}

func nativeOperationFingerprint(operation string, parts ...[]byte) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("badtower.external-native-operation.v1\x00"))
	hash.Write([]byte(operation))
	for _, part := range parts {
		hash.Write(appendUint64(nil, uint64(len(part))))
		hash.Write(part)
	}
	var fingerprint [32]byte
	copy(fingerprint[:], hash.Sum(nil))
	return fingerprint
}

func randomToken() ([32]byte, error) {
	var token [32]byte
	_, err := rand.Read(token[:])
	return token, err
}

func derivedToken(domain string, parts ...[]byte) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("badtower.licoarc-candidate." + domain + "\x00"))
	for _, part := range parts {
		hash.Write(part)
	}
	var token [32]byte
	copy(token[:], hash.Sum(nil))
	return token
}

func tokenString(token [32]byte) string { return base64.RawURLEncoding.EncodeToString(token[:]) }

func parseToken(value string) ([32]byte, bool) {
	var token [32]byte
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) != 32 || base64.RawURLEncoding.EncodeToString(raw) != value {
		return token, false
	}
	copy(token[:], raw)
	return token, true
}

func validNativeID(value, prefix string) bool {
	if len(value) != 25 || len(prefix) != 3 || value[:3] != prefix {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(value[3:])
	return err == nil && len(raw) == 16 && base64.RawURLEncoding.EncodeToString(raw) == value[3:]
}

func mapNativeFailure(err error) (Outcome, FailureClass, uint64) {
	if errors.Is(err, ErrStateCapacity) {
		return OutcomeTransient, FailureCapacityUnavailable, 1000
	}
	if errors.Is(err, ErrUntrustedStation) {
		return OutcomeAmbiguous, FailureInternalUnavailable, 0
	}
	if errors.Is(err, ErrInvalidControl) {
		return OutcomeRejected, FailureInvalidRequest, 0
	}
	if errors.Is(err, ErrUnsupportedProfile) {
		return OutcomeRejected, FailureUnsupportedProfile, 0
	}
	if errors.Is(err, ErrDescriptorUnavailable) {
		return OutcomeRejected, FailureTargetUnavailable, 0
	}
	if errors.Is(err, ErrHandleUnavailable) || errors.Is(err, ErrClaimUnavailable) {
		return OutcomeRejected, FailureTargetUnavailable, 0
	}
	converted := status.Convert(err)
	reason := ""
	retryable := false
	retryAfter := uint64(0)
	for _, detail := range converted.Details() {
		if failure, ok := detail.(*stationv1.StationFailure); ok {
			reason, retryable, retryAfter = failure.GetReason(), failure.GetRetryable(), uint64(failure.GetRetryAfterMs())
		}
	}
	switch reason {
	case "INVALID_REQUEST", "DEADLINE_REQUIRED":
		return OutcomeRejected, FailureInvalidRequest, 0
	case "VERSION_UNSUPPORTED":
		return OutcomeRejected, FailureUnsupportedProfile, 0
	case "ADDRESS_NOT_FOUND", "ADDRESS_EXPIRED", "CLAIM_STALE", "CLAIM_EXPIRED":
		return OutcomeRejected, FailureTargetUnavailable, 0
	case "CAPACITY_EXCEEDED":
		return OutcomeTransient, FailureCapacityUnavailable, clampRetry(retryAfter)
	case "RATE_LIMITED":
		return OutcomeTransient, FailureRateLimited, clampRetry(retryAfter)
	case "IDEMPOTENCY_CONFLICT":
		return OutcomeRejected, FailureConflict, 0
	case "DEADLINE_EXCEEDED":
		return OutcomeAmbiguous, FailureInternalUnavailable, 0
	case "DRAINING", "STORAGE_UNAVAILABLE", "CLOCK_UNSAFE":
		return OutcomeTransient, FailureInternalUnavailable, clampRetry(retryAfter)
	default:
		if retryable {
			return OutcomeTransient, FailureInternalUnavailable, clampRetry(retryAfter)
		}
		return OutcomeAmbiguous, FailureInternalUnavailable, 0
	}
}

func equal32(left [32]byte, right []byte) bool {
	return len(right) == 32 && string(left[:]) == string(right)
}
func minUint64(left, right uint64) uint64 {
	if left < right {
		return left
	}
	return right
}
func clampRetry(value uint64) uint64 {
	if value > MaxRetryAfterMS {
		return MaxRetryAfterMS
	}
	return value
}
func appendUint64(target []byte, value uint64) []byte {
	return append(target, byte(value>>56), byte(value>>48), byte(value>>40), byte(value>>32), byte(value>>24), byte(value>>16), byte(value>>8), byte(value))
}

func adapterStateBinding(config Config) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("badtower.licoarc-candidate.state-binding.v1\x00"))
	hash.Write([]byte(config.NativeNamespace))
	hash.Write(config.StationDescriptorDigest[:])
	hash.Write(config.Signer.Public().(ed25519.PublicKey))
	hash.Write(appendUint64(nil, config.DescriptorNotAfter))
	var binding [32]byte
	copy(binding[:], hash.Sum(nil))
	return binding
}
