package licoarccandidate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	controlMediaType = "application/licoarc-transport+cbor"
	packetMediaType  = "application/licoarc-protected-packet"
)

type cachedResponse struct {
	fingerprint [32]byte
	status      int
	headers     http.Header
	body        []byte
	expiresAt   time.Time
}

type Handler struct {
	adapter  *Adapter
	lanes    [64]sync.Mutex
	cacheMu  sync.Mutex
	cache    map[[16]byte]cachedResponse
	active   chan struct{}
	admitted chan struct{}
}

func NewHandler(adapter *Adapter) (*Handler, error) {
	if adapter == nil {
		return nil, ErrInvalidControl
	}
	return &Handler{
		adapter: adapter, cache: make(map[[16]byte]cachedResponse),
		active:   make(chan struct{}, adapter.config.MaxConcurrentOperations),
		admitted: make(chan struct{}, adapter.config.MaxConcurrentOperations+adapter.config.MaxQueuedOperations),
	}, nil
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	operationID, ok := validateCarrier(request)
	if !ok {
		handler.writeFailure(writer, operationUnknown, OutcomeRejected, FailureInvalidRequest, 0)
		return
	}
	op := operationForPath(request.URL.Path)
	select {
	case handler.admitted <- struct{}{}:
		defer func() { <-handler.admitted }()
	default:
		handler.writeFailure(writer, op, OutcomeTransient, FailureRateLimited, 100)
		return
	}
	select {
	case handler.active <- struct{}{}:
		defer func() { <-handler.active }()
	case <-request.Context().Done():
		handler.writeFailure(writer, op, OutcomeTransient, FailureInternalUnavailable, 100)
		return
	}
	if err := handler.adapter.pruneExpired(); err != nil {
		handler.writeFailure(writer, op, OutcomeTransient, FailureInternalUnavailable, 100)
		return
	}
	lane := &handler.lanes[operationID[0]&63]
	lane.Lock()
	defer lane.Unlock()

	body, ok := readBody(request)
	if !ok {
		handler.writeFailure(writer, operationForPath(request.URL.Path), OutcomeRejected, FailureInvalidRequest, 0)
		return
	}
	fingerprint := requestFingerprint(request, body)
	if cached, found := handler.cached(operationID); found {
		if cached.fingerprint != fingerprint {
			handler.writeFailure(writer, operationForPath(request.URL.Path), OutcomeRejected, FailureConflict, 0)
			return
		}
		writeRecorded(writer, cached.status, cached.headers, cached.body)
		return
	}

	recorded := handler.process(request.Context(), request, operationID, body)
	recorded.fingerprint = fingerprint
	recorded.expiresAt = handler.adapter.config.Now().Add(IdempotencyWindowSeconds * time.Second)
	if err := handler.adapter.store.putResponse(operationID, recorded); err != nil {
		recorded = failureResponse(operationForPath(request.URL.Path), OutcomeAmbiguous, FailureInternalUnavailable, 0)
		recorded.fingerprint = fingerprint
		recorded.expiresAt = handler.adapter.config.Now().Add(IdempotencyWindowSeconds * time.Second)
		writeRecorded(writer, recorded.status, recorded.headers, recorded.body)
		return
	}
	handler.cacheMu.Lock()
	handler.cache[operationID] = recorded
	handler.pruneLocked(handler.adapter.config.Now())
	handler.cacheMu.Unlock()
	writeRecorded(writer, recorded.status, recorded.headers, recorded.body)
}

type operation uint8

const (
	operationUnknown operation = iota
	operationAffiliate
	operationReserve
	operationSubmit
	operationRetrieve
	operationClaim
	operationSettle
)

func (handler *Handler) process(ctx context.Context, request *http.Request, operationID [16]byte, body []byte) cachedResponse {
	op, handle, ok := route(request.Method, request.URL.Path)
	if !ok {
		return failureResponse(operationUnknown, OutcomeRejected, FailureInvalidRequest, 0)
	}
	switch op {
	case operationAffiliate:
		decoded, err := DecodeAffiliate(body)
		if err != nil {
			return failureResponse(op, OutcomeRejected, FailureInvalidRequest, 0)
		}
		notAfter, signature, err := handler.adapter.Affiliate(ctx, operationID, decoded)
		if err != nil {
			return mappedFailure(op, err)
		}
		response, err := EncodeAffiliateResponse(OutcomeAccepted, notAfter, signature, 0, 0)
		if err != nil {
			return mappedFailure(op, err)
		}
		return acceptedResponse(op, response, nil)
	case operationReserve:
		decoded, err := DecodeReserve(body)
		if err != nil {
			return failureResponse(op, OutcomeRejected, FailureInvalidRequest, 0)
		}
		deliveryHandle, serviceUntil, signature, err := handler.adapter.Reserve(ctx, operationID, decoded)
		if err != nil {
			return mappedFailure(op, err)
		}
		response, err := EncodeReserveResponse(OutcomeAccepted, deliveryHandle, decoded.HandleClass, serviceUntil, signature, 0, 0)
		if err != nil {
			return mappedFailure(op, err)
		}
		return acceptedResponse(op, response, nil)
	case operationSubmit:
		if len(body) == 0 || len(body) > MaxPacketBytes {
			return failureResponse(op, OutcomeRejected, FailurePacketTooLarge, 0)
		}
		if err := handler.adapter.Submit(ctx, operationID, handle, body); err != nil {
			return mappedFailure(op, err)
		}
		response, _ := EncodeSubmitResponse(OutcomeAccepted, 0, 0)
		return acceptedResponse(op, response, nil)
	case operationRetrieve:
		decoded, err := DecodeRetrieve(body)
		if err != nil {
			return failureResponse(op, OutcomeRejected, FailureInvalidRequest, 0)
		}
		packet, err := handler.adapter.Retrieve(ctx, handle, decoded)
		if err != nil {
			return mappedFailure(op, err)
		}
		return acceptedResponse(op, packet, http.Header{"Content-Type": []string{packetMediaType}})
	case operationClaim:
		decoded, err := DecodeClaim(body)
		if err != nil {
			return failureResponse(op, OutcomeRejected, FailureInvalidRequest, 0)
		}
		claimID, epoch, items, err := handler.adapter.Claim(ctx, operationID, handle, decoded)
		if err != nil {
			return mappedFailure(op, err)
		}
		response, err := EncodeClaimResponse(OutcomeAccepted, claimID, epoch, items, 0, 0)
		if err != nil {
			return mappedFailure(op, err)
		}
		headers := http.Header{}
		if len(items) > 0 {
			headers.Set("LicoArc-Claim-Id", tokenString(claimID))
			headers.Set("LicoArc-Claim-Epoch", strconv.FormatUint(epoch, 10))
		}
		return acceptedResponse(op, response, headers)
	case operationSettle:
		decoded, err := DecodeSettle(body)
		if err != nil {
			return failureResponse(op, OutcomeRejected, FailureInvalidRequest, 0)
		}
		results, err := handler.adapter.Settle(ctx, operationID, handle, decoded)
		if err != nil {
			return mappedFailure(op, err)
		}
		response, err := EncodeSettleResponse(OutcomeAccepted, results, 0, 0)
		if err != nil {
			return mappedFailure(op, err)
		}
		return acceptedResponse(op, response, nil)
	default:
		return failureResponse(operationUnknown, OutcomeRejected, FailureInvalidRequest, 0)
	}
}

func validateCarrier(request *http.Request) ([16]byte, bool) {
	var operationID [16]byte
	if request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || request.TLS.NegotiatedProtocol != "h2" || request.ProtoMajor != 2 || request.Method != http.MethodPost || request.Host == "" || request.URL.RawQuery != "" || request.URL.Fragment != "" || len(request.TransferEncoding) != 0 || len(request.Trailer) != 0 {
		return operationID, false
	}
	if encoding := request.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return operationID, false
	}
	if request.Header.Get("Trailer") != "" || request.ContentLength < 0 {
		return operationID, false
	}
	contentLength := request.Header.Get("Content-Length")
	if contentLength == "" || contentLength != strconv.FormatInt(request.ContentLength, 10) {
		return operationID, false
	}
	headerBytes := 0
	for name, values := range request.Header {
		headerBytes += len(name)
		for _, value := range values {
			headerBytes += len(value)
		}
	}
	if headerBytes > 2048 || len(request.URL.EscapedPath()) > 128 {
		return operationID, false
	}
	rawID := request.Header.Get("LicoArc-Operation-Id")
	if len(rawID) != 32 || strings.ToLower(rawID) != rawID {
		return operationID, false
	}
	decoded, err := hex.DecodeString(rawID)
	if err != nil || len(decoded) != 16 || hex.EncodeToString(decoded) != rawID {
		return operationID, false
	}
	copy(operationID[:], decoded)
	op, _, routeOK := route(request.Method, request.URL.Path)
	if !routeOK {
		return operationID, false
	}
	expectedMedia := controlMediaType
	if op == operationSubmit {
		expectedMedia = packetMediaType
	}
	if request.Header.Get("Content-Type") != expectedMedia {
		return operationID, false
	}
	return operationID, true
}

func readBody(request *http.Request) ([]byte, bool) {
	limit := int64(MaxControlBodyBytes)
	if operationForPath(request.URL.Path) == operationSubmit {
		limit = MaxPacketBytes
	}
	if request.ContentLength <= 0 || request.ContentLength > limit {
		return nil, false
	}
	limited := &io.LimitedReader{R: request.Body, N: limit + 1}
	raw, err := io.ReadAll(limited)
	if err != nil || int64(len(raw)) != request.ContentLength || limited.N == 0 {
		return nil, false
	}
	return raw, true
}

func route(method, path string) (operation, [32]byte, bool) {
	var zero [32]byte
	if method != http.MethodPost {
		return operationUnknown, zero, false
	}
	switch path {
	case "/v1/affiliate":
		return operationAffiliate, zero, true
	case "/v1/reserve":
		return operationReserve, zero, true
	}
	parts := strings.Split(path, "/")
	if len(parts) != 5 || parts[1] != "v1" || parts[2] != "handles" {
		return operationUnknown, zero, false
	}
	handle, ok := parseToken(parts[3])
	if !ok {
		return operationUnknown, zero, false
	}
	switch parts[4] {
	case "submit":
		return operationSubmit, handle, true
	case "retrieve":
		return operationRetrieve, handle, true
	case "claim":
		return operationClaim, handle, true
	case "settle":
		return operationSettle, handle, true
	default:
		return operationUnknown, zero, false
	}
}

func operationForPath(path string) operation {
	op, _, _ := route(http.MethodPost, path)
	return op
}

func requestFingerprint(request *http.Request, body []byte) [32]byte {
	hash := sha256.New()
	hash.Write([]byte("badtower.licoarc-candidate.http.v1\x00"))
	hash.Write([]byte(request.Method))
	hash.Write([]byte{0})
	hash.Write([]byte(request.URL.Path))
	hash.Write([]byte{0})
	hash.Write([]byte(request.Header.Get("Content-Type")))
	hash.Write([]byte{0})
	hash.Write(body)
	var result [32]byte
	copy(result[:], hash.Sum(nil))
	return result
}

func (handler *Handler) cached(operationID [16]byte) (cachedResponse, bool) {
	handler.cacheMu.Lock()
	defer handler.cacheMu.Unlock()
	cached, ok := handler.cache[operationID]
	if ok && !handler.adapter.config.Now().Before(cached.expiresAt) {
		delete(handler.cache, operationID)
		return cachedResponse{}, false
	}
	if ok {
		return cached, true
	}
	persisted, found := handler.adapter.store.getResponse(operationID, handler.adapter.config.Now())
	if found {
		handler.cache[operationID] = persisted
	}
	return persisted, found
}

func (handler *Handler) pruneLocked(now time.Time) {
	for operationID, cached := range handler.cache {
		if !now.Before(cached.expiresAt) {
			delete(handler.cache, operationID)
		}
	}
}

func acceptedResponse(op operation, body []byte, extra http.Header) cachedResponse {
	headers := http.Header{"LicoArc-Outcome": []string{"accepted"}, "Content-Type": []string{controlMediaType}}
	for name, values := range extra {
		headers[name] = append([]string(nil), values...)
	}
	statusCode := map[operation]int{operationAffiliate: 201, operationReserve: 201, operationSubmit: 202, operationRetrieve: 200, operationClaim: 200, operationSettle: 200}[op]
	return cachedResponse{status: statusCode, headers: headers, body: append([]byte(nil), body...)}
}

func mappedFailure(op operation, err error) cachedResponse {
	outcome, failure, retry := mapNativeFailure(err)
	return failureResponse(op, outcome, failure, retry)
}

func failureResponse(op operation, outcome Outcome, failure FailureClass, retry uint64) cachedResponse {
	var body []byte
	var err error
	switch op {
	case operationAffiliate:
		body, err = EncodeAffiliateResponse(outcome, 0, nil, failure, retry)
	case operationReserve:
		body, err = EncodeReserveResponse(outcome, [32]byte{}, 0, 0, nil, failure, retry)
	case operationClaim:
		body, err = EncodeClaimResponse(outcome, [32]byte{}, 0, nil, failure, retry)
	case operationSettle:
		body, err = EncodeSettleResponse(outcome, nil, failure, retry)
	default:
		body, err = EncodeSubmitResponse(outcome, failure, retry)
	}
	if err != nil {
		body, _ = EncodeSubmitResponse(OutcomeAmbiguous, FailureInternalUnavailable, 0)
		outcome, failure, retry = OutcomeAmbiguous, FailureInternalUnavailable, 0
	}
	headers := http.Header{
		"Content-Type":          []string{controlMediaType},
		"LicoArc-Outcome":       []string{outcomeName(outcome)},
		"LicoArc-Failure-Class": []string{failureName(failure)},
	}
	if outcome == OutcomeTransient {
		headers.Set("LicoArc-Retry-After-Ms", strconv.FormatUint(retry, 10))
	}
	return cachedResponse{status: statusFor(outcome, failure), headers: headers, body: body}
}

func (handler *Handler) writeFailure(writer http.ResponseWriter, op operation, outcome Outcome, failure FailureClass, retry uint64) {
	recorded := failureResponse(op, outcome, failure, retry)
	writeRecorded(writer, recorded.status, recorded.headers, recorded.body)
}

func writeRecorded(writer http.ResponseWriter, statusCode int, headers http.Header, body []byte) {
	for name, values := range headers {
		for _, value := range values {
			writer.Header().Add(name, value)
		}
	}
	writer.Header().Set("Content-Length", strconv.Itoa(len(body)))
	writer.WriteHeader(statusCode)
	_, _ = io.Copy(writer, bytes.NewReader(body))
}

func statusFor(outcome Outcome, failure FailureClass) int {
	switch outcome {
	case OutcomeRejected:
		switch failure {
		case FailureTargetUnavailable:
			return 404
		case FailureUnsupportedProfile:
			return 406
		case FailureConflict:
			return 409
		case FailurePacketTooLarge:
			return 413
		default:
			return 400
		}
	case OutcomeTransient:
		if failure == FailureRateLimited || failure == FailureCapacityUnavailable {
			return 429
		}
		return 503
	case OutcomeAmbiguous:
		return 504
	default:
		return 500
	}
}

func outcomeName(outcome Outcome) string {
	return [...]string{"accepted", "rejected", "transient", "ambiguous"}[outcome]
}

func failureName(failure FailureClass) string {
	return [...]string{"invalidRequest", "unsupportedProfile", "targetUnavailable", "packetTooLarge", "capacityUnavailable", "rateLimited", "conflict", "internalUnavailable"}[failure]
}
