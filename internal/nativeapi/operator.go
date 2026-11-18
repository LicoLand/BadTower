package nativeapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
	stationoverview "github.com/LicoLand/BadTower/web/station-overview"
	"golang.org/x/net/netutil"
)

const (
	// OperatorOverviewPath is the only operator endpoint.
	OperatorOverviewPath = "/operator/v1/overview"

	// OperatorOverviewSchema and OperatorErrorSchema freeze the operator
	// success and error JSON schemas; both are exact and versioned.
	OperatorOverviewSchema = "badtower.station-overview.v1"
	OperatorErrorSchema    = "badtower.station-overview.error.v1"

	// OperatorPollAfterMs is the fixed operator poll interval in milliseconds.
	OperatorPollAfterMs = 1000

	operatorJSONContentType  = "application/json; charset=utf-8"
	operatorMaxConnections   = 16
	operatorMaxHandlers      = 4
	operatorMaxHeaderBytes   = 8 << 10
	operatorMaxResponseBytes = 4 << 10
	operatorGlobalRate       = 8.0
	operatorGlobalBurst      = 16.0
	operatorCredentialRate   = 1.0
	operatorCredentialBurst  = 2.0
	operatorRetryAfterMs     = 1000

	operatorReadHeaderTimeout = 2 * time.Second
	operatorReadTimeout       = 3 * time.Second
	operatorWriteTimeout      = 3 * time.Second
	operatorIdleTimeout       = 15 * time.Second
	operatorShutdownTimeout   = 3 * time.Second

	// staticAssetPrefix is the only public static namespace: the embedded
	// hashed assets under dist/assets.
	staticAssetPrefix = "/assets/"

	// staticIndexCacheControl never caches the shell, so a fresh build always
	// reaches clients. staticAssetCacheControl pins hashed assets, which are
	// content-addressed and therefore safe to cache for one year.
	staticIndexCacheControl = "no-store"
	staticAssetCacheControl = "public, max-age=31536000, immutable"

	// operatorStaticCSP is the fixed content-security policy of every static
	// response: only same-origin scripts, styles, and connections; images may
	// inline data: (the embedded favicon); object, base, form, and frame are
	// denied.
	operatorStaticCSP = "default-src 'self'; script-src 'self'; style-src 'self'; " +
		"connect-src 'self'; img-src 'self' data:; object-src 'none'; " +
		"base-uri 'none'; form-action 'none'; frame-src 'none'; frame-ancestors 'none'"
)

// Readiness statuses and reasons form a closed contract: status is SERVING if
// and only if reason is NONE; every NOT_SERVING pairing carries exactly one
// fixed reason: STARTING, CLOCK_UNSAFE, DRAINING, or STOPPED.
const (
	ReadinessServing    = "SERVING"
	ReadinessNotServing = "NOT_SERVING"

	ReadinessReasonNone        = "NONE"
	ReadinessReasonStarting    = "STARTING"
	ReadinessReasonClockUnsafe = "CLOCK_UNSAFE"
	ReadinessReasonDraining    = "DRAINING"
	ReadinessReasonStopped     = "STOPPED"
)

// OverviewResponse is the frozen operator overview success schema. It carries
// only station-wide, identifier-free detail: no profile, namespace, address,
// unit, claim, request, path, credential, idempotency record, scheduler, or
// raw runtime value is ever emitted.
type OverviewResponse struct {
	Schema      string            `json:"schema"`
	Lifecycle   string            `json:"lifecycle"`
	Readiness   ReadinessOverview `json:"readiness"`
	Pressure    string            `json:"pressure"`
	Capacity    CapacityOverview  `json:"capacity"`
	PollAfterMs int               `json:"pollAfterMs"`
}

// ReadinessOverview is the exact readiness/reason pairing derived from
// core.Ready() and the observed lifecycle.
type ReadinessOverview struct {
	Status string `json:"status"`
	Reason string `json:"reason"`
}

// CapacityOverview reports the configured namespace count, the profile's
// implementation ceiling, and the O(1) station-wide aggregate usage. No
// namespace identity, per-namespace counter, or queue detail is included.
type CapacityOverview struct {
	ConfiguredNamespaceCount int                   `json:"configuredNamespaceCount"`
	ImplementationCeiling    ImplementationCeiling `json:"implementationCeiling"`
	AggregateUsage           CapacityUsage         `json:"aggregateUsage"`
}

// ImplementationCeiling mirrors the profile implementation ceilings 1:1,
// with the per-namespace limits collapsed into one capacity projection.
type ImplementationCeiling struct {
	NamespaceCount uint64        `json:"namespaceCount"`
	PerNamespace   CapacityUsage `json:"perNamespace"`
}

// CapacityUsage is the capacity projection shared by the implementation
// ceiling and the aggregate usage; it is identifier-free.
type CapacityUsage struct {
	Addresses            uint64 `json:"addresses"`
	TransportUnits       uint64 `json:"transportUnits"`
	LogicalRetainedBytes uint64 `json:"logicalRetainedBytes"`
	ActiveClaims         uint64 `json:"activeClaims"`
}

// Overview returns one privacy-minimal station-wide operator overview. Every
// value is derived from O(1) observation and profile limits: the configured
// namespace count from the validated config, aggregate usage and pressure
// from ObservationSnapshot, readiness from core.Ready() and the observed
// lifecycle, and the implementation ceiling from ProfileLimits. It performs
// no store scan, no maintenance pass, and no persistence.
func (server *Server) Overview() (OverviewResponse, error) {
	if server.core == nil {
		return OverviewResponse{}, stationFailure{Reason: "INTERNAL"}
	}
	observation := server.core.ObservationSnapshot()
	observation.Lifecycle = stationLifecycle(server.Lifecycle())
	ready, reason := server.Readiness()
	limits := server.core.ProfileLimits()
	return overviewResponse(observation, ready, reason, limits, len(server.config.Namespaces)), nil
}

func stationLifecycle(lifecycle LifecycleState) stationcore.StationLifecycle {
	switch lifecycle {
	case LifecycleStarting:
		return stationcore.StationLifecycleStarting
	case LifecycleRunning:
		return stationcore.StationLifecycleRunning
	case LifecycleDraining:
		return stationcore.StationLifecycleDraining
	default:
		return stationcore.StationLifecycleStopped
	}
}

// overviewResponse is the single pure projection of one station-wide overview
// from the O(1) observation, the exact core.Ready() pairing, and the profile
// limits. It is shared by the HTTP handler and tests.
func overviewResponse(observation stationcore.StationObservation, ready bool, reason stationcore.Reason, limits stationcore.ProfileLimits, configuredNamespaceCount int) OverviewResponse {
	return OverviewResponse{
		Schema:    OperatorOverviewSchema,
		Lifecycle: string(observation.Lifecycle),
		Readiness: readinessOverview(ready, reason, observation.Lifecycle),
		Pressure:  pressureLabel(observation.Pressure),
		Capacity: CapacityOverview{
			ConfiguredNamespaceCount: configuredNamespaceCount,
			ImplementationCeiling: ImplementationCeiling{
				NamespaceCount: limits.MaxNamespaces,
				PerNamespace: CapacityUsage{
					Addresses:            limits.MaxAddressesPerNamespace,
					TransportUnits:       limits.MaxUnitsPerNamespace,
					LogicalRetainedBytes: limits.MaxRetainedBytesPerNamespace,
					ActiveClaims:         limits.MaxActiveClaimsPerNamespace,
				},
			},
			AggregateUsage: capacityUsage(observation.Aggregate),
		},
		PollAfterMs: OperatorPollAfterMs,
	}
}

// readinessOverview maps the exact core.Ready() pairing and the observed
// lifecycle onto the closed readiness contract: SERVING iff reason NONE, and
// every NOT_SERVING pairing carries exactly one fixed reason (STARTING,
// CLOCK_UNSAFE, DRAINING, or STOPPED).
func readinessOverview(ready bool, reason stationcore.Reason, lifecycle stationcore.StationLifecycle) ReadinessOverview {
	if ready {
		return ReadinessOverview{Status: ReadinessServing, Reason: ReadinessReasonNone}
	}
	if reason == stationcore.ReasonClockUnsafe {
		return ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonClockUnsafe}
	}
	switch lifecycle {
	case stationcore.StationLifecycleStarting:
		return ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonStarting}
	case stationcore.StationLifecycleStopped:
		return ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonStopped}
	default:
		return ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonDraining}
	}
}

func capacityUsage(aggregate stationcore.StationAggregate) CapacityUsage {
	return CapacityUsage{
		Addresses:            aggregate.Addresses,
		TransportUnits:       aggregate.Units,
		LogicalRetainedBytes: aggregate.RetainedBytes,
		ActiveClaims:         aggregate.ActiveClaims,
	}
}

func pressureLabel(pressure stationcore.Pressure) string {
	switch pressure {
	case stationcore.PressureSoft:
		return "SOFT"
	case stationcore.PressureHard:
		return "HARD"
	default:
		return "NORMAL"
	}
}

// operatorCredential is one active operator bearer credential digest snapshot.
type operatorCredential struct {
	digest    string
	notBefore time.Time
	notAfter  time.Time
}

// operatorAuthenticator resolves operator bearer tokens fail-closed in
// constant time: every active digest is always compared, no comparison
// short-circuits, and no branch depends on token content or credential
// position. Config validation guarantees every stored digest has the same
// fixed length, so the comparisons themselves are uniform.
type operatorAuthenticator struct {
	credentials []operatorCredential
	now         func() time.Time
}

func newOperatorAuthenticator(config Config) *operatorAuthenticator {
	auth := &operatorAuthenticator{now: time.Now}
	if config.Operator == nil || !config.Operator.Enabled {
		return auth
	}
	for _, credential := range config.Operator.Credentials {
		auth.credentials = append(auth.credentials, operatorCredential{
			digest:    credential.SHA256,
			notBefore: credential.NotBefore,
			notAfter:  credential.NotAfter,
		})
	}
	return auth
}

// authorize reports whether one bearer token matches any active credential.
func (auth *operatorAuthenticator) authorize(token string) (int, bool) {
	if len(auth.credentials) == 0 {
		return 0, false
	}
	digest := TokenDigest(token)
	now := auth.now().UTC()
	matchedIndex := 0
	matched := 0
	for index, credential := range auth.credentials {
		equal := subtle.ConstantTimeCompare([]byte(credential.digest), []byte(digest))
		active := 0
		if !now.Before(credential.notBefore) && !now.After(credential.notAfter) {
			active = 1
		}
		admitted := equal & active
		matchedIndex = subtle.ConstantTimeSelect(admitted, index, matchedIndex)
		matched |= admitted
	}
	return matchedIndex, matched == 1
}

// tokenBucket is a fixed-size, in-memory limiter. It stores no request,
// credential, address, or other runtime identity.
type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newTokenBucket(rate, burst float64) *tokenBucket {
	now := time.Now
	return &tokenBucket{rate: rate, burst: burst, tokens: burst, last: now(), now: now}
}

func (bucket *tokenBucket) allow() bool {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	now := bucket.now()
	if elapsed := now.Sub(bucket.last).Seconds(); elapsed > 0 {
		bucket.tokens += elapsed * bucket.rate
		if bucket.tokens > bucket.burst {
			bucket.tokens = bucket.burst
		}
		bucket.last = now
	}
	if bucket.tokens < 1 {
		return false
	}
	bucket.tokens--
	return true
}

// OperatorServer exposes the strict operator overview listener. It is
// loopback-only by configuration, bearer-authenticated in constant time, and
// serves exactly one bounded endpoint.
type OperatorServer struct {
	server            *Server
	auth              *operatorAuthenticator
	enabled           bool
	handlers          chan struct{}
	ingress           *tokenBucket
	credentialIngress []*tokenBucket
	mu                sync.Mutex
	http              *http.Server
}

// NewOperatorServer constructs one operator listener bound to the process.
func NewOperatorServer(server *Server) *OperatorServer {
	auth := newOperatorAuthenticator(server.config)
	operator := &OperatorServer{
		server:   server,
		auth:     auth,
		enabled:  server.config.Operator != nil && server.config.Operator.Enabled,
		handlers: make(chan struct{}, operatorMaxHandlers),
		ingress:  newTokenBucket(operatorGlobalRate, operatorGlobalBurst),
	}
	for range auth.credentials {
		operator.credentialIngress = append(operator.credentialIngress,
			newTokenBucket(operatorCredentialRate, operatorCredentialBurst))
	}
	return operator
}

// httpHandler returns the operator HTTP handler for tests and embedding.
func (operator *OperatorServer) httpHandler() http.Handler {
	return http.HandlerFunc(operator.serveHTTP)
}

// Serve exposes the operator overview listener until ctx is canceled.
func (operator *OperatorServer) Serve(ctx context.Context, listener net.Listener) error {
	httpServer := &http.Server{
		Handler:           http.HandlerFunc(operator.serveHTTP),
		ReadHeaderTimeout: operatorReadHeaderTimeout,
		ReadTimeout:       operatorReadTimeout,
		WriteTimeout:      operatorWriteTimeout,
		IdleTimeout:       operatorIdleTimeout,
		MaxHeaderBytes:    operatorMaxHeaderBytes,
	}
	operator.mu.Lock()
	operator.http = httpServer
	operator.mu.Unlock()
	shutdownDone := make(chan struct{})
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), operatorShutdownTimeout)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			_ = httpServer.Close()
		}
		close(shutdownDone)
	}()
	err := httpServer.Serve(netutil.LimitListener(listener, operatorMaxConnections))
	operator.mu.Lock()
	if operator.http == httpServer {
		operator.http = nil
	}
	operator.mu.Unlock()
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	if ctx.Err() != nil {
		<-shutdownDone
	}
	return nil
}

// Close terminates the listener immediately when the process hard-stop budget
// expires. It is safe before Serve and safe to call more than once.
func (operator *OperatorServer) Close() {
	operator.mu.Lock()
	httpServer := operator.http
	operator.mu.Unlock()
	if httpServer != nil {
		_ = httpServer.Close()
	}
}

// serveHTTP routes each request between the public credential-gate shell and
// the frozen operator API. Static paths (the shell and its hashed assets) are
// served without Authorization; every other path falls through to the
// bearer-first operator contract, which is unchanged: auth first (fail-closed,
// no path oracle), then the exact path, then GET only, then JSON content
// shape validation, then bounded admission, and finally the overview. Every rejection
// emits the frozen error schema with fixed literals; secrets, identifiers,
// paths, and raw errors are never echoed.
func (operator *OperatorServer) serveHTTP(writer http.ResponseWriter, request *http.Request) {
	if name, ok := staticRoute(request.URL.Path); ok {
		if !operator.acquireHandler() {
			setOperatorSecurityHeaders(writer.Header())
			setOperatorJSONHeaders(writer.Header())
			writer.Header().Set("Retry-After", "1")
			writeOperatorError(writer, request.Method == http.MethodHead, http.StatusServiceUnavailable, "OVERLOADED", true, operatorRetryAfterMs)
			return
		}
		defer operator.releaseHandler()
		operator.serveStatic(writer, request, name)
		return
	}
	setOperatorSecurityHeaders(writer.Header())
	setOperatorJSONHeaders(writer.Header())
	credentialIndex, authorized := operator.authorize(request)
	if !authorized {
		writer.Header().Set("WWW-Authenticate", "Bearer")
		writeOperatorError(writer, request.Method == http.MethodHead, http.StatusUnauthorized, "UNAUTHENTICATED", false, 0)
		return
	}
	if !operator.acquireHandler() {
		writer.Header().Set("Retry-After", "1")
		writeOperatorError(writer, request.Method == http.MethodHead, http.StatusServiceUnavailable, "OVERLOADED", true, operatorRetryAfterMs)
		return
	}
	defer operator.releaseHandler()
	if request.URL.Path != OperatorOverviewPath {
		writeOperatorError(writer, request.Method == http.MethodHead, http.StatusNotFound, "NOT_FOUND", false, 0)
		return
	}
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeOperatorError(writer, request.Method == http.MethodHead, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", false, 0)
		return
	}
	if request.URL.RawQuery != "" || request.ContentLength != 0 || len(request.TransferEncoding) != 0 {
		writeOperatorError(writer, false, http.StatusBadRequest, "INVALID_REQUEST", false, 0)
		return
	}
	if !operator.ingress.allow() || credentialIndex >= len(operator.credentialIngress) || !operator.credentialIngress[credentialIndex].allow() {
		writer.Header().Set("Retry-After", "1")
		writeOperatorError(writer, false, http.StatusTooManyRequests, "RATE_LIMITED", true, operatorRetryAfterMs)
		return
	}
	overview, err := operator.server.Overview()
	if err != nil {
		writeOperatorError(writer, false, http.StatusInternalServerError, "INTERNAL", false, 0)
		return
	}
	payload, err := json.MarshalIndent(overview, "", "  ")
	if err != nil || len(payload) > operatorMaxResponseBytes {
		writeOperatorError(writer, false, http.StatusInternalServerError, "INTERNAL", false, 0)
		return
	}
	writer.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(payload)
}

func (operator *OperatorServer) acquireHandler() bool {
	select {
	case operator.handlers <- struct{}{}:
		return true
	default:
		return false
	}
}

func (operator *OperatorServer) releaseHandler() {
	<-operator.handlers
}

// staticRoute maps one request path onto one embedded file, or reports
// whether the path is a static route at all. Every "/assets/" path is a
// static route even when the file is absent or the name is malformed, so all
// static rejections are the same fixed 404 and no lookup is ever forwarded
// outside the closed embedded tree.
func staticRoute(path string) (string, bool) {
	if path == "/" || path == "/index.html" {
		return stationoverview.IndexFile, true
	}
	assets := stationoverview.AssetFiles()
	switch path {
	case "/" + assets[0]:
		return assets[0], true
	case "/" + assets[1]:
		return assets[1], true
	}
	if strings.HasPrefix(path, staticAssetPrefix) {
		return "", true
	}
	return "", false
}

// serveStatic serves the public credential-gate shell and its embedded
// hashed assets. Every response carries the fixed security headers; the
// shell is never cached, while content-addressed assets are immutable.
// Only GET and HEAD are admitted, and only files inside the closed embedded
// tree can be served; anything else is a fixed 404 that never reflects the
// request path.
func (operator *OperatorServer) serveStatic(writer http.ResponseWriter, request *http.Request, name string) {
	setOperatorSecurityHeaders(writer.Header())
	if request.Method != http.MethodGet && request.Method != http.MethodHead {
		writer.Header().Set("Allow", "GET, HEAD")
		writeStaticError(writer, request.Method == http.MethodHead, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
		return
	}
	content, ok := stationoverview.File(name)
	if !ok {
		writeStaticError(writer, request.Method == http.MethodHead, http.StatusNotFound, "NOT_FOUND")
		return
	}
	writer.Header().Set("Content-Type", staticContentType(name))
	if name == stationoverview.IndexFile {
		writer.Header().Set("Cache-Control", staticIndexCacheControl)
	} else {
		writer.Header().Set("Cache-Control", staticAssetCacheControl)
	}
	writer.Header().Set("Content-Length", strconv.Itoa(len(content)))
	writer.WriteHeader(http.StatusOK)
	if request.Method == http.MethodGet {
		_, _ = writer.Write(content)
	}
}

// staticContentType maps one embedded file to its exact media type. The
// embedded build is closed (one HTML shell, hashed JS and CSS), so any other
// embedded file falls back to octet-stream.
func staticContentType(name string) string {
	assets := stationoverview.AssetFiles()
	switch {
	case name == stationoverview.IndexFile:
		return "text/html; charset=utf-8"
	case name == assets[0]:
		return "text/javascript; charset=utf-8"
	case name == assets[1]:
		return "text/css; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}

// writeStaticError writes the frozen operator error schema with one fixed
// literal reason; the request path is never echoed. HEAD requests receive
// status and headers only.
func writeStaticError(writer http.ResponseWriter, headOnly bool, status int, reason string) {
	setOperatorJSONHeaders(writer.Header())
	payload := operatorErrorPayload(reason)
	writer.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	writer.WriteHeader(status)
	if !headOnly {
		_, _ = writer.Write(payload)
	}
}

// authorize checks one request fail-closed: exactly one well-formed Bearer
// header whose token matches an active credential in constant time.
func (operator *OperatorServer) authorize(request *http.Request) (int, bool) {
	if !operator.enabled {
		return 0, false
	}
	values := request.Header.Values("Authorization")
	if len(values) != 1 {
		return 0, false
	}
	const prefix = "Bearer "
	value := values[0]
	if len(value) <= len(prefix) || value[:len(prefix)] != prefix {
		return 0, false
	}
	return operator.auth.authorize(value[len(prefix):])
}

// errorResponse is the frozen operator error schema. Reason is one fixed
// bounded token; retryable and retryAfterMs are fixed literals that never
// depend on request content. Nothing ever carries a secret, identifier,
// path, or raw error.
type errorResponse struct {
	Schema       string `json:"schema"`
	Reason       string `json:"reason"`
	Retryable    bool   `json:"retryable"`
	RetryAfterMs int    `json:"retryAfterMs"`
}

func writeOperatorError(writer http.ResponseWriter, headOnly bool, status int, reason string, retryable bool, retryAfterMs int) {
	payload := operatorErrorPayloadWithRetry(reason, retryable, retryAfterMs)
	writer.Header().Set("Content-Length", strconv.Itoa(len(payload)))
	writer.WriteHeader(status)
	if !headOnly {
		_, _ = writer.Write(payload)
	}
}

// operatorErrorPayload builds the fixed bounded error body for one literal
// reason; it never carries a secret, identifier, or request path.
func operatorErrorPayload(reason string) []byte {
	return operatorErrorPayloadWithRetry(reason, false, 0)
}

func operatorErrorPayloadWithRetry(reason string, retryable bool, retryAfterMs int) []byte {
	payload, _ := json.MarshalIndent(errorResponse{
		Schema:       OperatorErrorSchema,
		Reason:       reason,
		Retryable:    retryable,
		RetryAfterMs: retryAfterMs,
	}, "", "  ")
	return payload
}

func setOperatorSecurityHeaders(header http.Header) {
	header.Set("Content-Security-Policy", operatorStaticCSP)
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
}

func setOperatorJSONHeaders(header http.Header) {
	header.Set("Content-Type", operatorJSONContentType)
	header.Set("Cache-Control", "no-store")
}
