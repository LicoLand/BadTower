package nativeapi

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
	stationoverview "github.com/LicoLand/BadTower/web/station-overview"
)

// mutableClock is a fixed wall clock whose time a test can move.
type mutableClock struct {
	now time.Time
}

func (clock *mutableClock) NowUnixMs() int64            { return clock.now.UnixMilli() }
func (clock *mutableClock) MonotonicNow() time.Duration { return 0 }

// operatorFixture wires one core, one operator-enabled server, and one valid
// operator bearer token around a mutable clock.
type operatorFixture struct {
	server    *Server
	core      *stationcore.Core
	namespace string
	token     string
	digest    string
	clock     *mutableClock
}

func openOperatorFixture(t *testing.T, namespaceCount int) *operatorFixture {
	t.Helper()
	clock := &mutableClock{now: time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)}
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  t.TempDir(),
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, clock)
	if err != nil {
		t.Fatalf("Open core: %v", err)
	}
	t.Cleanup(func() { _ = core.Close() })

	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand: %v", err)
	}
	token, digest, err := GenerateToken(raw)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	now := time.Now().UTC()
	namespaces := make([]NamespaceConfig, 0, namespaceCount)
	for index := 0; index < namespaceCount; index++ {
		namespaceRaw := make([]byte, 32)
		if _, err := rand.Read(namespaceRaw); err != nil {
			t.Fatalf("namespace rand: %v", err)
		}
		_, namespaceDigest, err := GenerateToken(namespaceRaw)
		if err != nil {
			t.Fatalf("GenerateToken namespace: %v", err)
		}
		namespaces = append(namespaces, NamespaceConfig{
			ID:          fmt.Sprintf("ns_%022d", index),
			Credentials: []CredentialConfig{{SHA256: namespaceDigest, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour)}},
		})
	}
	config, err := ValidateConfig(Config{
		Schema:     ConfigSchema,
		Profile:    ProfileSingleNode,
		DataRoot:   t.TempDir(),
		Transport:  TransportConfig{Mode: "unix", SocketPath: "data/station-transport.sock"},
		Namespaces: namespaces,
		Operator: &OperatorConfig{
			Enabled:     true,
			Credentials: []CredentialConfig{{SHA256: digest, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour)}},
		},
	})
	if err != nil {
		t.Fatalf("ValidateConfig: %v", err)
	}
	server := NewServer(core, config, NewAuthenticator(config))
	server.SetLifecycleRunning()
	return &operatorFixture{
		server:    server,
		core:      core,
		namespace: config.Namespaces[0].ID,
		token:     token,
		digest:    digest,
		clock:     clock,
	}
}

func (fixture *operatorFixture) get(t *testing.T, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set("Authorization", "Bearer "+fixture.token)
	recorder := httptest.NewRecorder()
	fixture.server.operator.httpHandler().ServeHTTP(recorder, request)
	return recorder
}

// request issues one method/path probe without any Authorization header.
func (fixture *operatorFixture) request(t *testing.T, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, path, nil)
	recorder := httptest.NewRecorder()
	fixture.server.operator.httpHandler().ServeHTTP(recorder, request)
	return recorder
}

// assertStaticSecurityHeaders checks the fixed security headers every static
// response must carry.
func assertStaticSecurityHeaders(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if got := recorder.Header().Get("Content-Security-Policy"); got != operatorStaticCSP {
		t.Fatalf("Content-Security-Policy = %q, want %q", got, operatorStaticCSP)
	}
	if got := recorder.Header().Get("Referrer-Policy"); got != "no-referrer" {
		t.Fatalf("Referrer-Policy = %q, want no-referrer", got)
	}
	if got := recorder.Header().Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}
	for _, directive := range []string{"script-src 'self'", "style-src 'self'", "connect-src 'self'", "frame-ancestors 'none'"} {
		if !strings.Contains(operatorStaticCSP, directive) {
			t.Fatalf("CSP missing required directive %q", directive)
		}
	}
}

// discoverEmbeddedAssets returns the "/assets/<name>" paths the embedded
// shell references, discovered from the shell itself so assertions stay
// valid across rebuilds with new content hashes.
func discoverEmbeddedAssets(t *testing.T) []string {
	t.Helper()
	index, ok := stationoverview.File(stationoverview.IndexFile)
	if !ok {
		t.Fatalf("embedded index missing")
	}
	pattern := regexp.MustCompile(`\./assets/[A-Za-z0-9._-]+\.(js|css)`)
	matches := pattern.FindAll(index, -1)
	if len(matches) == 0 {
		t.Fatalf("embedded index references no hashed assets")
	}
	assets := make([]string, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		path := "/" + strings.TrimPrefix(string(match), "./")
		if !seen[path] {
			seen[path] = true
			assets = append(assets, path)
		}
	}
	return assets
}

// assertNoCredentialLeakage verifies one response body never echoes the
// fixture bearer token, digest, or namespace identity.
func assertNoCredentialLeakage(t *testing.T, fixture *operatorFixture, label, body string) {
	t.Helper()
	for _, secret := range []string{fixture.token, fixture.digest, fixture.namespace} {
		if strings.Contains(body, secret) {
			t.Fatalf("%s must not echo %q", label, secret)
		}
	}
}

// assertOperatorError checks the frozen error schema, status, and headers.
// Every rejection is a fixed literal: reason is the only varying token and
// retryable/retryAfterMs never depend on request content.
func assertOperatorError(t *testing.T, recorder *httptest.ResponseRecorder, status int, reason string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d (body=%s)", recorder.Code, status, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != operatorJSONContentType {
		t.Fatalf("Content-Type = %q, want %q", got, operatorJSONContentType)
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", got)
	}
	assertStaticSecurityHeaders(t, recorder)
	want := fmt.Sprintf(`{
  "schema": "badtower.station-overview.error.v1",
  "reason": %q,
  "retryable": false,
  "retryAfterMs": 0
}`, reason)
	if got := recorder.Body.String(); got != want {
		t.Fatalf("error body mismatch:\n got: %s\nwant: %s", got, want)
	}
	if got := recorder.Header().Get("Content-Length"); got != strconv.Itoa(recorder.Body.Len()) {
		t.Fatalf("Content-Length = %q, want %d", got, recorder.Body.Len())
	}
}

func assertHeadOperatorError(t *testing.T, recorder *httptest.ResponseRecorder, status int, reason string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d", recorder.Code, status)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("HEAD error body = %d bytes, want empty", recorder.Body.Len())
	}
	if got := recorder.Header().Get("Content-Length"); got != strconv.Itoa(len(operatorErrorPayload(reason))) {
		t.Fatalf("Content-Length = %q, want frozen error length", got)
	}
	assertStaticSecurityHeaders(t, recorder)
}

func assertRetryableOperatorError(t *testing.T, recorder *httptest.ResponseRecorder, status int, reason string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d", recorder.Code, status)
	}
	want := string(operatorErrorPayloadWithRetry(reason, true, operatorRetryAfterMs))
	if got := recorder.Body.String(); got != want {
		t.Fatalf("retryable error body mismatch: got %s", got)
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	if got := recorder.Header().Get("Content-Length"); got != strconv.Itoa(len(want)) {
		t.Fatalf("Content-Length = %q, want %d", got, len(want))
	}
	assertStaticSecurityHeaders(t, recorder)
}

// decodeStrict decodes one overview body with unknown fields rejected, so any
// key outside the frozen schema is an error.
func decodeStrict(body string) (OverviewResponse, error) {
	var overview OverviewResponse
	decoder := json.NewDecoder(strings.NewReader(body))
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&overview)
	return overview, err
}

func decodeOverview(t *testing.T, body string) OverviewResponse {
	t.Helper()
	overview, err := decodeStrict(body)
	if err != nil {
		t.Fatalf("decode overview: %v (body=%s)", err, body)
	}
	return overview
}

func decodeOverviewErr(t *testing.T, body string) error {
	t.Helper()
	_, err := decodeStrict(body)
	return err
}

func cloneMap(root map[string]any) map[string]any {
	out := make(map[string]any, len(root))
	for key, value := range root {
		if nested, ok := value.(map[string]any); ok {
			out[key] = cloneMap(nested)
		} else {
			out[key] = value
		}
	}
	return out
}

func marshalMap(t *testing.T, node map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(node)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(payload)
}

func mapKeys(node map[string]any) []string {
	keys := make([]string, 0, len(node))
	for key := range node {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// assertExactKeys locks one JSON object to exactly the given key set.
func assertExactKeys(t *testing.T, node map[string]any, want []string, path string) {
	t.Helper()
	if len(node) != len(want) {
		t.Fatalf("%s key count = %d, want %d (keys=%v)", path, len(node), len(want), mapKeys(node))
	}
	for _, key := range want {
		if _, ok := node[key]; !ok {
			t.Fatalf("%s missing key %q (keys=%v)", path, key, mapKeys(node))
		}
	}
}

func TestOperatorConfigValidation(t *testing.T) {
	t.Parallel()
	digest := strings.Repeat("A", 43)
	now := time.Now().UTC()
	activeCredential := CredentialConfig{SHA256: digest, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
	namespaceCredential := CredentialConfig{SHA256: strings.Repeat("D", 43), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
	base := Config{
		Schema:    ConfigSchema,
		Profile:   ProfileSingleNode,
		Transport: TransportConfig{Mode: "unix", SocketPath: "data/station-transport.sock"},
		Namespaces: []NamespaceConfig{{
			ID:          "ns_" + strings.Repeat("B", 22),
			Credentials: []CredentialConfig{namespaceCredential},
		}},
	}
	withOperator := func(operator *OperatorConfig) Config {
		config := base
		config.Operator = operator
		return config
	}

	t.Run("absent block is inert", func(t *testing.T) {
		config, err := ValidateConfig(base)
		if err != nil {
			t.Fatalf("ValidateConfig: %v", err)
		}
		if config.Operator != nil {
			t.Fatalf("operator must remain nil when absent")
		}
	})
	t.Run("disabled block must carry no fields", func(t *testing.T) {
		for name, operator := range map[string]*OperatorConfig{
			"listen":      {Enabled: false, Listen: "127.0.0.1:7446"},
			"credentials": {Enabled: false, Credentials: []CredentialConfig{activeCredential}},
		} {
			t.Run(name, func(t *testing.T) {
				_, err := ValidateConfig(withOperator(operator))
				if err == nil || !strings.Contains(err.Error(), "disabled operator block must carry no fields") {
					t.Fatalf("error = %v, want disabled-block rejection", err)
				}
			})
		}
	})
	t.Run("enabled block defaults and validates", func(t *testing.T) {
		config, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: []CredentialConfig{activeCredential}}))
		if err != nil {
			t.Fatalf("ValidateConfig: %v", err)
		}
		if config.Operator.Listen != DefaultOperatorListen {
			t.Fatalf("operator listen = %q, want default %q", config.Operator.Listen, DefaultOperatorListen)
		}
	})
	t.Run("enabled non-loopback listen is rejected", func(t *testing.T) {
		operator := &OperatorConfig{Enabled: true, Listen: "0.0.0.0:7446", Credentials: []CredentialConfig{activeCredential}}
		_, err := ValidateConfig(withOperator(operator))
		if err == nil || !strings.Contains(err.Error(), "numeric loopback") {
			t.Fatalf("error = %v, want non-loopback rejection", err)
		}
	})
	t.Run("enabled hostname listen is rejected", func(t *testing.T) {
		operator := &OperatorConfig{Enabled: true, Listen: "localhost:7446", Credentials: []CredentialConfig{activeCredential}}
		_, err := ValidateConfig(withOperator(operator))
		if err == nil || !strings.Contains(err.Error(), "numeric loopback") {
			t.Fatalf("error = %v, want hostname rejection", err)
		}
	})
	t.Run("enabled ephemeral port is rejected", func(t *testing.T) {
		operator := &OperatorConfig{Enabled: true, Listen: "127.0.0.1:0", Credentials: []CredentialConfig{activeCredential}}
		_, err := ValidateConfig(withOperator(operator))
		if err == nil || !strings.Contains(err.Error(), "nonzero") {
			t.Fatalf("error = %v, want zero-port rejection", err)
		}
	})
	t.Run("credential count is bounded", func(t *testing.T) {
		for _, count := range []int{0, 3} {
			credentials := make([]CredentialConfig, 0, count)
			for index := 0; index < count; index++ {
				credentials = append(credentials, activeCredential)
			}
			_, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: credentials}))
			if err == nil || !strings.Contains(err.Error(), "1-2 records") {
				t.Fatalf("count=%d error = %v, want 1-2 records", count, err)
			}
		}
	})
	t.Run("credential digest is strict", func(t *testing.T) {
		credential := CredentialConfig{SHA256: strings.Repeat("A", 42), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
		_, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: []CredentialConfig{credential}}))
		if err == nil || !strings.Contains(err.Error(), "credential sha256 is invalid") {
			t.Fatalf("error = %v, want invalid digest rejection", err)
		}
	})
	t.Run("duplicate digest is rejected", func(t *testing.T) {
		_, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: []CredentialConfig{activeCredential, activeCredential}}))
		if err == nil || !strings.Contains(err.Error(), "duplicate operator credential digest") {
			t.Fatalf("error = %v, want duplicate digest rejection", err)
		}
	})
	t.Run("reversed lifetime is rejected", func(t *testing.T) {
		credential := CredentialConfig{SHA256: digest, NotBefore: now.Add(time.Hour), NotAfter: now.Add(-time.Hour)}
		_, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: []CredentialConfig{credential}}))
		if err == nil || !strings.Contains(err.Error(), "credential lifetime is invalid") {
			t.Fatalf("error = %v, want reversed lifetime rejection", err)
		}
	})
	t.Run("lifetime over 90 days is rejected", func(t *testing.T) {
		credential := CredentialConfig{SHA256: digest, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(91 * 24 * time.Hour)}
		_, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: []CredentialConfig{credential}}))
		if err == nil || !strings.Contains(err.Error(), "credential lifetime exceeds 90 days") {
			t.Fatalf("error = %v, want lifetime rejection", err)
		}
	})
	t.Run("no active credential is rejected", func(t *testing.T) {
		expired := CredentialConfig{SHA256: digest, NotBefore: now.Add(-2 * time.Hour), NotAfter: now.Add(-time.Hour)}
		_, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: []CredentialConfig{expired}}))
		if err == nil || !strings.Contains(err.Error(), "operator has no active credential") {
			t.Fatalf("error = %v, want no-active-credential rejection", err)
		}
	})
	t.Run("two active credentials validate", func(t *testing.T) {
		second := CredentialConfig{SHA256: strings.Repeat("C", 43), NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour)}
		config, err := ValidateConfig(withOperator(&OperatorConfig{Enabled: true, Credentials: []CredentialConfig{activeCredential, second}}))
		if err != nil {
			t.Fatalf("ValidateConfig: %v", err)
		}
		if len(config.Operator.Credentials) != 2 {
			t.Fatalf("credentials = %d, want 2", len(config.Operator.Credentials))
		}
	})
	t.Run("operator and namespace credential domains are disjoint", func(t *testing.T) {
		config := base
		config.Namespaces = append([]NamespaceConfig(nil), base.Namespaces...)
		config.Namespaces[0].Credentials = []CredentialConfig{activeCredential}
		config.Operator = &OperatorConfig{Enabled: true, Credentials: []CredentialConfig{activeCredential}}
		_, err := ValidateConfig(config)
		if err == nil || !strings.Contains(err.Error(), "credential domains must be disjoint") {
			t.Fatalf("error = %v, want credential-domain rejection", err)
		}
	})
}

func TestOperatorOverviewFrozenGolden(t *testing.T) {
	t.Parallel()
	for _, count := range []int{1, 2} {
		t.Run(fmt.Sprintf("namespaces=%d", count), func(t *testing.T) {
			fixture := openOperatorFixture(t, count)
			recorder := fixture.get(t, OperatorOverviewPath)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
			}
			if got := recorder.Header().Get("Content-Type"); got != operatorJSONContentType {
				t.Fatalf("Content-Type = %q, want %q", got, operatorJSONContentType)
			}
			if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}
			assertStaticSecurityHeaders(t, recorder)
			if recorder.Body.Len() > operatorMaxResponseBytes {
				t.Fatalf("overview response = %d bytes, limit %d", recorder.Body.Len(), operatorMaxResponseBytes)
			}
			if got := recorder.Header().Get("Content-Length"); got != strconv.Itoa(recorder.Body.Len()) {
				t.Fatalf("Content-Length = %q, want %d", got, recorder.Body.Len())
			}
			want := fmt.Sprintf(`{
  "schema": "badtower.station-overview.v1",
  "lifecycle": "RUNNING",
  "readiness": {
    "status": "SERVING",
    "reason": "NONE"
  },
  "pressure": "NORMAL",
  "capacity": {
    "configuredNamespaceCount": %d,
    "implementationCeiling": {
      "namespaceCount": 1000,
      "perNamespace": {
        "addresses": 10000,
        "transportUnits": 100000,
        "logicalRetainedBytes": 4294967296,
        "activeClaims": 10000
      }
    },
    "aggregateUsage": {
      "addresses": 0,
      "transportUnits": 0,
      "logicalRetainedBytes": 0,
      "activeClaims": 0
    }
  },
  "pollAfterMs": 1000
}`, count)
			if got := recorder.Body.String(); got != want {
				t.Fatalf("overview body mismatch:\n got: %s\nwant: %s", got, want)
			}
			// Privacy: no bearer secret, digest, or namespace identity may
			// appear anywhere in the frozen body.
			body := recorder.Body.String()
			for _, secret := range []string{fixture.token, fixture.digest, fixture.namespace} {
				if strings.Contains(body, secret) {
					t.Fatalf("overview body must not echo %q", secret)
				}
			}
			// The body decodes strictly into the frozen struct with no extra keys.
			_ = decodeOverview(t, body)
		})
	}
}

func TestOperatorOverviewExactKeys(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	recorder := fixture.get(t, OperatorOverviewPath)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var root map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	assertExactKeys(t, root, []string{"schema", "lifecycle", "readiness", "pressure", "capacity", "pollAfterMs"}, "root")
	readiness := root["readiness"].(map[string]any)
	assertExactKeys(t, readiness, []string{"status", "reason"}, "readiness")
	capacity := root["capacity"].(map[string]any)
	assertExactKeys(t, capacity, []string{"configuredNamespaceCount", "implementationCeiling", "aggregateUsage"}, "capacity")
	ceiling := capacity["implementationCeiling"].(map[string]any)
	assertExactKeys(t, ceiling, []string{"namespaceCount", "perNamespace"}, "capacity.implementationCeiling")
	perNamespace := ceiling["perNamespace"].(map[string]any)
	assertExactKeys(t, perNamespace, []string{"addresses", "transportUnits", "logicalRetainedBytes", "activeClaims"}, "capacity.implementationCeiling.perNamespace")
	usage := capacity["aggregateUsage"].(map[string]any)
	assertExactKeys(t, usage, []string{"addresses", "transportUnits", "logicalRetainedBytes", "activeClaims"}, "capacity.aggregateUsage")
}

func TestOperatorRejectsRemovedLegacyFields(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	recorder := fixture.get(t, OperatorOverviewPath)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
	}
	var root map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &root); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, key := range []string{"profile", "namespaces", "usage", "limits", "ready", "error", "code", "message"} {
		payload := cloneMap(root)
		payload[key] = true
		if err := decodeOverviewErr(t, marshalMap(t, payload)); err == nil {
			t.Fatalf("removed root field %q must be rejected", key)
		}
	}
	legacyNested := []struct{ parent, key string }{
		{"readiness", "ready"},
		{"namespaces", "configured"},
		{"namespaces", "maximum"},
		{"usage", "addresses"},
		{"usage", "units"},
		{"usage", "retainedBytes"},
		{"usage", "activeClaims"},
		{"usage", "idempotencyRecords"},
		{"limits", "maxNamespaces"},
		{"limits", "maxAddressesPerNamespace"},
		{"limits", "maxUnitsPerNamespace"},
		{"limits", "maxRetainedBytesPerNamespace"},
		{"limits", "maxActiveClaimsPerNamespace"},
		{"limits", "maxMutationQueueEntriesPerNamespace"},
		{"limits", "maxLongPollClaimsPerNamespace"},
		{"limits", "mutationLanes"},
		{"limits", "drrQuantumBytes"},
		{"limits", "drrChargeGranularity"},
		{"limits", "reservedControlQueueBytes"},
	}
	for _, legacy := range legacyNested {
		payload := cloneMap(root)
		parent, ok := payload[legacy.parent].(map[string]any)
		if !ok {
			parent = map[string]any{}
			payload[legacy.parent] = parent
		}
		parent[legacy.key] = true
		if err := decodeOverviewErr(t, marshalMap(t, payload)); err == nil {
			t.Fatalf("removed field %q under %q must be rejected", legacy.key, legacy.parent)
		}
	}
}

func TestOperatorOverviewAggregateFromObservation(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	reserved, err := fixture.server.Reserve(ctx, fixture.namespace, ReserveRequest{RequestID: "rq_opOverviewRes0001", TTLMs: 86_400_000})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := fixture.server.Submit(ctx, fixture.namespace, SubmitRequest{
		RequestID: "rq_opOverviewSub0001",
		AddressID: reserved.AddressID,
		Payload:   []byte("overview"),
		TTLMs:     86_400_000,
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	recorder := fixture.get(t, OperatorOverviewPath)
	overview := decodeOverview(t, recorder.Body.String())
	usage := overview.Capacity.AggregateUsage
	if usage.Addresses != 1 || usage.TransportUnits != 1 || usage.LogicalRetainedBytes == 0 || usage.ActiveClaims != 0 {
		t.Fatalf("aggregate usage after mutations = %+v, want one address and one unit", usage)
	}
	// The overview usage is exactly the O(1) core observation aggregate.
	observation := fixture.core.ObservationSnapshot()
	if usage != capacityUsage(observation.Aggregate) {
		t.Fatalf("overview aggregate usage %+v != observation aggregate %+v", usage, observation.Aggregate)
	}
	if overview.Pressure != "NORMAL" {
		t.Fatalf("pressure = %q, want NORMAL", overview.Pressure)
	}
	if overview.Capacity.ConfiguredNamespaceCount != 1 {
		t.Fatalf("configuredNamespaceCount = %d, want 1", overview.Capacity.ConfiguredNamespaceCount)
	}
	if overview.Lifecycle != string(stationcore.StationLifecycleRunning) ||
		overview.Readiness != (ReadinessOverview{Status: ReadinessServing, Reason: ReadinessReasonNone}) {
		t.Fatalf("lifecycle/readiness = %q %+v, want RUNNING SERVING/NONE", overview.Lifecycle, overview.Readiness)
	}
}

func TestOperatorAuthFailClosed(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	requests := []struct {
		name  string
		setup func(request *http.Request)
	}{
		{"missing header", func(request *http.Request) {}},
		{"empty header", func(request *http.Request) { request.Header.Set("Authorization", "") }},
		{"basic scheme", func(request *http.Request) { request.Header.Set("Authorization", "Basic "+fixture.token) }},
		{"lowercase scheme", func(request *http.Request) { request.Header.Set("Authorization", "bearer "+fixture.token) }},
		{"missing space", func(request *http.Request) { request.Header.Set("Authorization", "Bearer") }},
		{"unknown token", func(request *http.Request) { request.Header.Set("Authorization", "Bearer "+strings.Repeat("x", 43)) }},
		{"trailing garbage", func(request *http.Request) { request.Header.Set("Authorization", "Bearer "+fixture.token+" extra") }},
		{"duplicate headers", func(request *http.Request) {
			request.Header.Add("Authorization", "Bearer "+fixture.token)
			request.Header.Add("Authorization", "Bearer "+fixture.token)
		}},
	}
	var firstBody string
	for index, test := range requests {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, OperatorOverviewPath, nil)
			test.setup(request)
			recorder := httptest.NewRecorder()
			fixture.server.operator.httpHandler().ServeHTTP(recorder, request)
			assertOperatorError(t, recorder, http.StatusUnauthorized, "UNAUTHENTICATED")
			if got := recorder.Header().Get("WWW-Authenticate"); got != "Bearer" {
				t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
			}
			body := recorder.Body.String()
			if strings.Contains(body, fixture.token) {
				t.Fatalf("401 body must never echo the presented token")
			}
			if index == 0 {
				firstBody = body
			} else if body != firstBody {
				t.Fatalf("401 bodies must be byte-identical across failure modes")
			}
		})
	}
}

func TestOperatorAuthMatchesAnyActiveCredential(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		t.Fatalf("rand: %v", err)
	}
	secondToken, secondDigest, err := GenerateToken(raw)
	if err != nil {
		t.Fatalf("GenerateToken: %v", err)
	}
	now := time.Now().UTC()
	config := fixture.server.config
	config.Operator.Credentials = append(config.Operator.Credentials, CredentialConfig{
		SHA256:    secondDigest,
		NotBefore: now.Add(-time.Hour),
		NotAfter:  now.Add(time.Hour),
	})
	server := NewServer(fixture.core, config, fixture.server.auth)
	server.SetLifecycleRunning()
	handler := server.operator.httpHandler()
	for _, token := range []string{fixture.token, secondToken} {
		request := httptest.NewRequest(http.MethodGet, OperatorOverviewPath, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("active credential status = %d, want 200", recorder.Code)
		}
	}
}

func TestOperatorAuthWindowRecheckedFailClosed(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	credential := fixture.server.operator.auth.credentials[0]
	fixture.server.operator.auth.now = func() time.Time {
		return credential.notAfter.Add(time.Nanosecond)
	}
	recorder := fixture.get(t, OperatorOverviewPath)
	assertOperatorError(t, recorder, http.StatusUnauthorized, "UNAUTHENTICATED")
}

func TestOperatorRateAndConcurrencyBounds(t *testing.T) {
	t.Parallel()

	t.Run("credential burst", func(t *testing.T) {
		fixture := openOperatorFixture(t, 1)
		for attempt := 0; attempt < int(operatorCredentialBurst); attempt++ {
			if recorder := fixture.get(t, OperatorOverviewPath); recorder.Code != http.StatusOK {
				t.Fatalf("burst request %d status = %d, want 200", attempt, recorder.Code)
			}
		}
		recorder := fixture.get(t, OperatorOverviewPath)
		assertRetryableOperatorError(t, recorder, http.StatusTooManyRequests, "RATE_LIMITED")
	})

	t.Run("handler saturation", func(t *testing.T) {
		fixture := openOperatorFixture(t, 1)
		for index := 0; index < operatorMaxHandlers; index++ {
			fixture.server.operator.handlers <- struct{}{}
		}
		defer func() {
			for index := 0; index < operatorMaxHandlers; index++ {
				<-fixture.server.operator.handlers
			}
		}()
		recorder := fixture.get(t, OperatorOverviewPath)
		assertRetryableOperatorError(t, recorder, http.StatusServiceUnavailable, "OVERLOADED")
	})

	if operatorMaxConnections != 16 || operatorMaxHandlers != 4 ||
		operatorMaxHeaderBytes != 8<<10 || operatorMaxResponseBytes != 4<<10 ||
		operatorGlobalRate != 8 || operatorGlobalBurst != 16 ||
		operatorCredentialRate != 1 || operatorCredentialBurst != 2 {
		t.Fatal("operator resource bounds changed")
	}
}

func TestOperatorMethodPathRejection(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
		t.Run("method "+method, func(t *testing.T) {
			request := httptest.NewRequest(method, OperatorOverviewPath, nil)
			request.Header.Set("Authorization", "Bearer "+fixture.token)
			recorder := httptest.NewRecorder()
			fixture.server.operator.httpHandler().ServeHTTP(recorder, request)
			assertOperatorError(t, recorder, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
			if got := recorder.Header().Get("Allow"); got != http.MethodGet {
				t.Fatalf("Allow = %q, want GET", got)
			}
		})
	}
	head := httptest.NewRequest(http.MethodHead, OperatorOverviewPath, nil)
	head.Header.Set("Authorization", "Bearer "+fixture.token)
	headRecorder := httptest.NewRecorder()
	fixture.server.operator.httpHandler().ServeHTTP(headRecorder, head)
	assertHeadOperatorError(t, headRecorder, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
	if got := headRecorder.Header().Get("Allow"); got != http.MethodGet {
		t.Fatalf("HEAD Allow = %q, want GET", got)
	}
	for _, path := range []string{"/operator/v1", "/operator/v1/overview/", "/operator/v1/overview/extra", "/operator", "/other"} {
		t.Run("path "+path, func(t *testing.T) {
			recorder := fixture.get(t, path)
			assertOperatorError(t, recorder, http.StatusNotFound, "NOT_FOUND")
			if strings.Contains(recorder.Body.String(), path) {
				t.Fatalf("404 body must never echo the requested path")
			}
		})
	}
	// Auth runs before routing: unauthenticated probes of any path see only 401.
	request := httptest.NewRequest(http.MethodGet, "/operator/v1/overview/extra", nil)
	recorder := httptest.NewRecorder()
	fixture.server.operator.httpHandler().ServeHTTP(recorder, request)
	assertOperatorError(t, recorder, http.StatusUnauthorized, "UNAUTHENTICATED")
}

func TestOperatorAcceptHeaderDoesNotVarySchema(t *testing.T) {
	t.Parallel()
	for _, accept := range []string{
		"",
		"text/html",
		"application/xml",
		"application/json;q=0",
		"*/*;q=0",
		"application/json-patch+json",
		"text/html, application/xml",
		"application/json",
		"*/*",
		"APPLICATION/JSON",
		"application/*",
		"application/json;q=0.5",
		"text/html, application/json;q=0.5",
	} {
		t.Run("accept "+accept, func(t *testing.T) {
			fixture := openOperatorFixture(t, 1)
			request := httptest.NewRequest(http.MethodGet, OperatorOverviewPath, nil)
			request.Header.Set("Authorization", "Bearer "+fixture.token)
			request.Header.Set("Accept", accept)
			recorder := httptest.NewRecorder()
			fixture.server.operator.httpHandler().ServeHTTP(recorder, request)
			if recorder.Code != http.StatusOK {
				t.Fatalf("Accept %q status = %d, want 200 (body=%s)", accept, recorder.Code, recorder.Body.String())
			}
			if got := recorder.Header().Get("Content-Type"); got != operatorJSONContentType {
				t.Fatalf("Accept %q Content-Type = %q, want fixed JSON", accept, got)
			}
		})
	}
}

func TestOperatorInvalidRequestRejected(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		build func() *http.Request
	}{
		{"body", func() *http.Request {
			return httptest.NewRequest(http.MethodGet, OperatorOverviewPath, strings.NewReader("not empty"))
		}},
		{"query", func() *http.Request {
			return httptest.NewRequest(http.MethodGet, OperatorOverviewPath+"?detail=true", nil)
		}},
		{"transfer encoding", func() *http.Request {
			request := httptest.NewRequest(http.MethodGet, OperatorOverviewPath, nil)
			request.TransferEncoding = []string{"chunked"}
			return request
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := openOperatorFixture(t, 1)
			request := test.build()
			request.Header.Set("Authorization", "Bearer "+fixture.token)
			recorder := httptest.NewRecorder()
			fixture.server.operator.httpHandler().ServeHTTP(recorder, request)
			assertOperatorError(t, recorder, http.StatusBadRequest, "INVALID_REQUEST")
		})
	}
}

func TestOperatorServeListenerAndCancel(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- fixture.server.ServeOperator(ctx, listener) }()

	baseURL := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: 2 * time.Second}
	var served bool
	for attempt := 0; attempt < 20 && !served; attempt++ {
		request, err := http.NewRequest(http.MethodGet, baseURL+OperatorOverviewPath, nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		request.Header.Set("Authorization", "Bearer "+fixture.token)
		response, err := client.Do(request)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200 (body=%s)", response.StatusCode, body)
		}
		if !strings.Contains(string(body), `"schema": "badtower.station-overview.v1"`) {
			t.Fatalf("unexpected body: %s", body)
		}
		served = true
	}
	if !served {
		t.Fatalf("operator listener never became ready")
	}
	fixture.server.operator.mu.Lock()
	httpServer := fixture.server.operator.http
	fixture.server.operator.mu.Unlock()
	if httpServer == nil ||
		httpServer.ReadHeaderTimeout != operatorReadHeaderTimeout ||
		httpServer.ReadTimeout != operatorReadTimeout ||
		httpServer.WriteTimeout != operatorWriteTimeout ||
		httpServer.IdleTimeout != operatorIdleTimeout ||
		httpServer.MaxHeaderBytes != operatorMaxHeaderBytes {
		t.Fatal("operator HTTP server bounds changed")
	}

	response, err := client.Get(baseURL + OperatorOverviewPath)
	if err != nil {
		t.Fatalf("unauthenticated GET: %v", err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d, want 401 (body=%s)", response.StatusCode, body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ServeOperator: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("ServeOperator did not return after cancel")
	}
}

func TestOperatorServeDisabledFailsFast(t *testing.T) {
	t.Parallel()
	server, _ := openTestServer(t)
	err := server.ServeOperator(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("ServeOperator error = %v, want disabled rejection", err)
	}
}

func TestOperatorReadinessPairingsExact(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		ready     bool
		reason    stationcore.Reason
		lifecycle stationcore.StationLifecycle
		want      ReadinessOverview
	}{
		{"serving", true, "", stationcore.StationLifecycleRunning,
			ReadinessOverview{Status: ReadinessServing, Reason: ReadinessReasonNone}},
		{"clock unsafe", false, stationcore.ReasonClockUnsafe, stationcore.StationLifecycleRunning,
			ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonClockUnsafe}},
		{"starting", false, stationcore.ReasonDraining, stationcore.StationLifecycleStarting,
			ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonStarting}},
		{"draining", false, stationcore.ReasonDraining, stationcore.StationLifecycleDraining,
			ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonDraining}},
		{"stopped", false, stationcore.ReasonDraining, stationcore.StationLifecycleStopped,
			ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonStopped}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := readinessOverview(test.ready, test.reason, test.lifecycle); got != test.want {
				t.Fatalf("readinessOverview(%v, %q, %q) = %+v, want %+v", test.ready, test.reason, test.lifecycle, got, test.want)
			}
		})
	}
}

func TestOperatorReadinessClosedInvariant(t *testing.T) {
	t.Parallel()
	reasons := []stationcore.Reason{"", stationcore.ReasonClockUnsafe, stationcore.ReasonDraining}
	lifecycles := []stationcore.StationLifecycle{
		stationcore.StationLifecycleStarting,
		stationcore.StationLifecycleRunning,
		stationcore.StationLifecycleDraining,
		stationcore.StationLifecycleStopped,
	}
	statuses := map[string]bool{ReadinessServing: true, ReadinessNotServing: true}
	reasonTokens := map[string]bool{
		ReadinessReasonNone:        true,
		ReadinessReasonStarting:    true,
		ReadinessReasonClockUnsafe: true,
		ReadinessReasonDraining:    true,
		ReadinessReasonStopped:     true,
	}
	for _, ready := range []bool{false, true} {
		for _, reason := range reasons {
			for _, lifecycle := range lifecycles {
				overview := readinessOverview(ready, reason, lifecycle)
				if !statuses[overview.Status] {
					t.Fatalf("status %q outside closed domain (ready=%v reason=%q lifecycle=%q)", overview.Status, ready, reason, lifecycle)
				}
				if !reasonTokens[overview.Reason] {
					t.Fatalf("reason %q outside closed domain (ready=%v reason=%q lifecycle=%q)", overview.Reason, ready, reason, lifecycle)
				}
				serving := overview.Status == ReadinessServing
				if serving != (overview.Reason == ReadinessReasonNone) {
					t.Fatalf("pairing (%v, %q) from (%v, %q, %q) violates SERVING iff NONE", overview.Status, overview.Reason, ready, reason, lifecycle)
				}
			}
		}
	}
}

func TestOperatorStartingPairingExact(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	observation := stationcore.StationObservation{
		Lifecycle: stationcore.StationLifecycleStarting,
		Pressure:  stationcore.PressureNormal,
	}
	overview := overviewResponse(observation, false, stationcore.ReasonDraining, fixture.core.ProfileLimits(), 1)
	if overview.Schema != OperatorOverviewSchema {
		t.Fatalf("schema = %q, want %q", overview.Schema, OperatorOverviewSchema)
	}
	if overview.Lifecycle != string(stationcore.StationLifecycleStarting) {
		t.Fatalf("lifecycle = %q, want STARTING", overview.Lifecycle)
	}
	if overview.Readiness != (ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonStarting}) {
		t.Fatalf("readiness = %+v, want NOT_SERVING/STARTING", overview.Readiness)
	}
	if overview.PollAfterMs != OperatorPollAfterMs {
		t.Fatalf("pollAfterMs = %d, want %d", overview.PollAfterMs, OperatorPollAfterMs)
	}
	if overview.Capacity.AggregateUsage != (CapacityUsage{}) {
		t.Fatalf("aggregate usage = %+v, want zero usage at start", overview.Capacity.AggregateUsage)
	}
}

func TestOperatorDrainPairingExact(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)

	recorder := fixture.get(t, OperatorOverviewPath)
	overview := decodeOverview(t, recorder.Body.String())
	if overview.Lifecycle != "RUNNING" || overview.Readiness != (ReadinessOverview{Status: ReadinessServing, Reason: ReadinessReasonNone}) {
		t.Fatalf("before drain: %+v", overview)
	}

	fixture.server.BeginDrain()
	fixture.server.BeginDrain() // idempotent

	// Server drain must invoke the core drain.
	observation := fixture.core.ObservationSnapshot()
	if observation.Lifecycle != stationcore.StationLifecycleDraining {
		t.Fatalf("core lifecycle after server.BeginDrain = %q, want DRAINING", observation.Lifecycle)
	}
	ready, reason := fixture.core.Ready()
	if ready || reason != stationcore.ReasonDraining {
		t.Fatalf("core.Ready after drain = (%v, %q), want (false, DRAINING)", ready, reason)
	}

	recorder = fixture.get(t, OperatorOverviewPath)
	overview = decodeOverview(t, recorder.Body.String())
	if overview.Lifecycle != "DRAINING" {
		t.Fatalf("overview lifecycle = %q, want DRAINING", overview.Lifecycle)
	}
	wantReadiness := readinessOverview(ready, reason, observation.Lifecycle)
	if overview.Readiness != wantReadiness ||
		overview.Readiness != (ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonDraining}) {
		t.Fatalf("overview readiness = %+v, want exact core pairing NOT_SERVING/DRAINING", overview.Readiness)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := fixture.server.Reserve(ctx, fixture.namespace, ReserveRequest{RequestID: "rq_opDrainReserve001", TTLMs: 60_000})
	if err == nil || !strings.Contains(err.Error(), "DRAINING") {
		t.Fatalf("Reserve after drain error = %v, want DRAINING", err)
	}
}

func TestOperatorStoppedPairingExact(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	fixture.server.MarkStopped()
	if err := fixture.core.Close(); err != nil {
		t.Fatalf("Close core: %v", err)
	}

	observation := fixture.core.ObservationSnapshot()
	if observation.Lifecycle != stationcore.StationLifecycleStopped {
		t.Fatalf("core lifecycle after Close = %q, want STOPPED", observation.Lifecycle)
	}
	ready, reason := fixture.core.Ready()
	if ready || reason != stationcore.ReasonDraining {
		t.Fatalf("core.Ready after Close = (%v, %q), want (false, DRAINING)", ready, reason)
	}

	recorder := fixture.get(t, OperatorOverviewPath)
	overview := decodeOverview(t, recorder.Body.String())
	if overview.Lifecycle != "STOPPED" {
		t.Fatalf("overview lifecycle = %q, want STOPPED", overview.Lifecycle)
	}
	if overview.Readiness != readinessOverview(ready, reason, observation.Lifecycle) ||
		overview.Readiness != (ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonStopped}) {
		t.Fatalf("overview readiness = %+v, want exact core pairing NOT_SERVING/STOPPED", overview.Readiness)
	}
}

func TestOperatorClockUnsafePairingExact(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Advance the persisted watermark, then fall more than 60s behind it and
	// restart: readiness must pair CLOCK_UNSAFE while lifecycle stays RUNNING.
	if _, err := fixture.server.Reserve(ctx, fixture.namespace, ReserveRequest{RequestID: "rq_opClockReserve001", TTLMs: 86_400_000}); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	fixture.clock.now = fixture.clock.now.Add(-90 * time.Second)
	if err := fixture.core.Restart(); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	ready, reason := fixture.core.Ready()
	if ready || reason != stationcore.ReasonClockUnsafe {
		t.Fatalf("core.Ready after backward restart = (%v, %q), want (false, CLOCK_UNSAFE)", ready, reason)
	}
	observation := fixture.core.ObservationSnapshot()
	if observation.Lifecycle != stationcore.StationLifecycleRunning {
		t.Fatalf("core lifecycle = %q, want RUNNING", observation.Lifecycle)
	}

	recorder := fixture.get(t, OperatorOverviewPath)
	overview := decodeOverview(t, recorder.Body.String())
	if overview.Lifecycle != "RUNNING" {
		t.Fatalf("overview lifecycle = %q, want RUNNING", overview.Lifecycle)
	}
	if overview.Readiness != readinessOverview(ready, reason, observation.Lifecycle) ||
		overview.Readiness != (ReadinessOverview{Status: ReadinessNotServing, Reason: ReadinessReasonClockUnsafe}) {
		t.Fatalf("overview readiness = %+v, want exact core pairing NOT_SERVING/CLOCK_UNSAFE", overview.Readiness)
	}
}

func TestOperatorServesPublicShellUnauthenticated(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	index, ok := stationoverview.File(stationoverview.IndexFile)
	if !ok {
		t.Fatalf("embedded index missing")
	}
	for _, path := range []string{"/", "/index.html"} {
		t.Run("path "+path, func(t *testing.T) {
			recorder := fixture.request(t, http.MethodGet, path)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
			}
			if got := recorder.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
				t.Fatalf("Content-Type = %q, want text/html; charset=utf-8", got)
			}
			if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("Cache-Control = %q, want no-store", got)
			}
			if got := recorder.Body.String(); got != string(index) {
				t.Fatalf("shell body differs from the embedded index.html")
			}
			assertStaticSecurityHeaders(t, recorder)
			assertNoCredentialLeakage(t, fixture, "shell body", recorder.Body.String())
		})
	}
}

func TestOperatorStaticHeadAdmitted(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	index, _ := stationoverview.File(stationoverview.IndexFile)

	recorder := fixture.request(t, http.MethodHead, "/")
	if recorder.Code != http.StatusOK {
		t.Fatalf("HEAD / status = %d, want 200", recorder.Code)
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("HEAD / body = %d bytes, want empty", recorder.Body.Len())
	}
	if got := recorder.Header().Get("Content-Length"); got != strconv.Itoa(len(index)) {
		t.Fatalf("HEAD / Content-Length = %q, want %d", got, len(index))
	}
	if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("HEAD / Cache-Control = %q, want no-store", got)
	}
	assertStaticSecurityHeaders(t, recorder)

	missing := fixture.request(t, http.MethodHead, "/assets/")
	if missing.Code != http.StatusNotFound {
		t.Fatalf("HEAD /assets/ status = %d, want 404", missing.Code)
	}
	if missing.Body.Len() != 0 {
		t.Fatalf("HEAD /assets/ body = %d bytes, want empty", missing.Body.Len())
	}
	if got := missing.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("HEAD /assets/ Cache-Control = %q, want no-store", got)
	}
	assertStaticSecurityHeaders(t, missing)
}

func TestOperatorServesEmbeddedAssetsImmutable(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	for _, path := range discoverEmbeddedAssets(t) {
		name := strings.TrimPrefix(path, "/")
		content, ok := stationoverview.File(name)
		if !ok {
			t.Fatalf("embedded %s missing", name)
		}
		wantType := "text/javascript; charset=utf-8"
		if strings.HasSuffix(name, ".css") {
			wantType = "text/css; charset=utf-8"
		}
		t.Run("GET "+path, func(t *testing.T) {
			recorder := fixture.request(t, http.MethodGet, path)
			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200 (body=%s)", recorder.Code, recorder.Body.String())
			}
			if got := recorder.Body.String(); got != string(content) {
				t.Fatalf("asset body differs from the embedded file")
			}
			if got := recorder.Header().Get("Content-Type"); got != wantType {
				t.Fatalf("Content-Type = %q, want %q", got, wantType)
			}
			if got := recorder.Header().Get("Cache-Control"); got != staticAssetCacheControl {
				t.Fatalf("Cache-Control = %q, want %q", got, staticAssetCacheControl)
			}
			assertStaticSecurityHeaders(t, recorder)
			assertNoCredentialLeakage(t, fixture, "asset body", recorder.Body.String())
		})
		t.Run("HEAD "+path, func(t *testing.T) {
			recorder := fixture.request(t, http.MethodHead, path)
			if recorder.Code != http.StatusOK {
				t.Fatalf("HEAD status = %d, want 200", recorder.Code)
			}
			if recorder.Body.Len() != 0 {
				t.Fatalf("HEAD body = %d bytes, want empty", recorder.Body.Len())
			}
			if got := recorder.Header().Get("Content-Length"); got != strconv.Itoa(len(content)) {
				t.Fatalf("HEAD Content-Length = %q, want %d", got, len(content))
			}
			if got := recorder.Header().Get("Cache-Control"); got != staticAssetCacheControl {
				t.Fatalf("HEAD Cache-Control = %q, want %q", got, staticAssetCacheControl)
			}
			assertStaticSecurityHeaders(t, recorder)
		})
	}
}

func TestOperatorStaticPathRejectionsFixed(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	probes := []string{
		"/assets/",
		"/assets/..",
		"/assets/../operator/v1/overview",
		"/assets/%2e%2e/operator/v1/overview",
		"/assets/sub/index.html",
		"/assets/a/b.js",
		"/assets/missing-file.js",
		"/assets/source.js.map",
	}
	for _, path := range probes {
		t.Run("probe "+path, func(t *testing.T) {
			// Static rejections need no Authorization and are always the
			// same fixed 404: no listing, no traversal, no fallback, no echo.
			recorder := fixture.request(t, http.MethodGet, path)
			assertOperatorError(t, recorder, http.StatusNotFound, "NOT_FOUND")
			body := recorder.Body.String()
			if strings.Contains(body, path) {
				t.Fatalf("404 body must never echo the requested path %q", path)
			}
			if strings.Contains(body, "<html") || strings.Contains(body, "assets") {
				t.Fatalf("404 body must never render a directory listing")
			}
		})
	}
}

func TestOperatorUnknownPathsFallbackToBearerFirst(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	// Unknown non-static paths keep the bearer-first contract: 401 without a
	// credential, fixed 404 with one, and never an echo of the path.
	for _, path := range []string{"/other", "/operator", "/operator/v1", "/assets"} {
		t.Run("path "+path, func(t *testing.T) {
			recorder := fixture.request(t, http.MethodGet, path)
			assertOperatorError(t, recorder, http.StatusUnauthorized, "UNAUTHENTICATED")
			recorder = fixture.get(t, path)
			assertOperatorError(t, recorder, http.StatusNotFound, "NOT_FOUND")
			if strings.Contains(recorder.Body.String(), path) {
				t.Fatalf("404 body must never echo the requested path %q", path)
			}
		})
	}
}

func TestOperatorStaticMethodHandling(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	targets := append([]string{"/", "/index.html"}, discoverEmbeddedAssets(t)...)
	for _, path := range targets {
		for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodOptions} {
			t.Run(method+" "+path, func(t *testing.T) {
				recorder := fixture.request(t, method, path)
				assertOperatorError(t, recorder, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED")
				if got := recorder.Header().Get("Allow"); got != "GET, HEAD" {
					t.Fatalf("Allow = %q, want GET, HEAD", got)
				}
				assertStaticSecurityHeaders(t, recorder)
			})
		}
	}
	// Auth still precedes method on the API path: an unauthenticated POST to
	// the overview is 401, never 405.
	recorder := fixture.request(t, http.MethodPost, OperatorOverviewPath)
	assertOperatorError(t, recorder, http.StatusUnauthorized, "UNAUTHENTICATED")
}

func TestOperatorAPIStillRequiresBearerWithStaticRoutes(t *testing.T) {
	t.Parallel()
	fixture := openOperatorFixture(t, 1)
	// The frozen API contract is unchanged by public static routes: missing
	// bearer on the overview is still the fixed 401 with WWW-Authenticate.
	recorder := fixture.request(t, http.MethodGet, OperatorOverviewPath)
	assertOperatorError(t, recorder, http.StatusUnauthorized, "UNAUTHENTICATED")
	if got := recorder.Header().Get("WWW-Authenticate"); got != "Bearer" {
		t.Fatalf("WWW-Authenticate = %q, want Bearer", got)
	}
	if strings.Contains(recorder.Body.String(), fixture.token) {
		t.Fatalf("401 body must never echo the presented token")
	}
	// The public shell stays reachable without any credential.
	shell := fixture.request(t, http.MethodGet, "/")
	if shell.Code != http.StatusOK {
		t.Fatalf("shell status = %d, want 200", shell.Code)
	}
	// And the authenticated overview still resolves byte-for-byte.
	golden := fixture.get(t, OperatorOverviewPath)
	if golden.Code != http.StatusOK || !strings.Contains(golden.Body.String(), `"schema": "badtower.station-overview.v1"`) {
		t.Fatalf("overview after static routes = %d, want 200 frozen body", golden.Code)
	}
}

func TestOperatorEmbeddedTreeClosed(t *testing.T) {
	t.Parallel()
	assets := stationoverview.AssetFiles()
	allowed := []string{
		stationoverview.IndexFile,
		assets[0],
		assets[1],
	}
	for _, name := range allowed {
		if content, ok := stationoverview.File(name); !ok || len(content) == 0 {
			t.Fatalf("allowlisted embedded artifact is unavailable")
		}
	}
	for _, name := range []string{
		"", ".", "assets", "assets/", "../index.html",
		"assets/source.js.map", "assets/unreviewed.js", ".hidden",
	} {
		if _, ok := stationoverview.File(name); ok {
			t.Fatalf("non-allowlisted embedded name was exposed")
		}
	}

	// The shell must reference exactly the two allowlisted hashed assets.
	discovered := discoverEmbeddedAssets(t)
	want := map[string]bool{
		"/" + assets[0]: true,
		"/" + assets[1]: true,
	}
	for _, path := range discovered {
		if !want[path] {
			t.Fatalf("shell references a non-allowlisted asset")
		}
		delete(want, path)
	}
	if len(want) != 0 {
		t.Fatalf("shell does not reference every allowlisted hashed asset")
	}
}
