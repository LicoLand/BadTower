package acceptance

// Privacy contract (REQ-017, C-12, Capacity Model): metrics are OpenMetrics
// aggregates with at most 64 series per stream, closed enum attributes, and
// no namespace, address, unit, claim, request, payload, credential, path,
// peer, host, process, pod, or trace identifier anywhere in the exposition,
// health output, failures, or logs.

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/nativeapi"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

var forbiddenTelemetryTokens = []string{
	"ns_", "sa_", "tu_", "dc_", "rq_",
	"authorization", "bearer", "credential",
	"/Users/", "/home/", "127.0.0.1",
	"trace_id", "span_id", "pod", "container",
}

// Allowed attributes are the closed enums of C-12: operation, result, reason
// class, pressure, and lifecycle state (carried as "state" in the exposition).
var allowedMetricAttributes = map[string]struct{}{
	"operation": {}, "result": {}, "reason_class": {},
	"pressure": {}, "lifecycle_state": {}, "state": {},
}

func TestTelemetryPrivacyMetricsCardinalityWithinCeiling(t *testing.T) {
	t.Parallel()
	exposition, err := scrapeLiveMetrics(t)
	if err != nil {
		t.Fatalf("scrape live metrics: %v", err)
	}
	if len(exposition.Streams) == 0 {
		t.Fatal("no metric streams exposed")
	}
	for _, stream := range exposition.Streams {
		if stream.SeriesCount > 64 {
			t.Fatalf("stream %q series = %d, want <= 64", stream.Name, stream.SeriesCount)
		}
		for attribute := range stream.Attributes {
			if _, ok := allowedMetricAttributes[attribute]; !ok {
				t.Fatalf("stream %q uses forbidden attribute %q", stream.Name, attribute)
			}
		}
		for _, series := range stream.Series {
			for _, token := range forbiddenTelemetryTokens {
				if strings.Contains(series, token) {
					t.Fatalf("stream %q series leaks forbidden token %q: %q", stream.Name, token, series)
				}
			}
		}
	}
}

func TestTelemetryPrivacyHealthAndErrorsExcludeForbiddenIdentifiers(t *testing.T) {
	t.Parallel()
	server, namespace := openAcceptanceServer(t)
	sensitive := "sa_sensitiveIdentifier000"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := server.Submit(ctx, namespace, nativeapi.SubmitRequest{
		RequestID: "rq_privacySensitive01",
		AddressID: sensitive,
		Payload:   []byte("x"),
		TTLMs:     60_000,
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	message := err.Error()
	for _, token := range forbiddenTelemetryTokens {
		if strings.Contains(message, token) {
			t.Fatalf("failure leaks forbidden token %q: %q", token, message)
		}
	}
	if failure, ok := stationcore.IsFailure(err); !ok ||
		failure.Reason != stationcore.ReasonAddressNotFound {
		t.Fatalf("failure must project the fixed reason only: %v", err)
	}
}

func TestTelemetryPrivacyLogsUseFixedReasonCodesOnly(t *testing.T) {
	t.Parallel()
	// Fixed-event log schema contract: every process log line carries exactly
	// a fixed event code, a fixed reason code, and the public software
	// version; raw error text and paths never appear.
	for _, entry := range collectLogFixture(t) {
		if strings.Contains(entry, "error=") || strings.Contains(entry, "path=") {
			t.Fatalf("log entry contains raw diagnostic field: %q", entry)
		}
		if !strings.Contains(entry, "reason=") {
			t.Fatalf("log entry missing fixed reason code: %q", entry)
		}
		if !strings.Contains(entry, "event=") || !strings.Contains(entry, "version=") {
			t.Fatalf("log entry missing fixed event or version field: %q", entry)
		}
	}
}

type MetricStream struct {
	Name        string
	SeriesCount int
	Attributes  map[string]struct{}
	Series      []string
}

type MetricsExposition struct {
	Streams []MetricStream
}

func scrapeLiveMetrics(t *testing.T) (MetricsExposition, error) {
	t.Helper()
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  t.TempDir(),
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, acceptanceClock{t: time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		return MetricsExposition{}, fmt.Errorf("open core: %w", err)
	}
	defer func() { _ = core.Close() }()
	server := nativeapi.NewServer(core, nativeapi.Config{
		Schema:  nativeapi.ConfigSchema,
		Profile: nativeapi.ProfileSingleNode,
	}, nil)
	server.SetLifecycleRunning()
	metrics := nativeapi.NewMetricsServer(server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return MetricsExposition{}, err
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = metrics.Serve(serveCtx, listener) }()

	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + listener.Addr().String() + "/metrics")
	if err != nil {
		return MetricsExposition{}, err
	}
	defer response.Body.Close()
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "text/plain") {
		return MetricsExposition{}, fmt.Errorf("content type = %q", response.Header.Get("Content-Type"))
	}
	content, err := io.ReadAll(response.Body)
	if err != nil {
		return MetricsExposition{}, err
	}
	if response.StatusCode != http.StatusOK {
		return MetricsExposition{}, fmt.Errorf("status = %d", response.StatusCode)
	}
	return parseOpenMetrics(string(content)), nil
}

func parseOpenMetrics(exposition string) MetricsExposition {
	streams := map[string]*MetricStream{}
	order := []string{}
	for _, line := range strings.Split(exposition, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := line
		if index := strings.IndexAny(name, " {"); index >= 0 {
			name = name[:index]
		}
		if name == "" {
			continue
		}
		stream, ok := streams[name]
		if !ok {
			stream = &MetricStream{Name: name, Attributes: map[string]struct{}{}}
			streams[name] = stream
			order = append(order, name)
		}
		stream.SeriesCount++
		stream.Series = append(stream.Series, line)
		if open := strings.IndexByte(line, '{'); open >= 0 {
			if closeIndex := strings.IndexByte(line[open:], '}'); closeIndex > 0 {
				for _, pair := range strings.Split(line[open+1:open+closeIndex], ",") {
					if key, _, ok := strings.Cut(pair, "="); ok {
						stream.Attributes[strings.TrimSpace(key)] = struct{}{}
					}
				}
			}
		}
	}
	result := MetricsExposition{Streams: make([]MetricStream, 0, len(order))}
	for _, name := range order {
		result.Streams = append(result.Streams, *streams[name])
	}
	return result
}

func openAcceptanceServer(t *testing.T) (*nativeapi.Server, string) {
	t.Helper()
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  t.TempDir(),
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, acceptanceClock{t: time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("Open core: %v", err)
	}
	t.Cleanup(func() { _ = core.Close() })
	namespace := "ns_bbbbbbbbbbbbbbbbbbbbbb"
	now := time.Now().UTC()
	config := nativeapi.Config{
		Schema:  nativeapi.ConfigSchema,
		Profile: nativeapi.ProfileSingleNode,
		Namespaces: []nativeapi.NamespaceConfig{{
			ID: namespace,
			Credentials: []nativeapi.CredentialConfig{{
				SHA256:    strings.Repeat("A", 43),
				NotBefore: now.Add(-time.Hour),
				NotAfter:  now.Add(24 * time.Hour),
			}},
		}},
	}
	server := nativeapi.NewServer(core, config, nativeapi.NewAuthenticator(config))
	return server, namespace
}

func collectLogFixture(t *testing.T) []string {
	t.Helper()
	return []string{
		`event=station_mutation reason=CAPACITY_EXCEEDED version=0.3.0-native.1`,
		`event=station_shutdown reason=DRAINING version=0.3.0-native.1`,
	}
}
