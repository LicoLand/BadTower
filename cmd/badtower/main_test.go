package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/nativeapi"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

func TestMainNativeConfigMustRejectRetiredDataRoot(t *testing.T) {
	t.Parallel()
	config := filepath.Join(t.TempDir(), "badtower.station.v1.json")
	notBefore, notAfter := activeCredentialWindow()
	content := `{
  "schema":"badtower.station.config.v1",
  "profile":"single-node-general-v1",
  "dataRoot":"data/badtower.db",
  "transport":{"mode":"unix","socketPath":"/tmp/badtower.sock"},
  "health":{"listen":"127.0.0.1:7444","allowNonLoopback":false},
  "metrics":{"listen":"127.0.0.1:7445","allowNonLoopback":false},
  "namespaces":[{"id":"ns_ZZZZZZZZZZZZZZZZZZZZZZ","credentials":[{"sha256":"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC","notBefore":"` + notBefore + `","notAfter":"` + notAfter + `"}]}]
}`
	if err := os.WriteFile(config, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckConfig(config); err == nil {
		t.Fatal("retired dataRoot must be rejected")
	}
}

func TestMainCheckConfigAcceptsNativeFixture(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "badtower.station.v1.json")
	notBefore, notAfter := activeCredentialWindow()
	content := `{
  "schema":"badtower.station.config.v1",
  "profile":"single-node-general-v1",
  "dataRoot":"` + filepath.Join(dir, "station-core-v1") + `",
  "transport":{"mode":"unix","socketPath":"` + filepath.Join(dir, "badtower.sock") + `"},
  "health":{"listen":"127.0.0.1:7444","allowNonLoopback":false},
  "metrics":{"listen":"127.0.0.1:7445","allowNonLoopback":false},
  "namespaces":[{"id":"ns_AAAAAAAAAAAAAAAAAAAAAA","credentials":[{"sha256":"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD","notBefore":"` + notBefore + `","notAfter":"` + notAfter + `"}]}]
}`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := CheckConfig(configPath); err != nil {
		t.Fatalf("CheckConfig: %v", err)
	}
}

func TestMainRequiresConfigFlag(t *testing.T) {
	t.Parallel()
	if err := run(nil); err == nil {
		t.Fatal("expected missing --config error")
	}
}

func TestMainCheckConfigRejectsStrictViolations(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	notBefore := now.Add(-time.Hour).Format(time.RFC3339)
	notAfter := now.Add(24 * time.Hour).Format(time.RFC3339)
	base := strictConfigFixture(t, notBefore, notAfter)
	cases := []struct {
		name   string
		mutate func(string) string
	}{
		{
			name: "wrong schema",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"schema":"badtower.station.config.v1"`,
					`"schema":"badtower.station.config.v2"`, 1)
			},
		},
		{
			name: "wrong profile",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"profile":"single-node-general-v1"`,
					`"profile":"other-profile"`, 1)
			},
		},
		{
			name: "unknown key",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"schema"`, `"unknown":true,"schema"`, 1)
			},
		},
		{
			name: "duplicate key with identical value",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"schema":"badtower.station.config.v1"`,
					`"schema":"badtower.station.config.v1","schema":"badtower.station.config.v1"`, 1)
			},
		},
		{
			name: "expired credential",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"notAfter":"`+notAfter+`"`,
					`"notAfter":"`+now.Add(-30*time.Minute).Format(time.RFC3339)+`"`, 1)
			},
		},
		{
			name: "not-before in future",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"notBefore":"`+notBefore+`"`,
					`"notBefore":"`+now.Add(6*time.Hour).Format(time.RFC3339)+`"`, 1)
			},
		},
		{
			name: "invalid credential digest",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"sha256":"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC"`,
					`"sha256":"short"`, 1)
			},
		},
		{
			name: "lifetime over 90 days",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"notBefore":"`+notBefore+`"`,
					`"notBefore":"`+now.Add(-100*24*time.Hour).Format(time.RFC3339)+`"`, 1)
			},
		},
		{
			name: "tcp transport without tls files",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"transport":{"mode":"unix","socketPath":"/tmp/badtower.sock"}`,
					`"transport":{"mode":"tcp","listen":"127.0.0.1:7443"}`, 1)
			},
		},
		{
			name: "non-loopback health without allowNonLoopback",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"health":{"listen":"127.0.0.1:7444","allowNonLoopback":false}`,
					`"health":{"listen":"0.0.0.0:7444","allowNonLoopback":false}`, 1)
			},
		},
		{
			name: "zero namespace limit",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"credentials":[`,
					`"limits":{"units":0},"credentials":[`, 1)
			},
		},
		{
			name: "duplicate namespace id",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"namespaces":[{"id":"ns_ZZZZZZZZZZZZZZZZZZZZZZ"`,
					`"namespaces":[{"id":"ns_ZZZZZZZZZZZZZZZZZZZZZZ"},{"id":"ns_ZZZZZZZZZZZZZZZZZZZZZZ"`, 1)
			},
		},
		{
			name: "three credentials",
			mutate: func(content string) string {
				return strings.Replace(content,
					`"credentials":[{`,
					`"credentials":[{},{},{`, 1)
			},
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			configPath := filepath.Join(t.TempDir(), "invalid.json")
			if err := os.WriteFile(configPath, []byte(testCase.mutate(base)), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := CheckConfig(configPath); err == nil {
				t.Fatalf("config with %s must be rejected", testCase.name)
			}
		})
	}
}

func TestMainConfigMaterializesDefaults(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	configPath := filepath.Join(dir, "minimal.json")
	notBefore, notAfter := activeCredentialWindow()
	content := `{
  "schema":"badtower.station.config.v1",
  "profile":"single-node-general-v1",
  "transport":{"mode":"unix","socketPath":"` + filepath.Join(dir, "badtower.sock") + `"},
  "namespaces":[{"id":"ns_AAAAAAAAAAAAAAAAAAAAAA","credentials":[{"sha256":"DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD","notBefore":"` + notBefore + `","notAfter":"` + notAfter + `"}]}]
}`
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := nativeapi.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig minimal: %v", err)
	}
	if config.DataRoot != nativeapi.DefaultDataRoot {
		t.Fatalf("dataRoot = %q, want %q", config.DataRoot, nativeapi.DefaultDataRoot)
	}
	if config.Health.Listen != nativeapi.DefaultHealthListen {
		t.Fatalf("health.listen = %q, want %q", config.Health.Listen, nativeapi.DefaultHealthListen)
	}
	if config.Metrics.Listen != nativeapi.DefaultMetricsListen {
		t.Fatalf("metrics.listen = %q, want %q", config.Metrics.Listen, nativeapi.DefaultMetricsListen)
	}
}

// activeCredentialWindow returns RFC3339 credential window bounds that are
// active against the real clock at call time, so config fixtures never age
// out of validity.
func activeCredentialWindow() (notBefore, notAfter string) {
	now := time.Now().UTC()
	return now.Add(-time.Hour).Format(time.RFC3339), now.Add(24 * time.Hour).Format(time.RFC3339)
}

func strictConfigFixture(t *testing.T, notBefore, notAfter string) string {
	t.Helper()
	return `{
  "schema":"badtower.station.config.v1",
  "profile":"single-node-general-v1",
  "dataRoot":"data/station-core-v1",
  "transport":{"mode":"unix","socketPath":"/tmp/badtower.sock"},
  "health":{"listen":"127.0.0.1:7444","allowNonLoopback":false},
  "metrics":{"listen":"127.0.0.1:7445","allowNonLoopback":false},
  "namespaces":[{"id":"ns_ZZZZZZZZZZZZZZZZZZZZZZ","credentials":[{"sha256":"CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC","notBefore":"` + notBefore + `","notAfter":"` + notAfter + `"}]}]
}`
}

func TestNativeAPIRetiredPathConstant(t *testing.T) {
	t.Parallel()
	if nativeapi.RetiredDataPath != "data/badtower.db" {
		t.Fatalf("retired path = %q", nativeapi.RetiredDataPath)
	}
}

// operatorSelectionConfig returns a strict, validated station config whose
// operator block is absent, disabled, or enabled depending on mode. Listen
// auxiliary addresses use ephemeral ports; the strict operator address uses
// a preselected nonzero loopback port.
func operatorSelectionConfig(t *testing.T, mode string) nativeapi.Config {
	t.Helper()
	dir := t.TempDir()
	notBefore, notAfter := activeCredentialWindow()
	namespaceDigest := strings.Repeat("D", 43)
	operatorDigest := strings.Repeat("E", 43)
	// The Unix socket path must stay under the platform sun_path limit, so it
	// lives in a short-named directory instead of the long t.TempDir() one.
	socketPath := filepath.Join(shortSocketDir(t), "bt.sock")
	var operator string
	switch mode {
	case "absent":
	case "disabled":
		operator = `,"operator":{"enabled":false}`
	case "enabled":
		operator = `,"operator":{"enabled":true,"listen":"` + freeLoopbackListen(t) + `","credentials":[{"sha256":"` + operatorDigest + `","notBefore":"` + notBefore + `","notAfter":"` + notAfter + `"}]}`
	default:
		t.Fatalf("unknown operator mode %q", mode)
	}
	content := `{
  "schema":"badtower.station.config.v1",
  "profile":"single-node-general-v1",
  "dataRoot":"` + filepath.Join(dir, "station-core-v1") + `",
  "transport":{"mode":"unix","socketPath":"` + socketPath + `"},
  "health":{"listen":"127.0.0.1:0","allowNonLoopback":false},
  "metrics":{"listen":"127.0.0.1:0","allowNonLoopback":false},
  "namespaces":[{"id":"ns_AAAAAAAAAAAAAAAAAAAAAA","credentials":[{"sha256":"` + namespaceDigest + `","notBefore":"` + notBefore + `","notAfter":"` + notAfter + `"}]}]` + operator + `
}`
	configPath := filepath.Join(dir, "station.json")
	if err := os.WriteFile(configPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	config, err := nativeapi.LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return config
}

func freeLoopbackListen(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve loopback port: %v", err)
	}
	listen := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release loopback port: %v", err)
	}
	return listen
}

// shortSocketDir returns one short-named temp directory, cleaned up with the
// test, for Unix socket paths that must stay under the platform sun_path
// limit.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "bt")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

// dialListener reports whether one open listener accepts a connection on its
// own network, without needing an accept loop.
func dialListener(listener net.Listener) error {
	connection, err := net.Dial(listener.Addr().Network(), listener.Addr().String())
	if err != nil {
		return err
	}
	return connection.Close()
}

// trackingListener records Close calls so cleanup tests observe them without
// probing sockets.
type trackingListener struct {
	net.Listener
	closed atomic.Bool
}

func trackListener(listener net.Listener) *trackingListener {
	return &trackingListener{Listener: listener}
}

func (listener *trackingListener) Close() error {
	listener.closed.Store(true)
	return listener.Listener.Close()
}

func TestOperatorListenerSelection(t *testing.T) {
	t.Parallel()
	t.Run("absent block opens no operator listener", func(t *testing.T) {
		config := operatorSelectionConfig(t, "absent")
		data, health, metrics, operator, err := openStationListeners(config)
		if err != nil {
			t.Fatalf("openStationListeners: %v", err)
		}
		defer closeListeners(data, health, metrics, operator)
		if operator != nil {
			t.Fatal("operator listener must not be opened when the block is absent")
		}
		for name, listener := range map[string]net.Listener{"data": data, "health": health, "metrics": metrics} {
			if listener == nil {
				t.Fatalf("%s listener must be opened", name)
			}
			if err := dialListener(listener); err != nil {
				t.Fatalf("%s listener must accept connections: %v", name, err)
			}
		}
	})
	t.Run("disabled block opens no operator listener", func(t *testing.T) {
		config := operatorSelectionConfig(t, "disabled")
		data, health, metrics, operator, err := openStationListeners(config)
		if err != nil {
			t.Fatalf("openStationListeners: %v", err)
		}
		defer closeListeners(data, health, metrics, operator)
		if operator != nil {
			t.Fatal("operator listener must not be opened when the block is disabled")
		}
	})
	t.Run("enabled block opens exactly one operator listener", func(t *testing.T) {
		config := operatorSelectionConfig(t, "enabled")
		data, health, metrics, operator, err := openStationListeners(config)
		if err != nil {
			t.Fatalf("openStationListeners: %v", err)
		}
		defer closeListeners(data, health, metrics, operator)
		if operator == nil {
			t.Fatal("operator listener must be opened when the block is enabled")
		}
		if err := dialListener(operator); err != nil {
			t.Fatalf("operator listener must accept connections: %v", err)
		}
		if addr, ok := operator.Addr().(*net.TCPAddr); !ok || addr.IP == nil || addr.Port == 0 {
			t.Fatalf("operator listener must be a real TCP listener, got %v", operator.Addr())
		}
	})
}

func TestOperatorListenersCleanupOnStartupFailure(t *testing.T) {
	t.Parallel()
	// The health listener fails to open here; the already-opened data listener
	// (a Unix socket) must be closed by the time the error returns, which
	// removes its socket file.
	dir := t.TempDir()
	socketPath := filepath.Join(shortSocketDir(t), "bt.sock")
	config := nativeapi.Config{
		Schema:    nativeapi.ConfigSchema,
		Profile:   nativeapi.ProfileSingleNode,
		DataRoot:  filepath.Join(dir, "station-core-v1"),
		Transport: nativeapi.TransportConfig{Mode: "unix", SocketPath: socketPath},
		Health:    nativeapi.ListenerConfig{Listen: "127.0.0.1:99999"},
		Metrics:   nativeapi.ListenerConfig{Listen: "127.0.0.1:0"},
		Operator:  &nativeapi.OperatorConfig{Enabled: true, Listen: "127.0.0.1:0"},
	}
	data, health, metrics, operator, err := openStationListeners(config)
	if err == nil {
		t.Fatal("expected openStationListeners failure on invalid health listen")
	}
	if data != nil || health != nil || metrics != nil || operator != nil {
		t.Fatal("every listener must be nil after a partial startup failure")
	}
	if _, statErr := os.Stat(socketPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("data socket %s must be removed by cleanup", socketPath)
	}
}

func TestOperatorListenersClosedOnLaterStartupFailure(t *testing.T) {
	t.Parallel()
	// The TLS-load failure path closes every already-open listener, including
	// the optional operator listener, before returning. closeListeners is that
	// cleanup call, exercised here with the operator enabled.
	config := operatorSelectionConfig(t, "enabled")
	data, health, metrics, operator, err := openStationListeners(config)
	if err != nil {
		t.Fatalf("openStationListeners: %v", err)
	}
	tracked := []*trackingListener{
		trackListener(data), trackListener(health), trackListener(metrics), trackListener(operator),
	}
	closeListeners(tracked[0], tracked[1], tracked[2], tracked[3])
	for index, listener := range tracked {
		if !listener.closed.Load() {
			t.Fatalf("listener %d must be closed by cleanup", index)
		}
	}
	// Cleanup must tolerate nil entries and never panic.
	closeListeners(nil, nil)
}

func TestDrainStopsAdmissionInNativeAPIAndCore(t *testing.T) {
	t.Parallel()
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  t.TempDir(),
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, systemClock{})
	if err != nil {
		t.Fatalf("Open core: %v", err)
	}
	t.Cleanup(func() { _ = core.Close() })

	namespace := "ns_AAAAAAAAAAAAAAAAAAAAAA"
	now := time.Now().UTC()
	config := nativeapi.Config{
		Schema:  nativeapi.ConfigSchema,
		Profile: nativeapi.ProfileSingleNode,
		Namespaces: []nativeapi.NamespaceConfig{{
			ID: namespace,
			Credentials: []nativeapi.CredentialConfig{{
				SHA256:    "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
				NotBefore: now.Add(-time.Hour),
				NotAfter:  now.Add(time.Hour),
			}},
		}},
	}
	server := nativeapi.NewServer(core, config, nativeapi.NewAuthenticator(config))
	server.SetLifecycleRunning()

	beginDrain(server, core)

	if server.Lifecycle() != nativeapi.LifecycleDraining {
		t.Fatalf("server lifecycle = %v, want DRAINING", server.Lifecycle())
	}
	observation := core.ObservationSnapshot()
	if observation.Lifecycle != stationcore.StationLifecycleDraining {
		t.Fatalf("core lifecycle = %q, want DRAINING", observation.Lifecycle)
	}
	ready, reason := core.Ready()
	if ready || reason != stationcore.ReasonDraining {
		t.Fatalf("core.Ready = (%v, %q), want (false, DRAINING)", ready, reason)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := server.Reserve(ctx, namespace, nativeapi.ReserveRequest{RequestID: "rq_drainAdmission0001", TTLMs: 60_000}); err == nil || !strings.Contains(err.Error(), "DRAINING") {
		t.Fatalf("Reserve after drain error = %v, want DRAINING", err)
	}
}

func TestMainDrainHonorsHardDeadlineAndCannotRevive(t *testing.T) {
	t.Parallel()
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  t.TempDir(),
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, systemClock{})
	if err != nil {
		t.Fatalf("Open core: %v", err)
	}
	server := nativeapi.NewServer(core, nativeapi.Config{
		Schema:  nativeapi.ConfigSchema,
		Profile: nativeapi.ProfileSingleNode,
	}, nativeapi.NewAuthenticator(nativeapi.Config{}))
	if !server.SetLifecycleRunning() {
		t.Fatal("server did not enter RUNNING from STARTING")
	}

	var canceled atomic.Bool
	neverDone := make(chan struct{})
	started := time.Now()
	err = drainWithTimeouts(server, core, func() { canceled.Store(true) }, neverDone, time.Millisecond, 25*time.Millisecond)
	if err != nil {
		t.Fatalf("drainWithTimeouts: %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bounded drain exceeded test ceiling: %v", elapsed)
	}
	if !canceled.Load() {
		t.Fatal("drain did not propagate listener cancellation")
	}
	if server.Lifecycle() != nativeapi.LifecycleStopped {
		t.Fatalf("server lifecycle = %v, want STOPPED", server.Lifecycle())
	}
	if core.ObservationSnapshot().Lifecycle != stationcore.StationLifecycleStopped {
		t.Fatal("Station Core did not close after listener hard-stop")
	}
	if server.SetLifecycleRunning() {
		t.Fatal("stopped server was revived")
	}
}
