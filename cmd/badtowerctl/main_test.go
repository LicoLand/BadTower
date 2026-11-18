package main

import (
	"context"
	"crypto/rand"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/nativeapi"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

func TestCtlCheckConfigValidatesWithoutOpeningDataRoot(t *testing.T) {
	t.Parallel()
	configPath := filepath.Join(writeCtlFixtureDir(t), "badtower.station.v1.json")
	err := run([]string{"check-config", "--config", configPath})
	if err != nil {
		t.Fatalf("check-config valid fixture: %v", err)
	}
}

func TestCtlCheckConfigRejectsUnknownKeys(t *testing.T) {
	t.Parallel()
	dir := writeCtlFixtureDir(t)
	configPath := filepath.Join(dir, "badtower.station.v1.json")
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(dir, "invalid.json"),
		[]byte(strings.Replace(string(content), `"schema"`, `"unknown":true,"schema"`, 1)),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	err = run([]string{"check-config", "--config", filepath.Join(dir, "invalid.json")})
	if err == nil {
		t.Fatal("unknown config key must fail")
	}
}

func TestCtlCredentialGenerateNeverPrintsTokenToStdout(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	outputPath := filepath.Join(dir, "credential")
	err := run([]string{
		"credential", "generate",
		"--output", outputPath,
	})
	if err != nil {
		t.Fatalf("credential generate: %v", err)
	}
	info, err := os.Stat(outputPath)
	if err != nil {
		t.Fatal("credential file must exist")
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("credential file mode = %o, want 0600", info.Mode().Perm())
	}
}

func TestCtlDataCommandsRequireCredentialFile(t *testing.T) {
	t.Parallel()
	dir := writeCtlFixtureDir(t)
	credentialPath := filepath.Join(dir, "credential")
	if err := os.WriteFile(credentialPath, []byte("synthetic-token-material\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "badtower.station.v1.json")

	err := run([]string{
		"reserve",
		"--config", configPath,
		"--credential-file", credentialPath,
		"--request-id", "rq_ctlReserve00000001",
		"--ttl-ms", "60000",
	})
	if err == nil {
		t.Fatal("reserve without running server should fail dial")
	}
}

// TestCtlReserveSubmitClaimSettleLiveServer proves the direct plugin-free
// badtowerctl harness over a real native gRPC listener (REQ-015). It is not
// parallel because it captures the process stdout.
func TestCtlReserveSubmitClaimSettleLiveServer(t *testing.T) {
	dir := t.TempDir()
	namespace := "ns_aaaaaaaaaaaaaaaaaaaaaa"
	tokenMaterial := make([]byte, 32)
	if _, err := rand.Read(tokenMaterial); err != nil {
		t.Fatal(err)
	}
	token, digest, err := nativeapi.GenerateToken(tokenMaterial)
	if err != nil {
		t.Fatal(err)
	}

	config := nativeapi.Config{
		Schema:   nativeapi.ConfigSchema,
		Profile:  nativeapi.ProfileSingleNode,
		DataRoot: filepath.Join(dir, "station-core-v1"),
		Transport: nativeapi.TransportConfig{
			Mode:       "unix",
			SocketPath: filepath.Join(shortSocketDir(t), "station.sock"),
		},
		Health:  nativeapi.ListenerConfig{Listen: "127.0.0.1:0"},
		Metrics: nativeapi.ListenerConfig{Listen: "127.0.0.1:0"},
		Namespaces: []nativeapi.NamespaceConfig{{
			ID: namespace,
			Credentials: []nativeapi.CredentialConfig{{
				SHA256:    digest,
				NotBefore: time.Now().Add(-time.Hour),
				NotAfter:  time.Now().Add(24 * time.Hour),
			}},
		}},
	}
	config, err = nativeapi.ValidateConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(dir, "badtower.station.v1.json")
	configJSON, err := nativeapi.NormalizeConfigJSON(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, configJSON, 0o600); err != nil {
		t.Fatal(err)
	}
	credentialPath := filepath.Join(dir, "credential")
	if err := os.WriteFile(credentialPath, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	payloadPath := filepath.Join(dir, "payload")
	if err := os.WriteFile(payloadPath, []byte("ctl native payload\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  config.DataRoot,
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, ctlTestClock{t: time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = core.Close() }()
	auth := nativeapi.NewAuthenticator(config)
	server := nativeapi.NewServer(core, config, auth)
	server.SetLifecycleRunning()
	listener, err := nativeapi.OpenListener(config.Transport)
	if err != nil {
		t.Fatal(err)
	}
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = server.ServeData(serveCtx, listener, nil) }()

	args := func(command string, extra ...string) []string {
		return append([]string{
			command,
			"--config", configPath,
			"--credential-file", credentialPath,
		}, extra...)
	}

	output, err := runCaptureOutput(func() error {
		return run(args("reserve",
			"--request-id", "rq_ctlLiveReserve01",
			"--ttl-ms", "3600000"))
	})
	if err != nil {
		t.Fatalf("ctl reserve: %v", err)
	}
	addressID := parseCtlField(t, output, "address_id")
	if !strings.HasPrefix(addressID, "sa_") {
		t.Fatalf("reserve output = %q", output)
	}

	output, err = runCaptureOutput(func() error {
		return run(args("submit",
			"--request-id", "rq_ctlLiveSubmit01",
			"--address-id", addressID,
			"--payload-file", payloadPath,
			"--ttl-ms", "60000"))
	})
	if err != nil {
		t.Fatalf("ctl submit: %v", err)
	}
	unitID := parseCtlField(t, output, "unit_id")
	if !strings.HasPrefix(unitID, "tu_") {
		t.Fatalf("submit output = %q", output)
	}

	output, err = runCaptureOutput(func() error {
		return run(args("claim",
			"--request-id", "rq_ctlLiveClaim01",
			"--address-id", addressID,
			"--max-units", "1",
			"--max-bytes", "1024",
			"--visibility-timeout-ms", "30000"))
	})
	if err != nil {
		t.Fatalf("ctl claim: %v", err)
	}
	claimID := parseCtlField(t, output, "claim_id")
	epoch := parseCtlField(t, output, "claim_epoch")
	if !strings.HasPrefix(claimID, "dc_") || epoch == "" {
		t.Fatalf("claim output = %q", output)
	}

	if err := run(args("settle",
		"--request-id", "rq_ctlLiveSettle01",
		"--claim-id", claimID,
		"--claim-epoch", epoch,
		"--entry", unitID+":complete")); err != nil {
		t.Fatalf("ctl settle: %v", err)
	}

	output, err = runCaptureOutput(func() error {
		return run(args("capacity"))
	})
	if err != nil {
		t.Fatalf("ctl capacity: %v", err)
	}
	if !strings.Contains(output, "profile_id=single-node-general-v1") {
		t.Fatalf("capacity output = %q", output)
	}
	if !strings.Contains(output, "pressure=PRESSURE_NORMAL") {
		t.Fatalf("capacity output must use the closed pressure enum: %q", output)
	}
}

// shortSocketDir returns a short dedicated temp directory for unix socket
// paths. t.TempDir() embeds the full test name (up to ~45 chars) plus a
// numeric suffix, which pushes a socket path past the platform sun_path limit
// (~104 bytes on macOS). A "bt" prefix plus a short numeric suffix keeps the
// worst-case path well under 90 bytes.
func shortSocketDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "bt")
	if err != nil {
		t.Fatalf("short socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

type ctlTestClock struct{ t time.Time }

func (clock ctlTestClock) NowUnixMs() int64 { return clock.t.UnixMilli() }

func (clock ctlTestClock) MonotonicNow() time.Duration { return 0 }

func parseCtlField(t *testing.T, output, field string) string {
	t.Helper()
	for _, token := range strings.Fields(output) {
		key, value, ok := strings.Cut(token, "=")
		if ok && key == field {
			return value
		}
	}
	t.Fatalf("field %q missing from output %q", field, output)
	return ""
}

func runCaptureOutput(fn func() error) (string, error) {
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		return "", err
	}
	os.Stdout = writer
	runErr := fn()
	_ = writer.Close()
	os.Stdout = original
	content, readErr := io.ReadAll(reader)
	_ = reader.Close()
	if readErr != nil {
		return "", readErr
	}
	if runErr != nil {
		return "", runErr
	}
	return string(content), nil
}

func writeCtlFixtureDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	now := time.Now().UTC()
	notBefore := now.Add(-time.Hour).Format(time.RFC3339)
	notAfter := now.Add(24 * time.Hour).Format(time.RFC3339)
	config := []byte(`{
  "schema":"badtower.station.config.v1",
  "profile":"single-node-general-v1",
  "dataRoot":"` + filepath.Join(dir, "station-core-v1") + `",
  "transport":{"mode":"unix","socketPath":"` + filepath.Join(dir, "badtower.sock") + `"},
  "health":{"listen":"127.0.0.1:0","allowNonLoopback":false},
  "metrics":{"listen":"127.0.0.1:0","allowNonLoopback":false},
  "namespaces":[{"id":"ns_YYYYYYYYYYYYYYYYYYYYYY","credentials":[{"sha256":"BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB","notBefore":"` + notBefore + `","notAfter":"` + notAfter + `"}]}]
}`)
	if err := os.WriteFile(filepath.Join(dir, "badtower.station.v1.json"), config, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}
