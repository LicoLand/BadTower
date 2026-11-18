package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
)

func TestDescriptorBindsActualBinaryDigest(t *testing.T) {
	root := t.TempDir()
	binaryPath := filepath.Join(root, "plugin")
	outputPath := filepath.Join(root, "descriptor.json")
	binary := []byte("exact-plugin-binary")
	if err := os.WriteFile(binaryPath, binary, 0o700); err != nil {
		t.Fatal(err)
	}
	err := run([]string{
		"descriptor", "--plugin-id", "licoarc.candidate", "--artifact-version", "0.1.0",
		"--configuration-schema", "badtower.licoarc-candidate.config.v1", "--local-state-schema", "badtower.licoarc-candidate.state.v1",
		"--protocol-identity", "licoarc.protocol-line.v1", "--protocol-line", "licoarc.v1",
		"--protocol-lifecycle", "Candidate", "--listener-transport", "licoarc.https-transport.v1",
		"--binary", binaryPath, "--binary-archive-path", "bin/badtower-licoarc-candidate", "--output", outputPath,
		"--max-concurrent-calls", "64", "--max-queued-calls", "256", "--max-request-bytes", "524288",
		"--max-state-bytes", "1073741824", "--max-metric-series", "32",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := extension.DecodeDescriptor(raw)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	if descriptor.Binary.SHA256 != hex.EncodeToString(digest[:]) || descriptor.ExternalProtocol.Line != "licoarc.v1" {
		t.Fatalf("descriptor=%#v", descriptor)
	}
}
