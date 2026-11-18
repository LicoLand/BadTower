package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LicoLand/BadTower/internal/nodepackage"
)

func TestPackageManifestProjectsNativeStoreIdentity(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "badtower-input")
	licensePath := filepath.Join(directory, "license-input")
	archivePath := filepath.Join(directory, "package.tar.gz")
	manifestPath := filepath.Join(directory, "package.manifest.json")
	writeTestFile(t, binaryPath, []byte("synthetic executable input\n"), 0o700)
	writeTestFile(t, licensePath, []byte("synthetic license input\n"), 0o600)

	created, err := run([]string{
		"create",
		"--binary", binaryPath,
		"--license", licensePath,
		"--archive", archivePath,
		"--manifest", manifestPath,
		"--version", "0.3.0-native.1",
		"--platform", "linux-amd64",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	const want = "badtower.station-core.store.v2"
	if created.StationStoreFormat != want {
		t.Fatalf("receipt stationStoreFormat = %q, want %q", created.StationStoreFormat, want)
	}
	manifestBytes, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(manifestBytes), want) {
		t.Fatalf("manifest missing native store identity %q", want)
	}
}

func TestRunCreateSignVerifyLifecycle(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "badtower-input")
	licensePath := filepath.Join(directory, "license-input")
	archivePath := filepath.Join(directory, "package.tar.gz")
	manifestPath := filepath.Join(directory, "package.manifest.json")
	privateKeyPath := filepath.Join(directory, "private.pem")
	publicKeyPath := filepath.Join(directory, "public.pem")
	signaturePath := filepath.Join(directory, "package.signature.json")

	writeTestFile(t, binaryPath, []byte("synthetic executable input\n"), 0o700)
	writeTestFile(t, licensePath, []byte("synthetic license input\n"), 0o600)

	created, err := run([]string{
		"create",
		"--binary", binaryPath,
		"--license", licensePath,
		"--archive", archivePath,
		"--manifest", manifestPath,
		"--version", "2.1.0-rc.1",
		"--platform", "linux-amd64",
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.Operation != "create" || created.Status != "created" ||
		created.Version != "2.1.0-rc.1" || created.Platform != "linux-amd64" {
		t.Fatalf("unexpected create receipt: %#v", created)
	}

	privatePEM, publicPEM := syntheticPEMKeys(t)
	writeTestFile(t, privateKeyPath, privatePEM, 0o600)
	writeTestFile(t, publicKeyPath, publicPEM, 0o644)
	clear(privatePEM)

	signed, err := run([]string{
		"sign",
		"--manifest", manifestPath,
		"--private-key", privateKeyPath,
		"--signature", signaturePath,
	})
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	if signed.Operation != "sign" || signed.Status != "signed" ||
		signed.Signature == nil || signed.Signature.Algorithm != "Ed25519" {
		t.Fatalf("unexpected sign receipt: %#v", signed)
	}

	verified, err := run([]string{
		"verify",
		"--archive", archivePath,
		"--manifest", manifestPath,
		"--signature", signaturePath,
		"--public-key", publicKeyPath,
	})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if verified.Operation != "verify" || verified.Status != "verified" ||
		verified.Archive != created.Archive ||
		verified.Manifest != created.Manifest {
		t.Fatalf("unexpected verify receipt: %#v", verified)
	}
	encodedReceipt, err := json.Marshal(verified)
	if err != nil {
		t.Fatalf("marshal verify receipt: %v", err)
	}
	if strings.Contains(string(encodedReceipt), directory) {
		t.Fatal("verification receipt disclosed a local path")
	}
}

func TestRunNeverOverwritesOutputs(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "binary")
	licensePath := filepath.Join(directory, "license")
	archivePath := filepath.Join(directory, "archive")
	manifestPath := filepath.Join(directory, "manifest")
	writeTestFile(t, binaryPath, []byte("binary"), 0o700)
	writeTestFile(t, licensePath, []byte("license"), 0o600)

	sentinel := []byte("existing output")
	writeTestFile(t, archivePath, sentinel, 0o600)
	_, err := run([]string{
		"create",
		"--binary", binaryPath,
		"--license", licensePath,
		"--archive", archivePath,
		"--manifest", manifestPath,
		"--version", "1.0.0",
		"--platform", "linux-amd64",
	})
	requireCLIErrorCode(t, err, nodepackage.CodeOutputExists)
	content, err := os.ReadFile(archivePath)
	if err != nil {
		t.Fatalf("read existing output: %v", err)
	}
	if !bytes.Equal(content, sentinel) {
		t.Fatal("existing archive output was modified")
	}
}

func TestRunRollsBackNewArchiveWhenManifestExists(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "binary")
	licensePath := filepath.Join(directory, "license")
	archivePath := filepath.Join(directory, "archive")
	manifestPath := filepath.Join(directory, "manifest")
	writeTestFile(t, binaryPath, []byte("binary"), 0o700)
	writeTestFile(t, licensePath, []byte("license"), 0o600)
	sentinel := []byte("existing manifest")
	writeTestFile(t, manifestPath, sentinel, 0o600)

	_, err := run([]string{
		"create",
		"--binary", binaryPath,
		"--license", licensePath,
		"--archive", archivePath,
		"--manifest", manifestPath,
		"--version", "1.0.0",
		"--platform", "linux-amd64",
	})
	requireCLIErrorCode(t, err, nodepackage.CodeOutputExists)
	if _, err := os.Stat(archivePath); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("new archive was not rolled back")
	}
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read existing manifest: %v", err)
	}
	if !bytes.Equal(content, sentinel) {
		t.Fatal("existing manifest output was modified")
	}
}

func TestRunSignNeverOverwritesSignature(t *testing.T) {
	t.Parallel()
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "binary")
	licensePath := filepath.Join(directory, "license")
	archivePath := filepath.Join(directory, "archive")
	manifestPath := filepath.Join(directory, "manifest")
	privateKeyPath := filepath.Join(directory, "private.pem")
	signaturePath := filepath.Join(directory, "signature")
	writeTestFile(t, binaryPath, []byte("binary"), 0o700)
	writeTestFile(t, licensePath, []byte("license"), 0o600)
	if _, err := run([]string{
		"create",
		"--binary", binaryPath,
		"--license", licensePath,
		"--archive", archivePath,
		"--manifest", manifestPath,
		"--version", "1.0.0",
		"--platform", "linux-amd64",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	privatePEM, _ := syntheticPEMKeys(t)
	writeTestFile(t, privateKeyPath, privatePEM, 0o600)
	clear(privatePEM)
	sentinel := []byte("existing signature")
	writeTestFile(t, signaturePath, sentinel, 0o600)

	_, err := run([]string{
		"sign",
		"--manifest", manifestPath,
		"--private-key", privateKeyPath,
		"--signature", signaturePath,
	})
	requireCLIErrorCode(t, err, nodepackage.CodeOutputExists)
	content, err := os.ReadFile(signaturePath)
	if err != nil {
		t.Fatal("read existing signature")
	}
	if !bytes.Equal(content, sentinel) {
		t.Fatal("existing signature output was modified")
	}
}

func TestRunReturnsOnlyStablePathFreeFailures(t *testing.T) {
	t.Parallel()
	sensitivePathFragment := "sensitive-local-input"
	_, err := run([]string{
		"verify",
		"--archive", sensitivePathFragment,
		"--manifest", sensitivePathFragment,
		"--signature", sensitivePathFragment,
		"--public-key", sensitivePathFragment,
	})
	requireCLIErrorCode(t, err, nodepackage.CodeInputOpenFailed)
	failure := nodepackage.ProjectFailure(err)
	encoded, marshalErr := json.Marshal(failure)
	if marshalErr != nil {
		t.Fatalf("marshal failure: %v", marshalErr)
	}
	if strings.Contains(string(encoded), sensitivePathFragment) ||
		strings.Contains(failure.Error(), sensitivePathFragment) {
		t.Fatal("failure projection disclosed an input path")
	}

	_, err = run([]string{"create", "--unexpected"})
	requireCLIErrorCode(t, err, nodepackage.CodeArgumentsInvalid)
}

func syntheticPEMKeys(t *testing.T) ([]byte, []byte) {
	t.Helper()
	seed := sha256.Sum256([]byte("badtower-package synthetic CLI test key"))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	defer clear(privateKey)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal synthetic private key: %v", err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("marshal synthetic public key: %v", err)
	}
	return pem.EncodeToMemory(
			&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER},
		), pem.EncodeToMemory(
			&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER},
		)
}

func writeTestFile(t *testing.T, path string, content []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, content, mode); err != nil {
		t.Fatal("write synthetic test input")
	}
}

func requireCLIErrorCode(t *testing.T, err error, expected nodepackage.Code) {
	t.Helper()
	var failure *nodepackage.Failure
	if !errors.As(err, &failure) {
		t.Fatalf("expected typed failure, got %T", err)
	}
	if failure.Code != expected {
		t.Fatalf("failure code = %q, want %q", failure.Code, expected)
	}
}
