package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/LicoLand/BadTower/internal/pluginpackage"
	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "PLUGIN_PACKAGE_FAILED")
		os.Exit(1)
	}
}

func run(arguments []string) error {
	if len(arguments) == 0 {
		return errors.New("subcommand required")
	}
	switch arguments[0] {
	case "descriptor":
		return descriptor(arguments[1:])
	case "create":
		return create(arguments[1:])
	case "sign":
		return sign(arguments[1:])
	case "verify":
		return verify(arguments[1:])
	default:
		return errors.New("unknown subcommand")
	}
}

func descriptor(arguments []string) error {
	flags := flag.NewFlagSet("descriptor", flag.ContinueOnError)
	pluginID := flags.String("plugin-id", "", "closed plugin identifier")
	artifactVersion := flags.String("artifact-version", "", "plugin artifact SemVer")
	configurationSchema := flags.String("configuration-schema", "", "closed plugin configuration schema")
	localStateSchema := flags.String("local-state-schema", "", "closed plugin local-state schema or none")
	protocolIdentity := flags.String("protocol-identity", "", "external wire identity")
	protocolLine := flags.String("protocol-line", "", "external protocol line")
	protocolLifecycle := flags.String("protocol-lifecycle", "", "external lifecycle")
	listenerTransport := flags.String("listener-transport", "", "external listener transport")
	binaryPath := flags.String("binary", "", "built plugin binary")
	binaryArchivePath := flags.String("binary-archive-path", "", "binary path inside package")
	outputPath := flags.String("output", "", "canonical descriptor output")
	maxConcurrent := flags.Uint("max-concurrent-calls", 0, "plugin concurrent native calls")
	maxQueued := flags.Uint("max-queued-calls", 0, "plugin queued native calls")
	maxRequestBytes := flags.Uint64("max-request-bytes", 0, "plugin request byte ceiling")
	maxStateBytes := flags.Uint64("max-state-bytes", 0, "plugin local-state byte ceiling")
	maxMetricSeries := flags.Uint("max-metric-series", 0, "plugin metric-series ceiling")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *binaryPath == "" || *outputPath == "" {
		return errors.New("required path missing")
	}
	binary, err := os.Open(*binaryPath)
	if err != nil {
		return err
	}
	defer binary.Close()
	info, err := binary.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 128<<20 {
		return errors.New("binary invalid")
	}
	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(binary, (128<<20)+1))
	if err != nil || written != info.Size() {
		return errors.New("binary digest failed")
	}
	value := extension.Descriptor{
		SchemaVersion: extension.SchemaVersion, PluginID: *pluginID, ArtifactVersion: *artifactVersion,
		ConfigurationSchema: *configurationSchema, LocalStateSchema: *localStateSchema,
		ExternalProtocol: extension.ExternalProtocol{Identity: *protocolIdentity, Line: *protocolLine, Lifecycle: *protocolLifecycle},
		Contract:         extension.ContractRange{Minimum: extension.ContractVersion, Maximum: extension.ContractVersion},
		Listener:         extension.Listener{Owner: "external-plugin", Transport: *listenerTransport},
		NativeScope:      extension.NativeScope{NamespaceMode: "one-opaque-namespace", CredentialMode: "scoped-native-caller"},
		Budgets: extension.Budgets{
			MaxConcurrentCalls: uint32(*maxConcurrent), MaxQueuedCalls: uint32(*maxQueued),
			MaxRequestBytes: *maxRequestBytes, MaxStateBytes: *maxStateBytes, MaxMetricSeries: uint32(*maxMetricSeries),
		},
		Binary: extension.Binary{Path: *binaryArchivePath, SHA256: hex.EncodeToString(hash.Sum(nil))},
	}
	canonical, err := value.CanonicalJSON()
	if err != nil {
		return err
	}
	return writeExclusive(*outputPath, canonical, 0o600)
}

func create(arguments []string) error {
	flags := flag.NewFlagSet("create", flag.ContinueOnError)
	descriptorPath := flags.String("descriptor", "", "canonical plugin descriptor")
	binaryPath := flags.String("binary", "", "plugin binary")
	licensePath := flags.String("license", "", "AGPL license text")
	archivePath := flags.String("archive", "", "output archive")
	manifestPath := flags.String("manifest", "", "output detached manifest")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if *descriptorPath == "" || *binaryPath == "" || *licensePath == "" || *archivePath == "" || *manifestPath == "" {
		return errors.New("required path missing")
	}
	descriptor, err := readBounded(*descriptorPath, 64<<10)
	if err != nil {
		return err
	}
	binary, err := os.Open(*binaryPath)
	if err != nil {
		return err
	}
	defer binary.Close()
	info, err := binary.Stat()
	if err != nil {
		return err
	}
	license, err := os.Open(*licensePath)
	if err != nil {
		return err
	}
	defer license.Close()
	licenseInfo, err := license.Stat()
	if err != nil || !licenseInfo.Mode().IsRegular() {
		return errors.New("license invalid")
	}
	archive, err := os.OpenFile(*archivePath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	manifest, receipt, createErr := pluginpackage.Create(archive, pluginpackage.CreateInput{
		Descriptor: descriptor, License: pluginpackage.Source{Reader: license, Size: licenseInfo.Size()},
		Binary: pluginpackage.Source{Reader: binary, Size: info.Size()},
	})
	closeErr := archive.Close()
	if createErr != nil {
		return createErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := writeExclusive(*manifestPath, manifest, 0o600); err != nil {
		return err
	}
	return writeReceipt(receipt)
}

func sign(arguments []string) error {
	flags := flag.NewFlagSet("sign", flag.ContinueOnError)
	manifestPath := flags.String("manifest", "", "detached manifest")
	privateKeyPath := flags.String("private-key", "", "PKCS#8 Ed25519 key")
	signaturePath := flags.String("signature", "", "output signature")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	manifest, err := readBounded(*manifestPath, 64<<10)
	if err != nil {
		return err
	}
	privateKey, err := readPrivateKey(*privateKeyPath)
	if err != nil {
		return err
	}
	defer clear(privateKey)
	signature, receipt, err := pluginpackage.Sign(manifest, privateKey)
	if err != nil {
		return err
	}
	if err := writeExclusive(*signaturePath, signature, 0o600); err != nil {
		return err
	}
	return writeReceipt(receipt)
}

func verify(arguments []string) error {
	flags := flag.NewFlagSet("verify", flag.ContinueOnError)
	archivePath := flags.String("archive", "", "plugin archive")
	manifestPath := flags.String("manifest", "", "detached manifest")
	signaturePath := flags.String("signature", "", "detached signature")
	publicKeyPath := flags.String("public-key", "", "PKIX Ed25519 public key")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	archive, err := os.Open(*archivePath)
	if err != nil {
		return err
	}
	defer archive.Close()
	manifest, err := readBounded(*manifestPath, 64<<10)
	if err != nil {
		return err
	}
	signature, err := readBounded(*signaturePath, 16<<10)
	if err != nil {
		return err
	}
	publicKey, err := readPublicKey(*publicKeyPath)
	if err != nil {
		return err
	}
	receipt, err := pluginpackage.Verify(archive, manifest, signature, publicKey)
	if err != nil {
		return err
	}
	return writeReceipt(receipt)
}

func readBounded(path string, maximum int64) ([]byte, error) {
	if path == "" {
		return nil, errors.New("path required")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > maximum {
		return nil, errors.New("bounded read failed")
	}
	return raw, nil
}

func writeExclusive(path string, content []byte, mode os.FileMode) error {
	if path == "" {
		return errors.New("path required")
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func readPrivateKey(path string) (ed25519.PrivateKey, error) {
	raw, err := readBounded(path, 16<<10)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != "PRIVATE KEY" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("private key invalid")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	clear(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(key) != ed25519.PrivateKeySize {
		return nil, errors.New("private key invalid")
	}
	return append(ed25519.PrivateKey(nil), key...), nil
}

func readPublicKey(path string) (ed25519.PublicKey, error) {
	raw, err := readBounded(path, 16<<10)
	if err != nil {
		return nil, err
	}
	block, rest := pem.Decode(raw)
	if block == nil || block.Type != "PUBLIC KEY" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("public key invalid")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(ed25519.PublicKey)
	if !ok || len(key) != ed25519.PublicKeySize {
		return nil, errors.New("public key invalid")
	}
	return append(ed25519.PublicKey(nil), key...), nil
}

func writeReceipt(receipt pluginpackage.Receipt) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(receipt)
}
