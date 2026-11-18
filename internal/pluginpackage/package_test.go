package pluginpackage

import (
	"archive/tar"
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
)

func TestPluginPackageCreateSignVerifyAndTamper(t *testing.T) {
	t.Parallel()
	binary := []byte("synthetic-plugin-binary")
	license := []byte("AGPL-3.0-or-later")
	digest := sha256.Sum256(binary)
	descriptor := extension.Descriptor{
		SchemaVersion: extension.SchemaVersion, PluginID: "synthetic.plugin", ArtifactVersion: "1.0.0",
		ConfigurationSchema: "badtower.synthetic.config.v1", LocalStateSchema: "none",
		ExternalProtocol: extension.ExternalProtocol{Identity: "synthetic", Line: "synthetic.v1", Lifecycle: "Candidate"},
		Contract:         extension.ContractRange{Minimum: extension.ContractVersion, Maximum: extension.ContractVersion},
		Listener:         extension.Listener{Owner: "external-plugin", Transport: "synthetic"},
		NativeScope:      extension.NativeScope{NamespaceMode: "one-opaque-namespace", CredentialMode: "scoped-native-caller"},
		Budgets:          extension.Budgets{MaxConcurrentCalls: 1, MaxQueuedCalls: 1, MaxRequestBytes: 1024, MaxStateBytes: 1024, MaxMetricSeries: 1},
		Binary:           extension.Binary{Path: "bin/synthetic-plugin", SHA256: hex.EncodeToString(digest[:])},
	}
	descriptorBytes, err := descriptor.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	manifest, created, err := Create(&archive, CreateInput{Descriptor: descriptorBytes, License: Source{Reader: bytes.NewReader(license), Size: int64(len(license))}, Binary: Source{Reader: bytes.NewReader(binary), Size: int64(len(binary))}})
	if err != nil || created.Status != "created" {
		t.Fatalf("Create: receipt=%#v err=%v", created, err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signature, _, err := Sign(manifest, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := Verify(bytes.NewReader(archive.Bytes()), manifest, signature, publicKey)
	if err != nil || verified.Status != "verified" {
		t.Fatalf("Verify: receipt=%#v err=%v", verified, err)
	}
	tampered := append([]byte(nil), archive.Bytes()...)
	tampered[600] ^= 1
	if _, err := Verify(bytes.NewReader(tampered), manifest, signature, publicKey); err == nil {
		t.Fatal("tampered archive verified")
	}
}

func TestPluginPackageIsDeterministic(t *testing.T) {
	t.Parallel()
	binary := []byte("x")
	license := []byte("AGPL")
	digest := sha256.Sum256(binary)
	descriptor := extension.Descriptor{
		SchemaVersion: extension.SchemaVersion, PluginID: "deterministic.plugin", ArtifactVersion: "1.0.0",
		ConfigurationSchema: "badtower.synthetic.config.v1", LocalStateSchema: "none",
		ExternalProtocol: extension.ExternalProtocol{Identity: "synthetic", Line: "synthetic.v1", Lifecycle: "Candidate"},
		Contract:         extension.ContractRange{Minimum: extension.ContractVersion, Maximum: extension.ContractVersion},
		Listener:         extension.Listener{Owner: "external-plugin", Transport: "synthetic"},
		NativeScope:      extension.NativeScope{NamespaceMode: "one-opaque-namespace", CredentialMode: "scoped-native-caller"},
		Budgets:          extension.Budgets{MaxConcurrentCalls: 1, MaxQueuedCalls: 1, MaxRequestBytes: 1, MaxStateBytes: 1, MaxMetricSeries: 1},
		Binary:           extension.Binary{Path: "bin/deterministic-plugin", SHA256: hex.EncodeToString(digest[:])},
	}
	descriptorBytes, _ := descriptor.CanonicalJSON()
	create := func() ([]byte, []byte) {
		var archive bytes.Buffer
		manifest, _, err := Create(&archive, CreateInput{Descriptor: descriptorBytes, License: Source{Reader: bytes.NewReader(license), Size: int64(len(license))}, Binary: Source{Reader: bytes.NewReader(binary), Size: 1}})
		if err != nil {
			t.Fatal(err)
		}
		return archive.Bytes(), manifest
	}
	firstArchive, firstManifest := create()
	secondArchive, secondManifest := create()
	if !bytes.Equal(firstArchive, secondArchive) || !bytes.Equal(firstManifest, secondManifest) {
		t.Fatal("package output is not deterministic")
	}
}

func TestPluginPackageRejectsSignedDescriptorManifestMismatch(t *testing.T) {
	t.Parallel()
	binary := []byte("synthetic-plugin-binary")
	license := []byte("AGPL-3.0-or-later")
	digest := sha256.Sum256(binary)
	descriptor := extension.Descriptor{
		SchemaVersion: extension.SchemaVersion, PluginID: "synthetic.plugin", ArtifactVersion: "1.0.0",
		ConfigurationSchema: "badtower.synthetic.config.v1", LocalStateSchema: "none",
		ExternalProtocol: extension.ExternalProtocol{Identity: "synthetic", Line: "synthetic.v1", Lifecycle: "Candidate"},
		Contract:         extension.ContractRange{Minimum: extension.ContractVersion, Maximum: extension.ContractVersion},
		Listener:         extension.Listener{Owner: "external-plugin", Transport: "synthetic"},
		NativeScope:      extension.NativeScope{NamespaceMode: "one-opaque-namespace", CredentialMode: "scoped-native-caller"},
		Budgets:          extension.Budgets{MaxConcurrentCalls: 1, MaxQueuedCalls: 1, MaxRequestBytes: 1024, MaxStateBytes: 1024, MaxMetricSeries: 1},
		Binary:           extension.Binary{Path: "bin/synthetic-plugin", SHA256: hex.EncodeToString(digest[:])},
	}
	descriptorBytes, err := descriptor.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	manifestBytes, _, err := Create(&archive, CreateInput{
		Descriptor: descriptorBytes,
		License:    Source{Reader: bytes.NewReader(license), Size: int64(len(license))},
		Binary:     Source{Reader: bytes.NewReader(binary), Size: int64(len(binary))},
	})
	if err != nil {
		t.Fatal(err)
	}
	var manifest Manifest
	if err := decodeCanonical(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.PluginID = "different.plugin"
	manifestBytes, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes = append(manifestBytes, '\n')
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signature, _, err := Sign(manifestBytes, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(bytes.NewReader(archive.Bytes()), manifestBytes, signature, publicKey); err == nil {
		t.Fatal("signed descriptor/manifest identity mismatch verified")
	}

	// A signed manifest may not redirect the executable member away from the
	// descriptor's closed binary path, even when the archive follows it.
	manifest.PluginID = descriptor.PluginID
	manifest.Entries[2].Path = "bin/other-plugin"
	var redirected bytes.Buffer
	tw := tar.NewWriter(&redirected)
	entries := [][]byte{descriptorBytes, license, binary}
	for index, entry := range entries {
		expected := manifest.Entries[index]
		mode := parseMode(expected.Mode)
		if err := tw.WriteHeader(&tar.Header{Name: expected.Path, Mode: mode, Size: int64(len(entry)), Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	redirectedDigest := sha256.Sum256(redirected.Bytes())
	manifest.Archive = Digest{Bytes: int64(redirected.Len()), SHA256: hex.EncodeToString(redirectedDigest[:])}
	manifestBytes, err = json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestBytes = append(manifestBytes, '\n')
	signature, _, err = Sign(manifestBytes, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Verify(bytes.NewReader(redirected.Bytes()), manifestBytes, signature, publicKey); err == nil {
		t.Fatal("signed descriptor/binary path mismatch verified")
	}
}

func TestPluginPackageRejectsNonCanonicalTarLayoutEvenWhenSigned(t *testing.T) {
	t.Parallel()
	binary := []byte("synthetic-plugin-binary")
	license := []byte("AGPL-3.0-or-later")
	digest := sha256.Sum256(binary)
	descriptor := extension.Descriptor{
		SchemaVersion: extension.SchemaVersion, PluginID: "synthetic.plugin", ArtifactVersion: "1.0.0",
		ConfigurationSchema: "badtower.synthetic.config.v1", LocalStateSchema: "none",
		ExternalProtocol: extension.ExternalProtocol{Identity: "synthetic", Line: "synthetic.v1", Lifecycle: "Candidate"},
		Contract:         extension.ContractRange{Minimum: extension.ContractVersion, Maximum: extension.ContractVersion},
		Listener:         extension.Listener{Owner: "external-plugin", Transport: "synthetic"},
		NativeScope:      extension.NativeScope{NamespaceMode: "one-opaque-namespace", CredentialMode: "scoped-native-caller"},
		Budgets:          extension.Budgets{MaxConcurrentCalls: 1, MaxQueuedCalls: 1, MaxRequestBytes: 1024, MaxStateBytes: 1024, MaxMetricSeries: 1},
		Binary:           extension.Binary{Path: "bin/synthetic-plugin", SHA256: hex.EncodeToString(digest[:])},
	}
	descriptorBytes, err := descriptor.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	var canonicalArchive bytes.Buffer
	baseManifestBytes, _, err := Create(&canonicalArchive, CreateInput{
		Descriptor: descriptorBytes,
		License:    Source{Reader: bytes.NewReader(license), Size: int64(len(license))},
		Binary:     Source{Reader: bytes.NewReader(binary), Size: int64(len(binary))},
	})
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	signMutated := func(archiveBytes []byte) ([]byte, []byte) {
		var manifest Manifest
		if err := decodeCanonical(baseManifestBytes, &manifest); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(archiveBytes)
		manifest.Archive = Digest{Bytes: int64(len(archiveBytes)), SHA256: hex.EncodeToString(sum[:])}
		raw, err := json.Marshal(manifest)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, '\n')
		sig, _, err := Sign(raw, privateKey)
		if err != nil {
			t.Fatal(err)
		}
		return raw, sig
	}

	// 1. PAX extension header injected before a valid entry must be rejected.
	var paxArchive bytes.Buffer
	pw := tar.NewWriter(&paxArchive)
	entries := []struct {
		name string
		mode int64
		data []byte
	}{
		{DescriptorPath, 0o644, descriptorBytes},
		{LicensePath, 0o644, license},
		{descriptor.Binary.Path, 0o755, binary},
	}
	for index, entry := range entries {
		hdr := &tar.Header{
			Name: entry.name, Mode: entry.mode, Size: int64(len(entry.data)),
			Typeflag: tar.TypeReg, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR,
		}
		if index == 0 {
			hdr.Format = tar.FormatPAX
			hdr.PAXRecords = map[string]string{"comment": "extra"}
		}
		if err := pw.WriteHeader(hdr); err != nil {
			t.Fatal(err)
		}
		if _, err := pw.Write(entry.data); err != nil {
			t.Fatal(err)
		}
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	paxManifest, paxSig := signMutated(paxArchive.Bytes())
	if _, err := Verify(bytes.NewReader(paxArchive.Bytes()), paxManifest, paxSig, publicKey); err == nil {
		t.Fatal("signed PAX-extended archive was accepted")
	}

	// 2. Truncated single-block terminator (512 zero bytes instead of 1024) must be rejected.
	raw := canonicalArchive.Bytes()
	shortTerminator := append([]byte(nil), raw[:len(raw)-tarBlockSize]...)
	shortManifest, shortSig := signMutated(shortTerminator)
	if _, err := Verify(bytes.NewReader(shortTerminator), shortManifest, shortSig, publicKey); err == nil {
		t.Fatal("signed archive with truncated tar terminator was accepted")
	}

	// 3. Non-zero padding byte after entry content must be rejected.
	dirtyPadding := append([]byte(nil), raw...)
	dirtyPadding[tarBlockSize+len(descriptorBytes)] = 0x55
	dirtyManifest, dirtySig := signMutated(dirtyPadding)
	if _, err := Verify(bytes.NewReader(dirtyPadding), dirtyManifest, dirtySig, publicKey); err == nil {
		t.Fatal("signed archive with non-zero tar padding was accepted")
	}
}
