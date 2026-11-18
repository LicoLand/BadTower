package pluginpackage

import (
	"archive/tar"
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
)

const (
	ManifestSchema     = "badtower.external-plugin-package.v1"
	SignatureSchema    = "badtower.external-plugin-signature.v1"
	MediaType          = "application/vnd.licoland.badtower.external-plugin.v1+tar"
	DescriptorPath     = "plugin-descriptor.json"
	LicensePath        = "LICENSE"
	maxArchiveBytes    = int64(160 << 20)
	maxBinaryBytes     = int64(128 << 20)
	maxDescriptorBytes = int64(64 << 10)
	signingDomain      = "badtower.external-plugin-package.v1\x00"
)

var ErrInvalidPackage = errors.New("invalid external plugin package")

type Source struct {
	Reader io.Reader
	Size   int64
}

type CreateInput struct {
	Descriptor []byte
	License    Source
	Binary     Source
}

type Manifest struct {
	Schema    string  `json:"schema"`
	MediaType string  `json:"mediaType"`
	PluginID  string  `json:"pluginId"`
	Version   string  `json:"version"`
	Archive   Digest  `json:"archive"`
	Entries   []Entry `json:"entries"`
}

type Digest struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type Entry struct {
	Path   string `json:"path"`
	Mode   string `json:"mode"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type Signature struct {
	Schema    string `json:"schema"`
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"keyId"`
	Value     string `json:"signature"`
}

type Receipt struct {
	Status   string `json:"status"`
	PluginID string `json:"pluginId"`
	Version  string `json:"version"`
	Archive  Digest `json:"archive"`
	Manifest Digest `json:"manifest"`
	KeyID    string `json:"keyId,omitempty"`
}

func Create(output io.Writer, input CreateInput) ([]byte, Receipt, error) {
	if output == nil || int64(len(input.Descriptor)) == 0 || int64(len(input.Descriptor)) > maxDescriptorBytes || input.License.Reader == nil || input.License.Size <= 0 || input.License.Size > maxDescriptorBytes || input.Binary.Reader == nil || input.Binary.Size <= 0 || input.Binary.Size > maxBinaryBytes {
		return nil, Receipt{}, ErrInvalidPackage
	}
	descriptor, err := extension.DecodeDescriptor(input.Descriptor)
	if err != nil {
		return nil, Receipt{}, ErrInvalidPackage
	}
	canonical, err := descriptor.CanonicalJSON()
	if err != nil || !bytes.Equal(canonical, input.Descriptor) {
		return nil, Receipt{}, ErrInvalidPackage
	}

	hash := sha256.New()
	counted := &countingWriter{writer: io.MultiWriter(output, hash)}
	archive := tar.NewWriter(&limitedWriter{writer: counted, remaining: maxArchiveBytes})
	entries := make([]Entry, 0, 3)
	descriptorEntry, err := writeEntry(archive, DescriptorPath, 0o644, bytes.NewReader(input.Descriptor), int64(len(input.Descriptor)))
	if err != nil {
		return nil, Receipt{}, err
	}
	entries = append(entries, descriptorEntry)
	licenseEntry, err := writeEntry(archive, LicensePath, 0o644, input.License.Reader, input.License.Size)
	if err != nil {
		return nil, Receipt{}, err
	}
	entries = append(entries, licenseEntry)
	binaryEntry, err := writeEntry(archive, descriptor.Binary.Path, 0o755, input.Binary.Reader, input.Binary.Size)
	if err != nil {
		return nil, Receipt{}, err
	}
	entries = append(entries, binaryEntry)
	if err := archive.Close(); err != nil {
		return nil, Receipt{}, ErrInvalidPackage
	}
	if binaryEntry.SHA256 != descriptor.Binary.SHA256 {
		return nil, Receipt{}, ErrInvalidPackage
	}
	manifest := Manifest{
		Schema: ManifestSchema, MediaType: MediaType, PluginID: descriptor.PluginID, Version: descriptor.ArtifactVersion,
		Archive: Digest{Bytes: counted.written, SHA256: hex.EncodeToString(hash.Sum(nil))}, Entries: entries,
	}
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		return nil, Receipt{}, ErrInvalidPackage
	}
	manifestBytes = append(manifestBytes, '\n')
	receipt := Receipt{Status: "created", PluginID: manifest.PluginID, Version: manifest.Version, Archive: manifest.Archive, Manifest: digestOf(manifestBytes)}
	return manifestBytes, receipt, nil
}

func Sign(manifest []byte, privateKey ed25519.PrivateKey) ([]byte, Receipt, error) {
	parsed, err := parseManifest(manifest)
	if err != nil || len(privateKey) != ed25519.PrivateKeySize {
		return nil, Receipt{}, ErrInvalidPackage
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID := "sha256:" + sha256Hex(publicKey)
	value := ed25519.Sign(privateKey, append([]byte(signingDomain), manifest...))
	document := Signature{Schema: SignatureSchema, Algorithm: "Ed25519", KeyID: keyID, Value: base64.StdEncoding.EncodeToString(value)}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, Receipt{}, ErrInvalidPackage
	}
	raw = append(raw, '\n')
	return raw, Receipt{Status: "signed", PluginID: parsed.PluginID, Version: parsed.Version, Archive: parsed.Archive, Manifest: digestOf(manifest), KeyID: keyID}, nil
}

func Verify(archiveSource io.Reader, manifestBytes, signatureBytes []byte, publicKey ed25519.PublicKey) (Receipt, error) {
	manifest, err := parseManifest(manifestBytes)
	if err != nil || archiveSource == nil || len(publicKey) != ed25519.PublicKeySize {
		return Receipt{}, ErrInvalidPackage
	}
	var signature Signature
	if err := decodeCanonical(signatureBytes, &signature); err != nil || signature.Schema != SignatureSchema || signature.Algorithm != "Ed25519" || signature.KeyID != "sha256:"+sha256Hex(publicKey) {
		return Receipt{}, ErrInvalidPackage
	}
	value, err := base64.StdEncoding.DecodeString(signature.Value)
	if err != nil || len(value) != ed25519.SignatureSize || !ed25519.Verify(publicKey, append([]byte(signingDomain), manifestBytes...), value) {
		return Receipt{}, ErrInvalidPackage
	}
	if err := verifyArchive(archiveSource, manifest); err != nil {
		return Receipt{}, err
	}
	return Receipt{Status: "verified", PluginID: manifest.PluginID, Version: manifest.Version, Archive: manifest.Archive, Manifest: digestOf(manifestBytes), KeyID: signature.KeyID}, nil
}

func writeEntry(archive *tar.Writer, path string, mode int64, source io.Reader, size int64) (Entry, error) {
	if path == "" || size <= 0 {
		return Entry{}, ErrInvalidPackage
	}
	header := &tar.Header{Name: path, Mode: mode, Size: size, Typeflag: tar.TypeReg, Uid: 0, Gid: 0, ModTime: time.Unix(0, 0).UTC(), Format: tar.FormatUSTAR}
	if err := archive.WriteHeader(header); err != nil {
		return Entry{}, ErrInvalidPackage
	}
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(archive, hash), io.LimitReader(source, size))
	if err != nil || written != size {
		return Entry{}, ErrInvalidPackage
	}
	var extra [1]byte
	if count, err := source.Read(extra[:]); count != 0 || err != io.EOF {
		return Entry{}, ErrInvalidPackage
	}
	return Entry{Path: path, Mode: fmt.Sprintf("%04o", mode), Bytes: size, SHA256: hex.EncodeToString(hash.Sum(nil))}, nil
}

func parseManifest(raw []byte) (Manifest, error) {
	var manifest Manifest
	if err := decodeCanonical(raw, &manifest); err != nil || manifest.Schema != ManifestSchema || manifest.MediaType != MediaType || len(manifest.Entries) != 3 || manifest.Entries[0].Path != DescriptorPath || manifest.Entries[0].Mode != "0644" || manifest.Entries[1].Path != LicensePath || manifest.Entries[1].Mode != "0644" || manifest.Entries[2].Mode != "0755" || manifest.Archive.Bytes <= 0 || manifest.Archive.Bytes > maxArchiveBytes || !validDigest(manifest.Archive.SHA256) {
		return Manifest{}, ErrInvalidPackage
	}
	limits := [...]int64{maxDescriptorBytes, maxDescriptorBytes, maxBinaryBytes}
	for index, entry := range manifest.Entries {
		if entry.Bytes <= 0 || entry.Bytes > limits[index] || !validDigest(entry.SHA256) {
			return Manifest{}, ErrInvalidPackage
		}
	}
	return manifest, nil
}

func decodeCanonical(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrInvalidPackage
	}
	canonical, err := json.Marshal(target)
	if err != nil {
		return err
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(raw, canonical) {
		return ErrInvalidPackage
	}
	return nil
}

const tarBlockSize = 512

func expectedUSTARHeader(expected Entry) ([]byte, error) {
	mode := parseMode(expected.Mode)
	if mode == 0 {
		return nil, ErrInvalidPackage
	}
	var encoded bytes.Buffer
	writer := tar.NewWriter(&encoded)
	header := &tar.Header{
		Name:     expected.Path,
		Mode:     mode,
		Size:     expected.Bytes,
		Typeflag: tar.TypeReg,
		Uid:      0,
		Gid:      0,
		ModTime:  time.Unix(0, 0).UTC(),
		Format:   tar.FormatUSTAR,
	}
	if err := writer.WriteHeader(header); err != nil || encoded.Len() != tarBlockSize {
		return nil, ErrInvalidPackage
	}
	return encoded.Bytes(), nil
}

func tarPadding(size int64) int {
	remainder := size % tarBlockSize
	if remainder == 0 {
		return 0
	}
	return int(tarBlockSize - remainder)
}

func allZero(content []byte) bool {
	var combined byte
	for _, value := range content {
		combined |= value
	}
	return combined == 0
}

func verifyArchive(source io.Reader, manifest Manifest) error {
	limited := &io.LimitedReader{R: source, N: maxArchiveBytes + 1}
	hash := sha256.New()
	counted := &countingReader{reader: io.TeeReader(limited, hash)}
	var descriptorBytes []byte
	for index, expected := range manifest.Entries {
		var actualHeader [tarBlockSize]byte
		if _, err := io.ReadFull(counted, actualHeader[:]); err != nil {
			return ErrInvalidPackage
		}
		expectedHeader, err := expectedUSTARHeader(expected)
		if err != nil || !bytes.Equal(actualHeader[:], expectedHeader) {
			return ErrInvalidPackage
		}
		entryHash := sha256.New()
		entryTarget := io.Writer(entryHash)
		var descriptor bytes.Buffer
		if index == 0 {
			entryTarget = io.MultiWriter(entryHash, &descriptor)
		}
		written, err := io.CopyN(entryTarget, counted, expected.Bytes)
		if err != nil || written != expected.Bytes || hex.EncodeToString(entryHash.Sum(nil)) != expected.SHA256 {
			return ErrInvalidPackage
		}
		padding := tarPadding(expected.Bytes)
		var paddingBytes [tarBlockSize]byte
		if _, err := io.ReadFull(counted, paddingBytes[:padding]); err != nil || !allZero(paddingBytes[:padding]) {
			return ErrInvalidPackage
		}
		if index == 0 {
			descriptorBytes = append([]byte(nil), descriptor.Bytes()...)
		}
	}
	var endBlocks [tarBlockSize * 2]byte
	if _, err := io.ReadFull(counted, endBlocks[:]); err != nil || !allZero(endBlocks[:]) {
		return ErrInvalidPackage
	}
	var trailing [1]byte
	if count, err := counted.Read(trailing[:]); count != 0 || err != io.EOF {
		return ErrInvalidPackage
	}
	if limited.N == 0 || counted.read != manifest.Archive.Bytes || hex.EncodeToString(hash.Sum(nil)) != manifest.Archive.SHA256 {
		return ErrInvalidPackage
	}
	if count, err := source.Read(trailing[:]); count != 0 || err != io.EOF {
		return ErrInvalidPackage
	}
	descriptor, err := extension.DecodeDescriptor(descriptorBytes)
	if err != nil {
		return ErrInvalidPackage
	}
	canonical, err := descriptor.CanonicalJSON()
	if err != nil || !bytes.Equal(canonical, descriptorBytes) || manifest.PluginID != descriptor.PluginID || manifest.Version != descriptor.ArtifactVersion || manifest.Entries[2].Path != descriptor.Binary.Path || manifest.Entries[2].SHA256 != descriptor.Binary.SHA256 {
		return ErrInvalidPackage
	}
	return nil
}

type limitedWriter struct {
	writer    io.Writer
	remaining int64
}

func (writer *limitedWriter) Write(raw []byte) (int, error) {
	if int64(len(raw)) > writer.remaining {
		return 0, ErrInvalidPackage
	}
	written, err := writer.writer.Write(raw)
	writer.remaining -= int64(written)
	return written, err
}

type countingWriter struct {
	writer  io.Writer
	written int64
}

func (writer *countingWriter) Write(raw []byte) (int, error) {
	n, err := writer.writer.Write(raw)
	writer.written += int64(n)
	return n, err
}

type countingReader struct {
	reader io.Reader
	read   int64
}

func (reader *countingReader) Read(raw []byte) (int, error) {
	n, err := reader.reader.Read(raw)
	reader.read += int64(n)
	return n, err
}

func digestOf(raw []byte) Digest  { return Digest{Bytes: int64(len(raw)), SHA256: sha256Hex(raw)} }
func sha256Hex(raw []byte) string { sum := sha256.Sum256(raw); return hex.EncodeToString(sum[:]) }
func validDigest(value string) bool {
	_, err := hex.DecodeString(value)
	return len(value) == 64 && err == nil
}
func parseMode(value string) int64 {
	if value == "0644" {
		return 0o644
	}
	if value == "0755" {
		return 0o755
	}
	return 0
}
