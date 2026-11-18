package stationcluster

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

const (
	CoreSnapshotFormat        = "badtower.station-cluster.core-snapshot.v2"
	MinCoreSnapshotChunkBytes = 256
	MaxCoreSnapshotChunkBytes = 4 << 20
	MaxCoreSnapshotChunks     = 256
	MaxCoreSnapshotBytes      = 1 << 30
	maxCoreSnapshotFiles      = 4096
	maxCoreSnapshotNameBytes  = 255
	maxCoreSnapshotManifest   = 1 << 20
)

var (
	ErrInvalidSnapshot = errors.New("invalid core snapshot")
	ErrStaleSnapshot   = errors.New("stale core snapshot")
)

var coreSnapshotArchiveMagic = []byte("BTCORECP2")
var embeddedCoreSnapshotMagic = []byte("BTRSNAP2")

type CoreSnapshotBinding struct {
	ClusterID      string
	GroupID        uint64
	SourceMemberID uint64
	Partition      uint16
	OwnershipEpoch uint64
	AppliedIndex   uint64
}

type CoreSnapshotExpectation struct {
	ClusterID           string
	GroupID             uint64
	SourceMemberID      uint64
	Partition           uint16
	OwnershipEpoch      uint64
	CurrentAppliedIndex uint64
}

type CoreSnapshotChunk struct {
	Index        uint32 `json:"index"`
	SizeBytes    uint32 `json:"sizeBytes"`
	DigestSHA256 string `json:"digestSha256"`
}

type CoreSnapshotManifest struct {
	Format         string              `json:"format"`
	CoreIdentity   string              `json:"coreIdentity"`
	ClusterID      string              `json:"clusterId"`
	GroupID        uint64              `json:"groupId"`
	SourceMemberID uint64              `json:"sourceMemberId"`
	Partition      uint16              `json:"partition"`
	OwnershipEpoch uint64              `json:"ownershipEpoch"`
	AppliedIndex   uint64              `json:"appliedIndex"`
	TotalBytes     uint64              `json:"totalBytes"`
	DigestSHA256   string              `json:"digestSha256"`
	Chunks         []CoreSnapshotChunk `json:"chunks"`
}

// CreateCoreSnapshot writes a bounded, chunked artifact from one consistent
// Core checkpoint. It does not create, compact, or install a Raft snapshot.
func CreateCoreSnapshot(core *stationcore.Core, artifactRoot string, binding CoreSnapshotBinding, chunkBytes int) (CoreSnapshotManifest, error) {
	return createCoreSnapshot(core, artifactRoot, binding, chunkBytes, MaxCoreSnapshotBytes)
}

func createCoreSnapshot(core *stationcore.Core, artifactRoot string, binding CoreSnapshotBinding, chunkBytes int, maxArtifactBytes uint64) (CoreSnapshotManifest, error) {
	if core == nil || !core.OwnsOnlyPartition(binding.Partition) || validateSnapshotBinding(binding) != nil || chunkBytes < MinCoreSnapshotChunkBytes || chunkBytes > MaxCoreSnapshotChunkBytes {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if maxArtifactBytes == 0 || maxArtifactBytes > MaxCoreSnapshotBytes {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if snapshotRootsOverlap(core.DataRoot(), artifactRoot) {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if err := requireAbsentAbsoluteRoot(artifactRoot); err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := os.MkdirAll(artifactRoot, 0o700); err != nil {
		return CoreSnapshotManifest{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(artifactRoot)
		}
	}()

	checkpointRoot, err := os.MkdirTemp("", "badtower-core-snapshot-")
	if err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := os.Remove(checkpointRoot); err != nil {
		return CoreSnapshotManifest{}, err
	}
	defer os.RemoveAll(checkpointRoot)
	checkpoint, err := core.ExportSnapshotCheckpointBounded(checkpointRoot, maxArtifactBytes)
	if err != nil || checkpoint.StoreIdentity != stationcore.StoreIdentity {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}

	writer := newSnapshotChunkWriter(artifactRoot, chunkBytes, maxArtifactBytes)
	if err := encodeCoreCheckpoint(writer, checkpointRoot); err != nil {
		_ = writer.Close()
		return CoreSnapshotManifest{}, err
	}
	chunks, totalBytes, digest, err := writer.finish()
	if err != nil {
		return CoreSnapshotManifest{}, err
	}
	manifest := CoreSnapshotManifest{
		Format: CoreSnapshotFormat, CoreIdentity: stationcore.StoreIdentity,
		ClusterID: binding.ClusterID, GroupID: binding.GroupID, SourceMemberID: binding.SourceMemberID, Partition: binding.Partition,
		OwnershipEpoch: binding.OwnershipEpoch, AppliedIndex: binding.AppliedIndex,
		TotalBytes: totalBytes, DigestSHA256: digest, Chunks: chunks,
	}
	if err := validateCoreSnapshotManifest(manifest); err != nil {
		return CoreSnapshotManifest{}, err
	}
	raw, err := json.Marshal(manifest)
	if err != nil || len(raw) > maxCoreSnapshotManifest {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if err := writeSnapshotManifest(filepath.Join(artifactRoot, "manifest.json"), raw); err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := syncSnapshotDir(artifactRoot); err != nil {
		return CoreSnapshotManifest{}, err
	}
	ok = true
	return manifest, nil
}

// RestoreCoreSnapshot verifies all binding and content metadata before
// decoding into a temporary checkpoint and importing it into a fresh Core
// root. A snapshot at or behind CurrentAppliedIndex is stale.
func RestoreCoreSnapshot(artifactRoot string, expected CoreSnapshotExpectation, isolatedRoot string) (CoreSnapshotManifest, error) {
	if snapshotRootsOverlap(artifactRoot, isolatedRoot) {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	manifest, err := readCoreSnapshotManifest(artifactRoot)
	if err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := validateSnapshotExpectation(manifest, expected); err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := verifySnapshotArtifact(artifactRoot, manifest); err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := requireAbsentAbsoluteRoot(isolatedRoot); err != nil {
		return CoreSnapshotManifest{}, err
	}

	checkpointRoot, err := os.MkdirTemp("", "badtower-core-snapshot-restore-")
	if err != nil {
		return CoreSnapshotManifest{}, err
	}
	defer os.RemoveAll(checkpointRoot)
	reader := &snapshotChunkReader{root: artifactRoot, chunks: manifest.Chunks}
	if err := decodeCoreCheckpoint(reader, checkpointRoot, manifest.TotalBytes); err != nil {
		_ = reader.Close()
		return CoreSnapshotManifest{}, err
	}
	if err := reader.Close(); err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := stationcore.VerifySnapshotCheckpointTopology(checkpointRoot, stationcore.StorageModePartitionOne, manifest.Partition); err != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if err := stationcore.RestoreSnapshotCheckpoint(checkpointRoot, isolatedRoot); err != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	return manifest, nil
}

func installCoreSnapshot(core *stationcore.Core, artifactRoot string, expected CoreSnapshotExpectation) (CoreSnapshotManifest, error) {
	if core == nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	manifest, err := readCoreSnapshotManifest(artifactRoot)
	if err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := validateSnapshotExpectation(manifest, expected); err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := verifySnapshotArtifact(artifactRoot, manifest); err != nil {
		return CoreSnapshotManifest{}, err
	}
	checkpointRoot, err := os.MkdirTemp("", "badtower-core-snapshot-install-")
	if err != nil {
		return CoreSnapshotManifest{}, err
	}
	defer os.RemoveAll(checkpointRoot)
	reader := &snapshotChunkReader{root: artifactRoot, chunks: manifest.Chunks}
	if err := decodeCoreCheckpoint(reader, checkpointRoot, manifest.TotalBytes); err != nil {
		_ = reader.Close()
		return CoreSnapshotManifest{}, err
	}
	if err := reader.Close(); err != nil {
		return CoreSnapshotManifest{}, err
	}
	if err := core.InstallSnapshotCheckpoint(checkpointRoot); err != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	return manifest, nil
}

func validateSnapshotExpectation(manifest CoreSnapshotManifest, expected CoreSnapshotExpectation) error {
	if expected.ClusterID == "" || expected.GroupID == 0 || expected.SourceMemberID == 0 || expected.OwnershipEpoch == 0 || expected.Partition >= PartitionCount ||
		manifest.ClusterID != expected.ClusterID || manifest.GroupID != expected.GroupID || manifest.SourceMemberID != expected.SourceMemberID || manifest.Partition != expected.Partition || manifest.OwnershipEpoch != expected.OwnershipEpoch {
		return ErrInvalidSnapshot
	}
	if manifest.AppliedIndex <= expected.CurrentAppliedIndex {
		return ErrStaleSnapshot
	}
	return nil
}

func marshalCoreSnapshotArtifact(root string, maxBytes int) ([]byte, CoreSnapshotManifest, error) {
	manifest, err := readCoreSnapshotManifest(root)
	if err != nil || maxBytes <= len(embeddedCoreSnapshotMagic)+4 || maxBytes > maxPeerMessageBytes {
		return nil, CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if err := verifySnapshotArtifact(root, manifest); err != nil {
		return nil, CoreSnapshotManifest{}, err
	}
	rawManifest, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil || len(rawManifest) > maxCoreSnapshotManifest {
		return nil, CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	total := len(embeddedCoreSnapshotMagic) + 4 + len(rawManifest) + int(manifest.TotalBytes)
	if total > maxBytes || total < 0 {
		return nil, CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	payload := make([]byte, 0, total)
	payload = append(payload, embeddedCoreSnapshotMagic...)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(rawManifest)))
	payload = append(payload, length[:]...)
	payload = append(payload, rawManifest...)
	for _, chunk := range manifest.Chunks {
		raw, err := os.ReadFile(filepath.Join(root, snapshotChunkName(chunk.Index)))
		if err != nil || len(raw) != int(chunk.SizeBytes) {
			return nil, CoreSnapshotManifest{}, ErrInvalidSnapshot
		}
		payload = append(payload, raw...)
	}
	return payload, manifest, nil
}

func unmarshalCoreSnapshotArtifact(payload []byte, root string, maxBytes int) (CoreSnapshotManifest, error) {
	if len(payload) > maxBytes || len(payload) < len(embeddedCoreSnapshotMagic)+4 || !bytes.Equal(payload[:len(embeddedCoreSnapshotMagic)], embeddedCoreSnapshotMagic) {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	offset := len(embeddedCoreSnapshotMagic)
	manifestLength := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
	offset += 4
	if manifestLength <= 0 || manifestLength > maxCoreSnapshotManifest || offset+manifestLength > len(payload) {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	rawManifest := payload[offset : offset+manifestLength]
	decoder := json.NewDecoder(bytes.NewReader(rawManifest))
	decoder.DisallowUnknownFields()
	var manifest CoreSnapshotManifest
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateCoreSnapshotManifest(manifest) != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	offset += manifestLength
	if uint64(len(payload)-offset) != manifest.TotalBytes || requireAbsentAbsoluteRoot(root) != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return CoreSnapshotManifest{}, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(root)
		}
	}()
	if err := writeSnapshotFile(filepath.Join(root, "manifest.json"), rawManifest); err != nil {
		return CoreSnapshotManifest{}, err
	}
	for _, chunk := range manifest.Chunks {
		end := offset + int(chunk.SizeBytes)
		if end > len(payload) || writeSnapshotFile(filepath.Join(root, snapshotChunkName(chunk.Index)), payload[offset:end]) != nil {
			return CoreSnapshotManifest{}, ErrInvalidSnapshot
		}
		offset = end
	}
	if offset != len(payload) || verifySnapshotArtifact(root, manifest) != nil || syncSnapshotDir(root) != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	ok = true
	return manifest, nil
}

func validateSnapshotBinding(binding CoreSnapshotBinding) error {
	if !validSnapshotClusterID(binding.ClusterID) || binding.GroupID == 0 || binding.SourceMemberID == 0 || binding.Partition >= PartitionCount || binding.OwnershipEpoch == 0 || binding.AppliedIndex == 0 {
		return ErrInvalidSnapshot
	}
	return nil
}

func validSnapshotClusterID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if r < 0x21 || r > 0x7e {
			return false
		}
	}
	return true
}

func validateCoreSnapshotManifest(manifest CoreSnapshotManifest) error {
	if manifest.Format != CoreSnapshotFormat || manifest.CoreIdentity != stationcore.StoreIdentity ||
		validateSnapshotBinding(CoreSnapshotBinding{
			ClusterID: manifest.ClusterID, GroupID: manifest.GroupID, SourceMemberID: manifest.SourceMemberID,
			Partition: manifest.Partition, OwnershipEpoch: manifest.OwnershipEpoch, AppliedIndex: manifest.AppliedIndex,
		}) != nil ||
		manifest.TotalBytes == 0 || manifest.TotalBytes > MaxCoreSnapshotBytes || len(manifest.Chunks) == 0 || len(manifest.Chunks) > MaxCoreSnapshotChunks ||
		len(manifest.DigestSHA256) != sha256.Size*2 {
		return ErrInvalidSnapshot
	}
	var total uint64
	for index, chunk := range manifest.Chunks {
		if chunk.Index != uint32(index) || chunk.SizeBytes == 0 || chunk.SizeBytes > MaxCoreSnapshotChunkBytes || len(chunk.DigestSHA256) != sha256.Size*2 {
			return ErrInvalidSnapshot
		}
		total += uint64(chunk.SizeBytes)
		if total > MaxCoreSnapshotBytes {
			return ErrInvalidSnapshot
		}
		if _, err := hex.DecodeString(chunk.DigestSHA256); err != nil {
			return ErrInvalidSnapshot
		}
	}
	if total != manifest.TotalBytes {
		return ErrInvalidSnapshot
	}
	if _, err := hex.DecodeString(manifest.DigestSHA256); err != nil {
		return ErrInvalidSnapshot
	}
	return nil
}

func readCoreSnapshotManifest(root string) (CoreSnapshotManifest, error) {
	info, err := os.Lstat(filepath.Join(root, "manifest.json"))
	if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxCoreSnapshotManifest {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest CoreSnapshotManifest
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateCoreSnapshotManifest(manifest) != nil {
		return CoreSnapshotManifest{}, ErrInvalidSnapshot
	}
	return manifest, nil
}

func verifySnapshotArtifact(root string, manifest CoreSnapshotManifest) error {
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != len(manifest.Chunks)+1 {
		return ErrInvalidSnapshot
	}
	aggregate := sha256.New()
	for _, chunk := range manifest.Chunks {
		path := filepath.Join(root, snapshotChunkName(chunk.Index))
		info, statErr := os.Lstat(path)
		if statErr != nil || !info.Mode().IsRegular() {
			return ErrInvalidSnapshot
		}
		file, err := os.Open(path)
		if err != nil {
			return ErrInvalidSnapshot
		}
		chunkHash := sha256.New()
		read, copyErr := io.Copy(io.MultiWriter(aggregate, chunkHash), io.LimitReader(file, int64(chunk.SizeBytes)+1))
		closeErr := file.Close()
		if read != int64(chunk.SizeBytes) || copyErr != nil || closeErr != nil || hex.EncodeToString(chunkHash.Sum(nil)) != chunk.DigestSHA256 {
			return ErrInvalidSnapshot
		}
	}
	if hex.EncodeToString(aggregate.Sum(nil)) != manifest.DigestSHA256 {
		return ErrInvalidSnapshot
	}
	return nil
}

func encodeCoreCheckpoint(destination io.Writer, checkpointRoot string) error {
	var names []string
	err := filepath.WalkDir(checkpointRoot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return ErrInvalidSnapshot
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return ErrInvalidSnapshot
		}
		relative, err := filepath.Rel(checkpointRoot, path)
		if err != nil {
			return ErrInvalidSnapshot
		}
		names = append(names, filepath.ToSlash(relative))
		return nil
	})
	if err != nil || len(names) == 0 || len(names) > maxCoreSnapshotFiles {
		return ErrInvalidSnapshot
	}
	sort.Strings(names)
	if _, err := destination.Write(coreSnapshotArchiveMagic); err != nil {
		return err
	}
	if err := binary.Write(destination, binary.BigEndian, uint32(len(names))); err != nil {
		return err
	}
	var total uint64 = uint64(len(coreSnapshotArchiveMagic) + 4)
	for _, name := range names {
		path := filepath.Join(checkpointRoot, filepath.FromSlash(name))
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || len(name) == 0 || len(name) > maxCoreSnapshotNameBytes {
			return ErrInvalidSnapshot
		}
		size := uint64(info.Size())
		total += 2 + uint64(len(name)) + 8 + size
		if total > MaxCoreSnapshotBytes {
			return ErrInvalidSnapshot
		}
		if err := binary.Write(destination, binary.BigEndian, uint16(len(name))); err != nil {
			return err
		}
		if _, err := io.WriteString(destination, name); err != nil {
			return err
		}
		if err := binary.Write(destination, binary.BigEndian, size); err != nil {
			return err
		}
		file, err := os.Open(path)
		if err != nil {
			return err
		}
		copied, copyErr := io.CopyN(destination, file, int64(size))
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil || copied != int64(size) {
			return ErrInvalidSnapshot
		}
	}
	return nil
}

func decodeCoreCheckpoint(source io.Reader, checkpointRoot string, expectedBytes uint64) error {
	limited := &io.LimitedReader{R: source, N: int64(expectedBytes) + 1}
	reader := bufio.NewReader(limited)
	magic := make([]byte, len(coreSnapshotArchiveMagic))
	if _, err := io.ReadFull(reader, magic); err != nil || !bytes.Equal(magic, coreSnapshotArchiveMagic) {
		return ErrInvalidSnapshot
	}
	var count uint32
	if binary.Read(reader, binary.BigEndian, &count) != nil || count == 0 || count > maxCoreSnapshotFiles {
		return ErrInvalidSnapshot
	}
	seen := make(map[string]struct{}, count)
	for index := uint32(0); index < count; index++ {
		var nameLength uint16
		if binary.Read(reader, binary.BigEndian, &nameLength) != nil || nameLength == 0 || nameLength > maxCoreSnapshotNameBytes {
			return ErrInvalidSnapshot
		}
		nameBytes := make([]byte, nameLength)
		if _, err := io.ReadFull(reader, nameBytes); err != nil {
			return ErrInvalidSnapshot
		}
		name := string(nameBytes)
		clean := filepath.ToSlash(filepath.Clean(filepath.FromSlash(name)))
		if clean != name || filepath.IsAbs(name) || name == "." || name == ".." || strings.HasPrefix(name, "../") || strings.Contains(name, "\\") {
			return ErrInvalidSnapshot
		}
		if _, duplicate := seen[name]; duplicate {
			return ErrInvalidSnapshot
		}
		seen[name] = struct{}{}
		var size uint64
		if binary.Read(reader, binary.BigEndian, &size) != nil || size > MaxCoreSnapshotBytes {
			return ErrInvalidSnapshot
		}
		path := filepath.Join(checkpointRoot, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return ErrInvalidSnapshot
		}
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return ErrInvalidSnapshot
		}
		copied, copyErr := io.CopyN(file, reader, int64(size))
		syncErr := file.Sync()
		closeErr := file.Close()
		if copyErr != nil || syncErr != nil || closeErr != nil || copied != int64(size) {
			return ErrInvalidSnapshot
		}
	}
	if extra, err := reader.ReadByte(); err != io.EOF || extra != 0 {
		return ErrInvalidSnapshot
	}
	// The reader is capped at expectedBytes+1. Exactly one unread allowance
	// remains after consuming an artifact whose declared length is exact.
	if limited.N != 1 {
		return ErrInvalidSnapshot
	}
	return syncCheckpointDirectories(checkpointRoot)
}

func syncCheckpointDirectories(root string) error {
	var directories []string
	if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			directories = append(directories, path)
		}
		return nil
	}); err != nil {
		return ErrInvalidSnapshot
	}
	sort.Slice(directories, func(left, right int) bool {
		return strings.Count(directories[left], string(filepath.Separator)) > strings.Count(directories[right], string(filepath.Separator))
	})
	for _, directory := range directories {
		if err := syncSnapshotDir(directory); err != nil {
			return err
		}
	}
	return nil
}

type snapshotChunkWriter struct {
	root       string
	chunkBytes int
	maxBytes   uint64
	file       *os.File
	hash       hash.Hash
	aggregate  hash.Hash
	current    uint32
	total      uint64
	chunks     []CoreSnapshotChunk
}

func newSnapshotChunkWriter(root string, chunkBytes int, maxBytes uint64) *snapshotChunkWriter {
	return &snapshotChunkWriter{root: root, chunkBytes: chunkBytes, maxBytes: maxBytes, aggregate: sha256.New()}
}

func (writer *snapshotChunkWriter) Write(payload []byte) (int, error) {
	written := 0
	for len(payload) > 0 {
		if writer.file == nil {
			if len(writer.chunks) >= MaxCoreSnapshotChunks {
				return written, ErrInvalidSnapshot
			}
			file, err := os.OpenFile(filepath.Join(writer.root, snapshotChunkName(uint32(len(writer.chunks)))), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				return written, err
			}
			writer.file, writer.hash, writer.current = file, sha256.New(), 0
		}
		remaining := writer.chunkBytes - int(writer.current)
		part := payload
		if len(part) > remaining {
			part = part[:remaining]
		}
		n, err := writer.file.Write(part)
		if n > 0 {
			writer.hash.Write(part[:n])
			writer.aggregate.Write(part[:n])
			writer.current += uint32(n)
			writer.total += uint64(n)
			written += n
			payload = payload[n:]
		}
		if err != nil || writer.total > writer.maxBytes {
			if err == nil {
				err = ErrInvalidSnapshot
			}
			return written, err
		}
		if int(writer.current) == writer.chunkBytes {
			if err := writer.closeChunk(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

func (writer *snapshotChunkWriter) closeChunk() error {
	if writer.file == nil {
		return nil
	}
	if err := writer.file.Sync(); err != nil {
		_ = writer.file.Close()
		return err
	}
	if err := writer.file.Close(); err != nil {
		return err
	}
	writer.chunks = append(writer.chunks, CoreSnapshotChunk{
		Index: uint32(len(writer.chunks)), SizeBytes: writer.current,
		DigestSHA256: hex.EncodeToString(writer.hash.Sum(nil)),
	})
	writer.file, writer.hash, writer.current = nil, nil, 0
	return nil
}

func (writer *snapshotChunkWriter) finish() ([]CoreSnapshotChunk, uint64, string, error) {
	if err := writer.closeChunk(); err != nil {
		return nil, 0, "", err
	}
	if writer.total == 0 {
		return nil, 0, "", ErrInvalidSnapshot
	}
	return writer.chunks, writer.total, hex.EncodeToString(writer.aggregate.Sum(nil)), nil
}

func (writer *snapshotChunkWriter) Close() error { return writer.closeChunk() }

type snapshotChunkReader struct {
	root   string
	chunks []CoreSnapshotChunk
	index  int
	file   *os.File
}

func (reader *snapshotChunkReader) Read(payload []byte) (int, error) {
	for {
		if reader.file == nil {
			if reader.index >= len(reader.chunks) {
				return 0, io.EOF
			}
			file, err := os.Open(filepath.Join(reader.root, snapshotChunkName(uint32(reader.index))))
			if err != nil {
				return 0, err
			}
			reader.file = file
		}
		n, err := reader.file.Read(payload)
		if err == io.EOF {
			if closeErr := reader.file.Close(); closeErr != nil {
				return n, closeErr
			}
			reader.file = nil
			reader.index++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}

func (reader *snapshotChunkReader) Close() error {
	if reader.file == nil {
		return nil
	}
	err := reader.file.Close()
	reader.file = nil
	return err
}

func snapshotChunkName(index uint32) string { return fmt.Sprintf("chunk-%06d.bin", index) }

func requireAbsentAbsoluteRoot(root string) error {
	if root == "" || !filepath.IsAbs(root) {
		return ErrInvalidSnapshot
	}
	if _, err := os.Stat(root); err == nil {
		return ErrInvalidSnapshot
	} else if !os.IsNotExist(err) {
		return err
	}
	return nil
}

func snapshotRootsOverlap(left, right string) bool {
	leftAbs, leftErr := filepath.Abs(left)
	rightAbs, rightErr := filepath.Abs(right)
	if leftErr != nil || rightErr != nil {
		return true
	}
	for _, pair := range [][2]string{{leftAbs, rightAbs}, {rightAbs, leftAbs}} {
		rel, err := filepath.Rel(pair[0], pair[1])
		if err == nil && (rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))) {
			return true
		}
	}
	return false
}

func syncSnapshotDir(root string) error {
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func writeSnapshotManifest(path string, raw []byte) error {
	return writeSnapshotFile(path, raw)
}

func writeSnapshotFile(path string, raw []byte) error {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
