package stationcluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	streamSnapshotFormat           = "badtower.station-cluster.snapshot-stream.v2"
	streamSnapshotPathPrefix       = "/badtower.cluster.v1/snapshot/"
	streamSnapshotContentType      = "application/vnd.badtower.snapshot-stream.v2"
	streamSnapshotChunkBytes       = 64 << 10
	streamSnapshotFrameHeaderBytes = 48
	maxStreamSnapshotEnvelopeBytes = 1 << 20
	maxStreamSnapshotConcurrent    = 2
)

var streamSnapshotMagic = []byte("BTSTRM02")

type streamSnapshotReference struct {
	Format         string `json:"format"`
	ClusterID      string `json:"clusterId"`
	GroupID        uint64 `json:"groupId"`
	SourceMemberID uint64 `json:"sourceMemberId"`
	Partition      uint16 `json:"partition"`
	OwnershipEpoch uint64 `json:"ownershipEpoch"`
	AppliedIndex   uint64 `json:"appliedIndex"`
	TotalBytes     uint64 `json:"totalBytes"`
	DigestSHA256   string `json:"digestSha256"`
}

type streamSnapshotEnvelope struct {
	Format    string                  `json:"format"`
	Reference streamSnapshotReference `json:"reference"`
	Manifest  CoreSnapshotManifest    `json:"manifest"`
}

type streamSnapshotStore struct {
	root      string
	admission chan struct{}
	mu        sync.Mutex
	incoming  map[uint64]uint8
}

type snapshotDeadlineReader struct {
	reader     io.Reader
	controller *http.ResponseController
}

type snapshotDeadlineWriter struct {
	writer     io.Writer
	controller *http.ResponseController
}

func (writer *snapshotDeadlineWriter) Write(buffer []byte) (int, error) {
	if writer.controller != nil {
		_ = writer.controller.SetWriteDeadline(time.Now().Add(5 * time.Second))
	}
	return writer.writer.Write(buffer)
}

func (reader *snapshotDeadlineReader) Read(buffer []byte) (int, error) {
	if reader.controller != nil {
		_ = reader.controller.SetReadDeadline(time.Now().Add(5 * time.Second))
	}
	return reader.reader.Read(buffer)
}

func openStreamSnapshotStore(root string) (*streamSnapshotStore, error) {
	if !filepath.IsAbs(root) {
		return nil, ErrInvalidSnapshot
	}
	root = filepath.Join(root, "snapshot-stream-v1")
	if err := os.MkdirAll(filepath.Join(root, "source"), 0o700); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "received"), 0o700); err != nil {
		return nil, err
	}
	for _, kind := range []string{"source", "received"} {
		entries, readErr := os.ReadDir(filepath.Join(root, kind))
		if readErr != nil {
			return nil, readErr
		}
		for _, entry := range entries {
			if entry.IsDir() && (strings.HasPrefix(entry.Name(), ".building-") || strings.HasPrefix(entry.Name(), ".incoming-")) {
				_ = os.RemoveAll(filepath.Join(root, kind, entry.Name()))
			}
		}
	}
	return &streamSnapshotStore{
		root:      root,
		admission: make(chan struct{}, maxStreamSnapshotConcurrent),
		incoming:  make(map[uint64]uint8),
	}, nil
}

func (store *streamSnapshotStore) keyRoot(kind string, ref streamSnapshotReference) string {
	return filepath.Join(store.root, kind, fmt.Sprintf("g%03d-s%020d-i%020d", ref.GroupID, ref.SourceMemberID, ref.AppliedIndex))
}
func (store *streamSnapshotStore) sourceRoot(ref streamSnapshotReference) string {
	return store.keyRoot("source", ref)
}
func (store *streamSnapshotStore) receivedRoot(ref streamSnapshotReference) string {
	return store.keyRoot("received", ref)
}

func encodeStreamSnapshotReference(ref streamSnapshotReference) ([]byte, error) {
	if validateStreamSnapshotReference(ref) != nil {
		return nil, ErrInvalidSnapshot
	}
	raw, err := json.Marshal(ref)
	if err != nil || len(raw) > 4096 {
		return nil, ErrInvalidSnapshot
	}
	result := append([]byte{}, streamSnapshotMagic...)
	result = append(result, raw...)
	return result, nil
}
func decodeStreamSnapshotReference(raw []byte) (streamSnapshotReference, bool, error) {
	if !bytes.HasPrefix(raw, streamSnapshotMagic) {
		return streamSnapshotReference{}, false, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw[len(streamSnapshotMagic):]))
	decoder.DisallowUnknownFields()
	var ref streamSnapshotReference
	if decoder.Decode(&ref) != nil || decoder.Decode(&struct{}{}) != io.EOF || validateStreamSnapshotReference(ref) != nil {
		return streamSnapshotReference{}, true, ErrInvalidSnapshot
	}
	return ref, true, nil
}
func validateStreamSnapshotReference(ref streamSnapshotReference) error {
	if ref.Format != streamSnapshotFormat || !validSnapshotClusterID(ref.ClusterID) || ref.GroupID == 0 || ref.GroupID > PartitionCount || ref.SourceMemberID == 0 || ref.Partition >= PartitionCount || ref.GroupID != uint64(ref.Partition)+1 || ref.OwnershipEpoch == 0 || ref.AppliedIndex == 0 || ref.TotalBytes == 0 || ref.TotalBytes > MaxCoreSnapshotBytes {
		return ErrInvalidSnapshot
	}
	digest, err := hex.DecodeString(ref.DigestSHA256)
	if err != nil || len(digest) != sha256.Size {
		return ErrInvalidSnapshot
	}
	return nil
}
func referenceForManifest(manifest CoreSnapshotManifest) streamSnapshotReference {
	return streamSnapshotReference{Format: streamSnapshotFormat, ClusterID: manifest.ClusterID, GroupID: manifest.GroupID, SourceMemberID: manifest.SourceMemberID, Partition: manifest.Partition, OwnershipEpoch: manifest.OwnershipEpoch, AppliedIndex: manifest.AppliedIndex, TotalBytes: manifest.TotalBytes, DigestSHA256: manifest.DigestSHA256}
}

func (store *streamSnapshotStore) artifact(ref streamSnapshotReference, incoming bool) (string, error) {
	if validateStreamSnapshotReference(ref) != nil {
		return "", ErrInvalidSnapshot
	}
	root := store.sourceRoot(ref)
	if incoming {
		root = store.receivedRoot(ref)
	}
	manifest, err := readCoreSnapshotManifest(root)
	if err != nil || referenceForManifest(manifest) != ref || verifySnapshotArtifact(root, manifest) != nil {
		return "", ErrInvalidSnapshot
	}
	return root, nil
}

func (store *streamSnapshotStore) prune(ref streamSnapshotReference, incoming bool) {
	store.mu.Lock()
	defer store.mu.Unlock()
	kind := "source"
	if incoming {
		kind = "received"
	}
	parent := filepath.Join(store.root, kind)
	entries, err := os.ReadDir(parent)
	if err != nil {
		return
	}
	keep := filepath.Base(store.keyRoot(kind, ref))
	prefix := fmt.Sprintf("g%03d-", ref.GroupID)
	for _, entry := range entries {
		if entry.IsDir() && entry.Name() != keep && strings.HasPrefix(entry.Name(), prefix) {
			_ = os.RemoveAll(filepath.Join(parent, entry.Name()))
		}
	}
}

func (store *streamSnapshotStore) writeTo(ctx context.Context, writer io.Writer, ref streamSnapshotReference) error {
	select {
	case store.admission <- struct{}{}:
		defer func() { <-store.admission }()
	case <-ctx.Done():
		return ctx.Err()
	default:
		return ErrPeerBackpressure
	}
	root, err := store.artifact(ref, false)
	if err != nil {
		return err
	}
	manifest, err := readCoreSnapshotManifest(root)
	if err != nil {
		return err
	}
	envelopeRaw, err := json.Marshal(streamSnapshotEnvelope{Format: streamSnapshotFormat, Reference: ref, Manifest: manifest})
	if err != nil || len(envelopeRaw) > maxStreamSnapshotEnvelopeBytes {
		return ErrInvalidSnapshot
	}
	buffered := bufio.NewWriterSize(writer, streamSnapshotChunkBytes)
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(envelopeRaw)))
	if _, err = buffered.Write(length[:]); err != nil {
		return err
	}
	if _, err = buffered.Write(envelopeRaw); err != nil {
		return err
	}
	data := make([]byte, streamSnapshotChunkBytes)
	var header [streamSnapshotFrameHeaderBytes]byte
	for _, chunk := range manifest.Chunks {
		file, openErr := os.Open(filepath.Join(root, snapshotChunkName(chunk.Index)))
		if openErr != nil {
			return openErr
		}
		offset := uint64(0)
		for offset < uint64(chunk.SizeBytes) {
			want := streamSnapshotChunkBytes
			if remaining := int(uint64(chunk.SizeBytes) - offset); remaining < want {
				want = remaining
			}
			n, readErr := io.ReadFull(file, data[:want])
			if readErr != nil {
				_ = file.Close()
				return readErr
			}
			digest := sha256.Sum256(data[:n])
			binary.BigEndian.PutUint32(header[0:4], chunk.Index)
			binary.BigEndian.PutUint64(header[4:12], offset)
			binary.BigEndian.PutUint32(header[12:16], uint32(n))
			copy(header[16:], digest[:])
			if _, err = buffered.Write(header[:]); err != nil {
				_ = file.Close()
				return err
			}
			if _, err = buffered.Write(data[:n]); err != nil {
				_ = file.Close()
				return err
			}
			offset += uint64(n)
		}
		if extra, readErr := file.Read(data[:1]); readErr != io.EOF || extra != 0 {
			_ = file.Close()
			return ErrInvalidSnapshot
		}
		if err = file.Close(); err != nil {
			return err
		}
	}
	return buffered.Flush()
}

type streamSnapshotReceiveExpectation struct {
	ClusterID      string
	GroupID        uint64
	Partition      uint16
	SourceMemberID uint64
	AppliedIndex   uint64
	OwnershipEpoch uint64
}

func (expected streamSnapshotReceiveExpectation) valid() bool {
	return expected.ClusterID != "" &&
		expected.GroupID > 0 &&
		expected.GroupID <= PartitionCount &&
		uint64(expected.Partition)+1 == expected.GroupID &&
		expected.SourceMemberID != 0 &&
		expected.AppliedIndex != 0
}

func (store *streamSnapshotStore) receive(ctx context.Context, reader io.Reader, expected streamSnapshotReceiveExpectation) (streamSnapshotReference, error) {
	if !expected.valid() {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	select {
	case store.admission <- struct{}{}:
		defer func() { <-store.admission }()
	case <-ctx.Done():
		return streamSnapshotReference{}, ctx.Err()
	default:
		return streamSnapshotReference{}, ErrPeerBackpressure
	}
	buffered := bufio.NewReaderSize(io.LimitReader(reader, MaxCoreSnapshotBytes+maxStreamSnapshotEnvelopeBytes+1<<20), streamSnapshotChunkBytes)
	var length [4]byte
	if _, err := io.ReadFull(buffered, length[:]); err != nil {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	size := binary.BigEndian.Uint32(length[:])
	if size == 0 || size > maxStreamSnapshotEnvelopeBytes {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	raw := make([]byte, size)
	if _, err := io.ReadFull(buffered, raw); err != nil {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var envelope streamSnapshotEnvelope
	if decoder.Decode(&envelope) != nil || decoder.Decode(&struct{}{}) != io.EOF || envelope.Format != streamSnapshotFormat || referenceForManifest(envelope.Manifest) != envelope.Reference || validateCoreSnapshotManifest(envelope.Manifest) != nil {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	ref := envelope.Reference
	if ref.ClusterID != expected.ClusterID ||
		ref.GroupID != expected.GroupID ||
		ref.Partition != expected.Partition ||
		ref.SourceMemberID != expected.SourceMemberID ||
		ref.AppliedIndex != expected.AppliedIndex ||
		(expected.OwnershipEpoch != 0 && ref.OwnershipEpoch != expected.OwnershipEpoch) {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	final := store.receivedRoot(ref)
	if _, err := os.Stat(final); err == nil {
		if _, artifactErr := store.artifact(ref, true); artifactErr != nil {
			return streamSnapshotReference{}, artifactErr
		}
		return ref, nil
	}
	store.mu.Lock()
	prefix := fmt.Sprintf("g%03d-", ref.GroupID)
	persisted := 0
	if entries, readErr := os.ReadDir(filepath.Join(store.root, "received")); readErr == nil {
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
				persisted++
			}
		}
	}
	if persisted >= 2 || store.incoming[ref.GroupID] > 0 {
		store.mu.Unlock()
		return streamSnapshotReference{}, ErrPeerBackpressure
	}
	store.incoming[ref.GroupID]++
	store.mu.Unlock()
	defer func() {
		store.mu.Lock()
		store.incoming[ref.GroupID]--
		store.mu.Unlock()
	}()
	parent := filepath.Dir(final)
	staging, err := os.MkdirTemp(parent, ".incoming-")
	if err != nil {
		return streamSnapshotReference{}, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(staging)
		}
	}()
	data := make([]byte, streamSnapshotChunkBytes)
	var header [streamSnapshotFrameHeaderBytes]byte
	for _, chunk := range envelope.Manifest.Chunks {
		if err = receiveStreamSnapshotChunk(buffered, staging, chunk, data, header[:]); err != nil {
			return streamSnapshotReference{}, err
		}
	}
	if extra, readErr := buffered.ReadByte(); readErr != io.EOF || extra != 0 {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	manifestRaw, err := json.Marshal(envelope.Manifest)
	if err != nil || writeSnapshotManifest(filepath.Join(staging, "manifest.json"), manifestRaw) != nil || verifySnapshotArtifact(staging, envelope.Manifest) != nil || syncSnapshotDir(staging) != nil {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	if err = os.Rename(staging, final); err != nil {
		return streamSnapshotReference{}, err
	}
	if syncSnapshotDir(parent) != nil {
		return streamSnapshotReference{}, ErrInvalidSnapshot
	}
	complete = true
	return ref, nil
}

func receiveStreamSnapshotChunk(reader io.Reader, staging string, chunk CoreSnapshotChunk, data, header []byte) (resultErr error) {
	file, err := os.OpenFile(filepath.Join(staging, snapshotChunkName(chunk.Index)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := file.Close(); resultErr == nil && closeErr != nil {
			resultErr = closeErr
		}
	}()
	for offset := uint64(0); offset < uint64(chunk.SizeBytes); {
		if _, err = io.ReadFull(reader, header); err != nil {
			return ErrInvalidSnapshot
		}
		frameIndex := binary.BigEndian.Uint32(header[0:4])
		frameOffset := binary.BigEndian.Uint64(header[4:12])
		frameSize := binary.BigEndian.Uint32(header[12:16])
		if frameIndex != chunk.Index || frameOffset != offset || frameSize == 0 || frameSize > streamSnapshotChunkBytes || uint64(frameSize) > uint64(chunk.SizeBytes)-offset {
			return ErrInvalidSnapshot
		}
		if _, err = io.ReadFull(reader, data[:frameSize]); err != nil {
			return ErrInvalidSnapshot
		}
		digest := sha256.Sum256(data[:frameSize])
		if !bytes.Equal(digest[:], header[16:]) {
			return ErrInvalidSnapshot
		}
		if _, err = file.Write(data[:frameSize]); err != nil {
			return err
		}
		offset += uint64(frameSize)
	}
	return file.Sync()
}

func streamSnapshotURL(target *url.URL, ref streamSnapshotReference) *url.URL {
	endpoint := *target
	endpoint.Path = streamSnapshotPathPrefix + strconv.FormatUint(ref.GroupID, 10) + "/" + strconv.FormatUint(ref.AppliedIndex, 10)
	return &endpoint
}

func (client *nodePeerClient) sendStreamSnapshot(ctx context.Context, targetMemberID uint64, ref streamSnapshotReference) error {
	if client.snapshotStore == nil {
		return ErrInvalidSnapshot
	}
	target := client.targets[targetMemberID]
	if target == nil {
		return ErrMemberUnknown
	}
	pipeReader, pipeWriter := io.Pipe()
	writeDone := make(chan error, 1)
	go func() {
		err := client.snapshotStore.writeTo(ctx, pipeWriter, ref)
		_ = pipeWriter.CloseWithError(err)
		writeDone <- err
	}()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, streamSnapshotURL(target, ref).String(), pipeReader)
	if err != nil {
		_ = pipeReader.Close()
		return err
	}
	request.Header.Set("Content-Type", streamSnapshotContentType)
	response, err := client.do(targetMemberID, request)
	if err != nil {
		_ = pipeReader.Close()
		<-writeDone
		return ErrMemberStopped
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1024))
	writeErr := <-writeDone
	if response.StatusCode == http.StatusNoContent {
		return nil
	}
	if writeErr != nil {
		return writeErr
	}
	return ErrMemberStopped
}
