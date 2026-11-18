package nodepackage

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"io"
	"time"
)

var archiveEpoch = time.Unix(0, 0).UTC()

// Create writes one canonical tar.gz stream and returns its detached canonical
// manifest. The archive contains exactly bin/badtower and LICENSE.
func Create(output io.Writer, input CreateInput) (CreateResult, error) {
	limits, err := normalizeLimits(input.Limits)
	if err != nil {
		return CreateResult{}, err
	}
	if !validVersion(input.Version) {
		return CreateResult{}, NewFailure(CodeVersionInvalid)
	}
	if !validPlatform(input.Platform) {
		return CreateResult{}, NewFailure(CodePlatformInvalid)
	}
	if input.Binary.Reader == nil || input.Binary.Size <= 0 ||
		input.Binary.Size > limits.MaxBinaryBytes {
		return CreateResult{}, NewFailure(CodeBinarySizeInvalid)
	}
	if input.License.Reader == nil || input.License.Size <= 0 ||
		input.License.Size > limits.MaxLicenseBytes {
		return CreateResult{}, NewFailure(CodeLicenseSizeInvalid)
	}
	if output == nil {
		return CreateResult{}, NewFailure(CodeArchiveWriteFailed)
	}

	archiveHash := sha256.New()
	bounded := &boundedWriter{writer: output, remaining: limits.MaxArchiveBytes}
	counted := &countingDigestWriter{writer: bounded, digest: archiveHash}
	compressed, err := gzip.NewWriterLevel(counted, gzip.BestCompression)
	if err != nil {
		return CreateResult{}, archiveOutputFailure(err)
	}
	compressed.Header.ModTime = time.Time{}
	compressed.Header.OS = 255
	archive := tar.NewWriter(compressed)

	entries := make([]manifestEntry, 0, 2)
	binaryEntry, err := writeArchiveEntry(
		archive,
		BinaryPath,
		0o755,
		input.Binary,
		CodeBinarySizeInvalid,
	)
	if err != nil {
		return CreateResult{}, err
	}
	entries = append(entries, binaryEntry)

	licenseEntry, err := writeArchiveEntry(
		archive,
		LicensePath,
		0o644,
		input.License,
		CodeLicenseSizeInvalid,
	)
	if err != nil {
		return CreateResult{}, err
	}
	entries = append(entries, licenseEntry)

	if err := archive.Close(); err != nil {
		return CreateResult{}, archiveOutputFailure(err)
	}
	if err := compressed.Close(); err != nil {
		return CreateResult{}, archiveOutputFailure(err)
	}

	value := manifest{
		Schema:             Schema,
		MediaType:          PackageMediaType,
		Name:               PackageName,
		Version:            input.Version,
		Platform:           input.Platform,
		StationStoreFormat: StationStoreFormat,
		Archive: manifestArchive{
			Format: ArchiveFormat,
			Bytes:  counted.written,
			SHA256: hex.EncodeToString(archiveHash.Sum(nil)),
		},
		Entries: entries,
	}
	manifestBytes, err := marshalManifest(value)
	if err != nil {
		return CreateResult{}, err
	}
	if int64(len(manifestBytes)) > limits.MaxManifestBytes {
		return CreateResult{}, NewFailure(CodeManifestTooLarge)
	}
	return CreateResult{
		Manifest: manifestBytes,
		Receipt:  receiptFor("create", "created", value, manifestBytes, nil),
	}, nil
}

func writeArchiveEntry(
	archive *tar.Writer,
	path string,
	mode int64,
	source Source,
	sizeCode Code,
) (manifestEntry, error) {
	header := canonicalTarHeader(path, mode, source.Size)
	if err := archive.WriteHeader(header); err != nil {
		return manifestEntry{}, archiveOutputFailure(err)
	}

	contentHash := sha256.New()
	writer := &recordingWriter{writer: io.MultiWriter(archive, contentHash)}
	written, err := io.Copy(writer, io.LimitReader(source.Reader, source.Size))
	if err != nil {
		if writer.failed {
			return manifestEntry{}, archiveOutputFailure(err)
		}
		return manifestEntry{}, NewFailure(CodeSourceReadFailed)
	}
	if written != source.Size {
		return manifestEntry{}, NewFailure(sizeCode)
	}
	extra, err := readOne(source.Reader)
	if err != nil {
		return manifestEntry{}, NewFailure(CodeSourceReadFailed)
	}
	if extra {
		return manifestEntry{}, NewFailure(sizeCode)
	}
	return manifestEntry{
		Path:   path,
		Mode:   formatMode(mode),
		Bytes:  source.Size,
		SHA256: hex.EncodeToString(contentHash.Sum(nil)),
	}, nil
}

func canonicalTarHeader(path string, mode, size int64) *tar.Header {
	return &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     path,
		Size:     size,
		Mode:     mode,
		Uid:      0,
		Gid:      0,
		ModTime:  archiveEpoch,
		Format:   tar.FormatUSTAR,
	}
}

func readOne(reader io.Reader) (bool, error) {
	var buffer [1]byte
	for emptyReads := 0; emptyReads < 100; emptyReads++ {
		count, err := reader.Read(buffer[:])
		if count > 0 {
			return true, nil
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return false, nil
			}
			return false, err
		}
	}
	return false, io.ErrNoProgress
}

func formatMode(mode int64) string {
	switch mode {
	case 0o755:
		return "0755"
	case 0o644:
		return "0644"
	default:
		return ""
	}
}

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

func (writer *boundedWriter) Write(content []byte) (int, error) {
	if len(content) == 0 {
		return 0, nil
	}
	if writer.remaining <= 0 {
		return 0, NewFailure(CodeArchiveTooLarge)
	}
	exceeded := int64(len(content)) > writer.remaining
	if int64(len(content)) > writer.remaining {
		content = content[:writer.remaining]
	}
	count, err := writer.writer.Write(content)
	writer.remaining -= int64(count)
	if err != nil {
		return count, err
	}
	if count != len(content) {
		return count, io.ErrShortWrite
	}
	if exceeded {
		return count, NewFailure(CodeArchiveTooLarge)
	}
	return count, nil
}

type countingDigestWriter struct {
	writer  io.Writer
	digest  hash.Hash
	written int64
}

func (writer *countingDigestWriter) Write(content []byte) (int, error) {
	count, err := writer.writer.Write(content)
	if count > 0 {
		_, _ = writer.digest.Write(content[:count])
		writer.written += int64(count)
	}
	return count, err
}

type recordingWriter struct {
	writer io.Writer
	failed bool
}

func (writer *recordingWriter) Write(content []byte) (int, error) {
	count, err := writer.writer.Write(content)
	if err != nil {
		writer.failed = true
	}
	return count, err
}

func sha256Hex(content []byte) string {
	digest := sha256.Sum256(content)
	return hex.EncodeToString(digest[:])
}

func archiveOutputFailure(err error) error {
	var failure *Failure
	if errors.As(err, &failure) && failure.Code == CodeArchiveTooLarge {
		return NewFailure(CodeArchiveTooLarge)
	}
	return NewFailure(CodeArchiveWriteFailed)
}
