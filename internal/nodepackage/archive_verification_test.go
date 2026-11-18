package nodepackage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ed25519"
	"io"
	"strings"
	"testing"
	"time"
)

func TestVerifyRejectsNonCanonicalArchiveLayout(t *testing.T) {
	t.Parallel()
	headers := []*tar.Header{
		canonicalTestHeader(BinaryPath, 0o755, int64(len(fixtureBinary))),
		canonicalTestHeader(LicensePath, 0o644, int64(len(fixtureLicense))),
	}
	contents := [][]byte{fixtureBinary, fixtureLicense}

	cases := []struct {
		name    string
		archive func(*testing.T) []byte
		code    Code
		mutate  func(*manifest)
	}{
		{
			name: "extra member",
			archive: func(t *testing.T) []byte {
				extraHeaders := append([]*tar.Header(nil), headers...)
				extraHeaders = append(
					extraHeaders,
					canonicalTestHeader("unexpected", 0o644, 1),
				)
				extraContents := append([][]byte(nil), contents...)
				extraContents = append(extraContents, []byte("x"))
				return canonicalTestArchive(t, extraHeaders, extraContents, nil)
			},
			code: CodeArchiveLayoutInvalid,
		},
		{
			name: "path traversal",
			archive: func(t *testing.T) []byte {
				changed := cloneHeaders(headers)
				changed[0].Name = "../badtower"
				return canonicalTestArchive(t, changed, contents, nil)
			},
			code: CodeArchiveMetadataMismatch,
		},
		{
			name: "symlink",
			archive: func(t *testing.T) []byte {
				changed := cloneHeaders(headers)
				changed[0].Typeflag = tar.TypeSymlink
				changed[0].Linkname = "elsewhere"
				changed[0].Size = 0
				return canonicalTestArchive(
					t,
					changed,
					[][]byte{nil, fixtureLicense},
					nil,
				)
			},
			code: CodeArchiveMetadataMismatch,
		},
		{
			name: "wrong mode",
			archive: func(t *testing.T) []byte {
				changed := cloneHeaders(headers)
				changed[0].Mode = 0o777
				return canonicalTestArchive(t, changed, contents, nil)
			},
			code: CodeArchiveMetadataMismatch,
		},
		{
			name: "pax metadata",
			archive: func(t *testing.T) []byte {
				changed := cloneHeaders(headers)
				changed[0].Format = tar.FormatPAX
				changed[0].PAXRecords = map[string]string{
					"BADTOWER.synthetic": "rejected",
				}
				return canonicalTestArchive(t, changed, contents, nil)
			},
			code: CodeArchiveMetadataMismatch,
		},
		{
			name: "changed content",
			archive: func(t *testing.T) []byte {
				changed := append([]byte(nil), fixtureBinary...)
				changed[0] ^= 0xff
				return canonicalTestArchive(
					t,
					headers,
					[][]byte{changed, fixtureLicense},
					nil,
				)
			},
			code: CodeArchiveContentMismatch,
		},
		{
			name: "gzip name metadata",
			archive: func(t *testing.T) []byte {
				return canonicalTestArchive(
					t,
					headers,
					contents,
					func(writer *gzip.Writer) {
						writer.Header.Name = "noncanonical"
					},
				)
			},
			code: CodeArchiveFormatInvalid,
		},
		{
			name: "noncanonical gzip flags",
			archive: func(t *testing.T) []byte {
				archive := canonicalTestArchive(t, headers, contents, nil)
				archive[8] = 0
				return archive
			},
			code: CodeArchiveFormatInvalid,
		},
		{
			name: "archive size mismatch",
			archive: func(t *testing.T) []byte {
				return canonicalTestArchive(t, headers, contents, nil)
			},
			mutate: func(value *manifest) {
				value.Archive.Bytes++
			},
			code: CodeArchiveMetadataMismatch,
		},
		{
			name: "archive digest mismatch",
			archive: func(t *testing.T) []byte {
				return canonicalTestArchive(t, headers, contents, nil)
			},
			mutate: func(value *manifest) {
				value.Archive.SHA256 = strings.Repeat("0", 64)
			},
			code: CodeArchiveDigestMismatch,
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			archive := testCase.archive(t)
			manifestBytes, signature, publicKey := bindArchive(
				t,
				archive,
				testCase.mutate,
				DefaultLimits(),
			)
			_, err := Verify(VerifyInput{
				Archive:   bytes.NewReader(archive),
				Manifest:  manifestBytes,
				Signature: signature,
				PublicKey: publicKey,
			})
			requireCode(t, err, testCase.code)
		})
	}
}

func TestVerifyRejectsCompressedAndExpandedTrailingData(t *testing.T) {
	t.Parallel()
	validArchive, _ := createFixture(t)
	secondStream := gzipBytes(t, []byte("second stream"))
	compressedTrailing := append(append([]byte(nil), validArchive...), 0xff)
	concatenated := append(append([]byte(nil), validArchive...), secondStream...)
	expandedTrailing := archiveWithExpandedTrailing(t, []byte("unexpected"))

	for name, archive := range map[string][]byte{
		"compressed trailing byte": compressedTrailing,
		"concatenated gzip stream": concatenated,
		"expanded trailing bytes":  expandedTrailing,
	} {
		name := name
		archive := archive
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			manifestBytes, signature, publicKey := bindArchive(
				t,
				archive,
				nil,
				DefaultLimits(),
			)
			_, err := Verify(VerifyInput{
				Archive:   bytes.NewReader(archive),
				Manifest:  manifestBytes,
				Signature: signature,
				PublicKey: publicKey,
			})
			requireCode(t, err, CodeArchiveTrailingData)
		})
	}
}

func TestVerifyBoundsCompressedInput(t *testing.T) {
	t.Parallel()
	validArchive, result := createFixture(t)
	privateKey, publicKey := syntheticKey()
	signed, err := Sign(SignInput{
		Manifest:   result.Manifest,
		PrivateKey: privateKey,
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	limits := DefaultLimits()
	limits.MaxArchiveBytes = int64(len(validArchive))
	content := append(append([]byte(nil), validArchive...), bytes.Repeat([]byte{0xff}, 64<<10)...)
	counted := &countingReader{reader: bytes.NewReader(content)}
	_, err = Verify(VerifyInput{
		Archive:   counted,
		Manifest:  result.Manifest,
		Signature: signed.Signature,
		PublicKey: publicKey,
		Limits:    limits,
	})
	requireCode(t, err, CodeArchiveTooLarge)
	if counted.read > limits.MaxArchiveBytes+1 {
		t.Fatalf("archive verifier read %d bytes beyond limit %d", counted.read, limits.MaxArchiveBytes+1)
	}
}

func TestVerifyBoundsExpandedTarMetadata(t *testing.T) {
	t.Parallel()
	limits := DefaultLimits()
	limits.MaxArchiveBytes = 64 << 10
	limits.MaxBinaryBytes = 64
	limits.MaxLicenseBytes = 64

	archive := archiveWithExpandedTrailing(t, bytes.Repeat([]byte{0}, 32<<10))
	if int64(len(archive)) > limits.MaxArchiveBytes {
		t.Fatalf("compressed attack fixture exceeds compressed limit: %d", len(archive))
	}
	manifestBytes, signature, publicKey := bindArchive(t, archive, nil, limits)
	_, err := Verify(VerifyInput{
		Archive:   bytes.NewReader(archive),
		Manifest:  manifestBytes,
		Signature: signature,
		PublicKey: publicKey,
		Limits:    limits,
	})
	requireCode(t, err, CodeArchiveTooLarge)
}

func TestVerifyRejectsTruncatedGZIPTrailer(t *testing.T) {
	t.Parallel()
	archive, _ := createFixture(t)
	truncated := append([]byte(nil), archive[:len(archive)-4]...)
	manifestBytes, signature, publicKey := bindArchive(
		t,
		truncated,
		nil,
		DefaultLimits(),
	)
	_, err := Verify(VerifyInput{
		Archive:   bytes.NewReader(truncated),
		Manifest:  manifestBytes,
		Signature: signature,
		PublicKey: publicKey,
	})
	requireCode(t, err, CodeArchiveFormatInvalid)
}

func bindArchive(
	t *testing.T,
	archive []byte,
	mutate func(*manifest),
	limits Limits,
) ([]byte, []byte, ed25519.PublicKey) {
	t.Helper()
	_, base := createFixture(t)
	value, err := parseManifest(base.Manifest, DefaultLimits())
	if err != nil {
		t.Fatalf("parse base manifest: %v", err)
	}
	value.Archive.Bytes = int64(len(archive))
	value.Archive.SHA256 = sha256Hex(archive)
	if mutate != nil {
		mutate(&value)
	}
	manifestBytes, err := marshalManifest(value)
	if err != nil {
		t.Fatalf("marshal rebound manifest: %v", err)
	}
	privateKey, publicKey := syntheticKey()
	signed, err := Sign(SignInput{
		Manifest:   manifestBytes,
		PrivateKey: privateKey,
		Limits:     limits,
	})
	if err != nil {
		t.Fatalf("sign rebound manifest: %v", err)
	}
	return manifestBytes, signed.Signature, publicKey
}

func cloneHeaders(headers []*tar.Header) []*tar.Header {
	cloned := make([]*tar.Header, len(headers))
	for index, header := range headers {
		value := *header
		cloned[index] = &value
	}
	return cloned
}

func gzipBytes(t *testing.T, content []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := gzip.NewWriter(&output)
	writer.Header.ModTime = time.Time{}
	writer.Header.OS = 255
	if _, err := writer.Write(content); err != nil {
		t.Fatalf("write gzip content: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return output.Bytes()
}

func archiveWithExpandedTrailing(t *testing.T, trailing []byte) []byte {
	t.Helper()
	var output bytes.Buffer
	compressed, err := gzip.NewWriterLevel(&output, gzip.BestCompression)
	if err != nil {
		t.Fatalf("new gzip writer: %v", err)
	}
	compressed.Header.ModTime = time.Time{}
	compressed.Header.OS = 255
	archive := tar.NewWriter(compressed)
	headers := []*tar.Header{
		canonicalTestHeader(BinaryPath, 0o755, int64(len(fixtureBinary))),
		canonicalTestHeader(LicensePath, 0o644, int64(len(fixtureLicense))),
	}
	contents := [][]byte{fixtureBinary, fixtureLicense}
	for index, header := range headers {
		if err := archive.WriteHeader(header); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if _, err := archive.Write(contents[index]); err != nil {
			t.Fatalf("write tar content: %v", err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close tar: %v", err)
	}
	if _, err := compressed.Write(trailing); err != nil {
		t.Fatalf("write expanded trailing bytes: %v", err)
	}
	if err := compressed.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return output.Bytes()
}

type countingReader struct {
	reader io.Reader
	read   int64
}

func (reader *countingReader) Read(content []byte) (int, error) {
	count, err := reader.reader.Read(content)
	reader.read += int64(count)
	return count, err
}
