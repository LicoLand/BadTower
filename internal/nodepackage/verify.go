package nodepackage

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
)

// Verify authenticates a detached manifest before performing bounded,
// fail-closed inspection of the archive. It never extracts package content.
func Verify(input VerifyInput) (Receipt, error) {
	limits, err := normalizeLimits(input.Limits)
	if err != nil {
		return Receipt{}, err
	}
	if int64(len(input.Manifest)) > limits.MaxManifestBytes {
		return Receipt{}, NewFailure(CodeManifestTooLarge)
	}
	publicKey, err := validatePublicKey(input.PublicKey)
	if err != nil {
		return Receipt{}, err
	}
	document, signature, err := parseSignature(input.Signature, limits)
	if err != nil {
		return Receipt{}, err
	}
	keyID := publicKeyID(publicKey)
	if document.KeyID != keyID {
		return Receipt{}, NewFailure(CodeSignatureKeyMismatch)
	}
	if !ed25519.Verify(publicKey, signingMessage(input.Manifest), signature) {
		return Receipt{}, NewFailure(CodeSignatureVerificationFailed)
	}
	value, err := parseManifest(input.Manifest, limits)
	if err != nil {
		return Receipt{}, err
	}
	if input.Archive == nil {
		return Receipt{}, NewFailure(CodeArchiveFormatInvalid)
	}
	if err := verifyArchive(input.Archive, value, limits); err != nil {
		return Receipt{}, err
	}
	signatureReceipt := newSignatureReceipt(keyID, input.Signature)
	return receiptFor(
		"verify",
		"verified",
		value,
		input.Manifest,
		&signatureReceipt,
	), nil
}

func verifyArchive(source io.Reader, value manifest, limits Limits) error {
	compressedLimit := &io.LimitedReader{
		R: source,
		N: limits.MaxArchiveBytes + 1,
	}
	archiveHash := sha256.New()
	buffered := bufio.NewReaderSize(io.TeeReader(compressedLimit, archiveHash), 32<<10)
	headerBytes, err := buffered.Peek(10)
	if err != nil || !bytes.Equal(headerBytes, canonicalGZIPHeaderBytes) {
		return NewFailure(CodeArchiveFormatInvalid)
	}
	compressed, err := gzip.NewReader(buffered)
	if err != nil {
		return NewFailure(CodeArchiveFormatInvalid)
	}
	compressed.Multistream(false)
	if !canonicalGZIPHeader(compressed.Header) {
		_ = compressed.Close()
		return NewFailure(CodeArchiveFormatInvalid)
	}

	expandedMaximum := limits.MaxBinaryBytes + limits.MaxLicenseBytes + tarEnvelopeBytes
	expanded := &io.LimitedReader{R: compressed, N: expandedMaximum + 1}
	for index, expected := range value.Entries {
		var actualHeader [tarBlockSize]byte
		if _, err := io.ReadFull(expanded, actualHeader[:]); err != nil {
			_ = compressed.Close()
			if expanded.N == 0 {
				return NewFailure(CodeArchiveTooLarge)
			}
			return NewFailure(CodeArchiveLayoutInvalid)
		}
		maximum := limits.MaxLicenseBytes
		if index == 0 {
			maximum = limits.MaxBinaryBytes
		}
		if expected.Bytes > maximum {
			_ = compressed.Close()
			return NewFailure(CodeArchiveEntryTooLarge)
		}
		expectedHeader, err := expectedTarHeader(expected)
		if err != nil {
			_ = compressed.Close()
			return err
		}
		if !bytes.Equal(actualHeader[:], expectedHeader) {
			_ = compressed.Close()
			return NewFailure(CodeArchiveMetadataMismatch)
		}
		digest := sha256.New()
		written, err := io.CopyN(digest, expanded, expected.Bytes)
		if err != nil || written != expected.Bytes {
			_ = compressed.Close()
			if expanded.N == 0 {
				return NewFailure(CodeArchiveTooLarge)
			}
			return NewFailure(CodeArchiveContentMismatch)
		}
		if hex.EncodeToString(digest.Sum(nil)) != expected.SHA256 {
			_ = compressed.Close()
			return NewFailure(CodeArchiveContentMismatch)
		}
		padding := tarPadding(expected.Bytes)
		var paddingBytes [tarBlockSize]byte
		if _, err := io.ReadFull(expanded, paddingBytes[:padding]); err != nil {
			_ = compressed.Close()
			if expanded.N == 0 {
				return NewFailure(CodeArchiveTooLarge)
			}
			return NewFailure(CodeArchiveLayoutInvalid)
		}
		if !allZero(paddingBytes[:padding]) {
			_ = compressed.Close()
			return NewFailure(CodeArchiveLayoutInvalid)
		}
	}

	var endBlocks [tarBlockSize * 2]byte
	if _, err := io.ReadFull(expanded, endBlocks[:]); err != nil {
		_ = compressed.Close()
		if expanded.N == 0 {
			return NewFailure(CodeArchiveTooLarge)
		}
		return NewFailure(CodeArchiveLayoutInvalid)
	}
	if !allZero(endBlocks[:]) {
		_ = compressed.Close()
		return NewFailure(CodeArchiveLayoutInvalid)
	}
	trailingExpanded, err := io.Copy(io.Discard, expanded)
	if err != nil {
		_ = compressed.Close()
		return NewFailure(CodeArchiveFormatInvalid)
	}
	if expanded.N == 0 {
		_ = compressed.Close()
		return NewFailure(CodeArchiveTooLarge)
	}
	if trailingExpanded != 0 {
		_ = compressed.Close()
		return NewFailure(CodeArchiveTrailingData)
	}
	if err := compressed.Close(); err != nil {
		return NewFailure(CodeArchiveFormatInvalid)
	}

	trailingCompressed := false
	if _, err := buffered.ReadByte(); err == nil {
		trailingCompressed = true
	} else if !errors.Is(err, io.EOF) {
		return NewFailure(CodeArchiveFormatInvalid)
	}
	consumed := limits.MaxArchiveBytes + 1 - compressedLimit.N
	if consumed > limits.MaxArchiveBytes {
		return NewFailure(CodeArchiveTooLarge)
	}
	if trailingCompressed {
		return NewFailure(CodeArchiveTrailingData)
	}
	if consumed != value.Archive.Bytes {
		return NewFailure(CodeArchiveMetadataMismatch)
	}
	if hex.EncodeToString(archiveHash.Sum(nil)) != value.Archive.SHA256 {
		return NewFailure(CodeArchiveDigestMismatch)
	}
	return nil
}

const tarBlockSize = 512

var canonicalGZIPHeaderBytes = []byte{
	0x1f, 0x8b, 0x08, 0x00,
	0x00, 0x00, 0x00, 0x00,
	0x02, 0xff,
}

func canonicalGZIPHeader(header gzip.Header) bool {
	return header.Name == "" && header.Comment == "" && len(header.Extra) == 0 &&
		header.ModTime.IsZero() && header.OS == 255
}

func expectedTarHeader(expected manifestEntry) ([]byte, error) {
	mode := int64(0o644)
	if expected.Path == BinaryPath {
		mode = 0o755
	}
	var encoded bytes.Buffer
	writer := tar.NewWriter(&encoded)
	if err := writer.WriteHeader(canonicalTarHeader(expected.Path, mode, expected.Bytes)); err != nil {
		return nil, NewFailure(CodeInternal)
	}
	if encoded.Len() != tarBlockSize {
		return nil, NewFailure(CodeInternal)
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
