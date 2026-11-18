package nodepackage

import "io"

const (
	// Schema is the strict node package manifest identifier.
	Schema = "badtower.node-package.v1"

	SignatureSchema    = "badtower.node-package.signature.v1"
	ReceiptSchema      = "badtower.node-package.receipt.v1"
	PackageMediaType   = "application/vnd.licoland.badtower.node-package.v1+tar+gzip"
	StationStoreFormat = "badtower.station-core.store.v2"
	PackageName        = "badtower"
	ArchiveFormat      = "tar+gzip"
	SignatureAlgorithm = "Ed25519"
	BinaryPath         = "bin/badtower"
	LicensePath        = "LICENSE"
)

const (
	hardMaxArchiveBytes   int64 = 160 << 20
	hardMaxBinaryBytes    int64 = 128 << 20
	hardMaxLicenseBytes   int64 = 1 << 20
	hardMaxManifestBytes  int64 = 64 << 10
	hardMaxSignatureBytes int64 = 4 << 10
	hardMaxKeyBytes       int64 = 16 << 10
	tarEnvelopeBytes      int64 = 8 << 10
)

// Limits bounds every attacker-controlled representation used by this
// package. The zero value selects DefaultLimits. A non-zero value must provide
// every field and may only reduce the hard ceilings.
type Limits struct {
	MaxArchiveBytes   int64
	MaxBinaryBytes    int64
	MaxLicenseBytes   int64
	MaxManifestBytes  int64
	MaxSignatureBytes int64
	MaxKeyBytes       int64
}

// DefaultLimits returns the fixed public verification ceilings.
func DefaultLimits() Limits {
	return Limits{
		MaxArchiveBytes:   hardMaxArchiveBytes,
		MaxBinaryBytes:    hardMaxBinaryBytes,
		MaxLicenseBytes:   hardMaxLicenseBytes,
		MaxManifestBytes:  hardMaxManifestBytes,
		MaxSignatureBytes: hardMaxSignatureBytes,
		MaxKeyBytes:       hardMaxKeyBytes,
	}
}

func normalizeLimits(limits Limits) (Limits, error) {
	if limits == (Limits{}) {
		return DefaultLimits(), nil
	}
	hard := DefaultLimits()
	if limits.MaxArchiveBytes <= 0 || limits.MaxArchiveBytes > hard.MaxArchiveBytes ||
		limits.MaxBinaryBytes <= 0 || limits.MaxBinaryBytes > hard.MaxBinaryBytes ||
		limits.MaxLicenseBytes <= 0 || limits.MaxLicenseBytes > hard.MaxLicenseBytes ||
		limits.MaxManifestBytes <= 0 || limits.MaxManifestBytes > hard.MaxManifestBytes ||
		limits.MaxSignatureBytes <= 0 || limits.MaxSignatureBytes > hard.MaxSignatureBytes ||
		limits.MaxKeyBytes <= 0 || limits.MaxKeyBytes > hard.MaxKeyBytes {
		return Limits{}, NewFailure(CodeLimitsInvalid)
	}
	return limits, nil
}

// Source supplies one package member with an explicit byte size.
type Source struct {
	Reader io.Reader
	Size   int64
}

// CreateInput defines all varying package inputs. No platform support matrix is
// inferred from these values.
type CreateInput struct {
	Version  string
	Platform string
	Binary   Source
	License  Source
	Limits   Limits
}

// CreateResult contains canonical manifest bytes and a safe creation receipt.
type CreateResult struct {
	Manifest []byte
	Receipt  Receipt
}

// SignInput supplies canonical manifest bytes and an Ed25519 private key.
type SignInput struct {
	Manifest   []byte
	PrivateKey []byte
	Limits     Limits
}

// SignResult contains a detached signature document and a safe signing receipt.
type SignResult struct {
	Signature []byte
	Receipt   Receipt
}

// VerifyInput supplies every detached object needed to authenticate and inspect
// a node package.
type VerifyInput struct {
	Archive   io.Reader
	Manifest  []byte
	Signature []byte
	PublicKey []byte
	Limits    Limits
}

// DigestReceipt is a bounded artifact's public verification metadata.
type DigestReceipt struct {
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// SignatureReceipt identifies the detached signature without exposing key
// provenance or signing environment metadata.
type SignatureReceipt struct {
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"keyId"`
	Bytes     int64  `json:"bytes"`
	SHA256    string `json:"sha256"`
}

// Receipt is the stable success projection for create, sign, and verify.
type Receipt struct {
	Schema             string            `json:"schema"`
	Operation          string            `json:"operation"`
	Status             string            `json:"status"`
	PackageSchema      string            `json:"packageSchema"`
	MediaType          string            `json:"mediaType"`
	Name               string            `json:"name"`
	Version            string            `json:"version"`
	Platform           string            `json:"platform"`
	StationStoreFormat string            `json:"stationStoreFormat"`
	Archive            DigestReceipt     `json:"archive"`
	Manifest           DigestReceipt     `json:"manifest"`
	Signature          *SignatureReceipt `json:"signature,omitempty"`
}

type manifest struct {
	Schema             string          `json:"schema"`
	MediaType          string          `json:"mediaType"`
	Name               string          `json:"name"`
	Version            string          `json:"version"`
	Platform           string          `json:"platform"`
	StationStoreFormat string          `json:"stationStoreFormat"`
	Archive            manifestArchive `json:"archive"`
	Entries            []manifestEntry `json:"entries"`
}

type manifestArchive struct {
	Format string `json:"format"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type manifestEntry struct {
	Path   string `json:"path"`
	Mode   string `json:"mode"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

type signatureDocument struct {
	Schema    string `json:"schema"`
	Algorithm string `json:"algorithm"`
	KeyID     string `json:"keyId"`
	Signature string `json:"signature"`
}

func receiptFor(
	operation string,
	status string,
	value manifest,
	manifestBytes []byte,
	signature *SignatureReceipt,
) Receipt {
	return Receipt{
		Schema:             ReceiptSchema,
		Operation:          operation,
		Status:             status,
		PackageSchema:      value.Schema,
		MediaType:          value.MediaType,
		Name:               value.Name,
		Version:            value.Version,
		Platform:           value.Platform,
		StationStoreFormat: value.StationStoreFormat,
		Archive: DigestReceipt{
			Bytes:  value.Archive.Bytes,
			SHA256: value.Archive.SHA256,
		},
		Manifest: DigestReceipt{
			Bytes:  int64(len(manifestBytes)),
			SHA256: sha256Hex(manifestBytes),
		},
		Signature: signature,
	}
}
