// Package nodepackage creates and verifies the BadTower node package format.
package nodepackage

import "errors"

const failureSchema = "badtower.node-package.error.v1"

// Code is a stable, machine-readable failure code.
type Code string

const (
	CodeArgumentsInvalid            Code = "arguments-invalid"
	CodeLimitsInvalid               Code = "limits-invalid"
	CodeVersionInvalid              Code = "version-invalid"
	CodePlatformInvalid             Code = "platform-invalid"
	CodeBinarySizeInvalid           Code = "binary-size-invalid"
	CodeLicenseSizeInvalid          Code = "license-size-invalid"
	CodeSourceReadFailed            Code = "source-read-failed"
	CodeArchiveWriteFailed          Code = "archive-write-failed"
	CodeManifestTooLarge            Code = "manifest-too-large"
	CodeManifestInvalid             Code = "manifest-invalid"
	CodeManifestNonCanonical        Code = "manifest-noncanonical"
	CodePrivateKeyInvalid           Code = "private-key-invalid"
	CodePublicKeyInvalid            Code = "public-key-invalid"
	CodeSignatureTooLarge           Code = "signature-too-large"
	CodeSignatureDocumentInvalid    Code = "signature-document-invalid"
	CodeSignatureNonCanonical       Code = "signature-noncanonical"
	CodeSignatureKeyMismatch        Code = "signature-key-mismatch"
	CodeSignatureVerificationFailed Code = "signature-verification-failed"
	CodeArchiveTooLarge             Code = "archive-too-large"
	CodeArchiveFormatInvalid        Code = "archive-format-invalid"
	CodeArchiveLayoutInvalid        Code = "archive-layout-invalid"
	CodeArchiveEntryTooLarge        Code = "archive-entry-too-large"
	CodeArchiveContentMismatch      Code = "archive-content-mismatch"
	CodeArchiveMetadataMismatch     Code = "archive-metadata-mismatch"
	CodeArchiveDigestMismatch       Code = "archive-digest-mismatch"
	CodeArchiveTrailingData         Code = "archive-trailing-data"
	CodeInputOpenFailed             Code = "input-open-failed"
	CodeInputReadFailed             Code = "input-read-failed"
	CodeOutputExists                Code = "output-exists"
	CodeOutputWriteFailed           Code = "output-write-failed"
	CodePreflightIdentityMismatch   Code = "preflight-identity-mismatch"
	CodePreflightRetiredFound       Code = "preflight-retired-found"
	CodeInternal                    Code = "internal-failure"
)

// Failure is the public, privacy-safe error projection.
//
// It intentionally contains no wrapped operating-system error, path, key
// material, or archive content.
type Failure struct {
	Schema    string `json:"schema"`
	Code      Code   `json:"code"`
	Stage     string `json:"stage"`
	Component string `json:"component"`
	Retryable bool   `json:"retryable"`
	Recovery  string `json:"recovery"`
}

// Error returns only the stable code and never includes sensitive context.
func (failure *Failure) Error() string {
	if failure == nil {
		return string(CodeInternal)
	}
	return string(failure.Code)
}

// NewFailure constructs the canonical projection for a stable code.
func NewFailure(code Code) *Failure {
	if !knownCode(code) {
		code = CodeInternal
	}
	stage, component, retryable, recovery := failureAttributes(code)
	return &Failure{
		Schema:    failureSchema,
		Code:      code,
		Stage:     stage,
		Component: component,
		Retryable: retryable,
		Recovery:  recovery,
	}
}

func knownCode(code Code) bool {
	switch code {
	case CodeArgumentsInvalid,
		CodeLimitsInvalid,
		CodeVersionInvalid,
		CodePlatformInvalid,
		CodeBinarySizeInvalid,
		CodeLicenseSizeInvalid,
		CodeSourceReadFailed,
		CodeArchiveWriteFailed,
		CodeManifestTooLarge,
		CodeManifestInvalid,
		CodeManifestNonCanonical,
		CodePrivateKeyInvalid,
		CodePublicKeyInvalid,
		CodeSignatureTooLarge,
		CodeSignatureDocumentInvalid,
		CodeSignatureNonCanonical,
		CodeSignatureKeyMismatch,
		CodeSignatureVerificationFailed,
		CodeArchiveTooLarge,
		CodeArchiveFormatInvalid,
		CodeArchiveLayoutInvalid,
		CodeArchiveEntryTooLarge,
		CodeArchiveContentMismatch,
		CodeArchiveMetadataMismatch,
		CodeArchiveDigestMismatch,
		CodeArchiveTrailingData,
		CodeInputOpenFailed,
		CodeInputReadFailed,
		CodeOutputExists,
		CodeOutputWriteFailed,
		CodePreflightIdentityMismatch,
		CodePreflightRetiredFound,
		CodeInternal:
		return true
	default:
		return false
	}
}

// ProjectFailure converts any error into the public failure contract.
func ProjectFailure(err error) Failure {
	var failure *Failure
	if errors.As(err, &failure) && failure != nil {
		return *NewFailure(failure.Code)
	}
	return *NewFailure(CodeInternal)
}

func failureAttributes(code Code) (stage, component string, retryable bool, recovery string) {
	switch code {
	case CodeArgumentsInvalid:
		return "input", "cli", false, "provide-valid-arguments"
	case CodeLimitsInvalid:
		return "input", "limits", false, "provide-valid-limits"
	case CodeVersionInvalid, CodePlatformInvalid:
		return "input", "package-builder", false, "provide-valid-package-identity"
	case CodeBinarySizeInvalid, CodeLicenseSizeInvalid:
		return "input", "package-builder", false, "provide-bounded-regular-source"
	case CodeSourceReadFailed:
		return "source", "package-builder", true, "retry-with-readable-source"
	case CodeArchiveWriteFailed:
		return "output", "archive-writer", true, "retry-with-writable-output"
	case CodeManifestTooLarge:
		return "manifest", "manifest-parser", false, "provide-bounded-manifest"
	case CodeManifestInvalid, CodeManifestNonCanonical:
		return "manifest", "manifest-parser", false, "regenerate-manifest"
	case CodePrivateKeyInvalid:
		return "signature", "private-key-parser", false, "provide-valid-ed25519-private-key"
	case CodePublicKeyInvalid:
		return "signature", "public-key-parser", false, "provide-valid-ed25519-public-key"
	case CodeSignatureTooLarge:
		return "signature", "signature-parser", false, "provide-bounded-signature"
	case CodeSignatureDocumentInvalid, CodeSignatureNonCanonical:
		return "signature", "signature-parser", false, "regenerate-signature"
	case CodeSignatureKeyMismatch:
		return "signature", "signature-verifier", false, "use-matching-public-key"
	case CodeSignatureVerificationFailed:
		return "signature", "signature-verifier", false, "replace-untrusted-signature"
	case CodeArchiveTooLarge, CodeArchiveEntryTooLarge:
		return "archive", "archive-verifier", false, "provide-bounded-archive"
	case CodeArchiveFormatInvalid, CodeArchiveLayoutInvalid, CodeArchiveTrailingData:
		return "archive", "archive-verifier", false, "regenerate-package"
	case CodeArchiveContentMismatch, CodeArchiveMetadataMismatch, CodeArchiveDigestMismatch:
		return "archive", "archive-verifier", false, "replace-corrupt-artifact"
	case CodeInputOpenFailed, CodeInputReadFailed:
		return "input", "cli-io", true, "retry-with-readable-input"
	case CodeOutputExists:
		return "output", "cli-io", false, "choose-unused-output"
	case CodeOutputWriteFailed:
		return "output", "cli-io", true, "retry-with-writable-output"
	case CodePreflightIdentityMismatch:
		return "preflight", "store-guard", false, "use-matching-native-store-identity"
	case CodePreflightRetiredFound:
		return "preflight", "store-guard", false, "remove-retired-state-before-upgrade"
	default:
		return "internal", "node-package", false, "report-stable-failure-code"
	}
}
