package nodepackage

import (
	"bytes"
	"crypto/ed25519"
	"crypto/subtle"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
)

const signingDomain = "badtower.node-package.v1\x00"

// Sign authenticates canonical manifest bytes and returns a detached,
// canonical signature document.
func Sign(input SignInput) (SignResult, error) {
	limits, err := normalizeLimits(input.Limits)
	if err != nil {
		return SignResult{}, err
	}
	value, err := parseManifest(input.Manifest, limits)
	if err != nil {
		return SignResult{}, err
	}
	privateKey, err := validatePrivateKey(input.PrivateKey)
	if err != nil {
		return SignResult{}, err
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	keyID := publicKeyID(publicKey)
	signature := ed25519.Sign(privateKey, signingMessage(input.Manifest))
	envelope := signatureDocument{
		Schema:    SignatureSchema,
		Algorithm: SignatureAlgorithm,
		KeyID:     keyID,
		Signature: base64.StdEncoding.EncodeToString(signature),
	}
	content, err := marshalSignature(envelope)
	if err != nil {
		return SignResult{}, err
	}
	if int64(len(content)) > limits.MaxSignatureBytes {
		return SignResult{}, NewFailure(CodeSignatureTooLarge)
	}
	signatureReceipt := newSignatureReceipt(keyID, content)
	return SignResult{
		Signature: content,
		Receipt:   receiptFor("sign", "signed", value, input.Manifest, &signatureReceipt),
	}, nil
}

// ParsePrivateKeyPEM accepts exactly one unencrypted PKCS#8 Ed25519 private key.
func ParsePrivateKeyPEM(content []byte, requested Limits) ([]byte, error) {
	limits, err := normalizeLimits(requested)
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > limits.MaxKeyBytes {
		return nil, NewFailure(CodePrivateKeyInvalid)
	}
	block, ok := decodeSinglePEM(content, "PRIVATE KEY")
	if !ok {
		return nil, NewFailure(CodePrivateKeyInvalid)
	}
	defer clear(block.Bytes)
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, NewFailure(CodePrivateKeyInvalid)
	}
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, NewFailure(CodePrivateKeyInvalid)
	}
	defer clear(privateKey)
	validated, err := validatePrivateKey(privateKey)
	if err != nil {
		return nil, err
	}
	result := append([]byte(nil), validated...)
	return result, nil
}

// ParsePublicKeyPEM accepts exactly one PKIX Ed25519 public key.
func ParsePublicKeyPEM(content []byte, requested Limits) ([]byte, error) {
	limits, err := normalizeLimits(requested)
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > limits.MaxKeyBytes {
		return nil, NewFailure(CodePublicKeyInvalid)
	}
	block, ok := decodeSinglePEM(content, "PUBLIC KEY")
	if !ok {
		return nil, NewFailure(CodePublicKeyInvalid)
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, NewFailure(CodePublicKeyInvalid)
	}
	publicKey, ok := parsed.(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return nil, NewFailure(CodePublicKeyInvalid)
	}
	return append([]byte(nil), publicKey...), nil
}

func validatePrivateKey(content []byte) (ed25519.PrivateKey, error) {
	if len(content) != ed25519.PrivateKeySize {
		return nil, NewFailure(CodePrivateKeyInvalid)
	}
	reconstructed := ed25519.NewKeyFromSeed(content[:ed25519.SeedSize])
	defer clear(reconstructed)
	if subtle.ConstantTimeCompare(reconstructed, content) != 1 {
		return nil, NewFailure(CodePrivateKeyInvalid)
	}
	return ed25519.PrivateKey(content), nil
}

func validatePublicKey(content []byte) (ed25519.PublicKey, error) {
	if len(content) != ed25519.PublicKeySize {
		return nil, NewFailure(CodePublicKeyInvalid)
	}
	return ed25519.PublicKey(content), nil
}

func decodeSinglePEM(content []byte, blockType string) (*pem.Block, bool) {
	block, rest := pem.Decode(content)
	return block, block != nil && block.Type == blockType &&
		len(block.Headers) == 0 && len(bytes.TrimSpace(rest)) == 0
}

func marshalSignature(value signatureDocument) ([]byte, error) {
	content, err := json.Marshal(value)
	if err != nil {
		return nil, NewFailure(CodeInternal)
	}
	return append(content, '\n'), nil
}

func parseSignature(content []byte, limits Limits) (signatureDocument, []byte, error) {
	if int64(len(content)) > limits.MaxSignatureBytes {
		return signatureDocument{}, nil, NewFailure(CodeSignatureTooLarge)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var value signatureDocument
	if err := decoder.Decode(&value); err != nil {
		return signatureDocument{}, nil, NewFailure(CodeSignatureDocumentInvalid)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return signatureDocument{}, nil, NewFailure(CodeSignatureDocumentInvalid)
	}
	canonical, err := marshalSignature(value)
	if err != nil {
		return signatureDocument{}, nil, err
	}
	if !bytes.Equal(content, canonical) {
		return signatureDocument{}, nil, NewFailure(CodeSignatureNonCanonical)
	}
	if value.Schema != SignatureSchema || value.Algorithm != SignatureAlgorithm ||
		!validKeyID(value.KeyID) {
		return signatureDocument{}, nil, NewFailure(CodeSignatureDocumentInvalid)
	}
	signature, err := base64.StdEncoding.DecodeString(value.Signature)
	if err != nil || len(signature) != ed25519.SignatureSize ||
		value.Signature != base64.StdEncoding.EncodeToString(signature) {
		return signatureDocument{}, nil, NewFailure(CodeSignatureDocumentInvalid)
	}
	return value, signature, nil
}

func signingMessage(manifestBytes []byte) []byte {
	message := make([]byte, 0, len(signingDomain)+len(manifestBytes))
	message = append(message, signingDomain...)
	return append(message, manifestBytes...)
}

func publicKeyID(publicKey ed25519.PublicKey) string {
	return "sha256:" + sha256Hex(publicKey)
}

func validKeyID(value string) bool {
	const prefix = "sha256:"
	return len(value) == len(prefix)+64 && value[:len(prefix)] == prefix &&
		validSHA256(value[len(prefix):])
}

func newSignatureReceipt(keyID string, content []byte) SignatureReceipt {
	return SignatureReceipt{
		Algorithm: SignatureAlgorithm,
		KeyID:     keyID,
		Bytes:     int64(len(content)),
		SHA256:    sha256Hex(content),
	}
}
