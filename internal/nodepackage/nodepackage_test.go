package nodepackage

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

func TestPackageNativeStoreIdentityRequiredInManifest(t *testing.T) {
	t.Parallel()
	_, result := createFixture(t)
	value, err := parseManifest(result.Manifest, DefaultLimits())
	if err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if value.StationStoreFormat != StationStoreFormat {
		t.Fatalf("stationStoreFormat = %q, want %q", value.StationStoreFormat, StationStoreFormat)
	}
}

func TestPackageRejectsForeignStoreFormatInManifest(t *testing.T) {
	t.Parallel()
	for _, retired := range []string{"badtower.station-store.v1", "data/badtower.db"} {
		retired := retired
		t.Run(retired, func(t *testing.T) {
			t.Parallel()
			_, result := createFixture(t)
			value, err := parseManifest(result.Manifest, DefaultLimits())
			if err != nil {
				t.Fatalf("parse manifest: %v", err)
			}
			value.StationStoreFormat = retired
			foreign, err := marshalManifest(value)
			if err != nil {
				t.Fatalf("marshal foreign manifest: %v", err)
			}
			_, err = parseManifest(foreign, DefaultLimits())
			requireCode(t, err, CodeManifestInvalid)
		})
	}
}

func TestCreateIsDeterministicAndVerifiable(t *testing.T) {
	t.Parallel()
	firstArchive, firstResult := createFixture(t)
	secondArchive, secondResult := createFixture(t)
	if !bytes.Equal(firstArchive, secondArchive) {
		t.Fatal("Create produced different archive bytes for identical inputs")
	}
	if !bytes.Equal(firstResult.Manifest, secondResult.Manifest) {
		t.Fatal("Create produced different manifest bytes for identical inputs")
	}
	if firstResult.Receipt.Operation != "create" ||
		firstResult.Receipt.Status != "created" ||
		firstResult.Receipt.Archive.Bytes != int64(len(firstArchive)) {
		t.Fatalf("unexpected create receipt: %#v", firstResult.Receipt)
	}

	value, err := parseManifest(firstResult.Manifest, DefaultLimits())
	if err != nil {
		t.Fatalf("parse created manifest: %v", err)
	}
	if value.MediaType != PackageMediaType ||
		value.StationStoreFormat != StationStoreFormat ||
		value.Version != "1.2.3" ||
		value.Platform != "linux-amd64" {
		t.Fatalf("unexpected manifest identity: %#v", value)
	}

	privateKey, publicKey := syntheticKey()
	signed, err := Sign(SignInput{
		Manifest:   firstResult.Manifest,
		PrivateKey: privateKey,
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	receipt, err := Verify(VerifyInput{
		Archive:   bytes.NewReader(firstArchive),
		Manifest:  firstResult.Manifest,
		Signature: signed.Signature,
		PublicKey: publicKey,
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if receipt.Operation != "verify" || receipt.Status != "verified" ||
		receipt.Signature == nil ||
		receipt.Signature.KeyID != publicKeyID(publicKey) {
		t.Fatalf("unexpected verification receipt: %#v", receipt)
	}
}

func TestCreatedArchiveHasOnlyCanonicalMembers(t *testing.T) {
	t.Parallel()
	content, _ := createFixture(t)
	compressed, err := gzip.NewReader(bytes.NewReader(content))
	if err != nil {
		t.Fatalf("open gzip: %v", err)
	}
	if !canonicalGZIPHeader(compressed.Header) {
		t.Fatalf("non-canonical gzip header: %#v", compressed.Header)
	}
	archive := tar.NewReader(compressed)
	expected := []struct {
		name string
		mode int64
	}{
		{BinaryPath, 0o755},
		{LicensePath, 0o644},
	}
	for _, want := range expected {
		header, err := archive.Next()
		if err != nil {
			t.Fatalf("next archive member: %v", err)
		}
		if header.Name != want.name || header.Mode != want.mode ||
			header.Typeflag != tar.TypeReg || header.Format != tar.FormatUSTAR {
			t.Fatalf("unexpected archive header: %#v", header)
		}
		if _, err := io.Copy(io.Discard, archive); err != nil {
			t.Fatalf("read archive member: %v", err)
		}
	}
	if header, err := archive.Next(); !errors.Is(err, io.EOF) || header != nil {
		t.Fatalf("unexpected extra archive member: header=%#v err=%v", header, err)
	}
}

func TestCreateRejectsInvalidOrMismatchedSources(t *testing.T) {
	t.Parallel()
	defaults := validCreateInput()
	cases := []struct {
		name   string
		mutate func(*CreateInput)
		code   Code
	}{
		{
			name: "invalid version",
			mutate: func(input *CreateInput) {
				input.Version = "not-semver"
			},
			code: CodeVersionInvalid,
		},
		{
			name: "invalid platform",
			mutate: func(input *CreateInput) {
				input.Platform = "Linux/amd64"
			},
			code: CodePlatformInvalid,
		},
		{
			name: "binary shorter than declared",
			mutate: func(input *CreateInput) {
				input.Binary.Size++
			},
			code: CodeBinarySizeInvalid,
		},
		{
			name: "binary longer than declared",
			mutate: func(input *CreateInput) {
				input.Binary.Size--
			},
			code: CodeBinarySizeInvalid,
		},
		{
			name: "source read failure",
			mutate: func(input *CreateInput) {
				input.Binary = Source{Reader: failingReader{}, Size: 1}
			},
			code: CodeSourceReadFailed,
		},
		{
			name:   "archive write failure",
			mutate: func(input *CreateInput) {},
			code:   CodeArchiveWriteFailed,
		},
		{
			name: "archive size ceiling",
			mutate: func(input *CreateInput) {
				input.Limits = DefaultLimits()
				input.Limits.MaxArchiveBytes = 1
			},
			code: CodeArchiveTooLarge,
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			input := defaults
			input.Binary.Reader = bytes.NewReader(fixtureBinary)
			input.License.Reader = bytes.NewReader(fixtureLicense)
			testCase.mutate(&input)
			var output io.Writer = io.Discard
			if testCase.name == "archive write failure" {
				output = failingWriter{}
			}
			_, err := Create(output, input)
			requireCode(t, err, testCase.code)
		})
	}
}

func TestVersionAndPlatformSyntaxDoNotDeclareSupport(t *testing.T) {
	t.Parallel()
	validVersions := []string{
		"0.0.0",
		"1.2.3",
		"1.2.3-alpha.1",
		"1.2.3-alpha-1+build.007",
	}
	for _, value := range validVersions {
		if !validVersion(value) {
			t.Errorf("validVersion(%q) = false", value)
		}
	}
	invalidVersions := []string{
		"v1.2.3",
		"1.2",
		"01.2.3",
		"1.02.3",
		"1.2.03",
		"1.2.3-01",
		"1.2.3-",
		"1.2.3+",
		"1.2.3+bad_value",
	}
	for _, value := range invalidVersions {
		if validVersion(value) {
			t.Errorf("validVersion(%q) = true", value)
		}
	}
	validPlatforms := []string{"linux-amd64", "darwin-arm64", "js-wasm", "plan9-386"}
	for _, value := range validPlatforms {
		if !validPlatform(value) {
			t.Errorf("validPlatform(%q) = false", value)
		}
	}
	invalidPlatforms := []string{"linux/amd64", "Linux-amd64", "linux", "linux-amd64-v2"}
	for _, value := range invalidPlatforms {
		if validPlatform(value) {
			t.Errorf("validPlatform(%q) = true", value)
		}
	}
}

func TestManifestParsingIsStrict(t *testing.T) {
	t.Parallel()
	_, result := createFixture(t)
	valid := result.Manifest
	value, err := parseManifest(valid, DefaultLimits())
	if err != nil {
		t.Fatalf("parse fixture manifest: %v", err)
	}
	value.MediaType = "application/octet-stream"
	invalidMediaType, err := marshalManifest(value)
	if err != nil {
		t.Fatalf("marshal invalid manifest: %v", err)
	}
	cases := []struct {
		name    string
		content []byte
		code    Code
	}{
		{
			name: "unknown field",
			content: bytes.Replace(
				valid,
				[]byte(`"name":"badtower"`),
				[]byte(`"unknown":true,"name":"badtower"`),
				1,
			),
			code: CodeManifestInvalid,
		},
		{
			name:    "non-canonical whitespace",
			content: append(append([]byte(nil), valid[:len(valid)-1]...), ' ', '\n'),
			code:    CodeManifestNonCanonical,
		},
		{
			name: "duplicate field",
			content: bytes.Replace(
				valid,
				[]byte(`"name":"badtower"`),
				[]byte(`"name":"badtower","name":"badtower"`),
				1,
			),
			code: CodeManifestNonCanonical,
		},
		{
			name:    "wrong media type",
			content: invalidMediaType,
			code:    CodeManifestInvalid,
		},
		{
			name:    "trailing value",
			content: append(append([]byte(nil), valid...), []byte("{}")...),
			code:    CodeManifestInvalid,
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, err := parseManifest(testCase.content, DefaultLimits())
			requireCode(t, err, testCase.code)
		})
	}
}

func TestDetachedSignatureRejectsTamperingAndWrongKeys(t *testing.T) {
	t.Parallel()
	archive, result := createFixture(t)
	privateKey, publicKey := syntheticKey()
	signed, err := Sign(SignInput{
		Manifest:   result.Manifest,
		PrivateKey: privateKey,
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	otherPrivate, otherPublic := syntheticKeyFromLabel("other synthetic key")
	_ = otherPrivate
	_, err = Verify(VerifyInput{
		Archive:   bytes.NewReader(archive),
		Manifest:  result.Manifest,
		Signature: signed.Signature,
		PublicKey: otherPublic,
	})
	requireCode(t, err, CodeSignatureKeyMismatch)

	var document signatureDocument
	if err := jsonUnmarshalStrict(signed.Signature, &document); err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	raw, err := base64.StdEncoding.DecodeString(document.Signature)
	if err != nil {
		t.Fatalf("decode raw signature: %v", err)
	}
	raw[0] ^= 0xff
	document.Signature = base64.StdEncoding.EncodeToString(raw)
	tamperedSignature, err := marshalSignature(document)
	if err != nil {
		t.Fatalf("marshal tampered signature: %v", err)
	}
	_, err = Verify(VerifyInput{
		Archive:   bytes.NewReader(archive),
		Manifest:  result.Manifest,
		Signature: tamperedSignature,
		PublicKey: publicKey,
	})
	requireCode(t, err, CodeSignatureVerificationFailed)

	tamperedManifest := bytes.Replace(
		result.Manifest,
		[]byte(`"version":"1.2.3"`),
		[]byte(`"version":"1.2.4"`),
		1,
	)
	_, err = Verify(VerifyInput{
		Archive:   bytes.NewReader(archive),
		Manifest:  tamperedManifest,
		Signature: signed.Signature,
		PublicKey: publicKey,
	})
	requireCode(t, err, CodeSignatureVerificationFailed)
}

func TestSignatureDocumentParsingIsStrict(t *testing.T) {
	t.Parallel()
	archive, result := createFixture(t)
	privateKey, publicKey := syntheticKey()
	signed, err := Sign(SignInput{
		Manifest:   result.Manifest,
		PrivateKey: privateKey,
	})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	var document signatureDocument
	if err := jsonUnmarshalStrict(signed.Signature, &document); err != nil {
		t.Fatalf("decode signature: %v", err)
	}
	document.Algorithm = "ed25519"
	wrongAlgorithm, err := marshalSignature(document)
	if err != nil {
		t.Fatalf("marshal wrong algorithm: %v", err)
	}
	cases := []struct {
		name      string
		signature []byte
		code      Code
	}{
		{
			name: "unknown field",
			signature: bytes.Replace(
				signed.Signature,
				[]byte(`"algorithm":"Ed25519"`),
				[]byte(`"unknown":true,"algorithm":"Ed25519"`),
				1,
			),
			code: CodeSignatureDocumentInvalid,
		},
		{
			name: "noncanonical whitespace",
			signature: append(
				append([]byte(nil), signed.Signature[:len(signed.Signature)-1]...),
				' ',
				'\n',
			),
			code: CodeSignatureNonCanonical,
		},
		{
			name:      "wrong algorithm",
			signature: wrongAlgorithm,
			code:      CodeSignatureDocumentInvalid,
		},
	}
	for _, testCase := range cases {
		testCase := testCase
		t.Run(testCase.name, func(t *testing.T) {
			t.Parallel()
			_, err := Verify(VerifyInput{
				Archive:   bytes.NewReader(archive),
				Manifest:  result.Manifest,
				Signature: testCase.signature,
				PublicKey: publicKey,
			})
			requireCode(t, err, testCase.code)
		})
	}

	_, err = Sign(SignInput{
		Manifest:   result.Manifest,
		PrivateKey: make([]byte, ed25519.PrivateKeySize),
	})
	requireCode(t, err, CodePrivateKeyInvalid)
}

func TestKeyPEMParsingIsEd25519OnlyAndStrict(t *testing.T) {
	t.Parallel()
	privateKey, publicKey := syntheticKey()
	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})

	parsedPrivate, err := ParsePrivateKeyPEM(privatePEM, Limits{})
	if err != nil || !bytes.Equal(parsedPrivate, privateKey) {
		t.Fatalf("ParsePrivateKeyPEM: err=%v", err)
	}
	parsedPublic, err := ParsePublicKeyPEM(publicPEM, Limits{})
	if err != nil || !bytes.Equal(parsedPublic, publicKey) {
		t.Fatalf("ParsePublicKeyPEM: err=%v", err)
	}
	_, err = ParsePrivateKeyPEM(append(privatePEM, []byte("unexpected")...), Limits{})
	requireCode(t, err, CodePrivateKeyInvalid)
	_, err = ParsePublicKeyPEM(
		pem.EncodeToMemory(&pem.Block{Type: "WRONG", Bytes: publicDER}),
		Limits{},
	)
	requireCode(t, err, CodePublicKeyInvalid)
}

func TestFailureProjectionNeverIncludesUnderlyingContext(t *testing.T) {
	t.Parallel()
	sensitive := errors.New("sensitive-local-context")
	projected := ProjectFailure(sensitive)
	if projected.Code != CodeInternal ||
		strings.Contains(projected.Error(), "sensitive") {
		t.Fatalf("unsafe failure projection: %#v", projected)
	}
	known := NewFailure(CodeInputOpenFailed)
	if known.Error() != string(CodeInputOpenFailed) {
		t.Fatalf("failure Error = %q", known.Error())
	}
	unknown := NewFailure(Code("sensitive-local-context"))
	if unknown.Code != CodeInternal {
		t.Fatalf("unknown failure code was not normalized: %#v", unknown)
	}
}

func createFixture(t *testing.T) ([]byte, CreateResult) {
	t.Helper()
	input := validCreateInput()
	var archive bytes.Buffer
	result, err := Create(&archive, input)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return archive.Bytes(), result
}

func validCreateInput() CreateInput {
	return CreateInput{
		Version:  "1.2.3",
		Platform: "linux-amd64",
		Binary: Source{
			Reader: bytes.NewReader(fixtureBinary),
			Size:   int64(len(fixtureBinary)),
		},
		License: Source{
			Reader: bytes.NewReader(fixtureLicense),
			Size:   int64(len(fixtureLicense)),
		},
	}
}

var (
	fixtureBinary  = []byte("#!/synthetic/badtower\n")
	fixtureLicense = []byte("Synthetic AGPL-3.0-or-later test license.\n")
)

func syntheticKey() (ed25519.PrivateKey, ed25519.PublicKey) {
	return syntheticKeyFromLabel("badtower node package synthetic test key")
}

func syntheticKeyFromLabel(label string) (ed25519.PrivateKey, ed25519.PublicKey) {
	seed := sha256.Sum256([]byte(label))
	privateKey := ed25519.NewKeyFromSeed(seed[:])
	publicKey := append(ed25519.PublicKey(nil), privateKey.Public().(ed25519.PublicKey)...)
	return privateKey, publicKey
}

func requireCode(t *testing.T, err error, expected Code) {
	t.Helper()
	var failure *Failure
	if !errors.As(err, &failure) {
		t.Fatalf("expected code %q, got %T %v", expected, err, err)
	}
	if failure.Code != expected {
		t.Fatalf("failure code = %q, want %q", failure.Code, expected)
	}
}

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) {
	return 0, errors.New("synthetic read failure")
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("synthetic write failure")
}

func jsonUnmarshalStrict(content []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

func canonicalTestArchive(
	t *testing.T,
	headers []*tar.Header,
	contents [][]byte,
	configureGZIP func(*gzip.Writer),
) []byte {
	t.Helper()
	var output bytes.Buffer
	compressed, err := gzip.NewWriterLevel(&output, gzip.BestCompression)
	if err != nil {
		t.Fatalf("new gzip writer: %v", err)
	}
	compressed.Header.ModTime = time.Time{}
	compressed.Header.OS = 255
	if configureGZIP != nil {
		configureGZIP(compressed)
	}
	archive := tar.NewWriter(compressed)
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
	if err := compressed.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return output.Bytes()
}

func canonicalTestHeader(path string, mode, size int64) *tar.Header {
	return &tar.Header{
		Typeflag: tar.TypeReg,
		Name:     path,
		Mode:     mode,
		Size:     size,
		ModTime:  archiveEpoch,
		Format:   tar.FormatUSTAR,
	}
}
