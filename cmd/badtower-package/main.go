package main

import (
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"strings"

	"github.com/LicoLand/BadTower/internal/nodepackage"
)

func main() {
	receipt, err := run(os.Args[1:])
	if err == nil {
		if encodeErr := json.NewEncoder(os.Stdout).Encode(receipt); encodeErr != nil {
			err = nodepackage.NewFailure(nodepackage.CodeOutputWriteFailed)
		}
	}
	if err != nil {
		failure := nodepackage.ProjectFailure(err)
		_ = json.NewEncoder(os.Stderr).Encode(failure)
		os.Exit(1)
	}
}

func run(arguments []string) (nodepackage.Receipt, error) {
	if len(arguments) == 0 {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeArgumentsInvalid)
	}
	switch arguments[0] {
	case "create":
		return runCreate(arguments[1:])
	case "sign":
		return runSign(arguments[1:])
	case "verify":
		return runVerify(arguments[1:])
	default:
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeArgumentsInvalid)
	}
}

func runCreate(arguments []string) (nodepackage.Receipt, error) {
	flags := newFlagSet("create")
	binaryPath := flags.String("binary", "", "badtower binary input")
	licensePath := flags.String("license", "", "license input")
	archivePath := flags.String("archive", "", "new archive output")
	manifestPath := flags.String("manifest", "", "new manifest output")
	version := flags.String("version", "", "package version")
	platform := flags.String("platform", "", "package platform identifier")
	if err := parseFlags(flags, arguments); err != nil ||
		anyEmpty(*binaryPath, *licensePath, *archivePath, *manifestPath, *version, *platform) {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeArgumentsInvalid)
	}

	binary, binarySize, err := openRegular(*binaryPath)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	defer binary.Close()
	license, licenseSize, err := openRegular(*licensePath)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	defer license.Close()

	archive, err := createOutput(*archivePath)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	archiveComplete := false
	defer func() {
		if !archiveComplete {
			_ = archive.Close()
			_ = os.Remove(*archivePath)
		}
	}()
	manifest, err := createOutput(*manifestPath)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	manifestComplete := false
	defer func() {
		if !manifestComplete {
			_ = manifest.Close()
			_ = os.Remove(*manifestPath)
		}
	}()

	result, err := nodepackage.Create(archive, nodepackage.CreateInput{
		Version:  *version,
		Platform: *platform,
		Binary:   nodepackage.Source{Reader: binary, Size: binarySize},
		License:  nodepackage.Source{Reader: license, Size: licenseSize},
	})
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	if err := archive.Close(); err != nil {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeOutputWriteFailed)
	}

	if err := writeAll(manifest, result.Manifest); err != nil {
		return nodepackage.Receipt{}, err
	}
	if err := manifest.Close(); err != nil {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeOutputWriteFailed)
	}

	manifestComplete = true
	archiveComplete = true
	return result.Receipt, nil
}

func runSign(arguments []string) (nodepackage.Receipt, error) {
	flags := newFlagSet("sign")
	manifestPath := flags.String("manifest", "", "canonical manifest input")
	privateKeyPath := flags.String("private-key", "", "PKCS#8 Ed25519 private key input")
	signaturePath := flags.String("signature", "", "new detached signature output")
	if err := parseFlags(flags, arguments); err != nil ||
		anyEmpty(*manifestPath, *privateKeyPath, *signaturePath) {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeArgumentsInvalid)
	}

	limits := nodepackage.DefaultLimits()
	manifestBytes, err := readBounded(
		*manifestPath,
		limits.MaxManifestBytes,
		nodepackage.CodeManifestTooLarge,
	)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	keyBytes, err := readBounded(
		*privateKeyPath,
		limits.MaxKeyBytes,
		nodepackage.CodePrivateKeyInvalid,
	)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	defer clear(keyBytes)
	privateKey, err := nodepackage.ParsePrivateKeyPEM(keyBytes, limits)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	defer clear(privateKey)

	output, err := createOutput(*signaturePath)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	complete := false
	defer func() {
		if !complete {
			_ = output.Close()
			_ = os.Remove(*signaturePath)
		}
	}()
	result, err := nodepackage.Sign(nodepackage.SignInput{
		Manifest:   manifestBytes,
		PrivateKey: privateKey,
		Limits:     limits,
	})
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	if err := writeAll(output, result.Signature); err != nil {
		return nodepackage.Receipt{}, err
	}
	if err := output.Close(); err != nil {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeOutputWriteFailed)
	}
	complete = true
	return result.Receipt, nil
}

func runVerify(arguments []string) (nodepackage.Receipt, error) {
	flags := newFlagSet("verify")
	archivePath := flags.String("archive", "", "node package archive input")
	manifestPath := flags.String("manifest", "", "canonical manifest input")
	signaturePath := flags.String("signature", "", "detached signature input")
	publicKeyPath := flags.String("public-key", "", "PKIX Ed25519 public key input")
	if err := parseFlags(flags, arguments); err != nil ||
		anyEmpty(*archivePath, *manifestPath, *signaturePath, *publicKeyPath) {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeArgumentsInvalid)
	}

	limits := nodepackage.DefaultLimits()
	manifestBytes, err := readBounded(
		*manifestPath,
		limits.MaxManifestBytes,
		nodepackage.CodeManifestTooLarge,
	)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	signatureBytes, err := readBounded(
		*signaturePath,
		limits.MaxSignatureBytes,
		nodepackage.CodeSignatureTooLarge,
	)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	keyBytes, err := readBounded(
		*publicKeyPath,
		limits.MaxKeyBytes,
		nodepackage.CodePublicKeyInvalid,
	)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	publicKey, err := nodepackage.ParsePublicKeyPEM(keyBytes, limits)
	if err != nil {
		return nodepackage.Receipt{}, err
	}

	archive, archiveSize, err := openRegular(*archivePath)
	if err != nil {
		return nodepackage.Receipt{}, err
	}
	defer archive.Close()
	if archiveSize > limits.MaxArchiveBytes {
		return nodepackage.Receipt{}, nodepackage.NewFailure(nodepackage.CodeArchiveTooLarge)
	}
	return nodepackage.Verify(nodepackage.VerifyInput{
		Archive:   archive,
		Manifest:  manifestBytes,
		Signature: signatureBytes,
		PublicKey: publicKey,
		Limits:    limits,
	})
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return flags
}

func parseFlags(flags *flag.FlagSet, arguments []string) error {
	if err := flags.Parse(arguments); err != nil || flags.NArg() != 0 {
		return nodepackage.NewFailure(nodepackage.CodeArgumentsInvalid)
	}
	return nil
}

func anyEmpty(values ...string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return true
		}
	}
	return false
}

func openRegular(path string) (*os.File, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, 0, nodepackage.NewFailure(nodepackage.CodeInputOpenFailed)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, 0, nodepackage.NewFailure(nodepackage.CodeInputReadFailed)
	}
	return file, info.Size(), nil
}

func readBounded(path string, maximum int64, tooLarge nodepackage.Code) ([]byte, error) {
	file, size, err := openRegular(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if size > maximum {
		return nil, nodepackage.NewFailure(tooLarge)
	}
	content, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil {
		return nil, nodepackage.NewFailure(nodepackage.CodeInputReadFailed)
	}
	if int64(len(content)) > maximum {
		return nil, nodepackage.NewFailure(tooLarge)
	}
	return content, nil
}

func createOutput(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, nodepackage.NewFailure(nodepackage.CodeOutputExists)
		}
		return nil, nodepackage.NewFailure(nodepackage.CodeOutputWriteFailed)
	}
	return file, nil
}

func writeAll(output io.Writer, content []byte) error {
	for len(content) > 0 {
		written, err := output.Write(content)
		if err != nil {
			return nodepackage.NewFailure(nodepackage.CodeOutputWriteFailed)
		}
		if written <= 0 || written > len(content) {
			return nodepackage.NewFailure(nodepackage.CodeOutputWriteFailed)
		}
		content = content[written:]
	}
	return nil
}
