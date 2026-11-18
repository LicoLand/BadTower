package nodepackage

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
)

func marshalManifest(value manifest) ([]byte, error) {
	content, err := json.Marshal(value)
	if err != nil {
		return nil, NewFailure(CodeInternal)
	}
	content = append(content, '\n')
	return content, nil
}

func parseManifest(content []byte, limits Limits) (manifest, error) {
	if int64(len(content)) > limits.MaxManifestBytes {
		return manifest{}, NewFailure(CodeManifestTooLarge)
	}
	decoder := json.NewDecoder(bytes.NewReader(content))
	decoder.DisallowUnknownFields()
	var value manifest
	if err := decoder.Decode(&value); err != nil {
		return manifest{}, NewFailure(CodeManifestInvalid)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return manifest{}, NewFailure(CodeManifestInvalid)
	}
	canonical, err := marshalManifest(value)
	if err != nil {
		return manifest{}, err
	}
	if !bytes.Equal(content, canonical) {
		return manifest{}, NewFailure(CodeManifestNonCanonical)
	}
	if !validManifest(value, limits) {
		return manifest{}, NewFailure(CodeManifestInvalid)
	}
	return value, nil
}

func validManifest(value manifest, limits Limits) bool {
	if value.Schema != Schema || value.MediaType != PackageMediaType ||
		value.Name != PackageName || value.StationStoreFormat != StationStoreFormat ||
		!validVersion(value.Version) || !validPlatform(value.Platform) {
		return false
	}
	if value.Archive.Format != ArchiveFormat ||
		value.Archive.Bytes <= 0 || value.Archive.Bytes > limits.MaxArchiveBytes ||
		!validSHA256(value.Archive.SHA256) {
		return false
	}
	if len(value.Entries) != 2 {
		return false
	}
	expected := []struct {
		path    string
		mode    string
		maxSize int64
	}{
		{BinaryPath, "0755", limits.MaxBinaryBytes},
		{LicensePath, "0644", limits.MaxLicenseBytes},
	}
	for index, entry := range value.Entries {
		want := expected[index]
		if entry.Path != want.path || entry.Mode != want.mode ||
			entry.Bytes <= 0 || entry.Bytes > want.maxSize ||
			!validSHA256(entry.SHA256) {
			return false
		}
	}
	return true
}

func validVersion(value string) bool {
	if len(value) == 0 || len(value) > 64 || strings.Count(value, "+") > 1 {
		return false
	}
	coreAndPrerelease := value
	if buildIndex := strings.IndexByte(value, '+'); buildIndex >= 0 {
		if !validSemVerIdentifiers(value[buildIndex+1:], false) {
			return false
		}
		coreAndPrerelease = value[:buildIndex]
	}
	core := coreAndPrerelease
	if prereleaseIndex := strings.IndexByte(coreAndPrerelease, '-'); prereleaseIndex >= 0 {
		if !validSemVerIdentifiers(coreAndPrerelease[prereleaseIndex+1:], true) {
			return false
		}
		core = coreAndPrerelease[:prereleaseIndex]
	}
	parts := strings.Split(core, ".")
	return len(parts) == 3 &&
		validSemVerNumber(parts[0]) &&
		validSemVerNumber(parts[1]) &&
		validSemVerNumber(parts[2])
}

func validPlatform(value string) bool {
	if len(value) == 0 || len(value) > 64 || strings.Count(value, "-") != 1 {
		return false
	}
	parts := strings.Split(value, "-")
	return len(parts) == 2 &&
		validLowerAlphaNumeric(parts[0], false) &&
		validLowerAlphaNumeric(parts[1], true)
}

func validSemVerNumber(value string) bool {
	if value == "" || len(value) > 1 && value[0] == '0' {
		return false
	}
	for index := 0; index < len(value); index++ {
		if value[index] < '0' || value[index] > '9' {
			return false
		}
	}
	return true
}

func validSemVerIdentifiers(value string, numericLeadingZeroForbidden bool) bool {
	if value == "" {
		return false
	}
	for _, identifier := range strings.Split(value, ".") {
		if identifier == "" {
			return false
		}
		numeric := true
		for index := 0; index < len(identifier); index++ {
			character := identifier[index]
			if character < '0' || character > '9' {
				numeric = false
			}
			if !semVerIdentifierCharacter(character) {
				return false
			}
		}
		if numericLeadingZeroForbidden && numeric &&
			len(identifier) > 1 && identifier[0] == '0' {
			return false
		}
	}
	return true
}

func semVerIdentifierCharacter(character byte) bool {
	return character >= '0' && character <= '9' ||
		character >= 'a' && character <= 'z' ||
		character >= 'A' && character <= 'Z' ||
		character == '-'
}

func validLowerAlphaNumeric(value string, allowLeadingDigit bool) bool {
	if value == "" {
		return false
	}
	if !allowLeadingDigit && (value[0] < 'a' || value[0] > 'z') {
		return false
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if character < '0' || character > '9' {
			if character < 'a' || character > 'z' {
				return false
			}
		}
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return false
	}
	return value == hex.EncodeToString(decoded)
}
