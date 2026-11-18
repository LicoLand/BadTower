package extension

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
)

const (
	SchemaVersion   = "badtower.extension-descriptor.v1"
	ContractVersion = "1.0.0"
)

var (
	ErrInvalidDescriptor = errors.New("invalid external plugin descriptor")
	pluginIDPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{2,63}$`)
	versionPattern       = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?$`)
	pathPattern          = regexp.MustCompile(`^bin/[a-z0-9][a-z0-9._-]{1,127}$`)
	digestPattern        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	schemaIDPattern      = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{2,127}$`)
)

type Descriptor struct {
	SchemaVersion       string           `json:"schemaVersion"`
	PluginID            string           `json:"pluginId"`
	ArtifactVersion     string           `json:"artifactVersion"`
	ConfigurationSchema string           `json:"configurationSchema"`
	LocalStateSchema    string           `json:"localStateSchema"`
	ExternalProtocol    ExternalProtocol `json:"externalProtocol"`
	Contract            ContractRange    `json:"contract"`
	Listener            Listener         `json:"listener"`
	NativeScope         NativeScope      `json:"nativeScope"`
	Budgets             Budgets          `json:"budgets"`
	Binary              Binary           `json:"binary"`
}

type ExternalProtocol struct {
	Identity  string `json:"identity"`
	Line      string `json:"line"`
	Lifecycle string `json:"lifecycle"`
}

type ContractRange struct {
	Minimum string `json:"minimum"`
	Maximum string `json:"maximum"`
}

type Listener struct {
	Owner     string `json:"owner"`
	Transport string `json:"transport"`
}

type NativeScope struct {
	NamespaceMode  string `json:"namespaceMode"`
	CredentialMode string `json:"credentialMode"`
}

type Budgets struct {
	MaxConcurrentCalls uint32 `json:"maxConcurrentCalls"`
	MaxQueuedCalls     uint32 `json:"maxQueuedCalls"`
	MaxRequestBytes    uint64 `json:"maxRequestBytes"`
	MaxStateBytes      uint64 `json:"maxStateBytes"`
	MaxMetricSeries    uint32 `json:"maxMetricSeries"`
}

type Binary struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
}

func DecodeDescriptor(raw []byte) (Descriptor, error) {
	if err := rejectDuplicateJSONKeys(raw); err != nil {
		return Descriptor{}, ErrInvalidDescriptor
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var descriptor Descriptor
	if err := decoder.Decode(&descriptor); err != nil {
		return Descriptor{}, ErrInvalidDescriptor
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return Descriptor{}, ErrInvalidDescriptor
	}
	if err := descriptor.Validate(); err != nil {
		return Descriptor{}, err
	}
	return descriptor, nil
}

func rejectDuplicateJSONKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	return walkJSONValue(decoder)
}

func walkJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return ErrInvalidDescriptor
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrInvalidDescriptor
			}
			seen[key] = struct{}{}
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	case '[':
		for decoder.More() {
			if err := walkJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		return ErrInvalidDescriptor
	}
}

func (descriptor Descriptor) Validate() error {
	if descriptor.SchemaVersion != SchemaVersion || !pluginIDPattern.MatchString(descriptor.PluginID) || !versionPattern.MatchString(descriptor.ArtifactVersion) || !schemaIDPattern.MatchString(descriptor.ConfigurationSchema) || !schemaIDPattern.MatchString(descriptor.LocalStateSchema) {
		return ErrInvalidDescriptor
	}
	if len(descriptor.ExternalProtocol.Identity) == 0 || len(descriptor.ExternalProtocol.Identity) > 128 || len(descriptor.ExternalProtocol.Line) == 0 || len(descriptor.ExternalProtocol.Line) > 128 {
		return ErrInvalidDescriptor
	}
	switch descriptor.ExternalProtocol.Lifecycle {
	case "Candidate", "Published", "Retired":
	default:
		return ErrInvalidDescriptor
	}
	if descriptor.Contract.Minimum != ContractVersion || descriptor.Contract.Maximum != ContractVersion || descriptor.Listener.Owner != "external-plugin" || descriptor.Listener.Transport == "" || len(descriptor.Listener.Transport) > 64 {
		return ErrInvalidDescriptor
	}
	if descriptor.NativeScope.NamespaceMode != "one-opaque-namespace" || descriptor.NativeScope.CredentialMode != "scoped-native-caller" {
		return ErrInvalidDescriptor
	}
	if descriptor.Budgets.MaxConcurrentCalls == 0 || descriptor.Budgets.MaxConcurrentCalls > 4096 || descriptor.Budgets.MaxQueuedCalls == 0 || descriptor.Budgets.MaxQueuedCalls > 65536 || descriptor.Budgets.MaxRequestBytes == 0 || descriptor.Budgets.MaxRequestBytes > 64<<20 || descriptor.Budgets.MaxStateBytes == 0 || descriptor.Budgets.MaxStateBytes > 1<<40 || descriptor.Budgets.MaxMetricSeries == 0 || descriptor.Budgets.MaxMetricSeries > 64 {
		return ErrInvalidDescriptor
	}
	if !pathPattern.MatchString(descriptor.Binary.Path) || !digestPattern.MatchString(descriptor.Binary.SHA256) {
		return ErrInvalidDescriptor
	}
	return nil
}

func (descriptor Descriptor) CanonicalJSON() ([]byte, error) {
	if err := descriptor.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(descriptor)
}
