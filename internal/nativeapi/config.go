package nativeapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ConfigSchema          = "badtower.station.config.v1"
	ProfileSingleNode     = "single-node-general-v1"
	DefaultDataRoot       = "data/station-core-v1"
	RetiredDataPath       = "data/" + "badtower" + ".db"
	DefaultTCPListen      = "127.0.0.1:7443"
	DefaultHealthListen   = "127.0.0.1:7444"
	DefaultMetricsListen  = "127.0.0.1:7445"
	DefaultOperatorListen = "127.0.0.1:7446"
)

var (
	idPattern     = regexp.MustCompile(`^(ns|sa|tu|dc|rq)_[A-Za-z0-9_-]{22}$`)
	digestPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{43}$`)
)

// Config is the strict badtower.station.config.v1 process configuration.
type Config struct {
	Schema     string            `json:"schema"`
	Profile    string            `json:"profile"`
	DataRoot   string            `json:"dataRoot"`
	Transport  TransportConfig   `json:"transport"`
	Health     ListenerConfig    `json:"health"`
	Metrics    ListenerConfig    `json:"metrics"`
	Namespaces []NamespaceConfig `json:"namespaces"`
	// Operator optionally enables the loopback-only operator overview
	// listener. The block is absent by default; a present-but-disabled block
	// must carry no dangling listen or credential fields.
	Operator *OperatorConfig `json:"operator,omitempty"`
}

// TransportConfig selects the data-plane listener.
type TransportConfig struct {
	Mode               string `json:"mode"`
	Listen             string `json:"listen,omitempty"`
	SocketPath         string `json:"socketPath,omitempty"`
	TLSCertificateFile string `json:"tlsCertificateFile,omitempty"`
	TLSPrivateKeyFile  string `json:"tlsPrivateKeyFile,omitempty"`
}

// ListenerConfig configures an auxiliary HTTP or gRPC listener.
type ListenerConfig struct {
	Listen           string `json:"listen"`
	AllowNonLoopback bool   `json:"allowNonLoopback"`
}

// NamespaceConfig binds one namespace to credential digests and optional limits.
type NamespaceConfig struct {
	ID          string             `json:"id"`
	Credentials []CredentialConfig `json:"credentials"`
	Limits      *NamespaceLimits   `json:"limits,omitempty"`
}

// CredentialConfig stores one bearer digest and validity window.
type CredentialConfig struct {
	SHA256    string    `json:"sha256"`
	NotBefore time.Time `json:"notBefore"`
	NotAfter  time.Time `json:"notAfter"`
}

// OperatorConfig optionally enables the strict operator overview listener.
// When enabled it is always loopback-only with 1-2 active SHA-256 bearer
// credential digests; no plaintext token is ever accepted or stored.
type OperatorConfig struct {
	Enabled     bool               `json:"enabled"`
	Listen      string             `json:"listen,omitempty"`
	Credentials []CredentialConfig `json:"credentials,omitempty"`
}

// NamespaceLimits holds optional lower namespace ceilings.
type NamespaceLimits struct {
	Addresses     *uint64 `json:"addresses,omitempty"`
	Units         *uint64 `json:"units,omitempty"`
	RetainedBytes *uint64 `json:"retainedBytes,omitempty"`
	ActiveClaims  *uint64 `json:"activeClaims,omitempty"`
	Connections   *uint64 `json:"connections,omitempty"`
	InFlightRPCs  *uint64 `json:"inFlightRpcs,omitempty"`
	LongPolls     *uint64 `json:"longPolls,omitempty"`
	RequestRate   *uint64 `json:"requestRate,omitempty"`
	ByteRate      *uint64 `json:"byteRate,omitempty"`
}

// LoadConfig reads and validates one strict configuration file.
func LoadConfig(path string) (Config, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	if len(content) > 1<<20 {
		return Config{}, errors.New("config exceeds size limit")
	}
	if err := rejectDuplicateJSONKeys(content); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("decode config: %w", err)
	}
	var remainder any
	if err := decoder.Decode(&remainder); !errors.Is(err, io.EOF) {
		return Config{}, errors.New("config must contain one JSON value")
	}
	return ValidateConfig(config)
}

// rejectDuplicateJSONKeys walks one strict JSON document and rejects any
// object that repeats a key (C-05): the last occurrence must never silently
// win over an earlier one.
func rejectDuplicateJSONKeys(content []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(content))
	return walkStrictJSONValue(decoder)
}

func walkStrictJSONValue(decoder *json.Decoder) error {
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
		seen := map[string]struct{}{}
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid object key")
			}
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate key %q", key)
			}
			seen[key] = struct{}{}
			if err := walkStrictJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err := decoder.Token() // consume the closing delimiter
		return err
	case '[':
		for decoder.More() {
			if err := walkStrictJSONValue(decoder); err != nil {
				return err
			}
		}
		_, err := decoder.Token() // consume the closing delimiter
		return err
	default:
		return nil
	}
}

// ValidateConfig validates one parsed configuration object.
func ValidateConfig(config Config) (Config, error) {
	if config.Schema != ConfigSchema {
		return Config{}, fmt.Errorf("schema must be %q", ConfigSchema)
	}
	if config.Profile != ProfileSingleNode {
		return Config{}, fmt.Errorf("profile must be %q", ProfileSingleNode)
	}
	if strings.TrimSpace(config.DataRoot) == "" {
		config.DataRoot = DefaultDataRoot
	}
	cleanRoot := filepath.Clean(config.DataRoot)
	if cleanRoot == RetiredDataPath || strings.HasSuffix(cleanRoot, string(filepath.Separator)+"badtower.db") || filepath.Base(cleanRoot) == "badtower.db" {
		return Config{}, errors.New("retired data root is rejected")
	}
	config.DataRoot = cleanRoot

	switch config.Transport.Mode {
	case "tcp":
		if strings.TrimSpace(config.Transport.Listen) == "" {
			config.Transport.Listen = DefaultTCPListen
		}
		if err := validateTCPListen(config.Transport.Listen, true); err != nil {
			return Config{}, err
		}
		if strings.TrimSpace(config.Transport.TLSCertificateFile) == "" || strings.TrimSpace(config.Transport.TLSPrivateKeyFile) == "" {
			return Config{}, errors.New("tcp transport requires tls certificate and key files")
		}
	case "unix":
		if strings.TrimSpace(config.Transport.SocketPath) == "" {
			return Config{}, errors.New("unix transport requires socketPath")
		}
	default:
		return Config{}, errors.New("transport.mode must be tcp or unix")
	}

	if strings.TrimSpace(config.Health.Listen) == "" {
		config.Health.Listen = DefaultHealthListen
	}
	if err := validateTCPListen(config.Health.Listen, config.Health.AllowNonLoopback); err != nil {
		return Config{}, err
	}
	if strings.TrimSpace(config.Metrics.Listen) == "" {
		config.Metrics.Listen = DefaultMetricsListen
	}
	if err := validateTCPListen(config.Metrics.Listen, config.Metrics.AllowNonLoopback); err != nil {
		return Config{}, err
	}

	if err := validateOperator(config.Operator); err != nil {
		return Config{}, err
	}
	operatorDigests := make(map[string]struct{}, 2)
	if config.Operator != nil && config.Operator.Enabled {
		for _, credential := range config.Operator.Credentials {
			operatorDigests[credential.SHA256] = struct{}{}
		}
	}

	if len(config.Namespaces) == 0 || len(config.Namespaces) > 1000 {
		return Config{}, errors.New("namespaces must contain 1-1000 entries")
	}
	seen := map[string]struct{}{}
	allDigests := map[string]struct{}{}
	for index := range config.Namespaces {
		namespace := &config.Namespaces[index]
		if !idPattern.MatchString(namespace.ID) || !strings.HasPrefix(namespace.ID, "ns_") {
			return Config{}, errors.New("namespace id is invalid")
		}
		if _, exists := seen[namespace.ID]; exists {
			return Config{}, errors.New("duplicate namespace id")
		}
		seen[namespace.ID] = struct{}{}
		if len(namespace.Credentials) == 0 || len(namespace.Credentials) > 2 {
			return Config{}, errors.New("namespace credentials must contain 1-2 records")
		}
		now := time.Now().UTC()
		active := false
		for credIndex := range namespace.Credentials {
			credential := &namespace.Credentials[credIndex]
			if !digestPattern.MatchString(credential.SHA256) {
				return Config{}, errors.New("credential sha256 is invalid")
			}
			if _, exists := allDigests[credential.SHA256]; exists {
				return Config{}, errors.New("duplicate credential digest")
			}
			if _, exists := operatorDigests[credential.SHA256]; exists {
				return Config{}, errors.New("operator and namespace credential domains must be disjoint")
			}
			allDigests[credential.SHA256] = struct{}{}
			if credential.NotAfter.Before(credential.NotBefore) {
				return Config{}, errors.New("credential lifetime is invalid")
			}
			if credential.NotAfter.Sub(credential.NotBefore) > 90*24*time.Hour {
				return Config{}, errors.New("credential lifetime exceeds 90 days")
			}
			if !now.After(credential.NotAfter) && now.After(credential.NotBefore.Add(-time.Second)) {
				active = true
			}
		}
		if !active {
			return Config{}, errors.New("namespace has no active credential")
		}
		if err := validateLimits(namespace.Limits); err != nil {
			return Config{}, err
		}
	}
	sort.Slice(config.Namespaces, func(i, j int) bool { return config.Namespaces[i].ID < config.Namespaces[j].ID })
	return config, nil
}

// validateOperator validates the optional operator block. A nil or disabled
// block is inert and must carry no dangling fields; an enabled block is
// loopback-only with a bounded or default listen address and 1-2 active
// SHA-256 bearer credential digests, mirroring the namespace credential
// rules (C-05).
func validateOperator(operator *OperatorConfig) error {
	if operator == nil {
		return nil
	}
	if !operator.Enabled {
		if strings.TrimSpace(operator.Listen) != "" || len(operator.Credentials) != 0 {
			return errors.New("disabled operator block must carry no fields")
		}
		return nil
	}
	if strings.TrimSpace(operator.Listen) == "" {
		operator.Listen = DefaultOperatorListen
	}
	if err := validateOperatorListen(operator.Listen); err != nil {
		return err
	}
	if len(operator.Credentials) == 0 || len(operator.Credentials) > 2 {
		return errors.New("operator credentials must contain 1-2 records")
	}
	digests := map[string]struct{}{}
	now := time.Now().UTC()
	active := false
	for credIndex := range operator.Credentials {
		credential := &operator.Credentials[credIndex]
		if !digestPattern.MatchString(credential.SHA256) {
			return errors.New("operator credential sha256 is invalid")
		}
		if _, exists := digests[credential.SHA256]; exists {
			return errors.New("duplicate operator credential digest")
		}
		digests[credential.SHA256] = struct{}{}
		if credential.NotAfter.Before(credential.NotBefore) {
			return errors.New("operator credential lifetime is invalid")
		}
		if credential.NotAfter.Sub(credential.NotBefore) > 90*24*time.Hour {
			return errors.New("operator credential lifetime exceeds 90 days")
		}
		if !now.After(credential.NotAfter) && now.After(credential.NotBefore.Add(-time.Second)) {
			active = true
		}
	}
	if !active {
		return errors.New("operator has no active credential")
	}
	return nil
}

// validateOperatorListen admits only an explicit numeric loopback address and
// a nonzero TCP port. Unlike test-oriented auxiliary listeners, an enabled
// production operator endpoint can never request an ephemeral or wildcard
// binding and cannot rely on hostname resolution.
func validateOperatorListen(listen string) error {
	host, portText, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("invalid operator listen address: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("operator listen address must be numeric loopback")
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return errors.New("operator listen port must be numeric and nonzero")
	}
	return nil
}

func validateLimits(limits *NamespaceLimits) error {
	if limits == nil {
		return nil
	}
	return errors.New("per-namespace limits overrides are not yet enforced; omit limits to use profile ceilings")
}

func validateTCPListen(listen string, allowNonLoopback bool) error {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return fmt.Errorf("invalid listen address %q: %w", listen, err)
	}
	if !allowNonLoopback {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			return fmt.Errorf("non-loopback binding requires explicit allowNonLoopback for %q", listen)
		}
	}
	return nil
}

// NormalizeConfigJSON returns canonical JSON for check-config output.
func NormalizeConfigJSON(config Config) ([]byte, error) {
	validated, err := ValidateConfig(config)
	if err != nil {
		return nil, err
	}
	return json.MarshalIndent(validated, "", "  ")
}
