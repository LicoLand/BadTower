// Package profile owns BadTower's bounded, service-local transport profile.
package profile

import (
	"errors"
	"regexp"
)

const Version = "badtower.relay-profile.v1"

var contractVersionPattern = regexp.MustCompile(`^[a-z][a-z0-9.-]{2,127}$`)

// Limits are BadTower-owned implementation ceilings. Operators may lower
// values but cannot raise them above MaxLimits.
type Limits struct {
	MaxCiphertextBytes          int   `json:"maxCiphertextBytes"`
	MaxEnvelopeRetentionSeconds int64 `json:"maxEnvelopeRetentionSeconds"`
	MaxLeaseSeconds             int64 `json:"maxLeaseSeconds"`
	MaxMailboxEnvelopes         int   `json:"maxMailboxEnvelopes"`
	MaxMailboxes                int   `json:"maxMailboxes"`
	MaxRelayEnvelopes           int   `json:"maxRelayEnvelopes"`
}

var implementationCeilings = Limits{
	MaxCiphertextBytes:          1_048_576,
	MaxEnvelopeRetentionSeconds: 86_400,
	MaxLeaseSeconds:             86_400,
	MaxMailboxEnvelopes:         1_000,
	MaxMailboxes:                1_000,
	MaxRelayEnvelopes:           10_000,
}

func ImplementationCeilings() Limits {
	return implementationCeilings
}

// Config is the complete public profile input.
type Config struct {
	ProfileVersion           string   `json:"profileVersion"`
	AcceptedContractVersions []string `json:"acceptedContractVersions"`
	Limits                   Limits   `json:"limits"`
}

// Profile is an immutable validated projection of Config.
type Profile struct {
	accepted map[string]struct{}
	versions []string
	limits   Limits
}

func Default() Profile {
	value, err := New(Config{
		ProfileVersion:           Version,
		AcceptedContractVersions: []string{"licoarc.relay.v1"},
		Limits:                   implementationCeilings,
	})
	if err != nil {
		panic(err)
	}
	return value
}

func New(config Config) (Profile, error) {
	if config.ProfileVersion != Version {
		return Profile{}, errors.New("unsupported BadTower relay profile")
	}
	if len(config.AcceptedContractVersions) < 1 ||
		len(config.AcceptedContractVersions) > 16 {
		return Profile{}, errors.New("accepted contract versions must contain 1 to 16 values")
	}

	accepted := make(map[string]struct{}, len(config.AcceptedContractVersions))
	versions := make([]string, 0, len(config.AcceptedContractVersions))
	for _, version := range config.AcceptedContractVersions {
		if !contractVersionPattern.MatchString(version) {
			return Profile{}, errors.New("accepted contract version is invalid")
		}
		if _, exists := accepted[version]; exists {
			return Profile{}, errors.New("accepted contract versions must be unique")
		}
		accepted[version] = struct{}{}
		versions = append(versions, version)
	}

	if err := validateLimits(config.Limits); err != nil {
		return Profile{}, err
	}
	return Profile{
		accepted: accepted,
		versions: versions,
		limits:   config.Limits,
	}, nil
}

func (profile Profile) Accepts(contractVersion string) bool {
	_, ok := profile.accepted[contractVersion]
	return ok
}

func (profile Profile) AcceptedContractVersions() []string {
	return append([]string(nil), profile.versions...)
}

func (profile Profile) Limits() Limits {
	return profile.limits
}

func validateLimits(limits Limits) error {
	switch {
	case limits.MaxCiphertextBytes <= 0 ||
		limits.MaxCiphertextBytes > implementationCeilings.MaxCiphertextBytes:
		return errors.New("max ciphertext bytes exceeds the BadTower implementation boundary")
	case limits.MaxEnvelopeRetentionSeconds <= 0 ||
		limits.MaxEnvelopeRetentionSeconds > implementationCeilings.MaxEnvelopeRetentionSeconds:
		return errors.New("max envelope retention exceeds the BadTower implementation boundary")
	case limits.MaxLeaseSeconds <= 0 ||
		limits.MaxLeaseSeconds > implementationCeilings.MaxLeaseSeconds:
		return errors.New("max lease duration exceeds the BadTower implementation boundary")
	case limits.MaxMailboxEnvelopes <= 0 ||
		limits.MaxMailboxEnvelopes > implementationCeilings.MaxMailboxEnvelopes:
		return errors.New("max mailbox envelopes exceeds the BadTower implementation boundary")
	case limits.MaxMailboxes <= 0 ||
		limits.MaxMailboxes > implementationCeilings.MaxMailboxes:
		return errors.New("max mailboxes exceeds the BadTower implementation boundary")
	case limits.MaxRelayEnvelopes <= 0 ||
		limits.MaxRelayEnvelopes > implementationCeilings.MaxRelayEnvelopes:
		return errors.New("max relay envelopes exceeds the BadTower implementation boundary")
	default:
		return nil
	}
}
