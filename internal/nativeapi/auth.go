package nativeapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"time"
)

// authCredentialEntry binds one credential digest to its namespace and validity window.
type authCredentialEntry struct {
	Namespace string
	NotBefore time.Time
	NotAfter  time.Time
}

// AuthSnapshot maps credential digests to namespaces and validity bounds.
type AuthSnapshot struct {
	NamespaceByDigest map[string]string
	entriesByDigest   map[string]authCredentialEntry
}

// KnownNamespace reports whether one namespace is provisioned.
func (snapshot AuthSnapshot) KnownNamespace(namespace string) bool {
	for _, value := range snapshot.NamespaceByDigest {
		if value == namespace {
			return true
		}
	}
	return false
}

// Authenticator resolves bearer tokens to namespaces fail-closed (C-05).
type Authenticator struct {
	mu       sync.RWMutex
	snapshot AuthSnapshot
}

func buildAuthSnapshot(config Config) (AuthSnapshot, error) {
	snapshot := AuthSnapshot{
		NamespaceByDigest: map[string]string{},
		entriesByDigest:   map[string]authCredentialEntry{},
	}
	for _, namespace := range config.Namespaces {
		for _, credential := range namespace.Credentials {
			if _, duplicate := snapshot.NamespaceByDigest[credential.SHA256]; duplicate {
				return AuthSnapshot{}, errors.New("duplicate credential digest across namespaces")
			}
			snapshot.NamespaceByDigest[credential.SHA256] = namespace.ID
			snapshot.entriesByDigest[credential.SHA256] = authCredentialEntry{
				Namespace: namespace.ID,
				NotBefore: credential.NotBefore,
				NotAfter:  credential.NotAfter,
			}
		}
	}
	return snapshot, nil
}

// NewAuthenticator builds one authenticator from validated configuration.
// Any colliding digest fails closed rather than silently overwriting a binding.
func NewAuthenticator(config Config) *Authenticator {
	snapshot, err := buildAuthSnapshot(config)
	if err != nil {
		return &Authenticator{
			snapshot: AuthSnapshot{
				NamespaceByDigest: map[string]string{},
				entriesByDigest:   map[string]authCredentialEntry{},
			},
		}
	}
	return &Authenticator{snapshot: snapshot}
}

// Reload atomically replaces credential and limit snapshots.
func (auth *Authenticator) Reload(config Config) error {
	snapshot, err := buildAuthSnapshot(config)
	if err != nil {
		return err
	}
	auth.mu.Lock()
	auth.snapshot = snapshot
	auth.mu.Unlock()
	return nil
}

// NamespaceForBearer resolves one bearer token to its namespace.
func (auth *Authenticator) NamespaceForBearer(token string) (string, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return "", stationFailure{Reason: "UNAUTHENTICATED"}
	}
	digest := TokenDigest(token)
	now := time.Now().UTC()
	auth.mu.RLock()
	entry, hasEntry := auth.snapshot.entriesByDigest[digest]
	namespace, hasNamespace := auth.snapshot.NamespaceByDigest[digest]
	auth.mu.RUnlock()
	if hasEntry {
		if (!entry.NotBefore.IsZero() && now.Before(entry.NotBefore)) || (!entry.NotAfter.IsZero() && now.After(entry.NotAfter)) {
			return "", stationFailure{Reason: "UNAUTHENTICATED"}
		}
		return entry.Namespace, nil
	}
	if !hasNamespace {
		return "", stationFailure{Reason: "UNAUTHENTICATED"}
	}
	return namespace, nil
}

// TokenDigest returns the unpadded base64url SHA-256 digest for one token.
func TokenDigest(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// GenerateToken creates one 32-byte random bearer token and its digest.
func GenerateToken(raw []byte) (token string, digest string, err error) {
	if len(raw) != 32 {
		return "", "", errors.New("token material must be 32 bytes")
	}
	token = base64.RawURLEncoding.EncodeToString(raw)
	digest = TokenDigest(token)
	return token, digest, nil
}

func constantTimeDigestEqual(expected, actual string) bool {
	if len(expected) != len(actual) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(expected), []byte(actual)) == 1
}
