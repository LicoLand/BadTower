// Package station implements BadTower's durable, opaque transport station.
package station

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/LicoLand/BadTower/internal/profile"
	bolt "go.etcd.io/bbolt"
)

var (
	mailboxesBucket  = []byte("mailboxes")
	metadataBucket   = []byte("metadata")
	itemsBucket      = []byte("items")
	idsBucket        = []byte("ids")
	leaseKey         = []byte("lease_expires_at")
	mailboxCountKey  = []byte("mailbox_count")
	envelopeCountKey = []byte("envelope_count")
	storeVersionKey  = []byte("store_version")
	storeVersion     = []byte("badtower.station-store.v1")

	opaqueIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{16,128}$`)
	rfc3339Pattern  = regexp.MustCompile(
		`^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[Zz]|[+-]\d{2}:\d{2})$`,
	)
)

type Clock func() time.Time

type Envelope struct {
	ContractVersion string `json:"contractVersion"`
	EnvelopeID      string `json:"envelopeId"`
	MailboxID       string `json:"mailboxId"`
	Ciphertext      string `json:"ciphertext"`
	ExpiresAt       string `json:"expiresAt"`
}

type Lease struct {
	MailboxID      string    `json:"mailboxId"`
	LeaseExpiresAt time.Time `json:"leaseExpiresAt"`
}

type Delivery struct {
	Accepted  bool `json:"accepted"`
	Duplicate bool `json:"duplicate"`
}

type Acknowledgement struct {
	Acknowledged bool `json:"acknowledged"`
}

type CleanupResult struct {
	RemovedEnvelopes int `json:"removedEnvelopes"`
	RemovedMailboxes int `json:"removedMailboxes"`
}

type Station struct {
	db      *bolt.DB
	profile profile.Profile
	now     Clock
}

func Open(path string, relayProfile profile.Profile, now Clock) (*Station, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%w: data path is required", ErrInvalid)
	}
	if now == nil {
		now = time.Now
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create data directory: %w", err)
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, fmt.Errorf("open station store: %w", err)
	}
	instance := &Station{db: db, profile: relayProfile, now: now}
	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(mailboxesBucket); err != nil {
			return err
		}
		metadata, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		currentVersion := metadata.Get(storeVersionKey)
		if currentVersion == nil {
			return metadata.Put(storeVersionKey, storeVersion)
		}
		if string(currentVersion) != string(storeVersion) {
			return errors.New("unsupported station store version")
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize station store: %w", err)
	}
	return instance, nil
}

func (station *Station) Close() error {
	return station.db.Close()
}

func (station *Station) Lease(mailboxID string, requestedSeconds int64) (Lease, error) {
	if err := requireOpaqueID(mailboxID, "mailboxId"); err != nil {
		return Lease{}, err
	}
	if requestedSeconds <= 0 {
		return Lease{}, fmt.Errorf("%w: lease duration must be positive", ErrInvalid)
	}
	limits := station.profile.Limits()
	if requestedSeconds > limits.MaxLeaseSeconds {
		requestedSeconds = limits.MaxLeaseSeconds
	}
	now := station.now().UTC()
	expiresAt := now.Add(time.Duration(requestedSeconds) * time.Second)

	err := station.db.Update(func(tx *bolt.Tx) error {
		mailboxes := tx.Bucket(mailboxesBucket)
		metadata := tx.Bucket(metadataBucket)
		mailbox := mailboxes.Bucket([]byte(mailboxID))
		if mailbox != nil && !mailboxLease(mailbox).After(now) {
			if err := deleteMailbox(mailboxes, metadata, []byte(mailboxID)); err != nil {
				return err
			}
			mailbox = nil
		}
		if mailbox == nil {
			if int(readCount(metadata, mailboxCountKey)) >= limits.MaxMailboxes {
				if _, err := cleanupTransaction(mailboxes, metadata, now); err != nil {
					return err
				}
			}
			if int(readCount(metadata, mailboxCountKey)) >= limits.MaxMailboxes {
				return fmt.Errorf("%w: mailbox limit reached", ErrQuota)
			}
			var err error
			mailbox, err = mailboxes.CreateBucket([]byte(mailboxID))
			if err != nil {
				return err
			}
			if _, err := mailbox.CreateBucket(itemsBucket); err != nil {
				return err
			}
			if _, err := mailbox.CreateBucket(idsBucket); err != nil {
				return err
			}
			if err := writeCount(
				metadata,
				mailboxCountKey,
				readCount(metadata, mailboxCountKey)+1,
			); err != nil {
				return err
			}
		}
		return mailbox.Put(leaseKey, encodeInt64(expiresAt.UnixNano()))
	})
	if err != nil {
		return Lease{}, err
	}
	return Lease{MailboxID: mailboxID, LeaseExpiresAt: expiresAt}, nil
}

func (station *Station) Deliver(envelope Envelope) (Delivery, error) {
	now := station.now().UTC()
	_, err := station.validateEnvelope(envelope, now)
	if err != nil {
		return Delivery{}, err
	}
	limits := station.profile.Limits()
	result := Delivery{}

	err = station.db.Update(func(tx *bolt.Tx) error {
		mailboxes := tx.Bucket(mailboxesBucket)
		metadata := tx.Bucket(metadataBucket)
		mailbox, err := activeMailbox(mailboxes, envelope.MailboxID, now)
		if err != nil {
			return err
		}
		if _, err := pruneMailbox(mailbox, metadata, now); err != nil {
			return err
		}
		items := mailbox.Bucket(itemsBucket)
		ids := mailbox.Bucket(idsBucket)
		if sequenceKey := ids.Get([]byte(envelope.EnvelopeID)); sequenceKey != nil {
			var stored Envelope
			if err := json.Unmarshal(items.Get(sequenceKey), &stored); err != nil {
				return fmt.Errorf("decode stored transport unit: %w", err)
			}
			if stored != envelope {
				return ErrConflict
			}
			result = Delivery{Accepted: true, Duplicate: true}
			return nil
		}
		if int(readCount(mailbox, envelopeCountKey)) >= limits.MaxMailboxEnvelopes {
			return fmt.Errorf("%w: mailbox envelope limit reached", ErrQuota)
		}
		if int(readCount(metadata, envelopeCountKey)) >= limits.MaxRelayEnvelopes {
			if _, err := cleanupTransaction(mailboxes, metadata, now); err != nil {
				return err
			}
			mailbox, err = activeMailbox(mailboxes, envelope.MailboxID, now)
			if err != nil {
				return err
			}
			items = mailbox.Bucket(itemsBucket)
			ids = mailbox.Bucket(idsBucket)
		}
		if int(readCount(metadata, envelopeCountKey)) >= limits.MaxRelayEnvelopes {
			return fmt.Errorf("%w: station envelope limit reached", ErrQuota)
		}

		encoded, err := json.Marshal(envelope)
		if err != nil {
			return err
		}
		sequence, err := items.NextSequence()
		if err != nil {
			return err
		}
		sequenceKey := encodeUint64(sequence)
		if err := items.Put(sequenceKey, encoded); err != nil {
			return err
		}
		if err := ids.Put([]byte(envelope.EnvelopeID), sequenceKey); err != nil {
			return err
		}
		if err := writeCount(
			mailbox,
			envelopeCountKey,
			readCount(mailbox, envelopeCountKey)+1,
		); err != nil {
			return err
		}
		if err := writeCount(
			metadata,
			envelopeCountKey,
			readCount(metadata, envelopeCountKey)+1,
		); err != nil {
			return err
		}
		result = Delivery{Accepted: true, Duplicate: false}
		return nil
	})
	return result, err
}

func (station *Station) Receive(mailboxID string, limit int) ([]Envelope, error) {
	if err := requireOpaqueID(mailboxID, "mailboxId"); err != nil {
		return nil, err
	}
	if limit < 1 || limit > 100 {
		return nil, fmt.Errorf("%w: receive limit must be from 1 to 100", ErrInvalid)
	}
	now := station.now().UTC()
	result := make([]Envelope, 0, limit)
	err := station.db.Update(func(tx *bolt.Tx) error {
		metadata := tx.Bucket(metadataBucket)
		mailbox, err := activeMailbox(tx.Bucket(mailboxesBucket), mailboxID, now)
		if err != nil {
			return err
		}
		if _, err := pruneMailbox(mailbox, metadata, now); err != nil {
			return err
		}
		cursor := mailbox.Bucket(itemsBucket).Cursor()
		for key, value := cursor.First(); key != nil && len(result) < limit; key, value = cursor.Next() {
			var envelope Envelope
			if err := json.Unmarshal(value, &envelope); err != nil {
				return fmt.Errorf("decode stored transport unit: %w", err)
			}
			result = append(result, envelope)
		}
		return nil
	})
	return result, err
}

func (station *Station) Acknowledge(mailboxID, envelopeID string) (Acknowledgement, error) {
	if err := requireOpaqueID(mailboxID, "mailboxId"); err != nil {
		return Acknowledgement{}, err
	}
	if err := requireOpaqueID(envelopeID, "envelopeId"); err != nil {
		return Acknowledgement{}, err
	}
	now := station.now().UTC()
	result := Acknowledgement{}
	err := station.db.Update(func(tx *bolt.Tx) error {
		metadata := tx.Bucket(metadataBucket)
		mailbox, err := activeMailbox(tx.Bucket(mailboxesBucket), mailboxID, now)
		if err != nil {
			return err
		}
		if _, err := pruneMailbox(mailbox, metadata, now); err != nil {
			return err
		}
		ids := mailbox.Bucket(idsBucket)
		sequenceKey := append([]byte(nil), ids.Get([]byte(envelopeID))...)
		if sequenceKey == nil {
			return nil
		}
		if err := mailbox.Bucket(itemsBucket).Delete(sequenceKey); err != nil {
			return err
		}
		if err := ids.Delete([]byte(envelopeID)); err != nil {
			return err
		}
		if err := writeCount(
			mailbox,
			envelopeCountKey,
			readCount(mailbox, envelopeCountKey)-1,
		); err != nil {
			return err
		}
		if err := writeCount(
			metadata,
			envelopeCountKey,
			readCount(metadata, envelopeCountKey)-1,
		); err != nil {
			return err
		}
		result.Acknowledged = true
		return nil
	})
	return result, err
}

func (station *Station) Cleanup() (CleanupResult, error) {
	now := station.now().UTC()
	var result CleanupResult
	err := station.db.Update(func(tx *bolt.Tx) error {
		var err error
		result, err = cleanupTransaction(
			tx.Bucket(mailboxesBucket),
			tx.Bucket(metadataBucket),
			now,
		)
		return err
	})
	return result, err
}

func (station *Station) validateEnvelope(envelope Envelope, now time.Time) (time.Time, error) {
	if !station.profile.Accepts(envelope.ContractVersion) {
		return time.Time{}, fmt.Errorf("%w: unsupported relay contract", ErrInvalid)
	}
	if err := requireOpaqueID(envelope.EnvelopeID, "envelopeId"); err != nil {
		return time.Time{}, err
	}
	if err := requireOpaqueID(envelope.MailboxID, "mailboxId"); err != nil {
		return time.Time{}, err
	}
	if envelope.Ciphertext == "" {
		return time.Time{}, fmt.Errorf("%w: ciphertext is required", ErrInvalid)
	}
	if len([]byte(envelope.Ciphertext)) > station.profile.Limits().MaxCiphertextBytes {
		return time.Time{}, fmt.Errorf("%w: ciphertext limit exceeded", ErrQuota)
	}
	expiresAt, err := parseRFC3339(envelope.ExpiresAt)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: expiresAt must be an RFC 3339 date-time", ErrInvalid)
	}
	if !expiresAt.After(now) {
		return time.Time{}, fmt.Errorf("%w: expiresAt must be in the future", ErrInvalid)
	}
	retention := time.Duration(station.profile.Limits().MaxEnvelopeRetentionSeconds) * time.Second
	if expiresAt.Sub(now) > retention {
		return time.Time{}, fmt.Errorf("%w: envelope retention limit exceeded", ErrQuota)
	}
	return expiresAt, nil
}

func activeMailbox(mailboxes *bolt.Bucket, mailboxID string, now time.Time) (*bolt.Bucket, error) {
	mailbox := mailboxes.Bucket([]byte(mailboxID))
	if mailbox == nil || !mailboxLease(mailbox).After(now) {
		return nil, ErrLease
	}
	return mailbox, nil
}

func mailboxLease(mailbox *bolt.Bucket) time.Time {
	raw := mailbox.Get(leaseKey)
	if len(raw) != 8 {
		return time.Time{}
	}
	return time.Unix(0, int64(binary.BigEndian.Uint64(raw))).UTC()
}

func pruneMailbox(mailbox, metadata *bolt.Bucket, now time.Time) (int, error) {
	items := mailbox.Bucket(itemsBucket)
	ids := mailbox.Bucket(idsBucket)
	removed := 0
	cursor := items.Cursor()
	for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
		var envelope Envelope
		if err := json.Unmarshal(value, &envelope); err != nil {
			return 0, fmt.Errorf("decode stored transport unit: %w", err)
		}
		expiresAt, err := parseRFC3339(envelope.ExpiresAt)
		if err != nil {
			return 0, fmt.Errorf("decode stored transport unit expiry: %w", err)
		}
		if !expiresAt.After(now) {
			if err := ids.Delete([]byte(envelope.EnvelopeID)); err != nil {
				return 0, err
			}
			if err := cursor.Delete(); err != nil {
				return 0, err
			}
			removed++
		}
	}
	if removed > 0 {
		if err := writeCount(
			mailbox,
			envelopeCountKey,
			readCount(mailbox, envelopeCountKey)-uint64(removed),
		); err != nil {
			return 0, err
		}
		if err := writeCount(
			metadata,
			envelopeCountKey,
			readCount(metadata, envelopeCountKey)-uint64(removed),
		); err != nil {
			return 0, err
		}
	}
	return removed, nil
}

func cleanupTransaction(mailboxes, metadata *bolt.Bucket, now time.Time) (CleanupResult, error) {
	result := CleanupResult{}
	expiredMailboxes := make([][]byte, 0)
	err := mailboxes.ForEach(func(key, _ []byte) error {
		mailbox := mailboxes.Bucket(key)
		if mailbox == nil {
			return nil
		}
		if !mailboxLease(mailbox).After(now) {
			expiredMailboxes = append(expiredMailboxes, append([]byte(nil), key...))
			return nil
		}
		removed, err := pruneMailbox(mailbox, metadata, now)
		result.RemovedEnvelopes += removed
		return err
	})
	if err != nil {
		return CleanupResult{}, err
	}
	for _, key := range expiredMailboxes {
		mailbox := mailboxes.Bucket(key)
		envelopes := int(readCount(mailbox, envelopeCountKey))
		if err := deleteMailbox(mailboxes, metadata, key); err != nil {
			return CleanupResult{}, err
		}
		result.RemovedEnvelopes += envelopes
		result.RemovedMailboxes++
	}
	return result, nil
}

func deleteMailbox(mailboxes, metadata *bolt.Bucket, key []byte) error {
	mailbox := mailboxes.Bucket(key)
	if mailbox == nil {
		return nil
	}
	envelopes := readCount(mailbox, envelopeCountKey)
	if err := mailboxes.DeleteBucket(key); err != nil {
		return err
	}
	if err := writeCount(
		metadata,
		mailboxCountKey,
		readCount(metadata, mailboxCountKey)-1,
	); err != nil {
		return err
	}
	return writeCount(
		metadata,
		envelopeCountKey,
		readCount(metadata, envelopeCountKey)-envelopes,
	)
}

func requireOpaqueID(value, field string) error {
	if !opaqueIDPattern.MatchString(value) {
		return fmt.Errorf("%w: %s is not a valid opaque identifier", ErrInvalid, field)
	}
	return nil
}

func parseRFC3339(value string) (time.Time, error) {
	if !rfc3339Pattern.MatchString(value) {
		return time.Time{}, errors.New("invalid RFC 3339 syntax")
	}
	normalized := strings.Replace(value, "t", "T", 1)
	if strings.HasSuffix(normalized, "z") {
		normalized = normalized[:len(normalized)-1] + "Z"
	}
	return time.Parse(time.RFC3339Nano, normalized)
}

func readCount(bucket *bolt.Bucket, key []byte) uint64 {
	raw := bucket.Get(key)
	if len(raw) != 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

func writeCount(bucket *bolt.Bucket, key []byte, value uint64) error {
	return bucket.Put(key, encodeUint64(value))
}

func encodeInt64(value int64) []byte {
	return encodeUint64(uint64(value))
}

func encodeUint64(value uint64) []byte {
	result := make([]byte, 8)
	binary.BigEndian.PutUint64(result, value)
	return result
}
