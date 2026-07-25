package station

import (
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/profile"
	bolt "go.etcd.io/bbolt"
)

const (
	testMailboxID  = "box_000000000001"
	testEnvelopeID = "env_000000000001"
)

var testNow = time.Date(2029, 12, 31, 23, 0, 0, 0, time.UTC)

func TestLeaseDeliverReceiveAcknowledgeAndCleanup(t *testing.T) {
	t.Parallel()
	now := testNow
	instance := openTestStation(t, profile.Default(), func() time.Time { return now })

	lease, err := instance.Lease(testMailboxID, 60)
	if err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if !lease.LeaseExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("lease expiry = %s", lease.LeaseExpiresAt)
	}
	envelope := testEnvelope(testMailboxID, testEnvelopeID)
	first, err := instance.Deliver(envelope)
	if err != nil || !first.Accepted || first.Duplicate {
		t.Fatalf("first Deliver = %#v, %v", first, err)
	}
	duplicate, err := instance.Deliver(envelope)
	if err != nil || !duplicate.Accepted || !duplicate.Duplicate {
		t.Fatalf("duplicate Deliver = %#v, %v", duplicate, err)
	}
	received, err := instance.Receive(testMailboxID, 100)
	if err != nil || len(received) != 1 || received[0] != envelope {
		t.Fatalf("Receive = %#v, %v", received, err)
	}
	ack, err := instance.Acknowledge(testMailboxID, testEnvelopeID)
	if err != nil || !ack.Acknowledged {
		t.Fatalf("Acknowledge = %#v, %v", ack, err)
	}
	missing, err := instance.Acknowledge(testMailboxID, testEnvelopeID)
	if err != nil || missing.Acknowledged {
		t.Fatalf("missing Acknowledge = %#v, %v", missing, err)
	}
	now = now.Add(61 * time.Second)
	cleanup, err := instance.Cleanup()
	if err != nil || cleanup.RemovedMailboxes != 1 || cleanup.RemovedEnvelopes != 0 {
		t.Fatalf("Cleanup = %#v, %v", cleanup, err)
	}
}

func TestEnvelopeValidation(t *testing.T) {
	t.Parallel()
	instance := openTestStation(t, profile.Default(), func() time.Time { return testNow })
	if _, err := instance.Lease(testMailboxID, 60); err != nil {
		t.Fatalf("Lease: %v", err)
	}

	cases := []Envelope{
		withEnvelope(func(value *Envelope) { value.ContractVersion = "unsupported.relay.v1" }),
		withEnvelope(func(value *Envelope) { value.EnvelopeID = "short" }),
		withEnvelope(func(value *Envelope) { value.MailboxID = "short" }),
		withEnvelope(func(value *Envelope) { value.Ciphertext = "" }),
		withEnvelope(func(value *Envelope) { value.Ciphertext = strings.Repeat("😀", 262_145) }),
		withEnvelope(func(value *Envelope) { value.ExpiresAt = "2030-02-30T00:00:00Z" }),
		withEnvelope(func(value *Envelope) { value.ExpiresAt = "2030-01-02T23:00:00Z" }),
	}
	for _, value := range cases {
		if _, err := instance.Deliver(value); err == nil {
			t.Fatalf("expected envelope rejection: %#v", value)
		}
	}
}

func TestDeduplicationPrecedesQuotaAndPreservesOrder(t *testing.T) {
	t.Parallel()
	custom := profile.Config{
		ProfileVersion:           profile.Version,
		AcceptedContractVersions: []string{"licoarc.relay.v1"},
		Limits:                   profile.ImplementationCeilings(),
	}
	custom.Limits.MaxMailboxEnvelopes = 2
	custom.Limits.MaxRelayEnvelopes = 2
	relayProfile, err := profile.New(custom)
	if err != nil {
		t.Fatalf("profile.New: %v", err)
	}
	instance := openTestStation(t, relayProfile, func() time.Time { return testNow })
	if _, err := instance.Lease(testMailboxID, 60); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	first := testEnvelope(testMailboxID, "env_000000000001")
	second := testEnvelope(testMailboxID, "env_000000000002")
	for _, value := range []Envelope{first, second} {
		if _, err := instance.Deliver(value); err != nil {
			t.Fatalf("Deliver: %v", err)
		}
	}
	if result, err := instance.Deliver(first); err != nil || !result.Duplicate {
		t.Fatalf("duplicate at quota = %#v, %v", result, err)
	}
	conflict := first
	conflict.Ciphertext = "different-ciphertext"
	if _, err := instance.Deliver(conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict error = %v", err)
	}
	third := testEnvelope(testMailboxID, "env_000000000003")
	if _, err := instance.Deliver(third); !errors.Is(err, ErrQuota) {
		t.Fatalf("quota error = %v", err)
	}
	received, err := instance.Receive(testMailboxID, 2)
	if err != nil || len(received) != 2 ||
		received[0].EnvelopeID != first.EnvelopeID ||
		received[1].EnvelopeID != second.EnvelopeID {
		t.Fatalf("ordered Receive = %#v, %v", received, err)
	}
}

func TestExpiredMailboxDoesNotReviveState(t *testing.T) {
	t.Parallel()
	now := testNow
	instance := openTestStation(t, profile.Default(), func() time.Time { return now })
	if _, err := instance.Lease(testMailboxID, 1); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if _, err := instance.Deliver(testEnvelope(testMailboxID, testEnvelopeID)); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	now = now.Add(2 * time.Second)
	if _, err := instance.Lease(testMailboxID, 60); err != nil {
		t.Fatalf("renew Lease: %v", err)
	}
	received, err := instance.Receive(testMailboxID, 100)
	if err != nil || len(received) != 0 {
		t.Fatalf("Receive after re-lease = %#v, %v", received, err)
	}
}

func TestExpiredEnvelopeReclaimsQuota(t *testing.T) {
	t.Parallel()
	now := testNow
	custom := profile.Config{
		ProfileVersion:           profile.Version,
		AcceptedContractVersions: []string{"licoarc.relay.v1"},
		Limits:                   profile.ImplementationCeilings(),
	}
	custom.Limits.MaxMailboxEnvelopes = 1
	custom.Limits.MaxRelayEnvelopes = 1
	relayProfile, err := profile.New(custom)
	if err != nil {
		t.Fatalf("profile.New: %v", err)
	}
	instance := openTestStation(t, relayProfile, func() time.Time { return now })
	if _, err := instance.Lease(testMailboxID, 60); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	expiring := testEnvelope(testMailboxID, testEnvelopeID)
	expiring.ExpiresAt = now.Add(time.Second).Format(time.RFC3339Nano)
	if _, err := instance.Deliver(expiring); err != nil {
		t.Fatalf("Deliver expiring: %v", err)
	}
	now = now.Add(2 * time.Second)
	replacement := testEnvelope(testMailboxID, "env_000000000002")
	if _, err := instance.Deliver(replacement); err != nil {
		t.Fatalf("Deliver replacement: %v", err)
	}
}

func TestMailboxQuotaReclaimsExpiredMailbox(t *testing.T) {
	t.Parallel()
	now := testNow
	custom := profile.Config{
		ProfileVersion:           profile.Version,
		AcceptedContractVersions: []string{"licoarc.relay.v1"},
		Limits:                   profile.ImplementationCeilings(),
	}
	custom.Limits.MaxMailboxes = 1
	relayProfile, err := profile.New(custom)
	if err != nil {
		t.Fatalf("profile.New: %v", err)
	}
	instance := openTestStation(t, relayProfile, func() time.Time { return now })
	if _, err := instance.Lease(testMailboxID, 1); err != nil {
		t.Fatalf("Lease first: %v", err)
	}
	if _, err := instance.Lease("box_000000000002", 60); !errors.Is(err, ErrQuota) {
		t.Fatalf("mailbox quota error = %v", err)
	}
	now = now.Add(2 * time.Second)
	if _, err := instance.Lease("box_000000000002", 60); err != nil {
		t.Fatalf("Lease after reclaim: %v", err)
	}
}

func TestCleanupCountsExpiredMailboxContents(t *testing.T) {
	t.Parallel()
	now := testNow
	instance := openTestStation(t, profile.Default(), func() time.Time { return now })
	if _, err := instance.Lease(testMailboxID, 1); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	if _, err := instance.Deliver(testEnvelope(testMailboxID, testEnvelopeID)); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	now = now.Add(2 * time.Second)
	result, err := instance.Cleanup()
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if result.RemovedMailboxes != 1 || result.RemovedEnvelopes != 1 {
		t.Fatalf("Cleanup = %#v", result)
	}
}

func TestConcurrentDuplicateDeliveryIsAtomic(t *testing.T) {
	t.Parallel()
	instance := openTestStation(t, profile.Default(), func() time.Time { return testNow })
	if _, err := instance.Lease(testMailboxID, 60); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	envelope := testEnvelope(testMailboxID, testEnvelopeID)
	var accepted atomic.Int32
	var duplicates atomic.Int32
	var wait sync.WaitGroup
	errorsChannel := make(chan error, 32)
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := instance.Deliver(envelope)
			if err != nil {
				errorsChannel <- err
				return
			}
			accepted.Add(1)
			if result.Duplicate {
				duplicates.Add(1)
			}
		}()
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("Deliver: %v", err)
	}
	if accepted.Load() != 32 || duplicates.Load() != 31 {
		t.Fatalf("accepted = %d, duplicates = %d", accepted.Load(), duplicates.Load())
	}
}

func TestStatePersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	now := testNow
	path := filepath.Join(t.TempDir(), "station.db")
	first, err := Open(path, profile.Default(), func() time.Time { return now })
	if err != nil {
		t.Fatalf("Open first: %v", err)
	}
	if _, err := first.Lease(testMailboxID, 60); err != nil {
		t.Fatalf("Lease: %v", err)
	}
	envelope := testEnvelope(testMailboxID, testEnvelopeID)
	if _, err := first.Deliver(envelope); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close first: %v", err)
	}

	second, err := Open(path, profile.Default(), func() time.Time { return now })
	if err != nil {
		t.Fatalf("Open second: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })
	received, err := second.Receive(testMailboxID, 100)
	if err != nil || len(received) != 1 || received[0] != envelope {
		t.Fatalf("Receive after restart = %#v, %v", received, err)
	}
}

func TestOpenRejectsUnknownStoreVersion(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "station.db")
	database, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("bolt.Open: %v", err)
	}
	if err := database.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(mailboxesBucket); err != nil {
			return err
		}
		metadata, err := tx.CreateBucketIfNotExists(metadataBucket)
		if err != nil {
			return err
		}
		return metadata.Put(storeVersionKey, []byte("unknown.store.v1"))
	}); err != nil {
		t.Fatalf("seed database: %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("close database: %v", err)
	}
	if _, err := Open(path, profile.Default(), time.Now); err == nil {
		t.Fatal("expected unknown store version rejection")
	}
}

func TestLeaseAndReceiveInputsFailClosed(t *testing.T) {
	t.Parallel()
	instance := openTestStation(t, profile.Default(), func() time.Time { return testNow })
	if _, err := instance.Lease(testMailboxID, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero lease error = %v", err)
	}
	if _, err := instance.Receive(testMailboxID, 0); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero receive limit error = %v", err)
	}
	if _, err := instance.Receive(testMailboxID, 101); !errors.Is(err, ErrInvalid) {
		t.Fatalf("large receive limit error = %v", err)
	}
}

func openTestStation(t *testing.T, relayProfile profile.Profile, now Clock) *Station {
	t.Helper()
	instance, err := Open(filepath.Join(t.TempDir(), "station.db"), relayProfile, now)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() {
		if err := instance.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return instance
}

func testEnvelope(mailboxID, envelopeID string) Envelope {
	return Envelope{
		ContractVersion: "licoarc.relay.v1",
		EnvelopeID:      envelopeID,
		MailboxID:       mailboxID,
		Ciphertext:      "synthetic-ciphertext",
		ExpiresAt:       "2030-01-01T00:00:00.000Z",
	}
}

func withEnvelope(change func(*Envelope)) Envelope {
	value := testEnvelope(testMailboxID, testEnvelopeID)
	change(&value)
	return value
}
