package stationcore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strconv"
	"testing"
	"time"
)

type testClock struct {
	wall      time.Time
	monoStart time.Time
	monoNow   time.Duration
}

func newTestClock(start time.Time) *testClock {
	return &testClock{wall: start, monoStart: start, monoNow: 0}
}

func (clock *testClock) NowUnixMs() int64 { return clock.wall.UnixMilli() }

func (clock *testClock) MonotonicNow() time.Duration { return clock.monoNow }

func (clock *testClock) Advance(d time.Duration) {
	clock.wall = clock.wall.Add(d)
	clock.monoNow += d
}

func openTestCore(t *testing.T, clock *testClock) *Core {
	t.Helper()
	core, err := Open(Config{
		DataRoot:  t.TempDir(),
		ProfileID: ProfileSingleNodeGeneralV1,
	}, clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = core.Close() })
	return core
}

func reserveAddress(t *testing.T, core *Core, namespace, requestID string, ttlMs uint64) string {
	t.Helper()
	out, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       ttlMs,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if out.AddressID == "" || !out.Created {
		t.Fatalf("Reserve created = false or empty address: %#v", out)
	}
	return out.AddressID
}

func submitUnit(t *testing.T, core *Core, namespace, requestID, address string, payload []byte) string {
	t.Helper()
	out, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		AddressID:   address,
		Payload:     payload,
		TTLMs:       86_400_000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if out.UnitID == "" {
		t.Fatal("Submit returned empty unit_id")
	}
	return out.UnitID
}

func TestModelAddressLifecycleAbsentLiveExpiredDeleted(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_AAAAAAAAAAAAAAAAAAAAAA"

	create, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_createAddressLifecycle01",
		TTLMs:       120_000,
	})
	if err != nil {
		t.Fatalf("Reserve(create): %v", err)
	}
	if !create.Created {
		t.Fatal("expected created address")
	}
	expiry := create.ExpiresAtUnixMs

	renew, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_renewAddressLifecycle01",
		AddressID:   create.AddressID,
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("Reserve(renew): %v", err)
	}
	if renew.Created || renew.ExpiresAtUnixMs < expiry {
		t.Fatalf("renew must extend expiry: before=%d after=%d", expiry, renew.ExpiresAtUnixMs)
	}

	// Create TTL is 120s; renew TTL 60s must not shorten (C-06 max rule). Advance past
	// the retained create expiry before expecting ADDRESS_EXPIRED.
	clock.Advance(121 * time.Second)
	_, err = core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_submitAfterExpiry001",
		AddressID:   create.AddressID,
		Payload:     []byte("x"),
		TTLMs:       60_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonAddressExpired {
		t.Fatalf("Submit on expired address: err=%v", err)
	}
}

func TestModelUnitFIFOEligibleOrdering(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 1, 0, 0, 0, time.UTC)))
	namespace := "ns_BBBBBBBBBBBBBBBBBBBBBB"
	address := reserveAddress(t, core, namespace, "rq_fifoReserve00000001", 3_600_000)

	first := submitUnit(t, core, namespace, "rq_fifoSubmit000000001", address, []byte("first"))
	second := submitUnit(t, core, namespace, "rq_fifoSubmit000000002", address, []byte("second"))

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_fifoClaim0000000001",
		AddressID:           address,
		MaxUnits:            2,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(claim.Units) != 2 {
		t.Fatalf("Claim units = %d, want 2", len(claim.Units))
	}
	got := []string{claim.Units[0].UnitID, claim.Units[1].UnitID}
	want := []string{first, second}
	if !slices.Equal(got, want) {
		t.Fatalf("FIFO order = %v, want %v", got, want)
	}
}

func TestModelClaimEpochFencesOwnership(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 2, 0, 0, 0, time.UTC)))
	namespace := "ns_CCCCCCCCCCCCCCCCCCCCCC"
	address := reserveAddress(t, core, namespace, "rq_epochReserve0000001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_epochSubmit00000001", address, []byte("payload"))

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_epochClaim000000001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_epochSettleStale001",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch + 1,
		Entries: []SettleEntry{{
			UnitID: unitID,
			Action: SettleComplete,
		}},
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonClaimStale {
		t.Fatalf("stale epoch Settle: err=%v", err)
	}
}

func TestModelSubmitSyncBoundaryAcknowledgesOnlyAfterDurability(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 3, 0, 0, 0, time.UTC)))
	namespace := "ns_DDDDDDDDDDDDDDDDDDDDDD"
	address := reserveAddress(t, core, namespace, "rq_syncReserve00000001", 3_600_000)

	out, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_syncSubmit000000001",
		AddressID:   address,
		Payload:     []byte("durable"),
		TTLMs:       86_400_000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := core.Restart(); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_syncClaim0000000001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim after restart: %v", err)
	}
	if len(claim.Units) != 1 || claim.Units[0].UnitID != out.UnitID {
		t.Fatalf("admitted unit missing after restart: %#v", claim)
	}
}

func TestIdempotencyExactReplayReturnsStoredResult(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 4, 0, 0, 0, time.UTC)))
	namespace := "ns_EEEEEEEEEEEEEEEEEEEEEE"
	requestID := "rq_idempotentReplay0001"

	first, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       86_400_000,
	})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	second, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       86_400_000,
	})
	if err != nil {
		t.Fatalf("replay Reserve: %v", err)
	}
	if first != second {
		t.Fatalf("idempotent replay mismatch: first=%#v second=%#v", first, second)
	}
}

func TestIdempotencyConflictingFingerprintNeverMutates(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 5, 0, 0, 0, time.UTC)))
	namespace := "ns_FFFFFFFFFFFFFFFFFFFFFF"
	requestID := "rq_idempotencyConflict1"

	if _, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       86_400_000,
	}); err != nil {
		t.Fatalf("first Reserve: %v", err)
	}
	_, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       3_600_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonIdempotencyConflict {
		t.Fatalf("conflicting replay: err=%v", err)
	}
}

func TestIdempotencySettlementWholeRequestAtomic(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 6, 0, 0, 0, time.UTC)))
	namespace := "ns_GGGGGGGGGGGGGGGGGGGGGG"
	address := reserveAddress(t, core, namespace, "rq_settleAtomicRes001", 3_600_000)
	first := submitUnit(t, core, namespace, "rq_settleAtomicSub001", address, []byte("a"))
	second := submitUnit(t, core, namespace, "rq_settleAtomicSub002", address, []byte("b"))

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_settleAtomicClm001",
		AddressID:           address,
		MaxUnits:            2,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_settleAtomicBad001",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{
			{UnitID: first, Action: SettleComplete},
			{UnitID: second, Action: SettleAction(99)},
		},
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonInvalidRequest {
		t.Fatalf("invalid settle entry must reject whole batch: err=%v", err)
	}

	again, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_settleAtomicClm002",
		AddressID:           address,
		MaxUnits:            2,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim after rejected Settle: %v", err)
	}
	if len(again.Units) != 2 {
		t.Fatalf("units must remain claimed after rejected Settle: %#v", again)
	}
}

func TestModelClaimEmptyWaitSucceedsWithoutClaim(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 21, 0, 0, 0, time.UTC)))
	namespace := "ns_aaaaaaaaaaaaaaaaaaaaaa"
	address := reserveAddress(t, core, namespace, "rq_emptyWaitRes00001", 3_600_000)

	claimed, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace,
		RequestID:   "rq_emptyWaitClm00001",
		AddressID:   address,
		MaxUnits:    1,
		MaxBytes:    1024,
	})
	if err != nil {
		t.Fatalf("empty wait Claim: %v", err)
	}
	if claimed.ClaimID != "" || claimed.ClaimEpoch != 0 || len(claimed.Units) != 0 {
		t.Fatalf("empty wait must succeed with no claim: %#v", claimed)
	}

	// All-zero bounds select profile defaults and must still succeed empty.
	defaults, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace,
		RequestID:   "rq_emptyWaitClm00002",
		AddressID:   address,
	})
	if err != nil {
		t.Fatalf("default-bounds empty Claim: %v", err)
	}
	if defaults.ClaimID != "" || len(defaults.Units) != 0 {
		t.Fatalf("default-bounds empty Claim must not fabricate a claim: %#v", defaults)
	}
}

func TestModelReleaseDelayedRedeliveryIncrementsAttemptAndEpoch(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 22, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_bbbbbbbbbbbbbbbbbbbbbb"
	address := reserveAddress(t, core, namespace, "rq_releaseRes0000001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_releaseSub0000001", address, []byte("redeliver"))

	first, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_releaseClm0000001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("first Claim: %v", err)
	}
	if first.Units[0].DeliveryAttempt != 1 {
		t.Fatalf("first delivery attempt = %d, want 1", first.Units[0].DeliveryAttempt)
	}

	settled, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_releaseSet0000001",
		ClaimID:     first.ClaimID,
		ClaimEpoch:  first.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID:  unitID,
			Action:  SettleRelease,
			DelayMs: 5_000,
		}},
	})
	if err != nil {
		t.Fatalf("RELEASE: %v", err)
	}
	if settled.Units[0].Outcome != SettleOutcomeReleased {
		t.Fatalf("RELEASE outcome = %q, want %q", settled.Units[0].Outcome, SettleOutcomeReleased)
	}

	clock.Advance(5 * time.Second)
	second, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_releaseClm0000002",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim after release delay: %v", err)
	}
	if len(second.Units) != 1 || second.Units[0].UnitID != unitID {
		t.Fatalf("released unit not redelivered: %#v", second)
	}
	if second.Units[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivery attempt = %d, want 2", second.Units[0].DeliveryAttempt)
	}
	if second.ClaimEpoch <= first.ClaimEpoch {
		t.Fatalf("claim epoch must increase per claim: first=%d second=%d", first.ClaimEpoch, second.ClaimEpoch)
	}
}

func TestModelAttemptLimitTerminatesRedelivery(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 23, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_cccccccccccccccccccccc"
	address := reserveAddress(t, core, namespace, "rq_attemptRes00000001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_attemptSub00000001", address, []byte("bounded"))

	for attempt := 1; attempt <= maxDeliveryAttempts; attempt++ {
		claim, err := core.Claim(context.Background(), ClaimInput{
			NamespaceID:         namespace,
			RequestID:           "rq_attemptClm" + strconv.Itoa(attempt),
			AddressID:           address,
			MaxUnits:            1,
			MaxBytes:            1 << 20,
			VisibilityTimeoutMs: 30_000,
		})
		if err != nil {
			t.Fatalf("Claim attempt %d: %v", attempt, err)
		}
		if len(claim.Units) != 1 {
			t.Fatalf("Claim attempt %d returned no unit: %#v", attempt, claim)
		}
		if int(claim.Units[0].DeliveryAttempt) != attempt {
			t.Fatalf("delivery attempt = %d, want %d", claim.Units[0].DeliveryAttempt, attempt)
		}

		settled, err := core.Settle(context.Background(), SettleInput{
			NamespaceID: namespace,
			RequestID:   "rq_attemptSet" + strconv.Itoa(attempt),
			ClaimID:     claim.ClaimID,
			ClaimEpoch:  claim.ClaimEpoch,
			Entries: []SettleEntry{{
				UnitID:  unitID,
				Action:  SettleRelease,
				DelayMs: 1_000,
			}},
		})
		if err != nil {
			t.Fatalf("RELEASE attempt %d: %v", attempt, err)
		}
		if attempt < maxDeliveryAttempts {
			if settled.Units[0].Outcome != SettleOutcomeReleased {
				t.Fatalf("attempt %d RELEASE outcome = %q", attempt, settled.Units[0].Outcome)
			}
			clock.Advance(1 * time.Second)
			continue
		}
		if settled.Units[0].Outcome != SettleOutcomeAttemptLimit {
			t.Fatalf("attempt %d RELEASE outcome = %q, want %q",
				attempt, settled.Units[0].Outcome, SettleOutcomeAttemptLimit)
		}
	}

	after, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace,
		RequestID:   "rq_attemptClmFinal001",
		AddressID:   address,
		MaxUnits:    1,
		MaxBytes:    1 << 20,
	})
	if err != nil {
		t.Fatalf("Claim after attempt limit: %v", err)
	}
	if after.ClaimID != "" || len(after.Units) != 0 {
		t.Fatalf("attempt-limited unit must never be redelivered: %#v", after)
	}
}

func TestModelExtendCappedByUnitExpiryAndTenMinutes(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 0, 30, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_dddddddddddddddddddddd"
	address := reserveAddress(t, core, namespace, "rq_extendRes00000001", 3_600_000)

	submitted, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_extendSub00000001",
		AddressID:   address,
		Payload:     []byte("extend"),
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	unitExpiry := submitted.ExpiresAtUnixMs

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_extendClm00000001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	settled, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_extendSet00000001",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID:      submitted.UnitID,
			Action:      SettleExtend,
			ExtensionMs: 600_000,
		}},
	})
	if err != nil {
		t.Fatalf("EXTEND: %v", err)
	}
	if settled.Units[0].Outcome != SettleOutcomeExtended {
		t.Fatalf("EXTEND outcome = %q, want %q", settled.Units[0].Outcome, SettleOutcomeExtended)
	}
	if settled.Units[0].EffectiveAtUnixMs != unitExpiry {
		t.Fatalf("EXTEND must cap at unit expiry: effective=%d unitExpiry=%d",
			settled.Units[0].EffectiveAtUnixMs, unitExpiry)
	}

	// EXTEND is not full settlement (C-09): the same current claim and epoch
	// must still fence the unit, so a later COMPLETE on the same claim works.
	completed, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_extendComplete001",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: submitted.UnitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatalf("COMPLETE after EXTEND on same claim: %v", err)
	}
	if completed.Units[0].Outcome != SettleOutcomeCompleted {
		t.Fatalf("COMPLETE outcome = %q", completed.Units[0].Outcome)
	}
}

func TestModelStaleAndExpiredClaimsNeverMutateUnits(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 1, 30, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_eeeeeeeeeeeeeeeeeeeeee"
	address := reserveAddress(t, core, namespace, "rq_staleRes000000001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_staleSub000000001", address, []byte("fenced"))

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_staleClm00000001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 1_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// A well-formed but unknown claim ID is stale, never a wildcard match.
	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_staleSet000000001",
		ClaimID:     "dc_aaaaaaaaaaaaaaaaaaaaaa",
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: unitID,
			Action: SettleComplete,
		}},
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonClaimStale {
		t.Fatalf("unknown claim id: err=%v", err)
	}

	// The real claim still owns the unit: another Claim returns the active claim.
	again, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_staleClm00000002",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim after stale settle: %v", err)
	}
	if again.ClaimID != claim.ClaimID || len(again.Units) != 1 {
		t.Fatalf("rejected settle must not mutate ownership: %#v", again)
	}

	// Expired claim fences nothing.
	clock.Advance(2 * time.Second)
	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_staleExpiredSet01",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: unitID,
			Action: SettleComplete,
		}},
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonClaimExpired {
		t.Fatalf("settle with expired claim: err=%v", err)
	}
}

func TestModelClaimTimeoutRedeliversWithinDeadline(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 2, 30, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_ffffffffffffffffffffff"
	address := reserveAddress(t, core, namespace, "rq_timeoutRes0000001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_timeoutSub0000001", address, []byte("redeliver"))

	first, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_timeoutClm000001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 1_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	clock.Advance(2 * time.Second)

	// The deadline scanner must move the timed-out claim's units to DELAYED and
	// then back to eligible (C-08, REQ-010). Poll within the maintenance budget:
	// an attempt-1 return is the still-active old claim and means the scanner
	// has not yet redelivered, so keep polling.
	deadline := time.Now().Add(5 * time.Second)
	for {
		redelivered, err := core.Claim(context.Background(), ClaimInput{
			NamespaceID: namespace,
			RequestID:   "rq_timeoutClm000002",
			AddressID:   address,
			MaxUnits:    1,
			MaxBytes:    1 << 20,
		})
		if err != nil {
			t.Fatalf("Claim after timeout: %v", err)
		}
		if len(redelivered.Units) == 1 && redelivered.Units[0].UnitID == unitID {
			if redelivered.Units[0].DeliveryAttempt == 2 {
				if redelivered.ClaimEpoch <= first.ClaimEpoch {
					t.Fatalf("redelivery epoch %d must exceed %d", redelivered.ClaimEpoch, first.ClaimEpoch)
				}
				return
			}
			if redelivered.Units[0].DeliveryAttempt != 1 {
				t.Fatalf("unexpected delivery attempt = %d", redelivered.Units[0].DeliveryAttempt)
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("timed-out claim unit was never redelivered within the maintenance budget")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestIdempotencyRecordExpiresAfterOneHour(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 3, 30, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_gggggggggggggggggggggg"
	requestID := "rq_retentionReplay001"

	first, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       86_400_000,
	})
	if err != nil {
		t.Fatalf("first Reserve: %v", err)
	}

	clock.Advance(61 * time.Minute)
	second, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       86_400_000,
	})
	if err != nil {
		t.Fatalf("Reserve after one-hour retention: %v", err)
	}
	if !second.Created || second.AddressID == first.AddressID {
		t.Fatalf("expired idempotency record must not replay: first=%#v second=%#v", first, second)
	}
}

func TestModelRequestFingerprintDomainSeparation(t *testing.T) {
	t.Parallel()
	namespace := "ns_hhhhhhhhhhhhhhhhhhhhhh"
	request := "rq_fingerprintSep001"
	address := "sa_aaaaaaaaaaaaaaaaaaaaaa"
	payload := []byte("canonical")
	ttl := uint64(60_000)

	submit := func(ns, operation, requestID, addressID string, payload []byte, ttlMs uint64) []byte {
		return RequestFingerprint(ns, operation,
			FingerprintField{1, []byte(requestID)},
			FingerprintField{2, []byte(addressID)},
			FingerprintField{3, payloadCanonicalValue(payload)},
			FingerprintField{4, uint64BytesForTest(ttlMs)},
		)
	}
	base := submit(namespace, "submit", request, address, payload, ttl)
	if len(base) != 32 {
		t.Fatalf("fingerprint length = %d, want 32", len(base))
	}
	if !bytesEqual(submit(namespace, "submit", request, address, payload, ttl), base) {
		t.Fatal("identical inputs must produce the identical fingerprint")
	}
	cases := []struct {
		name string
		fp   []byte
	}{
		{"different namespace", submit(namespace+"X", "submit", request, address, payload, ttl)},
		{"different operation", submit(namespace, "reserve", request, address, payload, ttl)},
		{"different request id", submit(namespace, "submit", request+"X", address, payload, ttl)},
		{"different address", submit(namespace, "submit", request, "sa_bbbbbbbbbbbbbbbbbbbbbb", payload, ttl)},
		{"different payload", submit(namespace, "submit", request, address, []byte("other"), ttl)},
		{"different ttl", submit(namespace, "submit", request, address, payload, ttl+1)},
	}
	for _, testCase := range cases {
		if bytesEqual(testCase.fp, base) {
			t.Fatalf("%s must change the fingerprint", testCase.name)
		}
	}
}

func payloadCanonicalValue(payload []byte) []byte {
	value := make([]byte, 8+32)
	binary.BigEndian.PutUint64(value, uint64(len(payload)))
	sum := sha256.Sum256(payload)
	copy(value[8:], sum[:])
	return value
}

func uint64BytesForTest(value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return buf
}
