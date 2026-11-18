package stationcore

// Executor must provide:
//   - Clock with persisted wall watermark (C-10)
//   - func (*Core) Ready() (bool, Reason)
//   - foreground expiry enforcement on Reserve/Submit/Claim
//   - backward clock clamp up to 60s; fail readiness beyond 60s as CLOCK_UNSAFE

import (
	"context"
	"testing"
	"time"
)

func TestClockBackwardJumpWithinClampNeverExtendsExpiry(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 31, 10, 0, 0, 0, time.UTC)
	clock := newTestClock(start)
	core := openTestCore(t, clock)
	namespace := "ns_KKKKKKKKKKKKKKKKKKKKKK"

	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_clockClampRes00001",
		TTLMs:       300_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	expiry := reserved.ExpiresAtUnixMs

	clock.wall = start.Add(-30 * time.Second)
	_, err = core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_clockClampSub00001",
		AddressID:   reserved.AddressID,
		Payload:     []byte("x"),
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("Submit after small backward jump: %v", err)
	}

	renewed, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_clockClampRenew001",
		AddressID:   reserved.AddressID,
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("renew after clamp: %v", err)
	}
	if renewed.ExpiresAtUnixMs < expiry {
		t.Fatalf("clamp must not shrink effective expiry: before=%d after=%d", expiry, renewed.ExpiresAtUnixMs)
	}
}

func TestClockBackwardJumpOverThresholdFailsReadiness(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 31, 11, 0, 0, 0, time.UTC)
	clock := newTestClock(start)
	core := openTestCore(t, clock)
	namespace := "ns_LLLLLLLLLLLLLLLLLLLLLL"
	reserveAddress(t, core, namespace, "rq_clockUnsafeRes001", 3_600_000)

	clock.wall = start.Add(-90 * time.Second)
	_, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_clockUnsafeMut001",
		TTLMs:       60_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonClockUnsafe {
		t.Fatalf("large backward jump must block mutation: err=%v", err)
	}
}

func TestClockForwardJumpEnforcesForegroundExpiry(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)
	clock := newTestClock(start)
	core := openTestCore(t, clock)
	namespace := "ns_MMMMMMMMMMMMMMMMMMMMMM"

	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_forwardExpiryRes01",
		TTLMs:       120_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	clock.Advance(3 * time.Minute)
	_, err = core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_forwardExpirySub01",
		AddressID:   reserved.AddressID,
		Payload:     []byte("late"),
		TTLMs:       60_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonAddressExpired {
		t.Fatalf("forward jump must expire address in foreground: err=%v", err)
	}
}

func TestExpiryUnitDeadlineNotLaterThanAddress(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 13, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_NNNNNNNNNNNNNNNNNNNNNN"

	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_unitCapRes00000001",
		TTLMs:       120_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}

	submitted, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_unitCapSubmit0001",
		AddressID:   reserved.AddressID,
		Payload:     []byte("capped"),
		TTLMs:       7 * 24 * 60 * 60 * 1000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if submitted.ExpiresAtUnixMs > reserved.ExpiresAtUnixMs {
		t.Fatalf("unit expiry %d exceeds address expiry %d",
			submitted.ExpiresAtUnixMs, reserved.ExpiresAtUnixMs)
	}
}

func TestExpiryUnitExpiredNeverClaimed(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 13, 30, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_NNNNNNNNNNNNNNNNNNNNNN"

	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_unitExpiredRes001",
		TTLMs:       120_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_unitExpiredSub001",
		AddressID:   reserved.AddressID,
		Payload:     []byte("short-lived"),
		TTLMs:       60_000,
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Address is still live (120 s) but the unit (60 s) is expired: foreground
	// checks must make it semantically unavailable without waiting for cleanup.
	clock.Advance(61 * time.Second)
	claimed, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace,
		RequestID:   "rq_unitExpiredClm001",
		AddressID:   reserved.AddressID,
		MaxUnits:    1,
		MaxBytes:    1 << 20,
	})
	if err != nil {
		t.Fatalf("Claim with expired unit: %v", err)
	}
	if claimed.ClaimID != "" || len(claimed.Units) != 0 {
		t.Fatalf("expired unit must not be claimed: %#v", claimed)
	}
}

func TestExpiryClaimVisibilityCappedByUnitExpiry(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 14, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_OOOOOOOOOOOOOOOOOOOOOO"

	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_claimCapRes0000001",
		TTLMs:       90_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	submitUnit(t, core, namespace, "rq_claimCapSubmit001", reserved.AddressID, []byte("u"))

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_claimCapClaim0001",
		AddressID:           reserved.AddressID,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 600_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if claim.ClaimExpiresAtUnixMs > reserved.ExpiresAtUnixMs {
		t.Fatalf("claim deadline %d exceeds address expiry %d",
			claim.ClaimExpiresAtUnixMs, reserved.ExpiresAtUnixMs)
	}
}
