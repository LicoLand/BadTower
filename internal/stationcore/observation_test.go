package stationcore

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func stationAggregate(addresses, units, retainedBytes, activeClaims, idempotency uint64) StationAggregate {
	return StationAggregate{
		Addresses:          addresses,
		Units:              units,
		RetainedBytes:      retainedBytes,
		ActiveClaims:       activeClaims,
		IdempotencyRecords: idempotency,
	}
}

// assertObservation checks the running station observation against one exact
// aggregate. It never triggers maintenance itself.
func assertObservation(t *testing.T, core *Core, want StationAggregate) {
	t.Helper()
	got := core.ObservationSnapshot()
	if got.Lifecycle != StationLifecycleRunning {
		t.Fatalf("lifecycle = %q, want RUNNING", got.Lifecycle)
	}
	if got.Pressure != PressureNormal {
		t.Fatalf("pressure = %v, want NORMAL", got.Pressure)
	}
	if got.Aggregate != want {
		t.Fatalf("aggregate = %+v, want %+v", got.Aggregate, want)
	}
}

// assertAggregateMatchesNamespaceSum verifies that the derived station-wide
// aggregate equals the sum of the canonical per-namespace usage vectors.
func assertAggregateMatchesNamespaceSum(t *testing.T, core *Core, namespaces ...string) {
	t.Helper()
	got := core.ObservationSnapshot().Aggregate
	var sum ResourceVector
	for _, namespace := range namespaces {
		_, usage := core.NamespacePressure(namespace)
		sum.Addresses += usage.Addresses
		sum.Units += usage.Units
		sum.RetainedBytes += usage.RetainedBytes
		sum.ActiveClaims += usage.ActiveClaims
		sum.IdempotencyRecords += usage.IdempotencyRecords
	}
	if got.Addresses != sum.Addresses || got.Units != sum.Units ||
		got.RetainedBytes != sum.RetainedBytes || got.ActiveClaims != sum.ActiveClaims ||
		got.IdempotencyRecords != sum.IdempotencyRecords {
		t.Fatalf("aggregate %+v != namespace sum %+v", got, sum)
	}
}

func TestObservationAggregateAcrossTwoNamespaces(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 6, 0, 0, 0, time.UTC)))
	ns1 := "ns_AAAAAAAAAAAAAAAAAAAAAA"
	ns2 := "ns_BBBBBBBBBBBBBBBBBBBBBB"

	// Reserve one address in each namespace.
	address1 := reserveAddress(t, core, ns1, "rq_obs2nsReserveA0001", 3_600_000)
	address2 := reserveAddress(t, core, ns2, "rq_obs2nsReserveB0001", 3_600_000)
	assertObservation(t, core, stationAggregate(2, 0, 0, 0, 2))
	assertAggregateMatchesNamespaceSum(t, core, ns1, ns2)

	// Submit one unit in each namespace.
	payload1 := []byte("alpha")
	payload2 := []byte("beta-omega")
	unit1 := submitUnit(t, core, ns1, "rq_obs2nsSubmitA00001", address1, payload1)
	unit2 := submitUnit(t, core, ns2, "rq_obs2nsSubmitB00001", address2, payload2)
	charge := unitLogicalCharge(len(payload1)) + unitLogicalCharge(len(payload2))
	assertObservation(t, core, stationAggregate(2, 2, charge, 0, 4))
	assertAggregateMatchesNamespaceSum(t, core, ns1, ns2)

	// Claim one unit from each namespace.
	claim1 := claimUnits(t, core, ns1, "rq_obs2nsClaimA000001", address1, 1)
	claim2 := claimUnits(t, core, ns2, "rq_obs2nsClaimB000001", address2, 1)
	assertObservation(t, core, stationAggregate(2, 2, charge, 2, 6))
	assertAggregateMatchesNamespaceSum(t, core, ns1, ns2)

	// Settle COMPLETE one unit per namespace; both claims fully settle away.
	settleComplete(t, core, ns1, "rq_obs2nsSettleA00001", claim1, unit1)
	settleComplete(t, core, ns2, "rq_obs2nsSettleB00001", claim2, unit2)
	assertObservation(t, core, stationAggregate(2, 0, 0, 0, 8))
	assertAggregateMatchesNamespaceSum(t, core, ns1, ns2)
}

func TestObservationAggregateReleaseExtendCompleteExpiryReopen(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 7, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_CCCCCCCCCCCCCCCCCCCCCC"

	address := reserveAddress(t, core, namespace, "rq_obsLifeReserve0001", 3_600_000)
	payload := []byte("release-me")
	charge := unitLogicalCharge(len(payload))
	unitID := submitUnit(t, core, namespace, "rq_obsLifeSubmit00001", address, payload)

	// RELEASE keeps the unit and its retained bytes; the fully released claim
	// settles away.
	claim := claimUnits(t, core, namespace, "rq_obsLifeClaim000001", address, 1)
	out := settleEntry(t, core, namespace, "rq_obsLifeRelease0001", claim,
		SettleEntry{UnitID: unitID, Action: SettleRelease})
	if out.Units[0].Outcome != SettleOutcomeReleased {
		t.Fatalf("release outcome = %q, want %q", out.Units[0].Outcome, SettleOutcomeReleased)
	}
	assertObservation(t, core, stationAggregate(1, 1, charge, 0, 4))
	clock.Advance(2 * time.Second)

	// EXTEND keeps the unit claimed; the claim and unit counters both remain.
	claim = claimUnits(t, core, namespace, "rq_obsLifeClaim000002", address, 1)
	out = settleEntry(t, core, namespace, "rq_obsLifeExtend00001", claim,
		SettleEntry{UnitID: unitID, Action: SettleExtend, ExtensionMs: 60_000})
	if out.Units[0].Outcome != SettleOutcomeExtended {
		t.Fatalf("extend outcome = %q, want %q", out.Units[0].Outcome, SettleOutcomeExtended)
	}
	assertObservation(t, core, stationAggregate(1, 1, charge, 1, 6))

	// COMPLETE removes the unit and its retained bytes and settles the claim.
	settleComplete(t, core, namespace, "rq_obsLifeComplete0001", claim, unitID)
	assertObservation(t, core, stationAggregate(1, 0, 0, 0, 7))
	assertAggregateMatchesNamespaceSum(t, core, namespace)

	// A short-lived address and unit expire on the next maintenance pass.
	shortAddress := reserveAddress(t, core, namespace, "rq_obsLifeReserve0002", 60_000)
	expiryPayload := []byte("expiry-probe")
	expiryCharge := unitLogicalCharge(len(expiryPayload))
	submitUnit(t, core, namespace, "rq_obsLifeSubmit00002", shortAddress, expiryPayload)
	assertObservation(t, core, stationAggregate(2, 1, expiryCharge, 0, 9))

	clock.Advance(120 * time.Second)
	// ObservationSnapshot runs no maintenance: the expired address and unit
	// still count until a maintenance-triggering call runs the pass.
	assertObservation(t, core, stationAggregate(2, 1, expiryCharge, 0, 9))
	core.NamespacePressure(namespace)
	assertObservation(t, core, stationAggregate(1, 0, 0, 0, 9))

	// Reopen rebuilds the aggregate from the canonical durable counters.
	if err := core.Restart(); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	assertObservation(t, core, stationAggregate(1, 0, 0, 0, 9))
	assertAggregateMatchesNamespaceSum(t, core, namespace)

	// Live transitions keep maintaining the rebuilt aggregate.
	reserveAddress(t, core, namespace, "rq_obsLifeReserve0003", 3_600_000)
	assertObservation(t, core, stationAggregate(2, 0, 0, 0, 10))
	assertAggregateMatchesNamespaceSum(t, core, namespace)
}

func TestObservationSnapshotConcurrentWithMutations(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC)))
	ns1 := "ns_AAAAAAAAAAAAAAAAAAAAAA"
	ns2 := "ns_BBBBBBBBBBBBBBBBBBBBBB"
	address1 := reserveAddress(t, core, ns1, "rq_obsConcReserveA01", 3_600_000)
	reserveAddress(t, core, ns2, "rq_obsConcReserveB01", 3_600_000)
	core.SetSyncDelay(2 * time.Millisecond)

	limits := profileLimits()
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				obs := core.ObservationSnapshot()
				if obs.Lifecycle != StationLifecycleRunning {
					t.Errorf("concurrent snapshot lifecycle = %q, want RUNNING", obs.Lifecycle)
					return
				}
				// Station-wide ceilings never exceed the per-namespace ceilings.
				if obs.Aggregate.Units > limits.MaxUnitsPerNamespace*2 ||
					obs.Aggregate.Addresses > limits.MaxAddressesPerNamespace*2 ||
					obs.Aggregate.ActiveClaims > limits.MaxActiveClaimsPerNamespace*2 ||
					obs.Aggregate.RetainedBytes > limits.MaxRetainedBytesPerNamespace*2 {
					t.Errorf("concurrent snapshot exceeds station ceilings: %+v", obs.Aggregate)
					return
				}
				time.Sleep(20 * time.Microsecond)
			}
		}()
	}

	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; i < 4; i++ {
			unit := submitUnit(t, core, ns1, fmt.Sprintf("rq_obsConcSubmit%04d", i), address1, []byte("concurrent"))
			claim := claimUnits(t, core, ns1, fmt.Sprintf("rq_obsConcClaim%05d", i), address1, 1)
			settleComplete(t, core, ns1, fmt.Sprintf("rq_obsConcSettle%05d", i), claim, unit)
		}
	}()
	writer.Wait()
	close(stop)
	readers.Wait()

	// Quiescent exact aggregate: two live addresses, every unit claimed and
	// settled away, one idempotency record per mutation.
	assertObservation(t, core, stationAggregate(2, 0, 0, 0, 2+4*3))
	assertAggregateMatchesNamespaceSum(t, core, ns1, ns2)
}

func TestObservationSnapshotSideEffectFree(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 9, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_DDDDDDDDDDDDDDDDDDDDDD"

	address := reserveAddress(t, core, namespace, "rq_obsSefReserve0001", 60_000)
	payload := []byte("expiry-probe")
	charge := unitLogicalCharge(len(payload))
	submitUnit(t, core, namespace, "rq_obsSefSubmit000001", address, payload)

	clock.Advance(120 * time.Second)
	// Deadlines are due, but the snapshot itself runs no maintenance pass and
	// performs no store I/O, so the expired state still counts.
	assertObservation(t, core, stationAggregate(1, 1, charge, 0, 2))

	// A maintenance-triggering call expires the address and unit together.
	core.NamespacePressure(namespace)
	assertObservation(t, core, stationAggregate(0, 0, 0, 0, 2))

	// The snapshot path never touches the store: it still serves the last
	// in-memory observation after the store is closed.
	if err := core.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	after := core.ObservationSnapshot()
	if after.Lifecycle != StationLifecycleStopped {
		t.Fatalf("lifecycle after close = %q, want STOPPED", after.Lifecycle)
	}
	if after.Aggregate != stationAggregate(0, 0, 0, 0, 2) {
		t.Fatalf("aggregate after close = %+v", after.Aggregate)
	}
}

func TestObservationAggregateRebuildRejectsOverflow(t *testing.T) {
	t.Parallel()
	tracker := newCapacityTracker()
	tracker.setCounters("first", namespaceCounters{Units: ^uint64(0)})
	tracker.setCounters("second", namespaceCounters{Units: 1})
	if tracker.rebuildStationAggregate() {
		t.Fatal("overflowing station aggregate rebuild succeeded")
	}
	_, got := tracker.stationObservationSnapshot()
	if got != (StationAggregate{}) {
		t.Fatalf("failed rebuild published partial aggregate: %+v", got)
	}
}

func TestObservationStationWidePressure(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 11, 0, 0, 0, time.UTC)))
	namespace := "ns_MmMmMmMmMmMmMmMmMmMmMm"

	if got := core.ObservationSnapshot(); got.Pressure != PressureNormal {
		t.Fatalf("pressure before = %v, want NORMAL", got.Pressure)
	}
	// Per-namespace soft pressure raises the station-wide observation in O(1).
	core.ForceSoftPressure(namespace)
	if got := core.ObservationSnapshot(); got.Pressure != PressureSoft {
		t.Fatalf("pressure after soft = %v, want SOFT", got.Pressure)
	}
	// The global station signal is the ceiling for the station-wide pressure.
	core.capacity.mu.Lock()
	core.capacity.globalPressure = PressureHard
	core.capacity.mu.Unlock()
	if got := core.ObservationSnapshot(); got.Pressure != PressureHard {
		t.Fatalf("pressure after global hard = %v, want HARD", got.Pressure)
	}
}

func TestObservationDrainAdmissionRules(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 10, 0, 0, 0, time.UTC)))
	namespace := "ns_EEEEEEEEEEEEEEEEEEEEEE"
	address := reserveAddress(t, core, namespace, "rq_obsDrainReserve001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_obsDrainSubmit0001", address, []byte("drain"))
	claim := claimUnits(t, core, namespace, "rq_obsDrainClaim00001", address, 1)
	if obs := core.ObservationSnapshot(); obs.Lifecycle != StationLifecycleRunning {
		t.Fatalf("lifecycle before drain = %q, want RUNNING", obs.Lifecycle)
	}

	core.BeginDrain()
	core.BeginDrain() // idempotent
	if obs := core.ObservationSnapshot(); obs.Lifecycle != StationLifecycleDraining {
		t.Fatalf("lifecycle after drain = %q, want DRAINING", obs.Lifecycle)
	}
	if ready, reason := core.Ready(); ready || reason != ReasonDraining {
		t.Fatalf("Ready after drain = (%v, %q), want (false, DRAINING)", ready, reason)
	}

	// Reserve, Submit, and Claim reject after drain.
	_, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace, RequestID: "rq_obsDrainReserve002",
	})
	assertReason(t, err, ReasonDraining)
	_, err = core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace, RequestID: "rq_obsDrainSubmit0002",
		AddressID: address, Payload: []byte("after-drain"),
	})
	assertReason(t, err, ReasonDraining)
	_, err = core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace, RequestID: "rq_obsDrainClaim00002",
		AddressID: address, MaxUnits: 1, MaxBytes: 1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	assertReason(t, err, ReasonDraining)

	// Bounded Settle retains its native rules during drain.
	settleComplete(t, core, namespace, "rq_obsDrainSettle0001", claim, unitID)

	// GetCapacity and NamespacePressure remain available.
	if _, err := core.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: namespace}); err != nil {
		t.Fatalf("GetCapacity during drain: %v", err)
	}
	core.NamespacePressure(namespace)

	// A second BeginDrain stays a no-op, and settle rules are still enforced,
	// not bypassed: the fully settled claim is stale.
	core.BeginDrain()
	if obs := core.ObservationSnapshot(); obs.Lifecycle != StationLifecycleDraining {
		t.Fatalf("lifecycle after second drain = %q, want DRAINING", obs.Lifecycle)
	}
	_, err = core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace, RequestID: "rq_obsDrainReserve003",
	})
	assertReason(t, err, ReasonDraining)
	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace, RequestID: "rq_obsDrainSettle0002",
		ClaimID: claim.ClaimID, ClaimEpoch: claim.ClaimEpoch,
		Entries: []SettleEntry{{UnitID: unitID, Action: SettleComplete}},
	})
	assertReason(t, err, ReasonClaimStale)
}

// claimUnits claims exactly one eligible unit and fails the test otherwise.
func claimUnits(t *testing.T, core *Core, namespace, requestID, address string, maxUnits uint32) ClaimOutput {
	t.Helper()
	out, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           requestID,
		AddressID:           address,
		MaxUnits:            maxUnits,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(out.Units) == 0 || out.ClaimID == "" {
		t.Fatalf("Claim returned no units: %#v", out)
	}
	return out
}

// settleEntry settles one entry on one claim and fails the test otherwise.
func settleEntry(t *testing.T, core *Core, namespace, requestID string, claim ClaimOutput, entry SettleEntry) SettleOutput {
	t.Helper()
	out, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries:     []SettleEntry{entry},
	})
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if len(out.Units) != 1 || out.Units[0].UnitID != entry.UnitID {
		t.Fatalf("Settle units = %#v, want one entry for %q", out.Units, entry.UnitID)
	}
	return out
}

// settleComplete settles one entry as COMPLETE and fails the test otherwise.
func settleComplete(t *testing.T, core *Core, namespace, requestID string, claim ClaimOutput, unitID string) {
	t.Helper()
	out := settleEntry(t, core, namespace, requestID, claim, SettleEntry{UnitID: unitID, Action: SettleComplete})
	if out.Units[0].Outcome != SettleOutcomeCompleted {
		t.Fatalf("complete outcome = %q, want %q", out.Units[0].Outcome, SettleOutcomeCompleted)
	}
}

func assertReason(t *testing.T, err error, want Reason) {
	t.Helper()
	failure, ok := IsFailure(err)
	if !ok || failure.Reason != want {
		t.Fatalf("error = %v, want reason %q", err, want)
	}
}
