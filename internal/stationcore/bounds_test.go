package stationcore

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestAdmissionRejectsOversizedPayloadBeforeMutation(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 15, 0, 0, 0, time.UTC)))
	namespace := "ns_PPPPPPPPPPPPPPPPPPPPPP"
	address := reserveAddress(t, core, namespace, "rq_admitRes0000000001", 3_600_000)

	_, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_admitOversize00001",
		AddressID:   address,
		Payload:     make([]byte, 1<<20+1),
		TTLMs:       60_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonInvalidRequest {
		t.Fatalf("oversized payload: err=%v", err)
	}

	_, err = core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_admitEmpty0000001",
		AddressID:   address,
		Payload:     nil,
		TTLMs:       60_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonInvalidRequest {
		t.Fatalf("empty payload: err=%v", err)
	}
}

func TestAdmissionLogicalNamespaceUnitCeiling(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 16, 0, 0, 0, time.UTC)))
	limits := core.ProfileLimits()
	if limits.MaxUnitsPerNamespace == 0 {
		t.Fatal("executor must expose profile unit ceiling")
	}
	namespace := "ns_QQQQQQQQQQQQQQQQQQQQQQ"
	address := reserveAddress(t, core, namespace, "rq_unitCeilRes000001", 86_400_000)

	var lastErr error
	for index := uint64(0); index <= limits.MaxUnitsPerNamespace; index++ {
		_, lastErr = core.Submit(context.Background(), SubmitInput{
			NamespaceID: namespace,
			RequestID:   "rq_unitCeilSubmit" + strconv.FormatUint(index, 10),
			AddressID:   address,
			Payload:     []byte("u"),
			TTLMs:       86_400_000,
		})
		if lastErr != nil {
			break
		}
	}
	if failure, ok := IsFailure(lastErr); !ok || failure.Reason != ReasonCapacityExceeded {
		t.Fatalf("expected capacity exceeded at namespace unit ceiling: err=%v", lastErr)
	}
}

func TestDRRHotNamespaceDoesNotStarveColdNamespace(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 17, 0, 0, 0, time.UTC)))
	limits := core.ProfileLimits()
	if limits.MutationLanes != mutationLanes || limits.DRRQuantumBytes == 0 {
		t.Fatalf("profile must expose the fixed lane count and DRR quantum: %#v", limits)
	}

	hot := "ns_RRRRRRRRRRRRRRRRRRRRRR"
	cold := "ns_SSSSSSSSSSSSSSSSSSSSSS"
	hotAddress := reserveAddress(t, core, hot, "rq_drrHotRes000000001", 86_400_000)
	coldAddress := reserveAddress(t, core, cold, "rq_drrColdRes00000001", 86_400_000)

	for index := 0; index < 256; index++ {
		submitUnit(t, core, hot, "rq_drrHotSubmit"+strconv.Itoa(index), hotAddress, []byte("h"))
	}
	submitUnit(t, core, cold, "rq_drrColdSubmit001", coldAddress, []byte("c"))

	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("cold namespace never received service within 1s while capacity available")
		default:
		}
		claim, err := core.Claim(context.Background(), ClaimInput{
			NamespaceID:         cold,
			RequestID:           "rq_drrColdClaim000001",
			AddressID:           coldAddress,
			MaxUnits:            1,
			MaxBytes:            1 << 20,
			VisibilityTimeoutMs: 30_000,
		})
		if err != nil {
			t.Fatalf("Claim cold: %v", err)
		}
		if len(claim.Units) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestPressureSoftStopsSubmitWhileSettleRemainsAvailable(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 18, 0, 0, 0, time.UTC)))
	namespace := "ns_TTTTTTTTTTTTTTTTTTTTTT"
	address := reserveAddress(t, core, namespace, "rq_softPressRes00001", 86_400_000)
	unitID := submitUnit(t, core, namespace, "rq_softPressSub0001", address, []byte("p"))

	forceSoftPressure(t, core, namespace)

	_, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_softPressBlock001",
		AddressID:   address,
		Payload:     []byte("blocked"),
		TTLMs:       60_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonCapacityExceeded {
		t.Fatalf("soft pressure must stop Submit: err=%v", err)
	}

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_softPressClaim001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim under soft pressure: %v", err)
	}
	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_softPressSettle01",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: unitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatalf("Settle must remain under soft pressure: %v", err)
	}
}

func TestCleanupExpiredAddressImmediateSemanticUnavailable(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 19, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_UUUUUUUUUUUUUUUUUUUUUU"

	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_cleanupRes0000001",
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	submitUnit(t, core, namespace, "rq_cleanupSubmit001", reserved.AddressID, []byte("orphan"))

	clock.Advance(61 * time.Second)
	_, err = core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_cleanupClaim00001",
		AddressID:           reserved.AddressID,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonAddressExpired {
		t.Fatalf("expired address must reject Claim immediately: err=%v", err)
	}
}

func TestAdmissionClaimCountAndByteCeilings(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 21, 30, 0, 0, time.UTC)))
	namespace := "ns_aaaaaaaaaaaaaaaaaaaaaa"

	countAddress := reserveAddress(t, core, namespace, "rq_claimCeilRes001", 3_600_000)
	for index := 0; index < 140; index++ {
		submitUnit(t, core, namespace, "rq_claimCeilSub"+strconv.Itoa(index), countAddress, []byte("u"))
	}
	capped, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_claimCeilClm001",
		AddressID:           countAddress,
		MaxUnits:            1_000_000,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if len(capped.Units) != 128 {
		t.Fatalf("claim units = %d, want the 128 ceiling", len(capped.Units))
	}

	byteAddress := reserveAddress(t, core, namespace, "rq_claimBytesRes001", 3_600_000)
	meg := make([]byte, 1<<20)
	for index := 0; index < 5; index++ {
		submitUnit(t, core, namespace, "rq_claimBytesSub"+strconv.Itoa(index), byteAddress, meg)
	}
	byDefault, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_claimBytesClm001",
		AddressID:           byteAddress,
		MaxUnits:            16,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim default byte ceiling: %v", err)
	}
	if len(byDefault.Units) != 1 || len(byDefault.Units[0].Payload) != 1<<20 {
		t.Fatalf("default 1 MiB claim byte ceiling not enforced: %#v", byDefault)
	}

	byteCapped, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_claimBytesClm002",
		AddressID:           byteAddress,
		MaxUnits:            16,
		MaxBytes:            4 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim 4 MiB byte ceiling: %v", err)
	}
	if len(byteCapped.Units) != 4 {
		t.Fatalf("claim units = %d, want 4 at the 4 MiB ceiling", len(byteCapped.Units))
	}
	var total uint64
	for _, unit := range byteCapped.Units {
		total += uint64(len(unit.Payload))
	}
	if total > 4<<20 {
		t.Fatalf("claimed payload %d exceeds 4 MiB", total)
	}
}

func TestAdmissionSettleEntryCeiling(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 22, 30, 0, 0, time.UTC)))
	namespace := "ns_bbbbbbbbbbbbbbbbbbbbbb"
	address := reserveAddress(t, core, namespace, "rq_settleCeilRes001", 3_600_000)
	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_settleCeilClm001",
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
		RequestID:   "rq_settleCeilEmpty1",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonInvalidRequest {
		t.Fatalf("empty Settle must reject: err=%v", err)
	}

	entries := make([]SettleEntry, 129)
	for index := range entries {
		entries[index] = SettleEntry{
			UnitID: "tu_" + fmt.Sprintf("%022d", index),
			Action: SettleComplete,
		}
	}
	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_settleCeilOver1",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries:     entries,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonInvalidRequest {
		t.Fatalf("129 Settle entries must reject before mutation: err=%v", err)
	}
}

func TestAdmissionRateLimitedQueueCeilingWithSettleReserve(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 23, 30, 0, 0, time.UTC)))
	stormNamespace := "ns_cccccccccccccccccccccc"
	stormAddress := reserveAddress(t, core, stormNamespace, "rq_queueRes000000001", 3_600_000)

	// The Settle must share the storm's mutation lane so it competes for the
	// same queue ceiling (REQ-012 control reserve).
	settleNamespace := sameLaneNamespace(t, stormNamespace)
	settleAddress := reserveAddress(t, core, settleNamespace, "rq_queueSetNSRes001", 3_600_000)
	unitID := submitUnit(t, core, settleNamespace, "rq_queueSetNSSub001", settleAddress, []byte("reserved"))
	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         settleNamespace,
		RequestID:           "rq_queueSetNSClm001",
		AddressID:           settleAddress,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}

	limits := core.ProfileLimits()
	if limits.MaxMutationQueueEntriesPerNamespace == 0 {
		t.Fatal("profile must expose the mutation queue ceiling")
	}
	core.SetSyncDelay(50 * time.Millisecond)

	var mu sync.Mutex
	var rateLimited bool
	ctxs := make([]context.CancelFunc, 0, 200)
	var wg sync.WaitGroup
	for index := 0; index < 200; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		ctxs = append(ctxs, cancel)
		wg.Add(1)
		go func(sequence int) {
			defer wg.Done()
			_, err := core.Submit(ctx, SubmitInput{
				NamespaceID: stormNamespace,
				RequestID:   "rq_queueStorm" + strconv.Itoa(sequence),
				AddressID:   stormAddress,
				Payload:     []byte("storm"),
				TTLMs:       60_000,
			})
			if failure, ok := IsFailure(err); ok && failure.Reason == ReasonRateLimited {
				mu.Lock()
				rateLimited = true
				mu.Unlock()
			}
		}(index)
	}

	// Wait until the queue ceiling is observably enforced.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		seen := rateLimited
		mu.Unlock()
		if seen {
			break
		}
		if time.Now().After(deadline) {
			for _, cancel := range ctxs {
				cancel()
			}
			wg.Wait()
			t.Fatal("mutation queue ceiling was never enforced")
		}
		time.Sleep(5 * time.Millisecond)
	}

	// Control traffic (Settle) is accepted into the full lane queue and
	// completes while admission is rate limited (REQ-012).
	settled, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: settleNamespace,
		RequestID:   "rq_queueSettle00001",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: unitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatalf("Settle must remain available at the queue ceiling: %v", err)
	}
	if settled.Units[0].Outcome != SettleOutcomeCompleted {
		t.Fatalf("settle outcome = %q", settled.Units[0].Outcome)
	}

	for _, cancel := range ctxs {
		cancel()
	}
	wg.Wait()
	core.SetSyncDelay(0)

	followUp, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: stormNamespace,
		RequestID:   "rq_queueFollowUp001",
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("lane must remain healthy after the storm: %v", err)
	}
	if followUp.AddressID == "" {
		t.Fatal("empty follow-up address")
	}
}

// sameLaneNamespace returns a candidate namespace on the same mutation lane
// as the given one. The bounded search covers the fixed 64-lane profile with
// overwhelming margin and does not retain candidates after the test.
func sameLaneNamespace(t *testing.T, namespace string) string {
	t.Helper()
	for attempt := 0; attempt < mutationLanes*16; attempt++ {
		candidate, err := generateID("ns_")
		if err != nil {
			t.Fatal(err)
		}
		if namespaceLane(candidate) == namespaceLane(namespace) {
			return candidate
		}
	}
	t.Fatal("no same-lane namespace candidate found")
	return ""
}

func TestCancellationQueuedMutationNeverCommits(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 23, 45, 0, 0, time.UTC)))
	namespace := "ns_dddddddddddddddddddddd"
	address := reserveAddress(t, core, namespace, "rq_cancelQRes000001", 3_600_000)

	core.SetSyncDelay(100 * time.Millisecond)
	blockerDone := make(chan struct{})
	go func() {
		defer close(blockerDone)
		_, _ = core.Submit(context.Background(), SubmitInput{
			NamespaceID: namespace,
			RequestID:   "rq_cancelQBlock001",
			AddressID:   address,
			Payload:     []byte("blocker"),
			TTLMs:       60_000,
		})
	}()
	time.Sleep(20 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	queuedDone := make(chan error, 1)
	go func() {
		_, err := core.Submit(ctx, SubmitInput{
			NamespaceID: namespace,
			RequestID:   "rq_cancelQQueued001",
			AddressID:   address,
			Payload:     []byte("queued"),
			TTLMs:       60_000,
		})
		queuedDone <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	queuedErr := <-queuedDone
	if queuedErr != context.Canceled {
		t.Fatalf("cancelled queued mutation must return context.Canceled: %v", queuedErr)
	}
	<-blockerDone
	core.SetSyncDelay(0)

	followUp, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_cancelQFollow01",
		AddressID:   address,
		Payload:     []byte("after"),
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("lane must remain healthy after cancellation: %v", err)
	}
	if followUp.UnitID == "" {
		t.Fatal("empty follow-up unit")
	}
}

func TestCleanupExpiredUnitsCountersReleaseWithinBudget(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 23, 55, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_eeeeeeeeeeeeeeeeeeeeee"
	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_cleanupUnitsRes01",
		TTLMs:       60_000,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	for index := 0; index < 8; index++ {
		submitUnit(t, core, namespace, "rq_cleanupUnitsSub"+strconv.Itoa(index), reserved.AddressID, []byte("debt"))
	}
	if _, usage := core.NamespacePressure(namespace); usage.Units != 8 {
		t.Fatalf("units charged = %d, want 8", usage.Units)
	}

	clock.Advance(61 * time.Second)

	// Bounded incremental maintenance must delete expired units and release
	// logical counters within the profile cleanup budget (REQ-010, REQ-011).
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, usage := core.NamespacePressure(namespace)
		if usage.Units == 0 && usage.RetainedBytes == 0 && usage.Addresses == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expired address/unit counters not released within budget: %#v", usage)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The address expired with its children; freed capacity admits a new
	// address and unit after the bounded cleanup pass.
	fresh, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "rq_cleanupUnitsRes02",
		TTLMs:       3_600_000,
	})
	if err != nil {
		t.Fatalf("freed capacity must admit new addresses: %v", err)
	}
	if _, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "rq_cleanupUnitsSub09",
		AddressID:   fresh.AddressID,
		Payload:     []byte("reuse"),
		TTLMs:       60_000,
	}); err != nil {
		t.Fatalf("freed capacity must admit new units: %v", err)
	}
}

func TestCancellationReleasesUncommittedLongPollBudget(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 7, 31, 20, 0, 0, 0, time.UTC)))
	namespace := "ns_VVVVVVVVVVVVVVVVVVVVVV"
	address := reserveAddress(t, core, namespace, "rq_cancelRes00000001", 86_400_000)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	var claimErr error
	go func() {
		defer wg.Done()
		_, claimErr = core.Claim(ctx, ClaimInput{
			NamespaceID:         namespace,
			RequestID:           "rq_cancelClaim000001",
			AddressID:           address,
			MaxUnits:            1,
			MaxBytes:            1 << 20,
			VisibilityTimeoutMs: 30_000,
			WaitTimeoutMs:       20_000,
		})
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
	if claimErr != context.Canceled {
		t.Fatalf("cancelled Claim must release wait budget: err=%v", claimErr)
	}

	pressure, usage := core.NamespacePressure(namespace)
	if pressure == PressureHard && usage.ActiveClaims > 0 {
		t.Fatalf("cancelled wait must not retain active claim counters: %#v", usage)
	}
}

func forceSoftPressure(t *testing.T, core *Core, namespace string) {
	t.Helper()
	core.ForceSoftPressure(namespace)
}
