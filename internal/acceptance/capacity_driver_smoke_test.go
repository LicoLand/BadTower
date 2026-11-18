//go:build station_acceptance

package acceptance

import (
	"context"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

func TestCapacityDriverSmoke(t *testing.T) {
	driver, err := newCapacityDriver(t, CapacityProfileSingleNodeGeneralV1)
	if err != nil {
		t.Fatalf("newCapacityDriver: %v", err)
	}
	result := driver.RunLoad(2*time.Second, LoadWorkloadSpec{
		Namespaces:        2,
		Clients:           16,
		HotNamespaceShare: 1,
		HotAddressShare:   1,
		PayloadMixKiB:     []int{1},
		ClaimBatchSize:    32,
		SettleBatchSize:   32,
	})
	if result.EndToEndUnitsPerSecond <= 0 {
		t.Fatalf("invalid throughput sample: %#v", result)
	}
	if result.MaxClaimBatchSize <= 1 || result.MaxClaimBatchSize > maxCapacityBatchSize {
		t.Fatalf("producer/consumer driver did not form a bounded batch: %#v", result)
	}
	t.Logf("short load: terminalUnitsPerSecond=%.1f meanClaimBatch=%.2f maxClaimBatch=%d p95=%s p99=%s",
		result.EndToEndUnitsPerSecond, result.MeanClaimBatchSize, result.MaxClaimBatchSize,
		result.MutationLatencyP95, result.MutationLatencyP99)
	ready := driver.Snapshot().ReadinessServing
	if !ready {
		t.Fatal("driver not serving after smoke load")
	}
}

func TestCapacityDriverRepresentativeShort(t *testing.T) {
	driver, err := newCapacityDriver(t, CapacityProfileSingleNodeGeneralV1)
	if err != nil {
		t.Fatalf("newCapacityDriver: %v", err)
	}
	result := driver.RunLoad(15*time.Second, LoadWorkloadSpec{
		Namespaces:               100,
		Clients:                  128,
		HotNamespaceShare:        0.20,
		HotAddressShare:          0.20,
		PayloadMixKiB:            []int{1, 16, 256},
		PayloadMixWeights:        []float64{0.80, 0.15, 0.05},
		ClaimBatchSize:           32,
		SettleBatchSize:          32,
		CompleteShare:            0.90,
		ReleaseShare:             0.05,
		ExtendShare:              0.03,
		NaturalClaimTimeoutShare: 0.02,
	})
	if result.Elapsed < 15*time.Second || result.Elapsed > 16*time.Second {
		t.Fatalf("representative short load elapsed = %s", result.Elapsed)
	}
	if result.EndToEndUnitsPerSecond <= 0 || result.MaxClaimBatchSize == 0 || result.MaxClaimBatchSize > maxCapacityBatchSize {
		t.Fatalf("invalid representative short result: %#v", result)
	}
	t.Logf("representative short load: actualElapsed=%s terminalUnitsPerSecond=%.1f fairness=%.4f meanClaimBatch=%.2f maxClaimBatch=%d p95=%s p99=%s",
		result.Elapsed, result.EndToEndUnitsPerSecond, result.JainFairness, result.MeanClaimBatchSize,
		result.MaxClaimBatchSize, result.MutationLatencyP95, result.MutationLatencyP99)
}

func TestCapacityClockTracksElapsedAndFaultOffset(t *testing.T) {
	start := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)
	clock := newCapacityClock(start)
	firstWall, firstMono := clock.NowUnixMs(), clock.MonotonicNow()
	time.Sleep(15 * time.Millisecond)
	secondWall, secondMono := clock.NowUnixMs(), clock.MonotonicNow()
	if secondWall <= firstWall || secondMono <= firstMono {
		t.Fatalf("capacity clock froze: wall %d -> %d, mono %s -> %s", firstWall, secondWall, firstMono, secondMono)
	}
	clock.Advance(2 * time.Second)
	if delta := clock.MonotonicNow() - secondMono; delta < 2*time.Second || delta > 2100*time.Millisecond {
		t.Fatalf("fault offset delta = %s", delta)
	}
}

func TestCapacityMetricsBoundedHistogramAndActualElapsed(t *testing.T) {
	metrics := newLoadMetrics()
	metrics.beginSegment()
	for index := 0; index < 95; index++ {
		metrics.recordLatency(time.Millisecond)
	}
	for index := 0; index < 5; index++ {
		metrics.recordLatency(100 * time.Millisecond)
	}
	metrics.recordCompletion(namespaceID(0), 100)
	time.Sleep(20 * time.Millisecond)
	result := metrics.result(1)
	if result.MutationLatencyP95 < time.Millisecond || result.MutationLatencyP95 > 1100*time.Microsecond {
		t.Fatalf("p95 = %s", result.MutationLatencyP95)
	}
	if result.MutationLatencyP99 < 100*time.Millisecond || result.MutationLatencyP99 > 104*time.Millisecond {
		t.Fatalf("p99 = %s", result.MutationLatencyP99)
	}
	if result.EndToEndUnitsPerSecond >= 10_000 || result.EndToEndUnitsPerSecond <= 1_000 {
		t.Fatalf("throughput did not use actual elapsed time: %f", result.EndToEndUnitsPerSecond)
	}
}

func TestCapacityFairnessNormalizesOfferedHotShare(t *testing.T) {
	accepted := map[string]uint64{namespaceID(0): 100, namespaceID(1): 10}
	completed := map[string]uint64{namespaceID(0): 90, namespaceID(1): 9}
	if fairness := jainServiceFairness(completed, accepted, 2); fairness != 1 {
		t.Fatalf("normalized fairness = %f, want 1", fairness)
	}
}

func TestSettlementSharesAndTerminalAccountingAreExact(t *testing.T) {
	spec := LoadWorkloadSpec{CompleteShare: .90, ReleaseShare: .05, ExtendShare: .03, NaturalClaimTimeoutShare: .02}
	cases := []struct {
		roll float64
		want settlementClass
	}{{0, settlementComplete}, {.899, settlementComplete}, {.90, settlementRelease}, {.949, settlementRelease}, {.95, settlementExtend}, {.979, settlementExtend}, {.98, settlementNaturalTimeout}, {.999, settlementNaturalTimeout}}
	for _, test := range cases {
		if got := settlementClassForRoll(test.roll, spec); got != test.want {
			t.Fatalf("roll %.3f class = %d, want %d", test.roll, got, test.want)
		}
	}
	units := []stationcore.SettledUnit{
		{Outcome: stationcore.SettleOutcomeCompleted},
		{Outcome: stationcore.SettleOutcomeReleased},
		{Outcome: stationcore.SettleOutcomeExtended},
		{Outcome: stationcore.SettleOutcomeAttemptLimit},
	}
	if terminal := terminalSettledUnits(units); terminal != 2 {
		t.Fatalf("terminal units = %d, want 2", terminal)
	}
	if !validWorkloadSpec(LoadWorkloadSpec{Namespaces: 1, Clients: 1, CompleteShare: 1, ClaimBatchSize: 32, SettleBatchSize: 32, PayloadMixKiB: []int{1}}) {
		t.Fatal("explicit zero non-terminal shares must remain valid")
	}
	if validWorkloadSpec(LoadWorkloadSpec{Namespaces: 1, Clients: 1, CompleteShare: .9, ExtendShare: .03, ClaimBatchSize: 32, SettleBatchSize: 32, PayloadMixKiB: []int{1}}) {
		t.Fatal("shares that do not total one must fail")
	}
}

func TestClaimSchedulerCoalescesBoundedAddressWork(t *testing.T) {
	scheduler := newClaimScheduler(1, 64, 2)
	work := claimWork{namespace: "ns", address: "address"}
	if !scheduler.notify(work) || !scheduler.notify(work) {
		t.Fatal("scheduler rejected bounded work")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, ok := scheduler.take(ctx)
	if !ok || first != work {
		t.Fatalf("first work = %#v, ok=%v", first, ok)
	}
	scheduler.done(first, 1, false)
	second, ok := scheduler.take(ctx)
	if !ok || second != work {
		t.Fatalf("dirty work was not coalesced and requeued: %#v, ok=%v", second, ok)
	}
	scheduler.done(second, 1, false)
	emptyCtx, emptyCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer emptyCancel()
	if _, ok := scheduler.take(emptyCtx); ok {
		t.Fatal("settled address remained queued")
	}
	limited := newClaimScheduler(1, len(work.key()), 1)
	if !limited.notify(work) || limited.notify(claimWork{namespace: "other", address: "address"}) {
		t.Fatal("scheduler address/key-byte bound was not enforced")
	}

	payloads := preparePayloads([]int{1, 16})
	rng := clientRNG(0)
	selected := pickPreparedPayload(LoadWorkloadSpec{PayloadMixKiB: []int{1}, PayloadMixWeights: nil}, payloads[:1], rng)
	if len(selected) != 1024 || cap(selected) != 1024 {
		t.Fatalf("prepared payload size = %d/%d", len(selected), cap(selected))
	}
}
