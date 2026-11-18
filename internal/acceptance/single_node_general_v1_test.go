//go:build station_acceptance

package acceptance

// Executor must provide:
//   - black-box workload driver StartNativeStationWorkload(profile single-node-general-v1)
//   - measurement oracles for throughput, latency, fairness, memory, storage (Capacity Model)
//   - 30-minute load phase after 5-minute warm-up; 2-hour soak at 60% target with fault injection

import (
	"testing"
	"time"
)

const (
	loadPhaseDuration = 30 * time.Minute

	// soakHourDuration runs each half of the 2h soak window one minute past
	// the hour. The driver's hourly RSS sample is taken once a segment has run
	// >= 1h, so the padding lands both samples mid-segment instead of racing
	// segment completion; the memory-growth oracle compares those two samples.
	// Soak hour two MUST run as one continuous segment: beginSegment resets
	// the hourly sampling window at every RunLoad start, so tiled 7m37.5s
	// fault segments never reach the 1h threshold and the second sample would
	// never be taken.
	soakPhaseDuration  = 2 * time.Hour
	soakHourDuration   = soakPhaseDuration/2 + time.Minute
	warmupDuration     = 5 * time.Minute
	sustainedUnitRate  = 3000
	jainFairnessTarget = 0.95
)

func TestStationInstanceLoad(t *testing.T) {
	if testing.Short() {
		t.Skip("capacity acceptance requires -tags=station_acceptance without -short")
	}
	driver, err := newCapacityDriver(t, CapacityProfileSingleNodeGeneralV1)
	if err != nil {
		t.Fatalf("newCapacityDriver: %v", err)
	}

	driver.WarmUp(warmupDuration)
	result := driver.RunLoad(loadPhaseDuration, LoadWorkloadSpec{
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

	assertCorrectnessOracles(t, result)
	assertThroughputAtLeast(t, result, sustainedUnitRate)
	assertMutationLatency(t, result, 50*time.Millisecond, 200*time.Millisecond)
	assertFairnessJainAtLeast(t, result, jainFairnessTarget)
	assertMemoryRSSAtMost(t, result, 3<<30)
	assertStorageAmplificationAtMost(t, result, 2.5)
}

func TestStationInstanceSoakAndFaults(t *testing.T) {
	if testing.Short() {
		t.Skip("soak acceptance requires -tags=station_acceptance without -short")
	}
	driver, err := newCapacityDriver(t, CapacityProfileSingleNodeGeneralV1)
	if err != nil {
		t.Fatalf("newCapacityDriver: %v", err)
	}

	driver.WarmUp(warmupDuration)

	// Soak hour one runs clean at 60% target; soak hour two runs as ONE
	// continuous RunLoad so the between-hours memory oracle is real. The
	// driver's hourly RSS sample fires at the 60-minute mark of each hour (the
	// 61m segments leave a 1m margin so the tick cannot race completion), and
	// the growth oracle compares those two samples. Eight separate 7m37.5s
	// fault segments would each reset the driver's hourly sampling window in
	// beginSegment and never reach the 1h threshold, leaving one sample and a
	// vacuous 0% growth reading.
	soakSpec := LoadWorkloadSpec{
		Namespaces:         100,
		Clients:            128,
		ThroughputFraction: 0.60,
	}
	driver.RunLoad(soakHourDuration, LoadWorkloadSpec{
		Namespaces:         100,
		Clients:            128,
		ThroughputFraction: 0.60,
		HotNamespaceShare:  0.20,
		HotAddressShare:    0.20,
		ClaimBatchSize:     32,
		SettleBatchSize:    32,
	})

	faults := []FaultSpec{
		{Name: "client-cancellation", Inject: driver.InjectClientCancellation},
		{Name: "token-reload", Inject: driver.InjectTokenReload},
		{Name: "process-restart", Inject: driver.InjectProcessRestart},
		{Name: "fsync-slowdown", Inject: driver.InjectFsyncSlowdown},
		{Name: "soft-disk-pressure", Inject: driver.InjectSoftDiskPressure},
		{Name: "clock-backward-60s", Inject: driver.InjectClockBackward},
		{Name: "clock-forward-10m", Inject: driver.InjectClockForward},
		{Name: "expiry-backlog-100k", Inject: driver.InjectExpiryBacklog},
	}

	hourTwoStart := time.Now()
	results := make(chan CapacityResult, 1)
	go func() {
		results <- driver.RunLoad(soakHourDuration, soakSpec)
	}()

	// Each fault is injected at its frozen segment-slot offset (61m / 8 =
	// 7m37.5s apart) into the still running soak; the load continues through
	// the fault's slot, then the correctness and recovery oracles run against
	// the pre-injection snapshot, without ending the RunLoad between faults.
	// The total wall time is warmup + 2h soak = 5m + 122m ≈ 127m plus
	// injection overhead, inside the frozen 150m command timeout.
	slotDuration := soakHourDuration / time.Duration(len(faults))
	for index, fault := range faults {
		fault := fault
		t.Run(fault.Name, func(t *testing.T) {
			sleepUntil(t, hourTwoStart.Add(time.Duration(index)*slotDuration))
			snapshotBefore := driver.Snapshot()
			if err := fault.Inject(); err != nil {
				t.Fatalf("inject %s: %v", fault.Name, err)
			}
			if index == len(faults)-1 {
				// The last slot ends exactly when the hour-two RunLoad
				// completes; its result carries the authoritative oracles.
				// The full-hour runtime guard is what makes the second hourly
				// RSS sample exist at all, so an early RunLoad return (e.g.
				// all load clients cancelled) fails here instead of silently
				// restoring the vacuous growth reading.
				result := <-results
				if elapsed := time.Since(hourTwoStart); elapsed < soakHourDuration-time.Minute {
					t.Fatalf("hour-two soak returned after %s, want the full %s so both hourly RSS samples exist", elapsed, soakHourDuration)
				}
				assertCorrectnessOracles(t, result)
				assertRecoveryWithinCeilings(t, fault.Name, snapshotBefore, driver.Snapshot())
				assertMemoryRSSAtMost(t, result, 3<<30)
				return
			}
			sleepUntil(t, hourTwoStart.Add(time.Duration(index+1)*slotDuration))
			// Mid-run correctness: replay verification runs at the frozen 2%
			// share of submissions, so the cumulative mismatch count read at
			// the slot boundary is a live oracle, never vacuous.
			assertCorrectnessOracles(t, CapacityResult{ReplayMismatches: driver.ReplayMismatchCount()})
			assertRecoveryWithinCeilings(t, fault.Name, snapshotBefore, driver.Snapshot())
		})
	}

	assertMemoryGrowthBetweenHours(t, driver, 0.05)
}

func sleepUntil(t *testing.T, target time.Time) {
	t.Helper()
	if delay := time.Until(target); delay > 0 {
		time.Sleep(delay)
	}
}

const CapacityProfileSingleNodeGeneralV1 = "single-node-general-v1"

type LoadWorkloadSpec struct {
	Namespaces               int
	Clients                  int
	ThroughputFraction       float64
	HotNamespaceShare        float64
	HotAddressShare          float64
	PayloadMixKiB            []int
	PayloadMixWeights        []float64
	ClaimBatchSize           int
	SettleBatchSize          int
	CompleteShare            float64
	ReleaseShare             float64
	ExtendShare              float64
	NaturalClaimTimeoutShare float64
}

type CapacityResult struct {
	Elapsed                time.Duration
	EndToEndUnitsPerSecond float64
	MeanClaimBatchSize     float64
	MaxClaimBatchSize      uint64
	MutationLatencyP95     time.Duration
	MutationLatencyP99     time.Duration
	JainFairness           float64
	RSSBytes               uint64
	StorageAmplification   float64
	ReplayMismatches       uint64
}

type CapacityDriver interface {
	WarmUp(time.Duration)
	RunLoad(time.Duration, LoadWorkloadSpec) CapacityResult
	Snapshot() CapacitySnapshot
	MemoryGrowthBetweenHours() float64
	ReplayMismatchCount() uint64
	InjectClientCancellation() error
	InjectTokenReload() error
	InjectProcessRestart() error
	InjectFsyncSlowdown() error
	InjectSoftDiskPressure() error
	InjectClockBackward() error
	InjectClockForward() error
	InjectExpiryBacklog() error
}

type CapacitySnapshot struct {
	RSSBytes           uint64
	MaintenanceBacklog uint64
	CompactionDebt     float64
	ReadinessServing   bool
}

type FaultSpec struct {
	Name   string
	Inject func() error
}

func newCapacityDriver(t *testing.T, profile string) (CapacityDriver, error) {
	return newStationCapacityDriver(t, profile)
}

func assertCorrectnessOracles(t *testing.T, result CapacityResult) {
	t.Helper()
	if result.EndToEndUnitsPerSecond < 0 {
		t.Fatal("invalid throughput sample")
	}
	if result.ReplayMismatches != 0 {
		t.Fatalf("idempotent replay mismatch under load: %d", result.ReplayMismatches)
	}
}

func assertThroughputAtLeast(t *testing.T, result CapacityResult, min float64) {
	t.Helper()
	if result.EndToEndUnitsPerSecond < min {
		t.Fatalf("throughput = %.2f units/s, want >= %.2f", result.EndToEndUnitsPerSecond, min)
	}
}

func assertMutationLatency(t *testing.T, result CapacityResult, p95, p99 time.Duration) {
	t.Helper()
	if result.MutationLatencyP95 > p95 || result.MutationLatencyP99 > p99 {
		t.Fatalf("mutation latency p95=%s p99=%s", result.MutationLatencyP95, result.MutationLatencyP99)
	}
}

func assertFairnessJainAtLeast(t *testing.T, result CapacityResult, min float64) {
	t.Helper()
	if result.JainFairness < min {
		t.Fatalf("Jain fairness = %.4f, want >= %.4f", result.JainFairness, min)
	}
}

func assertMemoryRSSAtMost(t *testing.T, result CapacityResult, max uint64) {
	t.Helper()
	if result.RSSBytes > max {
		t.Fatalf("RSS = %d, want <= %d", result.RSSBytes, max)
	}
}

func assertStorageAmplificationAtMost(t *testing.T, result CapacityResult, max float64) {
	t.Helper()
	if result.StorageAmplification > max {
		t.Fatalf("amplification = %.2f, want <= %.2f", result.StorageAmplification, max)
	}
}

func assertRecoveryWithinCeilings(t *testing.T, fault string, before, after CapacitySnapshot) {
	t.Helper()
	if !after.ReadinessServing {
		t.Fatalf("readiness false after fault %s", fault)
	}
	if after.MaintenanceBacklog > before.MaintenanceBacklog+100_000 {
		t.Fatalf("maintenance backlog unbounded after fault %s", fault)
	}
}

func assertMemoryGrowthBetweenHours(t *testing.T, driver CapacityDriver, maxGrowth float64) {
	t.Helper()
	growth := driver.MemoryGrowthBetweenHours()
	if growth > maxGrowth {
		t.Fatalf("post-GC live heap growth between soak hours = %.4f, want <= %.4f", growth, maxGrowth)
	}
}
