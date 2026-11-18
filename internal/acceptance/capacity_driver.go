//go:build station_acceptance

package acceptance

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io/fs"
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/nativeapi"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

const (
	defaultTargetUnitsPerSecond = 3000
	maxCapacityBatchSize        = 32
	maxCapacityClients          = 128
	maxCapacityNamespaces       = 100
	maxCapacityConsumers        = 64
	maxClaimSchedulerKeyBytes   = 64 << 10
	claimSchedulerDebounce      = 5 * time.Millisecond
)

type stationCapacityDriver struct {
	t        *testing.T
	core     *stationcore.Core
	clock    *capacityClock
	dataRoot string
	auth     *nativeapi.Authenticator
	config   nativeapi.Config

	mu              sync.Mutex
	namespaces      []string
	hotNamespace    string
	addresses       map[string][]string
	hotAddress      map[string]string
	metrics         *loadMetrics
	requestSeq      uint64
	replayMismatch  atomic.Uint64
	clientCancels   []context.CancelFunc
	longPollCancels []context.CancelFunc
}

// replayVerifyShare retries a fraction of acknowledged Submits with the same
// request ID to prove byte-equivalent idempotent replay under load.
const replayVerifyShare = 0.02

func newStationCapacityDriver(t *testing.T, profile string) (CapacityDriver, error) {
	t.Helper()
	if profile != CapacityProfileSingleNodeGeneralV1 {
		return nil, fmt.Errorf("unexpected profile %q", profile)
	}

	dataRoot := t.TempDir()
	clock := newCapacityClock(time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC))
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  dataRoot,
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, clock)
	if err != nil {
		return nil, fmt.Errorf("open station core: %w", err)
	}
	t.Cleanup(func() { _ = core.Close() })

	driver := &stationCapacityDriver{
		t:          t,
		core:       core,
		clock:      clock,
		dataRoot:   dataRoot,
		metrics:    newLoadMetrics(),
		addresses:  make(map[string][]string),
		hotAddress: make(map[string]string),
	}
	driver.namespaces = make([]string, 100)
	for index := range driver.namespaces {
		driver.namespaces[index] = namespaceID(index)
	}
	driver.hotNamespace = driver.namespaces[0]
	if err := driver.initAuth(); err != nil {
		_ = core.Close()
		return nil, err
	}
	return driver, nil
}

func namespaceID(index int) string {
	var raw [16]byte
	binary.BigEndian.PutUint64(raw[0:8], uint64(index))
	binary.BigEndian.PutUint64(raw[8:16], uint64(index))
	suffix := base64.RawURLEncoding.EncodeToString(raw[:])
	return "ns_" + suffix
}

func (driver *stationCapacityDriver) initAuth() error {
	namespaces := make([]nativeapi.NamespaceConfig, 0, len(driver.namespaces))
	now := time.Now().UTC()
	for index := 0; index < len(driver.namespaces); index++ {
		tokenMaterial := make([]byte, 32)
		if _, err := rand.Read(tokenMaterial); err != nil {
			return fmt.Errorf("generate token material: %w", err)
		}
		_, digest, err := nativeapi.GenerateToken(tokenMaterial)
		if err != nil {
			return fmt.Errorf("generate token digest: %w", err)
		}
		namespaces = append(namespaces, nativeapi.NamespaceConfig{
			ID: driver.namespaces[index],
			Credentials: []nativeapi.CredentialConfig{{
				SHA256:    digest,
				NotBefore: now.Add(-time.Hour),
				NotAfter:  now.Add(30 * 24 * time.Hour),
			}},
		})
	}
	driver.config = nativeapi.Config{
		Schema:     nativeapi.ConfigSchema,
		Profile:    nativeapi.ProfileSingleNode,
		DataRoot:   driver.dataRoot,
		Transport:  nativeapi.TransportConfig{Mode: "unix", SocketPath: filepath.Join(driver.dataRoot, "station.sock")},
		Health:     nativeapi.ListenerConfig{Listen: "127.0.0.1:0"},
		Metrics:    nativeapi.ListenerConfig{Listen: "127.0.0.1:0"},
		Namespaces: namespaces,
	}
	validated, err := nativeapi.ValidateConfig(driver.config)
	if err != nil {
		return fmt.Errorf("validate auth config: %w", err)
	}
	driver.config = validated
	driver.auth = nativeapi.NewAuthenticator(driver.config)
	return nil
}

func (driver *stationCapacityDriver) WarmUp(duration time.Duration) {
	spec := LoadWorkloadSpec{
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
	}
	driver.RunLoad(duration, spec)
}

func (driver *stationCapacityDriver) RunLoad(duration time.Duration, spec LoadWorkloadSpec) CapacityResult {
	driver.applyDefaults(&spec)
	if !validWorkloadSpec(spec) {
		driver.t.Errorf("invalid capacity workload shares or batch bounds")
		return CapacityResult{}
	}
	if err := driver.ensureAddresses(spec.Namespaces); err != nil {
		// RunLoad may run on a spawned goroutine (hour-two soak), where
		// t.Fatalf only exits that goroutine and the test then hangs on the
		// results channel until the go-test timeout. Record the failure with
		// the goroutine-safe t.Errorf and return an empty result instead.
		driver.t.Errorf("ensure addresses: %v", err)
		return CapacityResult{}
	}

	driver.metrics.beginSegment()
	deadline := time.Now().Add(duration)
	targetRate := float64(defaultTargetUnitsPerSecond)
	if spec.ThroughputFraction > 0 {
		targetRate = float64(defaultTargetUnitsPerSecond) * spec.ThroughputFraction
	}
	producerRate := targetRate / spec.CompleteShare
	limiter := newTokenBucket(producerRate, producerRate)
	payloads := preparePayloads(spec.PayloadMixKiB)
	scheduler := newClaimScheduler(spec.Namespaces*2, maxClaimSchedulerKeyBytes, min(spec.ClaimBatchSize, spec.SettleBatchSize))

	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	consumerCount := min(maxCapacityConsumers, max(1, spec.Clients/2))
	producerCount := max(1, spec.Clients-consumerCount)
	var wg sync.WaitGroup
	for consumer := 0; consumer < consumerCount; consumer++ {
		wg.Add(1)
		go func(consumerIndex int) {
			defer wg.Done()
			driver.runConsumer(ctx, spec, scheduler, producerCount+consumerIndex)
		}(consumer)
	}
	for producer := 0; producer < producerCount; producer++ {
		wg.Add(1)
		go func(producerIndex int) {
			defer wg.Done()
			driver.runProducer(ctx, spec, limiter, scheduler, payloads, producerIndex)
		}(producer)
	}

	sampleTicker := time.NewTicker(500 * time.Millisecond)
	defer sampleTicker.Stop()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	for {
		select {
		case <-done:
			driver.metrics.endSegment()
			driver.refreshStorageMetrics()
			result := driver.metrics.result(spec.Namespaces)
			result.ReplayMismatches = driver.replayMismatch.Load()
			return result
		case <-sampleTicker.C:
			driver.metrics.sampleRSS()
			driver.refreshStorageMetrics()
		}
	}
}

func (driver *stationCapacityDriver) applyDefaults(spec *LoadWorkloadSpec) {
	if spec.Namespaces <= 0 {
		spec.Namespaces = 100
	}
	if spec.Clients <= 0 {
		spec.Clients = 128
	}
	if spec.HotNamespaceShare <= 0 {
		spec.HotNamespaceShare = 0.20
	}
	if spec.HotAddressShare <= 0 {
		spec.HotAddressShare = 0.20
	}
	if len(spec.PayloadMixKiB) == 0 {
		spec.PayloadMixKiB = []int{1, 16, 256}
		spec.PayloadMixWeights = []float64{0.80, 0.15, 0.05}
	}
	if spec.ClaimBatchSize <= 0 {
		spec.ClaimBatchSize = 32
	}
	if spec.SettleBatchSize <= 0 {
		spec.SettleBatchSize = 32
	}
	if spec.CompleteShare+spec.ReleaseShare+spec.ExtendShare+spec.NaturalClaimTimeoutShare == 0 {
		spec.CompleteShare = 0.90
		spec.ReleaseShare = 0.05
		spec.ExtendShare = 0.03
		spec.NaturalClaimTimeoutShare = 0.02
	}
}

func validWorkloadSpec(spec LoadWorkloadSpec) bool {
	total := 0.0
	for _, share := range []float64{spec.CompleteShare, spec.ReleaseShare, spec.ExtendShare, spec.NaturalClaimTimeoutShare} {
		if share < 0 || share > 1 {
			return false
		}
		total += share
	}
	if math.Abs(total-1) > 1e-9 || spec.CompleteShare <= 0 || spec.Clients <= 0 || spec.Clients > maxCapacityClients ||
		spec.Namespaces <= 0 || spec.Namespaces > maxCapacityNamespaces || spec.ClaimBatchSize <= 0 || spec.ClaimBatchSize > maxCapacityBatchSize ||
		spec.SettleBatchSize <= 0 || spec.SettleBatchSize > maxCapacityBatchSize || len(spec.PayloadMixKiB) == 0 {
		return false
	}
	for _, size := range spec.PayloadMixKiB {
		if size <= 0 {
			return false
		}
	}
	if len(spec.PayloadMixWeights) != 0 {
		if len(spec.PayloadMixWeights) != len(spec.PayloadMixKiB) {
			return false
		}
		weightTotal := 0.0
		for _, weight := range spec.PayloadMixWeights {
			if weight < 0 {
				return false
			}
			weightTotal += weight
		}
		if math.Abs(weightTotal-1) > 1e-9 {
			return false
		}
	}
	return true
}

func (driver *stationCapacityDriver) ensureAddresses(namespaceCount int) error {
	if namespaceCount > len(driver.namespaces) {
		return fmt.Errorf("requested %d namespaces but only %d provisioned", namespaceCount, len(driver.namespaces))
	}
	for index := 0; index < namespaceCount; index++ {
		namespace := driver.namespaces[index]
		for len(driver.addresses[namespace]) < 2 {
			start := time.Now()
			out, err := driver.core.Reserve(context.Background(), stationcore.ReserveInput{
				NamespaceID: namespace,
				RequestID:   driver.requestID("res"),
				TTLMs:       86_400_000,
			})
			driver.metrics.recordLatency(time.Since(start))
			if err != nil {
				return fmt.Errorf("reserve address in %s: %w", namespace, err)
			}
			driver.addresses[namespace] = append(driver.addresses[namespace], out.AddressID)
		}
		driver.hotAddress[namespace] = driver.addresses[namespace][0]
	}
	return nil
}

func (driver *stationCapacityDriver) runProducer(ctx context.Context, spec LoadWorkloadSpec, limiter *tokenBucket, scheduler *claimScheduler, payloads [][]byte, producerIndex int) {
	rng := clientRNG(producerIndex)
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if wait := limiter.take(1); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}

		namespace := driver.pickNamespace(spec, rng)
		address := driver.pickAddress(namespace, spec, rng)
		payload := pickPreparedPayload(spec, payloads, rng)

		start := time.Now()
		submitInput := stationcore.SubmitInput{
			NamespaceID: namespace,
			RequestID:   driver.requestID("sub"),
			AddressID:   address,
			Payload:     payload,
			TTLMs:       86_400_000,
		}
		out, err := driver.core.Submit(ctx, submitInput)
		driver.metrics.recordLatency(time.Since(start))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if rng.Float64() < replayVerifyShare {
			replay, replayErr := driver.core.Submit(ctx, submitInput)
			if replayErr != nil {
				if ctx.Err() == nil {
					driver.replayMismatch.Add(1)
				}
			} else if replay.UnitID != out.UnitID ||
				replay.AcceptedAtUnixMs != out.AcceptedAtUnixMs ||
				replay.ExpiresAtUnixMs != out.ExpiresAtUnixMs {
				driver.replayMismatch.Add(1)
			}
		}
		driver.metrics.recordAccepted(namespace)
		if !scheduler.notify(claimWork{namespace: namespace, address: address}) {
			driver.t.Errorf("capacity claim scheduler exhausted its bounded address work set")
			return
		}
	}
}

func (driver *stationCapacityDriver) runConsumer(ctx context.Context, spec LoadWorkloadSpec, scheduler *claimScheduler, consumerIndex int) {
	rng := clientRNG(consumerIndex)
	maxUnits := min(spec.ClaimBatchSize, spec.SettleBatchSize)
	for {
		work, ok := scheduler.take(ctx)
		if !ok {
			return
		}
		start := time.Now()
		claim, err := driver.core.Claim(ctx, stationcore.ClaimInput{
			NamespaceID:         work.namespace,
			RequestID:           driver.requestID("clm"),
			AddressID:           work.address,
			MaxUnits:            uint32(maxUnits),
			MaxBytes:            4 << 20,
			VisibilityTimeoutMs: 30_000,
		})
		driver.metrics.recordLatency(time.Since(start))
		if err != nil || len(claim.Units) == 0 {
			scheduler.discard(work)
			if ctx.Err() != nil {
				return
			}
			continue
		}
		driver.metrics.recordClaimBatch(len(claim.Units))
		entries := driver.buildSettleEntries(claim.Units, spec, rng)
		if len(entries) == 0 {
			scheduler.done(work, len(claim.Units), len(claim.Units) == maxUnits)
			continue
		}
		start = time.Now()
		settled, err := driver.core.Settle(ctx, stationcore.SettleInput{
			NamespaceID: work.namespace,
			RequestID:   driver.requestID("set"),
			ClaimID:     claim.ClaimID,
			ClaimEpoch:  claim.ClaimEpoch,
			Entries:     entries,
		})
		driver.metrics.recordLatency(time.Since(start))
		scheduler.done(work, len(claim.Units), len(claim.Units) == maxUnits)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		if terminal := terminalSettledUnits(settled.Units); terminal > 0 {
			driver.metrics.recordCompletion(work.namespace, terminal)
		}
	}
}

func (driver *stationCapacityDriver) buildSettleEntries(units []stationcore.ClaimedUnit, spec LoadWorkloadSpec, rng *clientRand) []stationcore.SettleEntry {
	entries := make([]stationcore.SettleEntry, 0, len(units))
	for _, unit := range units {
		switch settlementClassForRoll(rng.Float64(), spec) {
		case settlementComplete:
			entries = append(entries, stationcore.SettleEntry{UnitID: unit.UnitID, Action: stationcore.SettleComplete})
		case settlementRelease:
			entries = append(entries, stationcore.SettleEntry{UnitID: unit.UnitID, Action: stationcore.SettleRelease})
		case settlementExtend:
			entries = append(entries, stationcore.SettleEntry{
				UnitID:      unit.UnitID,
				Action:      stationcore.SettleExtend,
				ExtensionMs: 5_000,
			})
		case settlementNaturalTimeout:
			continue
		}
	}
	return entries
}

type settlementClass uint8

const (
	settlementComplete settlementClass = iota
	settlementRelease
	settlementExtend
	settlementNaturalTimeout
)

func settlementClassForRoll(roll float64, spec LoadWorkloadSpec) settlementClass {
	const shareScale = int64(1_000_000_000)
	sample := int64(math.Floor(roll * float64(shareScale)))
	complete := int64(math.Round(spec.CompleteShare * float64(shareScale)))
	release := complete + int64(math.Round(spec.ReleaseShare*float64(shareScale)))
	extend := release + int64(math.Round(spec.ExtendShare*float64(shareScale)))
	if sample < complete {
		return settlementComplete
	}
	if sample < release {
		return settlementRelease
	}
	if sample < extend {
		return settlementExtend
	}
	return settlementNaturalTimeout
}

func terminalSettledUnits(units []stationcore.SettledUnit) uint64 {
	var terminal uint64
	for _, unit := range units {
		if unit.Outcome == stationcore.SettleOutcomeCompleted || unit.Outcome == stationcore.SettleOutcomeAttemptLimit {
			terminal++
		}
	}
	return terminal
}

func (driver *stationCapacityDriver) pickNamespace(spec LoadWorkloadSpec, rng *clientRand) string {
	if spec.Namespaces <= 1 || rng.Float64() < spec.HotNamespaceShare {
		return driver.hotNamespace
	}
	index := 1 + rng.Intn(max(1, spec.Namespaces-1))
	return driver.namespaces[index]
}

func (driver *stationCapacityDriver) pickAddress(namespace string, spec LoadWorkloadSpec, rng *clientRand) string {
	addresses := driver.addresses[namespace]
	if len(addresses) == 0 {
		return driver.hotAddress[namespace]
	}
	if rng.Float64() < spec.HotAddressShare {
		return driver.hotAddress[namespace]
	}
	return addresses[rng.Intn(len(addresses))]
}

func preparePayloads(sizesKiB []int) [][]byte {
	payloads := make([][]byte, len(sizesKiB))
	for payloadIndex, sizeKiB := range sizesKiB {
		payloads[payloadIndex] = make([]byte, sizeKiB*1024)
		for index := range payloads[payloadIndex] {
			payloads[payloadIndex][index] = byte('a' + (index % 26))
		}
	}
	return payloads
}

func pickPreparedPayload(spec LoadWorkloadSpec, payloads [][]byte, rng *clientRand) []byte {
	selected := 0
	if len(spec.PayloadMixKiB) > 1 && len(spec.PayloadMixWeights) == len(spec.PayloadMixKiB) {
		roll := rng.Float64()
		cumulative := 0.0
		for index, weight := range spec.PayloadMixWeights {
			cumulative += weight
			if roll <= cumulative {
				selected = index
				break
			}
		}
	}
	return payloads[selected]
}

func (driver *stationCapacityDriver) requestID(prefix string) string {
	sequence := atomic.AddUint64(&driver.requestSeq, 1)
	return fmt.Sprintf("rq_%s%012d", prefix, sequence)
}

func (driver *stationCapacityDriver) MemoryGrowthBetweenHours() float64 {
	return driver.metrics.memoryGrowthBetweenHours()
}

// ReplayMismatchCount reads the cumulative idempotent-replay mismatch counter
// mid-run, so the correctness oracle can be asserted for each fault slot of
// the continuous soak hour two without ending the RunLoad.
func (driver *stationCapacityDriver) ReplayMismatchCount() uint64 {
	return driver.replayMismatch.Load()
}

func (driver *stationCapacityDriver) Snapshot() CapacitySnapshot {
	ready, _ := driver.core.Ready()
	backlog := driver.maintenanceBacklog()
	logical, disk := driver.storageUsage()
	debt := 0.0
	if logical > 0 {
		debt = float64(disk) / float64(logical)
	}
	return CapacitySnapshot{
		RSSBytes:           atomic.LoadUint64(&driver.metrics.peakRSS),
		MaintenanceBacklog: backlog,
		CompactionDebt:     debt,
		ReadinessServing:   ready,
	}
}

func (driver *stationCapacityDriver) maintenanceBacklog() uint64 {
	var backlog uint64
	for _, namespace := range driver.namespaces {
		_, usage := driver.core.NamespacePressure(namespace)
		backlog += usage.IdempotencyRecords
	}
	return backlog
}

func (driver *stationCapacityDriver) refreshStorageMetrics() {
	logical, disk := driver.storageUsage()
	driver.metrics.setStorage(logical, disk)
}

func (driver *stationCapacityDriver) storageUsage() (logical, disk uint64) {
	for _, namespace := range driver.namespaces {
		_, usage := driver.core.NamespacePressure(namespace)
		logical += usage.RetainedBytes
	}
	disk = directorySize(driver.dataRoot)
	return logical, disk
}

func directorySize(root string) uint64 {
	var total uint64
	_ = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil {
			return nil
		}
		total += uint64(info.Size())
		return nil
	})
	return total
}

func (driver *stationCapacityDriver) InjectClientCancellation() error {
	driver.cancelTrackedClients()
	namespace := driver.hotNamespace
	address := driver.hotAddress[namespace]
	if address == "" {
		return fmt.Errorf("hot address unavailable for cancellation fault")
	}

	var wg sync.WaitGroup
	for index := 0; index < 16; index++ {
		ctx, cancel := context.WithCancel(context.Background())
		driver.trackLongPollCancel(cancel)
		wg.Add(1)
		go func(clientIndex int) {
			defer wg.Done()
			_, _ = driver.core.Claim(ctx, stationcore.ClaimInput{
				NamespaceID:         namespace,
				RequestID:           driver.requestID("cncl"),
				AddressID:           address,
				MaxUnits:            1,
				MaxBytes:            1 << 20,
				VisibilityTimeoutMs: 30_000,
				WaitTimeoutMs:       20_000,
			})
		}(index)
	}
	time.Sleep(50 * time.Millisecond)
	driver.cancelLongPollClients()
	wg.Wait()
	return nil
}

func (driver *stationCapacityDriver) InjectTokenReload() error {
	if driver.auth == nil {
		return fmt.Errorf("authenticator not initialized")
	}
	now := time.Now().UTC()
	reloaded := driver.config
	for index := range reloaded.Namespaces {
		tokenMaterial := make([]byte, 32)
		if _, err := rand.Read(tokenMaterial); err != nil {
			return fmt.Errorf("generate reload token: %w", err)
		}
		_, digest, err := nativeapi.GenerateToken(tokenMaterial)
		if err != nil {
			return fmt.Errorf("digest reload token: %w", err)
		}
		reloaded.Namespaces[index].Credentials = []nativeapi.CredentialConfig{{
			SHA256:    digest,
			NotBefore: now.Add(-time.Minute),
			NotAfter:  now.Add(30 * 24 * time.Hour),
		}}
	}
	validated, err := nativeapi.ValidateConfig(reloaded)
	if err != nil {
		return fmt.Errorf("validate reloaded config: %w", err)
	}
	if err := driver.auth.Reload(validated); err != nil {
		return fmt.Errorf("reload credentials: %w", err)
	}
	driver.config = validated
	return nil
}

func (driver *stationCapacityDriver) InjectProcessRestart() error {
	if err := driver.core.Restart(); err != nil {
		return fmt.Errorf("restart core: %w", err)
	}
	ready, reason := driver.core.Ready()
	if !ready {
		return fmt.Errorf("core not ready after restart: %s", reason)
	}
	return nil
}

func (driver *stationCapacityDriver) InjectFsyncSlowdown() error {
	driver.core.SetSyncDelay(50 * time.Millisecond)
	go func() {
		time.Sleep(5 * time.Minute)
		driver.core.SetSyncDelay(0)
	}()
	return nil
}

func (driver *stationCapacityDriver) InjectSoftDiskPressure() error {
	driver.core.ForceSoftPressure(driver.hotNamespace)
	return nil
}

func (driver *stationCapacityDriver) InjectClockBackward() error {
	driver.clock.Advance(-60 * time.Second)
	ready, reason := driver.core.Ready()
	if !ready && reason == stationcore.ReasonClockUnsafe {
		return nil
	}
	return nil
}

func (driver *stationCapacityDriver) InjectClockForward() error {
	driver.clock.Advance(10 * time.Minute)
	return nil
}

func (driver *stationCapacityDriver) InjectExpiryBacklog() error {
	namespace := driver.namespaces[len(driver.namespaces)-1]
	start := time.Now()
	reserved, err := driver.core.Reserve(context.Background(), stationcore.ReserveInput{
		NamespaceID: namespace,
		RequestID:   driver.requestID("exp"),
		TTLMs:       120_000,
	})
	if err != nil {
		return fmt.Errorf("reserve backlog address: %w", err)
	}
	driver.metrics.recordLatency(time.Since(start))

	for index := 0; index < 100_000; index++ {
		start = time.Now()
		_, err := driver.core.Submit(context.Background(), stationcore.SubmitInput{
			NamespaceID: namespace,
			RequestID:   driver.requestID("eb"),
			AddressID:   reserved.AddressID,
			Payload:     []byte("x"),
			TTLMs:       60_000,
		})
		driver.metrics.recordLatency(time.Since(start))
		if err != nil {
			return fmt.Errorf("submit backlog unit %d: %w", index, err)
		}
	}
	driver.clock.Advance(2 * time.Minute)
	return nil
}

func (driver *stationCapacityDriver) trackLongPollCancel(cancel context.CancelFunc) {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	driver.longPollCancels = append(driver.longPollCancels, cancel)
}

func (driver *stationCapacityDriver) cancelTrackedClients() {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	for _, cancel := range driver.clientCancels {
		cancel()
	}
	driver.clientCancels = nil
}

func (driver *stationCapacityDriver) cancelLongPollClients() {
	driver.mu.Lock()
	defer driver.mu.Unlock()
	for _, cancel := range driver.longPollCancels {
		cancel()
	}
	driver.longPollCancels = nil
}

type claimWork struct {
	namespace string
	address   string
}

func (work claimWork) key() string { return work.namespace + "\x00" + work.address }

type claimScheduleState struct {
	work      claimWork
	pending   int
	queued    bool
	inFlight  bool
	firstSeen time.Time
}

// claimScheduler bounds pending work by the finite address set and coalesces
// repeated submissions to one address. Consumers requeue a full batch, so a
// backlog is drained in batches without one Claim request per submitted unit.
type claimScheduler struct {
	mu           sync.Mutex
	queue        chan claimWork
	pending      map[string]*claimScheduleState
	maxAddresses int
	maxKeyBytes  int
	keyBytes     int
	batchTarget  int
}

func newClaimScheduler(addressCount, maxKeyBytes, batchTarget int) *claimScheduler {
	if addressCount <= 0 {
		addressCount = 1
	}
	if maxKeyBytes <= 0 {
		maxKeyBytes = 1
	}
	if batchTarget <= 0 || batchTarget > maxCapacityBatchSize {
		batchTarget = maxCapacityBatchSize
	}
	return &claimScheduler{
		queue: make(chan claimWork, addressCount), pending: make(map[string]*claimScheduleState, addressCount),
		maxAddresses: addressCount, maxKeyBytes: maxKeyBytes, batchTarget: batchTarget,
	}
}

func (scheduler *claimScheduler) notify(work claimWork) bool {
	key := work.key()
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if current := scheduler.pending[key]; current != nil {
		current.pending = min(scheduler.batchTarget, current.pending+1)
		if current.pending >= scheduler.batchTarget && !current.queued && !current.inFlight {
			scheduler.enqueueLocked(current)
		}
		return true
	}
	if len(scheduler.pending) >= scheduler.maxAddresses || scheduler.keyBytes+len(key) > scheduler.maxKeyBytes {
		return false
	}
	current := &claimScheduleState{work: work, pending: 1, firstSeen: time.Now()}
	scheduler.pending[key] = current
	scheduler.keyBytes += len(key)
	return true
}

func (scheduler *claimScheduler) take(ctx context.Context) (claimWork, bool) {
	ticker := time.NewTicker(claimSchedulerDebounce)
	defer ticker.Stop()
	for {
		select {
		case work := <-scheduler.queue:
			scheduler.markInFlight(work)
			return work, true
		case now := <-ticker.C:
			scheduler.flushReady(now)
		case <-ctx.Done():
			return claimWork{}, false
		}
	}
}

func (scheduler *claimScheduler) done(work claimWork, claimed int, more bool) {
	key := work.key()
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	current := scheduler.pending[key]
	if current == nil {
		return
	}
	current.inFlight = false
	current.pending = max(0, current.pending-claimed)
	if more {
		scheduler.enqueueLocked(current)
		return
	}
	if current.pending > 0 {
		current.firstSeen = time.Now()
		return
	}
	scheduler.deleteLocked(key)
}

func (scheduler *claimScheduler) discard(work claimWork) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	scheduler.deleteLocked(work.key())
}

func (scheduler *claimScheduler) markInFlight(work claimWork) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if current := scheduler.pending[work.key()]; current != nil {
		current.queued = false
		current.inFlight = true
	}
}

func (scheduler *claimScheduler) flushReady(now time.Time) {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	for _, current := range scheduler.pending {
		if current.pending > 0 && !current.queued && !current.inFlight && now.Sub(current.firstSeen) >= claimSchedulerDebounce {
			scheduler.enqueueLocked(current)
		}
	}
}

func (scheduler *claimScheduler) enqueueLocked(current *claimScheduleState) {
	if current.queued || current.inFlight {
		return
	}
	current.queued = true
	scheduler.queue <- current.work
}

func (scheduler *claimScheduler) deleteLocked(key string) {
	if current := scheduler.pending[key]; current != nil {
		scheduler.keyBytes -= len(key)
		delete(scheduler.pending, key)
	}
}

type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate, burst float64) *tokenBucket {
	if rate <= 0 {
		rate = defaultTargetUnitsPerSecond
	}
	if burst <= 0 {
		burst = rate
	}
	return &tokenBucket{
		rate:   rate,
		burst:  burst,
		tokens: burst,
		last:   time.Now(),
	}
}

func (bucket *tokenBucket) take(count float64) time.Duration {
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(bucket.last).Seconds()
	bucket.last = now
	bucket.tokens = math.Min(bucket.burst, bucket.tokens+elapsed*bucket.rate)
	if bucket.tokens >= count {
		bucket.tokens -= count
		return 0
	}
	deficit := count - bucket.tokens
	bucket.tokens = 0
	return time.Duration(deficit / bucket.rate * float64(time.Second))
}

type clientRand struct {
	state uint64
}

func clientRNG(seed int) *clientRand {
	return &clientRand{state: uint64(seed)*2654435761 + 1}
}

func (rng *clientRand) Float64() float64 {
	rng.state = rng.state*6364136223846793005 + 1
	return float64(rng.state>>11) / float64(1<<53)
}

func (rng *clientRand) Intn(n int) int {
	if n <= 1 {
		return 0
	}
	return int(rng.Float64() * float64(n))
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
