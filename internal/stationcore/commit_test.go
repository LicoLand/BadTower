package stationcore

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

func TestClaimReplayReferenceAvoidsPayloadRewriteAndSurvivesSettlement(t *testing.T) {
	clock := newTestClock(time.Date(2026, 8, 10, 0, 10, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_claimReplayCompact0123"
	address := reserveAddress(t, core, namespace, "claim-replay-reserve", 3_600_000)
	payload := bytes.Repeat([]byte("opaque-payload-marker-"), 40_000)
	submitted, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace, RequestID: "claim-replay-submit",
		AddressID: address, Payload: payload, TTLMs: 3_600_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if writes, writtenBytes := core.store.payloadWrites.Load(), core.store.payloadBytes.Load(); writes != 1 || writtenBytes != uint64(len(payload)) {
		t.Fatalf("immutable payload writes=%d bytes=%d", writes, writtenBytes)
	}
	shard, err := core.store.shardForNamespace(namespace)
	if err != nil {
		t.Fatal(err)
	}
	committer := shard.commits
	bytesBeforeClaim := committer.logicalBytes.Load()
	claimed, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace, RequestID: "claim-replay-claim",
		AddressID: address, MaxUnits: 1, MaxBytes: maxClaimBytes,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimWriteBytes := committer.logicalBytes.Load() - bytesBeforeClaim
	if claimWriteBytes >= uint64(len(payload))/4 {
		t.Fatalf("Claim durable bytes = %d; immutable payload was rewritten", claimWriteBytes)
	}
	if writes, writtenBytes := core.store.payloadWrites.Load(), core.store.payloadBytes.Load(); writes != 1 || writtenBytes != uint64(len(payload)) {
		t.Fatalf("Claim rewrote immutable payload: writes=%d bytes=%d", writes, writtenBytes)
	}
	rawIdempotency, err := core.store.get(idempotencyKey(namespace, opNameClaim, "claim-replay-claim"))
	if err != nil {
		t.Fatal(err)
	}
	_, response, _, err := decodeIdempotencyRecord(rawIdempotency)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(response, []byte("opaque-payload-marker-")) {
		t.Fatal("active Claim idempotency contains payload instead of compact reference")
	}
	if replay, recognized, err := decodeClaimReplay(response); err != nil || !recognized || replay.snapshot {
		t.Fatalf("active Claim replay record = recognized:%t snapshot:%t err:%v", recognized, replay.snapshot, err)
	}

	if _, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace, RequestID: "claim-replay-settle",
		ClaimID: claimed.ClaimID, ClaimEpoch: claimed.ClaimEpoch,
		Entries: []SettleEntry{{UnitID: submitted.UnitID, Action: SettleComplete}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := core.store.get(unitMetadataKey(namespace, address, submitted.UnitID)); err != pebble.ErrNotFound {
		t.Fatalf("settled metadata still present: %v", err)
	}
	if _, err := core.store.get(unitPayloadKey(namespace, address, submitted.UnitID)); err != pebble.ErrNotFound {
		t.Fatalf("settled payload still present: %v", err)
	}

	if err := core.Restart(); err != nil {
		t.Fatal(err)
	}
	replayed, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace, RequestID: "claim-replay-claim",
		AddressID: address, MaxUnits: 1, MaxBytes: maxClaimBytes,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim replay after settlement and restart: %v", err)
	}
	if len(replayed.Units) != 1 || replayed.Units[0].UnitID != submitted.UnitID || !bytes.Equal(replayed.Units[0].Payload, payload) {
		t.Fatal("Claim replay did not preserve the original opaque payload")
	}
}

func TestTimedOutClaimReplaySurvivesLaterSettlement(t *testing.T) {
	clock := newTestClock(time.Date(2026, 8, 10, 0, 20, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_timeoutReplayCompact12"
	address := reserveAddress(t, core, namespace, "timeout-replay-reserve", 3_600_000)
	payload := []byte("opaque-timeout-replay")
	unitID := submitUnit(t, core, namespace, "timeout-replay-submit", address, payload)
	first, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace, RequestID: "timeout-replay-claim-one",
		AddressID: address, MaxUnits: 1, MaxBytes: 1024, VisibilityTimeoutMs: 1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Second)
	core.NamespacePressure(namespace)
	second, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace, RequestID: "timeout-replay-claim-two",
		AddressID: address, MaxUnits: 1, MaxBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Units) != 1 || second.Units[0].DeliveryAttempt != 2 {
		t.Fatalf("redelivered Claim = %#v", second)
	}
	if _, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace, RequestID: "timeout-replay-settle",
		ClaimID: second.ClaimID, ClaimEpoch: second.ClaimEpoch,
		Entries: []SettleEntry{{UnitID: unitID, Action: SettleComplete}},
	}); err != nil {
		t.Fatal(err)
	}
	replayed, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace, RequestID: "timeout-replay-claim-one",
		AddressID: address, MaxUnits: 1, MaxBytes: 1024, VisibilityTimeoutMs: 1_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ClaimID != first.ClaimID || len(replayed.Units) != 1 || !bytes.Equal(replayed.Units[0].Payload, payload) {
		t.Fatal("timed-out original Claim replay changed after later settlement")
	}
}

func BenchmarkStationUnitLifecycle(b *testing.B) {
	const workers = 64
	core, err := Open(Config{
		DataRoot:  b.TempDir(),
		ProfileID: ProfileSingleNodeGeneralV1,
	}, newTestClock(time.Date(2026, 8, 10, 2, 0, 0, 0, time.UTC)))
	if err != nil {
		b.Fatal(err)
	}
	defer core.Close()

	namespaces := make([]string, workers)
	addresses := make([]string, workers)
	for worker := range workers {
		namespace, err := generateID("ns_")
		if err != nil {
			b.Fatal(err)
		}
		namespaces[worker] = namespace
		reserved, err := core.Reserve(context.Background(), ReserveInput{
			NamespaceID: namespace,
			RequestID:   fmt.Sprintf("bench-reserve-%d", worker),
			TTLMs:       3_600_000,
		})
		if err != nil {
			b.Fatal(err)
		}
		addresses[worker] = reserved.AddressID
	}

	b.ResetTimer()
	started := time.Now()
	var next atomic.Uint64
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				index := int(next.Add(1) - 1)
				if index >= b.N {
					return
				}
				namespace := namespaces[worker]
				address := addresses[worker]
				submitted, err := core.Submit(context.Background(), SubmitInput{
					NamespaceID: namespace,
					RequestID:   fmt.Sprintf("bench-submit-%d", index),
					AddressID:   address,
					Payload:     []byte("opaque"),
					TTLMs:       3_600_000,
				})
				if err != nil {
					b.Error(err)
					return
				}
				claimed, err := core.Claim(context.Background(), ClaimInput{
					NamespaceID:         namespace,
					RequestID:           fmt.Sprintf("bench-claim-%d", index),
					AddressID:           address,
					MaxUnits:            1,
					MaxBytes:            1024,
					VisibilityTimeoutMs: 30_000,
				})
				if err != nil {
					b.Error(err)
					return
				}
				if len(claimed.Units) != 1 || claimed.Units[0].UnitID != submitted.UnitID {
					b.Errorf("unexpected claim result")
					return
				}
				if _, err := core.Settle(context.Background(), SettleInput{
					NamespaceID: namespace,
					RequestID:   fmt.Sprintf("bench-settle-%d", index),
					ClaimID:     claimed.ClaimID,
					ClaimEpoch:  claimed.ClaimEpoch,
					Entries: []SettleEntry{{
						UnitID: submitted.UnitID,
						Action: SettleComplete,
					}},
				}); err != nil {
					b.Error(err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	elapsed := time.Since(started)
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "units/s")
	physical := core.store.physicalSyncs()
	logical := core.store.syncCount.Load()
	if physical > 0 {
		b.ReportMetric(float64(logical)/float64(physical), "logical-batches/physical-sync")
	}
	b.ReportMetric(float64(core.store.maximumGroupSize()), "max-group-batches")
}

func TestMutationResultAndStateUseOneDurableCommit(t *testing.T) {
	clock := newTestClock(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_atomicCommitBoundary12"

	before := core.store.syncCount.Load()
	reserved, err := core.Reserve(context.Background(), ReserveInput{
		NamespaceID: namespace,
		RequestID:   "reserve-atomic",
		TTLMs:       3_600_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOneSyncCommit(t, core, before)

	before = core.store.syncCount.Load()
	submitted, err := core.Submit(context.Background(), SubmitInput{
		NamespaceID: namespace,
		RequestID:   "submit-atomic",
		AddressID:   reserved.AddressID,
		Payload:     []byte("opaque"),
		TTLMs:       3_600_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOneSyncCommit(t, core, before)

	before = core.store.syncCount.Load()
	claimed, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "claim-atomic",
		AddressID:           reserved.AddressID,
		MaxUnits:            1,
		MaxBytes:            1024,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOneSyncCommit(t, core, before)

	before = core.store.syncCount.Load()
	_, err = core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "settle-atomic",
		ClaimID:     claimed.ClaimID,
		ClaimEpoch:  claimed.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: submitted.UnitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	assertOneSyncCommit(t, core, before)

	if err := core.Restart(); err != nil {
		t.Fatal(err)
	}
	replayed, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "settle-atomic",
		ClaimID:     claimed.ClaimID,
		ClaimEpoch:  claimed.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: submitted.UnitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatalf("durable result replay after restart: %v", err)
	}
	if len(replayed.Units) != 1 || replayed.Units[0].UnitID != submitted.UnitID {
		t.Fatalf("unexpected replay: %#v", replayed)
	}
}

func assertOneSyncCommit(t *testing.T, core *Core, before uint64) {
	t.Helper()
	if got := core.store.syncCount.Load() - before; got != 1 {
		t.Fatalf("durable commits = %d, want exactly one atomic commit", got)
	}
}

func TestSchedulerQueueByteBudgetIsBoundedWithControlReserve(t *testing.T) {
	sched := &scheduler{}
	if !sched.reserveQueueBytes(false, maxMutationQueueBytes) {
		t.Fatal("normal queue budget rejected its exact ceiling")
	}
	if sched.reserveQueueBytes(false, 1) {
		t.Fatal("normal queue admitted bytes above its ceiling")
	}
	if !sched.reserveQueueBytes(true, reservedControlQueueBytes) {
		t.Fatal("control queue could not use its reserved bytes")
	}
	if sched.reserveQueueBytes(true, 1) {
		t.Fatal("control queue admitted bytes above total ceiling")
	}
	sched.releaseQueueBytes(maxMutationQueueBytes + reservedControlQueueBytes)
	if got := sched.queuedBytes.Load(); got != 0 {
		t.Fatalf("queued bytes after release = %d", got)
	}
}

func TestWatermarkSlotsRecoverMaximumAcrossReverseCommitOrder(t *testing.T) {
	core := openTestCore(t, newTestClock(time.Date(2026, 8, 10, 0, 30, 0, 0, time.UTC)))
	lowReady := make(chan struct{})
	releaseLow := make(chan struct{})
	lowDone := make(chan error, 1)
	go func() {
		lowDone <- core.store.syncBatchAt(100, 1, func(batch *pebble.Batch, _ *pebble.DB) error {
			close(lowReady)
			<-releaseLow
			return batch.Set([]byte{keyMeta, 't', '1'}, []byte{1}, nil)
		})
	}()
	<-lowReady
	if err := core.store.syncBatchAt(200, 2, func(batch *pebble.Batch, _ *pebble.DB) error {
		return batch.Set([]byte{keyMeta, 't', '2'}, []byte{1}, nil)
	}); err != nil {
		t.Fatal(err)
	}
	close(releaseLow)
	if err := <-lowDone; err != nil {
		t.Fatal(err)
	}
	watermark, err := core.store.loadWatermark()
	if err != nil {
		t.Fatal(err)
	}
	if watermark != 200 {
		t.Fatalf("recovered watermark = %d, want maximum 200", watermark)
	}
}

func TestGroupCommitSharesOnePhysicalSyncAcrossLanes(t *testing.T) {
	core := openTestCore(t, newTestClock(time.Date(2026, 8, 10, 0, 45, 0, 0, time.UTC)))
	const requests = 32
	namespaces := namespacesForPartitionAndLanes(t, 0, requests)
	start := make(chan struct{})
	errors := make(chan error, requests)
	committer := core.store.shards[0].commits
	physicalBefore := committer.physicalSyncs.Load()
	logicalBefore := core.store.syncCount.Load()
	for index := range requests {
		go func(index int) {
			<-start
			errors <- core.store.syncNamespaceBatchAt(namespaces[index], int64(1000+index), namespaceLane(namespaces[index]), func(batch *pebble.Batch, _ *pebble.DB) error {
				return batch.Set([]byte(fmt.Sprintf("group-key-%02d", index)), []byte{1}, nil)
			})
		}(index)
	}
	close(start)
	for range requests {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	// Each call returned after its batch was applied; confirm the outstanding
	// durability window before reading the physical-sync counters.
	if err := core.store.shards[0].commits.drain(); err != nil {
		t.Fatal(err)
	}
	logical := core.store.syncCount.Load() - logicalBefore
	physical := committer.physicalSyncs.Load() - physicalBefore
	if logical != requests {
		t.Fatalf("logical durable batches = %d, want %d", logical, requests)
	}
	if physical == 0 || physical >= logical {
		t.Fatalf("physical syncs = %d for %d logical batches; no grouping occurred", physical, logical)
	}
	if group := committer.maxGroupSize.Load(); group <= 1 || group > groupCommitMaxEntries {
		t.Fatalf("maximum group size = %d", group)
	}
}

func TestPartitionOneDifferentNamespaceLanesGroupButSameNamespaceSerializes(t *testing.T) {
	partition := uint16(9)
	namespaces := namespacesForPartitionAndLanes(t, partition, partitionMutationLanes)
	core, err := Open(Config{
		DataRoot: t.TempDir(), ProfileID: ProfileSingleNodeGeneralV1,
		StorageMode: StorageModePartitionOne, Partition: partition,
	}, newTestClock(time.Date(2026, 8, 10, 0, 47, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	if len(core.sched.lanes) != partitionMutationLanes {
		t.Fatalf("partition scheduler lanes = %d", len(core.sched.lanes))
	}
	core.SetSyncDelay(40 * time.Millisecond)
	committer := core.store.shards[partition].commits
	physicalBefore := committer.physicalSyncs.Load()
	start := make(chan struct{})
	errors := make(chan error, len(namespaces))
	for index, namespace := range namespaces {
		go func(index int, namespace string) {
			<-start
			_, err := core.Reserve(context.Background(), ReserveInput{NamespaceID: namespace, RequestID: fmt.Sprintf("partition-group-%d", index), TTLMs: 60_000})
			errors <- err
		}(index, namespace)
	}
	close(start)
	for range namespaces {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if err := core.store.shards[partition].commits.drain(); err != nil {
		t.Fatal(err)
	}
	physical := committer.physicalSyncs.Load() - physicalBefore
	if physical == 0 || physical >= uint64(len(namespaces)) {
		t.Fatalf("same-partition physical syncs = %d for %d lanes", physical, len(namespaces))
	}

	started := time.Now()
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			_, err := core.Reserve(context.Background(), ReserveInput{NamespaceID: namespaces[0], RequestID: fmt.Sprintf("partition-serial-%d", index), TTLMs: 60_000})
			if err != nil {
				t.Errorf("same namespace Reserve: %v", err)
			}
		}(index)
	}
	wait.Wait()
	if elapsed := time.Since(started); elapsed < 70*time.Millisecond {
		t.Fatalf("same namespace mutations overlapped: %v", elapsed)
	}
}

func TestStoragePoolCommitAdmissionIsGlobalAndBounded(t *testing.T) {
	pool := NewStoragePool()
	for index := 0; index < groupCommitQueueEntries; index++ {
		if !pool.reserveCommit(1) {
			t.Fatalf("admission rejected entry %d", index)
		}
	}
	if pool.reserveCommit(1) {
		t.Fatal("global entry admission exceeded")
	}
	for index := 0; index < groupCommitQueueEntries; index++ {
		pool.releaseCommit(1)
	}
	if !pool.reserveCommit(groupCommitQueueBytes) || pool.reserveCommit(1) {
		t.Fatal("global byte admission is not exact")
	}
	pool.releaseCommit(groupCommitQueueBytes)
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStoragePoolIdleCompactionScannerDoesNotSelfWake(t *testing.T) {
	pool := NewStoragePool()
	core, err := Open(Config{
		DataRoot: t.TempDir(), ProfileID: ProfileSingleNodeGeneralV1,
		StorageMode: StorageModePartitionOne, Partition: 0, StoragePool: pool,
	}, newTestClock(time.Date(2026, 8, 10, 0, 49, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(25 * time.Millisecond)
	before := pool.compactionScans.Load()
	time.Sleep(50 * time.Millisecond)
	if scans := pool.compactionScans.Load() - before; scans > 4 {
		t.Fatalf("idle compaction scanner ran %d times", scans)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPartitionOneRejectsForeignPartitionReadsAndWrites(t *testing.T) {
	owned := uint16(11)
	core, err := Open(Config{
		DataRoot: t.TempDir(), ProfileID: ProfileSingleNodeGeneralV1,
		StorageMode: StorageModePartitionOne, Partition: owned,
	}, newTestClock(time.Date(2026, 8, 10, 0, 48, 0, 0, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	foreign := namespacesForPartitionAndLanes(t, (owned+1)%NamespacePartitions, 1)[0]
	if _, err := core.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: foreign}); err == nil {
		t.Fatal("foreign partition read was accepted")
	}
	if _, err := core.Reserve(context.Background(), ReserveInput{NamespaceID: foreign, RequestID: "foreign-partition", TTLMs: 60_000}); err == nil {
		t.Fatal("foreign partition write was accepted")
	}
}

func namespacesForPartitionAndLanes(t *testing.T, partition uint16, count int) []string {
	t.Helper()
	result := make([]string, 0, count)
	seen := make(map[int]bool, count)
	for attempts := 0; len(result) < count && attempts < 100_000; attempts++ {
		namespace, err := generateID("ns_")
		if err != nil {
			t.Fatal(err)
		}
		lane := namespaceLane(namespace) % count
		if PartitionFor(namespace) == partition && !seen[lane] {
			seen[lane] = true
			result = append(result, namespace)
		}
	}
	if len(result) != count {
		t.Fatalf("found %d/%d partition lane namespaces", len(result), count)
	}
	return result
}

func TestGroupCommitCloseDrainsQueuedBatches(t *testing.T) {
	core := openTestCore(t, newTestClock(time.Date(2026, 8, 10, 0, 50, 0, 0, time.UTC)))
	const requests = 192
	namespace := namespacesForPartitionAndLanes(t, 0, 1)[0]
	keyFor := func(index int) []byte {
		return idempotencyKey(namespace, "drain", fmt.Sprintf("pipeline-key-%03d", index))
	}
	entered := make(chan struct{}, requests)
	release := make(chan struct{})
	errors := make(chan error, requests)
	for index := range requests {
		go func(index int) {
			errors <- core.store.syncNamespaceBatchAt(namespace, 0, namespaceLane(namespace), func(batch *pebble.Batch, _ *pebble.DB) error {
				entered <- struct{}{}
				<-release
				return batch.Set(keyFor(index), []byte{byte(index)}, nil)
			})
		}(index)
	}
	for range requests {
		<-entered
	}
	closeDone := make(chan error, 1)
	go func() { closeDone <- core.store.close() }()
	close(release)
	for range requests {
		if err := <-errors; err != nil {
			t.Fatal(err)
		}
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
	if core.store.opened {
		t.Fatal("store remained open after close")
	}
	// The lifecycle write lock may close only after every queued synchronous
	// commit acknowledged. Reopening must observe every committed key.
	reopened, err := openStore(core.config.DataRoot)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.close()
	for index := range requests {
		if _, err := reopened.get(keyFor(index)); err != nil {
			t.Fatalf("missing drained key %d: %v", index, err)
		}
	}
}

func TestCrossNamespaceSyncWaitsOverlapButSameNamespaceSerializes(t *testing.T) {
	core := openTestCore(t, newTestClock(time.Date(2026, 8, 10, 1, 0, 0, 0, time.UTC)))
	core.SetSyncDelay(80 * time.Millisecond)

	namespaces := make([]string, 0, 2)
	seen := map[int]bool{}
	for i := 0; len(namespaces) < 2; i++ {
		ns, err := generateID("ns_")
		if err != nil {
			t.Fatal(err)
		}
		lane := namespaceLane(ns)
		if !seen[lane] {
			seen[lane] = true
			namespaces = append(namespaces, ns)
		}
	}

	run := func(tag string, names []string) time.Duration {
		start := time.Now()
		var wg sync.WaitGroup
		for i, namespace := range names {
			wg.Add(1)
			go func(i int, namespace string) {
				defer wg.Done()
				_, err := core.Reserve(context.Background(), ReserveInput{
					NamespaceID: namespace,
					RequestID:   fmt.Sprintf("reserve-%s-%d", tag, i),
					TTLMs:       3_600_000,
				})
				if err != nil {
					t.Errorf("Reserve: %v", err)
				}
			}(i, namespace)
		}
		wg.Wait()
		return time.Since(start)
	}

	cross := run("cross", namespaces)
	same := run("same", []string{namespaces[0], namespaces[0]})
	if cross >= 140*time.Millisecond {
		t.Fatalf("cross-namespace commits did not overlap: %v", cross)
	}
	if same < 140*time.Millisecond {
		t.Fatalf("same namespace mutations were not serialized: %v", same)
	}
}
