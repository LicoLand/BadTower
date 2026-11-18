package stationcore

import (
	"context"
	"encoding/binary"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const (
	maintenanceDebtPeriodMs    = 10
	maintenanceIdlePeriodMs    = 1000
	maintenanceMaxItemsPerPass = 1024
	maintenanceMaxDurationMs   = 10
)

type deadlineItem struct {
	key       []byte
	kind      byte
	namespace string
	address   string
	object    string
}

// maintenanceGate serializes bounded maintenance passes. Every request path
// runs one pass in the caller goroutine, so the wall clock is never read from
// an unsupervised background thread. While deadline debt exists a pass runs
// at most once every 10 ms; otherwise once per second (Capacity Model).
type maintenanceGate struct {
	mu     sync.Mutex
	last   time.Duration
	debt   bool
	debtMs time.Duration
	idleMs time.Duration
	next   atomic.Int64
}

func (gate *maintenanceGate) allow(mono time.Duration) bool {
	period := gate.idleMs
	if gate.debt {
		period = gate.debtMs
	}
	if mono-gate.last < period {
		gate.next.Store(int64(gate.last + period))
		return false
	}
	gate.last = mono
	gate.next.Store(int64(mono + period))
	return true
}

func (gate *maintenanceGate) recordPass(debt bool) {
	gate.debt = debt
	period := gate.idleMs
	if debt {
		period = gate.debtMs
	}
	gate.next.Store(int64(gate.last + period))
}

// runMaintenanceIfDue performs one bounded deadline pass after the calling
// request's own work completed (REQ-010). It reads at most the item and time
// budget, sends the collected work to the owning mutation lanes, and each
// lane job rechecks primary state before mutating anything.
func (core *Core) runMaintenanceIfDue() {
	if core.maintenance == nil {
		return
	}
	mono := core.clock.MonotonicNow()
	if next := core.maintenance.next.Load(); next > 0 && int64(mono) < next {
		return
	}
	if !core.maintenance.mu.TryLock() {
		return
	}
	defer core.maintenance.mu.Unlock()
	if !core.maintenance.allow(mono) {
		return
	}
	debt := core.runMaintenancePass()
	core.maintenance.recordPass(debt)
}

func (core *Core) runMaintenancePass() bool {
	now := core.effectiveNowMs()
	iter, err := core.store.newIter([]byte{keyDeadline})
	if err != nil {
		return false
	}
	defer iter.Close()

	var items []deadlineItem
	start := time.Now()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 10 || key[0] != keyDeadline {
			continue
		}
		deadline := int64(binary.BigEndian.Uint64(key[1:9]))
		if deadline > now {
			break
		}
		item, ok := parseDeadlineKey(key)
		if !ok {
			continue
		}
		// Pebble iterator key buffers are only valid until the next iteration;
		// the item keeps its own copy for the lane job.
		item.key = append([]byte(nil), key...)
		items = append(items, item)
		if len(items) >= maintenanceMaxItemsPerPass ||
			time.Since(start) >= maintenanceMaxDurationMs*time.Millisecond {
			break
		}
	}
	if len(items) == 0 {
		return false
	}

	core.mu.RLock()
	tracker := core.capacity
	core.mu.RUnlock()

	type maintenanceGroup struct {
		lane      *mutationLane
		partition uint16
	}
	byLaneAndShard := make(map[maintenanceGroup][]deadlineItem)
	for _, item := range items {
		lane := core.sched.laneFor(item.namespace)
		group := maintenanceGroup{lane: lane, partition: core.storageShardID(PartitionFor(item.namespace))}
		byLaneAndShard[group] = append(byLaneAndShard[group], item)
	}
	for _, grouped := range byLaneAndShard {
		grouped := grouped
		namespace := grouped[0].namespace
		_ = core.sched.run(context.Background(), namespace, true, 0, func() error {
			return core.processDeadlineItems(grouped, now, tracker)
		})
	}
	return true
}

func parseDeadlineKey(key []byte) (deadlineItem, bool) {
	const nsOffset = 10
	namespace := string(key[nsOffset : nsOffset+idTotalLen])
	if !validNamespaceShape(namespace) {
		return deadlineItem{}, false
	}
	item := deadlineItem{key: key, kind: key[9], namespace: namespace}
	switch item.kind {
	case deadlineKindAddress, deadlineKindUnit, deadlineKindClaim, deadlineKindSettlement:
		if len(key) != nsOffset+idTotalLen+idTotalLen+idTotalLen+8 {
			return deadlineItem{}, false
		}
		item.address = string(key[nsOffset+idTotalLen : nsOffset+2*idTotalLen])
		item.object = string(key[nsOffset+2*idTotalLen : nsOffset+3*idTotalLen])
	case deadlineKindIdempotency:
		if len(key) <= nsOffset+idTotalLen+8 {
			return deadlineItem{}, false
		}
		item.object = string(key[nsOffset+idTotalLen : len(key)-8])
	default:
		return deadlineItem{}, false
	}
	return item, true
}

func validNamespaceShape(namespace string) bool {
	if len(namespace) != idTotalLen || namespace[:3] != "ns_" {
		return false
	}
	for index := 3; index < len(namespace); index++ {
		character := namespace[index]
		if character >= 'A' && character <= 'Z' ||
			character >= 'a' && character <= 'z' ||
			character >= '0' && character <= '9' ||
			character == '-' || character == '_' {
			continue
		}
		return false
	}
	return true
}

func (core *Core) processDeadlineItems(items []deadlineItem, now int64, tracker *capacityTracker) error {
	if len(items) == 0 {
		return nil
	}
	return core.store.syncNamespaceBatchAt(items[0].namespace, now, namespaceLane(items[0].namespace), func(batch *pebble.Batch, db *pebble.DB) error {
		for _, item := range items {
			if err := core.applyDeadlineItem(batch, db, item, now, tracker); err != nil {
				return err
			}
		}
		return nil
	})
}

func (core *Core) applyDeadlineItem(
	batch *pebble.Batch,
	db *pebble.DB,
	item deadlineItem,
	now int64,
	tracker *capacityTracker,
) error {
	switch item.kind {
	case deadlineKindAddress:
		return core.expireAddress(batch, db, item, now, tracker)
	case deadlineKindUnit:
		return core.expireUnit(batch, db, item, now, tracker)
	case deadlineKindClaim:
		return core.expireClaim(batch, db, item, now, tracker)
	case deadlineKindIdempotency:
		return core.expireIdempotency(batch, db, item, now, tracker)
	case deadlineKindSettlement:
		return core.expireSettlement(batch, db, item)
	default:
		return nil
	}
}

func (core *Core) expireAddress(
	batch *pebble.Batch,
	db *pebble.DB,
	item deadlineItem,
	now int64,
	tracker *capacityTracker,
) error {
	raw, closer, err := db.Get(addressKey(item.namespace, item.address))
	if err == pebble.ErrNotFound {
		batch.Delete(item.key, nil)
		return nil
	}
	if err != nil {
		return err
	}
	expiry, _, _, _, decErr := decodeAddressRecord(raw)
	closer.Close()
	if decErr != nil {
		return decErr
	}
	if now < expiry {
		batch.Delete(item.key, nil)
		return nil
	}
	batch.Delete(addressKey(item.namespace, item.address), nil)
	batch.Delete(item.key, nil)
	if err := core.persistCountersBatch(batch, db, item.namespace, func(c *namespaceCounters) {
		if c.Addresses > 0 {
			c.Addresses--
		}
	}); err != nil {
		return err
	}
	tracker.releaseAddress(item.namespace)
	return nil
}

func (core *Core) expireUnit(
	batch *pebble.Batch,
	db *pebble.DB,
	item deadlineItem,
	now int64,
	tracker *capacityTracker,
) error {
	raw, closer, err := db.Get(unitMetadataKey(item.namespace, item.address, item.object))
	if err == pebble.ErrNotFound {
		batch.Delete(item.key, nil)
		return nil
	}
	if err != nil {
		return err
	}
	_, expiresAt, availableAt, seq, _, decErr := decodeUnitMetadata(raw)
	closer.Close()
	if decErr != nil {
		return decErr
	}
	if now < expiresAt {
		batch.Delete(item.key, nil)
		return nil
	}
	payload, payloadCloser, payloadErr := db.Get(unitPayloadKey(item.namespace, item.address, item.object))
	if payloadErr != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	charge := unitLogicalCharge(len(payload))
	payloadCloser.Close()
	batch.Delete(unitMetadataKey(item.namespace, item.address, item.object), nil)
	batch.Delete(unitPayloadKey(item.namespace, item.address, item.object), nil)
	batch.Delete(readyKey(item.namespace, item.address, uint64(availableAt), uint64(seq), item.object), nil)
	batch.Delete(item.key, nil)
	batch.Set(settlementKey(item.namespace, item.address, item.object), encodeSettlementRecord(settlementReasonExpired, now), nil)
	batch.Set(deadlineKey(now+idempotencyRetentionMs, deadlineKindSettlement, item.namespace, item.address, item.object, 1), nil, nil)
	if err := core.releaseUnitFromAddress(batch, db, item.namespace, item.address); err != nil {
		return err
	}
	if err := core.persistCountersBatch(batch, db, item.namespace, func(c *namespaceCounters) {
		if c.Units > 0 {
			c.Units--
		}
		if c.RetainedBytes >= charge {
			c.RetainedBytes -= charge
		}
	}); err != nil {
		return err
	}
	tracker.releaseUnit(item.namespace, charge)
	return nil
}

func (core *Core) expireClaim(
	batch *pebble.Batch,
	db *pebble.DB,
	item deadlineItem,
	now int64,
	tracker *capacityTracker,
) error {
	raw, closer, err := db.Get(claimKey(item.namespace, item.address, item.object))
	if err == pebble.ErrNotFound {
		batch.Delete(item.key, nil)
		return nil
	}
	if err != nil {
		return err
	}
	_, claimExp, claimRequest, unitIDs, decErr := decodeClaimRecord(raw)
	closer.Close()
	if decErr != nil {
		return decErr
	}
	if now < claimExp {
		batch.Delete(item.key, nil)
		return nil
	}
	if err := upgradeClaimReplaySnapshotBatch(batch, db, item.namespace, claimRequest); err != nil {
		return err
	}
	for _, unitID := range unitIDs {
		unitRaw, unitCloser, gErr := db.Get(unitMetadataKey(item.namespace, item.address, unitID))
		if gErr == pebble.ErrNotFound {
			continue
		}
		if gErr != nil {
			return gErr
		}
		_, expiresAt, availableAt, seq, attempt, unitDecErr := decodeUnitMetadata(unitRaw)
		unitCloser.Close()
		if unitDecErr != nil {
			return unitDecErr
		}
		redeliverAt := claimExp + int64(releaseDelay(attempt, 0))
		if attempt >= maxDeliveryAttempts || now >= expiresAt || redeliverAt >= expiresAt {
			payload, payloadCloser, payloadErr := db.Get(unitPayloadKey(item.namespace, item.address, unitID))
			if payloadErr != nil {
				return fail(ReasonCorruptState, false, 0)
			}
			charge := unitLogicalCharge(len(payload))
			payloadCloser.Close()
			reason := settlementReasonExpired
			if attempt >= maxDeliveryAttempts {
				reason = settlementReasonAttemptLimit
			}
			batch.Delete(unitMetadataKey(item.namespace, item.address, unitID), nil)
			batch.Delete(unitPayloadKey(item.namespace, item.address, unitID), nil)
			batch.Delete(readyKey(item.namespace, item.address, uint64(availableAt), uint64(seq), unitID), nil)
			batch.Set(settlementKey(item.namespace, item.address, unitID), encodeSettlementRecord(reason, now), nil)
			batch.Set(deadlineKey(now+idempotencyRetentionMs, deadlineKindSettlement, item.namespace, item.address, unitID, 1), nil, nil)
			if err := core.releaseUnitFromAddress(batch, db, item.namespace, item.address); err != nil {
				return err
			}
			if err := core.persistCountersBatch(batch, db, item.namespace, func(c *namespaceCounters) {
				if c.Units > 0 {
					c.Units--
				}
				if c.RetainedBytes >= charge {
					c.RetainedBytes -= charge
				}
			}); err != nil {
				return err
			}
			tracker.releaseUnit(item.namespace, charge)
			continue
		}
		batch.Set(readyKey(item.namespace, item.address, uint64(redeliverAt), uint64(seq), unitID), []byte{1}, nil)
	}
	batch.Delete(claimKey(item.namespace, item.address, item.object), nil)
	batch.Delete(item.key, nil)
	if err := core.persistCountersBatch(batch, db, item.namespace, func(c *namespaceCounters) {
		if c.ActiveClaims > 0 {
			c.ActiveClaims--
		}
	}); err != nil {
		return err
	}
	tracker.releaseClaim(item.namespace)
	core.sched.laneFor(item.namespace).wakeWaiters(waiterKey(item.namespace, item.address))
	return nil
}

func (core *Core) expireIdempotency(
	batch *pebble.Batch,
	db *pebble.DB,
	item deadlineItem,
	now int64,
	tracker *capacityTracker,
) error {
	separator := strings.IndexByte(item.object, 0)
	if separator < 0 {
		batch.Delete(item.key, nil)
		return nil
	}
	operation, request := item.object[:separator], item.object[separator+1:]
	raw, closer, err := db.Get(idempotencyKey(item.namespace, operation, request))
	if err == pebble.ErrNotFound {
		batch.Delete(item.key, nil)
		return nil
	}
	if err != nil {
		return err
	}
	_, _, expiresAt, decErr := decodeIdempotencyRecord(raw)
	closer.Close()
	if decErr != nil {
		return decErr
	}
	if now < expiresAt {
		batch.Delete(item.key, nil)
		return nil
	}
	batch.Delete(idempotencyKey(item.namespace, operation, request), nil)
	batch.Delete(item.key, nil)
	if err := core.persistCountersBatch(batch, db, item.namespace, func(c *namespaceCounters) {
		if c.IdempotencyRecords > 0 {
			c.IdempotencyRecords--
		}
	}); err != nil {
		return err
	}
	tracker.releaseIdempotency(item.namespace)
	return nil
}

func (core *Core) expireSettlement(batch *pebble.Batch, db *pebble.DB, item deadlineItem) error {
	raw, closer, err := db.Get(settlementKey(item.namespace, item.address, item.object))
	if err == nil {
		_, _, decErr := decodeSettlementRecord(raw)
		closer.Close()
		if decErr != nil {
			return decErr
		}
		batch.Delete(settlementKey(item.namespace, item.address, item.object), nil)
	} else if err != pebble.ErrNotFound {
		return err
	}
	batch.Delete(item.key, nil)
	return nil
}

// releaseUnitFromAddress decrements the address-local live unit count when the
// address record still exists.
func (core *Core) releaseUnitFromAddress(batch *pebble.Batch, db *pebble.DB, namespace, address string) error {
	raw, closer, err := db.Get(addressKey(namespace, address))
	if err == pebble.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	expiry, seq, epoch, liveUnits, decErr := decodeAddressRecord(raw)
	closer.Close()
	if decErr != nil {
		return decErr
	}
	if liveUnits > 0 {
		liveUnits--
	}
	batch.Set(addressKey(namespace, address), encodeAddressRecord(expiry, seq, epoch, liveUnits), nil)
	return nil
}
