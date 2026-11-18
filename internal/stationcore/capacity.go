package stationcore

import (
	"math/bits"
	"sync"
)

const (
	mutationLanes                 = 64
	partitionMutationLanes        = 8
	drrQuantumBytes               = 64 << 10
	drrChargeGranularity          = 4096
	reservedControlQueueBytes     = 16 << 20
	maxMutationQueueBytes         = 64 << 20
	maxMutationQueueEntriesPerNS  = 128
	maxControlQueueEntriesPerLane = 128
	maxLongPollClaimsPerNS        = 16
)

const maxUnitsPerAddress = 10_000

func profileLimits() ProfileLimits {
	return ProfileLimits{
		MaxNamespaces:                       1_000,
		MaxAddressesPerNamespace:            10_000,
		MaxUnitsPerNamespace:                100_000,
		MaxRetainedBytesPerNamespace:        4 << 30,
		MaxActiveClaimsPerNamespace:         10_000,
		MaxMutationQueueEntriesPerNamespace: maxMutationQueueEntriesPerNS,
		MaxLongPollClaimsPerNamespace:       maxLongPollClaimsPerNS,
		MutationLanes:                       mutationLanes,
		DRRQuantumBytes:                     drrQuantumBytes,
		DRRChargeGranularity:                drrChargeGranularity,
		ReservedControlQueueBytes:           reservedControlQueueBytes,
	}
}

func unitLogicalCharge(payloadLen int) uint64 {
	return uint64(payloadLen) + 512
}

func claimLogicalCharge(unitCount int) uint64 {
	return 256 + 64*uint64(unitCount)
}

func drrCharge(encodedBytes int) int64 {
	if encodedBytes <= 0 {
		return drrChargeGranularity
	}
	units := (encodedBytes + drrChargeGranularity - 1) / drrChargeGranularity
	charge := units * drrChargeGranularity
	if charge < drrChargeGranularity {
		return drrChargeGranularity
	}
	return int64(charge)
}

type namespaceCounters struct {
	Addresses          uint64
	Units              uint64
	RetainedBytes      uint64
	ActiveClaims       uint64
	IdempotencyRecords uint64
}

type capacityTracker struct {
	mu     sync.Mutex
	limits ProfileLimits

	globalPressure Pressure
	nsPressure     map[string]Pressure
	counters       map[string]*namespaceCounters

	// station is the derived station-wide aggregate, kept equal to the sum of
	// every per-namespace counter under the same mutex. It is never persisted;
	// rebuildStationAggregate restores it from the canonical durable counters
	// after a store open.
	station namespaceCounters

	// stationPressure is the derived station-wide pressure: the max of the
	// global signal and every per-namespace pressure. Per-namespace pressure
	// only ever rises (setSoftPressure), so the max is exact under the same
	// mutex and never needs a recompute pass.
	stationPressure Pressure
}

func newCapacityTracker() *capacityTracker {
	return &capacityTracker{
		limits:     profileLimits(),
		nsPressure: make(map[string]Pressure),
		counters:   make(map[string]*namespaceCounters),
	}
}

func (tracker *capacityTracker) pressure(namespace string) Pressure {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if p, ok := tracker.nsPressure[namespace]; ok && p > tracker.globalPressure {
		return p
	}
	return tracker.globalPressure
}

func (tracker *capacityTracker) setSoftPressure(namespace string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.nsPressure[namespace] = PressureSoft
	if tracker.stationPressure < PressureSoft {
		tracker.stationPressure = PressureSoft
	}
}

func (tracker *capacityTracker) snapshot(namespace string) (Pressure, ResourceVector, ResourceVector) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	counters := tracker.counters[namespace]
	if counters == nil {
		counters = &namespaceCounters{}
	}
	usage := ResourceVector{
		Addresses:          counters.Addresses,
		Units:              counters.Units,
		RetainedBytes:      counters.RetainedBytes,
		ActiveClaims:       counters.ActiveClaims,
		IdempotencyRecords: counters.IdempotencyRecords,
	}
	limit := ResourceVector{
		Addresses:          tracker.limits.MaxAddressesPerNamespace,
		Units:              tracker.limits.MaxUnitsPerNamespace,
		RetainedBytes:      tracker.limits.MaxRetainedBytesPerNamespace,
		ActiveClaims:       tracker.limits.MaxActiveClaimsPerNamespace,
		IdempotencyRecords: 3_000_000,
	}
	pressure := tracker.globalPressure
	if p, ok := tracker.nsPressure[namespace]; ok && p > pressure {
		pressure = p
	}
	return pressure, usage, limit
}

func (tracker *capacityTracker) canReserve(namespace string) error {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.pressureLocked(namespace) >= PressureSoft {
		return fail(ReasonCapacityExceeded, true, 100)
	}
	counters := tracker.counters[namespace]
	if counters == nil {
		counters = &namespaceCounters{}
	}
	if counters.Addresses >= tracker.limits.MaxAddressesPerNamespace {
		return fail(ReasonCapacityExceeded, false, 0)
	}
	return nil
}

func (tracker *capacityTracker) canSubmit(namespace string, addressUnits uint64, charge uint64) error {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.pressureLocked(namespace) >= PressureSoft {
		return fail(ReasonCapacityExceeded, true, 100)
	}
	if addressUnits >= maxUnitsPerAddress {
		return fail(ReasonCapacityExceeded, false, 0)
	}
	counters := tracker.counters[namespace]
	if counters == nil {
		counters = &namespaceCounters{}
	}
	if counters.Units >= tracker.limits.MaxUnitsPerNamespace {
		return fail(ReasonCapacityExceeded, false, 0)
	}
	maxRetained := tracker.limits.MaxRetainedBytesPerNamespace
	if counters.RetainedBytes > maxRetained || charge > maxRetained-counters.RetainedBytes {
		return fail(ReasonCapacityExceeded, false, 0)
	}
	return nil
}

func (tracker *capacityTracker) canClaim(namespace string) error {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.pressureLocked(namespace) >= PressureHard {
		return fail(ReasonCapacityExceeded, false, 0)
	}
	counters := tracker.counters[namespace]
	if counters == nil {
		counters = &namespaceCounters{}
	}
	if counters.ActiveClaims >= tracker.limits.MaxActiveClaimsPerNamespace {
		return fail(ReasonCapacityExceeded, false, 0)
	}
	return nil
}

func (tracker *capacityTracker) chargeAddress(namespace string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.nsLocked(namespace).Addresses++
	tracker.station.Addresses++
}

func (tracker *capacityTracker) releaseAddress(namespace string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	counters := tracker.nsLocked(namespace)
	if counters.Addresses > 0 {
		counters.Addresses--
		if tracker.station.Addresses > 0 {
			tracker.station.Addresses--
		}
	}
}

func (tracker *capacityTracker) chargeUnit(namespace string, charge uint64) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	counters := tracker.nsLocked(namespace)
	counters.Units++
	counters.RetainedBytes += charge
	tracker.station.Units++
	tracker.station.RetainedBytes += charge
}

func (tracker *capacityTracker) releaseUnit(namespace string, charge uint64) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	counters := tracker.nsLocked(namespace)
	if counters.Units > 0 {
		counters.Units--
		if tracker.station.Units > 0 {
			tracker.station.Units--
		}
	}
	// Subtract exactly what the namespace counter released so the aggregate
	// stays the exact sum of the per-namespace counters even under the
	// defensive underflow clamps.
	delta := counters.RetainedBytes
	if delta >= charge {
		delta = charge
	}
	counters.RetainedBytes -= delta
	if tracker.station.RetainedBytes >= delta {
		tracker.station.RetainedBytes -= delta
	} else {
		tracker.station.RetainedBytes = 0
	}
}

func (tracker *capacityTracker) chargeClaim(namespace string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.nsLocked(namespace).ActiveClaims++
	tracker.station.ActiveClaims++
}

func (tracker *capacityTracker) releaseClaim(namespace string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	counters := tracker.nsLocked(namespace)
	if counters.ActiveClaims > 0 {
		counters.ActiveClaims--
		if tracker.station.ActiveClaims > 0 {
			tracker.station.ActiveClaims--
		}
	}
}

func (tracker *capacityTracker) chargeIdempotency(namespace string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.nsLocked(namespace).IdempotencyRecords++
	tracker.station.IdempotencyRecords++
}

func (tracker *capacityTracker) releaseIdempotency(namespace string) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	counters := tracker.nsLocked(namespace)
	if counters.IdempotencyRecords > 0 {
		counters.IdempotencyRecords--
		if tracker.station.IdempotencyRecords > 0 {
			tracker.station.IdempotencyRecords--
		}
	}
}

// currentCounters returns a copy of the namespace's live counter vector. The
// live vector is the exact mirror of the canonical durable counters at every
// lane-serialized mutation boundary, so persistCountersBatch can safely use it
// as the read-modify-write base inside a syncBatch transaction: several
// counter mutations for one namespace in a single transaction (a maintenance
// pass) compose instead of clobbering each other.
func (tracker *capacityTracker) currentCounters(namespace string) namespaceCounters {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	value := *tracker.nsLocked(namespace)
	return value
}

// setCounters installs one namespace's canonical durable counters. It is used
// only on the load path (see rebuildStationAggregate) on a freshly built
// tracker, so it never touches the derived aggregate.
func (tracker *capacityTracker) setCounters(namespace string, counters namespaceCounters) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	value := counters
	tracker.counters[namespace] = &value
}

// rebuildStationAggregate recomputes the derived station-wide aggregate from
// the canonical per-namespace counters after a store open. The aggregate is
// never persisted; the per-namespace counters remain the only durable truth.
func (tracker *capacityTracker) rebuildStationAggregate() bool {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	station := namespaceCounters{}
	for _, counters := range tracker.counters {
		var carry uint64
		station.Addresses, carry = bits.Add64(station.Addresses, counters.Addresses, 0)
		if carry != 0 {
			return false
		}
		station.Units, carry = bits.Add64(station.Units, counters.Units, 0)
		if carry != 0 {
			return false
		}
		station.RetainedBytes, carry = bits.Add64(station.RetainedBytes, counters.RetainedBytes, 0)
		if carry != 0 {
			return false
		}
		station.ActiveClaims, carry = bits.Add64(station.ActiveClaims, counters.ActiveClaims, 0)
		if carry != 0 {
			return false
		}
		station.IdempotencyRecords, carry = bits.Add64(station.IdempotencyRecords, counters.IdempotencyRecords, 0)
		if carry != 0 {
			return false
		}
	}
	tracker.station = station
	return true
}

// stationObservationSnapshot returns pressure and the derived station-wide
// aggregate from one O(1), side-effect-free critical section. Keeping the two
// values under one lock prevents a response from combining different capacity
// transitions.
func (tracker *capacityTracker) stationObservationSnapshot() (Pressure, StationAggregate) {
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	pressure := tracker.globalPressure
	if tracker.stationPressure > pressure {
		pressure = tracker.stationPressure
	}
	if pressure < PressureNormal {
		pressure = PressureNormal
	}
	aggregate := StationAggregate{
		Addresses:          tracker.station.Addresses,
		Units:              tracker.station.Units,
		RetainedBytes:      tracker.station.RetainedBytes,
		ActiveClaims:       tracker.station.ActiveClaims,
		IdempotencyRecords: tracker.station.IdempotencyRecords,
	}
	return pressure, aggregate
}

func (tracker *capacityTracker) nsLocked(namespace string) *namespaceCounters {
	counters, ok := tracker.counters[namespace]
	if !ok {
		counters = &namespaceCounters{}
		tracker.counters[namespace] = counters
	}
	return counters
}

func (tracker *capacityTracker) pressureLocked(namespace string) Pressure {
	if p, ok := tracker.nsPressure[namespace]; ok && p > tracker.globalPressure {
		return p
	}
	return tracker.globalPressure
}
