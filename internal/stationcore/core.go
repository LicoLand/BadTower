package stationcore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type Core struct {
	config   Config // normalized once by Open and immutable thereafter
	clock    Clock
	store    *store
	sched    *scheduler
	capacity *capacityTracker
	life     *lifecycle

	watermark   int64
	clockSafe   bool
	maintenance *maintenanceGate

	mu sync.RWMutex
}

func Open(config Config, clock Clock) (*Core, error) {
	if config.ProfileID != ProfileSingleNodeGeneralV1 {
		return nil, fail(ReasonInvalidRequest, false, 0)
	}
	if config.DataRoot == "" {
		return nil, fail(ReasonInvalidRequest, false, 0)
	}
	if clock == nil {
		return nil, fail(ReasonInvalidRequest, false, 0)
	}
	config.StorageMode = normalizedStorageMode(config.StorageMode)

	st, err := openConfiguredStore(config)
	if err != nil {
		return nil, err
	}

	core := &Core{
		config:   config,
		clock:    clock,
		store:    st,
		capacity: newCapacityTracker(),
		life:     newLifecycle(),
	}
	core.sched = newScheduler(core)
	core.maintenance = &maintenanceGate{
		debtMs: maintenanceDebtPeriodMs * time.Millisecond,
		idleMs: maintenanceIdlePeriodMs * time.Millisecond,
	}

	watermark, err := st.loadWatermark()
	if err != nil {
		_ = st.close()
		return nil, err
	}
	core.watermark = watermark
	core.refreshClockSafety()
	if err := core.loadNamespaceCounters(); err != nil {
		_ = st.close()
		return nil, err
	}
	core.life.startServing()
	return core, nil
}

func (core *Core) Close() error {
	core.mu.Lock()
	core.life.stop()
	core.mu.Unlock()
	core.sched.close()
	return core.store.close()
}

func (core *Core) Restart() error {
	if err := core.store.close(); err != nil {
		return err
	}
	st, err := openConfiguredStore(core.config)
	if err != nil {
		return err
	}
	core.store = st
	watermark, err := st.loadWatermark()
	if err != nil {
		return err
	}
	core.watermark = watermark
	core.refreshClockSafety()
	return core.loadNamespaceCounters()
}

func (core *Core) DataRoot() string { return core.config.DataRoot }

func (core *Core) storageShardID(partition uint16) uint16 {
	if core.config.StorageMode == StorageModePartitionOne {
		return partition
	}
	return partition % instanceStorageShards
}

// Backup creates one independent, verified backup of the live store at the
// destination directory and returns its SHA-256 digest (S-02, REQ-018). The
// backup is a flushed-WAL checkpoint copied byte-for-byte to an independent
// destination, synchronized, digested, and opened in an isolated restore root
// before it is reported as successful. A destination equal to or inside the
// live data root is rejected; the live root is never opened or modified.
func (core *Core) Backup(destination string) (string, error) {
	if err := core.rejectBackupRestoreTargets(destination, ""); err != nil {
		return "", err
	}
	if err := os.MkdirAll(destination, 0o700); err != nil {
		return "", fail(ReasonStorageUnavailable, true, 100)
	}
	staging, err := os.MkdirTemp("", "badtower-backup-staging-")
	if err != nil {
		return "", fail(ReasonStorageUnavailable, true, 100)
	}
	// Pebble's Checkpoint requires the destination directory to not exist;
	// keep the unique MkdirTemp name but drop the pre-created directory so
	// the checkpoint itself creates it. The deferred RemoveAll still cleans
	// up the checkpoint contents and any partial staging residue.
	if err := os.Remove(staging); err != nil {
		return "", fail(ReasonStorageUnavailable, true, 100)
	}
	defer os.RemoveAll(staging)
	if err := core.store.checkpointTo(staging); err != nil {
		return "", err
	}
	if err := copyStoreTree(staging, destination); err != nil {
		return "", fail(ReasonStorageUnavailable, true, 100)
	}
	digest, err := storeTreeDigest(destination)
	if err != nil {
		return "", fail(ReasonStorageUnavailable, true, 100)
	}
	verifyRoot, err := os.MkdirTemp("", "badtower-backup-verify-")
	if err != nil {
		return "", fail(ReasonStorageUnavailable, true, 100)
	}
	defer os.RemoveAll(verifyRoot)
	if err := copyStoreTree(destination, verifyRoot); err != nil {
		return "", fail(ReasonStorageUnavailable, true, 100)
	}
	if err := verifyStoreRoot(verifyRoot); err != nil {
		return "", err
	}
	return digest, nil
}

// RestoreIsolated opens and verifies a backup copy inside an isolated restore
// root, leaving that root as a directly openable native store. The live data
// root is never opened, modified, or targeted, and retired state is never
// inspected (REQ-018).
func (core *Core) RestoreIsolated(backupRoot, isolatedRoot string) error {
	if err := core.rejectBackupRestoreTargets(isolatedRoot, backupRoot); err != nil {
		return err
	}
	if err := os.MkdirAll(isolatedRoot, 0o700); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if err := copyStoreTree(backupRoot, isolatedRoot); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if err := verifyStoreRoot(isolatedRoot); err != nil {
		_ = os.RemoveAll(isolatedRoot)
		return err
	}
	return nil
}

// rejectBackupRestoreTargets rejects destinations that would touch the live
// data root. When backupRoot is non-empty it must also stay outside the live
// root and outside the isolated destination.
func (core *Core) rejectBackupRestoreTargets(isolatedRoot, backupRoot string) error {
	live, err := filepath.Abs(core.config.DataRoot)
	if err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	for _, target := range []string{isolatedRoot, backupRoot} {
		if target == "" {
			continue
		}
		abs, err := filepath.Abs(target)
		if err != nil {
			return fail(ReasonStorageUnavailable, true, 100)
		}
		rel, err := filepath.Rel(live, abs)
		if err != nil {
			return fail(ReasonStorageUnavailable, true, 100)
		}
		if rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
			return fail(ReasonInvalidRequest, false, 0)
		}
		rel, err = filepath.Rel(abs, live)
		if err != nil {
			return fail(ReasonStorageUnavailable, true, 100)
		}
		if rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return fail(ReasonInvalidRequest, false, 0)
		}
	}
	return nil
}

func (core *Core) ProfileLimits() ProfileLimits { return core.capacity.limits }

func (core *Core) NamespacePressure(namespace string) (Pressure, ResourceVector) {
	if core.validateOwnedNamespace(namespace) != nil {
		return PressureHard, ResourceVector{}
	}
	pressure, usage, _ := core.namespacePressureSnapshot(namespace)
	return pressure, usage
}

// ObservationSnapshot returns an identifier-free, privacy-minimal station-wide
// observation. It is side-effect-free: no maintenance pass runs and no store
// I/O or persistence occurs. The aggregate and pressure are read in O(1) under
// the capacity mutex and the lifecycle status under the core lock.
func (core *Core) ObservationSnapshot() StationObservation {
	core.mu.RLock()
	lifecycle := observeLifecycle(core.life.state)
	capacity := core.capacity
	core.mu.RUnlock()
	pressure, aggregate := capacity.stationObservationSnapshot()
	return StationObservation{
		Lifecycle: lifecycle,
		Pressure:  pressure,
		Aggregate: aggregate,
	}
}

func observeLifecycle(state lifecycleState) StationLifecycle {
	switch state {
	case lifecycleStarting:
		return StationLifecycleStarting
	case lifecycleRunning:
		return StationLifecycleRunning
	case lifecycleDraining:
		return StationLifecycleDraining
	default:
		return StationLifecycleStopped
	}
}

// BeginDrain transitions the core lifecycle to DRAINING and is idempotent:
// afterwards Reserve, Submit, and Claim reject with ReasonDraining while
// bounded Settle retains its native rules (C-12). Calling it again, or after
// Close, is a no-op.
func (core *Core) BeginDrain() {
	core.mu.Lock()
	core.life.beginDrain()
	core.mu.Unlock()
}

func (core *Core) ForceSoftPressure(namespace string) {
	core.capacity.setSoftPressure(namespace)
}

// SetSyncDelay inserts an artificial delay before each synchronous Pebble commit.
// It exists for capacity fault injection and returns to zero delay when d <= 0.
func (core *Core) SetSyncDelay(d time.Duration) {
	core.store.setSyncDelay(d)
}

func (core *Core) Ready() (bool, Reason) {
	core.mu.RLock()
	defer core.mu.RUnlock()
	if !core.clockSafe {
		return false, ReasonClockUnsafe
	}
	if core.life.state != lifecycleRunning {
		return false, ReasonDraining
	}
	return true, ""
}

func (core *Core) GetCapacity(_ context.Context, in GetCapacityInput) (GetCapacityOutput, error) {
	if err := validateNamespace(in.NamespaceID); err != nil {
		return GetCapacityOutput{}, err
	}
	if err := core.validateOwnedNamespace(in.NamespaceID); err != nil {
		return GetCapacityOutput{}, err
	}
	pressure, usage, limit := core.namespacePressureSnapshot(in.NamespaceID)
	var retryAfter uint32
	if pressure >= PressureSoft {
		retryAfter = 100
	}
	return GetCapacityOutput{
		ProfileID:      ProfileSingleNodeGeneralV1,
		Pressure:       pressure,
		NamespaceUsage: usage,
		NamespaceLimit: limit,
		RetryAfterMs:   retryAfter,
	}, nil
}

func (core *Core) namespacePressureSnapshot(namespace string) (Pressure, ResourceVector, ResourceVector) {
	core.runMaintenanceIfDue()
	return core.capacity.snapshot(namespace)
}

func (core *Core) Reserve(ctx context.Context, in ReserveInput) (ReserveOutput, error) {
	return core.reserve(ctx, in, nil)
}

func (core *Core) reserve(ctx context.Context, in ReserveInput, directive *mutationDirective) (ReserveOutput, error) {
	if err := core.guardMutation(); err != nil {
		return ReserveOutput{}, err
	}
	if err := validateNamespace(in.NamespaceID); err != nil {
		return ReserveOutput{}, err
	}
	if err := core.validateOwnedNamespace(in.NamespaceID); err != nil {
		return ReserveOutput{}, err
	}
	if err := validateRequestID(in.RequestID); err != nil {
		return ReserveOutput{}, err
	}
	ttl := normalizeAddressTTL(in.TTLMs)
	fields := []FingerprintField{
		{1, []byte(in.RequestID)},
	}
	if in.AddressID != "" {
		fields = append(fields, FingerprintField{2, []byte(in.AddressID)})
	}
	fields = append(fields, FingerprintField{3, uint64Bytes(ttl)})
	fp := RequestFingerprint(in.NamespaceID, opNameReserve, fields...)

	var out ReserveOutput
	err := core.sched.run(ctx, in.NamespaceID, false, drrCharge(256), func() error {
		now := core.commandNow(directive)
		if replay, err := core.checkIdempotencyAt(in.NamespaceID, opNameReserve, in.RequestID, fp, now); err != nil {
			return err
		} else if replay != nil {
			return json.Unmarshal(replay, &out)
		}

		if in.AddressID == "" {
			if err := core.capacity.canReserve(in.NamespaceID); err != nil {
				return err
			}
			addressID, genErr := commandGeneratedID(directive, idPrefixAddress)
			if genErr != nil {
				return fail(ReasonInternal, false, 0)
			}
			expiresAt := now + int64(ttl)
			out = ReserveOutput{AddressID: addressID, ExpiresAtUnixMs: expiresAt, Created: true}
			if err := core.store.syncNamespaceBatchAt(in.NamespaceID, now, namespaceLane(in.NamespaceID), func(batch *pebble.Batch, db *pebble.DB) error {
				batch.Set(addressKey(in.NamespaceID, addressID), encodeAddressRecord(expiresAt, 1, 1, 0), nil)
				batch.Set(deadlineKey(expiresAt, deadlineKindAddress, in.NamespaceID, addressID, addressID, 1), nil, nil)
				if err := appendIdempotencyBatch(batch, in.NamespaceID, opNameReserve, in.RequestID, fp, out, now); err != nil {
					return err
				}
				return core.persistCountersBatch(batch, db, in.NamespaceID, func(c *namespaceCounters) {
					c.Addresses++
					c.IdempotencyRecords++
				})
			}); err != nil {
				return err
			}
			core.capacity.chargeAddress(in.NamespaceID)
			core.capacity.chargeIdempotency(in.NamespaceID)
		} else {
			if err := validateAddressID(in.AddressID); err != nil {
				return err
			}
			raw, err := core.store.get(addressKey(in.NamespaceID, in.AddressID))
			if err == pebble.ErrNotFound {
				return fail(ReasonAddressNotFound, false, 0)
			}
			if err != nil {
				return err
			}
			expiresAt, seq, epoch, liveUnits, err := decodeAddressRecord(raw)
			if err != nil {
				return err
			}
			if now >= expiresAt {
				return fail(ReasonAddressExpired, false, 0)
			}
			newExpiry := expiresAt
			if candidate := now + int64(ttl); candidate > newExpiry {
				newExpiry = candidate
			}
			out = ReserveOutput{AddressID: in.AddressID, ExpiresAtUnixMs: newExpiry, Created: false}
			if err := core.store.syncNamespaceBatchAt(in.NamespaceID, now, namespaceLane(in.NamespaceID), func(batch *pebble.Batch, db *pebble.DB) error {
				batch.Set(addressKey(in.NamespaceID, in.AddressID), encodeAddressRecord(newExpiry, seq, epoch, liveUnits), nil)
				batch.Set(deadlineKey(newExpiry, deadlineKindAddress, in.NamespaceID, in.AddressID, in.AddressID, 1), nil, nil)
				if err := appendIdempotencyBatch(batch, in.NamespaceID, opNameReserve, in.RequestID, fp, out, now); err != nil {
					return err
				}
				return core.persistCountersBatch(batch, db, in.NamespaceID, func(c *namespaceCounters) {
					c.IdempotencyRecords++
				})
			}); err != nil {
				return err
			}
			core.capacity.chargeIdempotency(in.NamespaceID)
		}
		return nil
	})
	if err != nil {
		return ReserveOutput{}, err
	}
	core.runMaintenanceIfDue()
	return out, nil
}

func (core *Core) Submit(ctx context.Context, in SubmitInput) (SubmitOutput, error) {
	return core.submit(ctx, in, nil)
}

func (core *Core) submit(ctx context.Context, in SubmitInput, directive *mutationDirective) (SubmitOutput, error) {
	if err := core.guardMutation(); err != nil {
		return SubmitOutput{}, err
	}
	if err := validateNamespace(in.NamespaceID); err != nil {
		return SubmitOutput{}, err
	}
	if err := core.validateOwnedNamespace(in.NamespaceID); err != nil {
		return SubmitOutput{}, err
	}
	if err := validateRequestID(in.RequestID); err != nil {
		return SubmitOutput{}, err
	}
	if err := validateAddressID(in.AddressID); err != nil {
		return SubmitOutput{}, err
	}
	if len(in.Payload) < minPayloadBytes || len(in.Payload) > maxPayloadBytes {
		return SubmitOutput{}, fail(ReasonInvalidRequest, false, 0)
	}
	ttl := normalizeUnitTTL(in.TTLMs)
	fp := RequestFingerprint(in.NamespaceID, opNameSubmit,
		FingerprintField{1, []byte(in.RequestID)},
		FingerprintField{2, []byte(in.AddressID)},
		FingerprintField{3, payloadField(in.Payload)},
		FingerprintField{4, uint64Bytes(ttl)},
	)

	var out SubmitOutput
	charge := unitLogicalCharge(len(in.Payload))
	err := core.sched.run(ctx, in.NamespaceID, false, drrCharge(len(in.Payload)+512), func() error {
		now := core.commandNow(directive)
		if replay, err := core.checkIdempotencyAt(in.NamespaceID, opNameSubmit, in.RequestID, fp, now); err != nil {
			return err
		} else if replay != nil {
			return json.Unmarshal(replay, &out)
		}
		if err := core.capacity.canSubmit(in.NamespaceID, 0, charge); err != nil {
			return err
		}
		addrRaw, err := core.store.get(addressKey(in.NamespaceID, in.AddressID))
		if err == pebble.ErrNotFound {
			return fail(ReasonAddressNotFound, false, 0)
		}
		if err != nil {
			return err
		}
		addrExp, seq, epoch, liveUnits, err := decodeAddressRecord(addrRaw)
		if err != nil {
			return err
		}
		if err := core.capacity.canSubmit(in.NamespaceID, liveUnits, charge); err != nil {
			return err
		}
		if now >= addrExp {
			return fail(ReasonAddressExpired, false, 0)
		}
		if seq <= 0 {
			return fail(ReasonCorruptState, false, 0)
		}
		unitExp := now + int64(ttl)
		if unitExp > addrExp {
			unitExp = addrExp
		}
		unitID, err := commandGeneratedID(directive, idPrefixUnit)
		if err != nil {
			return fail(ReasonInternal, false, 0)
		}
		out = SubmitOutput{UnitID: unitID, AcceptedAtUnixMs: now, ExpiresAtUnixMs: unitExp}
		availableAt := now
		unitMetadata := encodeUnitMetadata(now, unitExp, availableAt, seq, 0)
		if err := core.store.syncNamespaceBatchAt(in.NamespaceID, now, namespaceLane(in.NamespaceID), func(batch *pebble.Batch, db *pebble.DB) error {
			batch.Set(unitMetadataKey(in.NamespaceID, in.AddressID, unitID), unitMetadata, nil)
			batch.Set(unitPayloadKey(in.NamespaceID, in.AddressID, unitID), in.Payload, nil)
			batch.Set(readyKey(in.NamespaceID, in.AddressID, uint64(availableAt), uint64(seq), unitID), []byte{1}, nil)
			batch.Set(deadlineKey(unitExp, deadlineKindUnit, in.NamespaceID, in.AddressID, unitID, 1), nil, nil)
			batch.Set(addressKey(in.NamespaceID, in.AddressID), encodeAddressRecord(addrExp, seq+1, epoch, liveUnits+1), nil)
			if err := appendIdempotencyBatch(batch, in.NamespaceID, opNameSubmit, in.RequestID, fp, out, now); err != nil {
				return err
			}
			return core.persistCountersBatch(batch, db, in.NamespaceID, func(c *namespaceCounters) {
				c.Units++
				c.RetainedBytes += charge
				c.IdempotencyRecords++
			})
		}); err != nil {
			return err
		}
		core.capacity.chargeUnit(in.NamespaceID, charge)
		core.capacity.chargeIdempotency(in.NamespaceID)
		core.store.payloadWrites.Add(1)
		core.store.payloadBytes.Add(uint64(len(in.Payload)))
		core.sched.laneFor(in.NamespaceID).wakeWaiters(waiterKey(in.NamespaceID, in.AddressID))
		return nil
	})
	if err != nil {
		return SubmitOutput{}, err
	}
	core.runMaintenanceIfDue()
	return out, nil
}

func (core *Core) Claim(ctx context.Context, in ClaimInput) (ClaimOutput, error) {
	return core.claim(ctx, in, nil)
}

func (core *Core) claim(ctx context.Context, in ClaimInput, directive *mutationDirective) (ClaimOutput, error) {
	if err := core.guardClaim(); err != nil {
		return ClaimOutput{}, err
	}
	if err := validateNamespace(in.NamespaceID); err != nil {
		return ClaimOutput{}, err
	}
	if err := core.validateOwnedNamespace(in.NamespaceID); err != nil {
		return ClaimOutput{}, err
	}
	if err := validateRequestID(in.RequestID); err != nil {
		return ClaimOutput{}, err
	}
	if err := validateAddressID(in.AddressID); err != nil {
		return ClaimOutput{}, err
	}
	maxUnits, maxBytes, visibilityMs, waitMs := normalizeClaimBounds(in)
	fp := RequestFingerprint(in.NamespaceID, opNameClaim,
		FingerprintField{1, []byte(in.RequestID)},
		FingerprintField{2, []byte(in.AddressID)},
		FingerprintField{3, uint64Bytes(uint64(maxUnits))},
		FingerprintField{4, uint64Bytes(maxBytes)},
		FingerprintField{5, uint64Bytes(visibilityMs)},
		FingerprintField{6, uint64Bytes(waitMs)},
	)

	deadline := time.Time{}
	if waitMs > 0 {
		deadline = time.Now().Add(time.Duration(waitMs) * time.Millisecond)
	}

	for {
		var out ClaimOutput
		err := core.sched.run(ctx, in.NamespaceID, false, drrCharge(1024), func() error {
			now := core.commandNow(directive)
			if replay, err := core.checkClaimIdempotencyAt(in.NamespaceID, in.RequestID, fp, now); err != nil {
				return err
			} else if replay != nil {
				out = *replay
				return nil
			}
			result, created, claimErr := core.tryClaim(in.NamespaceID, in.AddressID, maxUnits, maxBytes, visibilityMs, now, directive, in.RequestID, fp)
			if claimErr != nil {
				return claimErr
			}
			if result == nil {
				// Only a final empty result is durable; an intermediate wait
				// cycle must never trap the same request in an empty replay.
				// A replicated Claim performs exactly one deterministic state
				// transition. Long-poll waiting belongs to the leader-side router;
				// it must never stall the single Raft apply goroutine.
				final := directive == nil || waitMs == 0 || directive.finalAttempt
				if ctx.Err() != nil {
					final = true
				}
				out = ClaimOutput{}
				if final {
					return core.storeIdempotency(in.NamespaceID, opNameClaim, in.RequestID, fp, out, now)
				}
				return nil
			}
			out = *result
			if !created {
				// Existing claim ownership is already durable; returning it
				// must not freeze a later redelivery behind one result.
				return nil
			}
			return nil
		})
		if err != nil {
			return ClaimOutput{}, err
		}
		core.runMaintenanceIfDue()
		if directive != nil {
			return out, nil
		}
		if len(out.Units) > 0 || waitMs == 0 {
			return out, nil
		}
		if ctx.Err() != nil {
			return ClaimOutput{}, ctx.Err()
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			return out, nil
		}
		lane := core.sched.laneFor(in.NamespaceID)
		waiter, err := lane.registerWaiter(waiterKey(in.NamespaceID, in.AddressID))
		if err != nil {
			return ClaimOutput{}, err
		}
		waitCtx := ctx
		remaining := time.Until(deadline)
		if err := waitForClaim(waitCtx, waiter, remaining); err != nil {
			lane.removeWaiter(waiterKey(in.NamespaceID, in.AddressID), waiter)
			return ClaimOutput{}, err
		}
		lane.removeWaiter(waiterKey(in.NamespaceID, in.AddressID), waiter)
	}
}

func (core *Core) Settle(ctx context.Context, in SettleInput) (SettleOutput, error) {
	return core.settle(ctx, in, nil)
}

func (core *Core) settle(ctx context.Context, in SettleInput, directive *mutationDirective) (SettleOutput, error) {
	if err := core.guardSettle(); err != nil {
		return SettleOutput{}, err
	}
	if err := validateNamespace(in.NamespaceID); err != nil {
		return SettleOutput{}, err
	}
	if err := core.validateOwnedNamespace(in.NamespaceID); err != nil {
		return SettleOutput{}, err
	}
	if err := validateRequestID(in.RequestID); err != nil {
		return SettleOutput{}, err
	}
	if err := validateClaimID(in.ClaimID); err != nil {
		return SettleOutput{}, err
	}
	if len(in.Entries) == 0 || len(in.Entries) > maxSettleEntries {
		return SettleOutput{}, fail(ReasonInvalidRequest, false, 0)
	}
	fpFields := []FingerprintField{
		{1, []byte(in.RequestID)},
		{2, []byte(in.ClaimID)},
		{3, uint64Bytes(in.ClaimEpoch)},
	}
	for i, entry := range in.Entries {
		fpFields = append(fpFields,
			FingerprintField{uint16(4 + i*4), []byte(entry.UnitID)},
			FingerprintField{uint16(5 + i*4), []byte{byte(entry.Action)}},
			FingerprintField{uint16(6 + i*4), uint64Bytes(entry.DelayMs)},
			FingerprintField{uint16(7 + i*4), uint64Bytes(entry.ExtensionMs)},
		)
	}
	fp := RequestFingerprint(in.NamespaceID, opNameSettle, fpFields...)

	var out SettleOutput
	err := core.sched.run(ctx, in.NamespaceID, true, drrCharge(512+len(in.Entries)*128), func() error {
		now := core.commandNow(directive)
		if replay, err := core.checkIdempotencyAt(in.NamespaceID, opNameSettle, in.RequestID, fp, now); err != nil {
			return err
		} else if replay != nil {
			return json.Unmarshal(replay, &out)
		}
		if err := core.validateSettleEntries(in.Entries); err != nil {
			return err
		}
		units, err := core.applySettle(in, now, fp)
		if err != nil {
			return err
		}
		out = SettleOutput{Units: units}
		return nil
	})
	if err != nil {
		return SettleOutput{}, err
	}
	core.runMaintenanceIfDue()
	return out, nil
}

const (
	opNameReserve = "reserve"
	opNameSubmit  = "submit"
	opNameClaim   = "claim"
	opNameSettle  = "settle"
)

func RequestFingerprint(namespace, operation string, fields ...FingerprintField) []byte {
	h := sha256.New()
	h.Write([]byte("badtower.station.v1.request"))
	h.Write([]byte{0})
	h.Write([]byte{operationByte(operation)})
	h.Write([]byte(namespace))
	for _, field := range fields {
		var num [2]byte
		binary.BigEndian.PutUint16(num[:], field.Number)
		h.Write(num[:])
		var length [8]byte
		binary.BigEndian.PutUint64(length[:], uint64(len(field.Value)))
		h.Write(length[:])
		h.Write(field.Value)
	}
	return h.Sum(nil)
}

func operationByte(operation string) byte {
	switch operation {
	case opNameReserve:
		return 1
	case opNameSubmit:
		return 2
	case opNameClaim:
		return 3
	case opNameSettle:
		return 4
	default:
		return 0
	}
}

func payloadField(payload []byte) []byte {
	sum := sha256.Sum256(payload)
	out := make([]byte, 8+32)
	binary.BigEndian.PutUint64(out, uint64(len(payload)))
	copy(out[8:], sum[:])
	return out
}

func uint64Bytes(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func (core *Core) guardMutation() error {
	core.mu.RLock()
	defer core.mu.RUnlock()
	now := core.clock.NowUnixMs()
	if now < core.watermark-clockBackwardClampMs {
		return fail(ReasonClockUnsafe, false, 0)
	}
	if !core.clockSafe {
		return fail(ReasonClockUnsafe, false, 0)
	}
	if !core.life.acceptsAdmission() {
		return fail(ReasonDraining, true, 100)
	}
	return nil
}

func (core *Core) guardClaim() error {
	core.mu.RLock()
	defer core.mu.RUnlock()
	now := core.clock.NowUnixMs()
	if now < core.watermark-clockBackwardClampMs {
		return fail(ReasonClockUnsafe, false, 0)
	}
	if !core.clockSafe {
		return fail(ReasonClockUnsafe, false, 0)
	}
	if !core.life.acceptsClaim() {
		return fail(ReasonDraining, true, 100)
	}
	return nil
}

func (core *Core) guardSettle() error {
	core.mu.RLock()
	defer core.mu.RUnlock()
	now := core.clock.NowUnixMs()
	if now < core.watermark-clockBackwardClampMs {
		return fail(ReasonClockUnsafe, false, 0)
	}
	if !core.clockSafe {
		return fail(ReasonClockUnsafe, false, 0)
	}
	if !core.life.acceptsSettle() {
		return fail(ReasonDraining, true, 100)
	}
	return nil
}

func (core *Core) effectiveNowMs() int64 {
	now := core.clock.NowUnixMs()
	core.mu.Lock()
	defer core.mu.Unlock()
	if now < core.watermark-clockBackwardClampMs {
		core.clockSafe = false
	}
	if now < core.watermark {
		now = core.watermark
	}
	if now > core.watermark {
		core.watermark = now
	}
	return now
}

func (core *Core) refreshClockSafety() {
	now := core.clock.NowUnixMs()
	core.mu.Lock()
	defer core.mu.Unlock()
	core.clockSafe = now >= core.watermark-clockBackwardClampMs
}

func (core *Core) loadNamespaceCounters() error {
	iter, err := core.store.newIter([]byte{keyNamespaceCtr})
	if err != nil {
		return err
	}
	defer iter.Close()
	replacement := newCapacityTracker()
	for iter.First(); iter.Valid(); iter.Next() {
		key := string(iter.Key())
		if len(key) <= 1 {
			continue
		}
		namespace := key[1:]
		counters, decErr := decodeNamespaceCounters(iter.Value())
		if decErr != nil {
			return decErr
		}
		replacement.setCounters(namespace, counters)
	}
	if !replacement.rebuildStationAggregate() {
		return fail(ReasonCorruptState, false, 0)
	}
	core.mu.Lock()
	core.capacity = replacement
	core.mu.Unlock()
	return nil
}

// persistCountersBatch applies one read-modify-write to the namespace's
// canonical durable counters inside an open syncBatch transaction, persisting
// the result atomically with the mutation that carries it. The read base is
// the live capacity tracker, not the DB: the tracker is the exact mirror of
// the durable counters at every lane-serialized mutation boundary (each
// mutation persists and then charges/releases under the same lane job), and a
// maintenance pass may persist several counter mutations for one namespace in
// a single batch — reading the DB for each would miss the earlier pending
// writes and clobber them, so maintenance-driven expiry would not survive a
// reopen. The db parameter is retained for the shared call-site signature.
func (core *Core) persistCountersBatch(batch *pebble.Batch, db *pebble.DB, namespace string, fn func(*namespaceCounters)) error {
	counters := core.capacity.currentCounters(namespace)
	fn(&counters)
	return batch.Set(namespaceCounterKey(namespace), encodeNamespaceCounters(counters), nil)
}

func (core *Core) checkIdempotency(namespace, operation, request string, fingerprint []byte) ([]byte, error) {
	return core.checkIdempotencyAt(namespace, operation, request, fingerprint, core.effectiveNowMs())
}

func (core *Core) checkIdempotencyAt(namespace, operation, request string, fingerprint []byte, now int64) ([]byte, error) {
	raw, err := core.store.get(idempotencyKey(namespace, operation, request))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	storedFP, response, expiresAt, err := decodeIdempotencyRecord(raw)
	if err != nil {
		return nil, err
	}
	if now >= expiresAt {
		return nil, nil
	}
	if !bytesEqual(storedFP, fingerprint) {
		return nil, fail(ReasonIdempotencyConflict, false, 0)
	}
	return response, nil
}

func appendIdempotencyBatch(batch *pebble.Batch, namespace, operation, request string, fingerprint []byte, payload any, now int64) error {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return fail(ReasonInternal, false, 0)
	}
	expiresAt := now + idempotencyRetentionMs
	record := encodeIdempotencyRecord(fingerprint, encoded, expiresAt)
	batch.Set(idempotencyKey(namespace, operation, request), record, nil)
	batch.Set(deadlineKey(expiresAt, deadlineKindIdempotency, namespace, "", operation+"\x00"+request, 1), nil, nil)
	return nil
}

func (core *Core) storeIdempotency(namespace, operation, request string, fingerprint []byte, payload any, now int64) error {
	if err := core.store.syncNamespaceBatchAt(namespace, now, namespaceLane(namespace), func(batch *pebble.Batch, db *pebble.DB) error {
		if err := appendIdempotencyBatch(batch, namespace, operation, request, fingerprint, payload, now); err != nil {
			return err
		}
		return core.persistCountersBatch(batch, db, namespace, func(c *namespaceCounters) {
			c.IdempotencyRecords++
		})
	}); err != nil {
		return err
	}
	core.capacity.chargeIdempotency(namespace)
	return nil
}

func (core *Core) activeClaimOutput(namespace, address string, maxUnits uint32, maxBytes uint64, now int64) (*ClaimOutput, error) {
	prefix := append([]byte{keyClaim}, []byte(namespace)...)
	prefix = append(prefix, []byte(address)...)
	iter, err := core.store.newIter(prefix)
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		claimID := string(iter.Key()[len(prefix):])
		epoch, claimExp, _, unitIDs, decErr := decodeClaimRecord(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		// A present claim keeps fencing its units until the deadline scanner
		// redelivers them; expiring it here would never mutate ownership.
		units := make([]ClaimedUnit, 0, len(unitIDs))
		var totalBytes uint64
		for _, unitID := range unitIDs {
			if uint32(len(units)) >= maxUnits {
				break
			}
			unitRaw, gErr := core.store.get(unitMetadataKey(namespace, address, unitID))
			if gErr != nil {
				continue
			}
			acceptedAt, unitExp, _, _, attempt, dErr := decodeUnitMetadata(unitRaw)
			if dErr != nil {
				return nil, dErr
			}
			payload, gErr := core.store.get(unitPayloadKey(namespace, address, unitID))
			if gErr != nil {
				return nil, fail(ReasonCorruptState, false, 0)
			}
			if totalBytes+uint64(len(payload)) > maxBytes {
				break
			}
			totalBytes += uint64(len(payload))
			units = append(units, ClaimedUnit{
				UnitID: unitID, Payload: payload,
				AcceptedAtUnixMs: acceptedAt, ExpiresAtUnixMs: unitExp,
				DeliveryAttempt: attempt,
			})
		}
		if len(units) == 0 {
			continue
		}
		return &ClaimOutput{
			ClaimID: claimID, ClaimEpoch: epoch,
			ClaimExpiresAtUnixMs: claimExp, Units: units,
		}, nil
	}
	return nil, nil
}

func (core *Core) tryClaim(namespace, address string, maxUnits uint32, maxBytes, visibilityMs uint64, now int64, directive *mutationDirective, request string, fingerprint []byte) (*ClaimOutput, bool, error) {
	if err := core.capacity.canClaim(namespace); err != nil {
		return nil, false, err
	}
	addrRaw, err := core.store.get(addressKey(namespace, address))
	if err == pebble.ErrNotFound {
		return nil, false, fail(ReasonAddressNotFound, false, 0)
	}
	if err != nil {
		return nil, false, err
	}
	addrExp, _, claimEpoch, _, err := decodeAddressRecord(addrRaw)
	if err != nil {
		return nil, false, err
	}
	if now >= addrExp {
		return nil, false, fail(ReasonAddressExpired, false, 0)
	}
	newEpoch := claimEpoch + 1

	iter, err := core.store.newIter(readyPrefix(namespace, address))
	if err != nil {
		return nil, false, err
	}
	defer iter.Close()

	type selected struct {
		unitID      string
		availableAt uint64
		seq         uint64
		payload     []byte
		acceptedAt  int64
		expiresAt   int64
		attempt     uint32
		charge      uint64
	}
	var picks []selected
	var totalBytes uint64

	for iter.First(); iter.Valid() && uint32(len(picks)) < maxUnits; iter.Next() {
		key := iter.Key()
		if len(key) < 1+len(namespace)+len(address)+16+idTotalLen {
			continue
		}
		off := 1 + len(namespace) + len(address)
		availableAt := binary.BigEndian.Uint64(key[off:])
		if int64(availableAt) > now {
			break
		}
		seq := binary.BigEndian.Uint64(key[off+8:])
		unitID := string(key[off+16:])
		unitRaw, gErr := core.store.get(unitMetadataKey(namespace, address, unitID))
		if gErr != nil {
			continue
		}
		acceptedAt, unitExp, _, _, attempt, dErr := decodeUnitMetadata(unitRaw)
		if dErr != nil {
			return nil, false, dErr
		}
		payload, gErr := core.store.get(unitPayloadKey(namespace, address, unitID))
		if gErr != nil {
			return nil, false, fail(ReasonCorruptState, false, 0)
		}
		if now >= unitExp {
			continue
		}
		if totalBytes+uint64(len(payload)) > maxBytes {
			break
		}
		picks = append(picks, selected{
			unitID: unitID, availableAt: availableAt, seq: seq,
			payload: payload, acceptedAt: acceptedAt, expiresAt: unitExp,
			attempt: attempt + 1, charge: unitLogicalCharge(len(payload)),
		})
		totalBytes += uint64(len(payload))
	}
	if len(picks) == 0 {
		if active, activeErr := core.activeClaimOutput(namespace, address, maxUnits, maxBytes, now); activeErr != nil {
			return nil, false, activeErr
		} else if active != nil {
			return active, false, nil
		}
		return nil, false, nil
	}

	claimID, err := commandGeneratedID(directive, idPrefixClaim)
	if err != nil {
		return nil, false, fail(ReasonInternal, false, 0)
	}
	claimEpoch = newEpoch
	claimDeadline := now + int64(visibilityMs)
	minUnitExp := picks[0].expiresAt
	for _, pick := range picks {
		if pick.expiresAt < minUnitExp {
			minUnitExp = pick.expiresAt
		}
	}
	if claimDeadline > minUnitExp {
		claimDeadline = minUnitExp
	}
	if claimDeadline > addrExp {
		claimDeadline = addrExp
	}

	unitIDs := make([]string, len(picks))
	units := make([]ClaimedUnit, len(picks))
	for i, pick := range picks {
		unitIDs[i] = pick.unitID
		units[i] = ClaimedUnit{
			UnitID: pick.unitID, Payload: pick.payload,
			AcceptedAtUnixMs: pick.acceptedAt, ExpiresAtUnixMs: pick.expiresAt,
			DeliveryAttempt: pick.attempt,
		}
	}

	out := &ClaimOutput{
		ClaimID: claimID, ClaimEpoch: uint64(claimEpoch),
		ClaimExpiresAtUnixMs: claimDeadline, Units: units,
	}
	if err := core.store.syncNamespaceBatchAt(namespace, now, namespaceLane(namespace), func(batch *pebble.Batch, db *pebble.DB) error {
		batch.Set(claimKey(namespace, address, claimID), encodeClaimRecord(uint64(claimEpoch), claimDeadline, request, unitIDs), nil)
		batch.Set(deadlineKey(claimDeadline, deadlineKindClaim, namespace, address, claimID, 1), nil, nil)
		exp, seq, _, liveUnits, dErr := decodeAddressRecord(addrRaw)
		if dErr != nil {
			return dErr
		}
		batch.Set(addressKey(namespace, address), encodeAddressRecord(exp, seq, claimEpoch, liveUnits), nil)
		for _, pick := range picks {
			batch.Delete(readyKey(namespace, address, pick.availableAt, pick.seq, pick.unitID), nil)
			batch.Set(unitMetadataKey(namespace, address, pick.unitID), encodeUnitMetadata(pick.acceptedAt, pick.expiresAt, int64(pick.availableAt), int64(pick.seq), pick.attempt), nil)
		}
		if err := appendClaimIdempotencyBatch(batch, namespace, request, fingerprint, newClaimReplayReference(address, out), now); err != nil {
			return err
		}
		return core.persistCountersBatch(batch, db, namespace, func(c *namespaceCounters) {
			c.ActiveClaims++
			c.IdempotencyRecords++
		})
	}); err != nil {
		return nil, false, err
	}
	core.capacity.chargeClaim(namespace)
	core.capacity.chargeIdempotency(namespace)
	return out, true, nil
}

func (core *Core) validateSettleEntries(entries []SettleEntry) error {
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if err := validateUnitID(entry.UnitID); err != nil {
			return err
		}
		if _, ok := seen[entry.UnitID]; ok {
			return fail(ReasonInvalidRequest, false, 0)
		}
		seen[entry.UnitID] = struct{}{}
		switch entry.Action {
		case SettleComplete:
			if entry.DelayMs != 0 || entry.ExtensionMs != 0 {
				return fail(ReasonInvalidRequest, false, 0)
			}
		case SettleRelease:
			if entry.ExtensionMs != 0 || entry.DelayMs > maxReleaseDelayMs {
				return fail(ReasonInvalidRequest, false, 0)
			}
		case SettleExtend:
			if entry.DelayMs != 0 || entry.ExtensionMs < minExtendMs || entry.ExtensionMs > maxExtendMs {
				return fail(ReasonInvalidRequest, false, 0)
			}
		default:
			return fail(ReasonInvalidRequest, false, 0)
		}
	}
	return nil
}

func (core *Core) applySettle(in SettleInput, now int64, fingerprint []byte) ([]SettledUnit, error) {
	claimRaw, claimAddress, err := core.findClaim(in.NamespaceID, in.ClaimID)
	if err != nil {
		return nil, err
	}
	epoch, claimExp, claimRequest, unitIDs, err := decodeClaimRecord(claimRaw)
	if err != nil {
		return nil, err
	}
	if epoch != in.ClaimEpoch {
		return nil, fail(ReasonClaimStale, false, 0)
	}
	if now >= claimExp {
		return nil, fail(ReasonClaimExpired, false, 0)
	}
	unitSet := make(map[string]struct{}, len(unitIDs))
	for _, unit := range unitIDs {
		unitSet[unit] = struct{}{}
	}
	for _, entry := range in.Entries {
		if _, ok := unitSet[entry.UnitID]; !ok {
			return nil, fail(ReasonInvalidRequest, false, 0)
		}
	}

	out := make([]SettledUnit, 0, len(in.Entries))
	var completedUnits []string
	var completedCharges []uint64

	addrRaw, err := core.store.get(addressKey(in.NamespaceID, claimAddress))
	if err != nil {
		return nil, err
	}
	addrExp, seq, addrEpoch, liveUnits, err := decodeAddressRecord(addrRaw)
	if err != nil {
		return nil, err
	}

	type unitState struct {
		acceptedAt  int64
		unitExp     int64
		availableAt int64
		seq         int64
		attempt     uint32
		payload     []byte
	}
	unitStates := make(map[string]unitState, len(in.Entries))
	needsReplaySnapshot := false
	for _, entry := range in.Entries {
		unitRaw, gErr := core.store.get(unitMetadataKey(in.NamespaceID, claimAddress, entry.UnitID))
		if gErr != nil {
			return nil, fail(ReasonInvalidRequest, false, 0)
		}
		acceptedAt, unitExp, availableAt, unitSeq, attempt, dErr := decodeUnitMetadata(unitRaw)
		if dErr != nil {
			return nil, dErr
		}
		payload, gErr := core.store.get(unitPayloadKey(in.NamespaceID, claimAddress, entry.UnitID))
		if gErr != nil {
			return nil, fail(ReasonCorruptState, false, 0)
		}
		unitStates[entry.UnitID] = unitState{
			acceptedAt: acceptedAt, unitExp: unitExp, availableAt: availableAt,
			seq: unitSeq, attempt: attempt, payload: payload,
		}
		// COMPLETE deletes the payload now. RELEASE transfers ownership to a
		// future Claim that may delete it later, so the original Claim replay
		// must stop depending on the active payload in both cases.
		if entry.Action != SettleExtend {
			needsReplaySnapshot = true
		}
	}

	// remaining tracks claim units that stay under this claim after the
	// settlement. EXTEND keeps units claimed; COMPLETE and RELEASE transfer
	// them out, so a claim is removed only by full settlement (C-09).
	remaining := make([]string, 0, len(unitIDs))
	remainingSet := make(map[string]struct{}, len(unitIDs))
	for _, unit := range unitIDs {
		remainingSet[unit] = struct{}{}
	}
	extendedExpiry := make(map[string]int64, len(in.Entries))
	var maxExtension uint64

	if err := core.store.syncNamespaceBatchAt(in.NamespaceID, now, namespaceLane(in.NamespaceID), func(batch *pebble.Batch, db *pebble.DB) error {
		if needsReplaySnapshot {
			if err := upgradeClaimReplaySnapshotBatch(batch, db, in.NamespaceID, claimRequest); err != nil {
				return err
			}
		}
		for _, entry := range in.Entries {
			state := unitStates[entry.UnitID]
			switch entry.Action {
			case SettleComplete:
				batch.Delete(unitMetadataKey(in.NamespaceID, claimAddress, entry.UnitID), nil)
				batch.Delete(unitPayloadKey(in.NamespaceID, claimAddress, entry.UnitID), nil)
				delete(remainingSet, entry.UnitID)
				completedUnits = append(completedUnits, entry.UnitID)
				completedCharges = append(completedCharges, unitLogicalCharge(len(state.payload)))
				out = append(out, SettledUnit{UnitID: entry.UnitID, Outcome: SettleOutcomeCompleted, EffectiveAtUnixMs: now})
			case SettleRelease:
				delay := releaseDelay(state.attempt, entry.DelayMs)
				availableAt := now + int64(delay)
				if state.attempt >= maxDeliveryAttempts || availableAt >= state.unitExp {
					batch.Delete(unitMetadataKey(in.NamespaceID, claimAddress, entry.UnitID), nil)
					batch.Delete(unitPayloadKey(in.NamespaceID, claimAddress, entry.UnitID), nil)
					delete(remainingSet, entry.UnitID)
					completedUnits = append(completedUnits, entry.UnitID)
					completedCharges = append(completedCharges, unitLogicalCharge(len(state.payload)))
					out = append(out, SettledUnit{UnitID: entry.UnitID, Outcome: SettleOutcomeAttemptLimit, EffectiveAtUnixMs: now})
				} else {
					batch.Set(readyKey(in.NamespaceID, claimAddress, uint64(availableAt), uint64(state.seq), entry.UnitID), []byte{1}, nil)
					batch.Set(unitMetadataKey(in.NamespaceID, claimAddress, entry.UnitID), encodeUnitMetadata(state.acceptedAt, state.unitExp, availableAt, state.seq, state.attempt), nil)
					delete(remainingSet, entry.UnitID)
					out = append(out, SettledUnit{UnitID: entry.UnitID, Outcome: SettleOutcomeReleased, EffectiveAtUnixMs: availableAt})
				}
			case SettleExtend:
				newExp := now + int64(entry.ExtensionMs)
				if newExp > state.unitExp {
					newExp = state.unitExp
				}
				capExp := now + 10*60*1000
				if newExp > capExp {
					newExp = capExp
				}
				batch.Set(unitMetadataKey(in.NamespaceID, claimAddress, entry.UnitID), encodeUnitMetadata(state.acceptedAt, newExp, state.availableAt, state.seq, state.attempt), nil)
				extendedExpiry[entry.UnitID] = newExp
				if entry.ExtensionMs > maxExtension {
					maxExtension = entry.ExtensionMs
				}
				out = append(out, SettledUnit{UnitID: entry.UnitID, Outcome: SettleOutcomeExtended, EffectiveAtUnixMs: newExp})
			}
		}
		if liveUnits >= uint64(len(completedUnits)) {
			liveUnits -= uint64(len(completedUnits))
		} else {
			liveUnits = 0
		}
		batch.Set(addressKey(in.NamespaceID, claimAddress), encodeAddressRecord(addrExp, seq, addrEpoch, liveUnits), nil)
		if len(remainingSet) == 0 {
			batch.Delete(claimKey(in.NamespaceID, claimAddress, in.ClaimID), nil)
			batch.Delete(deadlineKey(claimExp, deadlineKindClaim, in.NamespaceID, claimAddress, in.ClaimID, 1), nil)
		} else {
			newClaimExp := claimExp + int64(maxExtension)
			minUnitExp := newClaimExp
			for _, unit := range unitIDs {
				if _, ok := remainingSet[unit]; !ok {
					continue
				}
				expiry, ok := extendedExpiry[unit]
				if !ok {
					expiry = unitStates[unit].unitExp
				}
				if expiry < minUnitExp {
					minUnitExp = expiry
				}
			}
			if newClaimExp > minUnitExp {
				newClaimExp = minUnitExp
			}
			if newClaimExp > addrExp {
				newClaimExp = addrExp
			}
			for _, unit := range unitIDs {
				if _, ok := remainingSet[unit]; ok {
					remaining = append(remaining, unit)
				}
			}
			batch.Set(claimKey(in.NamespaceID, claimAddress, in.ClaimID), encodeClaimRecord(epoch, newClaimExp, claimRequest, remaining), nil)
			batch.Delete(deadlineKey(claimExp, deadlineKindClaim, in.NamespaceID, claimAddress, in.ClaimID, 1), nil)
			batch.Set(deadlineKey(newClaimExp, deadlineKindClaim, in.NamespaceID, claimAddress, in.ClaimID, 1), nil, nil)
		}
		if err := appendIdempotencyBatch(batch, in.NamespaceID, opNameSettle, in.RequestID, fingerprint, SettleOutput{Units: out}, now); err != nil {
			return err
		}
		return core.persistCountersBatch(batch, db, in.NamespaceID, func(c *namespaceCounters) {
			if len(remainingSet) == 0 && c.ActiveClaims > 0 {
				c.ActiveClaims--
			}
			for _, charge := range completedCharges {
				if c.Units > 0 {
					c.Units--
				}
				if c.RetainedBytes >= charge {
					c.RetainedBytes -= charge
				}
			}
			c.IdempotencyRecords++
		})
	}); err != nil {
		return nil, err
	}
	for _, charge := range completedCharges {
		core.capacity.releaseUnit(in.NamespaceID, charge)
	}
	if len(remainingSet) == 0 {
		core.capacity.releaseClaim(in.NamespaceID)
	}
	core.capacity.chargeIdempotency(in.NamespaceID)
	return out, nil
}

func (core *Core) findClaim(namespace, claimID string) ([]byte, string, error) {
	prefix := append([]byte{keyClaim}, []byte(namespace)...)
	iter, err := core.store.newIter(prefix)
	if err != nil {
		return nil, "", err
	}
	defer iter.Close()
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) < 1+len(namespace)+idTotalLen+idTotalLen {
			continue
		}
		if string(key[len(key)-idTotalLen:]) != claimID {
			continue
		}
		addressStart := 1 + len(namespace)
		address := string(key[addressStart : addressStart+idTotalLen])
		return append([]byte(nil), iter.Value()...), address, nil
	}
	return nil, "", fail(ReasonClaimStale, false, 0)
}

func generateID(prefix string) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return "", err
		}
		id := prefix + base64.RawURLEncoding.EncodeToString(raw[:])
		if len(id) == idTotalLen {
			return id, nil
		}
	}
	return "", fmt.Errorf("id generation failed")
}

func validateNamespace(id string) error {
	if len(id) != idTotalLen || id[:3] != "ns_" {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

func validateRequestID(id string) error {
	if len(id) == 0 || len(id) > 128 {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

func validateAddressID(id string) error {
	if len(id) != idTotalLen || id[:3] != "sa_" {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

func validateUnitID(id string) error {
	if len(id) != idTotalLen || id[:3] != "tu_" {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

func validateClaimID(id string) error {
	if len(id) != idTotalLen || id[:3] != "dc_" {
		return fail(ReasonInvalidRequest, false, 0)
	}
	return nil
}

func normalizeAddressTTL(ttlMs uint64) uint64 {
	if ttlMs == 0 {
		return defaultAddressTTLMs
	}
	if ttlMs < minTTLMs {
		return minTTLMs
	}
	if ttlMs > maxAddressTTLMs {
		return maxAddressTTLMs
	}
	return ttlMs
}

func normalizeUnitTTL(ttlMs uint64) uint64 {
	if ttlMs == 0 {
		return defaultUnitTTLMs
	}
	if ttlMs < minTTLMs {
		return minTTLMs
	}
	if ttlMs > maxUnitTTLMs {
		return maxUnitTTLMs
	}
	return ttlMs
}

func normalizeClaimBounds(in ClaimInput) (maxUnits uint32, maxBytes, visibilityMs, waitMs uint64) {
	maxUnits = in.MaxUnits
	if maxUnits == 0 {
		maxUnits = defaultClaimMaxUnits
	}
	if maxUnits > maxClaimUnits {
		maxUnits = maxClaimUnits
	}
	maxBytes = in.MaxBytes
	if maxBytes == 0 {
		maxBytes = defaultClaimMaxBytes
	}
	if maxBytes > maxClaimBytes {
		maxBytes = maxClaimBytes
	}
	visibilityMs = in.VisibilityTimeoutMs
	if visibilityMs == 0 {
		visibilityMs = defaultVisibilityMs
	}
	if visibilityMs < minVisibilityMs {
		visibilityMs = minVisibilityMs
	}
	if visibilityMs > maxVisibilityMs {
		visibilityMs = maxVisibilityMs
	}
	waitMs = in.WaitTimeoutMs
	if waitMs > maxWaitMs {
		waitMs = maxWaitMs
	}
	return maxUnits, maxBytes, visibilityMs, waitMs
}

func namespaceLane(namespace string) int {
	if len(namespace) < idTotalLen {
		return 0
	}
	raw, err := base64.RawURLEncoding.DecodeString(namespace[3:])
	if err != nil || len(raw) < 8 {
		return 0
	}
	return int(binary.BigEndian.Uint64(raw[:8]) % mutationLanes)
}

func waiterKey(namespace, address string) string {
	return namespace + "\x00" + address
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func releaseDelay(attempt uint32, requested uint64) uint64 {
	if requested > 0 {
		return requested
	}
	switch attempt {
	case 1:
		return 1_000
	case 2:
		return 5_000
	case 3:
		return 30_000
	case 4:
		return 120_000
	default:
		return 300_000
	}
}
