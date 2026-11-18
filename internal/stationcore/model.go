package stationcore

import "time"

const (
	StoreIdentity              = "badtower.station-core.store.v2"
	ProfileSingleNodeGeneralV1 = "single-node-general-v1"

	idPrefixRequest = "rq_"
	idPrefixAddress = "sa_"
	idPrefixUnit    = "tu_"
	idPrefixClaim   = "dc_"

	idRandomChars = 22
	idTotalLen    = 3 + idRandomChars

	minPayloadBytes = 1
	maxPayloadBytes = 1 << 20

	minTTLMs            = 60_000
	maxAddressTTLMs     = 30 * 24 * 60 * 60 * 1000
	defaultAddressTTLMs = 24 * 60 * 60 * 1000
	maxUnitTTLMs        = 7 * 24 * 60 * 60 * 1000
	defaultUnitTTLMs    = 24 * 60 * 60 * 1000

	defaultClaimMaxUnits = 32
	defaultClaimMaxBytes = 1 << 20
	defaultVisibilityMs  = 30_000
	maxClaimUnits        = 128
	maxClaimBytes        = 4 << 20
	maxVisibilityMs      = 10 * 60 * 1000
	minVisibilityMs      = 1_000
	maxWaitMs            = 20_000

	maxSettleEntries  = 128
	maxReleaseDelayMs = 300_000
	minExtendMs       = 1_000
	maxExtendMs       = 600_000

	maxDeliveryAttempts    = 16
	idempotencyRetentionMs = 60 * 60 * 1000

	clockBackwardClampMs = 60_000
)

type Config struct {
	DataRoot    string
	ProfileID   string
	StorageMode StorageMode
	Partition   uint16
	StoragePool *StoragePool
}

// StorageMode fixes the complete physical store topology at root creation.
// The zero value is the standalone fixed-shard topology. Cluster members use
// one PartitionOne Core per logical partition and share one StoragePool.
type StorageMode string

const (
	StorageModeInstanceSharded StorageMode = "INSTANCE_SHARDED"
	StorageModePartitionOne    StorageMode = "PARTITION_ONE"
)

type Clock interface {
	NowUnixMs() int64
	MonotonicNow() time.Duration
}

type ReserveInput struct {
	NamespaceID string
	RequestID   string
	AddressID   string
	TTLMs       uint64
}

type ReserveOutput struct {
	AddressID       string
	ExpiresAtUnixMs int64
	Created         bool
}

type SubmitInput struct {
	NamespaceID string
	RequestID   string
	AddressID   string
	Payload     []byte
	TTLMs       uint64
}

type SubmitOutput struct {
	UnitID           string
	AcceptedAtUnixMs int64
	ExpiresAtUnixMs  int64
}

type ClaimInput struct {
	NamespaceID         string
	RequestID           string
	AddressID           string
	MaxUnits            uint32
	MaxBytes            uint64
	VisibilityTimeoutMs uint64
	WaitTimeoutMs       uint64
}

type ClaimedUnit struct {
	UnitID           string
	Payload          []byte
	AcceptedAtUnixMs int64
	ExpiresAtUnixMs  int64
	DeliveryAttempt  uint32
}

type ClaimOutput struct {
	ClaimID              string
	ClaimEpoch           uint64
	ClaimExpiresAtUnixMs int64
	Units                []ClaimedUnit
}

type SettleAction int

const (
	SettleComplete SettleAction = iota + 1
	SettleRelease
	SettleExtend
)

type SettleEntry struct {
	UnitID      string
	Action      SettleAction
	DelayMs     uint64
	ExtensionMs uint64
}

type SettleInput struct {
	NamespaceID string
	RequestID   string
	ClaimID     string
	ClaimEpoch  uint64
	Entries     []SettleEntry
}

type SettledUnit struct {
	UnitID            string
	Outcome           string
	EffectiveAtUnixMs int64
}

type SettleOutput struct {
	Units []SettledUnit
}

const (
	SettleOutcomeCompleted    = "COMPLETED"
	SettleOutcomeReleased     = "RELEASED"
	SettleOutcomeExtended     = "EXTENDED"
	SettleOutcomeAttemptLimit = "ATTEMPT_LIMIT"
)

type Pressure int

const (
	PressureNormal Pressure = iota + 1
	PressureSoft
	PressureHard
)

type ResourceVector struct {
	Addresses          uint64
	Units              uint64
	RetainedBytes      uint64
	ActiveClaims       uint64
	IdempotencyRecords uint64
}

// StationAggregate is the derived station-wide capacity aggregate: the sum of
// every namespace counter. It is maintained in O(1) under the capacity mutex
// at every charge/release transition and rebuilt from the canonical durable
// namespace counters on open. It is never persisted; the per-namespace
// counters remain the only durable truth.
type StationAggregate struct {
	Addresses          uint64
	Units              uint64
	RetainedBytes      uint64
	ActiveClaims       uint64
	IdempotencyRecords uint64
}

// StationLifecycle is a privacy-minimal station lifecycle status.
type StationLifecycle string

const (
	StationLifecycleStarting StationLifecycle = "STARTING"
	StationLifecycleRunning  StationLifecycle = "RUNNING"
	StationLifecycleDraining StationLifecycle = "DRAINING"
	StationLifecycleStopped  StationLifecycle = "STOPPED"
)

// StationObservation is an identifier-free, privacy-minimal station-wide
// observation. It carries no Namespace, address, unit, claim, request, path,
// or raw runtime detail, and reading it performs no store I/O and no
// maintenance.
type StationObservation struct {
	Lifecycle StationLifecycle
	// Pressure is the station-wide pressure, the O(1) max of the global
	// station signal and every per-namespace pressure. It carries no
	// namespace identity.
	Pressure  Pressure
	Aggregate StationAggregate
}

type ProfileLimits struct {
	MaxNamespaces                       uint64
	MaxAddressesPerNamespace            uint64
	MaxUnitsPerNamespace                uint64
	MaxRetainedBytesPerNamespace        uint64
	MaxActiveClaimsPerNamespace         uint64
	MaxMutationQueueEntriesPerNamespace uint64
	MaxLongPollClaimsPerNamespace       uint64
	MutationLanes                       int
	DRRQuantumBytes                     uint64
	DRRChargeGranularity                uint64
	ReservedControlQueueBytes           uint64
}

type GetCapacityInput struct {
	NamespaceID string
}

type GetCapacityOutput struct {
	ProfileID      string
	Pressure       Pressure
	NamespaceUsage ResourceVector
	NamespaceLimit ResourceVector
	RetryAfterMs   uint32
}

type FingerprintField struct {
	Number uint16
	Value  []byte
}
