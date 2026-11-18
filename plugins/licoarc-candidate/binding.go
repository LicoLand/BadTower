package licoarccandidate

const (
	ProtocolWireID         = "licoarc.protocol-line.v1"
	ProtocolLineID         = "licoarc.v1"
	TransportProfileID     = "licoarc.https-transport.v1"
	Lifecycle              = "Candidate"
	ProtocolManifestSHA256 = "96688cc20ea8bcd4726fa33ced35a10ea12349e0a6d5abc23fb585197b06c503"
	TransportSourceDigest  = "6d4636c983606fb266443a3ce05171a8af7ab5284e0ce301be7024ce16592cb5"

	MaxPacketBytes           = 524288
	MaxControlBodyBytes      = 16384
	MaxClaimResponseBytes    = 33554432
	MaxClaimItems            = 64
	MaxClaimBytes            = 33554432
	MaxSettlementItems       = 64
	MaxRetryAfterMS          = 60000
	StorageWindowSeconds     = 3600
	IdempotencyWindowSeconds = 300
	ClaimWindowSeconds       = 300
	MaxConcurrentOperations  = 64
	MaxQueuedOperations      = 256
	MaxPluginStateBytes      = 1 << 30
)

type Outcome uint64

const (
	OutcomeAccepted Outcome = iota
	OutcomeRejected
	OutcomeTransient
	OutcomeAmbiguous
)

type FailureClass uint64

const (
	FailureInvalidRequest FailureClass = iota
	FailureUnsupportedProfile
	FailureTargetUnavailable
	FailurePacketTooLarge
	FailureCapacityUnavailable
	FailureRateLimited
	FailureConflict
	FailureInternalUnavailable
)

type HandleClass uint64

const (
	HandleAsync HandleClass = iota
	HandleFirstContact
)

type SettleAction uint64

const (
	SettleComplete SettleAction = iota
	SettleRelease
)

type SettleOutcome uint64

const (
	SettledCompleted SettleOutcome = iota
	SettledReleased
	SettledStaleClaim
	SettledAttemptLimit
)
