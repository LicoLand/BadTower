package stationcore

import "errors"

type Reason string

const (
	ReasonInvalidRequest      Reason = "INVALID_REQUEST"
	ReasonDeadlineRequired    Reason = "DEADLINE_REQUIRED"
	ReasonUnauthenticated     Reason = "UNAUTHENTICATED"
	ReasonPermissionDenied    Reason = "PERMISSION_DENIED"
	ReasonVersionUnsupported  Reason = "VERSION_UNSUPPORTED"
	ReasonAddressNotFound     Reason = "ADDRESS_NOT_FOUND"
	ReasonAddressExpired      Reason = "ADDRESS_EXPIRED"
	ReasonIdempotencyConflict Reason = "IDEMPOTENCY_CONFLICT"
	ReasonClaimStale          Reason = "CLAIM_STALE"
	ReasonClaimExpired        Reason = "CLAIM_EXPIRED"
	ReasonCapacityExceeded    Reason = "CAPACITY_EXCEEDED"
	ReasonRateLimited         Reason = "RATE_LIMITED"
	ReasonDraining            Reason = "DRAINING"
	ReasonStorageUnavailable  Reason = "STORAGE_UNAVAILABLE"
	ReasonClockUnsafe         Reason = "CLOCK_UNSAFE"
	ReasonCorruptState        Reason = "CORRUPT_STATE"
	ReasonDeadlineExceeded    Reason = "DEADLINE_EXCEEDED"
	ReasonInternal            Reason = "INTERNAL"
)

type Failure struct {
	Reason       Reason
	Retryable    bool
	RetryAfterMs uint32
}

func (failure *Failure) Error() string { return string(failure.Reason) }

func fail(reason Reason, retryable bool, retryAfterMs uint32) error {
	return &Failure{Reason: reason, Retryable: retryable, RetryAfterMs: retryAfterMs}
}

func IsFailure(err error) (*Failure, bool) {
	var failure *Failure
	if errors.As(err, &failure) {
		return failure, true
	}
	return nil, false
}
