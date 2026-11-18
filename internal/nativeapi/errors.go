package nativeapi

import (
	"errors"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	"github.com/LicoLand/BadTower/internal/stationcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stationFailure is the API-layer failure detail (C-11).
type stationFailure struct {
	Reason       string
	Retryable    bool
	RetryAfterMs uint32
}

func (failure stationFailure) Error() string { return failure.Reason }

func mapError(err error) error {
	if err == nil {
		return nil
	}
	var apiFailure stationFailure
	if errors.As(err, &apiFailure) {
		return grpcFailure(apiFailure.Reason, apiFailure.Retryable, apiFailure.RetryAfterMs)
	}
	if failure, ok := stationcore.IsFailure(err); ok {
		return grpcFailure(string(failure.Reason), failure.Retryable, failure.RetryAfterMs)
	}
	return grpcFailure("INTERNAL", false, 0)
}

func grpcFailure(reason string, retryable bool, retryAfterMs uint32) error {
	code := codes.Internal
	switch reason {
	case "INVALID_REQUEST", "DEADLINE_REQUIRED":
		code = codes.InvalidArgument
	case "UNAUTHENTICATED":
		code = codes.Unauthenticated
	case "PERMISSION_DENIED":
		code = codes.PermissionDenied
	case "VERSION_UNSUPPORTED":
		code = codes.Unimplemented
	case "ADDRESS_NOT_FOUND":
		code = codes.NotFound
	case "ADDRESS_EXPIRED", "CLAIM_EXPIRED":
		code = codes.FailedPrecondition
	case "IDEMPOTENCY_CONFLICT":
		code = codes.AlreadyExists
	case "CLAIM_STALE":
		code = codes.Aborted
	case "CAPACITY_EXCEEDED", "RATE_LIMITED", "DRAINING", "STORAGE_UNAVAILABLE", "CLOCK_UNSAFE":
		code = codes.Unavailable
	case "CORRUPT_STATE":
		code = codes.DataLoss
	case "DEADLINE_EXCEEDED":
		code = codes.DeadlineExceeded
	case "INTERNAL":
		code = codes.Internal
	}
	st := status.New(code, reason)
	detail := &stationv1.StationFailure{
		Reason:       reason,
		Retryable:    retryable,
		RetryAfterMs: retryAfterMs,
	}
	withDetails, err := st.WithDetails(detail)
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

func requireDeadline(hasDeadline bool) error {
	if !hasDeadline {
		return stationFailure{Reason: "DEADLINE_REQUIRED"}
	}
	return nil
}

func requireNamespace(server *Server, namespace string) error {
	if namespace == "" {
		return stationFailure{Reason: "UNAUTHENTICATED"}
	}
	if server.auth != nil && !server.auth.snapshot.KnownNamespace(namespace) {
		return stationFailure{Reason: "PERMISSION_DENIED"}
	}
	return nil
}

func mapSettleOutcome(outcome string) stationv1.SettleOutcome {
	switch outcome {
	case stationcore.SettleOutcomeCompleted:
		return stationv1.SettleOutcome_SETTLE_OUTCOME_COMPLETED
	case stationcore.SettleOutcomeReleased:
		return stationv1.SettleOutcome_SETTLE_OUTCOME_RELEASED
	case stationcore.SettleOutcomeExtended:
		return stationv1.SettleOutcome_SETTLE_OUTCOME_EXTENDED
	case stationcore.SettleOutcomeAttemptLimit:
		return stationv1.SettleOutcome_SETTLE_OUTCOME_ATTEMPT_LIMIT
	default:
		return stationv1.SettleOutcome_SETTLE_OUTCOME_UNSPECIFIED
	}
}

func mapPressure(pressure stationcore.Pressure) stationv1.Pressure {
	switch pressure {
	case stationcore.PressureSoft:
		return stationv1.Pressure_PRESSURE_SOFT
	case stationcore.PressureHard:
		return stationv1.Pressure_PRESSURE_HARD
	default:
		return stationv1.Pressure_PRESSURE_NORMAL
	}
}

func mapResourceVector(vector stationcore.ResourceVector) *stationv1.ResourceVector {
	return &stationv1.ResourceVector{
		Addresses:          vector.Addresses,
		Units:              vector.Units,
		RetainedBytes:      vector.RetainedBytes,
		ActiveClaims:       vector.ActiveClaims,
		IdempotencyRecords: vector.IdempotencyRecords,
	}
}
