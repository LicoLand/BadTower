package licoarccandidate

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

var ErrInvalidControl = errors.New("invalid LicoArc Candidate control body")

type AffiliateRequest struct {
	StationDescriptorDigest [32]byte
	AffiliationCommitment   [32]byte
}

type ReserveRequest struct {
	StationDescriptorDigest [32]byte
	AffiliationCommitment   [32]byte
	TransportProfileDigest  [32]byte
	HandleClass             HandleClass
}

type RetrieveRequest struct {
	ClaimID    [32]byte
	ClaimEpoch uint64
	ItemID     [32]byte
}

type ClaimRequest struct{ MaxItems uint64 }

type Settlement struct {
	ItemID [32]byte
	Action SettleAction
}

type SettleRequest struct {
	ClaimID     [32]byte
	ClaimEpoch  uint64
	Settlements []Settlement
}

type ClaimedItem struct {
	ItemID          [32]byte
	ProtectedPacket []byte
}

type SettlementResult struct {
	ItemID  [32]byte
	Outcome SettleOutcome
}

var (
	canonicalEncoder cbor.EncMode
	strictDecoder    cbor.DecMode
)

func init() {
	var err error
	canonicalEncoder, err = cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	strictDecoder, err = (cbor.DecOptions{
		DupMapKey:        cbor.DupMapKeyEnforcedAPF,
		IndefLength:      cbor.IndefLengthForbidden,
		TagsMd:           cbor.TagsForbidden,
		MaxNestedLevels:  8,
		MaxArrayElements: MaxSettlementItems,
		MaxMapPairs:      16,
		IntDec:           cbor.IntDecConvertNone,
	}).DecMode()
	if err != nil {
		panic(err)
	}
}

func DecodeAffiliate(raw []byte) (AffiliateRequest, error) {
	values, err := decodeClosedMap(raw, 0, 1)
	if err != nil {
		return AffiliateRequest{}, err
	}
	return AffiliateRequest{StationDescriptorDigest: digestValue(values[0]), AffiliationCommitment: digestValue(values[1])}, validateDigests(values[0], values[1])
}

func DecodeReserve(raw []byte) (ReserveRequest, error) {
	values, err := decodeClosedMap(raw, 0, 1, 2, 3)
	if err != nil {
		return ReserveRequest{}, err
	}
	if err := validateDigests(values[0], values[1], values[2]); err != nil {
		return ReserveRequest{}, err
	}
	handleClass, ok := uintValue(values[3])
	if !ok || handleClass > uint64(HandleFirstContact) {
		return ReserveRequest{}, ErrInvalidControl
	}
	return ReserveRequest{
		StationDescriptorDigest: digestValue(values[0]), AffiliationCommitment: digestValue(values[1]),
		TransportProfileDigest: digestValue(values[2]), HandleClass: HandleClass(handleClass),
	}, nil
}

func DecodeRetrieve(raw []byte) (RetrieveRequest, error) {
	values, err := decodeClosedMap(raw, 0, 1, 2)
	if err != nil {
		return RetrieveRequest{}, err
	}
	if err := validateDigests(values[0], values[2]); err != nil {
		return RetrieveRequest{}, err
	}
	epoch, ok := uintValue(values[1])
	if !ok {
		return RetrieveRequest{}, ErrInvalidControl
	}
	return RetrieveRequest{ClaimID: digestValue(values[0]), ClaimEpoch: epoch, ItemID: digestValue(values[2])}, nil
}

func DecodeClaim(raw []byte) (ClaimRequest, error) {
	values, err := decodeClosedMap(raw, 0)
	if err != nil {
		return ClaimRequest{}, err
	}
	maxItems, ok := uintValue(values[0])
	if !ok || maxItems == 0 || maxItems > MaxClaimItems {
		return ClaimRequest{}, ErrInvalidControl
	}
	return ClaimRequest{MaxItems: maxItems}, nil
}

func DecodeSettle(raw []byte) (SettleRequest, error) {
	values, err := decodeClosedMap(raw, 0, 1, 2)
	if err != nil {
		return SettleRequest{}, err
	}
	if err := validateDigests(values[0]); err != nil {
		return SettleRequest{}, err
	}
	epoch, ok := uintValue(values[1])
	if !ok {
		return SettleRequest{}, ErrInvalidControl
	}
	items, ok := values[2].([]any)
	if !ok || len(items) == 0 || len(items) > MaxSettlementItems {
		return SettleRequest{}, ErrInvalidControl
	}
	request := SettleRequest{ClaimID: digestValue(values[0]), ClaimEpoch: epoch, Settlements: make([]Settlement, len(items))}
	seen := make(map[[32]byte]struct{}, len(items))
	for index, item := range items {
		mapping, ok := item.(map[any]any)
		if !ok || len(mapping) != 2 {
			return SettleRequest{}, ErrInvalidControl
		}
		itemValues, err := normalizeMap(mapping, 0, 1)
		if err != nil || validateDigests(itemValues[0]) != nil {
			return SettleRequest{}, ErrInvalidControl
		}
		action, ok := uintValue(itemValues[1])
		if !ok || action > uint64(SettleRelease) {
			return SettleRequest{}, ErrInvalidControl
		}
		itemID := digestValue(itemValues[0])
		if _, duplicate := seen[itemID]; duplicate {
			return SettleRequest{}, ErrInvalidControl
		}
		seen[itemID] = struct{}{}
		request.Settlements[index] = Settlement{ItemID: itemID, Action: SettleAction(action)}
	}
	return request, nil
}

func EncodeAffiliateResponse(outcome Outcome, notAfter uint64, signature []byte, failure FailureClass, retryAfter uint64) ([]byte, error) {
	values := map[uint64]any{0: uint64(outcome)}
	if outcome == OutcomeAccepted {
		if notAfter == 0 || len(signature) != 64 {
			return nil, ErrInvalidControl
		}
		values[1], values[2] = notAfter, append([]byte(nil), signature...)
	} else if err := addFailure(values, outcome, 3, 4, failure, retryAfter); err != nil {
		return nil, err
	}
	return encodeMap(values)
}

func EncodeReserveResponse(outcome Outcome, handle [32]byte, class HandleClass, serviceUntil uint64, signature []byte, failure FailureClass, retryAfter uint64) ([]byte, error) {
	values := map[uint64]any{0: uint64(outcome)}
	if outcome == OutcomeAccepted {
		values[1], values[2] = handle[:], uint64(class)
		if class == HandleAsync {
			if serviceUntil == 0 || len(signature) != 64 {
				return nil, ErrInvalidControl
			}
			values[3], values[4] = serviceUntil, append([]byte(nil), signature...)
		} else if class != HandleFirstContact {
			return nil, ErrInvalidControl
		}
	} else if err := addFailure(values, outcome, 5, 6, failure, retryAfter); err != nil {
		return nil, err
	}
	return encodeMap(values)
}

func EncodeSubmitResponse(outcome Outcome, failure FailureClass, retryAfter uint64) ([]byte, error) {
	values := map[uint64]any{0: uint64(outcome)}
	if outcome != OutcomeAccepted {
		if err := addFailure(values, outcome, 1, 2, failure, retryAfter); err != nil {
			return nil, err
		}
	}
	return encodeMap(values)
}

func EncodeClaimResponse(outcome Outcome, claimID [32]byte, epoch uint64, items []ClaimedItem, failure FailureClass, retryAfter uint64) ([]byte, error) {
	values := map[uint64]any{0: uint64(outcome)}
	if outcome == OutcomeAccepted && len(items) > 0 {
		if len(items) > MaxClaimItems {
			return nil, ErrInvalidControl
		}
		encodedItems := make([]any, len(items))
		total := 0
		for index, item := range items {
			if len(item.ProtectedPacket) == 0 || len(item.ProtectedPacket) > MaxPacketBytes {
				return nil, ErrInvalidControl
			}
			total += len(item.ProtectedPacket)
			encodedItems[index] = map[uint64]any{0: item.ItemID[:], 1: item.ProtectedPacket}
		}
		if total > MaxClaimBytes {
			return nil, ErrInvalidControl
		}
		values[1], values[2], values[3] = claimID[:], epoch, encodedItems
	} else if outcome != OutcomeAccepted {
		if err := addFailure(values, outcome, 4, 5, failure, retryAfter); err != nil {
			return nil, err
		}
	}
	return encodeMapBounded(values, MaxClaimResponseBytes)
}

func EncodeSettleResponse(outcome Outcome, results []SettlementResult, failure FailureClass, retryAfter uint64) ([]byte, error) {
	values := map[uint64]any{0: uint64(outcome)}
	if outcome == OutcomeAccepted {
		if len(results) == 0 || len(results) > MaxSettlementItems {
			return nil, ErrInvalidControl
		}
		items := make([]any, len(results))
		for index, result := range results {
			if result.Outcome > SettledAttemptLimit {
				return nil, ErrInvalidControl
			}
			items[index] = map[uint64]any{0: result.ItemID[:], 1: uint64(result.Outcome)}
		}
		values[1] = items
	} else if err := addFailure(values, outcome, 2, 3, failure, retryAfter); err != nil {
		return nil, err
	}
	return encodeMap(values)
}

func decodeClosedMap(raw []byte, labels ...uint64) (map[uint64]any, error) {
	if len(raw) == 0 || len(raw) > MaxControlBodyBytes {
		return nil, ErrInvalidControl
	}
	var decoded map[any]any
	if err := strictDecoder.Unmarshal(raw, &decoded); err != nil {
		return nil, ErrInvalidControl
	}
	reencoded, err := canonicalEncoder.Marshal(decoded)
	if err != nil || !bytes.Equal(raw, reencoded) {
		return nil, ErrInvalidControl
	}
	return normalizeMap(decoded, labels...)
}

func normalizeMap(decoded map[any]any, labels ...uint64) (map[uint64]any, error) {
	if len(decoded) != len(labels) {
		return nil, ErrInvalidControl
	}
	values := make(map[uint64]any, len(labels))
	for _, label := range labels {
		value, ok := decoded[label]
		if !ok {
			return nil, ErrInvalidControl
		}
		values[label] = value
	}
	return values, nil
}

func validateDigests(values ...any) error {
	for _, value := range values {
		raw, ok := value.([]byte)
		if !ok || len(raw) != 32 {
			return ErrInvalidControl
		}
	}
	return nil
}

func digestValue(value any) [32]byte {
	var result [32]byte
	if raw, ok := value.([]byte); ok {
		copy(result[:], raw)
	}
	return result
}

func uintValue(value any) (uint64, bool) {
	switch typed := value.(type) {
	case uint64:
		return typed, true
	case uint32:
		return uint64(typed), true
	case uint16:
		return uint64(typed), true
	case uint8:
		return uint64(typed), true
	default:
		return 0, false
	}
}

func addFailure(values map[uint64]any, outcome Outcome, failureLabel, retryLabel uint64, failure FailureClass, retryAfter uint64) error {
	if outcome == OutcomeAccepted || outcome > OutcomeAmbiguous || failure > FailureInternalUnavailable {
		return ErrInvalidControl
	}
	values[failureLabel] = uint64(failure)
	if outcome == OutcomeTransient {
		if retryAfter > MaxRetryAfterMS {
			return ErrInvalidControl
		}
		values[retryLabel] = retryAfter
	} else if retryAfter != 0 {
		return ErrInvalidControl
	}
	return nil
}

func encodeMap(values map[uint64]any) ([]byte, error) {
	return encodeMapBounded(values, MaxControlBodyBytes)
}

func encodeMapBounded(values map[uint64]any, maximum int) ([]byte, error) {
	raw, err := canonicalEncoder.Marshal(values)
	if err != nil || len(raw) > maximum {
		return nil, fmt.Errorf("%w", ErrInvalidControl)
	}
	return raw, nil
}
