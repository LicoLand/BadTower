package stationcore

import (
	"encoding/binary"
	"encoding/json"

	"github.com/cockroachdb/pebble/v2"
)

var (
	claimReplayReferenceMagic = [4]byte{'C', 'R', 'R', 1}
	claimReplaySnapshotMagic  = [4]byte{'C', 'R', 'S', 1}
)

type claimReplayUnit struct {
	unitID     string
	acceptedAt int64
	expiresAt  int64
	attempt    uint32
	payload    []byte
}

type claimReplay struct {
	address  string
	claimID  string
	epoch    uint64
	deadline int64
	units    []claimReplayUnit
	snapshot bool
}

func newClaimReplayReference(address string, output *ClaimOutput) claimReplay {
	replay := claimReplay{
		address:  address,
		claimID:  output.ClaimID,
		epoch:    output.ClaimEpoch,
		deadline: output.ClaimExpiresAtUnixMs,
		units:    make([]claimReplayUnit, len(output.Units)),
	}
	for index, unit := range output.Units {
		replay.units[index] = claimReplayUnit{
			unitID: unit.UnitID, acceptedAt: unit.AcceptedAtUnixMs,
			expiresAt: unit.ExpiresAtUnixMs, attempt: unit.DeliveryAttempt,
		}
	}
	return replay
}

func encodeClaimReplay(replay claimReplay) ([]byte, error) {
	if len(replay.units) > maxClaimUnits {
		return nil, fail(ReasonInternal, false, 0)
	}
	size := 4 + 2*idTotalLen + 8 + 8 + 4
	for _, unit := range replay.units {
		size += idTotalLen + 8 + 8 + 4
		if replay.snapshot {
			size += 4 + len(unit.payload)
		}
	}
	if size > int(maxClaimBytes)+64*1024 {
		return nil, fail(ReasonInternal, false, 0)
	}
	encoded := make([]byte, size)
	magic := claimReplayReferenceMagic
	if replay.snapshot {
		magic = claimReplaySnapshotMagic
	}
	copy(encoded, magic[:])
	offset := 4
	offset += copy(encoded[offset:], replay.address)
	offset += copy(encoded[offset:], replay.claimID)
	binary.BigEndian.PutUint64(encoded[offset:], replay.epoch)
	offset += 8
	binary.BigEndian.PutUint64(encoded[offset:], uint64(replay.deadline))
	offset += 8
	binary.BigEndian.PutUint32(encoded[offset:], uint32(len(replay.units)))
	offset += 4
	for _, unit := range replay.units {
		offset += copy(encoded[offset:], unit.unitID)
		binary.BigEndian.PutUint64(encoded[offset:], uint64(unit.acceptedAt))
		offset += 8
		binary.BigEndian.PutUint64(encoded[offset:], uint64(unit.expiresAt))
		offset += 8
		binary.BigEndian.PutUint32(encoded[offset:], unit.attempt)
		offset += 4
		if replay.snapshot {
			binary.BigEndian.PutUint32(encoded[offset:], uint32(len(unit.payload)))
			offset += 4
			offset += copy(encoded[offset:], unit.payload)
		}
	}
	return encoded, nil
}

func decodeClaimReplay(encoded []byte) (claimReplay, bool, error) {
	const header = 4 + 2*idTotalLen + 8 + 8 + 4
	if len(encoded) < 4 {
		return claimReplay{}, false, nil
	}
	snapshot := false
	switch {
	case bytesEqual(encoded[:4], claimReplayReferenceMagic[:]):
	case bytesEqual(encoded[:4], claimReplaySnapshotMagic[:]):
		snapshot = true
	default:
		return claimReplay{}, false, nil
	}
	if len(encoded) < header {
		return claimReplay{}, true, fail(ReasonCorruptState, false, 0)
	}
	offset := 4
	replay := claimReplay{snapshot: snapshot}
	replay.address = string(encoded[offset : offset+idTotalLen])
	offset += idTotalLen
	replay.claimID = string(encoded[offset : offset+idTotalLen])
	offset += idTotalLen
	replay.epoch = binary.BigEndian.Uint64(encoded[offset:])
	offset += 8
	replay.deadline = int64(binary.BigEndian.Uint64(encoded[offset:]))
	offset += 8
	count := int(binary.BigEndian.Uint32(encoded[offset:]))
	offset += 4
	if count < 0 || count > maxClaimUnits {
		return claimReplay{}, true, fail(ReasonCorruptState, false, 0)
	}
	replay.units = make([]claimReplayUnit, count)
	var payloadBytes int
	for index := range replay.units {
		if offset+idTotalLen+20 > len(encoded) {
			return claimReplay{}, true, fail(ReasonCorruptState, false, 0)
		}
		unit := &replay.units[index]
		unit.unitID = string(encoded[offset : offset+idTotalLen])
		offset += idTotalLen
		unit.acceptedAt = int64(binary.BigEndian.Uint64(encoded[offset:]))
		offset += 8
		unit.expiresAt = int64(binary.BigEndian.Uint64(encoded[offset:]))
		offset += 8
		unit.attempt = binary.BigEndian.Uint32(encoded[offset:])
		offset += 4
		if snapshot {
			if offset+4 > len(encoded) {
				return claimReplay{}, true, fail(ReasonCorruptState, false, 0)
			}
			length := int(binary.BigEndian.Uint32(encoded[offset:]))
			offset += 4
			if length < 0 || offset+length > len(encoded) || payloadBytes > int(maxClaimBytes)-length {
				return claimReplay{}, true, fail(ReasonCorruptState, false, 0)
			}
			unit.payload = encoded[offset : offset+length]
			offset += length
			payloadBytes += length
		}
	}
	if offset != len(encoded) {
		return claimReplay{}, true, fail(ReasonCorruptState, false, 0)
	}
	return replay, true, nil
}

func (core *Core) materializeClaimReplay(namespace string, replay claimReplay) (ClaimOutput, error) {
	out := ClaimOutput{
		ClaimID: replay.claimID, ClaimEpoch: replay.epoch,
		ClaimExpiresAtUnixMs: replay.deadline,
		Units:                make([]ClaimedUnit, len(replay.units)),
	}
	for index, unit := range replay.units {
		payload := unit.payload
		if !replay.snapshot {
			var err error
			payload, err = core.store.get(unitPayloadKey(namespace, replay.address, unit.unitID))
			if err != nil {
				return ClaimOutput{}, fail(ReasonCorruptState, false, 0)
			}
		}
		out.Units[index] = ClaimedUnit{
			UnitID: unit.unitID, Payload: payload,
			AcceptedAtUnixMs: unit.acceptedAt, ExpiresAtUnixMs: unit.expiresAt,
			DeliveryAttempt: unit.attempt,
		}
	}
	return out, nil
}

func (core *Core) checkClaimIdempotencyAt(namespace, request string, fingerprint []byte, now int64) (*ClaimOutput, error) {
	raw, err := core.store.get(idempotencyKey(namespace, opNameClaim, request))
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	storedFingerprint, response, expiresAt, err := decodeIdempotencyRecord(raw)
	if err != nil {
		return nil, err
	}
	if now >= expiresAt {
		return nil, nil
	}
	if !bytesEqual(storedFingerprint, fingerprint) {
		return nil, fail(ReasonIdempotencyConflict, false, 0)
	}
	replay, recognized, err := decodeClaimReplay(response)
	if err != nil {
		return nil, err
	}
	if recognized {
		out, err := core.materializeClaimReplay(namespace, replay)
		return &out, err
	}
	var out ClaimOutput
	if err := json.Unmarshal(response, &out); err != nil {
		return nil, fail(ReasonCorruptState, false, 0)
	}
	return &out, nil
}

func appendClaimIdempotencyBatch(batch *pebble.Batch, namespace, request string, fingerprint []byte, replay claimReplay, now int64) error {
	response, err := encodeClaimReplay(replay)
	if err != nil {
		return err
	}
	expiresAt := now + idempotencyRetentionMs
	batch.Set(idempotencyKey(namespace, opNameClaim, request), encodeIdempotencyRecord(fingerprint, response, expiresAt), nil)
	batch.Set(deadlineKey(expiresAt, deadlineKindIdempotency, namespace, "", opNameClaim+"\x00"+request, 1), nil, nil)
	return nil
}

func upgradeClaimReplaySnapshotBatch(batch *pebble.Batch, db *pebble.DB, namespace, request string) error {
	raw, closer, err := db.Get(idempotencyKey(namespace, opNameClaim, request))
	if err != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	owned := append([]byte(nil), raw...)
	closer.Close()
	fingerprint, response, expiresAt, err := decodeIdempotencyRecord(owned)
	if err != nil {
		return err
	}
	replay, recognized, err := decodeClaimReplay(response)
	if err != nil {
		return err
	}
	if !recognized {
		return fail(ReasonCorruptState, false, 0)
	}
	if replay.snapshot {
		return nil
	}
	for index := range replay.units {
		payload, payloadCloser, err := db.Get(unitPayloadKey(namespace, replay.address, replay.units[index].unitID))
		if err != nil {
			return fail(ReasonCorruptState, false, 0)
		}
		replay.units[index].payload = append([]byte(nil), payload...)
		payloadCloser.Close()
	}
	replay.snapshot = true
	snapshot, err := encodeClaimReplay(replay)
	if err != nil {
		return err
	}
	return batch.Set(idempotencyKey(namespace, opNameClaim, request), encodeIdempotencyRecord(fingerprint, snapshot, expiresAt), nil)
}
