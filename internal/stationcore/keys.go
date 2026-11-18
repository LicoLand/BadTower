package stationcore

import (
	"encoding/binary"
)

const (
	keyMeta         byte = 0x00
	keyAddress      byte = 0x10
	keyUnitMetadata byte = 0x20
	keyUnitPayload  byte = 0x21
	keyReady        byte = 0x30
	keyDeadline     byte = 0x31
	keyClaim        byte = 0x40
	keyIdempotency  byte = 0x50
	keySettlement   byte = 0x60
	keyNamespaceCtr byte = 0x70
)

const (
	metaIdentity        = "identity"
	metaSchema          = "schema"
	metaFormat          = "format"
	metaWatermarkPrefix = "watermark/"
)

const (
	deadlineKindAddress     byte = 1
	deadlineKindClaim       byte = 2
	deadlineKindUnit        byte = 3
	deadlineKindIdempotency byte = 4
	deadlineKindSettlement  byte = 5
)

func metaKey(name string) []byte {
	key := make([]byte, 1+len(name))
	key[0] = keyMeta
	copy(key[1:], name)
	return key
}

func watermarkKey(slot int) []byte {
	key := make([]byte, 1+len(metaWatermarkPrefix)+1)
	key[0] = keyMeta
	copy(key[1:], metaWatermarkPrefix)
	key[len(key)-1] = byte(slot)
	return key
}

func watermarkPrefix() []byte {
	return metaKey(metaWatermarkPrefix)
}

func encodeID(id string) []byte {
	return []byte(id)
}

func addressKey(namespace, address string) []byte {
	key := make([]byte, 1+len(namespace)+len(address))
	key[0] = keyAddress
	off := 1
	off += copy(key[off:], encodeID(namespace))
	copy(key[off:], encodeID(address))
	return key
}

func unitMetadataKey(namespace, address, unit string) []byte {
	key := make([]byte, 1+len(namespace)+len(address)+len(unit))
	key[0] = keyUnitMetadata
	off := 1
	off += copy(key[off:], encodeID(namespace))
	off += copy(key[off:], encodeID(address))
	copy(key[off:], encodeID(unit))
	return key
}

func unitPayloadKey(namespace, address, unit string) []byte {
	key := make([]byte, 1+len(namespace)+len(address)+len(unit))
	key[0] = keyUnitPayload
	off := 1
	off += copy(key[off:], encodeID(namespace))
	off += copy(key[off:], encodeID(address))
	copy(key[off:], encodeID(unit))
	return key
}

func readyKey(namespace, address string, availableAt, seq uint64, unit string) []byte {
	key := make([]byte, 1+len(namespace)+len(address)+8+8+len(unit))
	key[0] = keyReady
	off := 1
	off += copy(key[off:], encodeID(namespace))
	off += copy(key[off:], encodeID(address))
	binary.BigEndian.PutUint64(key[off:], availableAt)
	off += 8
	binary.BigEndian.PutUint64(key[off:], seq)
	off += 8
	copy(key[off:], encodeID(unit))
	return key
}

func readyPrefix(namespace, address string) []byte {
	key := make([]byte, 1+len(namespace)+len(address))
	key[0] = keyReady
	off := 1
	off += copy(key[off:], encodeID(namespace))
	copy(key[off:], encodeID(address))
	return key
}

func deadlineKey(deadline int64, kind byte, namespace, address, object string, stateVersion uint64) []byte {
	key := make([]byte, 1+8+1+len(namespace)+len(address)+len(object)+8)
	key[0] = keyDeadline
	off := 1
	binary.BigEndian.PutUint64(key[off:], uint64(deadline))
	off += 8
	key[off] = kind
	off++
	off += copy(key[off:], encodeID(namespace))
	off += copy(key[off:], encodeID(address))
	off += copy(key[off:], encodeID(object))
	binary.BigEndian.PutUint64(key[off:], stateVersion)
	return key
}

func claimKey(namespace, address, claim string) []byte {
	key := make([]byte, 1+len(namespace)+len(address)+len(claim))
	key[0] = keyClaim
	off := 1
	off += copy(key[off:], encodeID(namespace))
	off += copy(key[off:], encodeID(address))
	copy(key[off:], encodeID(claim))
	return key
}

func idempotencyKey(namespace, operation, request string) []byte {
	key := make([]byte, 1+len(namespace)+len(operation)+len(request))
	key[0] = keyIdempotency
	off := 1
	off += copy(key[off:], encodeID(namespace))
	off += copy(key[off:], encodeID(operation))
	copy(key[off:], encodeID(request))
	return key
}

func settlementKey(namespace, address, unit string) []byte {
	key := make([]byte, 1+len(namespace)+len(address)+len(unit))
	key[0] = keySettlement
	off := 1
	off += copy(key[off:], encodeID(namespace))
	off += copy(key[off:], encodeID(address))
	copy(key[off:], encodeID(unit))
	return key
}

func namespaceCounterKey(namespace string) []byte {
	key := make([]byte, 1+len(namespace))
	key[0] = keyNamespaceCtr
	copy(key[1:], encodeID(namespace))
	return key
}
