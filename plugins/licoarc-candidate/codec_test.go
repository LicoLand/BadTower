package licoarccandidate

import (
	"bytes"
	"testing"
)

func TestCandidateCodecAcceptsExactCanonicalRequests(t *testing.T) {
	t.Parallel()
	digestA := bytes.Repeat([]byte{1}, 32)
	digestB := bytes.Repeat([]byte{2}, 32)
	digestC := bytes.Repeat([]byte{3}, 32)

	affiliate := mustEncode(t, map[uint64]any{0: digestA, 1: digestB})
	if _, err := DecodeAffiliate(affiliate); err != nil {
		t.Fatal(err)
	}
	reserve := mustEncode(t, map[uint64]any{0: digestA, 1: digestB, 2: digestC, 3: uint64(HandleAsync)})
	if decoded, err := DecodeReserve(reserve); err != nil || decoded.HandleClass != HandleAsync {
		t.Fatalf("DecodeReserve: %#v err=%v", decoded, err)
	}
	claim := mustEncode(t, map[uint64]any{0: uint64(64)})
	if decoded, err := DecodeClaim(claim); err != nil || decoded.MaxItems != 64 {
		t.Fatalf("DecodeClaim: %#v err=%v", decoded, err)
	}
	settle := mustEncode(t, map[uint64]any{0: digestA, 1: uint64(1), 2: []any{map[uint64]any{0: digestB, 1: uint64(SettleComplete)}}})
	if decoded, err := DecodeSettle(settle); err != nil || len(decoded.Settlements) != 1 {
		t.Fatalf("DecodeSettle: %#v err=%v", decoded, err)
	}
}

func TestCandidateCodecRejectsNonCanonicalUnknownDuplicateAndBounds(t *testing.T) {
	t.Parallel()
	if _, err := DecodeClaim([]byte{0xb8, 0x01, 0x00, 0x01}); err == nil {
		t.Fatal("non-shortest map accepted")
	}
	unknown := mustEncode(t, map[uint64]any{0: uint64(1), 1: uint64(2)})
	if _, err := DecodeClaim(unknown); err == nil {
		t.Fatal("unknown label accepted")
	}
	duplicate := []byte{0xa2, 0x00, 0x01, 0x00, 0x02}
	if _, err := DecodeClaim(duplicate); err == nil {
		t.Fatal("duplicate label accepted")
	}
	over := mustEncode(t, map[uint64]any{0: uint64(65)})
	if _, err := DecodeClaim(over); err == nil {
		t.Fatal("claim bound widening accepted")
	}
}

func TestCandidateCodecRawPacketResponseDoesNotWrapPacket(t *testing.T) {
	t.Parallel()
	packet := []byte{0x01, 0x02, 0x03}
	item := ClaimedItem{ProtectedPacket: packet}
	item.ItemID[0] = 1
	var claimID [32]byte
	claimID[0] = 2
	raw, err := EncodeClaimResponse(OutcomeAccepted, claimID, 1, []ClaimedItem{item}, 0, 0)
	if err != nil || !bytes.Contains(raw, packet) {
		t.Fatalf("EncodeClaimResponse: %x err=%v", raw, err)
	}
}

func TestCandidateCodecClaimResponseUsesExactLargeBound(t *testing.T) {
	t.Parallel()
	packet := bytes.Repeat([]byte{0xa5}, MaxControlBodyBytes)
	item := ClaimedItem{ProtectedPacket: packet}
	item.ItemID[0] = 1
	var claimID [32]byte
	claimID[0] = 2
	raw, err := EncodeClaimResponse(OutcomeAccepted, claimID, 1, []ClaimedItem{item}, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) <= MaxControlBodyBytes || len(raw) > MaxClaimResponseBytes {
		t.Fatalf("claim response bytes=%d outside (%d,%d]", len(raw), MaxControlBodyBytes, MaxClaimResponseBytes)
	}
}

func mustEncode(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := canonicalEncoder.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
