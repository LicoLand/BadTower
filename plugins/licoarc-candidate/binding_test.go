package licoarccandidate

import (
	"encoding/hex"
	"testing"
)

func TestCandidateBindingIsExactAndClosed(t *testing.T) {
	t.Parallel()
	if ProtocolWireID != "licoarc.protocol-line.v1" || ProtocolLineID != "licoarc.v1" || TransportProfileID != "licoarc.https-transport.v1" || Lifecycle != "Candidate" {
		t.Fatal("exact Candidate identity changed")
	}
	for name, digest := range map[string]string{
		"protocol manifest": ProtocolManifestSHA256,
		"transport source":  TransportSourceDigest,
	} {
		raw, err := hex.DecodeString(digest)
		if err != nil || len(raw) != 32 || hex.EncodeToString(raw) != digest {
			t.Fatalf("%s digest is not exact lowercase SHA-256", name)
		}
	}
	if MaxPacketBytes != 524288 || MaxClaimItems != 64 || IdempotencyWindowSeconds != 300 || StorageWindowSeconds != 3600 {
		t.Fatal("exact Candidate bounds changed")
	}
}
