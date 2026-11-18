package extension

import (
	"bytes"
	"testing"
)

func TestDescriptorClosedAndCanonical(t *testing.T) {
	t.Parallel()
	descriptor := validDescriptor()
	raw, err := descriptor.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeDescriptor(raw)
	if err != nil {
		t.Fatal(err)
	}
	second, err := decoded.CanonicalJSON()
	if err != nil || !bytes.Equal(raw, second) {
		t.Fatalf("canonical mismatch: err=%v", err)
	}
	withUnknown := append(raw[:len(raw)-1], []byte(`,"metadata":{}}`)...)
	if _, err := DecodeDescriptor(withUnknown); err == nil {
		t.Fatal("unknown descriptor member accepted")
	}
	duplicate := append(raw[:len(raw)-1], []byte(`,"pluginId":"shadow.plugin"}`)...)
	if _, err := DecodeDescriptor(duplicate); err == nil {
		t.Fatal("duplicate descriptor member accepted")
	}
}

func TestDescriptorRejectsBudgetAndScopeWidening(t *testing.T) {
	t.Parallel()
	descriptor := validDescriptor()
	descriptor.Budgets.MaxMetricSeries = 65
	if err := descriptor.Validate(); err == nil {
		t.Fatal("metric series widening accepted")
	}
	descriptor = validDescriptor()
	descriptor.NativeScope.NamespaceMode = "all"
	if err := descriptor.Validate(); err == nil {
		t.Fatal("native scope widening accepted")
	}
}

func validDescriptor() Descriptor {
	return Descriptor{
		SchemaVersion: SchemaVersion, PluginID: "licoarc.candidate", ArtifactVersion: "0.1.0-candidate",
		ConfigurationSchema: "badtower.licoarc-candidate.config.v1", LocalStateSchema: "badtower.licoarc-candidate.state.v1",
		ExternalProtocol: ExternalProtocol{Identity: "licoarc", Line: "licoarc.protocol-line.v1", Lifecycle: "Candidate"},
		Contract:         ContractRange{Minimum: ContractVersion, Maximum: ContractVersion},
		Listener:         Listener{Owner: "external-plugin", Transport: "https-tls13-h2"},
		NativeScope:      NativeScope{NamespaceMode: "one-opaque-namespace", CredentialMode: "scoped-native-caller"},
		Budgets:          Budgets{MaxConcurrentCalls: 128, MaxQueuedCalls: 1024, MaxRequestBytes: 524288, MaxStateBytes: 1 << 30, MaxMetricSeries: 32},
		Binary:           Binary{Path: "bin/badtower-licoarc-candidate", SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}
}
