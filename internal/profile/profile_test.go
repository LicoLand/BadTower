package profile

import "testing"

func TestDefaultProfile(t *testing.T) {
	t.Parallel()
	value := Default()
	if !value.Accepts("licoarc.relay.v1") {
		t.Fatal("default profile must accept licoarc.relay.v1")
	}
	versions := value.AcceptedContractVersions()
	versions[0] = "modified.example.v1"
	if !value.Accepts("licoarc.relay.v1") || value.Accepts(versions[0]) {
		t.Fatal("profile exposed mutable contract versions")
	}
}

func TestCustomProfileIsBoundedAndCopied(t *testing.T) {
	t.Parallel()
	config := Config{
		ProfileVersion:           Version,
		AcceptedContractVersions: []string{"example.relay.v1"},
		Limits:                   ImplementationCeilings(),
	}
	config.Limits.MaxMailboxes = 1
	value, err := New(config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	config.AcceptedContractVersions[0] = "modified.relay.v1"
	if !value.Accepts("example.relay.v1") || value.Accepts("modified.relay.v1") {
		t.Fatal("profile retained caller-owned slice")
	}

	config = Config{
		ProfileVersion:           Version,
		AcceptedContractVersions: []string{"example.relay.v1"},
		Limits:                   ImplementationCeilings(),
	}
	config.Limits.MaxRelayEnvelopes++
	if _, err := New(config); err == nil {
		t.Fatal("expected over-ceiling profile rejection")
	}
}

func TestProfileRejectsInvalidVersions(t *testing.T) {
	t.Parallel()
	for _, versions := range [][]string{
		nil,
		{"A"},
		{"example.relay.v1", "example.relay.v1"},
	} {
		config := Config{
			ProfileVersion:           Version,
			AcceptedContractVersions: versions,
			Limits:                   ImplementationCeilings(),
		}
		if _, err := New(config); err == nil {
			t.Fatalf("expected versions %v to fail", versions)
		}
	}
}
