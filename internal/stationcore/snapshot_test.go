package stationcore

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSnapshotCheckpointRestoresStateCountersAndIdempotency(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	core := openTestCore(t, clock)
	namespace := "ns_SSSSSSSSSSSSSSSSSSSSSS"
	reserve := ReserveInput{NamespaceID: namespace, RequestID: "rq_snapshotReserve00001", TTLMs: 3_600_000}
	reserved, err := core.Reserve(context.Background(), reserve)
	if err != nil {
		t.Fatal(err)
	}
	submit := SubmitInput{NamespaceID: namespace, RequestID: "rq_snapshotSubmit000001", AddressID: reserved.AddressID, Payload: []byte("opaque-checkpoint"), TTLMs: 300_000}
	submitted, err := core.Submit(context.Background(), submit)
	if err != nil {
		t.Fatal(err)
	}
	wantCapacity, err := core.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}

	checkpointRoot := filepath.Join(t.TempDir(), "checkpoint")
	checkpoint, err := core.ExportSnapshotCheckpoint(checkpointRoot)
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint.StoreIdentity != StoreIdentity || len(checkpoint.DigestSHA256) != 64 {
		t.Fatalf("checkpoint descriptor is invalid")
	}
	restoredRoot := filepath.Join(t.TempDir(), "restored")
	if err := RestoreSnapshotCheckpoint(checkpointRoot, restoredRoot); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(Config{DataRoot: restoredRoot, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })

	gotCapacity, err := restored.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}
	if gotCapacity != wantCapacity {
		t.Fatalf("restored capacity differs: got %+v want %+v", gotCapacity, wantCapacity)
	}
	replayed, err := restored.Submit(context.Background(), submit)
	if err != nil {
		t.Fatal(err)
	}
	if replayed != submitted {
		t.Fatalf("idempotent response differs after restore")
	}
	claimed, err := restored.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace, RequestID: "rq_snapshotClaim0000001", AddressID: reserved.AddressID,
		MaxUnits: 1, MaxBytes: 1024, VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed.Units) != 1 || claimed.Units[0].UnitID != submitted.UnitID || string(claimed.Units[0].Payload) != "opaque-checkpoint" {
		t.Fatalf("restored unit differs")
	}
}

func TestSnapshotCheckpointRejectsForeignAndNonFreshRoots(t *testing.T) {
	t.Parallel()
	foreign := t.TempDir()
	if err := os.WriteFile(filepath.Join(foreign, "foreign"), []byte("not-a-core"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RestoreSnapshotCheckpoint(foreign, filepath.Join(t.TempDir(), "restore")); err == nil {
		t.Fatal("foreign checkpoint accepted")
	}

	core := openTestCore(t, newTestClock(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)))
	checkpointRoot := filepath.Join(t.TempDir(), "checkpoint")
	if _, err := core.ExportSnapshotCheckpoint(checkpointRoot); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(checkpointRoot, "CURRENT"), filepath.Join(checkpointRoot, "foreign-link")); err != nil {
		t.Fatal(err)
	}
	if err := RestoreSnapshotCheckpoint(checkpointRoot, filepath.Join(t.TempDir(), "symlink-restore")); err == nil {
		t.Fatal("checkpoint containing a symlink accepted")
	}
	if err := os.Remove(filepath.Join(checkpointRoot, "foreign-link")); err != nil {
		t.Fatal(err)
	}
	nonFresh := t.TempDir()
	if err := RestoreSnapshotCheckpoint(checkpointRoot, nonFresh); err == nil {
		t.Fatal("non-fresh restore root accepted")
	}
	if _, err := core.ExportSnapshotCheckpoint(checkpointRoot); err == nil {
		t.Fatal("existing checkpoint root accepted")
	}
}

func TestInstallSnapshotCheckpointRollsBackEveryDestructiveFaultPoint(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC))
	source := openTestCore(t, clock)
	target := openTestCore(t, clock)
	sourceNamespace := "ns_IIIIIIIIIIIIIIIIIIIIII"
	targetNamespace := "ns_JJJJJJJJJJJJJJJJJJJJJJ"
	if _, err := source.Reserve(context.Background(), ReserveInput{
		NamespaceID: sourceNamespace, RequestID: "rq_installSource0000001", TTLMs: 3_600_000,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Reserve(context.Background(), ReserveInput{
		NamespaceID: targetNamespace, RequestID: "rq_installTarget0000001", TTLMs: 3_600_000,
	}); err != nil {
		t.Fatal(err)
	}
	checkpointRoot := filepath.Join(t.TempDir(), "checkpoint")
	if _, err := source.ExportSnapshotCheckpoint(checkpointRoot); err != nil {
		t.Fatal(err)
	}

	for _, faultPoint := range []snapshotInstallFault{snapshotInstallFaultAfterClose, snapshotInstallFaultAfterSwap} {
		if err := target.installSnapshotCheckpoint(checkpointRoot, faultPoint); err == nil {
			t.Fatalf("fault point %d unexpectedly succeeded", faultPoint)
		}
		oldCapacity, err := target.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: targetNamespace})
		if err != nil || oldCapacity.NamespaceUsage.Addresses != 1 {
			t.Fatalf("old state unavailable after fault %d: usage=%+v err=%v", faultPoint, oldCapacity.NamespaceUsage, err)
		}
		newCapacity, err := target.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: sourceNamespace})
		if err != nil || newCapacity.NamespaceUsage.Addresses != 0 {
			t.Fatalf("candidate leaked after fault %d: usage=%+v err=%v", faultPoint, newCapacity.NamespaceUsage, err)
		}
	}

	if err := target.InstallSnapshotCheckpoint(checkpointRoot); err != nil {
		t.Fatal(err)
	}
	installed, err := target.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: sourceNamespace})
	if err != nil || installed.NamespaceUsage.Addresses != 1 {
		t.Fatalf("installed state missing: usage=%+v err=%v", installed.NamespaceUsage, err)
	}
	retired, err := target.GetCapacity(context.Background(), GetCapacityInput{NamespaceID: targetNamespace})
	if err != nil || retired.NamespaceUsage.Addresses != 0 {
		t.Fatalf("old state survived install: usage=%+v err=%v", retired.NamespaceUsage, err)
	}
}

func TestSnapshotCheckpointBoundRejectsBeforePublishingArtifact(t *testing.T) {
	t.Parallel()
	core := openTestCore(t, newTestClock(time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)))
	destination := filepath.Join(t.TempDir(), "bounded")
	_, err := core.ExportSnapshotCheckpointBounded(destination, 1)
	failure, ok := IsFailure(err)
	if !ok || failure.Reason != ReasonCapacityExceeded {
		t.Fatalf("bounded export error = %v", err)
	}
	if _, statErr := os.Stat(destination); !os.IsNotExist(statErr) {
		t.Fatalf("oversized artifact was published: %v", statErr)
	}
}
