package stationcluster

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

type snapshotTestClock struct {
	wall time.Time
}

func (clock *snapshotTestClock) NowUnixMs() int64            { return clock.wall.UnixMilli() }
func (clock *snapshotTestClock) MonotonicNow() time.Duration { return 0 }

func TestCoreSnapshotRoundTripPreservesStateCountersAndIdempotency(t *testing.T) {
	t.Parallel()
	clock := &snapshotTestClock{wall: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	namespace := "ns_TTTTTTTTTTTTTTTTTTTTTT"
	partition := stationcore.PartitionFor(namespace)
	core := openSnapshotTestCore(t, clock, partition)
	reserveInput := stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_snapshotReserve00002", TTLMs: 3_600_000}
	reserved, err := core.Reserve(context.Background(), reserveInput)
	if err != nil {
		t.Fatal(err)
	}
	submitInput := stationcore.SubmitInput{
		NamespaceID: namespace, RequestID: "rq_snapshotSubmit000002", AddressID: reserved.AddressID,
		Payload: make([]byte, 32<<10), TTLMs: 300_000,
	}
	for index := range submitInput.Payload {
		submitInput.Payload[index] = byte(index % 251)
	}
	submitted, err := core.Submit(context.Background(), submitInput)
	if err != nil {
		t.Fatal(err)
	}
	claimInput := stationcore.ClaimInput{
		NamespaceID: namespace, RequestID: "rq_snapshotClaim0000002", AddressID: reserved.AddressID,
		MaxUnits: 1, MaxBytes: 64 << 10, VisibilityTimeoutMs: 30_000,
	}
	claimed, err := core.Claim(context.Background(), claimInput)
	if err != nil {
		t.Fatal(err)
	}
	wantCapacity, err := core.GetCapacity(context.Background(), stationcore.GetCapacityInput{NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}

	binding := CoreSnapshotBinding{ClusterID: "cluster-snapshot-contract", GroupID: 48, SourceMemberID: 2, Partition: partition, OwnershipEpoch: 9, AppliedIndex: 71}
	artifactRoot := filepath.Join(t.TempDir(), "artifact")
	manifest, err := CreateCoreSnapshot(core, artifactRoot, binding, MinCoreSnapshotChunkBytes)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Partition != partition || manifest.OwnershipEpoch != 9 || manifest.AppliedIndex != 71 || len(manifest.Chunks) < 2 {
		t.Fatalf("manifest binding or chunking differs")
	}
	for _, chunk := range manifest.Chunks {
		if chunk.SizeBytes == 0 || chunk.SizeBytes > MinCoreSnapshotChunkBytes || len(chunk.DigestSHA256) != 64 {
			t.Fatalf("unbounded or invalid chunk descriptor")
		}
	}

	restoredRoot := filepath.Join(t.TempDir(), "restored")
	gotManifest, err := RestoreCoreSnapshot(artifactRoot, CoreSnapshotExpectation{
		ClusterID: binding.ClusterID, GroupID: binding.GroupID, SourceMemberID: binding.SourceMemberID,
		Partition: partition, OwnershipEpoch: 9, CurrentAppliedIndex: 70,
	}, restoredRoot)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(gotManifest, manifest) {
		t.Fatal("restored manifest differs")
	}
	restored, err := stationcore.Open(stationcore.Config{DataRoot: restoredRoot, ProfileID: stationcore.ProfileSingleNodeGeneralV1, StorageMode: stationcore.StorageModePartitionOne, Partition: partition}, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restored.Close() })

	gotCapacity, err := restored.GetCapacity(context.Background(), stationcore.GetCapacityInput{NamespaceID: namespace})
	if err != nil {
		t.Fatal(err)
	}
	if gotCapacity != wantCapacity {
		t.Fatalf("capacity differs after restore")
	}
	replayedReserve, err := restored.Reserve(context.Background(), reserveInput)
	if err != nil || replayedReserve != reserved {
		t.Fatalf("reserve idempotency differs after restore")
	}
	replayedSubmit, err := restored.Submit(context.Background(), submitInput)
	if err != nil || replayedSubmit != submitted {
		t.Fatalf("submit idempotency differs after restore")
	}
	replayedClaim, err := restored.Claim(context.Background(), claimInput)
	if err != nil || !reflect.DeepEqual(replayedClaim, claimed) {
		t.Fatalf("claim idempotency differs after restore")
	}
	settleInput := stationcore.SettleInput{
		NamespaceID: namespace, RequestID: "rq_snapshotSettle000002", ClaimID: claimed.ClaimID, ClaimEpoch: claimed.ClaimEpoch,
		Entries: []stationcore.SettleEntry{{UnitID: submitted.UnitID, Action: stationcore.SettleComplete}},
	}
	sourceSettlement, sourceErr := core.Settle(context.Background(), settleInput)
	restoredSettlement, restoredErr := restored.Settle(context.Background(), settleInput)
	if sourceErr != nil || restoredErr != nil || !reflect.DeepEqual(sourceSettlement, restoredSettlement) {
		t.Fatalf("settlement differs after restore: source=%v restored=%v", sourceErr, restoredErr)
	}
}

func TestCoreSnapshotRejectsWrongBindingStaleTruncationAndTamper(t *testing.T) {
	t.Parallel()
	clock := &snapshotTestClock{wall: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	core := openSnapshotTestCore(t, clock, 4)
	binding := CoreSnapshotBinding{ClusterID: "cluster-snapshot-negative", GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 12, AppliedIndex: 88}
	artifactRoot := filepath.Join(t.TempDir(), "artifact")
	manifest, err := CreateCoreSnapshot(core, artifactRoot, binding, 4096)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := CreateCoreSnapshot(core, filepath.Join(core.DataRoot(), "nested-artifact"), binding, 4096); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("artifact inside live Core root error = %v", err)
	}
	if _, err := RestoreCoreSnapshot(artifactRoot, CoreSnapshotExpectation{ClusterID: binding.ClusterID, GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 12, CurrentAppliedIndex: 1}, filepath.Join(artifactRoot, "nested-restore")); !errors.Is(err, ErrInvalidSnapshot) {
		t.Fatalf("restore inside artifact error = %v", err)
	}

	tests := []struct {
		name     string
		expected CoreSnapshotExpectation
		want     error
	}{
		{"foreign-cluster", CoreSnapshotExpectation{ClusterID: "another-cluster", GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 12, CurrentAppliedIndex: 1}, ErrInvalidSnapshot},
		{"wrong-partition", CoreSnapshotExpectation{ClusterID: binding.ClusterID, GroupID: 5, SourceMemberID: 1, Partition: 5, OwnershipEpoch: 12, CurrentAppliedIndex: 1}, ErrInvalidSnapshot},
		{"wrong-epoch", CoreSnapshotExpectation{ClusterID: binding.ClusterID, GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 13, CurrentAppliedIndex: 1}, ErrInvalidSnapshot},
		{"stale-equal-index", CoreSnapshotExpectation{ClusterID: binding.ClusterID, GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 12, CurrentAppliedIndex: 88}, ErrStaleSnapshot},
		{"stale-older-index", CoreSnapshotExpectation{ClusterID: binding.ClusterID, GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 12, CurrentAppliedIndex: 89}, ErrStaleSnapshot},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := RestoreCoreSnapshot(artifactRoot, test.expected, filepath.Join(t.TempDir(), "restore"))
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}

	t.Run("foreign-core-identity", func(t *testing.T) {
		root := cloneSnapshotArtifact(t, artifactRoot)
		foreign := manifest
		foreign.CoreIdentity = "foreign.store.v1"
		raw, err := json.Marshal(foreign)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "manifest.json"), raw, 0o600); err != nil {
			t.Fatal(err)
		}
		_, err = RestoreCoreSnapshot(root, CoreSnapshotExpectation{ClusterID: binding.ClusterID, GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 12, CurrentAppliedIndex: 1}, filepath.Join(t.TempDir(), "restore"))
		if !errors.Is(err, ErrInvalidSnapshot) {
			t.Fatalf("foreign identity error = %v", err)
		}
	})

	for _, mutation := range []struct {
		name string
		edit func([]byte) []byte
	}{
		{"truncation", func(raw []byte) []byte { return raw[:len(raw)-1] }},
		{"tamper", func(raw []byte) []byte { raw[len(raw)/2] ^= 0x80; return raw }},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			root := cloneSnapshotArtifact(t, artifactRoot)
			chunkPath := filepath.Join(root, snapshotChunkName(0))
			raw, err := os.ReadFile(chunkPath)
			if err != nil || len(raw) < 2 {
				t.Fatalf("read chunk: %v", err)
			}
			if err := os.WriteFile(chunkPath, mutation.edit(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err = RestoreCoreSnapshot(root, CoreSnapshotExpectation{ClusterID: binding.ClusterID, GroupID: 5, SourceMemberID: 1, Partition: 4, OwnershipEpoch: 12, CurrentAppliedIndex: 1}, filepath.Join(t.TempDir(), "restore"))
			if !errors.Is(err, ErrInvalidSnapshot) {
				t.Fatalf("corruption error = %v", err)
			}
		})
	}
}

func openSnapshotTestCore(t *testing.T, clock stationcore.Clock, partition uint16) *stationcore.Core {
	t.Helper()
	core, err := stationcore.Open(stationcore.Config{
		DataRoot: filepath.Join(t.TempDir(), "core"), ProfileID: stationcore.ProfileSingleNodeGeneralV1,
		StorageMode: stationcore.StorageModePartitionOne, Partition: partition,
	}, clock)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = core.Close() })
	return core
}

func cloneSnapshotArtifact(t *testing.T, source string) string {
	t.Helper()
	destination := filepath.Join(t.TempDir(), "artifact")
	if err := os.Mkdir(destination, 0o700); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		raw, err := os.ReadFile(filepath.Join(source, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(destination, entry.Name()), raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return destination
}
