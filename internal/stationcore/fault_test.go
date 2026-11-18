package stationcore

// Executor must provide:
//   - func Open(Config, Clock) (*Core, error) with WAL recovery (S-02, C-07)
//   - func (*Core) DataRoot() string
//   - strict store identity and schema-4 root manifest (S-01)
//   - typed ReasonCorruptState mapped to DATA_LOSS (C-11)
//   - no import/open of data/badtower.db or badtower.station-store.v1 (S-01, M-02)

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSchema4RootRejectsMissingExtraSymlinkAndSwappedShards(t *testing.T) {
	base := filepath.Join(t.TempDir(), "base")
	core, err := Open(Config{DataRoot: base, ProfileID: ProfileSingleNodeGeneralV1}, newTestClock(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}

	tests := map[string]func(*testing.T, string){
		"partial": func(t *testing.T, root string) {
			if err := os.Remove(filepath.Join(root, storeRootManifestName)); err != nil {
				t.Fatal(err)
			}
		},
		"missing": func(t *testing.T, root string) {
			if err := os.RemoveAll(filepath.Join(root, shardDirectoryName(0))); err != nil {
				t.Fatal(err)
			}
		},
		"extra": func(t *testing.T, root string) {
			if err := os.WriteFile(filepath.Join(root, "extra"), []byte{1}, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"symlink": func(t *testing.T, root string) {
			shard := filepath.Join(root, shardDirectoryName(0))
			if err := os.RemoveAll(shard); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(root, shard); err != nil {
				t.Fatal(err)
			}
		},
		"manifest": func(t *testing.T, root string) {
			path := filepath.Join(root, storeRootManifestName)
			manifest, err := readStoreRootManifest(path)
			if err != nil {
				t.Fatal(err)
			}
			manifest.Schema++
			raw, err := json.Marshal(manifest)
			if err != nil || os.WriteFile(path, raw, 0o600) != nil {
				t.Fatal("write manifest")
			}
		},
		"duplicate-manifest-key": func(t *testing.T, root string) {
			path := filepath.Join(root, storeRootManifestName)
			raw, err := os.ReadFile(path)
			if err != nil || len(raw) < 2 {
				t.Fatal(err)
			}
			raw = append(raw[:len(raw)-1], []byte(`,"schema":4}`)...)
			if err := os.WriteFile(path, raw, 0o600); err != nil {
				t.Fatal(err)
			}
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "candidate")
			if err := os.Mkdir(root, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := copyStoreTree(base, root); err != nil {
				t.Fatal(err)
			}
			mutate(t, root)
			_, err := Open(Config{DataRoot: root, ProfileID: ProfileSingleNodeGeneralV1}, newTestClock(time.Now()))
			if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonCorruptState {
				t.Fatalf("schema-4 root mutation opened: %v", err)
			}
		})
	}
}

func TestSchema4PartitionRootsRejectSwappedShardDirectories(t *testing.T) {
	roots := [2]string{filepath.Join(t.TempDir(), "partition-1"), filepath.Join(t.TempDir(), "partition-2")}
	for index, root := range roots {
		partition := uint16(index + 1)
		core, err := Open(Config{DataRoot: root, ProfileID: ProfileSingleNodeGeneralV1, StorageMode: StorageModePartitionOne, Partition: partition}, newTestClock(time.Now()))
		if err != nil {
			t.Fatal(err)
		}
		if err := core.Close(); err != nil {
			t.Fatal(err)
		}
	}
	one := filepath.Join(roots[0], shardDirectoryName(1))
	two := filepath.Join(roots[1], shardDirectoryName(2))
	temporary := filepath.Join(t.TempDir(), "swap")
	if err := os.Rename(one, temporary); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(two, one); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temporary, two); err != nil {
		t.Fatal(err)
	}
	for index, root := range roots {
		partition := uint16(index + 1)
		_, err := Open(Config{DataRoot: root, ProfileID: ProfileSingleNodeGeneralV1, StorageMode: StorageModePartitionOne, Partition: partition}, newTestClock(time.Now()))
		if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonCorruptState {
			t.Fatalf("swapped partition %d opened: %v", partition, err)
		}
	}
}

func TestCrashAcknowledgedSubmitSurvivesImmediateRestart(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 7, 0, 0, 0, time.UTC))
	dir := t.TempDir()
	core, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	namespace := "ns_HHHHHHHHHHHHHHHHHHHHHH"
	address := reserveAddress(t, core, namespace, "rq_crashSubmitRes001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_crashSubmitSub001", address, []byte("survive"))
	if err := core.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restarted, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })

	claim, err := restarted.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_crashSubmitClm001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim after crash: %v", err)
	}
	if len(claim.Units) != 1 || claim.Units[0].UnitID != unitID {
		t.Fatalf("unit missing after WAL recovery: %#v", claim)
	}
}

func TestCrashActiveClaimRetainsEpochAndDeadline(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 8, 0, 0, 0, time.UTC))
	dir := t.TempDir()
	core, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	namespace := "ns_IIIIIIIIIIIIIIIIIIIIII"
	address := reserveAddress(t, core, namespace, "rq_crashClaimRes0001", 3_600_000)
	submitUnit(t, core, namespace, "rq_crashClaimSub0001", address, []byte("claimed"))

	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_crashClaimClm0001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 120_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	epoch := claim.ClaimEpoch
	claimID := claim.ClaimID
	if err := core.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restarted, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })

	settled, err := restarted.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_crashClaimSet0001",
		ClaimID:     claimID,
		ClaimEpoch:  epoch,
		Entries: []SettleEntry{{
			UnitID: claim.Units[0].UnitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatalf("Settle after crash with preserved claim: %v", err)
	}
	if len(settled.Units) != 1 {
		t.Fatalf("Settle units = %d", len(settled.Units))
	}
}

func TestCrashSettleDurableAndReplayAfterRestart(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 7, 30, 0, 0, time.UTC))
	dir := t.TempDir()
	core, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	namespace := "ns_aaaaaaaaaaaaaaaaaaaaab"
	address := reserveAddress(t, core, namespace, "rq_crashSetRes000001", 3_600_000)
	unitID := submitUnit(t, core, namespace, "rq_crashSetSub000001", address, []byte("settled"))
	claim, err := core.Claim(context.Background(), ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_crashSetClm000001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	settled, err := core.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_crashSetSet000001",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: unitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatalf("Settle: %v", err)
	}
	if settled.Units[0].Outcome != SettleOutcomeCompleted {
		t.Fatalf("settle outcome = %q", settled.Units[0].Outcome)
	}
	if err := core.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	restarted, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = restarted.Close() })

	after, err := restarted.Claim(context.Background(), ClaimInput{
		NamespaceID: namespace,
		RequestID:   "rq_crashSetClm000002",
		AddressID:   address,
		MaxUnits:    1,
		MaxBytes:    1 << 20,
	})
	if err != nil {
		t.Fatalf("Claim after restart: %v", err)
	}
	if len(after.Units) != 0 {
		t.Fatalf("completed unit must stay deleted after restart: %#v", after)
	}

	replayed, err := restarted.Settle(context.Background(), SettleInput{
		NamespaceID: namespace,
		RequestID:   "rq_crashSetSet000001",
		ClaimID:     claim.ClaimID,
		ClaimEpoch:  claim.ClaimEpoch,
		Entries: []SettleEntry{{
			UnitID: unitID,
			Action: SettleComplete,
		}},
	})
	if err != nil {
		t.Fatalf("idempotent Settle replay after restart: %v", err)
	}
	if len(replayed.Units) != 1 || replayed.Units[0].Outcome != SettleOutcomeCompleted {
		t.Fatalf("settle replay must return the stored result: %#v", replayed)
	}
}

func TestCrashWatermarkPersistedStartupClockBackwardFailsClosed(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 31, 8, 30, 0, 0, time.UTC)
	dir := t.TempDir()

	firstClock := newTestClock(start)
	core, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, firstClock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	reserveAddress(t, core, "ns_bbbbbbbbbbbbbbbbbbbbbb", "rq_wmReserve00000001", 3_600_000)
	if err := core.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Startup more than 60 seconds behind the persisted watermark: readiness
	// false and every mutation fails CLOCK_UNSAFE (C-10).
	behind := newTestClock(start.Add(-70 * time.Second))
	unsafe, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, behind)
	if err != nil {
		t.Fatalf("Open behind clock: %v", err)
	}
	if ready, reason := unsafe.Ready(); ready || reason != ReasonClockUnsafe {
		t.Fatalf("startup clock beyond clamp must fail readiness: ready=%t reason=%s", ready, reason)
	}
	_, err = unsafe.Reserve(context.Background(), ReserveInput{
		NamespaceID: "ns_bbbbbbbbbbbbbbbbbbbbbb",
		RequestID:   "rq_wmBlocked00000001",
		TTLMs:       60_000,
	})
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonClockUnsafe {
		t.Fatalf("mutation with unsafe startup clock: err=%v", err)
	}
	if err := unsafe.Close(); err != nil {
		t.Fatalf("Close unsafe: %v", err)
	}

	// Startup within the 60-second clamp: clamped, safe, and serving.
	clamped := newTestClock(start.Add(-30 * time.Second))
	reopened, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clamped)
	if err != nil {
		t.Fatalf("Open clamped clock: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if ready, reason := reopened.Ready(); !ready {
		t.Fatalf("clamped startup must be ready: reason=%s", reason)
	}
	if _, err := reopened.Reserve(context.Background(), ReserveInput{
		NamespaceID: "ns_bbbbbbbbbbbbbbbbbbbbbb",
		RequestID:   "rq_wmClamped00000001",
		TTLMs:       60_000,
	}); err != nil {
		t.Fatalf("mutation with clamped startup clock: %v", err)
	}
}

func TestCorruptionForeignDirectoryNeverImported(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("not a pebble store"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, newTestClock(time.Now()))
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonCorruptState {
		t.Fatalf("foreign directory must fail closed without import: err=%v", err)
	}
}

func TestCorruptionMissingStoreMarkerFailsClosed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "LOCK"), []byte("foreign"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, newTestClock(time.Now()))
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonCorruptState {
		t.Fatalf("missing marker must fail closed: err=%v", err)
	}
}

func TestCorruptionRetiredStoreIdentityRejectedWithoutInspection(t *testing.T) {
	t.Parallel()
	for _, retired := range []string{"data/badtower.db", "badtower.station-store.v1"} {
		retired := retired
		t.Run(retired, func(t *testing.T) {
			t.Parallel()
			_, err := Open(Config{
				DataRoot:  retired,
				ProfileID: ProfileSingleNodeGeneralV1,
			}, newTestClock(time.Now()))
			if err == nil {
				t.Fatal("retired store path must not open")
			}
		})
	}
}

func TestCorruptionTamperedRecordNeverAutoRepairs(t *testing.T) {
	t.Parallel()
	clock := newTestClock(time.Date(2026, 7, 31, 9, 0, 0, 0, time.UTC))
	dir := t.TempDir()
	core, err := Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	namespace := "ns_JJJJJJJJJJJJJJJJJJJJJJ"
	reserveAddress(t, core, namespace, "rq_corruptRes00000001", 3_600_000)
	if err := core.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	corruptStoreFiles(t, dir)

	_, err = Open(Config{DataRoot: dir, ProfileID: ProfileSingleNodeGeneralV1}, clock)
	if failure, ok := IsFailure(err); !ok || failure.Reason != ReasonCorruptState {
		t.Fatalf("tampered store must fail closed without repair: err=%v", err)
	}
}

func corruptStoreFiles(t *testing.T, root string) {
	t.Helper()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		info, statErr := entry.Info()
		if statErr != nil || info.Size() == 0 {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		content[0] ^= 0xff
		return os.WriteFile(path, content, info.Mode())
	})
	if err != nil {
		t.Fatalf("corrupt store files: %v", err)
	}
}
