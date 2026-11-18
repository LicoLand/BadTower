package acceptance

// Executor must provide (S-02, REQ-018; K4 evidence):
//   - func (core *stationcore.Core) Backup(destination string) (digest string, err error)
//     one synchronous Pebble checkpoint with flushed WAL, copied to an
//     independent destination, destination sync, and a SHA-256 digest;
//     a hard-link-only copy is not a backup; destination equal to or inside
//     the live data root is rejected; no data request path may run it.
//   - func (core *stationcore.Core) RestoreIsolated(backupRoot, isolatedRoot string) error
//     opens the backup copy inside an isolated restore root, verifies the
//     exact identity/schema/format and Pebble level check, never opens or
//     modifies the live root, never accesses retired state, and leaves
//     isolatedRoot as a directly openable native store.
//   - func UpgradeRollbackPreflight(currentStoreIdentity, candidateStoreFormat string,
//     retiredPaths []string) (found []string, err error)
//     returns every discovered retired path and fails the preflight when any
//     is found or when the identities do not match; a clean result returns
//     no error and no findings.

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/nodepackage"
	"github.com/LicoLand/BadTower/internal/stationcluster"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

const (
	nativeStoreIdentity       = "badtower.station-core.store.v2"
	nativeStoreSchema         = 4
	nativeClusterStoreVersion = 2
)

func TestNativeStoreIdentitySchemaAndClusterFloorAreExact(t *testing.T) {
	if stationcore.StoreIdentity != nativeStoreIdentity || stationcluster.StoreVersion != nativeClusterStoreVersion || nativeStoreSchema != 4 {
		t.Fatalf("native store contract identity=%q schema=%d clusterFloor=%d", stationcore.StoreIdentity, nativeStoreSchema, stationcluster.StoreVersion)
	}
	root := t.TempDir()
	core, err := stationcore.Open(stationcore.Config{DataRoot: root, ProfileID: stationcore.ProfileSingleNodeGeneralV1}, acceptanceClock{t: time.Date(2026, 8, 11, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest struct {
		Format                string                  `json:"format"`
		Identity              string                  `json:"identity"`
		Schema                int                     `json:"schema"`
		PebbleFormat          int                     `json:"pebbleFormat"`
		Mode                  stationcore.StorageMode `json:"mode"`
		LogicalPartitionCount uint16                  `json:"logicalPartitionCount"`
		StorageShardCount     uint16                  `json:"storageShardCount"`
		Partition             uint16                  `json:"partition"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&manifest) != nil || decoder.Decode(&struct{}{}) != io.EOF || manifest.Format != "badtower.station-core.root.v2" || manifest.Identity != nativeStoreIdentity || manifest.Schema != nativeStoreSchema || manifest.PebbleFormat <= 0 || manifest.Mode != stationcore.StorageModeInstanceSharded || manifest.LogicalPartitionCount != stationcluster.PartitionCount || manifest.StorageShardCount != 1 || manifest.Partition != 0 {
		t.Fatalf("native root manifest contract mismatch: identity=%q schema=%d mode=%q logicalPartitions=%d storageShards=%d partition=%d", manifest.Identity, manifest.Schema, manifest.Mode, manifest.LogicalPartitionCount, manifest.StorageShardCount, manifest.Partition)
	}
}

func TestPackageManifestBindsNativeStoreIdentity(t *testing.T) {
	t.Parallel()
	_, result := createSyntheticPackage(t)
	if result.Receipt.StationStoreFormat != nativeStoreIdentity {
		t.Fatalf("receipt stationStoreFormat = %q, want %q",
			result.Receipt.StationStoreFormat, nativeStoreIdentity)
	}
	if !strings.Contains(string(result.Manifest), `"stationStoreFormat":"`+nativeStoreIdentity+`"`) {
		t.Fatalf("manifest must bind the native store identity: %s", result.Manifest)
	}
}

func TestPackageDeterministicAcrossRepeatedCreate(t *testing.T) {
	t.Parallel()
	firstArchive, first := createSyntheticPackage(t)
	secondArchive, second := createSyntheticPackage(t)
	if !bytes.Equal(firstArchive, secondArchive) {
		t.Fatal("Create produced different archive bytes for identical inputs")
	}
	if !bytes.Equal(first.Manifest, second.Manifest) {
		t.Fatal("Create produced different manifest bytes for identical inputs")
	}
	if first.Receipt.Archive.SHA256 != second.Receipt.Archive.SHA256 {
		t.Fatalf("archive digests differ: %q vs %q",
			first.Receipt.Archive.SHA256, second.Receipt.Archive.SHA256)
	}
}

func TestBackupRestoreIsolatedOpenVerifiesCheckpoint(t *testing.T) {
	t.Parallel()
	clock := acceptanceClock{t: time.Date(2026, 7, 31, 0, 0, 0, 0, time.UTC)}
	core, err := stationcore.Open(stationcore.Config{
		DataRoot:  t.TempDir(),
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, clock)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = core.Close() }()
	namespace := "ns_aaaaaaaaaaaaaaaaaaaaaa"
	address := acceptanceReserve(t, core, namespace, "rq_backupReserve001", 3_600_000)
	unitID := acceptanceSubmit(t, core, namespace, "rq_backupSubmit001", address, []byte("backup me"))

	backupRoot := filepath.Join(t.TempDir(), "backup")
	digest, err := core.Backup(backupRoot)
	if err != nil {
		t.Fatalf("Backup: %v", err)
	}
	if len(digest) != 64 {
		t.Fatalf("backup digest = %q, want 64 hex chars", digest)
	}
	if _, err := hex.DecodeString(digest); err != nil {
		t.Fatalf("backup digest is not hex: %q", digest)
	}
	entries, err := os.ReadDir(backupRoot)
	if err != nil || len(entries) == 0 {
		t.Fatalf("backup destination must contain the copied store: %v", err)
	}

	// The live root keeps serving while the backup exists.
	claim, err := core.Claim(context.Background(), stationcore.ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_backupClaim001",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil || len(claim.Units) != 1 || claim.Units[0].UnitID != unitID {
		t.Fatalf("live core disturbed by backup: err=%v claim=%#v", err, claim)
	}

	isolated := filepath.Join(t.TempDir(), "restore")
	if err := core.RestoreIsolated(backupRoot, isolated); err != nil {
		t.Fatalf("RestoreIsolated: %v", err)
	}
	restored, err := stationcore.Open(stationcore.Config{
		DataRoot:  isolated,
		ProfileID: stationcore.ProfileSingleNodeGeneralV1,
	}, clock)
	if err != nil {
		t.Fatalf("isolated restored store must open: %v", err)
	}
	defer func() { _ = restored.Close() }()
	modelClaim, err := restored.Claim(context.Background(), stationcore.ClaimInput{
		NamespaceID:         namespace,
		RequestID:           "rq_backupRestoreClaim",
		AddressID:           address,
		MaxUnits:            1,
		MaxBytes:            1 << 20,
		VisibilityTimeoutMs: 30_000,
	})
	if err != nil {
		t.Fatalf("model sampling on restored store: %v", err)
	}
	if len(modelClaim.Units) != 1 || modelClaim.Units[0].UnitID != unitID {
		t.Fatalf("restored store lost acknowledged state: %#v", modelClaim)
	}

	// Destructive targets must be rejected without touching the live root.
	if _, err := core.Backup(core.DataRoot()); err == nil {
		t.Fatal("Backup into the live data root must be rejected")
	}
	if _, err := core.Backup(filepath.Join(core.DataRoot(), "nested")); err == nil {
		t.Fatal("Backup inside the live data root must be rejected")
	}
	if err := core.RestoreIsolated(backupRoot, core.DataRoot()); err == nil {
		t.Fatal("RestoreIsolated into the live data root must be rejected")
	}
}

func TestUpgradeRollbackPreflightRejectsRetiredStore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	retiredPaths := []string{
		filepath.Join(dir, "data", "badtower.db"),
		filepath.Join(dir, "retired-station-store"),
	}
	found, err := nodepackage.UpgradeRollbackPreflight(
		nativeStoreIdentity, nativeStoreIdentity, retiredPaths)
	if err != nil {
		t.Fatalf("clean preflight must pass: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("clean preflight reported retired findings: %#v", found)
	}

	if _, err := nodepackage.UpgradeRollbackPreflight(
		"other-identity", nativeStoreIdentity, retiredPaths); err == nil {
		t.Fatal("identity mismatch must fail the preflight")
	}
	if _, err := nodepackage.UpgradeRollbackPreflight(
		nativeStoreIdentity, "other-format", retiredPaths); err == nil {
		t.Fatal("candidate format mismatch must fail the preflight")
	}

	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "data", "badtower.db"), []byte("retired"), 0o600); err != nil {
		t.Fatal(err)
	}
	found, err = nodepackage.UpgradeRollbackPreflight(
		nativeStoreIdentity, nativeStoreIdentity, retiredPaths)
	if err == nil {
		t.Fatal("preflight must refuse when a retired path exists")
	}
	if len(found) != 1 {
		t.Fatalf("preflight must report the exact retired finding: %#v", found)
	}
}

type acceptanceClock struct{ t time.Time }

func (clock acceptanceClock) NowUnixMs() int64 { return clock.t.UnixMilli() }

func (clock acceptanceClock) MonotonicNow() time.Duration { return 0 }

func acceptanceReserve(t *testing.T, core *stationcore.Core, namespace, requestID string, ttlMs uint64) string {
	t.Helper()
	out, err := core.Reserve(context.Background(), stationcore.ReserveInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		TTLMs:       ttlMs,
	})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if out.AddressID == "" {
		t.Fatalf("empty address: %#v", out)
	}
	return out.AddressID
}

func acceptanceSubmit(t *testing.T, core *stationcore.Core, namespace, requestID, address string, payload []byte) string {
	t.Helper()
	out, err := core.Submit(context.Background(), stationcore.SubmitInput{
		NamespaceID: namespace,
		RequestID:   requestID,
		AddressID:   address,
		Payload:     payload,
		TTLMs:       86_400_000,
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if out.UnitID == "" {
		t.Fatal("empty unit")
	}
	return out.UnitID
}

var (
	fixtureBinary  = []byte("#!/synthetic/badtower\n")
	fixtureLicense = []byte("Synthetic AGPL-3.0-or-later test license.\n")
)

func createSyntheticPackage(t *testing.T) ([]byte, nodepackage.CreateResult) {
	t.Helper()
	input := nodepackage.CreateInput{
		Version:  "0.3.0-native.1",
		Platform: "linux-amd64",
		Binary: nodepackage.Source{
			Reader: bytes.NewReader(fixtureBinary),
			Size:   int64(len(fixtureBinary)),
		},
		License: nodepackage.Source{
			Reader: bytes.NewReader(fixtureLicense),
			Size:   int64(len(fixtureLicense)),
		},
	}
	var archive bytes.Buffer
	result, err := nodepackage.Create(&archive, input)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return archive.Bytes(), result
}
