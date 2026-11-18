package stationcore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	storeRootManifestName = "manifest.json"
	storeRootFormat       = "badtower.station-core.root.v2"
	instanceStorageShards = 1
)

type storeRootManifest struct {
	Format                string      `json:"format"`
	Identity              string      `json:"identity"`
	Schema                int         `json:"schema"`
	PebbleFormat          int         `json:"pebbleFormat"`
	Mode                  StorageMode `json:"mode"`
	LogicalPartitionCount uint16      `json:"logicalPartitionCount"`
	StorageShardCount     uint16      `json:"storageShardCount"`
	Partition             uint16      `json:"partition"`
}

func normalizedStorageMode(mode StorageMode) StorageMode {
	if mode == "" {
		return StorageModeInstanceSharded
	}
	return mode
}

func configuredShardIDs(mode StorageMode, partition uint16) ([]uint16, error) {
	switch normalizedStorageMode(mode) {
	case StorageModeInstanceSharded:
		if partition != 0 {
			return nil, fail(ReasonInvalidRequest, false, 0)
		}
		ids := make([]uint16, instanceStorageShards)
		for id := range ids {
			ids[id] = uint16(id)
		}
		return ids, nil
	case StorageModePartitionOne:
		if partition >= NamespacePartitions {
			return nil, fail(ReasonInvalidRequest, false, 0)
		}
		return []uint16{partition}, nil
	default:
		return nil, fail(ReasonInvalidRequest, false, 0)
	}
}

func expectedStoreManifest(mode StorageMode, partition uint16) (storeRootManifest, error) {
	ids, err := configuredShardIDs(mode, partition)
	if err != nil {
		return storeRootManifest{}, err
	}
	return storeRootManifest{
		Format: storeRootFormat, Identity: StoreIdentity, Schema: storeSchemaVersion,
		PebbleFormat: pebbleFormatMajor, Mode: normalizedStorageMode(mode),
		LogicalPartitionCount: NamespacePartitions, StorageShardCount: uint16(len(ids)), Partition: partition,
	}, nil
}

func manifestShardIDs(manifest storeRootManifest) ([]uint16, error) {
	if manifest.LogicalPartitionCount != NamespacePartitions || manifest.StorageShardCount == 0 {
		return nil, fail(ReasonCorruptState, false, 0)
	}
	ids, err := configuredShardIDs(manifest.Mode, manifest.Partition)
	if err != nil || uint16(len(ids)) != manifest.StorageShardCount {
		return nil, fail(ReasonCorruptState, false, 0)
	}
	return ids, nil
}

func shardDirectoryName(id uint16) string {
	return fmt.Sprintf("shard-%02d", id)
}

func prepareStoreRoot(root string, expected storeRootManifest) (bool, error) {
	if root == "" {
		return false, fail(ReasonInvalidRequest, false, 0)
	}
	info, err := os.Lstat(root)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(root, 0o700); err != nil {
			return false, fail(ReasonStorageUnavailable, true, 100)
		}
		info, err = os.Lstat(root)
	}
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, fail(ReasonCorruptState, false, 0)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, fail(ReasonStorageUnavailable, true, 100)
	}
	if len(entries) == 0 {
		if err := initializeStoreRoot(root, expected); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := verifyStoreRootManifest(root, entries, expected); err != nil {
		return false, err
	}
	return false, nil
}

func initializeStoreRoot(root string, manifest storeRootManifest) error {
	ids, err := manifestShardIDs(manifest)
	if err != nil {
		return err
	}
	created := make([]string, 0, len(ids))
	for _, id := range ids {
		path := filepath.Join(root, shardDirectoryName(id))
		if err := os.Mkdir(path, 0o700); err != nil {
			for _, createdPath := range created {
				_ = os.Remove(createdPath)
			}
			return fail(ReasonStorageUnavailable, true, 100)
		}
		created = append(created, path)
	}
	return syncDir(root)
}

func writeStoreRootManifest(root string, manifest storeRootManifest) error {
	raw, err := json.Marshal(manifest)
	if err != nil {
		return fail(ReasonInternal, false, 0)
	}
	file, err := os.OpenFile(filepath.Join(root, storeRootManifestName), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	if _, err = file.Write(raw); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil || syncDir(root) != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	return nil
}

func verifyStoreRootManifest(root string, entries []os.DirEntry, expected storeRootManifest) error {
	ids, err := manifestShardIDs(expected)
	if err != nil || len(entries) != len(ids)+1 {
		return fail(ReasonCorruptState, false, 0)
	}
	allowed := map[string]uint16{}
	for _, id := range ids {
		allowed[shardDirectoryName(id)] = id
	}
	manifestSeen := false
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return fail(ReasonCorruptState, false, 0)
		}
		if entry.Name() == storeRootManifestName {
			if !info.Mode().IsRegular() {
				return fail(ReasonCorruptState, false, 0)
			}
			manifestSeen = true
			continue
		}
		if _, ok := allowed[entry.Name()]; !ok || !info.IsDir() {
			return fail(ReasonCorruptState, false, 0)
		}
	}
	if !manifestSeen {
		return fail(ReasonCorruptState, false, 0)
	}
	actual, err := readStoreRootManifest(filepath.Join(root, storeRootManifestName))
	if err != nil || !equalStoreManifest(actual, expected) {
		return fail(ReasonCorruptState, false, 0)
	}
	return nil
}

func readStoreRootManifest(path string) (storeRootManifest, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return storeRootManifest{}, err
	}
	if len(raw) == 0 || len(raw) > 4096 || hasDuplicateTopLevelJSONKey(raw) {
		return storeRootManifest{}, fail(ReasonCorruptState, false, 0)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var manifest storeRootManifest
	if err := decoder.Decode(&manifest); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return storeRootManifest{}, fail(ReasonCorruptState, false, 0)
	}
	return manifest, nil
}

func hasDuplicateTopLevelJSONKey(raw []byte) bool {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return true
	}
	seen := make(map[string]struct{}, 6)
	for decoder.More() {
		token, err := decoder.Token()
		key, ok := token.(string)
		if err != nil || !ok {
			return true
		}
		if _, duplicate := seen[key]; duplicate {
			return true
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return true
		}
	}
	_, err = decoder.Token()
	return err != nil
}

func equalStoreManifest(left, right storeRootManifest) bool {
	if left.Format != right.Format || left.Identity != right.Identity || left.Schema != right.Schema ||
		left.PebbleFormat != right.PebbleFormat || left.Mode != right.Mode ||
		left.LogicalPartitionCount != right.LogicalPartitionCount || left.StorageShardCount != right.StorageShardCount ||
		left.Partition != right.Partition {
		return false
	}
	return true
}
