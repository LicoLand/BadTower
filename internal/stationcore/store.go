package stationcore

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

const (
	storeSchemaVersion = 4
	pebbleFormatMajor  = 13
)

const (
	metaShard = "shard"
	metaMode  = "mode"
)

// copyStoreTree recursively copies the closed schema-4 tree, syncing every
// regular file and directory. Symlinks and non-regular entries fail closed.
func copyStoreTree(source, destination string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid store entry")
		}
		if info.IsDir() {
			target := filepath.Join(destination, entry.Name())
			if err := os.Mkdir(target, 0o700); err != nil {
				return err
			}
			if err := copyStoreTree(filepath.Join(source, entry.Name()), target); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("invalid store entry")
		}
		if err := copyStoreFile(source, destination, entry.Name()); err != nil {
			return err
		}
	}
	return syncDir(destination)
}

func copyStoreFile(source, destination, name string) error {
	info, err := os.Stat(filepath.Join(source, name))
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("non-regular store file in copy")
	}
	input, err := os.Open(filepath.Join(source, name))
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(filepath.Join(destination, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := io.Copy(output, input); err != nil {
		_ = output.Close()
		return err
	}
	if err := output.Sync(); err != nil {
		_ = output.Close()
		return err
	}
	return output.Close()
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// storeTreeDigest returns one deterministic SHA-256 digest over every relative
// path, entry kind, and regular-file content in the recursive store tree.
func storeTreeDigest(root string) (string, error) {
	hash := sha256.New()
	if err := digestStoreDirectory(hash, root, ""); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func digestStoreDirectory(hash io.Writer, root, relative string) error {
	entries, err := os.ReadDir(filepath.Join(root, relative))
	if err != nil {
		return err
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid store entry")
		}
		name := filepath.ToSlash(filepath.Join(relative, entry.Name()))
		if info.IsDir() {
			hash.Write([]byte{'d', 0})
			hash.Write([]byte(name))
			hash.Write([]byte{0})
			if err := digestStoreDirectory(hash, root, name); err != nil {
				return err
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("invalid store entry")
		}
		hash.Write([]byte{'f', 0})
		hash.Write([]byte(name))
		hash.Write([]byte{0})
		file, err := os.Open(filepath.Join(root, filepath.FromSlash(name)))
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
	}
	return nil
}

func storeTreeBytes(root string, maximum uint64) (uint64, error) {
	var total uint64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		info, err := entry.Info()
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("invalid store entry")
		}
		if info.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() || info.Size() < 0 {
			return fmt.Errorf("invalid store entry")
		}
		size := uint64(info.Size())
		if size > maximum || total > maximum-size {
			return fail(ReasonCapacityExceeded, false, 0)
		}
		total += size
		return nil
	})
	return total, err
}

var retiredStorePaths = map[string]struct{}{
	"data/" + "badtower" + ".db":     {},
	"badtower" + ".station-store.v1": {},
}

type store struct {
	path          string
	mode          StorageMode
	partition     uint16
	active        []uint16
	shards        [NamespacePartitions]*storeShard
	opened        bool
	pool          *StoragePool
	ownsPool      bool
	mu            sync.RWMutex
	syncDelay     atomic.Int64
	syncCount     atomic.Uint64
	payloadWrites atomic.Uint64
	payloadBytes  atomic.Uint64
}

type storeShard struct {
	id      uint16
	db      *pebble.DB
	commits *groupCommitter
}

func openStore(dataRoot string) (*store, error) {
	return openConfiguredStore(Config{DataRoot: dataRoot, ProfileID: ProfileSingleNodeGeneralV1})
}

func openConfiguredStore(config Config) (*store, error) {
	dataRoot := config.DataRoot
	if _, retired := retiredStorePaths[dataRoot]; retired {
		return nil, fail(ReasonCorruptState, false, 0)
	}
	if strings.Contains(dataRoot, "badtower"+".station-store.v1") {
		return nil, fail(ReasonCorruptState, false, 0)
	}

	manifest, err := expectedStoreManifest(config.StorageMode, config.Partition)
	if err != nil {
		return nil, err
	}
	fresh, err := prepareStoreRoot(dataRoot, manifest)
	if err != nil {
		return nil, err
	}

	pool := config.StoragePool
	ownsPool := pool == nil
	if pool == nil {
		pool = NewStoragePool()
	}
	if !pool.attach() {
		return nil, fail(ReasonStorageUnavailable, true, 100)
	}
	st := &store{
		path: dataRoot, mode: manifest.Mode, partition: config.Partition,
		pool: pool, ownsPool: ownsPool,
	}
	st.active, err = manifestShardIDs(manifest)
	if err != nil {
		st.closeOpenedShards()
		return nil, err
	}
	for _, id := range st.active {
		root := filepath.Join(dataRoot, shardDirectoryName(id))
		inspection, inspectErr := inspectDataRoot(root)
		if inspectErr != nil || !inspection.allowOpen {
			st.closeOpenedShards()
			return nil, fail(ReasonCorruptState, false, 0)
		}
		db, openErr := pebble.Open(root, pool.options())
		if openErr != nil {
			st.closeOpenedShards()
			if inspection.nonEmpty {
				return nil, fail(ReasonCorruptState, false, 0)
			}
			return nil, fail(ReasonStorageUnavailable, true, 100)
		}
		if identityErr := ensureShardIdentity(db, inspection.nonEmpty, id, st.mode); identityErr != nil {
			_ = db.Close()
			st.closeOpenedShards()
			return nil, identityErr
		}
		shard := &storeShard{id: id, db: db}
		shard.commits = newGroupCommitter(db, pool)
		st.shards[id] = shard
	}
	if fresh {
		if err := writeStoreRootManifest(dataRoot, manifest); err != nil {
			st.closeOpenedShards()
			return nil, err
		}
	}
	st.opened = true
	return st, nil
}

func (st *store) closeOpenedShards() {
	for _, id := range st.active {
		shard := st.shards[id]
		if shard == nil {
			continue
		}
		if shard.commits != nil {
			shard.commits.close()
		}
		_ = shard.db.Close()
		st.shards[id] = nil
	}
	st.pool.detach()
	if st.ownsPool {
		_ = st.pool.Close()
	}
}

type dataRootInspection struct {
	nonEmpty  bool
	allowOpen bool
}

// inspectDataRoot classifies a data root before any store is opened. A
// directory containing only foreign files must never be opened or imported;
// a directory with Pebble content is safe to open and is then validated by
// the in-store identity marker. The lock file alone is not store content.
func inspectDataRoot(root string) (dataRootInspection, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return dataRootInspection{}, nil
		}
		return dataRootInspection{}, err
	}
	inspection := dataRootInspection{nonEmpty: len(entries) > 0, allowOpen: len(entries) == 0}
	for _, entry := range entries {
		name := entry.Name()
		if name == "CURRENT" || strings.HasPrefix(name, "MANIFEST") ||
			strings.HasPrefix(name, "OPTIONS") || strings.HasPrefix(name, "LOG") ||
			strings.HasPrefix(name, "MARKER") || strings.HasSuffix(name, ".sst") ||
			strings.HasSuffix(name, ".blob") {
			inspection.allowOpen = true
			break
		}
	}
	return inspection, nil
}

func ensureShardIdentity(db *pebble.DB, foreign bool, shardID uint16, mode StorageMode) error {
	identity, closer, err := db.Get(metaKey(metaIdentity))
	if err != nil && err != pebble.ErrNotFound {
		return fail(ReasonCorruptState, false, 0)
	}
	if err == pebble.ErrNotFound {
		if foreign {
			return fail(ReasonCorruptState, false, 0)
		}
		batch := db.NewBatch()
		batch.Set(metaKey(metaIdentity), []byte(StoreIdentity), nil)
		batch.Set(metaKey(metaSchema), []byte(strconv.Itoa(storeSchemaVersion)), nil)
		batch.Set(metaKey(metaFormat), []byte(strconv.Itoa(pebbleFormatMajor)), nil)
		batch.Set(metaKey(metaShard), []byte(strconv.Itoa(int(shardID))), nil)
		batch.Set(metaKey(metaMode), []byte(mode), nil)
		if err := batch.Commit(pebble.Sync); err != nil {
			return fail(ReasonStorageUnavailable, true, 100)
		}
		return nil
	}
	defer closer.Close()
	if string(identity) != StoreIdentity {
		return fail(ReasonCorruptState, false, 0)
	}
	schema, closer2, err := db.Get(metaKey(metaSchema))
	if err != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	defer closer2.Close()
	if string(schema) != strconv.Itoa(storeSchemaVersion) {
		return fail(ReasonCorruptState, false, 0)
	}
	format, closer3, err := db.Get(metaKey(metaFormat))
	if err != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	defer closer3.Close()
	if string(format) != strconv.Itoa(pebbleFormatMajor) {
		return fail(ReasonCorruptState, false, 0)
	}
	storedShard, closer4, err := db.Get(metaKey(metaShard))
	if err != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	defer closer4.Close()
	storedMode, closer5, err := db.Get(metaKey(metaMode))
	if err != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	defer closer5.Close()
	if string(storedShard) != strconv.Itoa(int(shardID)) || string(storedMode) != string(mode) {
		return fail(ReasonCorruptState, false, 0)
	}
	return nil
}

func (st *store) close() error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.closeResourcesLocked()
}

func (st *store) closeResourcesLocked() error {
	if !st.opened {
		return nil
	}
	var err error
	// Every admitted commit must be confirmed before the write-ahead log is
	// closed, so a committed but unconfirmed batch can never be reported as a
	// successful response and then be dropped.
	if drainErr := st.drainCommitsLocked(); drainErr != nil {
		err = drainErr
	}
	for _, id := range st.active {
		shard := st.shards[id]
		if shard == nil {
			continue
		}
		if shard.commits != nil {
			shard.commits.close()
			shard.commits = nil
		}
		if closeErr := shard.db.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
		shard.db = nil
		st.shards[id] = nil
	}
	st.opened = false
	st.pool.detach()
	if st.ownsPool {
		if poolErr := st.pool.Close(); err == nil {
			err = poolErr
		}
	}
	return err
}

func (st *store) adoptLocked(source *store) {
	st.path, st.mode, st.partition = source.path, source.mode, source.partition
	st.active = source.active
	st.shards = source.shards
	st.opened = source.opened
	st.pool, st.ownsPool = source.pool, source.ownsPool
	source.active = nil
	source.shards = [NamespacePartitions]*storeShard{}
	source.opened = false
	source.pool = nil
	source.ownsPool = false
}

// checkpointTo creates one flushed-WAL checkpoint in a fresh destination
// directory. The caller copies the checkpoint to an independent destination;
// the checkpoint itself is not a backup.
func (st *store) checkpointTo(dest string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.opened {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	// A checkpoint must contain every commit that was admitted before it, so
	// the outstanding durability window is confirmed first.
	if err := st.drainCommitsLocked(); err != nil {
		return err
	}
	if err := os.Mkdir(dest, 0o700); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	ok := false
	defer func() {
		if !ok {
			_ = os.RemoveAll(dest)
		}
	}()
	for _, id := range st.active {
		shard := st.shards[id]
		if shard == nil || shard.db == nil || shard.db.Checkpoint(filepath.Join(dest, shardDirectoryName(id)), pebble.WithFlushedWAL()) != nil {
			return fail(ReasonStorageUnavailable, true, 100)
		}
	}
	manifest, err := expectedStoreManifest(st.mode, st.partition)
	if err != nil || writeStoreRootManifest(dest, manifest) != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	ok = true
	return nil
}

// verifyStoreRoot opens one copied store root, verifies the exact
// identity/schema/format markers, performs a Pebble level check, and closes
// it without touching any other root.
func verifyStoreRoot(root string) error {
	actual, err := readStoreRootManifest(filepath.Join(root, storeRootManifestName))
	if err != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	partition := actual.Partition
	verified, err := openConfiguredStore(Config{DataRoot: root, ProfileID: ProfileSingleNodeGeneralV1, StorageMode: actual.Mode, Partition: partition})
	if err != nil {
		return fail(ReasonCorruptState, false, 0)
	}
	defer verified.close()
	for _, id := range verified.active {
		if err := verified.shards[id].db.CheckLevels(&pebble.CheckLevelsStats{}); err != nil {
			return fail(ReasonCorruptState, false, 0)
		}
	}
	return nil
}

type coreStorageLogger struct{}

func (coreStorageLogger) Infof(string, ...interface{})  {}
func (coreStorageLogger) Errorf(string, ...interface{}) {}
func (coreStorageLogger) Fatalf(string, ...interface{}) { panic("STATION_STORAGE_FATAL") }

func (st *store) setSyncDelay(delay time.Duration) {
	if delay <= 0 {
		st.syncDelay.Store(0)
		return
	}
	st.syncDelay.Store(int64(delay))
}

func (st *store) physicalSyncs() uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var total uint64
	for _, id := range st.active {
		if shard := st.shards[id]; shard != nil && shard.commits != nil {
			total += shard.commits.physicalSyncs.Load()
		}
	}
	return total
}

func (st *store) maximumGroupSize() uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()
	var maximum uint64
	for _, id := range st.active {
		if shard := st.shards[id]; shard != nil && shard.commits != nil {
			if size := shard.commits.maxGroupSize.Load(); size > maximum {
				maximum = size
			}
		}
	}
	return maximum
}

func (st *store) syncBatch(set func(batch *pebble.Batch, db *pebble.DB) error) error {
	return st.syncBatchAt(0, 0, set)
}

// syncBatchAt atomically enqueues one bounded mutation batch into the group
// committer and returns once the batch is applied to the memtable and the
// write-ahead log, which makes it visible to every later read. The batch is
// not yet durable: the caller that owns the mutation confirms durability with
// durabilityForNamespace before it acknowledges anything.
//
// The lifecycle read lock keeps close and checkpoints mutually exclusive with
// the apply. Those paths drain outstanding commits under the lifecycle write
// lock before they copy or close the store, so a checkpoint can never capture
// a state that omits an unconfirmed commit.
//
// The wall-clock watermark is folded into the same atomic batch using one
// fixed slot per mutation lane. A slot is monotonic because its lane is
// serialized; restart takes the maximum across all slots, so cross-lane commit
// order cannot regress the recovered watermark.
func (st *store) syncBatchAt(watermark int64, watermarkSlot int, set func(batch *pebble.Batch, db *pebble.DB) error) error {
	return st.syncPartitionBatchAt(uint16(watermarkSlot%NamespacePartitions), watermark, watermarkSlot, set)
}

func (st *store) syncNamespaceBatchAt(namespace string, watermark int64, watermarkSlot int, set func(batch *pebble.Batch, db *pebble.DB) error) error {
	return st.syncPartitionBatchAt(PartitionFor(namespace), watermark, watermarkSlot, set)
}

func (st *store) syncPartitionBatchAt(partition uint16, watermark int64, watermarkSlot int, set func(batch *pebble.Batch, db *pebble.DB) error) error {
	st.mu.RLock()
	defer st.mu.RUnlock()
	shard, err := st.shardForPartitionLocked(partition)
	if err != nil || shard.db == nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	batch := shard.db.NewBatch()
	if err := set(batch, shard.db); err != nil {
		_ = batch.Close()
		return err
	}
	if delay := st.syncDelay.Load(); delay > 0 {
		time.Sleep(time.Duration(delay))
	}

	if watermark > 0 {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(watermark))
		if err := batch.Set(watermarkKey(watermarkSlot), buf, nil); err != nil {
			_ = batch.Close()
			return fail(ReasonStorageUnavailable, true, 100)
		}
	}
	if _, err := shard.commits.submit(batch); err != nil {
		return fail(ReasonStorageUnavailable, true, 100)
	}
	st.syncCount.Add(1)
	return nil
}

// durabilityForNamespace returns the ticket whose barrier sync must complete
// before a response derived from the current state of namespace may be
// acknowledged. Reads and writes of one namespace only ever execute on that
// namespace's mutation lane, so a ticket read after a lane job completed
// covers every commit that job depends on: its own commit, and any earlier
// commit whose effect that job observed.
func (st *store) durabilityForNamespace(namespace string) durabilityTicket {
	shard, err := st.shardForNamespace(namespace)
	if err != nil || shard == nil || shard.commits == nil {
		return durabilityTicket{}
	}
	return durabilityTicket{committer: shard.commits, ticket: shard.commits.submitted.Load()}
}

// drainCommitsLocked confirms durability for every submitted batch of every
// open shard. Callers hold the lifecycle write lock, which blocks new
// admission, so the drain observes a closed window.
func (st *store) drainCommitsLocked() error {
	for _, id := range st.active {
		shard := st.shards[id]
		if shard == nil || shard.commits == nil {
			continue
		}
		if err := shard.commits.drain(); err != nil {
			return fail(ReasonStorageUnavailable, true, 100)
		}
	}
	return nil
}

func (st *store) get(key []byte) ([]byte, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	shard, err := st.shardForKeyLocked(key)
	if err != nil || shard.db == nil {
		return nil, fail(ReasonStorageUnavailable, true, 100)
	}
	value, closer, err := shard.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, pebble.ErrNotFound
		}
		return nil, fail(ReasonCorruptState, false, 0)
	}
	out := append([]byte(nil), value...)
	closer.Close()
	return out, nil
}

func (st *store) loadWatermark() (int64, error) {
	prefix := watermarkPrefix()
	iter, err := st.newIter(prefix)
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	var maximum int64
	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) != len(prefix)+1 || int(iter.Key()[len(prefix)]) >= mutationLanes || len(iter.Value()) != 8 {
			return 0, fail(ReasonCorruptState, false, 0)
		}
		value := int64(binary.BigEndian.Uint64(iter.Value()))
		if value > maximum {
			maximum = value
		}
	}
	if err := iter.Error(); err != nil {
		return 0, fail(ReasonCorruptState, false, 0)
	}
	return maximum, nil
}

func (st *store) newIter(prefix []byte) (*storeIterator, error) {
	st.mu.RLock()
	if !st.opened {
		st.mu.RUnlock()
		return nil, fail(ReasonStorageUnavailable, true, 100)
	}
	ids := st.active
	if partition, ok := partitionFromKey(prefix); ok {
		ids = []uint16{partition}
	}
	opts := &pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)}
	result := &storeIterator{store: st, current: -1}
	for _, id := range ids {
		shard, err := st.shardForPartitionLocked(id)
		if err != nil || shard.db == nil {
			_ = result.Close()
			return nil, fail(ReasonStorageUnavailable, true, 100)
		}
		pebbleIter, err := shard.db.NewIter(opts)
		if err != nil {
			_ = result.Close()
			return nil, fail(ReasonCorruptState, false, 0)
		}
		result.iters = append(result.iters, pebbleIter)
	}
	return result, nil
}

func (st *store) shardForPartitionLocked(partition uint16) (*storeShard, error) {
	if partition >= NamespacePartitions || st.mode == StorageModePartitionOne && partition != st.partition {
		return nil, fail(ReasonInvalidRequest, false, 0)
	}
	storageShard := st.storageShardID(partition)
	shard := st.shards[storageShard]
	if shard == nil {
		return nil, fail(ReasonCorruptState, false, 0)
	}
	return shard, nil
}

func (st *store) storageShardID(partition uint16) uint16 {
	if st.mode == StorageModePartitionOne {
		return partition
	}
	return partition % instanceStorageShards
}

func (st *store) shardForNamespace(namespace string) (*storeShard, error) {
	st.mu.RLock()
	defer st.mu.RUnlock()
	return st.shardForPartitionLocked(PartitionFor(namespace))
}

func (st *store) shardForKeyLocked(key []byte) (*storeShard, error) {
	partition, ok := partitionFromKey(key)
	if !ok {
		return nil, fail(ReasonCorruptState, false, 0)
	}
	return st.shardForPartitionLocked(partition)
}

func partitionFromKey(key []byte) (uint16, bool) {
	if len(key) < 1+idTotalLen || key[0] == keyMeta {
		return 0, false
	}
	namespace := string(key[1 : 1+idTotalLen])
	if !validNamespaceShape(namespace) {
		return 0, false
	}
	return PartitionFor(namespace), true
}

func prefixUpperBound(prefix []byte) []byte {
	end := append([]byte(nil), prefix...)
	for i := len(end) - 1; i >= 0; i-- {
		end[i]++
		if end[i] != 0 {
			return end
		}
	}
	return nil
}

func encodeAddressRecord(expiresAt, nextSeq, nextEpoch int64, liveUnits uint64) []byte {
	buf := make([]byte, 32)
	binary.BigEndian.PutUint64(buf[0:], uint64(expiresAt))
	binary.BigEndian.PutUint64(buf[8:], uint64(nextSeq))
	binary.BigEndian.PutUint64(buf[16:], uint64(nextEpoch))
	binary.BigEndian.PutUint64(buf[24:], liveUnits)
	return buf
}

func decodeAddressRecord(raw []byte) (expiresAt, nextSeq, nextEpoch int64, liveUnits uint64, err error) {
	if len(raw) < 24 {
		return 0, 0, 0, 0, fail(ReasonCorruptState, false, 0)
	}
	expiresAt = int64(binary.BigEndian.Uint64(raw[0:8]))
	nextSeq = int64(binary.BigEndian.Uint64(raw[8:16]))
	nextEpoch = int64(binary.BigEndian.Uint64(raw[16:24]))
	if len(raw) >= 32 {
		liveUnits = binary.BigEndian.Uint64(raw[24:32])
	}
	return expiresAt, nextSeq, nextEpoch, liveUnits, nil
}

func encodeUnitMetadata(acceptedAt, expiresAt, availableAt, seq int64, attempt uint32) []byte {
	metadata := make([]byte, 36)
	binary.BigEndian.PutUint64(metadata[0:], uint64(acceptedAt))
	binary.BigEndian.PutUint64(metadata[8:], uint64(expiresAt))
	binary.BigEndian.PutUint64(metadata[16:], uint64(availableAt))
	binary.BigEndian.PutUint64(metadata[24:], uint64(seq))
	binary.BigEndian.PutUint32(metadata[32:], attempt)
	return metadata
}

func decodeUnitMetadata(raw []byte) (acceptedAt, expiresAt, availableAt, seq int64, attempt uint32, err error) {
	if len(raw) != 36 {
		return 0, 0, 0, 0, 0, fail(ReasonCorruptState, false, 0)
	}
	acceptedAt = int64(binary.BigEndian.Uint64(raw[0:8]))
	expiresAt = int64(binary.BigEndian.Uint64(raw[8:16]))
	availableAt = int64(binary.BigEndian.Uint64(raw[16:24]))
	seq = int64(binary.BigEndian.Uint64(raw[24:32]))
	attempt = binary.BigEndian.Uint32(raw[32:36])
	return acceptedAt, expiresAt, availableAt, seq, attempt, nil
}

const (
	settlementReasonExpired      byte = 1
	settlementReasonAttemptLimit byte = 2
)

func encodeSettlementRecord(reason byte, effectiveAt int64) []byte {
	buf := make([]byte, 9)
	buf[0] = reason
	binary.BigEndian.PutUint64(buf[1:], uint64(effectiveAt))
	return buf
}

func decodeSettlementRecord(raw []byte) (reason byte, effectiveAt int64, err error) {
	if len(raw) < 9 {
		return 0, 0, fail(ReasonCorruptState, false, 0)
	}
	return raw[0], int64(binary.BigEndian.Uint64(raw[1:9])), nil
}

func encodeClaimRecord(epoch uint64, expiresAt int64, request string, units []string) []byte {
	buf := make([]byte, 8+8+2+len(request)+4+len(units)*idTotalLen)
	binary.BigEndian.PutUint64(buf[0:], epoch)
	binary.BigEndian.PutUint64(buf[8:], uint64(expiresAt))
	binary.BigEndian.PutUint16(buf[16:], uint16(len(request)))
	off := 18
	off += copy(buf[off:], request)
	binary.BigEndian.PutUint32(buf[off:], uint32(len(units)))
	off += 4
	for _, unit := range units {
		off += copy(buf[off:], []byte(unit))
	}
	return buf
}

func decodeClaimRecord(raw []byte) (epoch uint64, expiresAt int64, request string, units []string, err error) {
	if len(raw) < 22 {
		return 0, 0, "", nil, fail(ReasonCorruptState, false, 0)
	}
	epoch = binary.BigEndian.Uint64(raw[0:8])
	expiresAt = int64(binary.BigEndian.Uint64(raw[8:16]))
	requestLength := int(binary.BigEndian.Uint16(raw[16:18]))
	off := 18
	if requestLength <= 0 || requestLength > 128 || off+requestLength+4 > len(raw) {
		return 0, 0, "", nil, fail(ReasonCorruptState, false, 0)
	}
	request = string(raw[off : off+requestLength])
	off += requestLength
	count := binary.BigEndian.Uint32(raw[off : off+4])
	off += 4
	units = make([]string, 0, count)
	for i := uint32(0); i < count; i++ {
		if off+idTotalLen > len(raw) {
			return 0, 0, "", nil, fail(ReasonCorruptState, false, 0)
		}
		units = append(units, string(raw[off:off+idTotalLen]))
		off += idTotalLen
	}
	if off != len(raw) {
		return 0, 0, "", nil, fail(ReasonCorruptState, false, 0)
	}
	return epoch, expiresAt, request, units, nil
}

func encodeIdempotencyRecord(fingerprint, response []byte, expiresAt int64) []byte {
	buf := make([]byte, 8+4+len(fingerprint)+len(response))
	binary.BigEndian.PutUint64(buf[0:], uint64(expiresAt))
	binary.BigEndian.PutUint32(buf[8:], uint32(len(fingerprint)))
	off := 12
	off += copy(buf[off:], fingerprint)
	copy(buf[off:], response)
	return buf
}

func decodeIdempotencyRecord(raw []byte) (fingerprint, response []byte, expiresAt int64, err error) {
	if len(raw) < 12 {
		return nil, nil, 0, fail(ReasonCorruptState, false, 0)
	}
	expiresAt = int64(binary.BigEndian.Uint64(raw[0:8]))
	fpLen := binary.BigEndian.Uint32(raw[8:12])
	if int(12+fpLen) > len(raw) {
		return nil, nil, 0, fail(ReasonCorruptState, false, 0)
	}
	// The caller owns raw for the lifetime of both views.
	fingerprint = raw[12 : 12+fpLen]
	response = raw[12+fpLen:]
	return fingerprint, response, expiresAt, nil
}

func encodeNamespaceCounters(counters namespaceCounters) []byte {
	buf := make([]byte, 40)
	binary.BigEndian.PutUint64(buf[0:], counters.Addresses)
	binary.BigEndian.PutUint64(buf[8:], counters.Units)
	binary.BigEndian.PutUint64(buf[16:], counters.RetainedBytes)
	binary.BigEndian.PutUint64(buf[24:], counters.ActiveClaims)
	binary.BigEndian.PutUint64(buf[32:], counters.IdempotencyRecords)
	return buf
}

func decodeNamespaceCounters(raw []byte) (namespaceCounters, error) {
	if len(raw) < 40 {
		return namespaceCounters{}, fail(ReasonCorruptState, false, 0)
	}
	return namespaceCounters{
		Addresses:          binary.BigEndian.Uint64(raw[0:8]),
		Units:              binary.BigEndian.Uint64(raw[8:16]),
		RetainedBytes:      binary.BigEndian.Uint64(raw[16:24]),
		ActiveClaims:       binary.BigEndian.Uint64(raw[24:32]),
		IdempotencyRecords: binary.BigEndian.Uint64(raw[32:40]),
	}, nil
}
