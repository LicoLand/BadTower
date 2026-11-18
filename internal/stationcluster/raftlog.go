package stationcluster

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const raftLogIdentity = "badtower.station-cluster.raft-log.v1"

type raftLogBinding struct {
	clusterID      string
	memberID       uint64
	groupID        uint64
	partition      uint16
	ownershipEpoch uint64
}

func (binding raftLogBinding) valid() bool {
	return binding.clusterID != "" && len(binding.clusterID) <= 128 && binding.memberID != 0 && binding.groupID != 0 && binding.partition < PartitionCount && binding.ownershipEpoch != 0
}

type raftLog struct {
	mu sync.Mutex
	db *pebble.DB
}

type recoveredRaft struct {
	storage   *raft.MemoryStorage
	applied   uint64
	confState *raftpb.ConfState
	snapshot  *raftpb.Snapshot
	fresh     bool
}

func openRaftLog(root string, binding raftLogBinding) (*raftLog, recoveredRaft, error) {
	if root == "" || !filepath.IsAbs(root) || !binding.valid() {
		return nil, recoveredRaft{}, ErrInvalidTopology
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, recoveredRaft{}, err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, recoveredRaft{}, err
	}
	if len(entries) > 0 {
		pebbleRoot := false
		for _, entry := range entries {
			name := entry.Name()
			if name == "CURRENT" || strings.HasPrefix(name, "MANIFEST") || strings.HasPrefix(name, "OPTIONS") || strings.HasSuffix(name, ".sst") {
				pebbleRoot = true
			}
		}
		if !pebbleRoot {
			return nil, recoveredRaft{}, ErrInvalidTopology
		}
	}
	db, err := pebble.Open(root, &pebble.Options{FormatMajorVersion: 13, Logger: clusterStorageLogger{}})
	if err != nil {
		return nil, recoveredRaft{}, err
	}
	log := &raftLog{db: db}
	identity, closer, err := db.Get([]byte("meta/identity"))
	fresh := err == pebble.ErrNotFound
	if fresh {
		batch := db.NewBatch()
		defer batch.Close()
		var rawMember [8]byte
		binary.BigEndian.PutUint64(rawMember[:], binding.memberID)
		var rawGroup [8]byte
		binary.BigEndian.PutUint64(rawGroup[:], binding.groupID)
		var rawPartition [2]byte
		binary.BigEndian.PutUint16(rawPartition[:], binding.partition)
		var rawEpoch [8]byte
		binary.BigEndian.PutUint64(rawEpoch[:], binding.ownershipEpoch)
		if err := batch.Set([]byte("meta/identity"), []byte(raftLogIdentity), nil); err != nil {
			_ = db.Close()
			return nil, recoveredRaft{}, err
		}
		if err := batch.Set([]byte("meta/cluster-id"), []byte(binding.clusterID), nil); err != nil {
			_ = db.Close()
			return nil, recoveredRaft{}, err
		}
		if err := batch.Set([]byte("meta/member-id"), rawMember[:], nil); err != nil {
			_ = db.Close()
			return nil, recoveredRaft{}, err
		}
		if err := batch.Set([]byte("meta/group-id"), rawGroup[:], nil); err != nil {
			_ = db.Close()
			return nil, recoveredRaft{}, err
		}
		if err := batch.Set([]byte("meta/partition"), rawPartition[:], nil); err != nil {
			_ = db.Close()
			return nil, recoveredRaft{}, err
		}
		if err := batch.Set([]byte("meta/ownership-epoch"), rawEpoch[:], nil); err != nil {
			_ = db.Close()
			return nil, recoveredRaft{}, err
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			_ = db.Close()
			return nil, recoveredRaft{}, err
		}
	} else if err != nil {
		_ = db.Close()
		return nil, recoveredRaft{}, err
	} else {
		valid := string(identity) == raftLogIdentity
		closer.Close()
		clusterID, clusterOK, clusterErr := readBoundValue(db, "meta/cluster-id")
		memberID, memberOK, memberErr := readBoundUint64(db, "meta/member-id")
		groupID, groupOK, groupErr := readBoundUint64(db, "meta/group-id")
		partition, partitionOK, partitionErr := readBoundUint16(db, "meta/partition")
		epoch, epochOK, epochErr := readBoundUint64(db, "meta/ownership-epoch")
		if !valid || clusterErr != nil || memberErr != nil || groupErr != nil || partitionErr != nil || epochErr != nil || !clusterOK || !memberOK || !groupOK || !partitionOK || !epochOK || string(clusterID) != binding.clusterID || memberID != binding.memberID || groupID != binding.groupID || partition != binding.partition || epoch != binding.ownershipEpoch {
			_ = db.Close()
			return nil, recoveredRaft{}, ErrInvalidTopology
		}
	}
	recovered, err := log.recover(fresh)
	if err != nil {
		_ = db.Close()
		return nil, recoveredRaft{}, err
	}
	return log, recovered, nil
}

func readBoundUint16(db *pebble.DB, key string) (uint16, bool, error) {
	raw, ok, err := readBoundValue(db, key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(raw) != 2 {
		return 0, false, ErrInvalidTopology
	}
	return binary.BigEndian.Uint16(raw), true, nil
}

func readBoundValue(db *pebble.DB, key string) ([]byte, bool, error) {
	raw, closer, err := db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), raw...), true, nil
}

func readBoundUint64(db *pebble.DB, key string) (uint64, bool, error) {
	raw, ok, err := readBoundValue(db, key)
	if err != nil || !ok {
		return 0, ok, err
	}
	if len(raw) != 8 {
		return 0, false, ErrInvalidTopology
	}
	return binary.BigEndian.Uint64(raw), true, nil
}

func (log *raftLog) recover(fresh bool) (recoveredRaft, error) {
	storage := raft.NewMemoryStorage()
	confState := &raftpb.ConfState{}
	var applied uint64
	var appliedTerm uint64
	var snapshotIndex uint64
	var recoveredSnapshot *raftpb.Snapshot
	if raw, ok, err := log.read("meta/snapshot"); err != nil {
		return recoveredRaft{}, err
	} else if ok {
		var snapshot raftpb.Snapshot
		if proto.Unmarshal(raw, &snapshot) != nil || raft.IsEmptySnap(&snapshot) || len(snapshot.Data) == 0 {
			return recoveredRaft{}, ErrInvalidTopology
		}
		if err := storage.ApplySnapshot(&snapshot); err != nil {
			return recoveredRaft{}, err
		}
		snapshotIndex = snapshot.GetMetadata().GetIndex()
		applied = snapshot.GetMetadata().GetIndex()
		appliedTerm = snapshot.GetMetadata().GetTerm()
		if recoveredConfState := snapshot.GetMetadata().GetConfState(); recoveredConfState != nil {
			confState = proto.Clone(recoveredConfState).(*raftpb.ConfState)
		}
		recoveredSnapshot = proto.Clone(&snapshot).(*raftpb.Snapshot)
	}
	if raw, ok, err := log.read("meta/conf-state"); err != nil {
		return recoveredRaft{}, err
	} else if ok && recoveredSnapshot == nil && proto.Unmarshal(raw, confState) != nil {
		return recoveredRaft{}, ErrInvalidTopology
	}
	if raw, ok, err := log.read("meta/applied"); err != nil {
		return recoveredRaft{}, err
	} else if ok {
		if len(raw) != 8 {
			return recoveredRaft{}, ErrInvalidTopology
		}
		persistedApplied := binary.BigEndian.Uint64(raw)
		if persistedApplied < snapshotIndex {
			return recoveredRaft{}, ErrInvalidTopology
		}
		applied = persistedApplied
	}
	if raw, ok, err := log.read("meta/applied-term"); err != nil {
		return recoveredRaft{}, err
	} else if ok {
		if len(raw) != 8 {
			return recoveredRaft{}, ErrInvalidTopology
		}
		appliedTerm = binary.BigEndian.Uint64(raw)
	}
	if applied > 0 && appliedTerm == 0 {
		return recoveredRaft{}, ErrInvalidTopology
	}
	iterator, err := log.db.NewIter(&pebble.IterOptions{LowerBound: []byte("entry/"), UpperBound: []byte("entry0")})
	if err != nil {
		return recoveredRaft{}, err
	}
	var logEntries []*raftpb.Entry
	for iterator.First(); iterator.Valid(); iterator.Next() {
		var entry raftpb.Entry
		if proto.Unmarshal(iterator.Value(), &entry) != nil || entry.GetIndex() == 0 {
			_ = iterator.Close()
			return recoveredRaft{}, ErrInvalidTopology
		}
		if entry.GetIndex() <= snapshotIndex {
			continue
		}
		logEntries = append(logEntries, proto.Clone(&entry).(*raftpb.Entry))
	}
	if err := iterator.Close(); err != nil {
		return recoveredRaft{}, err
	}
	if len(logEntries) > 0 {
		if err := storage.Append(logEntries); err != nil {
			return recoveredRaft{}, err
		}
	}
	if snapshotIndex == 0 && applied > 0 && (len(logEntries) == 0 || logEntries[len(logEntries)-1].GetIndex() < applied) {
		return recoveredRaft{}, ErrInvalidTopology
	}
	if snapshotIndex == 0 && applied > 0 {
		// MemoryStorage needs the durable ConfState during RawNode bootstrap.
		// This in-memory marker is never persisted or compacted and therefore
		// cannot be selected instead of retained log entries for repair.
		if _, err := storage.CreateSnapshot(applied, confState, nil); err != nil {
			return recoveredRaft{}, err
		}
	}
	if raw, ok, err := log.read("meta/hard-state"); err != nil {
		return recoveredRaft{}, err
	} else if ok {
		var hardState raftpb.HardState
		if proto.Unmarshal(raw, &hardState) != nil || hardState.GetCommit() < applied {
			return recoveredRaft{}, ErrInvalidTopology
		}
		if err := storage.SetHardState(&hardState); err != nil {
			return recoveredRaft{}, err
		}
	}
	if recoveredSnapshot != nil {
		// OpenRaftMember installs the Core-bearing snapshot first, then asks
		// RawNode to replay the bounded committed tail.
		applied = snapshotIndex
	}
	return recoveredRaft{storage: storage, applied: applied, confState: confState, snapshot: recoveredSnapshot, fresh: fresh}, nil
}

func (log *raftLog) persist(ready raft.Ready) error {
	log.mu.Lock()
	defer log.mu.Unlock()
	batch := log.db.NewBatch()
	defer batch.Close()
	dirty := false
	if ready.Snapshot != nil && !raft.IsEmptySnap(ready.Snapshot) {
		if len(ready.Snapshot.Data) == 0 || ready.Snapshot.GetMetadata().GetIndex() == 0 || ready.Snapshot.GetMetadata().GetTerm() == 0 || ready.Snapshot.GetMetadata().GetConfState() == nil {
			return ErrInvalidSnapshot
		}
		dirty = true
		raw, err := proto.Marshal(ready.Snapshot)
		if err != nil {
			return err
		}
		if err := batch.Set([]byte("meta/snapshot"), raw, nil); err != nil {
			return err
		}
		if err := batch.DeleteRange([]byte("entry/"), entryKey(ready.Snapshot.GetMetadata().GetIndex()+1), nil); err != nil {
			return err
		}
		if err := setAppliedBatch(batch, ready.Snapshot.GetMetadata().GetIndex(), ready.Snapshot.GetMetadata().GetTerm(), ready.Snapshot.GetMetadata().GetConfState()); err != nil {
			return err
		}
	}
	if len(ready.Entries) > 0 {
		dirty = true
		first := ready.Entries[0].GetIndex()
		if err := batch.DeleteRange(entryKey(first), []byte("entry0"), nil); err != nil {
			return err
		}
		for _, entry := range ready.Entries {
			raw, err := proto.Marshal(entry)
			if err != nil {
				return err
			}
			if err := batch.Set(entryKey(entry.GetIndex()), raw, nil); err != nil {
				return err
			}
		}
	}
	if ready.HardState != nil && !raft.IsEmptyHardState(ready.HardState) {
		dirty = true
		raw, err := proto.Marshal(ready.HardState)
		if err != nil {
			return err
		}
		if err := batch.Set([]byte("meta/hard-state"), raw, nil); err != nil {
			return err
		}
	}
	if !dirty {
		return nil
	}
	return batch.Commit(pebble.Sync)
}

func (log *raftLog) markApplied(index, term uint64, confState *raftpb.ConfState) error {
	if confState == nil {
		return ErrInvalidTopology
	}
	if index > 0 && term == 0 {
		return ErrInvalidTopology
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	batch := log.db.NewBatch()
	defer batch.Close()
	if err := setAppliedBatch(batch, index, term, confState); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func setAppliedBatch(batch *pebble.Batch, index, term uint64, confState *raftpb.ConfState) error {
	var rawIndex [8]byte
	binary.BigEndian.PutUint64(rawIndex[:], index)
	if err := batch.Set([]byte("meta/applied"), rawIndex[:], nil); err != nil {
		return err
	}
	var rawTerm [8]byte
	binary.BigEndian.PutUint64(rawTerm[:], term)
	if err := batch.Set([]byte("meta/applied-term"), rawTerm[:], nil); err != nil {
		return err
	}
	rawConf, err := proto.Marshal(confState)
	if err != nil {
		return err
	}
	return batch.Set([]byte("meta/conf-state"), rawConf, nil)
}

func (log *raftLog) persistLocalSnapshot(snapshot *raftpb.Snapshot) error {
	if snapshot == nil || raft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 || snapshot.GetMetadata().GetConfState() == nil {
		return ErrInvalidSnapshot
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	raw, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	batch := log.db.NewBatch()
	defer batch.Close()
	if err := batch.Set([]byte("meta/snapshot"), raw, nil); err != nil {
		return err
	}
	if err := batch.DeleteRange([]byte("entry/"), entryKey(snapshot.GetMetadata().GetIndex()+1), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func (log *raftLog) entryCount() (int, error) {
	log.mu.Lock()
	defer log.mu.Unlock()
	iterator, err := log.db.NewIter(&pebble.IterOptions{LowerBound: []byte("entry/"), UpperBound: []byte("entry0")})
	if err != nil {
		return 0, err
	}
	defer iterator.Close()
	count := 0
	for iterator.First(); iterator.Valid(); iterator.Next() {
		count++
	}
	return count, iterator.Error()
}

func (log *raftLog) read(key string) ([]byte, bool, error) {
	raw, closer, err := log.db.Get([]byte(key))
	if err == pebble.ErrNotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	copyRaw := append([]byte(nil), raw...)
	closer.Close()
	return copyRaw, true, nil
}

func (log *raftLog) close() error {
	log.mu.Lock()
	defer log.mu.Unlock()
	return log.db.Close()
}

func entryKey(index uint64) []byte { return []byte(fmt.Sprintf("entry/%020d", index)) }

type clusterStorageLogger struct{}

func (clusterStorageLogger) Infof(string, ...interface{})  {}
func (clusterStorageLogger) Errorf(string, ...interface{}) {}
func (clusterStorageLogger) Fatalf(string, ...interface{}) { panic("CLUSTER_STORAGE_FATAL") }
