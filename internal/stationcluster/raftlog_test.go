package stationcluster

import (
	"testing"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestRaftLogPersistsBeforeRecoverAndRejectsForeignRoot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	binding := raftLogBinding{clusterID: "cluster-test", memberID: 1, groupID: 1, partition: 0, ownershipEpoch: 1}
	log, recovered, err := openRaftLog(root, binding)
	if err != nil || !recovered.fresh {
		t.Fatalf("open fresh: fresh=%v err=%v", recovered.fresh, err)
	}
	term, vote, commit := uint64(1), uint64(1), uint64(1)
	index, entryTerm := uint64(1), uint64(1)
	entryType := raftpb.EntryType_EntryNormal
	data := []byte("command")
	ready := raft.Ready{
		HardState: &raftpb.HardState{Term: &term, Vote: &vote, Commit: &commit},
		Entries:   []*raftpb.Entry{{Term: &entryTerm, Index: &index, Type: &entryType, Data: data}},
	}
	if err := log.persist(ready); err != nil {
		t.Fatal(err)
	}
	confState := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	if err := log.markApplied(0, 0, confState); err != nil {
		t.Fatal(err)
	}
	if err := log.close(); err != nil {
		t.Fatal(err)
	}
	log, recovered, err = openRaftLog(root, binding)
	if err != nil || recovered.fresh {
		t.Fatalf("reopen: fresh=%v err=%v", recovered.fresh, err)
	}
	last, err := recovered.storage.LastIndex()
	if err != nil || last != 1 {
		t.Fatalf("last index=%d err=%v", last, err)
	}
	_ = log.close()
}

func TestRaftLogPersistsSnapshot(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	binding := raftLogBinding{clusterID: "cluster-test", memberID: 1, groupID: 1, partition: 0, ownershipEpoch: 1}
	log, _, err := openRaftLog(root, binding)
	if err != nil {
		t.Fatal(err)
	}
	index, term := uint64(8), uint64(3)
	confState := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	ready := raft.Ready{Snapshot: &raftpb.Snapshot{Data: []byte("bounded-core-snapshot"), Metadata: &raftpb.SnapshotMetadata{
		Index: &index, Term: &term, ConfState: confState,
	}}}
	if err := log.persist(ready); err != nil {
		t.Fatal(err)
	}
	if err := log.markApplied(index, term, confState); err != nil {
		t.Fatal(err)
	}
	if err := log.close(); err != nil {
		t.Fatal(err)
	}
	log, recovered, err := openRaftLog(root, binding)
	if err != nil {
		t.Fatal(err)
	}
	defer log.close()
	first, err := recovered.storage.FirstIndex()
	if err != nil || first != index+1 || recovered.applied != index {
		t.Fatalf("first=%d applied=%d err=%v", first, recovered.applied, err)
	}
}

func TestRaftLogRejectsRootBoundToAnotherIdentity(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	binding := raftLogBinding{clusterID: "cluster-a", memberID: 1, groupID: 1, partition: 0, ownershipEpoch: 1}
	log, _, err := openRaftLog(root, binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.close(); err != nil {
		t.Fatal(err)
	}
	for _, foreign := range []raftLogBinding{
		{clusterID: "cluster-b", memberID: 1, groupID: 1, partition: 0, ownershipEpoch: 1},
		{clusterID: "cluster-a", memberID: 2, groupID: 1, partition: 0, ownershipEpoch: 1},
		{clusterID: "cluster-a", memberID: 1, groupID: 2, partition: 0, ownershipEpoch: 1},
		{clusterID: "cluster-a", memberID: 1, groupID: 1, partition: 1, ownershipEpoch: 1},
		{clusterID: "cluster-a", memberID: 1, groupID: 1, partition: 0, ownershipEpoch: 2},
	} {
		if foreignLog, _, openErr := openRaftLog(root, foreign); openErr != ErrInvalidTopology {
			if foreignLog != nil {
				_ = foreignLog.close()
			}
			t.Fatalf("foreign binding accepted: err=%v", openErr)
		}
	}
}
