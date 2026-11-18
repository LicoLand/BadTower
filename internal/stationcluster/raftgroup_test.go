package stationcluster

import (
	"bytes"
	"errors"
	"testing"
)

func TestRaftGroupCommitsIdenticalEntryOnQuorum(t *testing.T) {
	t.Parallel()
	group, err := NewRaftGroup([]uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("opaque-command")
	index, err := group.Propose(payload)
	if err != nil || index == 0 {
		t.Fatalf("Propose: index=%d err=%v", index, err)
	}
	for _, id := range []uint64{1, 2, 3} {
		entries := group.Committed(id)
		if len(entries) != 1 || !bytes.Equal(entries[0].Data, payload) {
			t.Fatalf("member %d committed %#v", id, entries)
		}
	}
}

func TestRaftGroupFailsClosedWithoutQuorum(t *testing.T) {
	t.Parallel()
	group, err := NewRaftGroup([]uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	leader := group.Leader()
	if err := group.SetActive(leader, false); err != nil {
		t.Fatal(err)
	}
	var second uint64
	for _, id := range []uint64{1, 2, 3} {
		if id != leader {
			second = id
			break
		}
	}
	if err := group.SetActive(second, false); err != nil {
		t.Fatal(err)
	}
	if _, err := group.Propose([]byte("must-not-commit")); !errors.Is(err, ErrQuorumUnavailable) {
		t.Fatalf("minority proposal err=%v", err)
	}
}

func TestRaftGroupElectsNewLeaderAfterOneFailure(t *testing.T) {
	t.Parallel()
	group, err := NewRaftGroup([]uint64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	oldLeader := group.Leader()
	if err := group.SetActive(oldLeader, false); err != nil {
		t.Fatal(err)
	}
	if _, err := group.Propose([]byte("after-failover")); err != nil {
		t.Fatal(err)
	}
	if group.Leader() == 0 || group.Leader() == oldLeader {
		t.Fatalf("leader did not fail over: old=%d new=%d", oldLeader, group.Leader())
	}
}
