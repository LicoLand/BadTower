package stationcluster

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const (
	maxProposalBytes           = 2 << 20
	maxSizePerMessage          = 1 << 20
	maxUncommittedEntriesBytes = 8 << 20
	maxInflightMessages        = 64
	maxInflightBytes           = 8 << 20
	maxDriveRounds             = 10_000
)

var (
	ErrQuorumUnavailable = errors.New("cluster quorum unavailable")
	ErrProposalTooLarge  = errors.New("cluster proposal too large")
)

type committedEntry struct {
	Index uint64
	Data  []byte
}

type raftReplica struct {
	id        uint64
	node      *raft.RawNode
	storage   *raft.MemoryStorage
	active    bool
	committed []committedEntry
	applied   uint64
}

// RaftGroup is a bounded deterministic replicated-log driver. Durable
// production storage and peer transport implement the same Ready ordering;
// this driver is also the exact adversarial acceptance oracle.
type RaftGroup struct {
	replicas map[uint64]*raftReplica
	ids      []uint64
	leader   uint64
}

func NewRaftGroup(memberIDs []uint64) (*RaftGroup, error) {
	if len(memberIDs) < MinMembers || len(memberIDs) > MaxMembers {
		return nil, ErrInvalidTopology
	}
	ids := append([]uint64(nil), memberIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	peers := make([]raft.Peer, len(ids))
	group := &RaftGroup{replicas: make(map[uint64]*raftReplica, len(ids)), ids: ids}
	for index, id := range ids {
		if id == 0 || index > 0 && id == ids[index-1] {
			return nil, ErrInvalidTopology
		}
		peers[index] = raft.Peer{ID: id}
	}
	for _, id := range ids {
		storage := raft.NewMemoryStorage()
		node, err := raft.NewRawNode(&raft.Config{
			ID: id, ElectionTick: 10, HeartbeatTick: 1, Storage: storage,
			MaxSizePerMsg:             maxSizePerMessage,
			MaxCommittedSizePerReady:  maxSizePerMessage,
			MaxUncommittedEntriesSize: maxUncommittedEntriesBytes,
			MaxInflightMsgs:           maxInflightMessages,
			MaxInflightBytes:          maxInflightBytes,
			CheckQuorum:               true, PreVote: true, ReadOnlyOption: raft.ReadOnlySafe,
			DisableProposalForwarding: true, StepDownOnRemoval: true,
			Logger: consensusLogger{},
		})
		if err != nil {
			return nil, err
		}
		if err := node.Bootstrap(peers); err != nil {
			return nil, err
		}
		group.replicas[id] = &raftReplica{id: id, node: node, storage: storage, active: true}
	}
	if err := group.drive(nil); err != nil {
		return nil, err
	}
	if err := group.replicas[ids[0]].node.Campaign(); err != nil {
		return nil, err
	}
	if err := group.drive(nil); err != nil {
		return nil, err
	}
	if group.leader == 0 {
		return nil, ErrQuorumUnavailable
	}
	return group, nil
}

func (group *RaftGroup) Leader() uint64 { return group.leader }

func (group *RaftGroup) SetActive(memberID uint64, active bool) error {
	replica, ok := group.replicas[memberID]
	if !ok {
		return ErrInvalidTopology
	}
	replica.active = active
	if !active && group.leader == memberID {
		group.leader = 0
	}
	return nil
}

func (group *RaftGroup) Propose(data []byte) (uint64, error) {
	if len(data) == 0 || len(data) > maxProposalBytes {
		return 0, ErrProposalTooLarge
	}
	if group.activeCount() < len(group.ids)/2+1 {
		return 0, ErrQuorumUnavailable
	}
	if group.leader == 0 || !group.replicas[group.leader].active {
		if err := group.elect(); err != nil {
			return 0, err
		}
	}
	before := group.replicas[group.leader].applied
	if err := group.replicas[group.leader].node.Propose(append([]byte(nil), data...)); err != nil {
		return 0, fmt.Errorf("propose: %w", err)
	}
	if err := group.drive(data); err != nil {
		return 0, err
	}
	index := group.replicas[group.leader].applied
	if index <= before {
		return 0, ErrQuorumUnavailable
	}
	return index, nil
}

func (group *RaftGroup) Committed(memberID uint64) []committedEntry {
	replica := group.replicas[memberID]
	if replica == nil {
		return nil
	}
	out := make([]committedEntry, len(replica.committed))
	for index, entry := range replica.committed {
		out[index] = committedEntry{Index: entry.Index, Data: append([]byte(nil), entry.Data...)}
	}
	return out
}

func (group *RaftGroup) activeCount() int {
	count := 0
	for _, replica := range group.replicas {
		if replica.active {
			count++
		}
	}
	return count
}

func (group *RaftGroup) elect() error {
	for _, id := range group.ids {
		if !group.replicas[id].active {
			continue
		}
		if err := group.replicas[id].node.Campaign(); err != nil {
			continue
		}
		if err := group.drive(nil); err != nil {
			return err
		}
		if group.leader != 0 {
			return nil
		}
	}
	return ErrQuorumUnavailable
}

func (group *RaftGroup) drive(want []byte) error {
	for round := 0; round < maxDriveRounds; round++ {
		progress := false
		var messages []*raftpb.Message
		for _, id := range group.ids {
			replica := group.replicas[id]
			if !replica.active || !replica.node.HasReady() {
				continue
			}
			progress = true
			ready := replica.node.Ready()
			if ready.Snapshot != nil && !raft.IsEmptySnap(ready.Snapshot) {
				if err := replica.storage.ApplySnapshot(ready.Snapshot); err != nil {
					return err
				}
			}
			if len(ready.Entries) > 0 {
				if err := replica.storage.Append(ready.Entries); err != nil {
					return err
				}
			}
			if ready.HardState != nil && !raft.IsEmptyHardState(ready.HardState) {
				if err := replica.storage.SetHardState(ready.HardState); err != nil {
					return err
				}
			}
			for _, entry := range ready.CommittedEntries {
				if err := applyCommitted(replica, entry); err != nil {
					return err
				}
			}
			messages = append(messages, ready.Messages...)
			replica.node.Advance(ready)
		}
		for _, message := range messages {
			target := group.replicas[message.GetTo()]
			if target == nil || !target.active {
				continue
			}
			progress = true
			if err := target.node.Step(message); err != nil && !errors.Is(err, raft.ErrStepPeerNotFound) {
				return err
			}
		}
		group.refreshLeader()
		if len(want) > 0 && group.committedOnQuorum(want) {
			return nil
		}
		if len(want) == 0 && !progress {
			return nil
		}
		if !progress {
			for _, replica := range group.replicas {
				if replica.active {
					replica.node.Tick()
				}
			}
		}
	}
	return ErrQuorumUnavailable
}

func applyCommitted(replica *raftReplica, entry *raftpb.Entry) error {
	if entry.GetIndex() <= replica.applied {
		return nil
	}
	switch entry.GetType() {
	case raftpb.EntryType_EntryNormal:
		if len(entry.Data) > 0 {
			replica.committed = append(replica.committed, committedEntry{Index: entry.GetIndex(), Data: append([]byte(nil), entry.Data...)})
		}
	case raftpb.EntryType_EntryConfChange:
		var change raftpb.ConfChange
		if err := proto.Unmarshal(entry.Data, &change); err != nil {
			return err
		}
		replica.node.ApplyConfChange(&change)
	case raftpb.EntryType_EntryConfChangeV2:
		var change raftpb.ConfChangeV2
		if err := proto.Unmarshal(entry.Data, &change); err != nil {
			return err
		}
		replica.node.ApplyConfChange(&change)
	}
	replica.applied = entry.GetIndex()
	return nil
}

func (group *RaftGroup) refreshLeader() {
	group.leader = 0
	for _, id := range group.ids {
		replica := group.replicas[id]
		if !replica.active {
			continue
		}
		if lead := replica.node.BasicStatus().Lead; lead != 0 {
			if group.replicas[lead] != nil && group.replicas[lead].active {
				group.leader = lead
				return
			}
		}
	}
}

func (group *RaftGroup) committedOnQuorum(want []byte) bool {
	count := 0
	for _, id := range group.ids {
		replica := group.replicas[id]
		if !replica.active || len(replica.committed) == 0 {
			continue
		}
		if bytes.Equal(replica.committed[len(replica.committed)-1].Data, want) {
			count++
		}
	}
	return count >= len(group.ids)/2+1
}
