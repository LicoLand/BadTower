package stationcluster

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"
	"sort"
)

const (
	MinMembers        = 3
	MaxMembers        = 7
	ReplicationFactor = 3
	PartitionCount    = 64
)

var (
	ErrInvalidTopology = errors.New("invalid cluster topology")
	ErrStaleEpoch      = errors.New("stale ownership epoch")
)

type Member struct {
	ID          uint64
	FaultDomain string
	Incarnation uint64
}

type Placement struct {
	Partition uint16
	Epoch     uint64
	Replicas  [ReplicationFactor]uint64
}

type Topology struct {
	ClusterID string
	Epoch     uint64
	Members   []Member
}

func (topology Topology) Validate() error {
	if topology.ClusterID == "" || topology.Epoch == 0 || len(topology.Members) < MinMembers || len(topology.Members) > MaxMembers {
		return ErrInvalidTopology
	}
	ids := make(map[uint64]struct{}, len(topology.Members))
	domains := make(map[string]struct{}, len(topology.Members))
	for _, member := range topology.Members {
		if member.ID == 0 || member.Incarnation == 0 || member.FaultDomain == "" {
			return ErrInvalidTopology
		}
		if _, exists := ids[member.ID]; exists {
			return ErrInvalidTopology
		}
		ids[member.ID] = struct{}{}
		domains[member.FaultDomain] = struct{}{}
	}
	if len(domains) < ReplicationFactor {
		return ErrInvalidTopology
	}
	return nil
}

// Placements applies weighted rendezvous hashing with fault-domain diversity.
// It performs O(partitions*members log members) work over at most seven
// members, uses no persistent per-namespace map, and changes only the
// partitions whose winning member set changes.
func (topology Topology) Placements() ([]Placement, error) {
	if err := topology.Validate(); err != nil {
		return nil, err
	}
	placements := make([]Placement, PartitionCount)
	for partition := uint16(0); partition < PartitionCount; partition++ {
		members := slices.Clone(topology.Members)
		sort.Slice(members, func(i, j int) bool {
			left := rendezvousScore(topology.ClusterID, partition, members[i])
			right := rendezvousScore(topology.ClusterID, partition, members[j])
			if left == right {
				return members[i].ID < members[j].ID
			}
			return left > right
		})
		placement := Placement{Partition: partition, Epoch: topology.Epoch}
		usedDomains := make(map[string]struct{}, ReplicationFactor)
		index := 0
		for _, member := range members {
			if _, used := usedDomains[member.FaultDomain]; used {
				continue
			}
			placement.Replicas[index] = member.ID
			usedDomains[member.FaultDomain] = struct{}{}
			index++
			if index == ReplicationFactor {
				break
			}
		}
		if index != ReplicationFactor {
			return nil, ErrInvalidTopology
		}
		placements[partition] = placement
	}
	return placements, nil
}

func (placement Placement) Accepts(memberID, epoch uint64) error {
	if epoch != placement.Epoch {
		return ErrStaleEpoch
	}
	if !slices.Contains(placement.Replicas[:], memberID) {
		return ErrStaleEpoch
	}
	return nil
}

func rendezvousScore(clusterID string, partition uint16, member Member) uint64 {
	hash := sha256.New()
	hash.Write([]byte("badtower.cluster.placement.v1\x00"))
	hash.Write([]byte(clusterID))
	var raw [18]byte
	binary.BigEndian.PutUint16(raw[:2], partition)
	binary.BigEndian.PutUint64(raw[2:10], member.ID)
	binary.BigEndian.PutUint64(raw[10:], member.Incarnation)
	hash.Write(raw[:])
	sum := hash.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}
