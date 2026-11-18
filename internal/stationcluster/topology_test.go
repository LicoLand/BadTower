package stationcluster

import (
	"errors"
	"reflect"
	"testing"
)

func TestPlacementsAreDeterministicAndFaultDomainAware(t *testing.T) {
	t.Parallel()
	topology := testTopology(1)
	first, err := topology.Placements()
	if err != nil {
		t.Fatal(err)
	}
	second, err := topology.Placements()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, second) || len(first) != PartitionCount {
		t.Fatal("placement is not deterministic and complete")
	}
	for _, placement := range first {
		if placement.Epoch != topology.Epoch || placement.Replicas[0] == placement.Replicas[1] || placement.Replicas[1] == placement.Replicas[2] {
			t.Fatalf("invalid placement: %#v", placement)
		}
	}
}

func TestPlacementFencesStaleEpochAndNonOwner(t *testing.T) {
	t.Parallel()
	placements, err := testTopology(9).Placements()
	if err != nil {
		t.Fatal(err)
	}
	placement := placements[0]
	if err := placement.Accepts(placement.Replicas[0], 9); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(placement.Accepts(placement.Replicas[0], 8), ErrStaleEpoch) {
		t.Fatal("stale epoch accepted")
	}
	if !errors.Is(placement.Accepts(99, 9), ErrStaleEpoch) {
		t.Fatal("non-owner accepted")
	}
}

func TestTopologyRequiresThreeFaultDomains(t *testing.T) {
	t.Parallel()
	topology := testTopology(1)
	topology.Members[2].FaultDomain = topology.Members[1].FaultDomain
	topology.Members[3].FaultDomain = topology.Members[0].FaultDomain
	if !errors.Is(topology.Validate(), ErrInvalidTopology) {
		t.Fatal("topology with two fault domains accepted")
	}
}

func testTopology(epoch uint64) Topology {
	return Topology{
		ClusterID: "cluster-synthetic", Epoch: epoch,
		Members: []Member{
			{ID: 1, FaultDomain: "zone-a", Incarnation: 1},
			{ID: 2, FaultDomain: "zone-b", Incarnation: 1},
			{ID: 3, FaultDomain: "zone-c", Incarnation: 1},
			{ID: 4, FaultDomain: "zone-d", Incarnation: 1},
		},
	}
}
