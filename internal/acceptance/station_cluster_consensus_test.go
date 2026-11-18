//go:build station_cluster_acceptance

package acceptance

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcluster"
	"github.com/LicoLand/BadTower/internal/stationcore"
)

type clusterAcceptanceClock struct{ now time.Time }

func (clock clusterAcceptanceClock) NowUnixMs() int64            { return clock.now.UnixMilli() }
func (clock clusterAcceptanceClock) MonotonicNow() time.Duration { return 0 }

func TestStationClusterConsensusLifecycle(t *testing.T) {
	topology := stationcluster.Topology{
		ClusterID: "synthetic-cluster", Epoch: 1,
		Members: []stationcluster.Member{
			{ID: 1, FaultDomain: "a", Incarnation: 1},
			{ID: 2, FaultDomain: "b", Incarnation: 1},
			{ID: 3, FaultDomain: "c", Incarnation: 1},
			{ID: 4, FaultDomain: "d", Incarnation: 1},
		},
	}
	cores := make(map[uint64]*stationcore.Core, len(topology.Members))
	clock := clusterAcceptanceClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	for _, member := range topology.Members {
		core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1}, clock)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = core.Close() })
		cores[member.ID] = core
	}
	cluster, err := stationcluster.NewCluster(topology, cores)
	if err != nil {
		t.Fatal(err)
	}
	namespace := "ns_ZZZZZZZZZZZZZZZZZZZZZZ"
	reserveCommand, err := cluster.Prepare(stationcore.OperationReserve, stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_acceptClusterReserve1", TTLMs: 3_600_000})
	if err != nil {
		t.Fatal(err)
	}
	reserveResult, err := cluster.Apply(context.Background(), reserveCommand)
	if err != nil {
		t.Fatal(err)
	}
	var reserved stationcore.ReserveOutput
	if err := json.Unmarshal(reserveResult.Payload, &reserved); err != nil {
		t.Fatal(err)
	}
	placements, err := topology.Placements()
	if err != nil {
		t.Fatal(err)
	}
	placement := placements[stationcore.PartitionFor(namespace)]
	if err := cluster.SetMemberActive(placement.Replicas[0], false); err != nil {
		t.Fatal(err)
	}
	submitCommand, err := cluster.Prepare(stationcore.OperationSubmit, stationcore.SubmitInput{
		NamespaceID: namespace, RequestID: "rq_acceptClusterSubmit01", AddressID: reserved.AddressID, Payload: []byte("opaque"), TTLMs: 300_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Apply(context.Background(), submitCommand); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetMemberActive(placement.Replicas[1], false); err != nil {
		t.Fatal(err)
	}
	blocked, err := cluster.Prepare(stationcore.OperationReserve, stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_acceptClusterBlocked1", TTLMs: 60_000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Apply(context.Background(), blocked); !errors.Is(err, stationcluster.ErrQuorumUnavailable) {
		t.Fatalf("minority mutation err=%v", err)
	}
}
