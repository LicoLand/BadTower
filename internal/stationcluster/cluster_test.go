package stationcluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

type clusterClock struct{ now time.Time }

func (clock clusterClock) NowUnixMs() int64            { return clock.now.UnixMilli() }
func (clock clusterClock) MonotonicNow() time.Duration { return 0 }

func TestClusterReplicatesNativeLifecycleAndFailsOver(t *testing.T) {
	t.Parallel()
	cluster, cores := openTestCluster(t)
	namespace := "ns_CCCCCCCCCCCCCCCCCCCCCC"

	reserve := stationcore.ReserveInput{NamespaceID: namespace, RequestID: "rq_clusterReserve0001", TTLMs: 3_600_000}
	command, err := cluster.Prepare(stationcore.OperationReserve, reserve)
	if err != nil {
		t.Fatal(err)
	}
	result, err := cluster.Apply(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	var reserved stationcore.ReserveOutput
	if err := json.Unmarshal(result.Payload, &reserved); err != nil || reserved.AddressID == "" {
		t.Fatalf("reserve result=%s err=%v", result.Payload, err)
	}

	placement := cluster.placements[stationcore.PartitionFor(namespace)]
	runtime := cluster.partitions[stationcore.PartitionFor(namespace)]
	oldLeader := runtime.group.Leader()
	if err := cluster.SetMemberActive(oldLeader, false); err != nil {
		t.Fatal(err)
	}
	submit := stationcore.SubmitInput{
		NamespaceID: namespace, RequestID: "rq_clusterSubmit00001", AddressID: reserved.AddressID,
		Payload: []byte("opaque"), TTLMs: 300_000,
	}
	command, err = cluster.Prepare(stationcore.OperationSubmit, submit)
	if err != nil {
		t.Fatal(err)
	}
	result, err = cluster.Apply(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	var submitted stationcore.SubmitOutput
	if err := json.Unmarshal(result.Payload, &submitted); err != nil || submitted.UnitID == "" {
		t.Fatalf("submit result=%s err=%v", result.Payload, err)
	}
	for _, memberID := range placement.Replicas {
		if memberID == oldLeader {
			continue
		}
		capacity, err := cores[memberID].GetCapacity(context.Background(), stationcore.GetCapacityInput{NamespaceID: namespace})
		if err != nil || capacity.NamespaceUsage.Addresses != 1 || capacity.NamespaceUsage.Units != 1 {
			t.Fatalf("member %d capacity=%#v err=%v", memberID, capacity, err)
		}
	}
}

func TestClusterRejectsMutationWithoutQuorum(t *testing.T) {
	t.Parallel()
	cluster, _ := openTestCluster(t)
	namespace := "ns_QQQQQQQQQQQQQQQQQQQQQQ"
	placement := cluster.placements[stationcore.PartitionFor(namespace)]
	if err := cluster.SetMemberActive(placement.Replicas[0], false); err != nil {
		t.Fatal(err)
	}
	if err := cluster.SetMemberActive(placement.Replicas[1], false); err != nil {
		t.Fatal(err)
	}
	command, err := cluster.Prepare(stationcore.OperationReserve, stationcore.ReserveInput{
		NamespaceID: namespace, RequestID: "rq_clusterNoQuorum001", TTLMs: 60_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cluster.Apply(context.Background(), command); err != ErrQuorumUnavailable {
		t.Fatalf("Apply err=%v", err)
	}
}

func openTestCluster(t *testing.T) (*Cluster, map[uint64]*stationcore.Core) {
	t.Helper()
	topology := testTopology(1)
	cores := make(map[uint64]*stationcore.Core, len(topology.Members))
	clock := clusterClock{now: time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC)}
	for _, member := range topology.Members {
		core, err := stationcore.Open(stationcore.Config{DataRoot: t.TempDir(), ProfileID: stationcore.ProfileSingleNodeGeneralV1}, clock)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = core.Close() })
		cores[member.ID] = core
	}
	cluster, err := NewCluster(topology, cores)
	if err != nil {
		t.Fatal(err)
	}
	return cluster, cores
}
