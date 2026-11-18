package stationcluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

var (
	ErrReplicaDiverged = errors.New("cluster replica diverged")
	ErrMemberUnknown   = errors.New("cluster member unknown")
)

type partitionRuntime struct {
	mu      sync.Mutex
	group   *RaftGroup
	applied map[uint64]int
}

// Cluster provides the unchanged native mutation contract over fixed
// partition Raft groups. Each member owns a distinct Core data root; the
// replicated command is the only mutation input shared between them.
type Cluster struct {
	topology   Topology
	placements [PartitionCount]Placement
	cores      map[uint64]*stationcore.Core
	active     map[uint64]bool
	partitions [PartitionCount]*partitionRuntime
	mu         sync.RWMutex
}

func NewCluster(topology Topology, cores map[uint64]*stationcore.Core) (*Cluster, error) {
	placements, err := topology.Placements()
	if err != nil {
		return nil, err
	}
	if len(cores) != len(topology.Members) {
		return nil, ErrInvalidTopology
	}
	cluster := &Cluster{topology: topology, cores: make(map[uint64]*stationcore.Core, len(cores)), active: make(map[uint64]bool, len(cores))}
	for _, member := range topology.Members {
		core := cores[member.ID]
		if core == nil {
			return nil, ErrInvalidTopology
		}
		cluster.cores[member.ID] = core
		cluster.active[member.ID] = true
	}
	copy(cluster.placements[:], placements)
	return cluster, nil
}

func (cluster *Cluster) SetMemberActive(memberID uint64, active bool) error {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if _, exists := cluster.cores[memberID]; !exists {
		return ErrMemberUnknown
	}
	cluster.active[memberID] = active
	for _, runtime := range cluster.partitions {
		if runtime == nil {
			continue
		}
		runtime.mu.Lock()
		_ = runtime.group.SetActive(memberID, active)
		runtime.mu.Unlock()
	}
	return nil
}

func (cluster *Cluster) Prepare(operation string, input any) (stationcore.Command, error) {
	namespace, err := inputNamespace(input)
	if err != nil {
		return stationcore.Command{}, err
	}
	partition := stationcore.PartitionFor(namespace)
	placement := cluster.placements[partition]
	leaderCore := cluster.firstActiveCore(placement)
	if leaderCore == nil {
		return stationcore.Command{}, ErrQuorumUnavailable
	}
	return leaderCore.PrepareCommand(operation, input, placement.Epoch)
}

func (cluster *Cluster) Apply(ctx context.Context, command stationcore.Command) (stationcore.CommandResult, error) {
	if err := command.Validate(); err != nil {
		return stationcore.CommandResult{}, err
	}
	partition := stationcore.PartitionFor(command.NamespaceID)
	placement := cluster.placements[partition]
	if command.OwnershipEpoch != placement.Epoch {
		return stationcore.CommandResult{}, ErrStaleEpoch
	}
	runtime, err := cluster.partitionRuntime(partition, placement)
	if err != nil {
		return stationcore.CommandResult{}, err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()

	raw, err := json.Marshal(command)
	if err != nil {
		return stationcore.CommandResult{}, err
	}
	if _, err := runtime.group.Propose(raw); err != nil {
		return stationcore.CommandResult{}, err
	}

	var canonical stationcore.CommandResult
	var canonicalErr error
	haveCanonical := false
	for _, memberID := range placement.Replicas {
		entries := runtime.group.Committed(memberID)
		for runtime.applied[memberID] < len(entries) {
			entry := entries[runtime.applied[memberID]]
			var replicated stationcore.Command
			if err := json.Unmarshal(entry.Data, &replicated); err != nil {
				return stationcore.CommandResult{}, ErrReplicaDiverged
			}
			result, applyErr := cluster.cores[memberID].ApplyCommand(ctx, replicated)
			if !haveCanonical {
				canonical, canonicalErr, haveCanonical = result, applyErr, true
			} else if result.Reason != canonical.Reason || !bytes.Equal(result.Payload, canonical.Payload) || !sameFailure(applyErr, canonicalErr) {
				return stationcore.CommandResult{}, ErrReplicaDiverged
			}
			runtime.applied[memberID]++
		}
	}
	if !haveCanonical {
		return stationcore.CommandResult{}, ErrQuorumUnavailable
	}
	return canonical, canonicalErr
}

func (cluster *Cluster) Reserve(ctx context.Context, input stationcore.ReserveInput) (stationcore.ReserveOutput, error) {
	command, err := cluster.Prepare(stationcore.OperationReserve, input)
	if err != nil {
		return stationcore.ReserveOutput{}, err
	}
	result, err := cluster.Apply(ctx, command)
	var output stationcore.ReserveOutput
	if err == nil {
		err = json.Unmarshal(result.Payload, &output)
	}
	return output, err
}

func (cluster *Cluster) Submit(ctx context.Context, input stationcore.SubmitInput) (stationcore.SubmitOutput, error) {
	command, err := cluster.Prepare(stationcore.OperationSubmit, input)
	if err != nil {
		return stationcore.SubmitOutput{}, err
	}
	result, err := cluster.Apply(ctx, command)
	var output stationcore.SubmitOutput
	if err == nil {
		err = json.Unmarshal(result.Payload, &output)
	}
	return output, err
}

func (cluster *Cluster) Claim(ctx context.Context, input stationcore.ClaimInput) (stationcore.ClaimOutput, error) {
	wait := time.Duration(input.WaitTimeoutMs) * time.Millisecond
	if wait > 20*time.Second {
		wait = 20 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		command, err := cluster.Prepare(stationcore.OperationClaim, input)
		if err != nil {
			return stationcore.ClaimOutput{}, err
		}
		if wait > 0 && !time.Now().Before(deadline) {
			command.FinalAttempt = true
		}
		result, err := cluster.Apply(ctx, command)
		var output stationcore.ClaimOutput
		if err == nil {
			err = json.Unmarshal(result.Payload, &output)
		}
		if err != nil || len(output.Units) > 0 || wait == 0 || command.FinalAttempt {
			return output, err
		}
		timer := time.NewTimer(min(25*time.Millisecond, time.Until(deadline)))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return stationcore.ClaimOutput{}, ctx.Err()
		}
	}
}

func (cluster *Cluster) Settle(ctx context.Context, input stationcore.SettleInput) (stationcore.SettleOutput, error) {
	command, err := cluster.Prepare(stationcore.OperationSettle, input)
	if err != nil {
		return stationcore.SettleOutput{}, err
	}
	result, err := cluster.Apply(ctx, command)
	var output stationcore.SettleOutput
	if err == nil {
		err = json.Unmarshal(result.Payload, &output)
	}
	return output, err
}

func (cluster *Cluster) GetCapacity(ctx context.Context, input stationcore.GetCapacityInput) (stationcore.GetCapacityOutput, error) {
	placement := cluster.placements[stationcore.PartitionFor(input.NamespaceID)]
	core := cluster.firstActiveCore(placement)
	if core == nil {
		return stationcore.GetCapacityOutput{}, ErrQuorumUnavailable
	}
	return core.GetCapacity(ctx, input)
}

func (cluster *Cluster) ProfileLimits() stationcore.ProfileLimits {
	for _, member := range cluster.topology.Members {
		if core := cluster.cores[member.ID]; core != nil {
			return core.ProfileLimits()
		}
	}
	return stationcore.ProfileLimits{}
}

func (cluster *Cluster) NamespacePressure(namespace string) (stationcore.Pressure, stationcore.ResourceVector) {
	placement := cluster.placements[stationcore.PartitionFor(namespace)]
	core := cluster.firstActiveCore(placement)
	if core == nil {
		return stationcore.PressureHard, stationcore.ResourceVector{}
	}
	return core.NamespacePressure(namespace)
}

func (cluster *Cluster) ObservationSnapshot() stationcore.StationObservation {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	for _, member := range cluster.topology.Members {
		if cluster.active[member.ID] {
			return cluster.cores[member.ID].ObservationSnapshot()
		}
	}
	return stationcore.StationObservation{Lifecycle: stationcore.StationLifecycleStopped, Pressure: stationcore.PressureHard}
}

func (cluster *Cluster) Ready() (bool, stationcore.Reason) {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	active := 0
	for _, member := range cluster.topology.Members {
		if !cluster.active[member.ID] {
			continue
		}
		ready, reason := cluster.cores[member.ID].Ready()
		if !ready {
			return false, reason
		}
		active++
	}
	if active < len(cluster.topology.Members)/2+1 {
		return false, stationcore.ReasonStorageUnavailable
	}
	return true, ""
}

func (cluster *Cluster) BeginDrain() {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	for _, core := range cluster.cores {
		core.BeginDrain()
	}
}

func (cluster *Cluster) Close() error {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	var first error
	for _, core := range cluster.cores {
		if err := core.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

func (cluster *Cluster) partitionRuntime(partition uint16, placement Placement) (*partitionRuntime, error) {
	cluster.mu.Lock()
	defer cluster.mu.Unlock()
	if current := cluster.partitions[partition]; current != nil {
		return current, nil
	}
	ids := append([]uint64(nil), placement.Replicas[:]...)
	group, err := NewRaftGroup(ids)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if !cluster.active[id] {
			_ = group.SetActive(id, false)
		}
	}
	runtime := &partitionRuntime{group: group, applied: make(map[uint64]int, len(ids))}
	cluster.partitions[partition] = runtime
	return runtime, nil
}

func (cluster *Cluster) firstActiveCore(placement Placement) *stationcore.Core {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	for _, memberID := range placement.Replicas {
		if cluster.active[memberID] {
			return cluster.cores[memberID]
		}
	}
	return nil
}

func inputNamespace(input any) (string, error) {
	switch typed := input.(type) {
	case stationcore.ReserveInput:
		return typed.NamespaceID, nil
	case stationcore.SubmitInput:
		return typed.NamespaceID, nil
	case stationcore.ClaimInput:
		return typed.NamespaceID, nil
	case stationcore.SettleInput:
		return typed.NamespaceID, nil
	default:
		return "", ErrInvalidTopology
	}
}

func sameFailure(left, right error) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	leftFailure, leftOK := stationcore.IsFailure(left)
	rightFailure, rightOK := stationcore.IsFailure(right)
	return leftOK && rightOK && leftFailure.Reason == rightFailure.Reason && leftFailure.Retryable == rightFailure.Retryable
}
