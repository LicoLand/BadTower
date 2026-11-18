package stationcluster

import (
	"context"
	"errors"
	"math"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

type Router struct{ node *Node }

func (router *Router) station(namespace string) (*MemberStation, error) {
	if router == nil || router.node == nil {
		return nil, ErrMemberStopped
	}
	partition := stationcore.PartitionFor(namespace)
	station := router.node.stations[partition]
	if station == nil {
		return nil, ErrMemberStopped
	}
	return station, nil
}

func (router *Router) Reserve(ctx context.Context, input stationcore.ReserveInput) (stationcore.ReserveOutput, error) {
	station, err := router.station(input.NamespaceID)
	if err != nil {
		return stationcore.ReserveOutput{}, err
	}
	output, err := station.Reserve(ctx, input)
	return output, stableRouterError(err)
}
func (router *Router) Submit(ctx context.Context, input stationcore.SubmitInput) (stationcore.SubmitOutput, error) {
	station, err := router.station(input.NamespaceID)
	if err != nil {
		return stationcore.SubmitOutput{}, err
	}
	output, err := station.Submit(ctx, input)
	return output, stableRouterError(err)
}
func (router *Router) Claim(ctx context.Context, input stationcore.ClaimInput) (stationcore.ClaimOutput, error) {
	station, err := router.station(input.NamespaceID)
	if err != nil {
		return stationcore.ClaimOutput{}, err
	}
	output, err := station.Claim(ctx, input)
	return output, stableRouterError(err)
}
func (router *Router) Settle(ctx context.Context, input stationcore.SettleInput) (stationcore.SettleOutput, error) {
	station, err := router.station(input.NamespaceID)
	if err != nil {
		return stationcore.SettleOutput{}, err
	}
	output, err := station.Settle(ctx, input)
	return output, stableRouterError(err)
}
func (router *Router) GetCapacity(ctx context.Context, input stationcore.GetCapacityInput) (stationcore.GetCapacityOutput, error) {
	station, err := router.station(input.NamespaceID)
	if err != nil {
		return stationcore.GetCapacityOutput{}, err
	}
	output, err := station.GetCapacity(ctx, input)
	return output, stableRouterError(err)
}

func stableRouterError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrQuorumUnavailable) || errors.Is(err, ErrPeerBackpressure) {
		return errors.Join(err, &stationcore.Failure{Reason: stationcore.ReasonStorageUnavailable, Retryable: true, RetryAfterMs: 100})
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(err, &stationcore.Failure{Reason: stationcore.ReasonDeadlineExceeded})
	}
	return err
}
func (router *Router) ProfileLimits() stationcore.ProfileLimits {
	if router == nil || router.node == nil || router.node.stations[0] == nil {
		return stationcore.ProfileLimits{}
	}
	return router.node.stations[0].ProfileLimits()
}
func (router *Router) NamespacePressure(namespace string) (stationcore.Pressure, stationcore.ResourceVector) {
	station, err := router.station(namespace)
	if err != nil {
		return stationcore.PressureHard, stationcore.ResourceVector{}
	}
	return station.NamespacePressure(namespace)
}
func (router *Router) ObservationSnapshot() stationcore.StationObservation {
	if router == nil || router.node == nil {
		return stationcore.StationObservation{Lifecycle: stationcore.StationLifecycleStopped, Pressure: stationcore.PressureHard}
	}
	result := stationcore.StationObservation{Lifecycle: stationcore.StationLifecycleRunning, Pressure: stationcore.PressureNormal}
	for _, core := range router.node.cores {
		snapshot := core.ObservationSnapshot()
		if snapshot.Lifecycle != stationcore.StationLifecycleRunning {
			result.Lifecycle = snapshot.Lifecycle
		}
		if snapshot.Pressure > result.Pressure {
			result.Pressure = snapshot.Pressure
		}
		result.Aggregate.Addresses = saturatingAdd(result.Aggregate.Addresses, snapshot.Aggregate.Addresses)
		result.Aggregate.Units = saturatingAdd(result.Aggregate.Units, snapshot.Aggregate.Units)
		result.Aggregate.RetainedBytes = saturatingAdd(result.Aggregate.RetainedBytes, snapshot.Aggregate.RetainedBytes)
		result.Aggregate.ActiveClaims = saturatingAdd(result.Aggregate.ActiveClaims, snapshot.Aggregate.ActiveClaims)
		result.Aggregate.IdempotencyRecords = saturatingAdd(result.Aggregate.IdempotencyRecords, snapshot.Aggregate.IdempotencyRecords)
	}
	return result
}
func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}
func (router *Router) Ready() (bool, stationcore.Reason) {
	if router == nil || router.node == nil {
		return false, stationcore.ReasonStorageUnavailable
	}
	for partition, member := range router.node.members {
		lead, _, fatal := member.Status()
		ready, reason := router.node.cores[partition].Ready()
		if fatal != nil || lead == 0 {
			return false, stationcore.ReasonStorageUnavailable
		}
		if !ready {
			return false, reason
		}
	}
	return true, ""
}
func (router *Router) BeginDrain() {
	if router != nil && router.node != nil {
		for _, core := range router.node.cores {
			core.BeginDrain()
		}
	}
}
