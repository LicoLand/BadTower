package stationcluster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

// MemberStation is the native Station surface for one production Raft group.
// Mutations are accepted only by its current leader. Read operations cross a
// quorum ReadIndex and wait until the local state machine has applied that
// index before they inspect Core.
type MemberStation struct {
	member *RaftMember
	core   *stationcore.Core
	epoch  uint64
}

func NewMemberStation(member *RaftMember, core *stationcore.Core, ownershipEpoch uint64) (*MemberStation, error) {
	if member == nil || core == nil || ownershipEpoch == 0 {
		return nil, ErrInvalidTopology
	}
	return &MemberStation{member: member, core: core, epoch: ownershipEpoch}, nil
}

func (station *MemberStation) mutate(ctx context.Context, operation string, input any, final bool, output any) error {
	command, err := station.core.PrepareCommand(operation, input, station.epoch)
	if err != nil {
		return err
	}
	command.FinalAttempt = final
	result, err := station.member.Propose(ctx, command)
	if err != nil {
		return err
	}
	return json.Unmarshal(result.Payload, output)
}

func (station *MemberStation) Reserve(ctx context.Context, input stationcore.ReserveInput) (stationcore.ReserveOutput, error) {
	var output stationcore.ReserveOutput
	err := station.mutate(ctx, stationcore.OperationReserve, input, false, &output)
	return output, err
}

func (station *MemberStation) Submit(ctx context.Context, input stationcore.SubmitInput) (stationcore.SubmitOutput, error) {
	var output stationcore.SubmitOutput
	err := station.mutate(ctx, stationcore.OperationSubmit, input, false, &output)
	return output, err
}

func (station *MemberStation) Claim(ctx context.Context, input stationcore.ClaimInput) (stationcore.ClaimOutput, error) {
	wait := time.Duration(input.WaitTimeoutMs) * time.Millisecond
	if wait > 20*time.Second {
		wait = 20 * time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		final := wait > 0 && !time.Now().Before(deadline)
		var output stationcore.ClaimOutput
		if err := station.mutate(ctx, stationcore.OperationClaim, input, final, &output); err != nil {
			return stationcore.ClaimOutput{}, err
		}
		if len(output.Units) > 0 || wait == 0 || final {
			return output, nil
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

func (station *MemberStation) Settle(ctx context.Context, input stationcore.SettleInput) (stationcore.SettleOutput, error) {
	var output stationcore.SettleOutput
	err := station.mutate(ctx, stationcore.OperationSettle, input, false, &output)
	return output, err
}

func (station *MemberStation) GetCapacity(ctx context.Context, input stationcore.GetCapacityInput) (stationcore.GetCapacityOutput, error) {
	if err := station.member.ReadBarrier(ctx); err != nil {
		return stationcore.GetCapacityOutput{}, err
	}
	return station.core.GetCapacity(ctx, input)
}

func (station *MemberStation) ProfileLimits() stationcore.ProfileLimits {
	return station.core.ProfileLimits()
}
func (station *MemberStation) NamespacePressure(namespace string) (stationcore.Pressure, stationcore.ResourceVector) {
	return station.core.NamespacePressure(namespace)
}
func (station *MemberStation) ObservationSnapshot() stationcore.StationObservation {
	return station.core.ObservationSnapshot()
}
func (station *MemberStation) Ready() (bool, stationcore.Reason) { return station.core.Ready() }
func (station *MemberStation) BeginDrain()                       { station.core.BeginDrain() }
