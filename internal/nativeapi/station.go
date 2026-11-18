package nativeapi

import (
	"context"

	"github.com/LicoLand/BadTower/internal/stationcore"
)

// Station is the protocol-neutral native execution surface shared by an
// independently operable Instance and a quorum-backed Cluster. The public
// gRPC contract does not branch on the implementation behind this interface.
type Station interface {
	Reserve(context.Context, stationcore.ReserveInput) (stationcore.ReserveOutput, error)
	Submit(context.Context, stationcore.SubmitInput) (stationcore.SubmitOutput, error)
	Claim(context.Context, stationcore.ClaimInput) (stationcore.ClaimOutput, error)
	Settle(context.Context, stationcore.SettleInput) (stationcore.SettleOutput, error)
	GetCapacity(context.Context, stationcore.GetCapacityInput) (stationcore.GetCapacityOutput, error)
	ProfileLimits() stationcore.ProfileLimits
	NamespacePressure(string) (stationcore.Pressure, stationcore.ResourceVector)
	ObservationSnapshot() stationcore.StationObservation
	Ready() (bool, stationcore.Reason)
	BeginDrain()
}
