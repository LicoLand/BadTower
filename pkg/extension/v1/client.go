package extension

import (
	"context"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	"google.golang.org/grpc"
)

// StationClient is the complete native capability available to an external
// plugin. It deliberately matches the public native service and exposes no
// Core store, scheduler, topology, listener, or process lifecycle.
type StationClient interface {
	Reserve(context.Context, *stationv1.ReserveRequest, ...grpc.CallOption) (*stationv1.ReserveResponse, error)
	Submit(context.Context, *stationv1.SubmitRequest, ...grpc.CallOption) (*stationv1.SubmitResponse, error)
	Claim(context.Context, *stationv1.ClaimRequest, ...grpc.CallOption) (*stationv1.ClaimResponse, error)
	Settle(context.Context, *stationv1.SettleRequest, ...grpc.CallOption) (*stationv1.SettleResponse, error)
	GetCapacity(context.Context, *stationv1.GetCapacityRequest, ...grpc.CallOption) (*stationv1.GetCapacityResponse, error)
}
