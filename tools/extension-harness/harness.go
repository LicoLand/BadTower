package extensionharness

import (
	"context"
	"errors"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

var ErrBudgetExceeded = errors.New("external plugin budget exceeded")

// BoundedClient applies one descriptor's local process budgets around an
// ordinary native Station client. It owns no Station lifecycle and does not
// alter the underlying client's availability when the wrapper is exhausted or
// discarded.
type BoundedClient struct {
	client   extension.StationClient
	active   chan struct{}
	admitted chan struct{}
	maxBytes uint64
}

func NewBoundedClient(client extension.StationClient, descriptor extension.Descriptor) (*BoundedClient, error) {
	if client == nil || descriptor.Validate() != nil {
		return nil, extension.ErrInvalidDescriptor
	}
	return &BoundedClient{
		client: client, active: make(chan struct{}, descriptor.Budgets.MaxConcurrentCalls),
		admitted: make(chan struct{}, descriptor.Budgets.MaxConcurrentCalls+descriptor.Budgets.MaxQueuedCalls),
		maxBytes: descriptor.Budgets.MaxRequestBytes,
	}, nil
}

func (client *BoundedClient) call(ctx context.Context, request proto.Message, invoke func() error) error {
	if request == nil || uint64(proto.Size(request)) > client.maxBytes {
		return ErrBudgetExceeded
	}
	select {
	case client.admitted <- struct{}{}:
	default:
		return ErrBudgetExceeded
	}
	defer func() { <-client.admitted }()
	select {
	case client.active <- struct{}{}:
		defer func() { <-client.active }()
		return invoke()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (client *BoundedClient) Reserve(ctx context.Context, request *stationv1.ReserveRequest, options ...grpc.CallOption) (response *stationv1.ReserveResponse, err error) {
	err = client.call(ctx, request, func() error { response, err = client.client.Reserve(ctx, request, options...); return err })
	return response, err
}

func (client *BoundedClient) Submit(ctx context.Context, request *stationv1.SubmitRequest, options ...grpc.CallOption) (response *stationv1.SubmitResponse, err error) {
	err = client.call(ctx, request, func() error { response, err = client.client.Submit(ctx, request, options...); return err })
	return response, err
}

func (client *BoundedClient) Claim(ctx context.Context, request *stationv1.ClaimRequest, options ...grpc.CallOption) (response *stationv1.ClaimResponse, err error) {
	err = client.call(ctx, request, func() error { response, err = client.client.Claim(ctx, request, options...); return err })
	return response, err
}

func (client *BoundedClient) Settle(ctx context.Context, request *stationv1.SettleRequest, options ...grpc.CallOption) (response *stationv1.SettleResponse, err error) {
	err = client.call(ctx, request, func() error { response, err = client.client.Settle(ctx, request, options...); return err })
	return response, err
}

func (client *BoundedClient) GetCapacity(ctx context.Context, request *stationv1.GetCapacityRequest, options ...grpc.CallOption) (response *stationv1.GetCapacityResponse, err error) {
	err = client.call(ctx, request, func() error { response, err = client.client.GetCapacity(ctx, request, options...); return err })
	return response, err
}
