package extensionharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"
	"time"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	extension "github.com/LicoLand/BadTower/pkg/extension/v1"
	"google.golang.org/grpc"
)

type syntheticClient struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (client *syntheticClient) Reserve(context.Context, *stationv1.ReserveRequest, ...grpc.CallOption) (*stationv1.ReserveResponse, error) {
	client.once.Do(func() { close(client.started) })
	<-client.release
	return &stationv1.ReserveResponse{AddressId: "sa_AAAAAAAAAAAAAAAAAAAAAA"}, nil
}
func (*syntheticClient) Submit(context.Context, *stationv1.SubmitRequest, ...grpc.CallOption) (*stationv1.SubmitResponse, error) {
	return &stationv1.SubmitResponse{}, nil
}
func (*syntheticClient) Claim(context.Context, *stationv1.ClaimRequest, ...grpc.CallOption) (*stationv1.ClaimResponse, error) {
	return &stationv1.ClaimResponse{}, nil
}
func (*syntheticClient) Settle(context.Context, *stationv1.SettleRequest, ...grpc.CallOption) (*stationv1.SettleResponse, error) {
	return &stationv1.SettleResponse{}, nil
}
func (*syntheticClient) GetCapacity(context.Context, *stationv1.GetCapacityRequest, ...grpc.CallOption) (*stationv1.GetCapacityResponse, error) {
	return &stationv1.GetCapacityResponse{ProfileId: "single-node-general-v1"}, nil
}

func TestExhaustedOrRemovedPluginLeavesDirectNativeClientUnchanged(t *testing.T) {
	underlying := &syntheticClient{started: make(chan struct{}), release: make(chan struct{})}
	digest := sha256.Sum256([]byte("synthetic"))
	descriptor := extension.Descriptor{
		SchemaVersion: extension.SchemaVersion, PluginID: "synthetic.plugin", ArtifactVersion: "1.0.0",
		ConfigurationSchema: "badtower.synthetic.config.v1", LocalStateSchema: "none",
		ExternalProtocol: extension.ExternalProtocol{Identity: "synthetic", Line: "synthetic.v1", Lifecycle: "Candidate"},
		Contract:         extension.ContractRange{Minimum: extension.ContractVersion, Maximum: extension.ContractVersion},
		Listener:         extension.Listener{Owner: "external-plugin", Transport: "synthetic"},
		NativeScope:      extension.NativeScope{NamespaceMode: "one-opaque-namespace", CredentialMode: "scoped-native-caller"},
		Budgets:          extension.Budgets{MaxConcurrentCalls: 1, MaxQueuedCalls: 1, MaxRequestBytes: 1024, MaxStateBytes: 1024, MaxMetricSeries: 1},
		Binary:           extension.Binary{Path: "bin/synthetic-plugin", SHA256: hex.EncodeToString(digest[:])},
	}
	plugin, err := NewBoundedClient(underlying, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() {
		_, err := plugin.Reserve(context.Background(), &stationv1.ReserveRequest{RequestId: "first"})
		first <- err
	}()
	<-underlying.started
	secondContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	second := make(chan error, 1)
	go func() {
		_, err := plugin.Reserve(secondContext, &stationv1.ReserveRequest{RequestId: "second"})
		second <- err
	}()
	deadline := time.Now().Add(time.Second)
	for len(plugin.admitted) != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if _, err := plugin.Reserve(context.Background(), &stationv1.ReserveRequest{RequestId: "third"}); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("third call err=%v", err)
	}
	// Bypass/discarding the optional wrapper does not touch the Station client.
	if response, err := underlying.GetCapacity(context.Background(), &stationv1.GetCapacityRequest{}); err != nil || response.GetProfileId() == "" {
		t.Fatalf("direct native response=%#v err=%v", response, err)
	}
	close(underlying.release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}
