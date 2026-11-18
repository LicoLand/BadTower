package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	"google.golang.org/grpc"
)

type station struct {
	stationv1.UnimplementedStationServer
}

func (station) Reserve(_ context.Context, request *stationv1.ReserveRequest) (*stationv1.ReserveResponse, error) {
	return &stationv1.ReserveResponse{AddressId: syntheticID("sa_", request.GetRequestId()), Created: true}, nil
}
func (station) Submit(_ context.Context, request *stationv1.SubmitRequest) (*stationv1.SubmitResponse, error) {
	return &stationv1.SubmitResponse{UnitId: syntheticID("tu_", request.GetRequestId())}, nil
}
func (station) Claim(context.Context, *stationv1.ClaimRequest) (*stationv1.ClaimResponse, error) {
	return &stationv1.ClaimResponse{}, nil
}
func (station) Settle(context.Context, *stationv1.SettleRequest) (*stationv1.SettleResponse, error) {
	return &stationv1.SettleResponse{}, nil
}
func (station) GetCapacity(context.Context, *stationv1.GetCapacityRequest) (*stationv1.GetCapacityResponse, error) {
	return &stationv1.GetCapacityResponse{ProfileId: "synthetic-extension-harness-v1", Pressure: stationv1.Pressure_PRESSURE_NORMAL}, nil
}

func main() {
	listen := flag.String("listen", "127.0.0.1:0", "loopback test listener")
	flag.Parse()
	host, _, err := net.SplitHostPort(*listen)
	if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
		_, _ = fmt.Fprintln(os.Stderr, "SYNTHETIC_STATION_CONFIG_INVALID")
		os.Exit(1)
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "SYNTHETIC_STATION_LISTEN_FAILED")
		os.Exit(1)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(4<<20))
	stationv1.RegisterStationServer(server, station{})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() { <-ctx.Done(); server.GracefulStop() }()
	if err := server.Serve(listener); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "SYNTHETIC_STATION_SERVE_FAILED")
		os.Exit(1)
	}
}

func syntheticID(prefix, seed string) string {
	digest := sha256.Sum256([]byte(seed))
	return prefix + base64.RawURLEncoding.EncodeToString(digest[:16])
}
