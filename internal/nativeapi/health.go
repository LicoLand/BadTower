package nativeapi

import (
	"context"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const (
	HealthServiceStartup   = "badtower.station.v1.startup"
	HealthServiceLiveness  = "badtower.station.v1.liveness"
	HealthServiceReadiness = "badtower.station.v1.readiness"
)

// HealthServer serves grpc.health.v1 with closed service names (C-12). The
// stock grpc health registry implements Watch, but C-12 requires Watch to be
// UNIMPLEMENTED, so startup/liveness state is owned here while readiness is
// derived dynamically from the same Server source as the operator API.
type HealthServer struct {
	healthpb.UnimplementedHealthServer
	mu     sync.Mutex
	status map[string]healthpb.HealthCheckResponse_ServingStatus
	server *Server
	grpc   *grpc.Server
}

// NewHealthServer constructs one health server bound to the native API process.
func NewHealthServer(server *Server) *HealthServer {
	return &HealthServer{
		status: map[string]healthpb.HealthCheckResponse_ServingStatus{
			"":                    healthpb.HealthCheckResponse_SERVING,
			HealthServiceStartup:  healthpb.HealthCheckResponse_NOT_SERVING,
			HealthServiceLiveness: healthpb.HealthCheckResponse_NOT_SERVING,
		},
		server: server,
	}
}

// SetStartup marks startup health serving when initialization completes.
func (healthServer *HealthServer) SetStartup(serving bool) {
	status := healthpb.HealthCheckResponse_NOT_SERVING
	if serving {
		status = healthpb.HealthCheckResponse_SERVING
	}
	healthServer.mu.Lock()
	healthServer.status[HealthServiceStartup] = status
	healthServer.status[HealthServiceLiveness] = status
	healthServer.mu.Unlock()
}

// Serve exposes health checks until ctx is canceled.
func (healthServer *HealthServer) Serve(ctx context.Context, listener net.Listener) error {
	grpcServer := grpc.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.mu.Lock()
	healthServer.grpc = grpcServer
	healthServer.mu.Unlock()
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		grpcServer.GracefulStop()
		close(done)
	}()
	err := grpcServer.Serve(listener)
	healthServer.mu.Lock()
	if healthServer.grpc == grpcServer {
		healthServer.grpc = nil
	}
	healthServer.mu.Unlock()
	if ctx.Err() != nil {
		<-done
	}
	if err == grpc.ErrServerStopped {
		return nil
	}
	return err
}

// HardStop immediately terminates health serving at the process deadline.
func (healthServer *HealthServer) HardStop() {
	healthServer.mu.Lock()
	grpcServer := healthServer.grpc
	healthServer.mu.Unlock()
	if grpcServer != nil {
		grpcServer.Stop()
	}
}

// Check implements the standard health check entrypoint.
func (healthServer *HealthServer) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, mapError(stationFailure{Reason: "DEADLINE_REQUIRED"})
	}
	switch req.Service {
	case "", HealthServiceStartup, HealthServiceLiveness, HealthServiceReadiness:
	default:
		return nil, mapError(stationFailure{Reason: "INVALID_REQUEST"})
	}
	if req.Service == HealthServiceReadiness {
		serving := healthpb.HealthCheckResponse_NOT_SERVING
		if ready, _ := healthServer.server.Readiness(); ready {
			serving = healthpb.HealthCheckResponse_SERVING
		}
		return &healthpb.HealthCheckResponse{Status: serving}, nil
	}
	healthServer.mu.Lock()
	serving, ok := healthServer.status[req.Service]
	if !ok {
		serving = healthpb.HealthCheckResponse_NOT_SERVING
	}
	healthServer.mu.Unlock()
	return &healthpb.HealthCheckResponse{Status: serving}, nil
}

// Watch is not part of the closed C-12 contract and must be UNIMPLEMENTED.
func (healthServer *HealthServer) Watch(req *healthpb.HealthCheckRequest, stream healthpb.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "watch is not supported")
}
