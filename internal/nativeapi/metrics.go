package nativeapi

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// MetricsServer exposes privacy-minimal OpenMetrics (M-01).
type MetricsServer struct {
	server *Server
	mu     sync.Mutex
	http   *http.Server
}

// NewMetricsServer constructs one metrics listener.
func NewMetricsServer(server *Server) *MetricsServer {
	return &MetricsServer{server: server}
}

// Serve exposes GET /metrics until ctx is canceled.
func (metrics *MetricsServer) Serve(ctx context.Context, listener net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", metrics.handleMetrics)
	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 2 * http.DefaultClient.Timeout,
	}
	if httpServer.ReadHeaderTimeout == 0 {
		httpServer.ReadHeaderTimeout = 2_000_000_000
	}
	metrics.mu.Lock()
	metrics.http = httpServer
	metrics.mu.Unlock()
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			_ = httpServer.Close()
		}
		close(done)
	}()
	err := httpServer.Serve(listener)
	metrics.mu.Lock()
	if metrics.http == httpServer {
		metrics.http = nil
	}
	metrics.mu.Unlock()
	if err != nil && err != http.ErrServerClosed {
		return err
	}
	if ctx.Err() != nil {
		<-done
	}
	return nil
}

// Close force-closes metrics serving when the process hard-stop budget ends.
func (metrics *MetricsServer) Close() {
	metrics.mu.Lock()
	httpServer := metrics.http
	metrics.mu.Unlock()
	if httpServer != nil {
		_ = httpServer.Close()
	}
}

func (metrics *MetricsServer) handleMetrics(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		http.NotFound(writer, request)
		return
	}
	writer.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	lifecycle := metrics.server.Lifecycle()
	fmt.Fprintf(writer, "# HELP badtower_station_lifecycle_state Process lifecycle state\n")
	fmt.Fprintf(writer, "# TYPE badtower_station_lifecycle_state gauge\n")
	fmt.Fprintf(writer, "badtower_station_lifecycle_state{state=%q} %d\n", lifecycleLabel(lifecycle), int64(lifecycle))
	fmt.Fprintf(writer, "# HELP badtower_station_in_flight_rpcs In-flight RPC count\n")
	fmt.Fprintf(writer, "# TYPE badtower_station_in_flight_rpcs gauge\n")
	fmt.Fprintf(writer, "badtower_station_in_flight_rpcs %d\n", metrics.server.inFlight.Load())
	fmt.Fprintf(writer, "# HELP badtower_station_connections Active connection count\n")
	fmt.Fprintf(writer, "# TYPE badtower_station_connections gauge\n")
	fmt.Fprintf(writer, "badtower_station_connections %d\n", metrics.server.connections.Load())
}

func lifecycleLabel(state LifecycleState) string {
	switch state {
	case LifecycleStarting:
		return "STARTING"
	case LifecycleRunning:
		return "RUNNING"
	case LifecycleDraining:
		return "DRAINING"
	case LifecycleStopped:
		return "STOPPED"
	default:
		return "UNKNOWN"
	}
}

// inFlightCounter supports metrics instrumentation from interceptors.
type inFlightCounter struct {
	value atomic.Int64
}

func (counter *inFlightCounter) Add(delta int64) { counter.value.Add(delta) }
func (counter *inFlightCounter) Load() int64     { return counter.value.Load() }
