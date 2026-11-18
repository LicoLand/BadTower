package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/LicoLand/BadTower/internal/clusterapi"
	"github.com/LicoLand/BadTower/internal/nativeapi"
	"github.com/LicoLand/BadTower/internal/stationcluster"
	"github.com/LicoLand/BadTower/internal/stationcore"
	"google.golang.org/grpc"
)

const (
	settleDrainTimeout = 10 * time.Second
	hardStopTimeout    = 30 * time.Second
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("BadTower stopped", "error", err)
		os.Exit(1)
	}
}

func run(arguments []string) error {
	flagSet := flag.NewFlagSet("badtower", flag.ContinueOnError)
	configPath := flagSet.String("config", "", "strict badtower.station.config.v1 path")
	clusterConfigPath := flagSet.String("cluster-config", "", "strict badtower.station-cluster.node.v1 path")
	if err := flagSet.Parse(arguments); err != nil {
		return err
	}
	if *configPath == "" {
		return errors.New("--config is required")
	}

	config, err := nativeapi.LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	var station nativeapi.Station
	var closer stationCloser
	var peerListener net.Listener
	var clusterNode *stationcluster.Node
	var clusterConfig stationcluster.NodeConfig
	var clusterOperatorListener net.Listener
	var clusterOperatorServer *grpc.Server
	if *clusterConfigPath == "" {
		core, err := stationcore.Open(stationcore.Config{DataRoot: config.DataRoot, ProfileID: config.Profile}, systemClock{})
		if err != nil {
			return fmt.Errorf("open station core: %w", err)
		}
		station, closer = core, core
	} else {
		clusterConfig, err = stationcluster.LoadNodeConfig(*clusterConfigPath)
		if err != nil {
			return fmt.Errorf("load cluster config: %w", err)
		}
		clusterNode, err = stationcluster.OpenNodeFromConfig(clusterConfig)
		if err != nil {
			return fmt.Errorf("open cluster node: %w", err)
		}
		peerListener, err = net.Listen("tcp", clusterConfig.PeerListen)
		if err != nil {
			_ = clusterNode.Close()
			return fmt.Errorf("open peer listener: %w", err)
		}
		station, closer = clusterNode.Router(), clusterNode
		if clusterConfig.Operator != nil && clusterConfig.Operator.Enabled {
			clusterOperator, openErr := stationcluster.NewOperator(clusterNode, clusterNode.PartitionMembership(), 1, 1)
			if openErr != nil {
				_ = peerListener.Close()
				_ = clusterNode.Close()
				return openErr
			}
			apiServer, openErr := clusterapi.NewServer(clusterOperator, clusterConfig.Operator.CredentialSHA256)
			if openErr != nil {
				_ = peerListener.Close()
				_ = clusterNode.Close()
				return openErr
			}
			clusterOperatorListener, openErr = nativeapi.OpenListener(nativeapi.TransportConfig{Mode: "unix", SocketPath: clusterConfig.Operator.SocketPath})
			if openErr != nil {
				_ = peerListener.Close()
				_ = clusterNode.Close()
				return openErr
			}
			clusterOperatorServer = apiServer.GRPCServer()
		}
	}

	auth := nativeapi.NewAuthenticator(config)
	server := nativeapi.NewServer(station, config, auth)

	dataListener, healthListener, metricsListener, operatorListener, err := openStationListeners(config)
	if err != nil {
		if clusterOperatorListener != nil {
			_ = clusterOperatorListener.Close()
		}
		if peerListener != nil {
			_ = peerListener.Close()
		}
		_ = closer.Close()
		return err
	}

	var tlsConfig *tls.Config
	if config.Transport.Mode == "tcp" {
		tlsConfig, err = nativeapi.LoadTLSConfig(config.Transport.TLSCertificateFile, config.Transport.TLSPrivateKeyFile)
		if err != nil {
			closeListeners(dataListener, healthListener, metricsListener, operatorListener)
			if clusterOperatorListener != nil {
				_ = clusterOperatorListener.Close()
			}
			if peerListener != nil {
				_ = peerListener.Close()
			}
			_ = closer.Close()
			return fmt.Errorf("load tls config: %w", err)
		}
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Listener cancellation is intentionally separate from the signal context:
	// a signal first enters DRAINING and leaves bounded Settle available before
	// auxiliary listeners are shut down.
	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 6)
	var serveGroup sync.WaitGroup
	serve := func(fn func() error) {
		serveGroup.Add(1)
		go func() {
			defer serveGroup.Done()
			errCh <- fn()
		}()
	}
	serve(func() error { return server.ServeData(serveCtx, dataListener, tlsConfig) })
	serve(func() error { return server.ServeHealth(serveCtx, healthListener) })
	serve(func() error { return server.ServeMetrics(serveCtx, metricsListener) })
	if operatorListener != nil {
		serve(func() error { return server.ServeOperator(serveCtx, operatorListener) })
	}
	if clusterNode != nil {
		serve(func() error { return clusterNode.ServePeerContext(serveCtx, peerListener) })
	}
	if clusterOperatorServer != nil {
		serve(func() error {
			done := make(chan struct{})
			go func() {
				select {
				case <-serveCtx.Done():
					clusterOperatorServer.GracefulStop()
				case <-done:
				}
			}()
			err := clusterOperatorServer.Serve(clusterOperatorListener)
			close(done)
			return err
		})
	}
	serveDone := make(chan struct{})
	go func() {
		serveGroup.Wait()
		close(serveDone)
	}()
	server.SetLifecycleRunning()

	slog.Info("BadTower native station started", "profile", config.Profile, "operator", operatorListener != nil)

	select {
	case <-signalCtx.Done():
		return drainStation(server, closer, cancel, serveDone)
	case err := <-errCh:
		cleanupErr := drainStation(server, closer, cancel, serveDone)
		if err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		return cleanupErr
	}
}

type stationCloser interface{ Close() error }

func drainStation(server *nativeapi.Server, closer stationCloser, cancel context.CancelFunc, serveDone <-chan struct{}) error {
	return drainStationWithTimeouts(server, closer, cancel, serveDone, settleDrainTimeout, hardStopTimeout)
}

func drain(server *nativeapi.Server, core *stationcore.Core, cancel context.CancelFunc, serveDone <-chan struct{}) error {
	return drainStation(server, core, cancel, serveDone)
}

// drainWithTimeouts keeps the native settlement window and the total process
// deadline independent. The hard deadline starts with BeginDrain, bounds both
// gRPC graceful stop and auxiliary HTTP shutdown, and force-closes listeners
// before the Station Core store is closed.
func drainWithTimeouts(server *nativeapi.Server, core *stationcore.Core, cancel context.CancelFunc, serveDone <-chan struct{}, settleBudget, hardBudget time.Duration) error {
	return drainStationWithTimeouts(server, core, cancel, serveDone, settleBudget, hardBudget)
}

func drainStationWithTimeouts(server *nativeapi.Server, closer stationCloser, cancel context.CancelFunc, serveDone <-chan struct{}, settleBudget, hardBudget time.Duration) error {
	if settleBudget < 0 {
		settleBudget = 0
	}
	if hardBudget < 0 {
		hardBudget = 0
	}
	server.BeginDrain()
	hardTimer := time.NewTimer(hardBudget)
	defer hardTimer.Stop()

	if settleBudget > 0 {
		settleTimer := time.NewTimer(settleBudget)
		select {
		case <-settleTimer.C:
		case <-serveDone:
			settleTimer.Stop()
		case <-hardTimer.C:
			settleTimer.Stop()
			cancel()
			server.HardStop()
			server.MarkStopped()
			return closeStation(closer)
		}
	}

	gracefulDone := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(gracefulDone)
	}()
	select {
	case <-gracefulDone:
		cancel()
	case <-hardTimer.C:
		cancel()
		server.HardStop()
		server.MarkStopped()
		return closeStation(closer)
	}

	select {
	case <-serveDone:
	case <-hardTimer.C:
		server.HardStop()
	}
	server.MarkStopped()
	return closeStation(closer)
}

func closeStation(closer stationCloser) error {
	if err := closer.Close(); err != nil {
		return fmt.Errorf("close station: %w", err)
	}
	return nil
}

func closeStationCore(core *stationcore.Core) error {
	return closeStation(core)
}

// beginDrain stops admission in the native API and the Station Core without
// waiting: the server lifecycle turns DRAINING so Reserve/Submit/Claim reject,
// and the core data plane rejects admissions with the same reason. The drain
// budget is unaffected: the operator listener keeps serving the DRAINING
// overview until serveCtx closes.
func beginDrain(server *nativeapi.Server, _ *stationcore.Core) {
	server.BeginDrain()
}

// openStationListeners opens the process listeners in fixed order: data,
// health, metrics, and the optional loopback operator listener. On any failure
// every already-opened listener is closed before returning, so a partial
// startup never leaks a socket. When the operator block is absent or disabled
// no operator listener is opened at all.
func openStationListeners(config nativeapi.Config) (data, health, metrics, operator net.Listener, err error) {
	data, err = nativeapi.OpenListener(config.Transport)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("open data listener: %w", err)
	}
	health, err = nativeapi.OpenTCPListener(config.Health.Listen)
	if err != nil {
		closeListeners(data)
		return nil, nil, nil, nil, fmt.Errorf("open health listener: %w", err)
	}
	metrics, err = nativeapi.OpenTCPListener(config.Metrics.Listen)
	if err != nil {
		closeListeners(data, health)
		return nil, nil, nil, nil, fmt.Errorf("open metrics listener: %w", err)
	}
	if config.Operator != nil && config.Operator.Enabled {
		operator, err = nativeapi.OpenTCPListener(config.Operator.Listen)
		if err != nil {
			closeListeners(data, health, metrics)
			return nil, nil, nil, nil, fmt.Errorf("open operator listener: %w", err)
		}
	}
	return data, health, metrics, operator, nil
}

// closeListeners closes every opened listener, tolerating nil entries, and
// ignores close errors: cleanup must never mask the startup failure that
// triggered it.
func closeListeners(listeners ...net.Listener) {
	for _, listener := range listeners {
		if listener != nil {
			_ = listener.Close()
		}
	}
}

type systemClock struct{}

func (systemClock) NowUnixMs() int64 { return time.Now().UTC().UnixMilli() }

func (systemClock) MonotonicNow() time.Duration { return time.Since(processStart) }

var processStart = time.Now()

// CheckConfig validates configuration without opening the data root.
func CheckConfig(path string) error {
	_, err := nativeapi.LoadConfig(path)
	return err
}

func CheckClusterConfig(path string) error {
	_, err := stationcluster.LoadNodeConfig(path)
	return err
}
