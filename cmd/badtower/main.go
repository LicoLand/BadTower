package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/LicoLand/BadTower/internal/httpapi"
	"github.com/LicoLand/BadTower/internal/profile"
	"github.com/LicoLand/BadTower/internal/station"
)

func main() {
	if err := run(); err != nil {
		slog.Error("BadTower stopped", "error", err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	listenAddress := flag.String(
		"listen",
		environmentOrDefault("BADTOWER_LISTEN", "127.0.0.1:8080"),
		"HTTP listen address",
	)
	dataPath := flag.String(
		"data",
		environmentOrDefault("BADTOWER_DATA_PATH", "data/badtower.db"),
		"durable station database path",
	)
	profilePath := flag.String(
		"profile",
		os.Getenv("BADTOWER_PROFILE_PATH"),
		"optional strict JSON relay profile path",
	)
	flag.Parse()

	relayProfile, err := loadProfile(*profilePath)
	if err != nil {
		return err
	}
	instance, err := station.Open(*dataPath, relayProfile, time.Now)
	if err != nil {
		return err
	}
	defer func() {
		runErr = errors.Join(runErr, instance.Close())
	}()

	server := &http.Server{
		Addr:              *listenAddress,
		Handler:           httpapi.New(instance),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	shutdownContext, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	cleanupDone := make(chan struct{})
	go runCleanup(shutdownContext, instance, cleanupDone)

	serverError := make(chan error, 1)
	go func() {
		slog.Info("BadTower station started")
		serverError <- server.ListenAndServe()
	}()

	select {
	case <-shutdownContext.Done():
		timeout, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(timeout); err != nil {
			return fmt.Errorf("shutdown HTTP server: %w", err)
		}
		<-cleanupDone
		return nil
	case err := <-serverError:
		stop()
		<-cleanupDone
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve HTTP: %w", err)
	}
}

func runCleanup(ctx context.Context, instance *station.Station, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := instance.Cleanup(); err != nil {
				slog.Warn("station cleanup failed")
			}
		}
	}
}

func loadProfile(path string) (profile.Profile, error) {
	if strings.TrimSpace(path) == "" {
		return profile.Default(), nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return profile.Profile{}, fmt.Errorf("open relay profile: %w", err)
	}
	if len(content) > 64*1024 {
		return profile.Profile{}, errors.New("relay profile exceeds size limit")
	}
	decoder := json.NewDecoder(strings.NewReader(string(content)))
	decoder.DisallowUnknownFields()
	var config profile.Config
	if err := decoder.Decode(&config); err != nil {
		return profile.Profile{}, fmt.Errorf("decode relay profile: %w", err)
	}
	var remainder any
	if err := decoder.Decode(&remainder); !errors.Is(err, io.EOF) {
		return profile.Profile{}, errors.New("relay profile must contain one JSON value")
	}
	value, err := profile.New(config)
	if err != nil {
		return profile.Profile{}, fmt.Errorf("validate relay profile: %w", err)
	}
	return value, nil
}

func environmentOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}
