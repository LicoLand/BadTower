package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	stationv1 "github.com/LicoLand/BadTower/gen/go/badtower/station/v1"
	licoarccandidate "github.com/LicoLand/BadTower/plugins/licoarc-candidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	maxConfigBytes = 64 << 10
	configSchema   = "badtower.licoarc-candidate.config.v1"
)

type config struct {
	Schema   string `json:"schema"`
	Listener struct {
		Address         string `json:"address"`
		CertificatePath string `json:"certificatePath"`
		PrivateKeyPath  string `json:"privateKeyPath"`
	} `json:"listener"`
	Native struct {
		Endpoint   string `json:"endpoint"`
		ServerName string `json:"serverName"`
		CAPath     string `json:"caPath"`
		Namespace  string `json:"namespace"`
		Bearer     string `json:"bearer"`
	} `json:"native"`
	StateRoot               string `json:"stateRoot"`
	SigningKeyPath          string `json:"signingKeyPath"`
	StationDescriptorDigest string `json:"stationDescriptorDigest"`
	DescriptorNotAfter      uint64 `json:"descriptorNotAfter"`
	Budgets                 struct {
		MaxConcurrentOperations uint32 `json:"maxConcurrentOperations"`
		MaxQueuedOperations     uint32 `json:"maxQueuedOperations"`
		MaxStateBytes           uint64 `json:"maxStateBytes"`
	} `json:"budgets"`
}

func main() {
	configPath := flag.String("config", "", "path to the external adapter configuration")
	flag.Parse()
	if *configPath == "" {
		fatal("CONFIG_REQUIRED")
	}
	value, err := loadConfig(*configPath)
	if err != nil {
		fatal("CONFIG_INVALID")
	}
	nativeTLS, err := clientTLS(value.Native.CAPath, value.Native.ServerName)
	if err != nil {
		fatal("NATIVE_TLS_INVALID")
	}
	connection, err := grpc.NewClient(value.Native.Endpoint,
		grpc.WithTransportCredentials(credentials.NewTLS(nativeTLS)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(36<<20), grpc.MaxCallSendMsgSize(1<<20)),
	)
	if err != nil {
		fatal("NATIVE_CONNECT_FAILED")
	}
	defer connection.Close()
	signer, err := loadSigner(value.SigningKeyPath)
	if err != nil {
		fatal("SIGNER_INVALID")
	}
	defer clear(signer)
	descriptorDigest, ok := parseDigest(value.StationDescriptorDigest)
	if !ok {
		fatal("CONFIG_INVALID")
	}
	adapter, err := licoarccandidate.NewAdapter(stationv1.NewStationClient(connection), licoarccandidate.Config{
		NativeNamespace: value.Native.Namespace, NativeBearer: value.Native.Bearer,
		StateRoot: value.StateRoot, Signer: signer, StationDescriptorDigest: descriptorDigest, DescriptorNotAfter: value.DescriptorNotAfter,
		MaxConcurrentOperations: value.Budgets.MaxConcurrentOperations,
		MaxQueuedOperations:     value.Budgets.MaxQueuedOperations, MaxStateBytes: value.Budgets.MaxStateBytes,
	})
	if err != nil {
		fatal("STATE_OPEN_FAILED")
	}
	defer adapter.Close()
	handler, err := licoarccandidate.NewHandler(adapter)
	if err != nil {
		fatal("HANDLER_INIT_FAILED")
	}
	listenerCert, err := loadListenerCertificate(value.Listener.CertificatePath, value.Listener.PrivateKeyPath)
	if err != nil {
		fatal("LISTENER_TLS_INVALID")
	}
	listener, err := net.Listen("tcp", value.Listener.Address)
	if err != nil {
		fatal("LISTENER_FAILED")
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    2048,
		TLSConfig: &tls.Config{
			MinVersion:   tls.VersionTLS13,
			MaxVersion:   tls.VersionTLS13,
			Certificates: []tls.Certificate{listenerCert},
			NextProtos:   []string{"h2"},
		},
		ErrorLog: newFixedLogger(),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.ServeTLS(listener, "", "")
	}()
	select {
	case <-ctx.Done():
		shutdownContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if server.Shutdown(shutdownContext) != nil {
			fatal("SHUTDOWN_FAILED")
		}
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			fatal("SERVE_FAILED")
		}
	}
}

func readOwnerOnlyFile(path string, maxBytes int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() == 0 || info.Size() > maxBytes {
		return nil, errors.New("invalid secret file")
	}
	raw, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || len(raw) == 0 || int64(len(raw)) > maxBytes {
		return nil, errors.New("invalid secret file")
	}
	return raw, nil
}

func loadListenerCertificate(certPath, keyPath string) (tls.Certificate, error) {
	certPEM, err := os.ReadFile(certPath)
	if err != nil || len(certPEM) == 0 || len(certPEM) > 1<<20 {
		return tls.Certificate{}, errors.New("invalid certificate")
	}
	keyPEM, err := readOwnerOnlyFile(keyPath, 64<<10)
	if err != nil {
		return tls.Certificate{}, errors.New("invalid private key")
	}
	defer clear(keyPEM)
	return tls.X509KeyPair(certPEM, keyPEM)
}

func loadConfig(path string) (config, error) {
	raw, err := readOwnerOnlyFile(path, maxConfigBytes)
	if err != nil {
		return config{}, errors.New("invalid config")
	}
	if err := rejectDuplicateConfigKeys(raw); err != nil {
		return config{}, err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var value config
	if err := decoder.Decode(&value); err != nil {
		return config{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return config{}, errors.New("trailing config")
	}
	if value.Schema != configSchema || value.Listener.Address == "" || value.Listener.CertificatePath == "" || value.Listener.PrivateKeyPath == "" || value.Native.Endpoint == "" || value.Native.ServerName == "" || value.Native.CAPath == "" || value.Native.Namespace == "" || value.Native.Bearer == "" || value.StateRoot == "" || value.SigningKeyPath == "" || value.StationDescriptorDigest == "" || value.DescriptorNotAfter == 0 || value.Budgets.MaxConcurrentOperations == 0 || value.Budgets.MaxQueuedOperations == 0 || value.Budgets.MaxStateBytes == 0 {
		return config{}, errors.New("incomplete config")
	}
	if len(value.Listener.Address) > 255 || len(value.Native.Endpoint) > 255 || len(value.Native.ServerName) > 253 || len(value.Native.Bearer) > 4096 || len(value.Listener.CertificatePath) > 4096 || len(value.Listener.PrivateKeyPath) > 4096 || len(value.Native.CAPath) > 4096 || len(value.StateRoot) > 4096 || len(value.SigningKeyPath) > 4096 || !validNamespace(value.Native.Namespace) || !validDigest(value.StationDescriptorDigest) || value.DescriptorNotAfter <= uint64(time.Now().Unix()) || value.Budgets.MaxConcurrentOperations > licoarccandidate.MaxConcurrentOperations || value.Budgets.MaxQueuedOperations > licoarccandidate.MaxQueuedOperations || value.Budgets.MaxStateBytes > licoarccandidate.MaxPluginStateBytes {
		return config{}, errors.New("config bounds invalid")
	}
	return value, nil
}

func rejectDuplicateConfigKeys(raw []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if err := walkConfigJSON(decoder); err != nil {
		return errors.New("invalid config")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("trailing config")
	}
	return nil
}

func walkConfigJSON(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delimiter {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("invalid config key")
			}
			if _, duplicate := seen[key]; duplicate {
				return errors.New("duplicate config key")
			}
			seen[key] = struct{}{}
			if err := walkConfigJSON(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return errors.New("invalid config object")
		}
		return nil
	case '[':
		for decoder.More() {
			if err := walkConfigJSON(decoder); err != nil {
				return err
			}
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return errors.New("invalid config array")
		}
		return nil
	default:
		return errors.New("invalid config delimiter")
	}
}

func validNamespace(value string) bool {
	if len(value) != 25 || value[:3] != "ns_" {
		return false
	}
	raw, err := base64.RawURLEncoding.DecodeString(value[3:])
	return err == nil && len(raw) == 16 && base64.RawURLEncoding.EncodeToString(raw) == value[3:]
}

func validDigest(value string) bool {
	_, ok := parseDigest(value)
	return ok
}

func parseDigest(value string) ([32]byte, bool) {
	var digest [32]byte
	raw, err := hex.DecodeString(value)
	if err != nil || len(raw) != len(digest) || hex.EncodeToString(raw) != value {
		return digest, false
	}
	copy(digest[:], raw)
	return digest, true
}

func clientTLS(caPath, serverName string) (*tls.Config, error) {
	content, err := os.ReadFile(caPath)
	if err != nil || len(content) == 0 || len(content) > 1<<20 {
		return nil, errors.New("invalid ca")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(content) {
		return nil, errors.New("invalid ca")
	}
	return &tls.Config{MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13, ServerName: serverName, RootCAs: roots, NextProtos: []string{"h2"}}, nil
}

func loadSigner(path string) (ed25519.PrivateKey, error) {
	content, err := readOwnerOnlyFile(path, 16<<10)
	if err != nil {
		return nil, errors.New("invalid signer")
	}
	defer clear(content)
	block, rest := pem.Decode(content)
	if block == nil || block.Type != "PRIVATE KEY" || len(block.Headers) != 0 || len(bytes.TrimSpace(rest)) != 0 {
		return nil, errors.New("invalid signer")
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	clear(block.Bytes)
	if err != nil {
		return nil, errors.New("invalid signer")
	}
	privateKey, ok := parsed.(ed25519.PrivateKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return nil, errors.New("invalid signer")
	}
	return append(ed25519.PrivateKey(nil), privateKey...), nil
}

type fixedLogger struct{}

func newFixedLogger() *log.Logger { return log.New(fixedLogger{}, "", 0) }
func (fixedLogger) Write(raw []byte) (int, error) {
	_, _ = os.Stderr.WriteString("LICOARC_CARRIER_ERROR\n")
	return len(raw), nil
}
func fatal(code string) { _, _ = fmt.Fprintln(os.Stderr, code); os.Exit(1) }
