package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/LicoLand/BadTower/internal/profile"
	"github.com/LicoLand/BadTower/internal/station"
)

const (
	mailboxID  = "box_000000000001"
	envelopeID = "env_000000000001"
)

func TestOpaqueTransportHTTPFlow(t *testing.T) {
	t.Parallel()
	now := time.Date(2029, 12, 31, 23, 0, 0, 0, time.UTC)
	instance, err := station.Open(
		filepath.Join(t.TempDir(), "station.db"),
		profile.Default(),
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatalf("station.Open: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	server := httptest.NewServer(New(instance))
	t.Cleanup(server.Close)

	assertRequest(t, http.MethodPost, server.URL+"/v1/mailboxes/"+mailboxID+"/lease",
		`{"leaseSeconds":60}`, http.StatusOK)
	envelope := `{"contractVersion":"licoarc.relay.v1",` +
		`"envelopeId":"` + envelopeID + `",` +
		`"mailboxId":"` + mailboxID + `",` +
		`"ciphertext":"synthetic-ciphertext",` +
		`"expiresAt":"2030-01-01T00:00:00.000Z"}`
	assertRequest(t, http.MethodPost, server.URL+"/v1/envelopes",
		envelope, http.StatusAccepted)
	response := assertRequest(t, http.MethodGet,
		server.URL+"/v1/mailboxes/"+mailboxID+"/envelopes?limit=1",
		"", http.StatusOK)
	var received struct {
		Envelopes []station.Envelope `json:"envelopes"`
	}
	if err := json.Unmarshal(response, &received); err != nil {
		t.Fatalf("decode receive: %v", err)
	}
	if len(received.Envelopes) != 1 || received.Envelopes[0].EnvelopeID != envelopeID {
		t.Fatalf("received = %#v", received.Envelopes)
	}
	assertRequest(t, http.MethodDelete,
		server.URL+"/v1/mailboxes/"+mailboxID+"/envelopes/"+envelopeID,
		"", http.StatusOK)
}

func TestHTTPRejectsAuthorityBearingAndMalformedInput(t *testing.T) {
	t.Parallel()
	instance, err := station.Open(
		filepath.Join(t.TempDir(), "station.db"),
		profile.Default(),
		func() time.Time {
			return time.Date(2029, 12, 31, 23, 0, 0, 0, time.UTC)
		},
	)
	if err != nil {
		t.Fatalf("station.Open: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	server := httptest.NewServer(New(instance))
	t.Cleanup(server.Close)

	for _, forbiddenField := range []string{
		"plaintext",
		"clientKey",
		"permissionGrant",
		"encryptionAlgorithm",
	} {
		assertRequest(t, http.MethodPost, server.URL+"/v1/envelopes",
			`{"contractVersion":"licoarc.relay.v1","`+forbiddenField+`":"forbidden"}`,
			http.StatusBadRequest)
	}
	assertRequest(t, http.MethodPost, server.URL+"/v1/envelopes",
		`{"contractVersion":"licoarc.relay.v1"}{"extra":true}`,
		http.StatusBadRequest)
	assertRequest(t, http.MethodGet,
		server.URL+"/v1/mailboxes/"+mailboxID+"/envelopes?limit=101",
		"", http.StatusBadRequest)
}

func TestHealthDoesNotExposeRuntimeData(t *testing.T) {
	t.Parallel()
	instance, err := station.Open(
		filepath.Join(t.TempDir(), "station.db"),
		profile.Default(),
		time.Now,
	)
	if err != nil {
		t.Fatalf("station.Open: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close() })
	server := httptest.NewServer(New(instance))
	t.Cleanup(server.Close)
	response := assertRequest(t, http.MethodGet, server.URL+"/healthz", "", http.StatusOK)
	if string(bytes.TrimSpace(response)) != `{"status":"ok"}` {
		t.Fatalf("health response = %s", response)
	}
}

func assertRequest(
	t *testing.T,
	method, url, body string,
	expectedStatus int,
) []byte {
	t.Helper()
	request, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			t.Errorf("close response: %v", err)
		}
	}()
	var decoded bytes.Buffer
	if _, err := decoded.ReadFrom(response.Body); err != nil {
		t.Fatalf("read response: %v", err)
	}
	if response.StatusCode != expectedStatus {
		t.Fatalf("%s %s status = %d, body = %s", method, url, response.StatusCode, decoded.String())
	}
	return decoded.Bytes()
}
