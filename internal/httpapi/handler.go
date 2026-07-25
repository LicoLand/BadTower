// Package httpapi adapts BadTower's station-local HTTP surface to the durable
// opaque transport implementation.
package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/LicoLand/BadTower/internal/station"
)

const maxRequestBytes = 1_114_112

type transportStation interface {
	Lease(mailboxID string, requestedSeconds int64) (station.Lease, error)
	Deliver(envelope station.Envelope) (station.Delivery, error)
	Receive(mailboxID string, limit int) ([]station.Envelope, error)
	Acknowledge(mailboxID, envelopeID string) (station.Acknowledgement, error)
}

type Handler struct {
	station transportStation
	mux     *http.ServeMux
}

func New(transport transportStation) *Handler {
	handler := &Handler{station: transport, mux: http.NewServeMux()}
	handler.mux.HandleFunc("GET /healthz", handler.health)
	handler.mux.HandleFunc("POST /v1/mailboxes/{mailboxId}/lease", handler.lease)
	handler.mux.HandleFunc("POST /v1/envelopes", handler.deliver)
	handler.mux.HandleFunc("GET /v1/mailboxes/{mailboxId}/envelopes", handler.receive)
	handler.mux.HandleFunc(
		"DELETE /v1/mailboxes/{mailboxId}/envelopes/{envelopeId}",
		handler.acknowledge,
	)
	return handler
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Cache-Control", "no-store")
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	handler.mux.ServeHTTP(writer, request)
}

func (handler *Handler) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
}

func (handler *Handler) lease(writer http.ResponseWriter, request *http.Request) {
	var input struct {
		LeaseSeconds int64 `json:"leaseSeconds"`
	}
	if err := readJSON(writer, request, &input); err != nil {
		writeProblem(writer, err)
		return
	}
	result, err := handler.station.Lease(request.PathValue("mailboxId"), input.LeaseSeconds)
	if err != nil {
		writeProblem(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (handler *Handler) deliver(writer http.ResponseWriter, request *http.Request) {
	var envelope station.Envelope
	if err := readJSON(writer, request, &envelope); err != nil {
		writeProblem(writer, err)
		return
	}
	result, err := handler.station.Deliver(envelope)
	if err != nil {
		writeProblem(writer, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (handler *Handler) receive(writer http.ResponseWriter, request *http.Request) {
	limit := 100
	if raw := request.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeProblem(writer, station.ErrInvalid)
			return
		}
		limit = parsed
	}
	envelopes, err := handler.station.Receive(request.PathValue("mailboxId"), limit)
	if err != nil {
		writeProblem(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, struct {
		Envelopes []station.Envelope `json:"envelopes"`
	}{Envelopes: envelopes})
}

func (handler *Handler) acknowledge(writer http.ResponseWriter, request *http.Request) {
	result, err := handler.station.Acknowledge(
		request.PathValue("mailboxId"),
		request.PathValue("envelopeId"),
	)
	if err != nil {
		writeProblem(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func readJSON(writer http.ResponseWriter, request *http.Request, target any) error {
	request.Body = http.MaxBytesReader(writer, request.Body, maxRequestBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.Join(station.ErrInvalid, err)
	}
	var remainder any
	if err := decoder.Decode(&remainder); !errors.Is(err, io.EOF) {
		return errors.Join(station.ErrInvalid, errors.New("request must contain one JSON value"))
	}
	return nil
}

func writeProblem(writer http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	code := "internal_error"
	switch {
	case errors.Is(err, station.ErrInvalid):
		status, code = http.StatusBadRequest, "invalid_request"
	case errors.Is(err, station.ErrLease):
		status, code = http.StatusConflict, "lease_required"
	case errors.Is(err, station.ErrConflict):
		status, code = http.StatusConflict, "transport_conflict"
	case errors.Is(err, station.ErrQuota):
		status, code = http.StatusTooManyRequests, "station_limit"
	}
	writeJSON(writer, status, struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}{
		Error: struct {
			Code string `json:"code"`
		}{Code: code},
	})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
