# BadTower Runbook

BadTower ships as a Go HTTP station service with a local transactional bbolt
database. This runbook covers routine verification and service-local
operation.

## Prerequisites

- Go 1.26 or newer, as declared in `go.mod`.
- A clean checkout of the repository.

## Routine verification

Run the final repository closure after targeted package tests:

```sh
go test -race ./...
go vet ./...
go build ./...
```

Format checks must also produce no paths:

```sh
test -z "$(gofmt -l .)"
```

## Relay profile rejection

`internal/profile/profile.go` rejects malformed profiles, unknown profile versions,
duplicate or invalid wire identifiers, unknown fields, and limits above the
BadTower implementation ceilings. When this happens:

1. Do not bypass or loosen the check.
2. Confirm the desired wire contract is compatible with BadTower's closed
   envelope behavior.
3. Add or update independent BadTower behavior tests.
4. Change the local profile in a BadTower release and rerun the repository
   closure.

## Starting the service

```sh
go run ./cmd/badtower \
  -listen 127.0.0.1:8080 \
  -data data/badtower.db
```

The default loopback binding prevents accidental public exposure. Production
operators must provide their own TLS termination, access controls, process
supervision, backups, and resource isolation. The database file is
service-local opaque transport state, not end-to-end delivery evidence.

## Health and shutdown

`GET /healthz` returns only `{"status":"ok"}` and exposes no queue, mailbox,
endpoint, path, or runtime identity. `SIGINT` and `SIGTERM` trigger bounded
graceful HTTP shutdown. Expired state is also cleaned once per minute while
the service runs.
