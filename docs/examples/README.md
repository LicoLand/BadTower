# Examples

All examples use synthetic values maintained in this repository.
The walkthrough exercises the generic station HTTP surface. It is not a
LicoUp integration contract.

## Start a local station

```sh
go run ./cmd/badtower -listen 127.0.0.1:8080 -data data/example.db
```

## Relay walkthrough

Open a mailbox lease:

```sh
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -d '{"leaseSeconds":60}' \
  http://127.0.0.1:8080/v1/mailboxes/box_000000000001/lease
```

Deliver an opaque transport unit:

```sh
curl --fail-with-body \
  -H 'Content-Type: application/json' \
  -d '{"contractVersion":"licoarc.relay.v1","envelopeId":"env_000000000001","mailboxId":"box_000000000001","ciphertext":"synthetic-ciphertext","expiresAt":"2030-01-01T00:00:00Z"}' \
  http://127.0.0.1:8080/v1/envelopes
```

Receive and acknowledge it:

```sh
curl --fail-with-body \
  'http://127.0.0.1:8080/v1/mailboxes/box_000000000001/envelopes?limit=100'
curl --fail-with-body -X DELETE \
  http://127.0.0.1:8080/v1/mailboxes/box_000000000001/envelopes/env_000000000001
```

## Verification

- `go test ./...` — package tests.
- `go test -race ./...` — final concurrency regression.
- `go vet ./...` — static checks.
- `go build ./...` — command and package compilation.
