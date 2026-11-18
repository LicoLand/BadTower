# BadTower Runbook

BadTower ships as a Go native Station service with a local Pebble store.
This runbook covers routine verification and service-local operation.

## Prerequisites

- Go 1.26 or newer, as declared in `go.mod`.
- A clean checkout of the repository.
- One strict `badtower.station.config.v1` file with namespace credentials,
  transport, health, and metrics listeners.

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

## Configuration validation

Validate configuration before starting the service:

```sh
badtowerctl check-config --config /path/to/badtower.station.v1.json
```

Configuration is explicit, bounded, and fail closed. Unknown fields, retired
data roots, invalid namespace or credential shapes, and non-loopback listeners
without explicit allowance stop startup instead of being coerced.

Generate a namespace bearer credential (mode `0600` output):

```sh
badtowerctl credential generate --output /path/to/credential.txt
```

Map the raw token to a credential digest and bind it in the namespace section
of the configuration file. Credentials are never accepted from argv or
environment variables.

## Starting the service

```sh
badtower --config /path/to/badtower.station.v1.json
```

The default configuration uses loopback listeners and Unix socket transport to
prevent accidental public exposure. Production operators must provide their
own TLS certificates, access controls, process supervision, backups, and
resource isolation. The data root holds service-local opaque transport state,
not end-to-end delivery evidence.

## Native client operations

Use `badtowerctl` against the configured gRPC transport with a mode-`0600`
credential file:

```sh
badtowerctl reserve \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_AAAAAAAAAAAAAAAAAAAAAA \
  --ttl-ms 86400000

badtowerctl submit \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_BBBBBBBBBBBBBBBBBBBBBB \
  --address-id sa_CCCCCCCCCCCCCCCCCCCCCC \
  --payload-file /path/to/payload.bin \
  --ttl-ms 3600000

badtowerctl claim \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_DDDDDDDDDDDDDDDDDDDDDD \
  --address-id sa_CCCCCCCCCCCCCCCCCCCCCC \
  --max-units 128 \
  --max-bytes 4194304 \
  --visibility-timeout-ms 60000 \
  --wait-timeout-ms 5000

badtowerctl settle \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_EEEEEEEEEEEEEEEEEEEEEE \
  --claim-id dc_FFFFFFFFFFFFFFFFFFFFFF \
  --claim-epoch 1

badtowerctl capacity \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt
```

All request, address, unit, and claim identifiers must match the native ID
patterns enforced by the contract.

## Standard node package verification

The package command has three separate operations:

```sh
go run ./cmd/badtower-package create --help
go run ./cmd/badtower-package sign --help
go run ./cmd/badtower-package verify --help
```

Creation requires an already built BadTower binary, the repository license,
an explicit SemVer, and one explicit `GOOS-GOARCH` target. Signing accepts
only an unencrypted PKCS#8 Ed25519 private key. Verification accepts only the
matching PKIX public key and checks the signature before inspecting the
archive.

Never commit a signing key or include one in an archive, manifest, receipt,
log, fixture, or support report. A successful local verification receipt is
limited to public digest and verification metadata and does not establish
publication.

## Health and shutdown

Standard gRPC health probes the configured health listener. Readiness falls
before drain; new Reserve, Submit, and Claim RPCs stop during drain; Settle
remains available for a bounded window; hard stop follows the closed lifecycle
deadlines.

`SIGINT` and `SIGTERM` trigger bounded graceful shutdown. Expired state is
reclaimed incrementally while the service runs.

## External extension candidate verification

External tooling is never required to start or use `badtower`:

```sh
go test -race ./pkg/extension/v1 ./internal/pluginpackage ./tools/extension-harness/...
go run ./cmd/badtower-plugin-package descriptor --help
go run ./cmd/badtower-plugin-package create --help
go run ./cmd/badtower-plugin-package sign --help
go run ./cmd/badtower-plugin-package verify --help
```

Build an exact plugin binary first, then generate its descriptor from that
binary so the SHA-256 field cannot become stale. Keep config, credentials,
signing keys, state roots, runtime data, and protocol corpora outside the
descriptor and archive. A successful local receipt is not publication.

## Exact LicoArc Candidate adapter

The adapter is a separate process and remains production-quarantined while the
Native Excellence Gate is open. Focused verification is:

```sh
go test -race ./plugins/licoarc-candidate/...
go build -trimpath ./plugins/licoarc-candidate/cmd/badtower-licoarc-candidate
```

Its strict external config owns the TLS 1.3 listener certificate, scoped native
endpoint and bearer, opaque namespace, independent Pebble state root, Ed25519
Station-statement key, and descriptor validity bound. Never place that config,
credential, private key, or state in the plugin package or a verification
receipt.
