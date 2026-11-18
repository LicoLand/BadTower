# Examples

All examples use synthetic values maintained in this repository.
The walkthrough exercises the native gRPC Station Contract through
`badtowerctl`. It is not an endpoint-product integration contract or a
protocol-publication claim.

## Example configuration

Create a strict `badtower.station.config.v1` file. This synthetic example uses
Unix socket transport and loopback auxiliary listeners:

```json
{
  "schema": "badtower.station.config.v1",
  "profile": "single-node-general-v1",
  "dataRoot": "data/station-core-v1",
  "transport": {
    "mode": "unix",
    "socketPath": "/run/badtower/native.sock"
  },
  "health": {
    "listen": "127.0.0.1:7444",
    "allowNonLoopback": false
  },
  "metrics": {
    "listen": "127.0.0.1:7445",
    "allowNonLoopback": false
  },
  "namespaces": [
    {
      "id": "ns_AAAAAAAAAAAAAAAAAAAAAA",
      "credentials": [
        {
          "sha256": "DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD",
          "notBefore": "2026-07-15T00:00:00Z",
          "notAfter": "2026-08-14T23:59:59Z"
        }
      ]
    }
  ]
}
```

Generate a bearer credential, compute its SHA-256 digest, and replace the
synthetic digest above before use.

Validate the configuration:

```sh
badtowerctl check-config --config /path/to/badtower.station.v1.json
```

## Start a local station

```sh
badtower --config /path/to/badtower.station.v1.json
```

## Native walkthrough

Reserve a Station Address:

```sh
badtowerctl reserve \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_AAAAAAAAAAAAAAAAAAAAAA \
  --ttl-ms 86400000
```

Submit an opaque Transport Unit:

```sh
badtowerctl submit \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_BBBBBBBBBBBBBBBBBBBBBB \
  --address-id sa_CCCCCCCCCCCCCCCCCCCCCC \
  --payload-file /path/to/synthetic-payload.bin \
  --ttl-ms 3600000
```

Claim a bounded batch:

```sh
badtowerctl claim \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_DDDDDDDDDDDDDDDDDDDDDD \
  --address-id sa_CCCCCCCCCCCCCCCCCCCCCC \
  --max-units 128 \
  --max-bytes 4194304 \
  --visibility-timeout-ms 60000 \
  --wait-timeout-ms 5000
```

Settle the claim:

```sh
badtowerctl settle \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt \
  --request-id rq_EEEEEEEEEEEEEEEEEEEEEE \
  --claim-id dc_FFFFFFFFFFFFFFFFFFFFFF \
  --claim-epoch 1
```

Inspect capacity:

```sh
badtowerctl capacity \
  --config /path/to/badtower.station.v1.json \
  --credential-file /path/to/credential.txt
```

## Verification

- `go test ./...` — package tests.
- `go test -race ./...` — final concurrency regression.
- `go vet ./...` — static checks.
- `go build ./...` — command and package compilation.
