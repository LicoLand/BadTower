# BadTower

**[简体中文](README.zh-CN.md)** — English is the normative language of this
README pair; the Simplified Chinese version is the localized language version.

BadTower is a general-purpose, independently operated communication station in
the Lico Arc Network. It is assumed to be malicious by every endpoint and
stores and forwards only opaque Lico Arc Protocol transport units.

LicoUp and other endpoints may pass traffic through a BadTower, but never
integrate with it as a trusted product or delegate identity, encryption,
integrity, delivery, or security authority to it. HTTP responses, storage,
queues, leases, acknowledgements, and receipts are station-local,
untrusted transport hints. Endpoint security remains end to end.

BadTower has no source, build, vendored-artifact, or runtime dependency on the
LicoArc repository. Its default local relay profile accepts the published
`licoarc.relay.v1` wire identifier, while BadTower owns its operational limits
and release lifecycle.

BadTower is implemented in Go. Its HTTP service persists opaque transport
units in a local bbolt database and exposes station-local lease, delivery,
receive, and acknowledgement operations.

Run the service:

```sh
go run ./cmd/badtower
```

Run repository verification:

```sh
go test -race ./...
go vet ./...
go build ./...
```

## Documentation

- [Product overview](PRODUCT.md)
- [Documentation index](docs/README.md)
- [Runbook](docs/RUNBOOK.md)
- [Compatibility](docs/COMPATIBILITY.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

License: [AGPL-3.0-or-later](LICENSE).
