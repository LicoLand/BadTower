# Compatibility

This document describes the current native compatibility surface.
Lifecycle and publication status are authoritative in
[`STATUS.md`](STATUS.md).

## Current implementation

- Go 1.26 or newer is the current source implementation boundary.
- The implemented executable entry is `badtower --config <path>` or a binary
  built from `cmd/badtower`.
- The Native Station Contract is the service compatibility surface:
  `badtower.station.v1` over gRPC/HTTP2. Internal Go packages are not a client
  SDK contract; use generated bindings or `badtowerctl`.

## Native Station Contract

- Service package: `badtower.station.v1`; service name:
  `badtower.station.v1.Station`.
- RPCs: `Reserve`, `Submit`, `Claim`, `Settle`, and `GetCapacity`.
- Health: standard `grpc.health.v1.Health`.
- Transport: TLS 1.3 with ALPN `h2` on TCP, or Unix socket mode with mode
  `0600`; bearer credentials on every data-plane RPC.
- Unknown service or version returns gRPC `UNIMPLEMENTED`.
- Additive optional Protobuf fields with old default behavior are allowed in
  v1; removed tags and names are reserved; semantic change requires `v2`.
- The contract carries only fixed protocol-neutral transport fields. Arbitrary
  protocol metadata cannot enter Core APIs or persistent state.

## Configuration

- Process configuration schema: `badtower.station.config.v1`.
- Capacity profile: `single-node-general-v1`.
- Data root default: `data/station-core-v1`.
- Retired paths and store identities (`data/badtower.db`,
  `badtower.station-store.v1`) are rejected; BadTower initializes only current
  native state.

## Storage

- Store identity: `badtower.station-core.store.v2`.
- Engine: Pebble v2.1.5 at the configured data root with one exclusive lock.
- Unknown store version fails closed; BadTower does not guess, import, rename,
  or convert retired state.

## Standard node package

- `badtower.node-package.v1` is the closed package-manifest identifier.
- The package target is one explicit lowercase `GOOS-GOARCH` value. Its
  presence in a manifest does not declare a supported platform matrix.
- The deterministic `tar+gzip` archive contains only `bin/badtower` and
  `LICENSE`; their paths, modes, byte counts, and SHA-256 digests are bound by
  the signed canonical manifest.
- `stationStoreFormat` records `badtower.station-core.store.v2`. It is not a
  downgrade, cross-version migration, or rollback guarantee.
- Signatures use detached Ed25519 verification with an explicit public-key
  identifier. Private key handling and publication authorization remain
  outside the package format.
- A locally verified package is not a source tag, package publication,
  support declaration, or hosted deployment.

## External Extension Contract candidate

- Descriptor schema: `badtower.extension-descriptor.v1`.
- Contract range: exactly `1.0.0` for this candidate.
- Package schema: `badtower.external-plugin-package.v1`.
- The descriptor, SDK conventions, harness, package, and every plugin remain
  external to Core and are not part of `badtower.station.v1` compatibility.

## Exact LicoArc Candidate adapter

The separately built `badtower-licoarc-candidate` process pins only
`licoarc.protocol-line.v1`, line `licoarc.v1`, Candidate lifecycle, and
`licoarc.https-transport.v1` with the source digests recorded in
[`protocols/LICOARC-CANDIDATE-ADAPTER.md`](protocols/LICOARC-CANDIDATE-ADAPTER.md).
It accepts no alternate line, profile, lifecycle, runtime bundle, translation,
or downgrade. This implemented source boundary is not production-admitted,
published, supported, or operated while Native Excellence remains open.

A2A, ACP, and MCP remain unimplemented optional plans.

## Versioning

The package follows semantic versioning; see
[CHANGELOG.md](../CHANGELOG.md). Version metadata does not prove a source tag,
package publication, container publication, or hosted operation.
