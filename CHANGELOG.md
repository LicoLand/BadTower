# Changelog

All notable changes to BadTower are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Native Station Core with fixed-shard Pebble store identity `badtower.station-core.store.v2`.
- Native Station Contract `badtower.station.v1` over gRPC/HTTP2 with Reserve,
  Submit, Claim, Settle, GetCapacity, and standard health.
- Strict process configuration `badtower.station.config.v1` with bearer-token
  namespaces, TLS or Unix transport, and separate health and metrics listeners.
- `badtowerctl` for configuration validation, credential generation, and native
  RPC harness commands.
- Focused native acceptance partitions for station core, native API, and control
  CLI components.
- Deterministic `badtower.node-package.v1` archives binding
  `badtower.station-core.store.v2`, canonical manifests, detached Ed25519
  signing, and bounded independent verification.
- Go-native branch-flow policy verification and continuous integration.
- One deterministic native mutation command machine plus a candidate Station
  Cluster with production control/data-group assembly, 64-partition placement,
  ReadIndex routing, bounded etcd/raft, Core-bearing streamed snapshots,
  dynamic membership/rebalance, distributed backup/restore, rolling-upgrade
  preflight, an independently authenticated operator API/CLI, and mTLS peers.
- External Extension Contract v1 with a closed descriptor, minimal scoped
  native client conventions, bounded synthetic harnesses, and a deterministic
  independently signed plugin package.
- One separately built exact LicoArc Candidate adapter with pinned binding
  digests, strict HTTP/2/TLS 1.3 and deterministic CBOR/raw-packet carrier,
  all six operations, independent bounded state, and crash-safe replay.

### Changed

- Replace the integrated HTTP relay path with the native gRPC Station Core as
  the sole production communication surface.
- Complete the production migration to Go and remove the superseded runtime,
  package metadata, tests, verification tools, and integrated compatibility
  surface.

### Removed

- Integrated HTTP relay routes, compile-time transport adapter registry, and
  bbolt store identity `badtower.station-store.v1`.
- Retired data path `data/badtower.db` and all integrated LicoArc-shaped
  configuration, validation, and documentation.

### Security

- Reject retired store paths and unknown store identities at configuration and
  open time.
- Enforce bearer-token namespace authentication, TLS 1.3 on TCP transport, and
  mode-`0600` Unix socket and credential file permissions.
- Keep extension and protocol code outside the Core import graph; require mTLS
  TLS 1.3 for Raft peers and fail malformed native responses closed in the
  external adapter.
- Reject non-canonical manifests and signatures, mismatched keys or platforms,
  unsafe or duplicate archive entries, links, special files, size violations,
  content tampering, concatenated streams, and trailing data in node packages.

## [0.1.0] - 2026-07-24

Historical pre-native release. Its integrated HTTP relay surface, bbolt store,
and compile-time adapter registry were superseded by the native Station Core
migration recorded under `[Unreleased]`. Do not treat 0.1.0 behavior as current
support or compatibility guidance.
