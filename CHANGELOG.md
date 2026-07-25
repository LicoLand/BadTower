# Changelog

All notable changes to BadTower are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security

- Migrate completely to the immutable Fabrigent v2 artifact, whose verified
  digest binds interpretation metadata, and validate all required governance
  limits.
- Require construction through the verified factory and freeze the loaded
  protocol artifact.
- Enforce RFC 3339 expiry, maximum retention, relay-wide mailbox and envelope
  quotas, conflict-safe deduplication, and automatic expired-entry pruning.
- Reject accessor, inherited, symbol-keyed, missing, and otherwise non-JSON
  envelope shapes before retention.

## [0.1.0] - 2026-07-24

### Added

- Initial publication of BadTower as an intentionally untrusted relay node
  split from the retired LicoTower codebase.
- In-memory mailbox relay with leases, deduplicated delivery, bounded
  retention, acknowledgements, expiry, and cleanup (`src/relay.mjs`).
- Fail-closed consumption of the Fabrigent `v1` artifact pinned by SHA-256
  (`vendor/fabrigent-v1.json`, `src/fabrigent.mjs`).
- Boundary verification tool (`tools/verify-boundary.mjs`) and relay test
  suite (`tests/relay.test.mjs`).
- Documentation governance structure under `docs/`.
