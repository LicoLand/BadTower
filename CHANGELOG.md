# Changelog

All notable changes to BadTower are documented in this file. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Go HTTP station service with bounded request handling and graceful shutdown.
- Durable transactional mailbox, lease, ordered-envelope, deduplication,
  acknowledgement, quota, expiry, and cleanup state backed by bbolt.
- Strict optional JSON profile loading and explicit service configuration.
- Go-native branch-flow policy verification and continuous integration.

### Changed

- Complete the production migration to Go and remove the superseded runtime,
  package metadata, tests, verification tools, and compatibility surface.

### Security

- Remove the vendored LicoArc artifact, digest pin, sibling release coupling,
  and runtime artifact loader.
- Move accepted wire identifiers and operational limits into a closed,
  immutable, BadTower-owned relay profile with hard implementation ceilings.
- Require construction through the validated local profile factory.
- Enforce RFC 3339 expiry, maximum retention, relay-wide mailbox and envelope
  quotas, conflict-safe deduplication, and automatic expired-entry pruning.
- Reject unknown, missing, malformed, and otherwise non-contract JSON envelope
  shapes before retention.

## [0.1.0] - 2026-07-24

### Added

- Initial publication of BadTower as an intentionally untrusted communication
  station.
- Initial mailbox relay behavior with leases, deduplicated delivery, bounded
  retention, acknowledgements, expiry, and cleanup.
- Closed opaque-envelope validation and bounded local relay limits.
- Initial boundary verification and transport behavior tests.
- Documentation governance structure under `docs/`.
