# Compatibility

## Production implementation

- Go 1.26 or newer is the production implementation boundary.
- The supported executable entry is `go run ./cmd/badtower` or a binary built
  from that package.
- HTTP paths and the closed JSON envelope are the service compatibility
  surface. Internal Go packages are not a client SDK contract.

## Protocol

- The default BadTower relay profile accepts the published wire identifier
  `licoarc.relay.v1`.
- Compatibility is an explicit wire-level claim, not a source, build,
  vendored-artifact, runtime, or synchronized-release dependency on LicoArc.
- Additional identifiers require an explicit validated BadTower relay profile
  and independent behavior tests. A profile cannot raise the implementation
  ceilings.
- Envelopes carrying fields outside the contract are rejected.
- Compatibility covers only the opaque outer transport-unit boundary. It
  creates no LicoUp-specific API, endpoint identity, encryption, trust,
  delivery-proof, or synchronized-release relationship.
- BadTower HTTP, storage, ordering, queue, lease, acknowledgement, receipt,
  clock, and retry behavior remains service-local and untrusted.

## State and migration

- BadTower initializes only its current database format. It never discovers,
  imports, renames, or converts state from a retired product.
- Mailbox state is held in one local bbolt database and bounded by the relay
  profile. Restarting the same service with the same data path preserves
  active leases and unexpired opaque transport units.
- The database is marked `badtower.station-store.v1`. An unknown store version
  fails closed; BadTower does not guess, import, or mutate it.

## Versioning

The package follows semantic versioning; see
[CHANGELOG.md](../CHANGELOG.md).
