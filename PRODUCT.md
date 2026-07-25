# BadTower Product Overview

BadTower is a general-purpose, independently operated communication station in
the Lico Arc Network. Every endpoint assumes every BadTower may be malicious.
The station adapts the outer opaque Lico Arc Protocol transport-unit boundary
and provides its own local forwarding service while keeping only untrusted
transport responsibilities.

## Trust model

The relay is untrusted by design:

- It handles only the closed opaque ciphertext envelope shape supported by
  its local relay profile.
- It cannot decrypt content and never holds client keys or key material.
- It is not an identity, policy, permission, encryption, or client
  coordination authority.
- LicoUp and other endpoints may pass traffic through it, but do not form a
  product integration or trust relationship with it.
- Endpoints authenticate peers and protect confidentiality, integrity,
  freshness, replay handling, and end-to-end acceptance independently of the
  station.
- HTTP results and all storage, queue, lease, acknowledgement, receipt, time,
  and delivery state are station-local, untrusted transport hints.

## Owned capabilities

- Relay ingress and egress of opaque envelopes.
- Mailboxes with bounded ciphertext retention.
- Delivery leases, acknowledgements, quotas, expiry, and cleanup.
- Service-local HTTP, storage, queueing, retry, and availability behavior.
- A BadTower-owned, bounded relay profile whose default accepted wire
  identifier is `licoarc.relay.v1`.
- An explicit profile injection boundary for independently tested compatible
  wire contracts; profiles can reduce but never exceed implementation limits.

## Explicit non-goals

- Holding user plaintext, private keys, or any decryption capability.
- Client key custody, identity proof, local approvals, or local effects.
- A LicoUp-specific API, SDK, account model, identity model, encryption
  workflow, delivery proof, or security contract.
- Client user experience, federation policy, committee governance,
  certification rules, or neutral protocol authority (owned by Lico Arc Protocol).
- Meshrix identities, permissions, and process bootstrap (owned by Meshrix).
- Source, build, vendored-artifact, runtime, or synchronized-release
  dependencies on the LicoArc repository.
- Compatibility aliases, retired-name state discovery, or dual-write paths.

## Implementation language

The station service is implemented in Go. The standard library owns HTTP
serving, request bounds, lifecycle, and graceful shutdown. The etcd project's
bbolt library owns the embedded transactional store. Mailboxes, lease expiry,
ordered opaque transport units, deduplication indexes, acknowledgements, and
global counters persist atomically in one service-local database.

## Repository

- Source: <https://github.com/LicoLand/BadTower>
- License: AGPL-3.0-or-later (see [LICENSE](LICENSE)).
