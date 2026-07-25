# Architecture

## Role

BadTower is a general-purpose, independently operated, single-station
communication service in the Lico Arc Network. Every endpoint assumes the
station may be malicious. BadTower stores and forwards only opaque outer
transport units and implements station-local ingress, egress, mailboxes,
leases, quotas, acknowledgements, expiry, and cleanup.

LicoUp and other endpoints may pass traffic through a BadTower, but do not
integrate with it as a trusted product and do not delegate any security
authority to it. The station cannot decrypt content and is not an identity,
policy, permission, encryption, integrity, delivery, or client-coordination
authority. Endpoints authenticate peers and protect confidentiality,
integrity, freshness, replay handling, and acceptance end to end.

BadTower stations are independently implemented and operated. A station is not a
LicoUp backend, and its local HTTP or storage design does not define the
endpoint protocol or the Lico Arc Network governance model.

## Canonical boundary

- Lico Arc Protocol owns the outer opaque transport-unit semantics and wire
  identifiers.
- BadTower adapts that outer boundary and owns only its service-local HTTP,
  storage, queueing, retry, quota, cleanup, and availability behavior.
- HTTP status, persistence state, ordering, clocks, leases,
  acknowledgements, receipts, and delivery claims are untrusted transport
  hints. They are not proof of peer identity, message authenticity,
  end-to-end acceptance, or delivery.
- BadTower must not expose a LicoUp-specific API or own endpoint accounts,
  identity proof, peer authentication, encryption, decryption, key custody,
  plaintext, replay authority, or security policy.
- Product-specific endpoint behavior belongs to the endpoint. Neutral
  protocol and network-governance behavior belong to Lico Arc Protocol.

## Components

| Component | Path | Responsibility |
| --- | --- | --- |
| Service command | `cmd/badtower/main.go` | Loads explicit configuration, opens durable state, serves HTTP, performs periodic cleanup, and shuts down gracefully. |
| HTTP adapter | `internal/httpapi/` | Strict JSON and path decoding, bounded request bodies, transport operations, safe status mapping, and minimal health response. |
| Station core | `internal/station/` | Transactional mailbox leases, ordered opaque units, deduplication, acknowledgement, quotas, expiry, cleanup, and persistence. |
| Relay profile | `internal/profile/` | Accepted wire identifiers, local limits, defensive copying, validation, and implementation ceilings. |
| Embedded store | bbolt | One service-local transactional database with owner-only file permissions. |

## Trust boundaries

- Protocol authority stays outside BadTower. Compatibility is expressed only
  through an accepted published wire identifier and independently maintained
  tests; BadTower never imports, vendors, discovers, or loads sibling
  repository content.
- Envelopes are opaque: the contract admits only `contractVersion`,
  `envelopeId`, `mailboxId`, `ciphertext`, and `expiresAt`, and the relay
  rejects anything else.
- Retention is bounded by BadTower-owned implementation limits (ciphertext size,
  envelope lifetime, lease duration, envelopes per mailbox, mailbox count,
  and relay-wide envelope count). State is persisted atomically in the local
  bbolt database.
- No station response or retained state crosses the end-to-end trust boundary.

## Production implementation

Go owns the network service, HTTP handling, process lifecycle, profile
validation, mailboxes, leases, quotas, acknowledgements, and cleanup. bbolt
provides the embedded transactional persistence boundary. There is one
production implementation and one Go verification path.

## Failure model

- Unknown, malformed, or over-ceiling relay profile: fail closed at
  construction.
- Unknown envelope fields or contract versions: rejected at delivery.
- Conflicting reuse of an envelope identifier: rejected at delivery.
- Missing or expired lease: mailbox operations are refused.
- Expired envelopes are removed before delivery or receipt and by cleanup;
  expired mailboxes are removed by cleanup.
