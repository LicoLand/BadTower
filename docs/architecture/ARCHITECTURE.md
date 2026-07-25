# Architecture

## Role

BadTower is an intentionally untrusted communication node. It stores and
forwards opaque encrypted envelopes between LicoUp clients and implements
only mailboxes, leases, quotas, acknowledgements, expiry, and cleanup. It
was split from the retired LicoTower codebase and keeps only the untrusted
relay responsibilities.

The relay cannot decrypt content and is not an identity, policy,
permission, encryption, or client coordination authority. Clients
authenticate peers and protect confidentiality and integrity end to end.

## Components

| Component | Path | Responsibility |
| --- | --- | --- |
| Relay core | `src/relay.mjs` | In-memory mailboxes: lease, deliver, receive, acknowledge, cleanup, with quota and expiry enforcement. |
| Artifact loader | `src/fabrigent.mjs` | Loads the vendored Fabrigent artifact, verifies its self-digest and the pinned SHA-256, and asserts the untrusted-relay governance policy. |
| Public entry | `src/index.mjs` | Re-exports the relay and the artifact loader. |
| Boundary check | `tools/verify-boundary.mjs` | Fails verification if forbidden authority-capability markers appear outside the exempted boundary files. |
| Vendored artifact | `vendor/fabrigent-v2.json` | Fabrigent `v2` contract, governance policy, and conformance fixtures. |

## Trust boundaries

- Protocol authority stays in Fabrigent. BadTower consumes the artifact by
  exact version and digest and never imports sibling repository source.
- Envelopes are opaque: the contract admits only `contractVersion`,
  `envelopeId`, `mailboxId`, `ciphertext`, and `expiresAt`, and the relay
  rejects anything else.
- Retention is bounded by the vendored policy limits (ciphertext size,
  envelope lifetime, lease duration, envelopes per mailbox, mailbox count,
  and relay-wide envelope count), and all state is in memory.

## Failure model

- Artifact self-digest or pin mismatch: fail closed at load time.
- Unknown envelope fields or contract versions: rejected at delivery.
- Conflicting reuse of an envelope identifier: rejected at delivery.
- Missing or expired lease: mailbox operations are refused.
- Expired envelopes are removed before delivery or receipt and by cleanup;
  expired mailboxes are removed by cleanup.
