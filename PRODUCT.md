# BadTower Product Overview

BadTower is an intentionally untrusted communication node for the LicoLand
ecosystem. Independent operators run BadTower to store and forward opaque
encrypted envelopes between LicoUp clients. It was split out of the retired
LicoTower codebase and keeps only the untrusted relay responsibilities.

## Trust model

The relay is untrusted by design:

- It handles only the opaque ciphertext envelopes defined by the Fabrigent
  `fabrigent.relay.v1` contract.
- It cannot decrypt content and never holds client keys or key material.
- It is not an identity, policy, permission, encryption, or client
  coordination authority.
- Clients authenticate peers and protect confidentiality and integrity end
  to end.

## Owned capabilities

- Relay ingress and egress of opaque envelopes.
- Mailboxes with bounded ciphertext retention.
- Delivery leases, acknowledgements, quotas, expiry, and cleanup.
- Fail-closed consumption of the pinned Fabrigent protocol artifact
  (`vendor/fabrigent-v1.json`, pinned by SHA-256 in `src/fabrigent.mjs`).

## Explicit non-goals

- Holding user plaintext, private keys, or any decryption capability.
- Client key custody, identity proof, local approvals, or local effects.
- Client user experience, federation policy, committee governance,
  certification rules, or neutral protocol authority (owned by Fabrigent).
- Meshrix identities, permissions, and process bootstrap (owned by Meshrix).
- Compatibility aliases, retired-name state discovery, or dual-write paths.

## Repository

- Source: <https://github.com/LicoLand/BadTower>
- License: AGPL-3.0-or-later (see [LICENSE](LICENSE)).
