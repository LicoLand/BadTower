# Compatibility

## Runtime

- Node.js 22 or newer, as declared in `package.json` `engines`.
- The package is ESM-only (`"type": "module"`).

## Protocol

- The only supported relay contract is Fabrigent `fabrigent.relay.v2`,
  consumed through the vendored artifact `vendor/fabrigent-v2.json`.
- The artifact is pinned by SHA-256 (`PINNED_FABRIGENT_DIGEST` in
  `src/fabrigent.mjs`). Any change to the artifact content or its
  governance boundary fails closed; there are no compatibility aliases or
  dual-contract modes.
- Envelopes carrying fields outside the contract are rejected.

## State and migration

- BadTower initializes fresh state only. It never discovers, imports,
  renames, or converts state from the retired LicoTower codebase or any
  other retired product name.
- Mailbox state is held in memory and bounded by the vendored policy
  limits; restarting the process drops retained envelopes.

## Versioning

The package follows semantic versioning; see
[CHANGELOG.md](../CHANGELOG.md).
