# Fabrigent Relay Contract v2

BadTower consumes the Fabrigent `v2` artifact through
`vendor/fabrigent-v2.json`. The artifact is the only protocol authority;
this document summarizes it. Exact identifiers and limits are defined in
the artifact itself.

## Envelope schema

The `fabrigent.relay.v2` envelope (`contracts/v2/relay-envelope.schema.json`)
is a closed JSON object (`additionalProperties: false`) with required
fields:

- `contractVersion` — constant `fabrigent.relay.v2`.
- `envelopeId`, `mailboxId` — opaque identifiers of 16 to 128 characters
  from the URL-safe set.
- `ciphertext` — opaque string of 1 to 1048576 characters.
- `expiresAt` — date-time after which the envelope must not be delivered.

## Governance policy

`policies/v2/relay-governance.json` fixes the trust model
`relay-is-untrusted`:

- Required capabilities: opaque envelope relay, mailbox lease, quota,
  acknowledgement, and expiry cleanup.
- Forbidden capability classes: client key custody, encryption, decryption,
  plaintext inspection, host permission authority, and client runtime
  coordination. The exact capability identifiers live in the artifact.
- Limits: ciphertext at most 1048576 bytes, envelope retention and leases
  at most 86400 seconds, at most 1000 envelopes per mailbox, at most 1000
  mailboxes, and at most 10000 envelopes per relay.

## Pinning

`src/fabrigent.mjs` recomputes the artifact self-digest (SHA-256 over the
canonical serialization of its version, digest algorithm, and sources,
plus a trailing newline) and compares it with both the embedded digest and
the compile-time
`PINNED_FABRIGENT_DIGEST`. Any mismatch fails closed. The loader also
asserts the untrusted trust model, the presence of every forbidden
capability class, and positive bounded limits in the policy.

Conformance fixtures (`conformance/v2/valid.json` and
`conformance/v2/invalid.json`) use synthetic values only.
