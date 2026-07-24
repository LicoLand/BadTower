# Fabrigent Relay Contract v1

BadTower consumes the Fabrigent `v1` artifact through
`vendor/fabrigent-v1.json`. The artifact is the only protocol authority;
this document summarizes it. Exact identifiers and limits are defined in
the artifact itself.

## Envelope schema

The `fabrigent.relay.v1` envelope (`contracts/v1/relay-envelope.schema.json`)
is a closed JSON object (`additionalProperties: false`) with required
fields:

- `contractVersion` — constant `fabrigent.relay.v1`.
- `envelopeId`, `mailboxId` — opaque identifiers of 16 to 128 characters
  from the URL-safe set.
- `ciphertext` — opaque string of 1 to 1048576 characters.
- `expiresAt` — date-time after which the envelope must not be delivered.

## Governance policy

`policies/v1/relay-governance.json` fixes the trust model
`relay-is-untrusted`:

- Required capabilities: opaque envelope relay, mailbox lease, quota,
  acknowledgement, and expiry cleanup.
- Forbidden capability classes: client key custody, encryption, decryption,
  plaintext inspection, host permission authority, and client runtime
  coordination. The exact capability identifiers live in the artifact.
- Limits: ciphertext at most 1048576 bytes, leases at most 86400 seconds,
  and at most 1000 envelopes per mailbox.

## Pinning

`src/fabrigent.mjs` recomputes the artifact self-digest (SHA-256 over the
canonical serialization of the artifact sources plus a trailing newline)
and compares it with both the embedded digest and the compile-time
`PINNED_FABRIGENT_DIGEST`. Any mismatch fails closed. The loader also
asserts the untrusted trust model and the presence of every forbidden
capability class in the policy.

Conformance fixtures (`conformance/v1/valid.json` and
`conformance/v1/invalid.json`) use synthetic values only.
