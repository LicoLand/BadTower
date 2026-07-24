# Relay Operations

All behavior below is implemented by `BadTowerRelay` in `src/relay.mjs`
and tested in `tests/relay.test.mjs`. Limits come from the pinned
Fabrigent governance policy.

## Lease

`lease(mailboxId, requestedSeconds)` opens or renews a mailbox lease. The
duration is clamped to the policy maximum (86400 seconds) and must be a
positive integer. Every mailbox operation requires an active lease.

## Deliver

`deliver(envelope)` validates the envelope against the
`fabrigent.relay.v1` contract:

- Only `contractVersion`, `envelopeId`, `mailboxId`, `ciphertext`, and
  `expiresAt` are accepted; any other field is rejected.
- Identifiers are opaque strings matching the contract pattern.
- Ciphertext must be non-empty and within the policy size limit
  (1048576 bytes).
- `expiresAt` must be a future date-time.

A duplicate `envelopeId` is reported as a duplicate without rewriting
stored state. Delivery fails when the mailbox reaches the policy quota
(1000 envelopes).

## Receive

`receive(mailboxId, limit)` returns up to `limit` non-expired envelopes.
The limit must be an integer from 1 to 100. Expired envelopes are never
returned.

## Acknowledge

`acknowledge(mailboxId, envelopeId)` removes a stored envelope and reports
whether it existed.

## Cleanup

`cleanup()` removes every expired envelope and every mailbox whose lease
has expired, returning the removal counts.
