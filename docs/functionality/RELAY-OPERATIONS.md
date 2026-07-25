# Relay Operations

All behavior below is implemented by `BadTowerRelay` in `src/relay.mjs`
and tested in `tests/relay.test.mjs`. Limits come from the pinned
Fabrigent governance policy.

## Lease

`lease(mailboxId, requestedSeconds)` opens or renews a mailbox lease. The
duration is clamped to the policy maximum (86400 seconds) and must be a
positive integer. Every mailbox operation requires an active lease. Leasing
an expired mailbox starts with empty retained state rather than reviving its
previous envelopes.

## Deliver

`deliver(envelope)` validates the envelope against the
`fabrigent.relay.v2` contract:

- Only `contractVersion`, `envelopeId`, `mailboxId`, `ciphertext`, and
  `expiresAt` are accepted; any other field is rejected.
- Identifiers are opaque strings matching the contract pattern.
- Ciphertext must be non-empty and within the policy size limit
  (1048576 bytes).
- `expiresAt` must be an RFC 3339 future date-time no more than 86400
  seconds in the future.

An exact duplicate `envelopeId` is reported as a duplicate without
rewriting stored state. Reuse of an identifier with different content is
rejected. Delivery fails when a mailbox reaches 1000 envelopes or the
relay reaches 10000 envelopes.

## Receive

`receive(mailboxId, limit)` removes expired entries and returns up to
`limit` remaining envelopes. The limit must be an integer from 1 to 100.

## Acknowledge

`acknowledge(mailboxId, envelopeId)` removes a stored envelope and reports
whether it existed.

## Cleanup

`cleanup()` removes every expired envelope and every mailbox whose lease
has expired, returning the removal counts.
