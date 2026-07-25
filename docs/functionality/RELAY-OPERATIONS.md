# Relay Operations

All behavior below is implemented in `internal/station/`, configured by
`internal/profile/`, adapted to HTTP by `internal/httpapi/`, and tested beside
those packages. Limits are owned by BadTower.

These are station-local transport operations. Their results, including
leases and acknowledgements, are untrusted hints and never prove peer
identity, message authenticity, end-to-end acceptance, or delivery.

## Lease

`POST /v1/mailboxes/{mailboxId}/lease` with `{"leaseSeconds":60}` opens or
renews a mailbox lease. The
duration is clamped to the local profile maximum (86400 seconds) and must be a
positive integer. Every mailbox operation requires an active lease. Leasing
an expired mailbox starts with empty retained state rather than reviving its
previous envelopes.

## Deliver

`POST /v1/envelopes` validates the JSON envelope against the
contract identifier accepted by the selected relay profile:

- Only `contractVersion`, `envelopeId`, `mailboxId`, `ciphertext`, and
  `expiresAt` are accepted; any other field is rejected.
- Identifiers are opaque strings matching the contract pattern.
- Ciphertext must be non-empty and within the local profile size limit
  (1048576 bytes).
- `expiresAt` must be an RFC 3339 future date-time no more than 86400
  seconds in the future.

An exact duplicate `envelopeId` is reported as a duplicate without
rewriting stored state. Reuse of an identifier with different content is
rejected. Delivery fails when a mailbox reaches 1000 envelopes or the
relay reaches 10000 envelopes.

## Receive

`GET /v1/mailboxes/{mailboxId}/envelopes?limit=100` removes expired entries
and returns up to `limit` remaining envelopes in first-insertion order. The
limit must be an integer from 1 to 100.

## Acknowledge

`DELETE /v1/mailboxes/{mailboxId}/envelopes/{envelopeId}` removes a stored
envelope and reports whether it existed.

## Cleanup

The service runs cleanup once per minute. The station core also exposes
`Cleanup()` for bounded operational verification; it removes every expired
envelope and every mailbox whose lease has expired, returning removal counts.

All state transitions occur in bbolt write transactions. A successful HTTP
response is still only a station-local claim and never proves peer receipt.
