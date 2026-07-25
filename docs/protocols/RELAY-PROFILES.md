# Relay Profiles

The normative BadTower profile API is implemented in
`internal/profile/profile.go`. It is intentionally independent from every
protocol repository and release pipeline and is not a product-specific
endpoint contract.

## Profile shape

A `badtower.relay-profile.v1` value is a closed object containing:

- `acceptedContractVersions` — one to sixteen unique wire identifiers;
- `limits` — positive integer limits for ciphertext bytes, retention, lease
  duration, envelopes per mailbox, mailboxes, and relay-wide envelopes.

Profiles are defensively copied into unexported Go state before use. This is
local runtime mutation safety, not a frozen cross-product protocol: BadTower
adapts published Lico Arc outer transport versions independently and never
preserves a LicoUp or retired Client-Link API. Unknown JSON fields, duplicate
identifiers, unsupported profile versions, and invalid values are rejected.
Operator-provided limits may reduce but cannot exceed BadTower's implementation
ceilings.

## Default compatibility

`profile.Default()` accepts `licoarc.relay.v1`. This is a wire-level
compatibility declaration only. BadTower does not import, vendor, fetch,
discover, or load the LicoArc repository or its generated artifacts.

Compatibility means that BadTower adapts the published outer opaque transport
unit. It does not mean that LicoUp or any other endpoint integrates with,
registers with, or trusts BadTower. BadTower must not extend the unit with a
LicoUp-specific API, identity, encryption, key-custody, plaintext, security,
or end-to-end delivery contract.

LicoArc may develop and publish independently. BadTower changes its accepted
wire identifiers only when its maintainers deliberately implement and test a
new compatible behavior. Neither repository requires a synchronized commit,
tag, build, or release.

## Envelope boundary

Every accepted contract identifier currently uses BadTower's closed opaque
envelope shape:

- `contractVersion`;
- opaque `envelopeId` and `mailboxId`;
- non-empty `ciphertext`;
- RFC 3339 `expiresAt`.

Adding an identifier whose semantics require another shape or authority is a
BadTower implementation change, not a profile-only configuration change.

HTTP requests and responses, local persistence, ordering, queue state, leases,
acknowledgements, receipts, clocks, and retry behavior are BadTower-local
transport mechanisms. Endpoints must treat every such signal as untrusted; no
signal changes the meaning or security properties of the opaque transport
unit.
