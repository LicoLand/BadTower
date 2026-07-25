# Security Policy

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub Security
Advisories at <https://github.com/LicoLand/BadTower/security/advisories>.
Do not open public issues for unpatched vulnerabilities.

Maintainers triage reports as capacity allows and coordinate disclosure
with reporters.

## Supported versions

Only the latest state of the `main` branch receives security fixes. The
0.1.x line is the initial pre-release series.

## Security model

BadTower is an intentionally untrusted communication station:

- It stores and forwards only opaque Lico Arc Protocol transport units;
  confidentiality and integrity are protected end to end by endpoints.
- It holds no client keys and has no decryption capability by design.
- LicoUp and other endpoints may pass traffic through the station, but never
  trust it or delegate identity, encryption, integrity, delivery, or other
  security authority to it.
- HTTP responses, storage, ordering, clocks, queues, leases,
  acknowledgements, receipts, and delivery claims are untrusted
  station-local transport hints.
- No LicoUp-specific API, identity, encryption workflow, or security contract
  belongs in BadTower.
- Relay profiles are validated and bounded by BadTower-owned implementation
  ceilings. Making a runtime profile immutable after validation is local
  implementation safety; it does not freeze a LicoUp or retired Client-Link
  contract. Unknown profile fields, versions, or excessive limits fail closed
  before state is created.
- No protocol repository, remote artifact, or sibling checkout is loaded at
  build time or runtime.

Reports about any weakening of this boundary are in scope.
