# Security Policy

## Reporting a vulnerability

Report suspected vulnerabilities privately through GitHub Security
Advisories at <https://github.com/LicoLand/BadTower/security/advisories>.
Do not open public issues for unpatched vulnerabilities.

Maintainers triage reports as capacity allows and coordinate disclosure
with reporters.

## Supported versions

Only the latest state of the `main` branch receives security fixes. The
0.1.x line is the initial pre-release series and its integrated relay surface
is superseded by the native Station Core migration.

## Security model

BadTower is an intentionally untrusted communication station:

- It stores and forwards only opaque Transport Units; confidentiality and
  integrity are protected end to end by endpoints.
- It holds no client keys and has no decryption capability by design.
- A compatible endpoint may pass traffic through the station, but never trusts
  it or delegates identity, encryption, integrity, delivery, or other security
  authority to it.
- gRPC responses, persistence, ordering, clocks, queues, claims, settlements,
  and capacity signals are untrusted station-local transport hints.
- No endpoint-product-specific API, identity, encryption workflow, or security
  contract belongs in BadTower.
- Process configuration is strict JSON with bearer-token namespace credentials.
  Credentials are never accepted from argv or environment variables. Retired
  store paths and unknown store identities fail closed.
- No protocol repository, external protocol artifact, remote artifact, or
  sibling checkout is imported, pinned, discovered, fetched, or loaded at
  build time or runtime.

Reports about any weakening of this boundary are in scope.
