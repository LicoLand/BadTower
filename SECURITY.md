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

BadTower is an intentionally untrusted relay node:

- It stores and forwards only opaque ciphertext envelopes; confidentiality
  and integrity are protected end to end by LicoUp clients.
- It holds no client keys and has no decryption capability by design.
- The Fabrigent `v2` artifact is pinned by SHA-256, and the node fails
  closed when the artifact content or its governance boundary changes.

Reports about any weakening of this boundary are in scope.
