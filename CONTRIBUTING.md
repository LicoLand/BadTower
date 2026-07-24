# Contributing to BadTower

BadTower is an intentionally untrusted relay node. Contributions must keep
the relay unable to decrypt content, hold keys, or act as any kind of
authority.

## Ground rules

- Stay inside the repository boundary: relay ingress and egress, mailboxes,
  leases, quotas, acknowledgements, expiry, cleanup, and service-local
  verification.
- Never add client key custody, encryption or decryption features,
  plaintext handling, identity proof, local approval or local effect
  behavior, federation policy, or client coordination behavior.
- Consume Fabrigent protocol artifacts only through the pinned vendored
  bundle (`vendor/fabrigent-v1.json`); never import sibling repository
  source. Protocol changes belong to Fabrigent; update the vendored
  artifact and the SHA-256 pin in `src/fabrigent.mjs` in one change.
- Keep configuration explicit, bounded, and fail closed. Do not add
  compatibility aliases, retired-name state discovery, or dual-write paths.
- Use synthetic values in examples and tests. Never commit machine
  identity, personal paths, secrets, private endpoints, or runtime data.

## Development

Requirements: Node.js 22 or newer.

- `npm test` — run the relay test suite.
- `node tools/verify-boundary.mjs` — verify the pinned artifact and run the
  forbidden-capability boundary scan.
- `npm run verify` — full repository closure (boundary scan plus tests).

## Documentation

Formal documentation lives under `docs/` and is indexed by
[docs/README.md](docs/README.md). Update the formal documents in the same
change that alters the capability or boundary they describe. Local plans
and reports stay in the ignored `docs/plans/` and `docs/reports/`
directories and are never committed.

Report security issues privately as described in
[SECURITY.md](SECURITY.md).
