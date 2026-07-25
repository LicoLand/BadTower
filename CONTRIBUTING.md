# Contributing to BadTower

BadTower is an intentionally untrusted communication station. Contributions must keep
the relay unable to decrypt content, hold keys, or act as any kind of
authority.

## Ground rules

- Stay inside the repository boundary: relay ingress and egress, mailboxes,
  leases, quotas, acknowledgements, expiry, cleanup, and service-local
  verification.
- Keep BadTower a general, independently operated Lico Arc Network station.
  Endpoints pass traffic through it; they do not integrate with it as a
  trusted product.
- Never add client key custody, encryption or decryption features,
  plaintext handling, identity proof, local approval or local effect
  behavior, federation policy, or client coordination behavior.
- Never add LicoUp-specific APIs, accounts, identities, encryption workflows,
  delivery proofs, SDK contracts, or security authority.
- Treat HTTP results, persistence, ordering, clocks, queues, leases,
  acknowledgements, receipts, and retry state as service-local, untrusted
  transport hints.
- Keep protocol compatibility at the published wire-contract boundary. Never
  import, vendor, discover, fetch, or load LicoArc repository content.
- Change accepted wire identifiers or limits only through the closed
  BadTower-owned profile and independent behavior tests. Protocol and
  BadTower releases never require synchronized commits.
- Keep configuration explicit, bounded, and fail closed. Do not add
  compatibility aliases, retired-name state discovery, or dual-write paths.
- Use synthetic values in examples and tests. Never commit machine
  identity, personal paths, secrets, private endpoints, or runtime data.

## Development

The production implementation language is Go. Keep the executable service
under `cmd/badtower/`, transport behavior under `internal/station/`, HTTP
adaptation under `internal/httpapi/`, and bounded configuration under
`internal/profile/`.

- `go test ./...` — run the targeted package suites.
- `go test -race ./...` — run the final concurrency regression.
- `go vet ./...` — run the Go static checks.
- `go build ./...` — compile every command and package.

## Documentation

Formal documentation lives under `docs/` and is indexed by
[docs/README.md](docs/README.md). Update the formal documents in the same
change that alters the capability or boundary they describe. Local plans
and reports stay in the ignored `docs/plans/` and `docs/reports/`
directories and are never committed.

Report security issues privately as described in
[SECURITY.md](SECURITY.md).
