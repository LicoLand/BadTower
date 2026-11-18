# Contributing to BadTower

BadTower is an intentionally untrusted communication station. Contributions must keep
the Station Core unable to decrypt content, hold keys, or act as any kind of
authority.

## Ground rules

- Stay inside the repository boundary: native Reserve, Submit, Claim, Settle,
  namespace authorization, capacity, health, metrics, packaging, and
  service-local verification.
- Keep BadTower a general, independently operated protocol-neutral Station.
  Endpoints pass traffic through it; they do not integrate with it as a
  trusted product.
- Never add client key custody, encryption or decryption features,
  plaintext handling, identity proof, local approval or local effect
  behavior, federation policy, or client coordination behavior.
- Never add endpoint-product-specific APIs, accounts, identities, encryption
  workflows, delivery proofs, SDK contracts, or security authority.
- Treat gRPC results, persistence, ordering, clocks, queues, claims,
  settlements, and retry state as service-local, untrusted transport hints.
- Keep the Native Station Contract protocol-neutral. Never import, vendor,
  discover, fetch, or load external protocol repository content into Core.
- Keep configuration explicit, bounded, and fail closed. Do not add
  compatibility aliases, retired-name state discovery, or dual-write paths.
- Use synthetic values in examples and tests. Never commit machine
  identity, personal paths, secrets, private endpoints, or runtime data.

## Development

The sole service implementation language is Go. Keep the executable service
under `cmd/badtower/`, the native API under `internal/nativeapi/`, the Station
Core under `internal/stationcore/`, and strict JSON configuration under
`badtower.station.config.v1`. Credentials are never accepted from argv or
environment variables.

- `go test ./...` — run the targeted package suites.
- `go test -race ./...` — run the final concurrency regression.
- `go vet ./...` — run the Go static checks.
- `go build ./...` — compile every command and package.

## Documentation

Formal documentation lives under `docs/` and is indexed by
[docs/README.md](docs/README.md). Update the formal documents in the same
change that alters the capability or boundary they describe. Local plans
under `docs/plans/` and privacy-minimal runtime reports under `docs/reports/`
remain ignored and never enter a commit. Promote only an explicitly approved,
fully redacted conclusion into its formal fact owner.

Report security issues privately as described in
[SECURITY.md](SECURITY.md).
