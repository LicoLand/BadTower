# BadTower Documentation

BadTower is a general, independently operated Lico Arc Network communication
station that every endpoint treats as potentially malicious. Its architecture
defines the canonical trust and product boundary. This index covers the formal
documentation set. Local plans and reports stay in the ignored `docs/plans/`
and `docs/reports/` directories and are never published.

## Formal documents

- [Runbook](RUNBOOK.md) — verification and service-local operation.
- [Compatibility](COMPATIBILITY.md) — runtime, wire-contract, and local
  profile compatibility.
- [Entity configuration layout](ENTITY-CONFIG-LAYOUT.md) — the complete
  configuration surface of the station.
- [Architecture](architecture/ARCHITECTURE.md) — components, trust
  boundaries, and failure model.
- [Relay operations](functionality/RELAY-OPERATIONS.md) — mailbox, lease,
  delivery, quota, acknowledgement, and cleanup behavior.
- [Relay profiles](protocols/RELAY-PROFILES.md) — the local profile contract
  and wire-level compatibility boundary.
- [Examples](examples/README.md) — synthetic usage walkthrough.
- [Architecture decision records](adrs/README.md) — ADR index and rules.
