# BadTower Documentation

Updated: 2026-09-25

BadTower is the Service Layer: one pure protocol-neutral communication
Station. The Station Core directly implements the Native Station Contract and
remains complete with no protocol plugin. A Station Instance must close before
a Station Cluster; only after native excellence may a separate external
extension contract and optional Protocol Compatibility Plugins be implemented.

The architecture document owns that target boundary. Current implementation
facts remain in the status document. Local Better Plan workspaces may live
under ignored `docs/plans/`; privacy-minimal runtime reports remain under
ignored `docs/reports/`. Neither enters a commit or creates a release or
publication claim.

## Formal documents

- [Product goal and boundary](../PRODUCT.md) — durable product intent and
  non-goals.
- [Domain language](../CONTEXT.md) — canonical Station, native-contract, and
  external-plugin vocabulary.
- [Current status](STATUS.md) — intent, implementation, verification, release,
  support, and operation.
- [Architecture](architecture/ARCHITECTURE.md) — native Station model,
  deployment order, extension firewall, current components, and migration.
- [Runbook](RUNBOOK.md) — current verification and service-local operation.
- [Compatibility](COMPATIBILITY.md) — native contract, configuration, and store
  compatibility surface.
- [Entity configuration layout](ENTITY-CONFIG-LAYOUT.md) — complete
  `badtower.station.config.v1` configuration surface.
- [Standard node package](functionality/NODE-PACKAGE.md) — deterministic
  packaging, detached signature, bounded verification, and receipt behavior.
- [Station Instance acceptance](functionality/STATION-INSTANCE-ACCEPTANCE.md) —
  plugin-free fast, load, soak, fault, and recovery gates.
- [Station Cluster](functionality/STATION-CLUSTER.md) — production assembly,
  durable consensus and recovery, operator lifecycle, and open capacity
  evidence.
- [Native Excellence Gate](functionality/NATIVE-EXCELLENCE.md) — hard evidence
  prerequisite and current unaccepted state.
- [External Extension Contract](functionality/EXTERNAL-EXTENSION-CONTRACT.md) —
  closed descriptor, scoped native access, budgets, and process firewall.
- [External plugin package](functionality/EXTERNAL-PLUGIN-PACKAGE.md) —
  deterministic descriptor/binary archive and detached verification.
- [Exact LicoArc Candidate adapter](protocols/LICOARC-CANDIDATE-ADAPTER.md) —
  pinned binding, carrier, state, trust, and admission boundaries.
- [Cluster capacity candidate](capacity/cluster-general-v1.json) — implemented
  ceilings and explicit missing evidence.
- [Station Instance capacity candidate](capacity/single-node-general-v1.json) —
  bounded workload and truthful terminal-throughput measurement semantics.
- [Examples](examples/README.md) — synthetic native gRPC and `badtowerctl`
  walkthrough.
- [Architecture decision records](adrs/README.md) — ADR index and rules.

## Local planning

Use `$better-plan` to manage one ignored local workspace under `docs/plans/`.
It may be the current execution authority for the active workspace, but it is
not formal documentation, a Git candidate, or a publication artifact.
Promote only explicitly approved durable intent or verified current facts into
the formal owners indexed above.
