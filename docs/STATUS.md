# BadTower Status

This document is the authority for current BadTower status. English is
normative. Product goals and boundaries are defined in
[`PRODUCT.md`](../PRODUCT.md); vocabulary is defined in
[`CONTEXT.md`](../CONTEXT.md).

Intent, implementation, verification, release, support, and operation are
reported independently.

## Intent

| Scope | Status | Meaning |
| --- | --- | --- |
| Product shape | approved goal | BadTower is one pure protocol-neutral communication Station whose native capability is the product |
| Native Station Contract | approved goal | Direct Reserve, Submit, Claim, Settle, health, capacity, and shutdown behavior works with no protocol plugin |
| Station Instance | mandatory first deployment | Complete one independently startable, durable, recoverable, packaged, observable, and performant native single-node Station |
| Station Cluster | approved second deployment | Extend the accepted native contract to multiple replaceable members only after complete standalone closure |
| Native excellence gate | mandatory prerequisite | Station Core, Station Instance, and Station Cluster correctness, performance, recovery, and complete migration precede extension implementation |
| External Extension Contract | approved third stage | Separately packaged descriptor, scoped native access, harnesses, and minimal SDK; no in-process loader, registry, or supervisor |
| Protocol compatibility | approved optional stage | Each explicitly selected line is an independent external process, absent by default and never a Core feature |
| Endpoint trust | out of scope | BadTower owns no endpoint identity, key, plaintext, approval, replay, authenticity, final receipt, or effect decision |
| Hosted network | out of scope | Operation of a named public service or network is separate from this source repository |

## Implementation

| Capability | Status | Current source boundary |
| --- | --- | --- |
| Native single-node service | implemented in source (candidate) | `cmd/badtower` serves gRPC Station, health, and metrics from strict config only |
| Station Core | implemented in source (candidate) | `internal/stationcore` owns Pebble-backed Reserve, Submit, Claim, Settle, capacity, and lifecycle; store identity v2/schema 4 uses one strict `INSTANCE_SHARDED` manifest with 64 logical partitions mapped to one fixed standalone storage shard, or one exact `PARTITION_ONE` cluster shard, separates immutable payloads from mutable delivery metadata, and fails closed on every earlier, partial, foreign, or topology-mismatched root |
| Native API surface | implemented in source (candidate) | `internal/nativeapi` maps gRPC, auth, health, metrics, and `StationFailure` |
| Native control CLI | implemented in source (candidate) | `cmd/badtowerctl` provides check-config, credential generate, and RPC harness commands |
| Signed standard node package | implemented in source | `internal/nodepackage` binds `badtower.station-core.store.v2` and verifies deterministic archives |
| Integrated HTTP relay path | removed | No `internal/httpapi`, `internal/profile`, or `internal/station` packages remain |
| Station Instance acceptance | implemented in source (candidate) | Direct lifecycle, bounds, durability, privacy, package, backup/restore, load, soak, and fault gates live under `internal/acceptance`; exact long-profile execution remains a separate verification fact |
| Station Cluster | implemented in source (candidate) | The production node assembles one control group and 64 data groups per member, with ReadIndex routing, durable Core-bearing embedded or streamed snapshots and log compaction, 3–7-member learner/joint-consensus lifecycle, resumable rebalance, privacy-minimal operator API/CLI, distributed coordinated backup/isolated restore, and rolling-upgrade preflight; focused four-node lifecycle evidence passes, while formal load and soak evidence remains open |
| Native Excellence Gate | not accepted | Repository regression and focused Cluster lifecycle evidence pass, but the required Station Instance `-race` capacity profile remains below 3,000 terminal units/s and the formal Instance/Cluster load and soak evidence is not closed |
| External Extension Contract | implemented as quarantined candidate source | Closed descriptor schema, minimal native client surface, bounded synthetic harnesses, independent deterministic package, detached signature, and digest-bound descriptor generation exist outside Core; production admission waits for Native Excellence |
| Exact LicoArc Candidate plugin | implemented as quarantined candidate source | A separate process pins one Candidate line and transport digest, enforces TLS 1.3/HTTP2 and deterministic CBOR/raw-packet carrier rules, maps six operations through scoped native gRPC, enforces lowerable concurrency/queue/state ceilings with indexed expiry, and packages independently; it is not production-admitted while Native Excellence remains open |
| A2A plugin | deferred; not implemented | No A2A listener, mapping, state, artifact, process, test, or support path exists |
| ACP plugin | deferred; not implemented | No independently admitted ACP listener, mapping, state, artifact, process, test, or support path exists |
| MCP plugin | deferred; not implemented | No MCP listener, mapping, state, artifact, process, test, or support path exists |
| Endpoint-product adapter | rejected | No endpoint-specific account, identity, key, security, or product route belongs in BadTower |
| Cross-protocol translation | rejected | No implicit translation or shared protocol state belongs in Core or between plugins |

Implementation in source does not establish verification, release, support, or
operation.

## Verification

- Focused race tests verify the deterministic native command machine,
  partition placement, quorum failure, leader replacement, durable Raft
  restart/catch-up, Core-bearing embedded and streamed snapshots, TLS 1.3
  mTLS peer admission, ReadIndex barriers, dynamic learner/joint membership,
  interrupted rebalance recovery, distributed backup/isolated restore,
  rolling-upgrade preflight, and the Core import firewall.
- Focused extension tests verify closed descriptors, request/concurrency queue
  budgets, direct-native non-interference, deterministic package creation,
  detached Ed25519 verification, and tamper rejection.
- Focused LicoArc tests verify the pinned Candidate identities and digests,
  canonical closed CBOR, raw packet preservation, all six operations, strict
  HTTP/2/TLS 1.3 carrier rejection before mutation, response replay across
  restart, first-contact single acceptance, and malicious Station rejection.
- The repository build, ordinary race suite, vet suite, and focused four-node
  Cluster lifecycle gate pass. The exact Station Instance load gate was
  exercised but remains below its required 3,000 terminal units/s; the formal
  Instance soak and Cluster load/soak runs are not recorded as accepted.
  Consequently Native Excellence and downstream production admission remain
  open.
- These tests are BadTower implementation evidence only. They do not verify,
  publish, support, or operate LicoArc itself.

## Release

| Channel | Status |
| --- | --- |
| Source tag | not claimed |
| Binary or package publication | not claimed |
| Container publication | not claimed |
| Hosted deployment | not claimed |

The signed standard node package is an implemented and locally verified
candidate for the planned `0.2.0` line. No public package, source tag, release
signature, or governed channel publication is claimed.

## Support

- The native Station Contract `badtower.station.v1` is implemented in source
  as a candidate only. It has no current publication, compatibility, or hosted
  support claim.
- The integrated HTTP relay path and its wire identifiers are removed and
  unsupported.
- Station Core, completed Station Instance, Station Cluster, External
  Extension Contract, and every protocol plugin have no current support claim.
- A protocol release elsewhere never promotes BadTower support automatically.

## Operation

No named hosted BadTower Station, Station Cluster, extension service, protocol
plugin, or official network is claimed operating. Source, DNS, static-site
content, or a successful local process start does not establish hosted
operation.
