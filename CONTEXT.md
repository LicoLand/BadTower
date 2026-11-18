# BadTower

BadTower defines the language of a pure, independently operated,
protocol-neutral communication station.

## Repository language

**Service Layer**:
The organizational layer owned by BadTower. It provides native, bounded
communication-station behavior and infrastructure. External protocol meaning
and endpoint trust remain outside it.
_Avoid_: Protocol Layer, endpoint backend, protocol runtime

**Station Core**:
The single BadTower runtime that directly implements the Native Station
Contract, durable opaque transport, scheduling, pressure control, health,
packaging, and one-member or multi-member Station behavior. It has no
production protocol implementation, registry, loader, or supervisor.
_Avoid_: plugin host, LicoArc runtime, agent server

**Native Station Contract**:
The versioned, protocol-neutral BadTower service contract for Reserve, Submit,
Claim, Settle, health, capacity, and graceful shutdown. It is directly usable
without a protocol plugin.
_Avoid_: external protocol, plugin API, endpoint SDK

**Station**:
One logical independently operated BadTower Service Layer boundary. It is
realized by one Station Instance or, after separate implementation and
verification, one Station Cluster.
_Avoid_: trusted relay, federation, fleet

**Station Instance**:
The mandatory first deployment closure: one independently startable,
durable, recoverable, packaged, observable, and performant Station Core node
with no cluster coordinator and no required protocol plugin.
_Avoid_: cluster, protocol service composition

**Station Cluster**:
The deferred multi-node deployment of one logical Station. It extends the
completed Station Instance contract with membership, placement, replication,
consistency, fencing, failover, repair, rebalance, horizontal change, and
disaster recovery.
_Avoid_: parallel single-node foundation, protocol topology, federation

**Station Namespace**:
A bounded native caller scope for authorization, resource accounting, state,
and observation. It carries no protocol identity or semantics.
_Avoid_: user account, agent identity, plugin registry entry

**Station Address**:
An opaque, bounded-lifetime destination token inside one Station Namespace.
It is not an endpoint identity, agent address, federation member, or proof of
ownership.
_Avoid_: mailbox identity, peer identity, account

**Transport Unit**:
Opaque bytes plus the fixed native metadata needed for unit identity,
destination, expiry, size, idempotency, and bounded carriage. Core does not
interpret the bytes.
_Avoid_: protocol message, task, tool call, plaintext envelope

**Delivery Claim**:
A time- and count-bounded claim over eligible Transport Units, bound to a
monotonic ownership epoch.
_Avoid_: endpoint lock, delivery proof, task lease

**Settlement**:
An idempotent native instruction to complete or release currently claimed
Transport Units.
_Avoid_: endpoint receipt, business acknowledgement, protocol transition

**Station Signal**:
Any reservation result, admission result, claim, time, capacity state,
settlement, health, or delivery statement produced by a Station. Its trust
meaning belongs to the caller's protocol and endpoint model.
_Avoid_: authenticity proof, final receipt, effect evidence

**Capacity Profile**:
A closed set of workload assumptions, hard limits, durability rules, failure
model, latency, throughput, fairness, memory, storage, and recovery targets for
one deployment shape.
_Avoid_: universal benchmark, marketing tier

**Implementation Ceiling**:
A hard upper bound enforced by the Station Core or an external plugin. Runtime
configuration may lower but never raise it.
_Avoid_: operator preference, advisory limit

**Native Excellence Gate**:
The hard prerequisite requiring verified Station Core, Station Instance, and
Station Cluster correctness, bounded resources, performance profiles,
recovery, and complete legacy-path removal before external extension
implementation begins.
_Avoid_: roadmap milestone inferred from partial code

**External Extension Contract**:
A separately packaged developer boundary consisting of a closed
descriptor, scoped native access, synthetic harnesses, and minimal SDK
conventions. It reuses but does not widen the Native Station Contract.
_Avoid_: in-process API, code loader, marketplace, protocol registry

**Protocol Compatibility Plugin**:
An optional external process that implements one exact external protocol
contract and maps it to the Native Station Contract. It owns its listener,
protocol code, configuration, optional bounded local state, resources,
metrics, tests, artifact, version, release, support, and operation.
_Avoid_: Core module, built-in protocol, shared adapter

**Plugin Descriptor**:
A closed external manifest binding one plugin artifact to its ID, external
protocol identity, extension-contract range, listener ownership,
configuration version, optional local-state version, and resource request.
Core does not discover or load it.
_Avoid_: runtime registry entry, dynamic code manifest

**Plugin-local State**:
Protocol-specific state stored outside Station state, absent by default and
bounded, versioned, backed up, restored, and retired by exactly one plugin.
_Avoid_: Core state extension, shared protocol database

**LicoArc Compatibility Plugin**:
The optional external BadTower process that implements one deliberately
admitted exact Lico Arc wire-and-lifecycle contract. BadTower owns all
adaptation and evidence; LicoArc owns none of this product work.
_Avoid_: Station Core, LicoArc repository runtime, shared release

**A2A Compatibility Plugin**:
The optional external process for one exact pinned A2A service contract. It
owns all A2A agent, task, message, artifact, context, binding, state, and
compatibility behavior it supports.
_Avoid_: Core agent runtime, ACP alias, MCP transport

**ACP Compatibility Plugin**:
The separately admitted optional external process for one exact pinned ACP
contract and support horizon. It shares no artifact, listener, state, alias,
migration, or lifecycle with the A2A plugin.
_Avoid_: hidden A2A route, permanent Core compatibility path

**MCP Compatibility Plugin**:
The optional external process for one exact pinned remote MCP server or
gateway contract. It owns MCP framing, negotiation, authorization mapping,
capabilities, protocol state, and any effect boundary it supports.
_Avoid_: Core tool runtime, A2A transport, application gateway

**External Protocol Intake**:
BadTower's unilateral decision to implement one exact external contract in
one Protocol Compatibility Plugin. It creates no implementation, fixture,
test, package, release, support, or receipt duty for the protocol authority.
_Avoid_: shared implementation plan, directed artifact handoff

**Direct Native Client**:
A client or test harness using only the Native Station Contract. It is the
canonical proof that BadTower communicates without a protocol plugin.
_Avoid_: compatibility plugin, endpoint product

**Infrastructure Distribution**:
Service Layer mechanisms that keep one logical Station available across
multiple Core members. It cannot define protocol participants, topology,
membership, tasks, sessions, routing, or delivery meaning.
_Avoid_: federation, agent network, protocol topology

**Communication Topology**:
Relationships among parties defined by an external protocol. Station
membership and partition placement never create this topology.
_Avoid_: cluster topology

**Black-box Compatibility Verification**:
Optional downstream verification that drives one public plugin boundary with
synthetic inputs and exact immutable identities. It is not Core verification,
protocol publication, release, support, or operation.
_Avoid_: source integration test, protocol-owner prerequisite

**Compatibility Receipt**:
A privacy-safe optional result binding only the immutable identities and
bounded outcome classes needed to reproduce one Black-box Compatibility
Verification.
_Avoid_: endpoint evidence, lifecycle promotion, runtime dependency

**Liveness**:
A service-local signal that the Station process can answer its liveness
contract.
_Avoid_: readiness, network availability

**Readiness**:
A service-local signal that the Station can admit native work within current
dependencies and ceilings.
_Avoid_: endpoint reachability, protocol support

**Graceful Shutdown**:
A bounded transition that stops new admission, handles accepted native work
under the closed contract, persists required state, and terminates.
_Avoid_: fleet rollout, delivery guarantee

**Station Observation**:
Privacy-minimal bounded statuses, counters, timings, saturation, fairness,
cleanup, topology, and recovery reason classes.
_Avoid_: payload log, endpoint evidence, raw runtime data

## Invariants

1. The Station Core is directly usable and verifiable with no plugin source,
   artifact, process, descriptor, or protocol identifier.
2. The Native Station Contract contains only fixed protocol-neutral transport
   fields; arbitrary protocol metadata cannot enter as generic expansion.
3. Station Instance completion precedes Station Cluster implementation.
4. Native Excellence Gate completion precedes external extension
   implementation.
5. A plugin is always an external process. Core never discovers, loads,
   links, embeds, starts, stops, upgrades, rolls back, or interprets it.
6. Every plugin owns disjoint listeners, configuration, state, native
   namespace, budgets, metrics, tests, artifact, version, release, support,
   and operation.
7. A plugin's absence, crash, overload, upgrade, rollback, or retirement
   cannot alter native Station behavior or another plugin.
8. A plugin requirement cannot widen Core unless the capability independently
   serves native clients, remains protocol-neutral, and passes Core acceptance.
9. Station Signals never become endpoint, protocol, task, tool, or effect
   evidence.
10. Migration initializes fresh current state and removes old implementations,
    names, schemas, aliases, dual paths, and discovery in one closure.
