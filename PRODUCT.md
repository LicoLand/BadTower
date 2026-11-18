# BadTower Product

Within LicoLand's product architecture, BadTower is the **Service Layer**: one
pure, protocol-neutral communication station. Its native communication
capability is the product.

BadTower is not a container for A2A, ACP, MCP, LicoArc, or another protocol.
Those protocols may later be supported only by optional external
**Protocol Compatibility Plugins** after the native Station is complete. The
Station Core contains no protocol implementation, registry, loader, or plugin
supervisor and remains fully useful when every plugin is absent.

Every endpoint or client treats Station Signals according to its own trust
model. BadTower provides bounded transport behavior without becoming an
endpoint, identity, cryptographic, protocol, topology, governance, execution,
or final-delivery authority.

Current implementation, verification, release, support, and operation facts
are recorded only in [`docs/STATUS.md`](docs/STATUS.md). Repository vocabulary
is defined in [`CONTEXT.md`](CONTEXT.md).

## Approved product intent

BadTower exposes one versioned **Native Station Contract** with four capability
families:

- reserve or renew one opaque Station Address;
- submit one opaque Transport Unit under explicit expiry and idempotency;
- claim a bounded ordered batch under an expiring Delivery Claim; and
- settle claimed units with deterministic complete or release behavior.

The contract also owns bounded health, capacity, and graceful-shutdown
behavior. It carries no external protocol semantics.

Delivery maturity is strictly ordered:

1. complete the protocol-neutral Station Core;
2. complete one independently startable, durable, recoverable, packaged,
   observable, and performant Station Instance;
3. only then extend the same accepted contract into a high-availability
   Station Cluster;
4. only after native single-node and clustered excellence is verified, create
   an external extension contract and tooling; and
5. only then implement explicitly admitted, independently packaged protocol
   compatibility plugins.

Later stages cannot repair or redefine unfinished native behavior. A protocol
plugin is never a prerequisite for Station Core, Station Instance, or Station
Cluster completion.

## Station Core ownership

The Station Core owns only:

- its process lifecycle, explicit configuration, liveness, readiness, graceful
  shutdown, and deterministic package;
- the Native Station Contract and direct native client behavior;
- Station Namespaces, opaque Station Addresses, opaque Transport Units,
  Delivery Claims, Settlement Records, and Capacity Profiles;
- bounded admission, authorization scope, storage, scheduling, fairness,
  idempotency, deduplication, expiry, retry, cleanup, and backpressure;
- hard ceilings for requests, bytes, connections, queues, workers, retained
  state, claims, retries, maintenance, and observation;
- privacy-minimal service-local metrics and failure classes;
- one-member durability, backup, restore, upgrade, rollback, and recovery; and
- after standalone completion, same-Station membership, placement,
  replication, consistency, fencing, failover, repair, rebalance, horizontal
  change, and disaster recovery.

The Core's public and persistent schemas contain no agent, task, tool,
resource, prompt, session, federation, external wire, protocol lifecycle,
endpoint identity, or product-specific field.

## External extension ownership

External extension work is a separate, deferred stage. It may provide:

- a versioned External Extension Contract that reuses but does not widen the
  Native Station Contract;
- a closed plugin descriptor, scoped native credentials, synthetic harnesses,
  and a minimal SDK;
- independent package, signature, compatibility, resource-budget, upgrade,
  rollback, retirement, and privacy-safe evidence conventions; and
- deployment examples in which an external supervisor owns plugin processes.

It does not add runtime discovery, code loading, protocol registration,
plugin supervision, or arbitrary extension state to the Core.

Each Protocol Compatibility Plugin is an external process with its own
listener, protocol implementation, configuration, optional bounded local
state, resource budget, metrics, tests, artifact, version, release, support,
and operation facts. It maps one exact external protocol contract onto the
fixed Native Station Contract.

The approved optional plugin plan set is:

- a BadTower-owned LicoArc compatibility plugin;
- an A2A compatibility plugin;
- an independently admitted ACP compatibility plugin; and
- an MCP compatibility plugin.

The plans are independent. ACP is not implemented as an alias or hidden path
inside A2A; the two plugins share no process, listener, configuration, state,
adapter, package, version, or support lifecycle. No old ACP state is
discovered or migrated.

## External protocol ownership

BadTower is not any external protocol's authority. Protocol owners retain
their schemas, wire meaning, lifecycle, conformance material, artifacts,
topology, and governance.

Adapting an external protocol is a unilateral BadTower decision. The selected
plugin owns every parser, validator, mapping, fixture, behavior and
compatibility test, package, release, support decision, and operation
procedure. BadTower cannot assign product-specific implementation,
integration, testing, debugging, release, support, or receipt work to the
protocol owner.

In particular, LicoArc develops and verifies its Protocol Layer independently.
If BadTower later supports one Lico Arc line, BadTower alone owns its external
LicoArc compatibility plugin. LicoArc is not a Station Core prerequisite and
owes no BadTower adaptation deliverable.

## Product boundaries

BadTower does not own:

- endpoint keys, encryption, decryption, plaintext, identity proof, approval,
  replay decisions, local effects, or final endpoint evidence;
- agent identity, task execution, tool execution, resources, prompts,
  sessions, discovery, protocol authorization, or protocol lifecycle inside
  the Station Core;
- endpoint-product-specific routes, fields, accounts, SDKs, or security
  workflows;
- LicoArc, A2A, ACP, MCP, or any other external protocol schema, policy,
  corpus, artifact, publication, topology, or governance;
- cross-plugin translation or shared protocol state;
- multi-station desired state, fleet reconciliation, rollout, rollback, or
  hosted-network operation; or
- application identities, permissions, plugins, services, and effect policy.

## Native performance standard

BadTower treats “optimal communication capability” as an evidence-bound
engineering target, never an unqualified claim. Each capacity profile freezes
its workload, limits, durability boundary, failure model, latency percentiles,
throughput, fairness, memory, storage amplification, restart, cleanup, backup,
and restore targets.

The native design must use bounded data structures, indexed expiry,
partition-local ownership, atomic idempotency, fair scheduling, batch-aware
I/O, backpressure, and incremental maintenance. It must compare relevant
primary open-source broker designs and justify any different structure with
measured correctness and resource evidence. Happy-path throughput cannot
compensate for starvation, ambiguous durability, unsafe recovery, or
unbounded allocation.

## Trust boundary

A Station Signal can guide service-local retry or operation, but cannot prove
peer identity, authenticity, integrity, freshness, replay rejection, user
approval, endpoint receipt, task completion, tool effect, or business
acknowledgement.

The Station Core treats payloads as opaque bytes. A plugin may parse the
minimum external protocol surface it owns but cannot transfer that meaning,
trust, or protected content into Core. Optional external black-box
compatibility verification drives only one public plugin boundary with
synthetic inputs and never becomes a Core, protocol-owner, or sibling-plugin
dependency.

## Deployment boundary

A Station Instance is one replaceable native Core node. A Station Cluster is
multiple replaceable Core members representing the same logical Station after
the complete Station Instance gate passes. Cluster membership, replication,
placement, consistency, failover, and recovery are Service Layer
infrastructure and never define external protocol topology.

Every deployment remains untrusted from an endpoint's perspective. External
fleet management neither changes nor overrides Station-local semantics.

## Privacy and migration invariants

Observation is bounded, low-cardinality, and privacy-minimal. It must not
expose payloads, ciphertext, endpoint identifiers, protocol content, keys,
credentials, private addresses, machine identity, or raw runtime state.

Migration to the native Station model is complete and one-way. It removes the
current integrated protocol-shaped routes, fields, registry, schemas,
configuration, tests, state discovery, names, and documentation in the same
closure. The current-name state initializes fresh. No compatibility facade,
dual read or write, automatic old-state migration, in-process plugin path, or
alternate native implementation remains.

## License

Repository source uses AGPL-3.0-or-later. See [`LICENSE`](LICENSE).
