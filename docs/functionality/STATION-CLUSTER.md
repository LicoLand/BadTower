# Station Cluster

The current Cluster source extends the native command machine without adding
an external protocol or a second domain implementation.

## Implemented candidate boundary

- Native mutations are immutable versioned commands containing leader-chosen
  time, generated identifiers, namespace, operation payload, and ownership
  epoch. Every replica applies the same bytes.
- Namespaces map into 64 logical partitions by interpreting the first eight
  bytes of `SHA-256(namespace bytes)` as one big-endian unsigned integer and
  reducing it modulo 64. Placement uses
  cached rendezvous ranking, replication factor three, fault-domain diversity,
  and monotonic ownership fencing across three to seven declared members.
- Consensus uses `go.etcd.io/raft/v3` with bounded proposal, message,
  uncommitted-entry, and in-flight byte ceilings. Quorum loss fails closed.
- The durable member loop synchronizes Snapshot, HardState, and Entries before
  peer sends, applies committed native commands in order, synchronizes applied
  index and term, and recovers/catches up after restart.
- The peer carrier admits only authenticated HTTP/2 over TLS 1.3, bounds the
  protobuf body before stepping Raft, and uses fixed privacy-safe failures.
- Each production member assembles one durable control group and 64 data
  groups over independent partition roots. Native mutations are leader-only;
  owned reads use quorum ReadIndex and an applied-index barrier.
- Core-bearing snapshots compact retained logs. Small snapshots use the
  bounded embedded path; larger snapshots use a bounded 64 KiB framed mTLS
  stream with per-frame and whole-artifact verification before installation.
- The control group persists 3–7-member admission, version floors, drain
  intent, placement epochs, learner/joint-consensus promotion, and one
  resumable partition move at a time. Data-group ConfChange, catch-up,
  leadership transfer, and removal must commit before control state advances.
- A separately authenticated operator API and CLI cover status, bootstrap,
  join, promote, drain, replace, remove, rebalance, backup, restore preflight,
  and upgrade preflight. Distributed backup discovers each partition leader,
  applies ReadIndex barriers, and publishes a manifest only after all 64
  artifacts verify; restore starts with client admission disabled.
- Focused four-node TLS acceptance covers join, streamed learner catch-up,
  upgrade preflight, interrupted rebalance recovery, drain/removal,
  distributed-leader backup/restore, stale fencing, surviving-quorum RPO 0,
  and measured failover/recovery time.

## Open evidence

The formal 30-minute Cluster load and two-hour soak/fault profiles remain
opt-in and are recorded as not run. The focused lifecycle gate establishes
source behavior and bounded recovery scenarios, not a formal capacity,
availability, release, support, or operation claim. Native Excellence also
depends on the still-failing Station Instance capacity gate, so the Cluster and
all downstream extensions remain candidate source only.
