# Station Instance Acceptance

The Station Instance acceptance surface is plugin-free. It exercises the
public native lifecycle and the same `internal/stationcore` state machine used
by replicated execution.

## Gates

- Fast race partitions cover Reserve, Submit, Claim, Settle, idempotency,
  indexed expiry, pressure, fairness, clock safety, crash points, corruption,
  privacy, package creation, backup, and isolated restore.
- The tagged load profile freezes 100 namespaces, 128 bounded producers and
  consumers, a 1/16/256 KiB prebuilt payload mix, Claim and Settle batches no
  larger than 32, a 3,000 terminal-units/s target, p95/p99 mutation ceilings,
  Jain fairness, RSS, and storage amplification. RELEASE, EXTEND, and natural
  claim timeout are non-terminal and never inflate throughput.
- Throughput uses actual monotonic segment elapsed time. Mutation latency uses
  a bounded sharded histogram, and the acceptance clock advances with real
  elapsed time plus explicit fault offsets.
- The tagged soak profile runs two continuous hours at 60% target and injects
  cancellation, credential reload, restart, fsync slowdown, disk pressure,
  backward/forward clock movement, and expiry backlog.

The long gates are intentionally separate from ordinary repository tests:

```sh
go test -race -tags=station_acceptance -timeout=60m ./internal/acceptance -run '^TestStationInstanceLoad$'
go test -race -tags=station_acceptance -timeout=150m ./internal/acceptance -run '^TestStationInstanceSoakAndFaults$'
```

Passing source tests does not create a release, support, or hosted-operation
claim. Current execution facts remain in [`../STATUS.md`](../STATUS.md).
The exact candidate workload and measurement semantics are frozen in
[`../capacity/single-node-general-v1.json`](../capacity/single-node-general-v1.json).

## Candidate store schema

Store identity v2 and schema 4 use one strict root manifest in
`INSTANCE_SHARDED` mode. The manifest fixes `logicalPartitionCount` at 64 and
`storageShardCount` at 1; the standalone instance maps every logical partition
to that one Pebble database so cross-namespace mutations can share a durable
WAL sync. A Cluster Core instead uses `PARTITION_ONE` mode and one database for
its bound logical partition. Both modes use the same bounded process cache and
commit admission budget. The schema separates
immutable Transport Unit payload bytes from mutable
delivery metadata. Submit writes both exactly once in one synchronous atomic
batch; Claim, RELEASE, and EXTEND update metadata without rewriting the opaque
payload; terminal settlement and expiry delete both keys atomically. Active
Claim idempotency stores a bounded compact metadata reference and reconstructs
the response from the immutable payload. Before a path deletes that payload,
the same synchronous batch upgrades the reference to a bounded raw replay
snapshot, preserving post-settlement and post-timeout replay without JSON or
base64 payload amplification.

This is a complete candidate-schema replacement. A root created by an earlier
candidate schema fails closed and must not be discovered, imported, dual-read,
or migrated. Partial, foreign, symlinked, missing, extra, swapped, earlier
flat, and topology-mismatched roots fail closed; verification starts with a
fresh schema-4 root.
