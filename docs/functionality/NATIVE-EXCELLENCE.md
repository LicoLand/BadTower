# Native Excellence Gate

Native Excellence is a hard evidence gate, not a source-code label. It requires
the same candidate to close Station Core, complete Station Instance acceptance,
complete Station Cluster lifecycle and capacity evidence, recovery, privacy,
package, migration, and retired-path absence.

The repository currently has a deterministic shared native machine, durable
single-node implementation, acceptance drivers, a production-assembled
control-plus-64-data-group Cluster, dynamic membership and rebalance,
Core-bearing snapshot recovery, coordinated backup/isolated restore,
rolling-upgrade preflight, and an executable Core firewall. Repository
regression and the focused four-node Cluster lifecycle gate pass.

The gate is nevertheless **not accepted**. The required Station Instance
`-race` capacity profile remains below 3,000 terminal units/s, its soak has not
been accepted, and the formal Cluster load/soak evidence remains unrecorded.
Source completeness and focused lifecycle evidence do not replace those
capacity gates.

External extension and LicoArc Candidate code present in this repository is
therefore quarantined candidate source. It is built and tested independently,
is absent from `cmd/badtower` and its dependency graph, and is not admitted as
a production extension until this document and [`../STATUS.md`](../STATUS.md)
can cite the complete native evidence.
