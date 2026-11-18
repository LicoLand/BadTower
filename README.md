# BadTower

Updated: 2026-09-25

**[简体中文](README.zh-CN.md)** — English is normative for this README pair.

BadTower is LicoLand's **Service Layer**: one pure, protocol-neutral
communication Station. Its native communication capability is the product.

The approved Native Station Contract directly provides bounded Reserve,
Submit, Claim, and Settle behavior for opaque Transport Units, together with
health, capacity, persistence, pressure control, recovery, and graceful
shutdown. It contains no agent, task, tool, resource, session, federation,
endpoint-trust, or external-protocol semantics and must work with no plugin.

Delivery order is mandatory:

1. complete the Station Core;
2. complete and verify one standalone Station Instance;
3. only then extend the same native contract into a Station Cluster;
4. only after native single-node and clustered excellence, provide an external
   extension contract; and
5. only then implement explicitly selected external Protocol Compatibility
   Plugins.

LicoArc, A2A, ACP, and MCP are optional external plugin plans, not Core
features. Each plugin is a separately built, packaged, configured, supervised,
versioned, released, supported, and operated process with its own listener,
protocol code, optional bounded state, resource budget, metrics, and tests.
The Core never discovers, loads, embeds, or supervises plugins.

Adding, removing, crashing, saturating, upgrading, rolling back, or retiring
one plugin cannot change the Core artifact, native contract, persisted schema,
or another plugin. A plugin requirement cannot widen the native contract
unless the capability independently serves direct native clients and passes
Core acceptance.

BadTower alone owns any LicoArc adaptation it later admits. LicoArc develops
and verifies its Protocol Layer independently and owes BadTower no adapter,
fixture, test, package, release, support, runner, or receipt.

BadTower is not an endpoint trust layer or external protocol authority.
Station Signals never prove identity, authenticity, integrity, freshness,
replay rejection, endpoint receipt, approval, task completion, tool effect, or
business acknowledgement. Observation must not expose payloads, protocol
content, keys, credentials, endpoint identifiers, machine identity, or raw
runtime state.

The current source implements a native Station Instance candidate and a
partial Cluster consensus foundation: one deterministic mutation machine,
64-partition placement, ownership fencing, bounded in-flight etcd/raft, durable restart
replay, quorum failover, and authenticated peers. Dynamic Cluster lifecycle,
full capacity/recovery evidence, and the Native Excellence Gate remain open.

The repository also contains an isolated External Extension Contract and one
separately built exact LicoArc Candidate adapter. They are quarantined candidate
source: absent from the Core dependency graph and not production-admitted while
Native Excellence remains open. Verification, release, support, and operation
are independent and are not inferred. See [current status](docs/STATUS.md).

The repository also implements a deterministic signed standard node package
with a strict manifest, detached Ed25519 signature, and bounded independent
verification. Local implementation and verification do not claim publication.

Run the service:

```sh
badtower --config /path/to/badtower.station.v1.json
```

Validate configuration:

```sh
badtowerctl check-config --config /path/to/badtower.station.v1.json
```

Run repository verification:

```sh
go test -race ./...
go vet ./...
go build ./...
```

## Documentation

- [Product overview](PRODUCT.md)
- [Domain language](CONTEXT.md)
- [Current status](docs/STATUS.md)
- [Documentation index](docs/README.md)
- [Architecture](docs/architecture/ARCHITECTURE.md)
- [Runbook](docs/RUNBOOK.md)
- [Compatibility](docs/COMPATIBILITY.md)
- [Standard node package](docs/functionality/NODE-PACKAGE.md)
- [Station Instance acceptance](docs/functionality/STATION-INSTANCE-ACCEPTANCE.md)
- [Station Cluster](docs/functionality/STATION-CLUSTER.md)
- [Native Excellence Gate](docs/functionality/NATIVE-EXCELLENCE.md)
- [External Extension Contract](docs/functionality/EXTERNAL-EXTENSION-CONTRACT.md)
- [Exact LicoArc Candidate adapter](docs/protocols/LICOARC-CANDIDATE-ADAPTER.md)
- [Security policy](SECURITY.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

License: [AGPL-3.0-or-later](LICENSE).
