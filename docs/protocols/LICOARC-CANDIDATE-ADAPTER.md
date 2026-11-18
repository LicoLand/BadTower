# Exact LicoArc Candidate Adapter

The external adapter under `plugins/licoarc-candidate` is BadTower-owned,
quarantined candidate source. It compiles exactly these immutable binding
facts:

| Fact | Value |
| --- | --- |
| Wire identity | `licoarc.protocol-line.v1` |
| Protocol Line | `licoarc.v1` |
| Transport Profile | `licoarc.https-transport.v1` |
| Lifecycle | `Candidate` |
| Protocol manifest SHA-256 | `96688cc20ea8bcd4726fa33ced35a10ea12349e0a6d5abc23fb585197b06c503` |
| Transport source digest | `6d4636c983606fb266443a3ce05171a8af7ab5284e0ce301be7024ce16592cb5` |

It accepts only HTTP/2 over TLS 1.3, exact POST paths and media types,
canonical Content-Length and bounded headers, deterministic closed CBOR
control maps, and unchanged raw protected-packet bodies. AFFILIATE, RESERVE,
SUBMIT, RETRIEVE, CLAIM, and SETTLE map through one scoped native gRPC caller.

The adapter owns its listener, signer, configuration, namespace, Pebble state,
handles, claims, operation replay, resource limits, tests, binary, descriptor,
and package. Configuration pins one exact lowercase Station descriptor digest
and validity bound; the state root is bound to that digest, the configured
signer, and the opaque native namespace. AFFILIATE rejects every other
descriptor and signs only the exact descriptor/commitment/not-after tuple.
It persists one bounded descriptor/commitment binding, and RESERVE rejects an
absent, mismatched, or expired binding while keeping route service no later
than both descriptor and matched-affiliation validity. Every native call
carries a Station-contract deadline and uses a persisted opaque request
generation, so crash retry is stable while reuse after the shorter adapter
window cannot collide with Native Station idempotency. First-contact
consumption is serialized by handle and persists the pending operation before
the native call. Reserve persists the operation-to-handle result atomically so
a crash before the HTTP response cannot create a second handle. A settled
claim projection remains until its existing expiry so a crash before response
recording can replay the same Native settlement. Ordered deadline keys prune
expired affiliation, native-operation, response, reservation, handle, and
claim state in bounded batches; logical persisted bytes and concurrent or
queued operations are hard-limited by lowerable configuration. CLAIM responses
use the exact 32 MiB Candidate response ceiling rather than the 16 KiB control
request ceiling. Native
responses are treated as untrusted and malformed IDs,
epochs, bounds, item sets, or outcomes fail ambiguous without creating
Endpoint evidence.

Production code and packages do not read, fetch, mount, or include a LicoArc
checkout, bundle, source corpus, alternate wire, or runtime schema. This local
implementation evidence is not LicoArc publication, conformance, support,
interoperability, or hosted operation. Production admission also remains
blocked by BadTower's open Native Excellence Gate.
