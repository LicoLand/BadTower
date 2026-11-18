# External Extension Contract v1

`badtower.extension-descriptor.v1` defines an external-process developer
boundary. It does not create a plugin host inside BadTower.

The closed descriptor binds plugin and artifact versions, one external
protocol identity and line, lifecycle, exact contract range, listener
ownership, one opaque native namespace with a scoped caller credential,
lowerable resource budgets, and the packaged binary path and SHA-256 digest.
Unknown fields and widened limits fail validation.

The minimal Go surface in `pkg/extension/v1` exposes only descriptor handling
and the five existing native gRPC methods. The synthetic harness applies
concurrent, queued, and request-byte admission around an ordinary native
client. Exhausting or removing that wrapper leaves the underlying direct
native client unchanged.

Core never discovers, loads, starts, stops, supervises, stores, or interprets
an extension. A deployment supervisor owns each process, listener,
configuration, credential, state root, metrics, upgrade, rollback, release,
support, and operation.
