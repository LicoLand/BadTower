# External Plugin Package v1

`badtower.external-plugin-package.v1` is a deterministic uncompressed tar
containing exactly:

- canonical `plugin-descriptor.json` with mode `0644`;
- repository `LICENSE` with mode `0644`; and
- the descriptor-bound binary with mode `0755`.

The detached canonical manifest binds both entries and the complete archive.
The detached signature uses Ed25519 under a package-specific domain. Bounded
verification rejects unexpected paths, modes, sizes, owners, timestamps,
links, special files, digest differences, signature differences, descriptor-to-
manifest identity/version mismatch, descriptor-to-binary path/digest mismatch,
concatenated archives, and trailing data. Receipts contain public identity,
version, digest, byte count, and verification key ID only.

`badtower-plugin-package descriptor` computes the descriptor binary digest
from the actual build; `create`, `sign`, and `verify` remain separate
operations. No configuration, credential, private key, state, protocol corpus,
runtime row, or publisher metadata enters the package.
