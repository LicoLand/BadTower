# BadTower Node Package

BadTower implements the `badtower.node-package.v1` artifact contract in
`internal/nodepackage/` and exposes it through `cmd/badtower-package/`. The
contract packages one independently built BadTower station executable for one
explicit target. It does not declare a supported-platform matrix, publisher
identity, deployment channel, or release readiness.

The package media type is
`application/vnd.licoland.badtower.node-package.v1+tar+gzip`.

## Detached objects

A complete verification input has three detached objects:

1. one `.tar.gz` archive;
2. one canonical JSON manifest; and
3. one canonical JSON Ed25519 signature envelope.

The manifest binds the archive's byte size and SHA-256 digest as well as the
size and SHA-256 digest of each archive member. The signature authenticates the
exact canonical manifest bytes. The archive does not embed either detached
object.

The manifest fields, in canonical order, are:

| Field | Contract |
| --- | --- |
| `schema` | Exactly `badtower.node-package.v1`. |
| `mediaType` | Exactly `application/vnd.licoland.badtower.node-package.v1+tar+gzip`. |
| `name` | Exactly `badtower`. |
| `version` | A SemVer 2.0.0 version without a `v` prefix. Numeric core and numeric prerelease identifiers reject leading zeroes. |
| `platform` | One lowercase `GOOS-GOARCH` syntax pair, such as `linux-amd64`. Syntax acceptance does not claim that BadTower was built, verified, released, or supported for that target. |
| `stationStoreFormat` | Exactly `badtower.station-core.store.v2`. |
| `archive` | `format`, `bytes`, and lowercase `sha256`; `format` is exactly `tar+gzip`. |
| `entries` | Exactly two ordered `path`, `mode`, `bytes`, and lowercase `sha256` records. |

JSON is UTF-8, has the field order above, contains no optional whitespace, and
ends in one line feed. Unknown fields, duplicate fields, alternate field order,
alternate escaping, trailing JSON values, and non-canonical whitespace fail
verification. Sizes are positive base-10 JSON integers. SHA-256 values are
lowercase 64-character hexadecimal strings.

The store-format field records the binary's current durable-state boundary.
BadTower initializes only that current format and rejects an unknown stored
format. The package contains no station database, does not discover retired
state, and provides no cross-version state import, downgrade, or preservation
guarantee.

## Canonical archive

Creation produces the same bytes for the same binary bytes and license bytes.
The gzip stream uses the Go standard library's best-compression level with an
empty name, comment, and extra field, a zero timestamp, OS value 255, and one
stream only.

The uncompressed stream is USTAR with exactly these regular files in this
order:

| Path | Mode |
| --- | --- |
| `bin/badtower` | `0755` |
| `LICENSE` | `0644` |

Each USTAR header uses UID and GID zero, empty user and group names, an empty
link name, Unix epoch modification time, and no access time, change time,
device, PAX, or extended-attribute metadata. Both files must be non-empty.
There are no directories, links, manifests, signatures, configuration files,
databases, provenance files, or other members.

Version and platform are deliberately detached manifest inputs. They do not
alter archive bytes and are not inferred from the local machine or binary.

## Detached signature

`sign` accepts exactly one unencrypted PKCS#8 PEM Ed25519 private key. `verify`
accepts exactly one PKIX PEM Ed25519 public key. Other algorithms, PEM block
types, multiple blocks, PEM headers, and trailing non-whitespace data are
rejected.

The signed message is:

```text
ASCII("badtower.node-package.v1") || 0x00 || canonical_manifest_bytes
```

The detached signature envelope has these canonical JSON fields:

| Field | Contract |
| --- | --- |
| `schema` | Exactly `badtower.node-package.signature.v1`. |
| `algorithm` | Exactly `Ed25519`. |
| `keyId` | `sha256:` followed by the lowercase SHA-256 digest of the raw 32-byte Ed25519 public key. |
| `signature` | Canonical padded base64 for exactly 64 signature bytes. |

The envelope uses the same no-whitespace, one-final-line-feed canonical JSON
rule as the manifest. The signing command never generates a key, writes a
private key, or includes key material, key paths, account data, or signing
environment metadata in output.

A deployment plan may bind the public key through its own `trustAnchorRef`.
That reference is consumer-owned configuration and is not embedded in this
portable package. Verification checks that the supplied public key matches the
signed `keyId`.

## Bounded fail-closed verification

Default hard ceilings are:

| Representation | Maximum |
| --- | ---: |
| Compressed archive | 160 MiB |
| `bin/badtower` | 128 MiB |
| `LICENSE` | 1 MiB |
| Manifest | 64 KiB |
| Signature envelope | 4 KiB |
| PEM key input | 16 KiB |

The tar stream also has an independent expanded-byte ceiling equal to the two
member ceilings plus 8 KiB of archive framing. Callers may lower all ceilings
together but cannot raise the hard ceilings.

Verification proceeds in this order:

1. bound the manifest, signature, public key, and compressed input;
2. parse the strict signature envelope, match its key identifier, and verify
   the Ed25519 signature over the exact manifest bytes;
3. parse and validate the strict manifest;
4. inspect the single gzip stream and the fixed USTAR members without
   extracting them;
5. verify member content, archive size, and archive digest; and
6. reject any remaining compressed or expanded bytes.

Verification fails closed on malformed gzip or tar framing, checksum failure,
concatenated gzip streams, trailing data, PAX or GNU metadata, non-regular
members, path changes, reordered or additional members, links, wrong modes,
size mismatch, digest mismatch, or any ceiling violation. Archive inspection
is sequential and retains only bounded buffers and hash state; it never writes
archive members to disk.

## CLI

Creation requires explicit source, destination, version, and target values:

```text
badtower-package create \
  --binary <binary-input> \
  --license <license-input> \
  --archive <new-archive-output> \
  --manifest <new-manifest-output> \
  --version <semver> \
  --platform <goos-goarch>
```

Detached signing and verification are separate operations:

```text
badtower-package sign \
  --manifest <manifest-input> \
  --private-key <pkcs8-private-key-input> \
  --signature <new-signature-output>

badtower-package verify \
  --archive <archive-input> \
  --manifest <manifest-input> \
  --signature <signature-input> \
  --public-key <pkix-public-key-input>
```

Creation and signing use exclusive output creation and never overwrite an
existing object. If either creation output cannot be completed, any new output
from that invocation is removed.

Successful commands emit one
`badtower.node-package.receipt.v1` JSON object. A receipt contains only the
operation and status, package schema, name, version, platform, archive and
manifest byte sizes and SHA-256 digests, media type, station store format, and,
when applicable, the signature algorithm, key identifier, and detached
signature-envelope byte size and SHA-256 digest.

Failed commands emit one `badtower.node-package.error.v1` JSON object with a
stable `code`, `stage`, `component`, `retryable` decision, and `recovery`
identifier. Failures never include a filesystem path, operating-system error,
key material, archive content, log, machine identity, or runtime station data.

The stable codes are grouped by their first failing stage:

- CLI and input: `arguments-invalid`, `limits-invalid`, `version-invalid`,
  `platform-invalid`, `binary-size-invalid`, `license-size-invalid`,
  `input-open-failed`, and `input-read-failed`;
- source and output: `source-read-failed`, `archive-write-failed`,
  `output-exists`, and `output-write-failed`;
- manifest: `manifest-too-large`, `manifest-invalid`, and
  `manifest-noncanonical`;
- signature: `private-key-invalid`, `public-key-invalid`,
  `signature-too-large`, `signature-envelope-invalid`,
  `signature-noncanonical`, `signature-key-mismatch`, and
  `signature-verification-failed`;
- archive: `archive-too-large`, `archive-format-invalid`,
  `archive-layout-invalid`, `archive-entry-too-large`,
  `archive-content-mismatch`, `archive-metadata-mismatch`,
  `archive-digest-mismatch`, and `archive-trailing-data`; and
- implementation invariant: `internal-failure`.

Success exits with status zero and uses the operation/status pairs
`create`/`created`, `sign`/`signed`, and `verify`/`verified`. Every typed
failure exits non-zero.
