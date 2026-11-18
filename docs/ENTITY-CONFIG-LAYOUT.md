# Entity Configuration Layout

BadTower has no entity configuration directory, vendored protocol artifact,
or implicit sibling-repository input. This document fixes the complete
configuration surface so operators can audit it in one place.

## Configuration schema

Process configuration uses strict JSON schema `badtower.station.config.v1`.
The service accepts exactly one file through `--config`; there are no flags or
environment variables for individual fields.

## Configuration inputs

| Field | Required | Notes |
| --- | --- | --- |
| `schema` | yes | Exactly `badtower.station.config.v1`. |
| `profile` | yes | Exactly `single-node-general-v1`. |
| `dataRoot` | yes | Defaults in code to `data/station-core-v1`; retired paths such as `data/badtower.db` are rejected. |
| `transport` | yes | `mode` is `tcp` or `unix`. TCP requires `listen`, `tlsCertificateFile`, and `tlsPrivateKeyFile`. Unix requires `socketPath` with mode `0600`. |
| `health` | yes | `listen` and `allowNonLoopback`; standard gRPC health. |
| `metrics` | yes | `listen` and `allowNonLoopback`; OpenMetrics-compatible scrape surface. |
| `namespaces` | yes | One or more namespace records with `id`, `credentials`, and optional lower `limits`. |

## Namespace credentials

Each namespace binds one or two bearer credentials:

| Field | Notes |
| --- | --- |
| `id` | Native namespace ID matching `ns_` plus 22 base64url characters. |
| `credentials[].sha256` | SHA-256 digest of the raw 32-byte bearer token; the token itself is never stored in configuration. |
| `credentials[].notBefore` / `notAfter` | RFC 3339 validity window; maximum credential lifetime is 90 days. |
| `limits` | Optional lower ceilings for addresses, units, retained bytes, active claims, connections, in-flight RPCs, and long polls. |

Generate bearer tokens with `badtowerctl credential generate`. Map each token to
its digest before adding it to the configuration file.

## Rules

- Configuration is explicit, bounded, and fail closed. Unknown or changed
  inputs stop the node instead of being coerced.
- No compatibility aliases, environment discovery, sibling checkout lookup,
  remote artifact fetch, plugin loading, or retired-name state roots exist or
  may be added.
- Credentials and key material never appear in argv, environment variables, or
  committed configuration. TLS private keys are file references only.
- Retired store paths and identities are rejected at load time.
