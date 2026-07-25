# Entity Configuration Layout

BadTower has no entity configuration directory, vendored protocol artifact,
or implicit sibling-repository input. This document fixes the complete
configuration surface so operators can audit it in one place.

## Configuration inputs

| Input | Authority | Notes |
| --- | --- | --- |
| Listen address | `-listen` / `BADTOWER_LISTEN` | Defaults to `127.0.0.1:8080`; operators explicitly choose any wider exposure. |
| Data path | `-data` / `BADTOWER_DATA_PATH` | Defaults to `data/badtower.db`; the file is created with owner-only permissions. |
| Default relay profile | `profile.Default()` in `internal/profile/profile.go` | Accepts `licoarc.relay.v1` and applies the implementation ceilings. |
| Optional relay profile | `-profile` / `BADTOWER_PROFILE_PATH` | Strict JSON decoded into `profile.Config`; it may reduce but never exceed implementation ceilings. |

## Rules

- Configuration is explicit, bounded, and fail closed. Unknown or changed
  inputs stop the node instead of being coerced.
- No compatibility aliases, environment discovery, sibling checkout lookup,
  remote artifact fetch, or retired-name state roots exist or may be added.
- The configuration surface contains no secrets; the node never holds
  credentials or key material.
