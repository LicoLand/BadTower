# Entity Configuration Layout

BadTower has no entity configuration directory and no externalized
configuration files. This document fixes the complete configuration
surface so operators can audit it in one place.

## Configuration inputs

| Input | Authority | Notes |
| --- | --- | --- |
| Policy limits | `vendor/fabrigent-v1.json` (`policies/v1/relay-governance.json`) | Maximum ciphertext bytes, maximum lease seconds, maximum envelopes per mailbox. |
| Artifact pin | `PINNED_FABRIGENT_DIGEST` in `src/fabrigent.mjs` | SHA-256 of the vendored artifact sources. |

## Rules

- Configuration is explicit, bounded, and fail closed. Unknown or changed
  inputs stop the node instead of being coerced.
- No compatibility aliases, environment discovery, or retired-name state
  roots exist or may be added.
- The configuration surface contains no secrets; the node never holds
  credentials or key material.
