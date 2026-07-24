# BadTower Runbook

BadTower currently ships as a library plus verification tooling. This
runbook covers routine verification and service-local checks. It does not
describe a networked deployment; the repository does not implement one yet.

## Prerequisites

- Node.js 22 or newer, as declared in `package.json` `engines`.
- A clean checkout of the repository.

## Routine verification

Run the full repository closure:

```sh
npm run verify
```

This executes `node tools/verify-boundary.mjs` (pinned artifact digest and
forbidden-capability boundary scan) followed by `npm test` (the relay test
suite in `tests/relay.test.mjs`). Both must pass before any change is
published.

## Fabrigent pin mismatch

`src/fabrigent.mjs` fails closed when the vendored artifact no longer
matches its self-digest or the pinned SHA-256. When this happens:

1. Do not bypass the check; the node must not run against a changed
   artifact.
2. Confirm the replacement artifact in the Fabrigent repository.
3. Update `vendor/fabrigent-v1.json` and `PINNED_FABRIGENT_DIGEST` in one
   change, then rerun `npm run verify`.

## Boundary scan findings

`tools/verify-boundary.mjs` scans repository files for forbidden
authority-capability markers. A finding means a change introduced
authority-bearing behavior outside the relay boundary; revert or reshape
the change before release.
