# Examples

All examples use synthetic values from the vendored conformance fixtures.
Run from the repository root with Node.js 22 or newer.

## Relay walkthrough

```js
import { BadTowerRelay } from "./src/index.mjs";

const relay = await BadTowerRelay.create();

const mailboxId = "box_000000000001";
relay.lease(mailboxId, 60);

const envelope = {
  contractVersion: "fabrigent.relay.v1",
  envelopeId: "env_000000000001",
  mailboxId,
  ciphertext: "synthetic-ciphertext",
  expiresAt: "2030-01-01T00:00:00.000Z"
};

relay.deliver(envelope);                          // { accepted: true, duplicate: false }
relay.receive(mailboxId);                         // [envelope]
relay.acknowledge(mailboxId, envelope.envelopeId); // { acknowledged: true }
relay.cleanup();                                  // removes expired state
```

## Verification

- `npm test` — relay test suite.
- `npm run verify` — boundary scan plus tests.
