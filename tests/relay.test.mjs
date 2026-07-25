import assert from "node:assert/strict";
import test from "node:test";
import { BadTowerRelay, loadPinnedFabrigent } from "../src/index.mjs";

const mailboxId = "box_000000000001";
const envelope = {
  contractVersion: "fabrigent.relay.v2",
  envelopeId: "env_000000000001",
  mailboxId,
  ciphertext: "synthetic-ciphertext",
  expiresAt: "2030-01-01T00:00:00.000Z"
};
const initialNow = Date.parse("2029-12-31T23:00:00.000Z");

test("loads only the pinned Fabrigent artifact", async () => {
  const { artifact, policy } = await loadPinnedFabrigent();
  assert.equal(policy.trustModel, "relay-is-untrusted");
  assert.equal(Object.isFrozen(artifact), true);
  assert.equal(Object.isFrozen(policy.limits), true);
  assert.throws(() => {
    policy.limits.maxCiphertextBytes = Number.MAX_SAFE_INTEGER;
  });
});

test("requires the verified asynchronous factory", () => {
  assert.throws(() => new BadTowerRelay({}, () => initialNow), {
    message: "BadTowerRelay must be created with BadTowerRelay.create()"
  });
});

test("leases, relays opaque envelopes, acknowledges, and cleans up", async () => {
  let now = initialNow;
  const relay = await BadTowerRelay.create({ now: () => now });
  relay.lease(mailboxId, 60);
  assert.deepEqual(relay.deliver(envelope), { accepted: true, duplicate: false });
  assert.deepEqual(relay.deliver(envelope), { accepted: true, duplicate: true });
  assert.deepEqual(relay.receive(mailboxId), [envelope]);
  assert.deepEqual(relay.acknowledge(mailboxId, envelope.envelopeId), {
    acknowledged: true
  });
  now += 61_000;
  assert.deepEqual(relay.cleanup(), { removedEnvelopes: 0, removedMailboxes: 1 });
});

test("rejects plaintext and other authority-bearing fields", async () => {
  const now = initialNow;
  const relay = await BadTowerRelay.create({ now: () => now });
  relay.lease(mailboxId, 60);
  assert.throws(() => relay.deliver({ ...envelope, plaintext: "forbidden" }));
  assert.throws(() => relay.deliver({ ...envelope, permissionGrant: "forbidden" }));
  assert.throws(() => relay.deliver({ ...envelope, clientKey: "forbidden" }));
});

test("rejects malformed envelopes and retention outside the policy bound", async () => {
  const relay = await BadTowerRelay.create({ now: () => initialNow });
  relay.lease(mailboxId, 60);
  assert.throws(() => relay.deliver({ ...envelope, envelopeId: "short" }));
  assert.throws(() => relay.deliver({ ...envelope, mailboxId: "short" }));
  assert.throws(() => relay.deliver({ ...envelope, ciphertext: "" }));
  assert.throws(() => relay.deliver({
    ...envelope,
    ciphertext: "😀".repeat(262_145)
  }));
  assert.throws(() => relay.deliver({
    ...envelope,
    expiresAt: "2030-02-30T00:00:00Z"
  }));
  assert.throws(() => relay.deliver({
    ...envelope,
    expiresAt: "2030-01-02T23:00:00.000Z"
  }), { message: "envelope retention quota exceeded" });

  const accessorEnvelope = { ...envelope };
  Object.defineProperty(accessorEnvelope, "ciphertext", {
    enumerable: true,
    get: () => "synthetic-ciphertext"
  });
  assert.throws(() => relay.deliver(accessorEnvelope), {
    message: "envelope fields must be enumerable data properties"
  });
  assert.throws(() => relay.deliver(Object.assign(
    Object.create({ plaintext: "forbidden" }),
    envelope
  )));
});

test("rejects conflicting deduplication and accepts an exact duplicate at quota", async () => {
  const relay = await BadTowerRelay.create({ now: () => initialNow });
  relay.lease(mailboxId, 60);
  for (let index = 0; index < 1000; index += 1) {
    relay.deliver({
      ...envelope,
      envelopeId: `env_${index.toString().padStart(12, "0")}`
    });
  }
  assert.deepEqual(relay.deliver({
    ...envelope,
    envelopeId: "env_000000000000"
  }), { accepted: true, duplicate: true });
  assert.throws(() => relay.deliver({
    ...envelope,
    envelopeId: "env_000000000000",
    ciphertext: "different-ciphertext"
  }), { message: "envelope identifier conflicts with stored content" });
  assert.throws(() => relay.deliver({
    ...envelope,
    envelopeId: "env_000000001000"
  }), { message: "mailbox quota exceeded" });
});

test("bounds relay mailboxes and removes expired quota before new delivery", async () => {
  let now = initialNow;
  const relay = await BadTowerRelay.create({ now: () => now });
  for (let index = 0; index < 1000; index += 1) {
    relay.lease(`box_${index.toString().padStart(12, "0")}`, 60);
  }
  assert.throws(() => relay.lease("box_000000001000", 60), {
    message: "relay mailbox quota exceeded"
  });

  const firstMailbox = "box_000000000000";
  relay.deliver({
    ...envelope,
    mailboxId: firstMailbox,
    expiresAt: "2029-12-31T23:00:01.000Z"
  });
  now += 2_000;
  assert.deepEqual(relay.receive(firstMailbox), []);
  assert.deepEqual(relay.deliver({
    ...envelope,
    mailboxId: firstMailbox,
    envelopeId: "env_000000000002"
  }), { accepted: true, duplicate: false });
  now += 59_000;
  assert.deepEqual(relay.lease("box_000000001000", 60), {
    mailboxId: "box_000000001000",
    leaseExpiresAt: now + 60_000
  });
});

test("does not revive retained envelopes when an expired mailbox is leased again", async () => {
  let now = initialNow;
  const relay = await BadTowerRelay.create({ now: () => now });
  relay.lease(mailboxId, 1);
  relay.deliver(envelope);
  now += 2_000;
  relay.lease(mailboxId, 60);
  assert.deepEqual(relay.receive(mailboxId), []);
});

test("enforces relay-wide quota and reclaims acknowledged or expired capacity", async () => {
  let now = initialNow;
  const relay = await BadTowerRelay.create({ now: () => now });
  for (let mailboxIndex = 0; mailboxIndex < 10; mailboxIndex += 1) {
    const currentMailbox = `box_${mailboxIndex.toString().padStart(12, "0")}`;
    relay.lease(currentMailbox, 86400);
    for (let envelopeIndex = 0; envelopeIndex < 1000; envelopeIndex += 1) {
      relay.deliver({
        ...envelope,
        mailboxId: currentMailbox,
        envelopeId: `env_${(mailboxIndex * 1000 + envelopeIndex)
          .toString().padStart(12, "0")}`
      });
    }
  }
  const overflowMailbox = "box_000000000010";
  relay.lease(overflowMailbox, 86400);
  assert.throws(() => relay.deliver({
    ...envelope,
    mailboxId: overflowMailbox,
    envelopeId: "env_000000010000"
  }), { message: "relay envelope quota exceeded" });
  assert.deepEqual(
    relay.acknowledge("box_000000000000", "env_000000000000"),
    { acknowledged: true }
  );
  assert.deepEqual(relay.deliver({
    ...envelope,
    mailboxId: overflowMailbox,
    envelopeId: "env_000000010000"
  }), { accepted: true, duplicate: false });
  now += 3_600_001;
  assert.deepEqual(relay.deliver({
    ...envelope,
    mailboxId: overflowMailbox,
    envelopeId: "env_000000010001",
    expiresAt: "2030-01-01T01:00:01.000Z"
  }), { accepted: true, duplicate: false });
});

test("validates lease and clock input without coercion", async () => {
  const relay = await BadTowerRelay.create({ now: () => initialNow });
  assert.throws(() => relay.lease(mailboxId, "60"));
  const invalidClockRelay = await BadTowerRelay.create({ now: () => Number.NaN });
  assert.throws(() => invalidClockRelay.lease(mailboxId, 60), {
    message: "now must return a safe integer timestamp"
  });
});
