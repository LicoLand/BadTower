import assert from "node:assert/strict";
import test from "node:test";
import { BadTowerRelay, loadPinnedFabrigent } from "../src/index.mjs";

const mailboxId = "box_000000000001";
const envelope = {
  contractVersion: "fabrigent.relay.v1",
  envelopeId: "env_000000000001",
  mailboxId,
  ciphertext: "synthetic-ciphertext",
  expiresAt: "2030-01-01T00:00:00.000Z"
};

test("loads only the pinned Fabrigent artifact", async () => {
  const { policy } = await loadPinnedFabrigent();
  assert.equal(policy.trustModel, "relay-is-untrusted");
});

test("leases, relays opaque envelopes, acknowledges, and cleans up", async () => {
  let now = Date.parse("2029-01-01T00:00:00.000Z");
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
  const now = Date.parse("2029-01-01T00:00:00.000Z");
  const relay = await BadTowerRelay.create({ now: () => now });
  relay.lease(mailboxId, 60);
  assert.throws(() => relay.deliver({ ...envelope, plaintext: "forbidden" }));
  assert.throws(() => relay.deliver({ ...envelope, permissionGrant: "forbidden" }));
  assert.throws(() => relay.deliver({ ...envelope, clientKey: "forbidden" }));
});
