import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

export const PINNED_FABRIGENT_DIGEST =
  "f1ecb7061131b3a4dee1af5004d663c407f2dfaee93a379a5262aad3fe110e3f";

export async function loadPinnedFabrigent() {
  const artifact = JSON.parse(
    await readFile(new URL("../vendor/fabrigent-v2.json", import.meta.url), "utf8")
  );
  assert.equal(artifact.artifactVersion, "fabrigent.bundle.v2");
  assert.equal(artifact.digestAlgorithm, "sha256");
  const canonical = `${JSON.stringify({
    artifactVersion: artifact.artifactVersion,
    digestAlgorithm: artifact.digestAlgorithm,
    sources: artifact.sources
  })}\n`;
  const actual = createHash("sha256").update(canonical).digest("hex");
  assert.equal(actual, artifact.digest, "Fabrigent artifact self-digest mismatch");
  assert.equal(actual, PINNED_FABRIGENT_DIGEST, "Fabrigent pin mismatch");

  const policy = artifact.sources["policies/v2/relay-governance.json"];
  assert.equal(policy.trustModel, "relay-is-untrusted");
  for (const capability of [
    "client-key-custody",
    "encryption",
    "decryption",
    "plaintext-inspection",
    "host-permission-authority",
    "client-runtime-coordination"
  ]) {
    assert.ok(policy.forbiddenCapabilities.includes(capability));
  }
  for (const limit of [
    "maxCiphertextBytes",
    "maxEnvelopeRetentionSeconds",
    "maxLeaseSeconds",
    "maxMailboxEnvelopes",
    "maxMailboxes",
    "maxRelayEnvelopes"
  ]) {
    assert.ok(Number.isSafeInteger(policy.limits[limit]));
    assert.ok(policy.limits[limit] > 0);
  }
  deepFreeze(artifact);
  return Object.freeze({ artifact, policy });
}

function deepFreeze(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value)) return value;
  for (const child of Object.values(value)) deepFreeze(child);
  return Object.freeze(value);
}
