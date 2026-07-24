import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

export const PINNED_FABRIGENT_DIGEST =
  "bf63cd32b9e24ac1c079ffd5b853e797cdd505c05e0defd4149a9207e2da4697";

export async function loadPinnedFabrigent() {
  const artifact = JSON.parse(
    await readFile(new URL("../vendor/fabrigent-v1.json", import.meta.url), "utf8")
  );
  const canonical = `${JSON.stringify(artifact.sources)}\n`;
  const actual = createHash("sha256").update(canonical).digest("hex");
  assert.equal(actual, artifact.digest, "Fabrigent artifact self-digest mismatch");
  assert.equal(actual, PINNED_FABRIGENT_DIGEST, "Fabrigent pin mismatch");

  const policy = artifact.sources["policies/v1/relay-governance.json"];
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
  return Object.freeze({ artifact, policy });
}
