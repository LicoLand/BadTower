import assert from "node:assert/strict";
import { readdir, readFile, stat } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadPinnedFabrigent } from "../src/fabrigent.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const forbidden = [
  /client-runtime/i,
  /host-permission/i,
  /operation-permission/i,
  /process-identity/i,
  /plaintext-inspection/i
];
const pinnedPolicyLiterals = [
  "client-runtime-coordination",
  "host-permission-authority",
  "plaintext-inspection"
];
const maxSourceBytes = 2 * 1024 * 1024;

async function sourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    if ([".git", "vendor", "node_modules", "build"].includes(entry.name)) continue;
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await sourceFiles(absolute));
    else {
      assert.ok(entry.isFile(), `${path.relative(root, absolute)} is not a regular file`);
      files.push(absolute);
    }
  }
  return files;
}

await loadPinnedFabrigent();
for (const file of await sourceFiles(root)) {
  if (file.endsWith("verify-boundary.mjs") ||
      file.endsWith("README.md") ||
      file.endsWith("README.zh-CN.md")) continue;
  const fileSize = (await stat(file)).size;
  assert.ok(fileSize <= maxSourceBytes, `${path.relative(root, file)} exceeds source size limit`);
  let content = await readFile(file, "utf8");
  if (file.endsWith("src/fabrigent.mjs")) {
    for (const literal of pinnedPolicyLiterals) {
      content = content.replaceAll(JSON.stringify(literal), "");
    }
  }
  for (const pattern of forbidden) {
    assert.ok(!pattern.test(content), `${path.relative(root, file)} violates BadTower boundary`);
  }
}
