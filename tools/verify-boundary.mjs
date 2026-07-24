import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
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

async function sourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    if ([".git", "vendor", "node_modules", "build"].includes(entry.name)) continue;
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await sourceFiles(absolute));
    else files.push(absolute);
  }
  return files;
}

await loadPinnedFabrigent();
for (const file of await sourceFiles(root)) {
  if (file.endsWith("verify-boundary.mjs") || file.endsWith("src/fabrigent.mjs") ||
      file.endsWith("README.md") ||
      file.endsWith("README.zh-CN.md")) continue;
  const content = await readFile(file, "utf8");
  for (const pattern of forbidden) {
    assert.ok(!pattern.test(content), `${path.relative(root, file)} violates BadTower boundary`);
  }
}
