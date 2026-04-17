import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { cpSync, existsSync, mkdirSync, rmSync } from "node:fs";

const cwd = process.cwd();
const distDir = join(cwd, "dist");
const resourceSourceDir = join(cwd, "src", "resources", "context");
const resourceDestDir = join(distDir, "resources", "context");
const tscBin = join(
  cwd,
  "node_modules",
  ".bin",
  process.platform === "win32" ? "tsc.cmd" : "tsc"
);

rmSync(distDir, { recursive: true, force: true });

const result = spawnSync(tscBin, [], {
  cwd,
  stdio: "inherit",
  shell: process.platform === "win32",
});

if (result.status !== 0) {
  process.exit(result.status ?? 1);
}

// Copy resource markdown into dist/ so registerCatalogResources can read
// them at runtime (tsc only emits the .ts files, not adjacent assets).
if (!existsSync(resourceSourceDir)) {
  throw new Error(`Resource source directory not found: ${resourceSourceDir}`);
}
mkdirSync(resourceDestDir, { recursive: true });
cpSync(resourceSourceDir, resourceDestDir, { recursive: true });
