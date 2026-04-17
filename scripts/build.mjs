import { spawnSync } from "node:child_process";
import { join } from "node:path";
import { rmSync } from "node:fs";

const cwd = process.cwd();
const distDir = join(cwd, "dist");
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
