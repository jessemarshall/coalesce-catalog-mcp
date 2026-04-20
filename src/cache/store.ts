import {
  mkdirSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, join, relative } from "node:path";
import { CACHE_DIR_NAME, getCacheRoot, getSessionDir } from "./paths.js";

const MAX_FILES_PER_SESSION = 500;
const STALE_SESSION_MS = 24 * 60 * 60 * 1000;

export interface StoredArtifact {
  relPath: string;
  absPath: string;
  byteSize: number;
}

/**
 * Write a JSON payload to the session cache directory atomically: write to a
 * temp file, then rename into place. This avoids readers seeing a
 * half-written file if the process crashes mid-write.
 */
export function writeJsonArtifact(
  relPath: string,
  payload: unknown
): StoredArtifact {
  const sessionDir = getSessionDir();
  const absPath = join(sessionDir, relPath);
  mkdirSync(dirname(absPath), { recursive: true });

  const serialized = JSON.stringify(payload, null, 2);
  const tempPath = `${absPath}.tmp-${process.pid}-${randomUUID()}`;
  writeFileSync(tempPath, serialized, "utf8");
  renameSync(tempPath, absPath);

  return {
    relPath,
    absPath,
    byteSize: Buffer.byteLength(serialized, "utf8"),
  };
}

export function readArtifact(absPath: string): string {
  return readFileSync(absPath, "utf8");
}

/**
 * Enumerate every artifact in the current session directory. Used by the
 * ResourceTemplate list() handler so clients can discover cached payloads.
 * Temp files (.tmp-*) are excluded.
 */
export function listSessionArtifacts(): StoredArtifact[] {
  const sessionDir = getSessionDir();
  let entries: { absPath: string; relPath: string; byteSize: number }[] = [];
  try {
    entries = walk(sessionDir, sessionDir);
  } catch (err) {
    if (isEnoent(err)) return [];
    throw err;
  }
  return entries;
}

function walk(root: string, dir: string): StoredArtifact[] {
  const out: StoredArtifact[] = [];
  for (const name of readdirSync(dir)) {
    if (name.includes(".tmp-")) continue;
    const abs = join(dir, name);
    const stat = statSync(abs);
    if (stat.isDirectory()) {
      out.push(...walk(root, abs));
      continue;
    }
    if (!stat.isFile()) continue;
    out.push({
      absPath: abs,
      relPath: relative(root, abs).split(/[\\/]/).join("/"),
      byteSize: stat.size,
    });
  }
  return out;
}

/**
 * Best-effort cleanup at server startup:
 *  - Remove sibling session directories whose mtime is older than 24h.
 *  - If the current session's file count exceeds the cap, drop the oldest.
 * All errors are swallowed — a failed cleanup must not prevent server boot.
 */
export function cleanupStaleSessions(): void {
  try {
    enforceSessionCap();
  } catch {
    // best-effort
  }
  try {
    purgeOldSiblings();
  } catch {
    // best-effort
  }
}

function enforceSessionCap(): void {
  const artifacts = listSessionArtifacts();
  if (artifacts.length <= MAX_FILES_PER_SESSION) return;
  const withMtime = artifacts.map((a) => ({
    ...a,
    mtimeMs: safeStatMtime(a.absPath),
  }));
  withMtime.sort((a, b) => a.mtimeMs - b.mtimeMs);
  const overflow = withMtime.length - MAX_FILES_PER_SESSION;
  for (let i = 0; i < overflow; i++) {
    try {
      rmSync(withMtime[i].absPath);
    } catch {
      // best-effort
    }
  }
}

function purgeOldSiblings(): void {
  const root = getCacheRoot();
  const currentSession = getSessionDir();
  let children: string[];
  try {
    children = readdirSync(root);
  } catch (err) {
    if (isEnoent(err)) return;
    throw err;
  }
  const now = Date.now();
  for (const name of children) {
    if (!name.startsWith("session-")) continue;
    const abs = join(root, name);
    if (abs === currentSession) continue;
    const mtime = safeStatMtime(abs);
    if (now - mtime < STALE_SESSION_MS) continue;
    try {
      rmSync(abs, { recursive: true, force: true });
    } catch {
      // best-effort
    }
  }
}

function safeStatMtime(path: string): number {
  try {
    return statSync(path).mtimeMs;
  } catch {
    return 0;
  }
}

function isEnoent(err: unknown): boolean {
  return (
    typeof err === "object" &&
    err !== null &&
    (err as { code?: string }).code === "ENOENT"
  );
}

export { CACHE_DIR_NAME };
