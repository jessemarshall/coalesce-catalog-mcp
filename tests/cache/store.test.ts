import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readdirSync,
  rmSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { getCacheRoot, getSessionDir } from "../../src/cache/paths.js";
import {
  cleanupStaleSessions,
  listSessionArtifacts,
  readArtifact,
  writeJsonArtifact,
} from "../../src/cache/store.js";

describe("cache store", () => {
  let dir: string;
  let originalEnv: string | undefined;

  beforeEach(() => {
    originalEnv = process.env.COALESCE_CACHE_DIR;
    dir = mkdtempSync(join(tmpdir(), "catalog-cache-store-"));
    process.env.COALESCE_CACHE_DIR = dir;
  });

  afterEach(() => {
    if (originalEnv === undefined) delete process.env.COALESCE_CACHE_DIR;
    else process.env.COALESCE_CACHE_DIR = originalEnv;
    rmSync(dir, { recursive: true, force: true });
  });

  it("writeJsonArtifact writes pretty JSON and returns byteSize", () => {
    const artifact = writeJsonArtifact("tool/abc.json", { hello: "world" });
    expect(existsSync(artifact.absPath)).toBe(true);
    const contents = readArtifact(artifact.absPath);
    expect(JSON.parse(contents)).toEqual({ hello: "world" });
    expect(artifact.byteSize).toBe(Buffer.byteLength(contents, "utf8"));
    expect(artifact.relPath).toBe("tool/abc.json");
  });

  it("writeJsonArtifact creates nested directories", () => {
    writeJsonArtifact("tool/sub/abc.json", { a: 1 });
    const session = getSessionDir();
    expect(existsSync(join(session, "tool/sub/abc.json"))).toBe(true);
  });

  it("leaves no temp files after a successful write", () => {
    writeJsonArtifact("tool/abc.json", { x: 1 });
    const session = getSessionDir();
    const files = readdirSync(join(session, "tool"));
    expect(files.every((f) => !f.includes(".tmp-"))).toBe(true);
  });

  it("listSessionArtifacts returns all files except temp files", () => {
    writeJsonArtifact("tool/a.json", { a: 1 });
    writeJsonArtifact("tool/b.json", { b: 2 });
    const session = getSessionDir();
    writeFileSync(join(session, "tool", "c.json.tmp-99-abc"), "ignored");

    const listed = listSessionArtifacts();
    expect(listed.map((a) => a.relPath).sort()).toEqual([
      "tool/a.json",
      "tool/b.json",
    ]);
  });

  it("listSessionArtifacts returns empty when session dir doesn't exist", () => {
    expect(listSessionArtifacts()).toEqual([]);
  });

  it("cleanupStaleSessions purges sibling session dirs older than 24h", () => {
    writeJsonArtifact("tool/a.json", { a: 1 });

    const root = getCacheRoot();
    const stale = join(root, "session-old-99999");
    mkdirSync(stale, { recursive: true });
    writeFileSync(join(stale, "x.json"), "{}");
    const twoDaysAgo = new Date(Date.now() - 2 * 24 * 60 * 60 * 1000);
    utimesSync(stale, twoDaysAgo, twoDaysAgo);

    cleanupStaleSessions();

    expect(existsSync(stale)).toBe(false);
    expect(existsSync(getSessionDir())).toBe(true);
  });

  it("cleanupStaleSessions preserves young sibling sessions", () => {
    writeJsonArtifact("tool/a.json", { a: 1 });

    const root = getCacheRoot();
    const young = join(root, "session-young-88888");
    mkdirSync(young, { recursive: true });
    writeFileSync(join(young, "x.json"), "{}");

    cleanupStaleSessions();

    expect(existsSync(young)).toBe(true);
  });

  it("cleanupStaleSessions is safe when cache root doesn't exist yet", () => {
    expect(() => cleanupStaleSessions()).not.toThrow();
  });

  // The session-cap path of cleanupStaleSessions: enforceSessionCap drops
  // the oldest files once the session exceeds MAX_FILES_PER_SESSION (500).
  // Previously exercised only incidentally — a regression in the sort
  // direction (newest-first instead of oldest-first) would silently delete
  // recent artifacts.
  it("cleanupStaleSessions drops the oldest files when the session exceeds the cap", () => {
    // Write 502 artifacts and stamp each with a strictly increasing mtime so
    // the sort order is deterministic. The first two are the "oldest" and
    // should be evicted.
    const session = getSessionDir();
    for (let i = 0; i < 502; i++) {
      writeJsonArtifact(`tool/file-${i}.json`, { i });
    }
    for (let i = 0; i < 502; i++) {
      const abs = join(session, `tool/file-${i}.json`);
      const stamp = new Date(Date.now() - (502 - i) * 1000);
      utimesSync(abs, stamp, stamp);
    }

    cleanupStaleSessions();

    const remaining = listSessionArtifacts();
    expect(remaining).toHaveLength(500);
    const names = new Set(remaining.map((a) => a.relPath));
    expect(names.has("tool/file-0.json")).toBe(false);
    expect(names.has("tool/file-1.json")).toBe(false);
    expect(names.has("tool/file-2.json")).toBe(true);
    expect(names.has("tool/file-501.json")).toBe(true);
  });

  it("cleanupStaleSessions is a no-op below the cap", () => {
    for (let i = 0; i < 10; i++) {
      writeJsonArtifact(`tool/file-${i}.json`, { i });
    }
    cleanupStaleSessions();
    expect(listSessionArtifacts()).toHaveLength(10);
  });

  // Orphan .tmp- files left from a mid-write crash must not count toward the
  // cap (they are excluded from listSessionArtifacts) — otherwise an agent
  // that crashes a few times would silently lose real artifacts when temps
  // accumulate. This locks the exclusion at the cap-enforcement layer, not
  // just at the listing layer.
  it("orphan .tmp- files are not counted toward the session cap", () => {
    const session = getSessionDir();
    mkdirSync(join(session, "tool"), { recursive: true });
    // 500 real artifacts (exactly at the cap) + 50 orphan temps.
    for (let i = 0; i < 500; i++) {
      writeJsonArtifact(`tool/file-${i}.json`, { i });
    }
    for (let i = 0; i < 50; i++) {
      writeFileSync(join(session, "tool", `orphan-${i}.json.tmp-99-abc`), "stale");
    }

    cleanupStaleSessions();

    // No real artifacts should have been evicted; we were exactly at the cap.
    expect(listSessionArtifacts()).toHaveLength(500);
  });
});
