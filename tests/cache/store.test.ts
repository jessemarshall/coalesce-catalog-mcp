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
});
