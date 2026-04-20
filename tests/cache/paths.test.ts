import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  buildCacheUri,
  buildRelPath,
  encodeCacheKey,
  getCacheBaseDir,
  getCacheRoot,
  getSessionDir,
  resolveCacheUri,
} from "../../src/cache/paths.js";

describe("cache paths", () => {
  let dir: string;
  let originalEnv: string | undefined;

  beforeEach(() => {
    originalEnv = process.env.COALESCE_CACHE_DIR;
    dir = mkdtempSync(join(tmpdir(), "catalog-cache-paths-"));
    process.env.COALESCE_CACHE_DIR = dir;
  });

  afterEach(() => {
    if (originalEnv === undefined) delete process.env.COALESCE_CACHE_DIR;
    else process.env.COALESCE_CACHE_DIR = originalEnv;
    rmSync(dir, { recursive: true, force: true });
  });

  it("honours COALESCE_CACHE_DIR", () => {
    expect(getCacheBaseDir()).toBe(dir);
    expect(getCacheRoot()).toBe(join(dir, "coalesce_catalog_mcp_cache"));
  });

  it("getSessionDir returns a process-scoped subdir of the cache root", () => {
    const session = getSessionDir();
    expect(session.startsWith(getCacheRoot())).toBe(true);
    expect(session).toMatch(/session-\d+-\d+$/);
  });

  it("buildRelPath sanitises bad segments and includes a uuid filename", () => {
    const rel = buildRelPath("catalog_summarize_asset", "columns/../etc", "abc");
    expect(rel).toBe("catalog_summarize_asset/columns_.._etc/abc.json");
  });

  it("encodeCacheKey is URL-safe base64 of the relative path", () => {
    const key = encodeCacheKey("summarize_asset/abc.json");
    expect(key).not.toContain("/");
    expect(key).not.toContain("+");
    expect(key).not.toContain("=");
    expect(Buffer.from(key, "base64url").toString("utf8")).toBe(
      "summarize_asset/abc.json"
    );
  });

  it("buildCacheUri round-trips through resolveCacheUri", () => {
    const rel = "summarize_asset/fake-uuid.json";
    const uri = buildCacheUri(rel);
    expect(uri.startsWith("catalog://cache/")).toBe(true);
    const resolved = resolveCacheUri(uri);
    expect(resolved).not.toBeNull();
    expect(resolved!.relPath).toBe(rel);
    expect(resolved!.absPath.startsWith(getSessionDir())).toBe(true);
  });

  it("resolveCacheUri rejects path-traversal attempts", () => {
    const evil = `catalog://cache/${Buffer.from("../../etc/passwd", "utf8").toString(
      "base64url"
    )}`;
    expect(resolveCacheUri(evil)).toBeNull();
  });

  it("resolveCacheUri rejects absolute paths", () => {
    const evil = `catalog://cache/${Buffer.from("/etc/passwd", "utf8").toString(
      "base64url"
    )}`;
    expect(resolveCacheUri(evil)).toBeNull();
  });

  it("resolveCacheUri rejects empty and slash-bearing keys", () => {
    expect(resolveCacheUri("catalog://cache/")).toBeNull();
    expect(resolveCacheUri("catalog://cache/a/b")).toBeNull();
  });

  it("resolveCacheUri rejects foreign URI schemes", () => {
    expect(resolveCacheUri("coalesce://cache/xxx")).toBeNull();
    expect(resolveCacheUri("file:///tmp/x")).toBeNull();
  });
});
