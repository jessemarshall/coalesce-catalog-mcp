import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  EXTERNALIZE_RESPONSE_THRESHOLD,
  externalizeIfLarge,
  isExternalizedPointer,
} from "../../src/cache/externalize.js";
import { resolveCacheUri } from "../../src/cache/paths.js";
import { readArtifact } from "../../src/cache/store.js";

describe("externalizeIfLarge", () => {
  let dir: string;
  let originalEnv: string | undefined;

  beforeEach(() => {
    originalEnv = process.env.COALESCE_CACHE_DIR;
    dir = mkdtempSync(join(tmpdir(), "catalog-externalize-"));
    process.env.COALESCE_CACHE_DIR = dir;
  });

  afterEach(() => {
    if (originalEnv === undefined) delete process.env.COALESCE_CACHE_DIR;
    else process.env.COALESCE_CACHE_DIR = originalEnv;
    rmSync(dir, { recursive: true, force: true });
  });

  it("returns the payload unchanged when under threshold", () => {
    const payload = { a: 1, b: "two" };
    const result = externalizeIfLarge(payload, {
      toolName: "demo",
      threshold: 1000,
    });
    expect(result).toBe(payload);
    expect(isExternalizedPointer(result)).toBe(false);
  });

  it("writes to disk and returns a pointer when over threshold", () => {
    const big = { data: "x".repeat(5000) };
    const result = externalizeIfLarge(big, {
      toolName: "demo",
      section: "columns",
      threshold: 2048,
    });
    expect(isExternalizedPointer(result)).toBe(true);
    if (!isExternalizedPointer(result)) throw new Error("unreachable");

    expect(result.resourceUri.startsWith("catalog://cache/")).toBe(true);
    expect(result.byteSize).toBeGreaterThan(2048);

    const resolved = resolveCacheUri(result.resourceUri);
    expect(resolved).not.toBeNull();
    expect(existsSync(resolved!.absPath)).toBe(true);
    expect(JSON.parse(readArtifact(resolved!.absPath))).toEqual(big);
  });

  it("embeds the toolName and section in the on-disk path", () => {
    const big = { data: "x".repeat(3000) };
    const result = externalizeIfLarge(big, {
      toolName: "catalog_summarize_asset",
      section: "quality",
      threshold: 1024,
    });
    if (!isExternalizedPointer(result)) throw new Error("expected pointer");
    const resolved = resolveCacheUri(result.resourceUri);
    expect(resolved!.relPath.startsWith("catalog_summarize_asset/quality/")).toBe(
      true
    );
  });

  it("threshold constants are the values we documented", () => {
    expect(EXTERNALIZE_RESPONSE_THRESHOLD).toBe(16384);
  });
});
