import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { withResponseExternalization } from "../../src/mcp/tool-helpers.js";
import type { CatalogToolDefinition, ToolResult } from "../../src/catalog/types.js";
import { textResult, errorResult } from "../../src/catalog/types.js";
import { EXTERNALIZE_RESPONSE_THRESHOLD } from "../../src/cache/externalize.js";
import { resolveCacheUri } from "../../src/cache/paths.js";
import { readArtifact } from "../../src/cache/store.js";

function handlerReturning(result: ToolResult): CatalogToolDefinition["handler"] {
  return async () => result;
}

describe("withResponseExternalization", () => {
  let dir: string;
  let originalEnv: string | undefined;

  beforeEach(() => {
    originalEnv = process.env.COALESCE_CACHE_DIR;
    dir = mkdtempSync(join(tmpdir(), "catalog-resp-ext-"));
    process.env.COALESCE_CACHE_DIR = dir;
  });

  afterEach(() => {
    if (originalEnv === undefined) delete process.env.COALESCE_CACHE_DIR;
    else process.env.COALESCE_CACHE_DIR = originalEnv;
    rmSync(dir, { recursive: true, force: true });
  });

  it("passes small responses through unchanged", async () => {
    const small = textResult({ a: 1, b: "two" });
    const wrapped = withResponseExternalization(handlerReturning(small), {
      toolName: "demo",
    });
    const result = await wrapped({}, undefined);
    expect(result).toBe(small);
  });

  it("externalizes responses over the threshold into a catalog://cache URI", async () => {
    const payload = { rows: new Array(5000).fill({ name: "wide-row-data" }) };
    const big = textResult(payload);
    expect(Buffer.byteLength(big.content[0].text, "utf8")).toBeGreaterThan(
      EXTERNALIZE_RESPONSE_THRESHOLD
    );
    const wrapped = withResponseExternalization(handlerReturning(big), {
      toolName: "demo_tool",
    });
    const result = await wrapped({}, undefined);

    const parsed = JSON.parse(result.content[0].text);
    expect(parsed.externalized).toBe(true);
    expect(parsed.resourceUri.startsWith("catalog://cache/")).toBe(true);
    expect(parsed.note).toContain("ReadResource");

    const resolved = resolveCacheUri(parsed.resourceUri);
    expect(resolved).not.toBeNull();
    expect(JSON.parse(readArtifact(resolved!.absPath))).toEqual(payload);
  });

  it("leaves error results inline even when oversized", async () => {
    const big = errorResult(
      "boom",
      new Array(2000).fill({ detail: "lots of context about the failure" })
    );
    expect(Buffer.byteLength(big.content[0].text, "utf8")).toBeGreaterThan(
      EXTERNALIZE_RESPONSE_THRESHOLD
    );
    const wrapped = withResponseExternalization(handlerReturning(big), {
      toolName: "demo_tool",
    });
    const result = await wrapped({}, undefined);
    expect(result).toBe(big);
    expect(result.isError).toBe(true);
  });

  it("neverExternalize:true is a fast-path that returns the original handler", async () => {
    const big = textResult({ rows: new Array(5000).fill({ name: "row" }) });
    const handler = handlerReturning(big);
    const wrapped = withResponseExternalization(handler, {
      toolName: "health_check",
      neverExternalize: true,
    });
    expect(wrapped).toBe(handler);
  });

  it("leaves non-JSON text results alone (e.g. future plain-text tools)", async () => {
    const plain: ToolResult = {
      content: [{ type: "text", text: "x".repeat(EXTERNALIZE_RESPONSE_THRESHOLD + 100) }],
    };
    const wrapped = withResponseExternalization(handlerReturning(plain), {
      toolName: "demo",
    });
    const result = await wrapped({}, undefined);
    expect(result).toBe(plain);
  });
});
