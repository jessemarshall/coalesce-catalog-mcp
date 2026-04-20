import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { makeServer } from "../helpers/make-server.js";
import { writeJsonArtifact } from "../../src/cache/store.js";
import { buildCacheUri } from "../../src/cache/paths.js";

describe("catalog://cache/* resource", () => {
  let dir: string;
  let originalEnv: string | undefined;

  beforeEach(() => {
    originalEnv = process.env.COALESCE_CACHE_DIR;
    dir = mkdtempSync(join(tmpdir(), "catalog-cache-resource-"));
    process.env.COALESCE_CACHE_DIR = dir;
  });

  afterEach(() => {
    if (originalEnv === undefined) delete process.env.COALESCE_CACHE_DIR;
    else process.env.COALESCE_CACHE_DIR = originalEnv;
    rmSync(dir, { recursive: true, force: true });
  });

  it("registers the cache resource template alongside static context resources", () => {
    const server = makeServer();
    const registered = Object.keys(
      (server as unknown as { _registeredResourceTemplates: Record<string, unknown> })
        ._registeredResourceTemplates ?? {}
    );
    expect(registered).toContain("Catalog Cache Artifact");
  });

  it("lists artifacts written to the session cache", async () => {
    const server = makeServer();
    writeJsonArtifact("demo/a.json", { a: 1 });

    const handle = (
      server as unknown as {
        _registeredResourceTemplates: Record<
          string,
          { resourceTemplate: { listCallback: () => Promise<{ resources: unknown[] }> } }
        >;
      }
    )._registeredResourceTemplates["Catalog Cache Artifact"];
    const listed = await handle.resourceTemplate.listCallback();
    expect(listed.resources).toHaveLength(1);
    expect((listed.resources[0] as { uri: string }).uri).toBe(
      buildCacheUri("demo/a.json")
    );
  });

  it("reading a known artifact returns its JSON contents", async () => {
    const server = makeServer();
    const artifact = writeJsonArtifact("demo/b.json", { b: 2 });
    const uri = buildCacheUri(artifact.relPath);

    const handle = (
      server as unknown as {
        _registeredResourceTemplates: Record<
          string,
          {
            readCallback: (
              uri: URL,
              vars: Record<string, string>
            ) => Promise<{ contents: { text: string }[] }>;
          }
        >;
      }
    )._registeredResourceTemplates["Catalog Cache Artifact"];

    const result = await handle.readCallback(new URL(uri), {});
    expect(JSON.parse(result.contents[0].text)).toEqual({ b: 2 });
  });

  it("reading an unknown artifact throws", async () => {
    const server = makeServer();
    const badUri = buildCacheUri("does/not/exist.json");
    const handle = (
      server as unknown as {
        _registeredResourceTemplates: Record<
          string,
          {
            readCallback: (
              uri: URL,
              vars: Record<string, string>
            ) => Promise<unknown>;
          }
        >;
      }
    )._registeredResourceTemplates["Catalog Cache Artifact"];
    await expect(handle.readCallback(new URL(badUri), {})).rejects.toThrow();
  });
});
