import { describe, it, expect } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { makeServer } from "./helpers/make-server.js";
import { SERVER_NAME, SERVER_VERSION } from "../src/constants.js";

describe("server metadata", () => {
  it("registers with the canonical server name", () => {
    expect(SERVER_NAME).toBe("coalesce-catalog");
  });

  it("has a semver-shaped version", () => {
    expect(SERVER_VERSION).toMatch(/^\d+\.\d+\.\d+(-[\w.]+)?$/);
  });

  it("matches the published package.json version", () => {
    // Guards against the hard-coded drift we had at 0.1.0 → 0.2.0.
    const pkg = JSON.parse(
      readFileSync(
        fileURLToPath(new URL("../package.json", import.meta.url)),
        "utf8"
      )
    ) as { version: string };
    expect(SERVER_VERSION).toBe(pkg.version);
  });

  it("builds an McpServer without throwing (any valid config)", () => {
    const server = makeServer();
    expect(server).toBeDefined();
    expect(typeof server.connect).toBe("function");
  });
});
