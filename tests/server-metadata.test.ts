import { describe, it, expect } from "vitest";
import { makeServer } from "./helpers/make-server.js";
import { SERVER_NAME, SERVER_VERSION } from "../src/constants.js";

describe("server metadata", () => {
  it("registers with the canonical server name", () => {
    expect(SERVER_NAME).toBe("coalesce-catalog");
  });

  it("has a semver-shaped version", () => {
    expect(SERVER_VERSION).toMatch(/^\d+\.\d+\.\d+(-[\w.]+)?$/);
  });

  it("builds an McpServer without throwing (any valid config)", () => {
    const server = makeServer();
    expect(server).toBeDefined();
    expect(typeof server.connect).toBe("function");
  });
});
