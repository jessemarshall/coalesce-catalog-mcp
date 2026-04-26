import { describe, it, expect } from "vitest";
import { createClient } from "../src/client.js";
import { buildAllToolDefinitions } from "../src/server.js";

const client = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

function allDefinitions() {
  // Single source of truth: assert against the same list the server registers.
  // If a new tool is added to server.ts, the count assertions below catch it
  // immediately rather than passing silently against a stale duplicate list.
  return buildAllToolDefinitions(client);
}

describe("tool registration", () => {
  it("registers 67 tools across all domains", () => {
    expect(allDefinitions()).toHaveLength(67);
  });

  it("every tool name starts with 'catalog_'", () => {
    for (const def of allDefinitions()) {
      expect(def.name).toMatch(/^catalog_/);
    }
  });

  it("tool names are unique", () => {
    const names = allDefinitions().map((d) => d.name);
    const unique = new Set(names);
    expect(unique.size).toBe(names.length);
  });

  it("every tool has a title", () => {
    for (const def of allDefinitions()) {
      expect(def.config.title).toBeTruthy();
      expect(typeof def.config.title).toBe("string");
    }
  });

  it("every tool has a non-empty description", () => {
    for (const def of allDefinitions()) {
      expect(def.config.description.length).toBeGreaterThan(20);
    }
  });

  it("every tool declares annotations", () => {
    for (const def of allDefinitions()) {
      expect(def.config.annotations).toBeDefined();
      expect(
        typeof def.config.annotations?.readOnlyHint === "boolean"
      ).toBe(true);
    }
  });

  it("splits roughly 42 read / 25 write", () => {
    const reads = allDefinitions().filter(
      (d) => d.config.annotations?.readOnlyHint === true
    );
    const writes = allDefinitions().filter(
      (d) => d.config.annotations?.readOnlyHint !== true
    );
    expect(reads).toHaveLength(42);
    expect(writes).toHaveLength(25);
  });

  it("destructive writes have destructiveHint=true", () => {
    const destructive = allDefinitions().filter(
      (d) => d.config.annotations?.destructiveHint === true
    );
    // detach_tags, delete_lineages, delete_external_links,
    // delete_term, remove_data_qualities, remove_user_owners,
    // remove_team_owners, remove_team_users, remove_pinned_assets
    expect(destructive.length).toBeGreaterThanOrEqual(9);
    for (const def of destructive) {
      expect(def.config.annotations?.readOnlyHint).not.toBe(true);
    }
  });
});
