import { describe, it, expect } from "vitest";
import { z } from "zod";
import { createClient } from "../../src/client.js";
import { defineGovernanceTools } from "../../src/mcp/governance.js";

const client = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

const tools = defineGovernanceTools(client);

function find(name: string) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

describe("governance tool inventory", () => {
  it("registers the three dedicated lookup tools", () => {
    const names = tools.map((t) => t.name);
    expect(names).toContain("catalog_get_user_owned_assets");
    expect(names).toContain("catalog_get_team_members");
    expect(names).toContain("catalog_get_team_owned_assets");
  });

  it("dedicated lookup tools are read-only", () => {
    for (const name of [
      "catalog_get_user_owned_assets",
      "catalog_get_team_members",
      "catalog_get_team_owned_assets",
    ]) {
      expect(find(name).config.annotations?.readOnlyHint).toBe(true);
    }
  });
});

describe("catalog_get_user_owned_assets input schema", () => {
  const schema = z.object(find("catalog_get_user_owned_assets").config.inputSchema);

  it("requires userId", () => {
    expect(() => schema.parse({})).toThrow();
  });

  it("rejects empty userId", () => {
    expect(() => schema.parse({ userId: "" })).toThrow();
  });

  it("accepts userId + pagination", () => {
    expect(() =>
      schema.parse({ userId: "u-123", nbPerPage: 50, page: 0 })
    ).not.toThrow();
  });

  it("honours max page size from PaginationInputShape", () => {
    expect(() =>
      schema.parse({ userId: "u-123", nbPerPage: 501 })
    ).toThrow();
  });
});

describe("catalog_get_team_members / catalog_get_team_owned_assets", () => {
  for (const name of [
    "catalog_get_team_members",
    "catalog_get_team_owned_assets",
  ]) {
    const schema = z.object(find(name).config.inputSchema);

    it(`${name} requires teamId`, () => {
      expect(() => schema.parse({})).toThrow();
    });

    it(`${name} accepts teamId + pagination`, () => {
      expect(() =>
        schema.parse({ teamId: "t-123", nbPerPage: 25 })
      ).not.toThrow();
    });
  }
});

describe("catalog_search_users description + description-free output", () => {
  const def = find("catalog_search_users");
  it("description references ownedAssetCount (not ownedAssetIds)", () => {
    expect(def.config.description).toMatch(/ownedAssetCount/);
    expect(def.config.description).not.toMatch(/ownedAssetIds (array|list|UUIDs)/);
  });
});

describe("catalog_search_teams description", () => {
  const def = find("catalog_search_teams");
  it("description references memberCount + ownedAssetCount", () => {
    expect(def.config.description).toMatch(/memberCount/);
    expect(def.config.description).toMatch(/ownedAssetCount/);
  });
});
