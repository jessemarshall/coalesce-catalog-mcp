import { describe, it, expect } from "vitest";
import { z } from "zod";
import { createClient } from "../../src/client.js";
import { defineGovernanceTools } from "../../src/mcp/governance.js";
import { GET_USERS, GET_TEAMS } from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

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

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const mock = makeMockClient(responder ?? (() => ({})));
  return { client: mock, tools: defineGovernanceTools(mock) };
}

function findIn(toolSet: ReturnType<typeof defineGovernanceTools>, name: string) {
  const match = toolSet.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }): unknown {
  return JSON.parse(r.content[0].text);
}

const userFixture = {
  id: "u-1",
  firstName: "Ada",
  lastName: "Lovelace",
  email: "ada@example.com",
  role: "ADMIN",
  status: "ACTIVE",
  isEmailValidated: true,
  createdAt: "2025-01-01T00:00:00Z",
  ownedAssetIds: ["a-1", "a-2", "a-3"],
};

const teamFixture = {
  id: "t-1",
  name: "Analytics",
  description: "Analytics team",
  email: "analytics@example.com",
  slackChannel: "#analytics",
  slackGroup: "@analytics",
  memberIds: ["u-1", "u-2"],
  ownedAssetIds: ["a-1", "a-2", "a-3", "a-4"],
  createdAt: "2025-01-01T00:00:00Z",
};

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

describe("catalog_search_users input schema", () => {
  const schema = z.object(find("catalog_search_users").config.inputSchema);

  it("accepts no arguments (projection defaults to summary)", () => {
    expect(() => schema.parse({})).not.toThrow();
  });

  it("accepts projection='summary'", () => {
    expect(() => schema.parse({ projection: "summary" })).not.toThrow();
  });

  it("accepts projection='detailed'", () => {
    expect(() => schema.parse({ projection: "detailed" })).not.toThrow();
  });

  it("rejects unknown projection values", () => {
    expect(() => schema.parse({ projection: "full" })).toThrow();
  });
});

describe("catalog_search_teams input schema", () => {
  const schema = z.object(find("catalog_search_teams").config.inputSchema);

  it("accepts no arguments (projection defaults to summary)", () => {
    expect(() => schema.parse({})).not.toThrow();
  });

  it("accepts projection='summary'", () => {
    expect(() => schema.parse({ projection: "summary" })).not.toThrow();
  });

  it("accepts projection='detailed'", () => {
    expect(() => schema.parse({ projection: "detailed" })).not.toThrow();
  });

  it("rejects unknown projection values", () => {
    expect(() => schema.parse({ projection: "full" })).toThrow();
  });
});

describe("catalog_search_users description", () => {
  const def = find("catalog_search_users");
  it("description references ownedAssetCount (not ownedAssetIds)", () => {
    expect(def.config.description).toMatch(/ownedAssetCount/);
    expect(def.config.description).not.toMatch(/ownedAssetIds (array|list|UUIDs)/);
  });

  it("description mentions the detailed projection opt-in", () => {
    expect(def.config.description).toMatch(/projection: "detailed"/);
    expect(def.config.description).toMatch(/ownedAssetIds/);
  });
});

describe("catalog_search_teams description", () => {
  const def = find("catalog_search_teams");
  it("description references memberCount + ownedAssetCount", () => {
    expect(def.config.description).toMatch(/memberCount/);
    expect(def.config.description).toMatch(/ownedAssetCount/);
  });

  it("description mentions the detailed projection opt-in", () => {
    expect(def.config.description).toMatch(/projection: "detailed"/);
    expect(def.config.description).toMatch(/memberIds/);
    expect(def.config.description).toMatch(/ownedAssetIds/);
  });
});

describe("catalog_get_user_owned_assets description", () => {
  const def = find("catalog_get_user_owned_assets");
  it("points callers with an email at projection:detailed", () => {
    expect(def.config.description).toMatch(/projection: "detailed"/);
  });
});

describe("catalog_get_team_members description", () => {
  const def = find("catalog_get_team_members");
  it("points callers with a name at projection:detailed", () => {
    expect(def.config.description).toMatch(/projection: "detailed"/);
  });
});

describe("catalog_get_team_owned_assets description", () => {
  const def = find("catalog_get_team_owned_assets");
  it("points callers with a name at projection:detailed", () => {
    expect(def.config.description).toMatch(/projection: "detailed"/);
  });
});

// ---------------------------------------------------------------------------
// catalog_search_users handler — projection behavior
// ---------------------------------------------------------------------------

describe("catalog_search_users handler", () => {
  it("calls GET_USERS with pagination", async () => {
    const { client: mock, tools: toolSet } = makeTools(() => ({
      getUsers: [userFixture],
    }));
    const tool = findIn(toolSet, "catalog_search_users");
    await tool.handler({ nbPerPage: 50, page: 2 });
    expect(mock.calls).toHaveLength(1);
    expect(mock.calls[0].document).toBe(GET_USERS);
    const vars = mock.calls[0].variables as { pagination: Record<string, unknown> };
    expect(vars.pagination).toMatchObject({ nbPerPage: 50, page: 2 });
  });

  it("summary projection strips ownedAssetIds and surfaces ownedAssetCount", async () => {
    const { tools: toolSet } = makeTools(() => ({ getUsers: [userFixture] }));
    const tool = findIn(toolSet, "catalog_search_users");
    const res = await tool.handler({});
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data).toHaveLength(1);
    const row = parsed.data[0];
    expect(row.ownedAssetIds).toBeUndefined();
    expect(row.ownedAssetCount).toBe(3);
    expect(row.email).toBe("ada@example.com");
  });

  it("explicit summary projection strips ownedAssetIds", async () => {
    const { tools: toolSet } = makeTools(() => ({ getUsers: [userFixture] }));
    const tool = findIn(toolSet, "catalog_search_users");
    const res = await tool.handler({ projection: "summary" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data[0].ownedAssetIds).toBeUndefined();
    expect(parsed.data[0].ownedAssetCount).toBe(3);
  });

  it("detailed projection inlines ownedAssetIds alongside the count", async () => {
    const { tools: toolSet } = makeTools(() => ({ getUsers: [userFixture] }));
    const tool = findIn(toolSet, "catalog_search_users");
    const res = await tool.handler({ projection: "detailed" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data).toHaveLength(1);
    const row = parsed.data[0];
    expect(row.ownedAssetIds).toEqual(["a-1", "a-2", "a-3"]);
    expect(row.ownedAssetCount).toBe(3);
    expect(row.email).toBe("ada@example.com");
  });

  it("detailed projection preserves other identity fields", async () => {
    const { tools: toolSet } = makeTools(() => ({ getUsers: [userFixture] }));
    const tool = findIn(toolSet, "catalog_search_users");
    const res = await tool.handler({ projection: "detailed" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data[0]).toMatchObject({
      id: "u-1",
      firstName: "Ada",
      lastName: "Lovelace",
      email: "ada@example.com",
      role: "ADMIN",
      isEmailValidated: true,
    });
  });

  it("handles an empty ownedAssetIds array in detailed mode", async () => {
    const empty = { ...userFixture, ownedAssetIds: [] };
    const { tools: toolSet } = makeTools(() => ({ getUsers: [empty] }));
    const tool = findIn(toolSet, "catalog_search_users");
    const res = await tool.handler({ projection: "detailed" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data[0].ownedAssetIds).toEqual([]);
    expect(parsed.data[0].ownedAssetCount).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// catalog_search_teams handler — projection behavior
// ---------------------------------------------------------------------------

describe("catalog_search_teams handler", () => {
  it("calls GET_TEAMS with pagination", async () => {
    const { client: mock, tools: toolSet } = makeTools(() => ({
      getTeams: [teamFixture],
    }));
    const tool = findIn(toolSet, "catalog_search_teams");
    await tool.handler({ nbPerPage: 10, page: 1 });
    expect(mock.calls[0].document).toBe(GET_TEAMS);
    const vars = mock.calls[0].variables as { pagination: Record<string, unknown> };
    expect(vars.pagination).toMatchObject({ nbPerPage: 10, page: 1 });
  });

  it("summary projection strips both memberIds and ownedAssetIds", async () => {
    const { tools: toolSet } = makeTools(() => ({ getTeams: [teamFixture] }));
    const tool = findIn(toolSet, "catalog_search_teams");
    const res = await tool.handler({});
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    const row = parsed.data[0];
    expect(row.memberIds).toBeUndefined();
    expect(row.ownedAssetIds).toBeUndefined();
    expect(row.memberCount).toBe(2);
    expect(row.ownedAssetCount).toBe(4);
  });

  it("detailed projection inlines memberIds and ownedAssetIds alongside counts", async () => {
    const { tools: toolSet } = makeTools(() => ({ getTeams: [teamFixture] }));
    const tool = findIn(toolSet, "catalog_search_teams");
    const res = await tool.handler({ projection: "detailed" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    const row = parsed.data[0];
    expect(row.memberIds).toEqual(["u-1", "u-2"]);
    expect(row.ownedAssetIds).toEqual(["a-1", "a-2", "a-3", "a-4"]);
    expect(row.memberCount).toBe(2);
    expect(row.ownedAssetCount).toBe(4);
  });

  it("detailed projection preserves slack routing and identity", async () => {
    const { tools: toolSet } = makeTools(() => ({ getTeams: [teamFixture] }));
    const tool = findIn(toolSet, "catalog_search_teams");
    const res = await tool.handler({ projection: "detailed" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data[0]).toMatchObject({
      id: "t-1",
      name: "Analytics",
      slackChannel: "#analytics",
      slackGroup: "@analytics",
    });
  });

  it("handles empty memberIds and ownedAssetIds arrays in detailed mode", async () => {
    const empty = { ...teamFixture, memberIds: [], ownedAssetIds: [] };
    const { tools: toolSet } = makeTools(() => ({ getTeams: [empty] }));
    const tool = findIn(toolSet, "catalog_search_teams");
    const res = await tool.handler({ projection: "detailed" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data[0].memberIds).toEqual([]);
    expect(parsed.data[0].ownedAssetIds).toEqual([]);
    expect(parsed.data[0].memberCount).toBe(0);
    expect(parsed.data[0].ownedAssetCount).toBe(0);
  });
});

describe("catalog_update_external_links input schema", () => {
  const tool = find("catalog_update_external_links");
  const schema = z.object(
    tool.config.inputSchema as Record<string, z.ZodTypeAny>
  );

  it("accepts rows with id + url", () => {
    expect(() =>
      schema.parse({
        data: [{ id: "link-1", url: "https://runbook.example.com/orders" }],
      })
    ).not.toThrow();
  });

  it("rejects rows missing the url field (no-op update)", () => {
    expect(() => schema.parse({ data: [{ id: "link-1" }] })).toThrow();
  });

  it("rejects rows with a non-URL url string", () => {
    expect(() =>
      schema.parse({ data: [{ id: "link-1", url: "not-a-url" }] })
    ).toThrow();
  });
});
