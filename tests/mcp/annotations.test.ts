import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineAnnotationTools } from "../../src/mcp/annotations.js";
import {
  GET_TERMS_SUMMARY,
  GET_TERMS_DETAIL_BATCH,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const client = makeMockClient(responder ?? (() => ({})));
  return { client, tools: defineAnnotationTools(client) };
}

function find(
  tools: ReturnType<typeof defineAnnotationTools>,
  name: string
) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }): unknown {
  return JSON.parse(r.content[0].text);
}

const termSummaryFixture = {
  id: "term-1",
  name: "Active User",
  description: "A user with a login in the last 30 days.",
  externalId: null,
  icon: null,
  parentTermId: null,
  depthLevel: 0,
  isVerified: true,
  isDeprecated: false,
  isDescriptionGenerated: false,
  slug: "active-user",
  createdAt: "2025-01-01T00:00:00Z",
  updatedAt: "2025-06-01T00:00:00Z",
  lastEditedAt: "2025-06-01T00:00:00Z",
  deletedAt: null,
  deprecatedAt: null,
  linkedTag: { id: "tag-1", label: "active-user", color: "BLUE" },
};

const termDetailFixture = {
  ...termSummaryFixture,
  ownerEntities: [
    {
      id: "oe-1",
      userId: "u-1",
      user: { id: "u-1", email: "ada@example.com", fullName: "Ada Lovelace" },
    },
  ],
  teamOwnerEntities: [
    {
      id: "toe-1",
      teamId: "t-1",
      team: { id: "t-1", name: "Analytics", email: "analytics@example.com" },
    },
  ],
  tagEntities: [
    { id: "te-1", tag: { id: "tag-domain", label: "domain:product", color: "GREEN" } },
  ],
};

// ---------------------------------------------------------------------------
// Inventory
// ---------------------------------------------------------------------------

describe("catalog_search_terms inventory", () => {
  const { tools } = makeTools();

  it("registers catalog_search_terms", () => {
    expect(tools.map((t) => t.name)).toContain("catalog_search_terms");
  });

  it("is a read-only tool", () => {
    expect(find(tools, "catalog_search_terms").config.annotations?.readOnlyHint).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// Input schema
// ---------------------------------------------------------------------------

describe("catalog_search_terms input schema", () => {
  const schema = z.object(
    find(makeTools().tools, "catalog_search_terms").config.inputSchema
  );

  it("accepts no arguments (projection defaults to summary)", () => {
    expect(() => schema.parse({})).not.toThrow();
  });

  it("accepts nameContains + ids + sorting + pagination", () => {
    expect(() =>
      schema.parse({
        nameContains: "user",
        ids: ["term-1", "term-2"],
        sortBy: "name",
        sortDirection: "ASC",
        nullsPriority: "LAST",
        nbPerPage: 50,
        page: 1,
      })
    ).not.toThrow();
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

  it("rejects invalid sortBy", () => {
    expect(() => schema.parse({ sortBy: "popularity" })).toThrow();
  });

  it("rejects nbPerPage > 500", () => {
    expect(() => schema.parse({ nbPerPage: 501 })).toThrow();
  });
});

// ---------------------------------------------------------------------------
// Description
// ---------------------------------------------------------------------------

describe("catalog_search_terms description", () => {
  const def = find(makeTools().tools, "catalog_search_terms");

  it("mentions the default summary fields", () => {
    expect(def.config.description).toMatch(/linkedTag/);
    expect(def.config.description).toMatch(/verification/);
  });

  it("mentions the detailed projection opt-in", () => {
    expect(def.config.description).toMatch(/projection: "detailed"/);
    expect(def.config.description).toMatch(/ownerEntities/);
    expect(def.config.description).toMatch(/tagEntities/);
  });
});

// ---------------------------------------------------------------------------
// Handler — operation selection
// ---------------------------------------------------------------------------

describe("catalog_search_terms handler — operation selection", () => {
  it("defaults to the summary operation when projection is omitted", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({});
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_TERMS_SUMMARY);
  });

  it("uses the summary operation when projection is 'summary'", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({ projection: "summary" });
    expect(client.calls[0].document).toBe(GET_TERMS_SUMMARY);
  });

  it("uses the detail-batch operation when projection is 'detailed'", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({ projection: "detailed" });
    expect(client.calls[0].document).toBe(GET_TERMS_DETAIL_BATCH);
  });
});

// ---------------------------------------------------------------------------
// Handler — scope + sorting + pagination wiring
// ---------------------------------------------------------------------------

describe("catalog_search_terms handler — variable wiring", () => {
  it("builds scope from nameContains + ids", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({ nameContains: "user", ids: ["term-1"] });
    const vars = client.calls[0].variables as { scope?: Record<string, unknown> };
    expect(vars.scope).toEqual({ nameContains: "user", ids: ["term-1"] });
  });

  it("omits scope when no filters are provided", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({});
    const vars = client.calls[0].variables as { scope?: unknown };
    expect(vars.scope).toBeUndefined();
  });

  it("builds sorting when sortBy is provided", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({
      sortBy: "name",
      sortDirection: "ASC",
      nullsPriority: "LAST",
    });
    const vars = client.calls[0].variables as {
      sorting?: Array<Record<string, unknown>>;
    };
    expect(vars.sorting).toEqual([
      { sortingKey: "name", direction: "ASC", nullsPriority: "LAST" },
    ]);
  });

  it("omits sorting when sortBy is absent", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({});
    const vars = client.calls[0].variables as { sorting?: unknown };
    expect(vars.sorting).toBeUndefined();
  });

  it("forwards pagination to the GraphQL variables", async () => {
    const { client, tools } = makeTools(() => ({
      getTerms: { page: 2, nbPerPage: 25, totalCount: 200, data: [] },
    }));
    const tool = find(tools, "catalog_search_terms");
    await tool.handler({ nbPerPage: 25, page: 2 });
    const vars = client.calls[0].variables as {
      pagination: Record<string, unknown>;
    };
    expect(vars.pagination).toMatchObject({ nbPerPage: 25, page: 2 });
  });
});

// ---------------------------------------------------------------------------
// Handler — response shape per projection
// ---------------------------------------------------------------------------

describe("catalog_search_terms handler — response shape", () => {
  it("returns listEnvelope with summary rows by default", async () => {
    const { tools } = makeTools(() => ({
      getTerms: {
        page: 0,
        nbPerPage: 25,
        totalCount: 1,
        data: [termSummaryFixture],
      },
    }));
    const tool = find(tools, "catalog_search_terms");
    const res = await tool.handler({ nbPerPage: 25 });
    expect(res.isError).toBeUndefined();
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed).toMatchObject({
      pagination: { page: 0, nbPerPage: 25, totalCount: 1, hasMore: false },
    });
    const data = parsed.data as Array<Record<string, unknown>>;
    expect(data).toHaveLength(1);
    expect(data[0].id).toBe("term-1");
    expect(data[0].ownerEntities).toBeUndefined();
    expect(data[0].tagEntities).toBeUndefined();
  });

  it("returns ownerEntities + teamOwnerEntities + tagEntities when detailed", async () => {
    const { tools } = makeTools(() => ({
      getTerms: {
        page: 0,
        nbPerPage: 25,
        totalCount: 1,
        data: [termDetailFixture],
      },
    }));
    const tool = find(tools, "catalog_search_terms");
    const res = await tool.handler({ projection: "detailed" });
    const parsed = parseResult(res) as { data: Array<Record<string, unknown>> };
    expect(parsed.data).toHaveLength(1);
    const row = parsed.data[0];
    expect(row.ownerEntities).toEqual(termDetailFixture.ownerEntities);
    expect(row.teamOwnerEntities).toEqual(termDetailFixture.teamOwnerEntities);
    expect(row.tagEntities).toEqual(termDetailFixture.tagEntities);
  });

  it("surfaces transport errors as isError results", async () => {
    const { tools } = makeTools(() => {
      throw new Error("graphql failure");
    });
    const tool = find(tools, "catalog_search_terms");
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/graphql failure/);
  });
});
