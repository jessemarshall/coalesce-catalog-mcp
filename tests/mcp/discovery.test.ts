import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineDiscoveryTools } from "../../src/mcp/discovery.js";
import {
  GET_SOURCES,
  GET_DATABASES,
  GET_SCHEMAS,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const client = makeMockClient(responder ?? (() => ({})));
  const tools = defineDiscoveryTools(client);
  return { client, tools };
}

function find(tools: ReturnType<typeof defineDiscoveryTools>, name: string) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }): unknown {
  return JSON.parse(r.content[0].text);
}

// ── catalog_search_sources ──────────────────────────────────────────────────

describe("catalog_search_sources", () => {
  const emptyPage = {
    getSources: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("calls GET_SOURCES with no scope when no filters provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_sources");
    await tool.handler({});
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_SOURCES);
    expect(client.calls[0].variables).toMatchObject({
      scope: undefined,
      pagination: { nbPerPage: 100, page: 0 },
    });
  });

  it("builds scope from nameContains filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_sources");
    await tool.handler({ nameContains: "snow" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { nameContains: "snow" },
    });
  });

  it("builds scope from origin filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_sources");
    await tool.handler({ origin: "API" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { origin: "API" },
    });
  });

  it("builds scope from technology filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_sources");
    await tool.handler({ technology: "SNOWFLAKE" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { technology: "SNOWFLAKE" },
    });
  });

  it("builds scope from type filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_sources");
    await tool.handler({ type: "WAREHOUSE" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { type: "WAREHOUSE" },
    });
  });

  it("builds scope with multiple filters combined", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_sources");
    await tool.handler({
      nameContains: "prod",
      origin: "EXTRACTION",
      type: "VISUALIZATION",
      withDeleted: true,
    });
    expect(client.calls[0].variables).toMatchObject({
      scope: {
        nameContains: "prod",
        origin: "EXTRACTION",
        type: "VISUALIZATION",
        withDeleted: true,
      },
    });
  });

  it("returns a listEnvelope with pagination and data", async () => {
    const { tools } = makeTools(() => ({
      getSources: {
        page: 0,
        nbPerPage: 25,
        totalCount: 2,
        data: [
          { id: "s1", name: "Snowflake Prod" },
          { id: "s2", name: "Tableau Cloud" },
        ],
      },
    }));
    const tool = find(tools, "catalog_search_sources");
    const res = await tool.handler({ nbPerPage: 25 });
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed).toMatchObject({
      pagination: { page: 0, nbPerPage: 25, totalCount: 2, hasMore: false },
      data: [
        { id: "s1", name: "Snowflake Prod" },
        { id: "s2", name: "Tableau Cloud" },
      ],
    });
  });

  it("signals hasMore when totalCount exceeds returned rows", async () => {
    const { tools } = makeTools(() => ({
      getSources: {
        page: 0,
        nbPerPage: 1,
        totalCount: 5,
        data: [{ id: "s1", name: "Source 1" }],
      },
    }));
    const tool = find(tools, "catalog_search_sources");
    const res = await tool.handler({ nbPerPage: 1 });
    const parsed = parseResult(res) as Record<string, unknown>;
    expect((parsed.pagination as Record<string, unknown>).hasMore).toBe(true);
  });

  it("forwards custom pagination", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_sources");
    await tool.handler({ nbPerPage: 50, page: 2 });
    expect(client.calls[0].variables).toMatchObject({
      pagination: { nbPerPage: 50, page: 2 },
    });
  });

  it("rejects invalid origin values at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_search_sources").config.inputSchema
    );
    expect(() => schema.parse({ origin: "MANUAL" })).toThrow();
  });

  it("rejects invalid type values at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_search_sources").config.inputSchema
    );
    expect(() => schema.parse({ type: "INVALID" })).toThrow();
  });
});

// ── catalog_search_databases ────────────────────────────────────────────────

describe("catalog_search_databases", () => {
  const emptyPage = {
    getDatabases: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("calls GET_DATABASES with no scope when no filters provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_databases");
    await tool.handler({});
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_DATABASES);
    expect(client.calls[0].variables).toMatchObject({
      scope: undefined,
      pagination: { nbPerPage: 100, page: 0 },
    });
  });

  it("builds scope from nameContains filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_databases");
    await tool.handler({ nameContains: "prod" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { nameContains: "prod" },
    });
  });

  it("builds scope from sourceIds filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_databases");
    await tool.handler({ sourceIds: ["src-1", "src-2"] });
    expect(client.calls[0].variables).toMatchObject({
      scope: { sourceIds: ["src-1", "src-2"] },
    });
  });

  it("builds scope with withDeleted and withHidden", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_databases");
    await tool.handler({ withDeleted: true, withHidden: true });
    expect(client.calls[0].variables).toMatchObject({
      scope: { withDeleted: true, withHidden: true },
    });
  });

  it("returns a listEnvelope with pagination and data", async () => {
    const { tools } = makeTools(() => ({
      getDatabases: {
        page: 0,
        nbPerPage: 100,
        totalCount: 1,
        data: [{ id: "db1", name: "ANALYTICS_DB" }],
      },
    }));
    const tool = find(tools, "catalog_search_databases");
    const res = await tool.handler({});
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed).toMatchObject({
      pagination: { page: 0, nbPerPage: 100, totalCount: 1, hasMore: false },
      data: [{ id: "db1", name: "ANALYTICS_DB" }],
    });
  });

  it("forwards custom pagination", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_databases");
    await tool.handler({ nbPerPage: 25, page: 3 });
    expect(client.calls[0].variables).toMatchObject({
      pagination: { nbPerPage: 25, page: 3 },
    });
  });
});

// ── catalog_search_schemas ──────────────────────────────────────────────────

describe("catalog_search_schemas", () => {
  const emptyPage = {
    getSchemas: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("calls GET_SCHEMAS with no scope when no filters provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_schemas");
    await tool.handler({});
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_SCHEMAS);
    expect(client.calls[0].variables).toMatchObject({
      scope: undefined,
      pagination: { nbPerPage: 100, page: 0 },
    });
  });

  it("builds scope from nameContains filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_schemas");
    await tool.handler({ nameContains: "public" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { nameContains: "public" },
    });
  });

  it("builds scope from databaseIds filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_schemas");
    await tool.handler({ databaseIds: ["db-1"] });
    expect(client.calls[0].variables).toMatchObject({
      scope: { databaseIds: ["db-1"] },
    });
  });

  it("builds scope from sourceIds filter", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_schemas");
    await tool.handler({ sourceIds: ["src-1"] });
    expect(client.calls[0].variables).toMatchObject({
      scope: { sourceIds: ["src-1"] },
    });
  });

  it("builds scope with multiple filters combined", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_schemas");
    await tool.handler({
      nameContains: "analytics",
      databaseIds: ["db-1"],
      sourceIds: ["src-1"],
      withDeleted: false,
      withHidden: true,
    });
    expect(client.calls[0].variables).toMatchObject({
      scope: {
        nameContains: "analytics",
        databaseIds: ["db-1"],
        sourceIds: ["src-1"],
        withDeleted: false,
        withHidden: true,
      },
    });
  });

  it("returns a listEnvelope with pagination and data", async () => {
    const { tools } = makeTools(() => ({
      getSchemas: {
        page: 0,
        nbPerPage: 100,
        totalCount: 3,
        data: [
          { id: "s1", name: "PUBLIC", databaseId: "db1" },
          { id: "s2", name: "ANALYTICS", databaseId: "db1" },
          { id: "s3", name: "RAW", databaseId: "db1" },
        ],
      },
    }));
    const tool = find(tools, "catalog_search_schemas");
    const res = await tool.handler({});
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed).toMatchObject({
      pagination: { page: 0, nbPerPage: 100, totalCount: 3, hasMore: false },
    });
    expect((parsed.data as unknown[]).length).toBe(3);
  });

  it("signals hasMore when totalCount exceeds returned rows", async () => {
    const { tools } = makeTools(() => ({
      getSchemas: {
        page: 0,
        nbPerPage: 2,
        totalCount: 10,
        data: [
          { id: "s1", name: "PUBLIC" },
          { id: "s2", name: "ANALYTICS" },
        ],
      },
    }));
    const tool = find(tools, "catalog_search_schemas");
    const res = await tool.handler({ nbPerPage: 2 });
    const parsed = parseResult(res) as Record<string, unknown>;
    expect((parsed.pagination as Record<string, unknown>).hasMore).toBe(true);
  });

  it("forwards custom pagination", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_schemas");
    await tool.handler({ nbPerPage: 10, page: 5 });
    expect(client.calls[0].variables).toMatchObject({
      pagination: { nbPerPage: 10, page: 5 },
    });
  });
});

// ── Error handling ──────────────────────────────────────────────────────────

describe("discovery tools error handling", () => {
  it("wraps CatalogGraphQLError in a structured error response", async () => {
    const { CatalogGraphQLError } = await import("../../src/client.js");
    const { tools } = makeTools(() => {
      throw new CatalogGraphQLError([
        { message: "Unauthorized", extensions: { code: "UNAUTHENTICATED" } },
      ]);
    });
    const tool = find(tools, "catalog_search_sources");
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed).toHaveProperty("error");
  });
});

// ── Annotations ─────────────────────────────────────────────────────────────

describe("discovery tools annotations", () => {
  it("all three tools are marked read-only", () => {
    const { tools } = makeTools();
    for (const tool of tools) {
      expect(tool.config.annotations?.readOnlyHint).toBe(true);
    }
  });

  it("registers exactly 3 tools", () => {
    const { tools } = makeTools();
    expect(tools).toHaveLength(3);
    expect(tools.map((t) => t.name).sort()).toEqual([
      "catalog_search_databases",
      "catalog_search_schemas",
      "catalog_search_sources",
    ]);
  });
});
