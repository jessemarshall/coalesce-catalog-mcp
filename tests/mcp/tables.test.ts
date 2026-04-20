import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineTableTools } from "../../src/mcp/tables.js";
import {
  GET_TABLES_SUMMARY,
  GET_TABLES_DETAIL_BATCH,
  GET_TABLE_DETAIL,
  GET_TABLE_QUERIES,
  UPDATE_TABLES,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const client = makeMockClient(responder ?? (() => ({})));
  const tools = defineTableTools(client);
  return { client, tools };
}

function find(
  tools: ReturnType<typeof defineTableTools>,
  name: string
) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }): unknown {
  return JSON.parse(r.content[0].text);
}

// ---------------------------------------------------------------------------
// Inventory & annotations
// ---------------------------------------------------------------------------

describe("table tool inventory", () => {
  const { tools } = makeTools();

  it("registers 4 tools", () => {
    expect(tools).toHaveLength(4);
  });

  it("names follow catalog_ prefix", () => {
    for (const t of tools) {
      expect(t.name).toMatch(/^catalog_/);
    }
  });

  it("expected tool names are present", () => {
    const names = tools.map((t) => t.name);
    expect(names).toContain("catalog_search_tables");
    expect(names).toContain("catalog_get_table");
    expect(names).toContain("catalog_get_table_queries");
    expect(names).toContain("catalog_update_table_metadata");
  });

  it("read tools are readOnlyHint=true", () => {
    for (const name of [
      "catalog_search_tables",
      "catalog_get_table",
      "catalog_get_table_queries",
    ]) {
      expect(find(tools, name).config.annotations?.readOnlyHint).toBe(true);
    }
  });

  it("update_table_metadata is a write tool", () => {
    const def = find(tools, "catalog_update_table_metadata");
    expect(def.config.annotations?.readOnlyHint).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// catalog_search_tables — input schema
// ---------------------------------------------------------------------------

describe("catalog_search_tables input schema", () => {
  const { tools } = makeTools();
  const schema = z.object(find(tools, "catalog_search_tables").config.inputSchema);

  it("accepts no arguments (unscoped search)", () => {
    expect(() => schema.parse({})).not.toThrow();
  });

  it("accepts nameContains", () => {
    expect(() => schema.parse({ nameContains: "orders" })).not.toThrow();
  });

  it("accepts full scope + sorting + pagination", () => {
    expect(() =>
      schema.parse({
        nameContains: "orders",
        pathContains: "PROD_DB.ANALYTICS",
        databaseId: "db-1",
        schemaId: "s-1",
        warehouseId: "wh-1",
        ids: ["t-1", "t-2"],
        withDeleted: true,
        withHidden: false,
        sortBy: "popularity",
        sortDirection: "DESC",
        nullsPriority: "LAST",
        nbPerPage: 50,
        page: 2,
      })
    ).not.toThrow();
  });

  it("rejects invalid sortBy", () => {
    expect(() => schema.parse({ sortBy: "invalid_key" })).toThrow();
  });

  it("rejects nbPerPage > 500", () => {
    expect(() => schema.parse({ nbPerPage: 501 })).toThrow();
  });
});

// ---------------------------------------------------------------------------
// catalog_search_tables — handler
// ---------------------------------------------------------------------------

describe("catalog_search_tables handler", () => {
  it("builds correct variables for an unscoped search", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");

    await tool.handler({});

    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_TABLES_SUMMARY);
    const vars = client.calls[0].variables as Record<string, unknown>;
    expect(vars.scope).toBeUndefined();
    expect(vars.sorting).toBeUndefined();
  });

  it("builds scope from nameContains + databaseId", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");

    await tool.handler({ nameContains: "orders", databaseId: "db-1" });

    const vars = client.calls[0].variables as {
      scope?: Record<string, unknown>;
    };
    expect(vars.scope).toEqual({
      nameContains: "orders",
      databaseId: "db-1",
    });
  });

  it("builds scope with all fields populated", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");

    await tool.handler({
      nameContains: "orders",
      pathContains: "PROD.ANALYTICS",
      databaseId: "db-1",
      schemaId: "s-1",
      warehouseId: "wh-1",
      ids: ["t-1"],
      withDeleted: true,
      withHidden: false,
    });

    const vars = client.calls[0].variables as {
      scope?: Record<string, unknown>;
    };
    expect(vars.scope).toEqual({
      nameContains: "orders",
      pathContains: "PROD.ANALYTICS",
      databaseId: "db-1",
      schemaId: "s-1",
      warehouseId: "wh-1",
      ids: ["t-1"],
      withDeleted: true,
      withHidden: false,
    });
  });

  it("builds sorting when sortBy is provided", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");

    await tool.handler({
      sortBy: "popularity",
      sortDirection: "DESC",
      nullsPriority: "LAST",
    });

    const vars = client.calls[0].variables as {
      sorting?: Array<Record<string, unknown>>;
    };
    expect(vars.sorting).toEqual([
      { sortingKey: "popularity", direction: "DESC", nullsPriority: "LAST" },
    ]);
  });

  it("omits direction and nulls from sorting when not provided", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");

    await tool.handler({ sortBy: "name" });

    const vars = client.calls[0].variables as {
      sorting?: Array<Record<string, unknown>>;
    };
    expect(vars.sorting).toEqual([{ sortingKey: "name" }]);
  });

  it("returns listEnvelope shape with pagination metadata", async () => {
    const { tools } = makeTools(() => ({
      getTables: {
        page: 0,
        nbPerPage: 25,
        totalCount: 42,
        data: [{ id: "t-1", name: "orders" }],
      },
    }));
    const tool = find(tools, "catalog_search_tables");

    const res = await tool.handler({ nbPerPage: 25 });

    expect(res.isError).toBeUndefined();
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed).toMatchObject({
      pagination: { page: 0, nbPerPage: 25, totalCount: 42, hasMore: true },
      data: [{ id: "t-1", name: "orders" }],
    });
  });

  it("hasMore is false on the last page", async () => {
    const { tools } = makeTools(() => ({
      getTables: {
        page: 0,
        nbPerPage: 100,
        totalCount: 2,
        data: [{ id: "t-1" }, { id: "t-2" }],
      },
    }));
    const tool = find(tools, "catalog_search_tables");

    const res = await tool.handler({});
    const parsed = parseResult(res) as Record<string, unknown>;
    const pag = parsed.pagination as Record<string, unknown>;
    expect(pag.hasMore).toBe(false);
  });

  it("forwards pagination params to the GraphQL variables", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 3, nbPerPage: 50, totalCount: 200, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");

    await tool.handler({ nbPerPage: 50, page: 3 });

    const vars = client.calls[0].variables as {
      pagination: Record<string, unknown>;
    };
    expect(vars.pagination).toMatchObject({ nbPerPage: 50, page: 3 });
  });

  it("defaults to the summary operation when projection is omitted", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");
    await tool.handler({});
    expect(client.calls[0].document).toBe(GET_TABLES_SUMMARY);
  });

  it("uses summary operation when projection is 'summary'", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");
    await tool.handler({ projection: "summary" });
    expect(client.calls[0].document).toBe(GET_TABLES_SUMMARY);
  });

  it("uses detail-batch operation when projection is 'detailed'", async () => {
    const { client, tools } = makeTools(() => ({
      getTables: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_tables");
    await tool.handler({ projection: "detailed" });
    expect(client.calls[0].document).toBe(GET_TABLES_DETAIL_BATCH);
  });

  it("rejects invalid projection values at the schema layer", () => {
    const schema = z.object(find(makeTools().tools, "catalog_search_tables").config.inputSchema);
    expect(() => schema.parse({ projection: "full" })).toThrow();
  });
});

// ---------------------------------------------------------------------------
// catalog_get_table — input schema + handler
// ---------------------------------------------------------------------------

describe("catalog_get_table", () => {
  const schema = z.object(
    find(makeTools().tools, "catalog_get_table").config.inputSchema
  );

  it("requires id", () => {
    expect(() => schema.parse({})).toThrow();
  });

  it("rejects empty id", () => {
    expect(() => schema.parse({ id: "" })).toThrow();
  });

  it("accepts a valid UUID string", () => {
    expect(() => schema.parse({ id: "table-uuid-123" })).not.toThrow();
  });

  it("fetches a single table by id using GET_TABLE_DETAIL", async () => {
    const tableData = {
      id: "t-1",
      name: "orders",
      description: "Order data",
    };
    const { client, tools } = makeTools(() => ({
      getTables: { data: [tableData] },
    }));
    const tool = find(tools, "catalog_get_table");

    const res = await tool.handler({ id: "t-1" });

    expect(res.isError).toBeUndefined();
    expect(client.calls[0].document).toBe(GET_TABLE_DETAIL);
    expect(client.calls[0].variables).toEqual({ ids: ["t-1"] });
    expect(parseResult(res)).toEqual({ table: tableData });
  });

  it("returns null when no table matches", async () => {
    const { tools } = makeTools(() => ({
      getTables: { data: [] },
    }));
    const tool = find(tools, "catalog_get_table");

    const res = await tool.handler({ id: "nonexistent" });

    expect(res.isError).toBeUndefined();
    expect(parseResult(res)).toEqual({ table: null });
  });
});

// ---------------------------------------------------------------------------
// catalog_get_table_queries — input schema
// ---------------------------------------------------------------------------

describe("catalog_get_table_queries input schema", () => {
  const schema = z.object(
    find(makeTools().tools, "catalog_get_table_queries").config.inputSchema
  );

  it("requires tableIds", () => {
    expect(() => schema.parse({})).toThrow();
  });

  it("rejects empty tableIds array", () => {
    expect(() => schema.parse({ tableIds: [] })).toThrow();
  });

  it("rejects more than 50 tableIds", () => {
    const ids = Array.from({ length: 51 }, (_, i) => `t-${i}`);
    expect(() => schema.parse({ tableIds: ids })).toThrow();
  });

  it("accepts valid tableIds + optional fields", () => {
    expect(() =>
      schema.parse({
        tableIds: ["t-1"],
        tableIdsFilterMode: "ANY",
        queryType: "SELECT",
        databaseId: "db-1",
        schemaId: "s-1",
        warehouseId: "wh-1",
        sortBy: "timestamp",
        sortDirection: "DESC",
        nbPerPage: 25,
        page: 0,
      })
    ).not.toThrow();
  });

  it("rejects invalid queryType", () => {
    expect(() =>
      schema.parse({ tableIds: ["t-1"], queryType: "DELETE" })
    ).toThrow();
  });

  it("rejects invalid filterMode", () => {
    expect(() =>
      schema.parse({ tableIds: ["t-1"], tableIdsFilterMode: "NONE" })
    ).toThrow();
  });
});

// ---------------------------------------------------------------------------
// catalog_get_table_queries — handler
// ---------------------------------------------------------------------------

describe("catalog_get_table_queries handler", () => {
  it("builds scope with required tableIds and optional filters", async () => {
    const { client, tools } = makeTools(() => ({
      getTableQueries: {
        page: 0,
        nbPerPage: 100,
        totalCount: 0,
        data: [],
      },
    }));
    const tool = find(tools, "catalog_get_table_queries");

    await tool.handler({
      tableIds: ["t-1", "t-2"],
      tableIdsFilterMode: "ANY",
      queryType: "SELECT",
      databaseId: "db-1",
    });

    expect(client.calls[0].document).toBe(GET_TABLE_QUERIES);
    const vars = client.calls[0].variables as {
      scope: Record<string, unknown>;
    };
    expect(vars.scope).toEqual({
      tableIds: ["t-1", "t-2"],
      tableIdsFilterMode: "ANY",
      queryType: "SELECT",
      databaseId: "db-1",
    });
  });

  it("builds minimal scope with just tableIds", async () => {
    const { client, tools } = makeTools(() => ({
      getTableQueries: {
        page: 0,
        nbPerPage: 100,
        totalCount: 0,
        data: [],
      },
    }));
    const tool = find(tools, "catalog_get_table_queries");

    await tool.handler({ tableIds: ["t-1"] });

    const vars = client.calls[0].variables as {
      scope: Record<string, unknown>;
    };
    expect(vars.scope).toEqual({ tableIds: ["t-1"] });
  });

  it("builds query sorting when sortBy is provided", async () => {
    const { client, tools } = makeTools(() => ({
      getTableQueries: {
        page: 0,
        nbPerPage: 100,
        totalCount: 0,
        data: [],
      },
    }));
    const tool = find(tools, "catalog_get_table_queries");

    await tool.handler({
      tableIds: ["t-1"],
      sortBy: "timestamp",
      sortDirection: "DESC",
    });

    const vars = client.calls[0].variables as {
      sorting?: Array<Record<string, unknown>>;
    };
    expect(vars.sorting).toEqual([
      { sortingKey: "timestamp", direction: "DESC" },
    ]);
  });

  it("omits sorting when sortBy is absent", async () => {
    const { client, tools } = makeTools(() => ({
      getTableQueries: {
        page: 0,
        nbPerPage: 100,
        totalCount: 0,
        data: [],
      },
    }));
    const tool = find(tools, "catalog_get_table_queries");

    await tool.handler({ tableIds: ["t-1"] });

    const vars = client.calls[0].variables as {
      sorting?: unknown;
    };
    expect(vars.sorting).toBeUndefined();
  });

  it("returns listEnvelope with query results", async () => {
    const queryRow = {
      id: "q-1",
      queryText: "SELECT * FROM orders",
      author: "analytics@co.com",
    };
    const { tools } = makeTools(() => ({
      getTableQueries: {
        page: 0,
        nbPerPage: 25,
        totalCount: 1,
        data: [queryRow],
      },
    }));
    const tool = find(tools, "catalog_get_table_queries");

    const res = await tool.handler({ tableIds: ["t-1"], nbPerPage: 25 });

    expect(res.isError).toBeUndefined();
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed).toMatchObject({
      pagination: { page: 0, nbPerPage: 25, totalCount: 1, hasMore: false },
      data: [queryRow],
    });
  });
});

// ---------------------------------------------------------------------------
// catalog_update_table_metadata — input schema + handler
// ---------------------------------------------------------------------------

describe("catalog_update_table_metadata", () => {
  const schema = z.object(
    find(makeTools().tools, "catalog_update_table_metadata").config.inputSchema
  );

  it("requires data array with at least one entry", () => {
    expect(() => schema.parse({ data: [] })).toThrow();
  });

  it("requires id in each entry", () => {
    expect(() =>
      schema.parse({ data: [{ name: "orders" }] })
    ).toThrow();
  });

  it("accepts a single update with all fields", () => {
    expect(() =>
      schema.parse({
        data: [
          {
            id: "t-1",
            name: "orders_v2",
            externalDescription: "Order table",
            tableType: "VIEW",
            url: "https://example.com/orders",
            externalId: "ext-123",
          },
        ],
      })
    ).not.toThrow();
  });

  it("accepts up to 500 entries", () => {
    const data = Array.from({ length: 500 }, (_, i) => ({
      id: `t-${i}`,
      name: `table_${i}`,
    }));
    expect(() => schema.parse({ data })).not.toThrow();
  });

  it("rejects more than 500 entries", () => {
    const data = Array.from({ length: 501 }, (_, i) => ({
      id: `t-${i}`,
    }));
    expect(() => schema.parse({ data })).toThrow();
  });

  it("rejects invalid tableType", () => {
    expect(() =>
      schema.parse({
        data: [{ id: "t-1", tableType: "MATERIALIZED_VIEW" }],
      })
    ).toThrow();
  });

  it("rejects invalid url", () => {
    expect(() =>
      schema.parse({
        data: [{ id: "t-1", url: "not-a-url" }],
      })
    ).toThrow();
  });

  it("sends data to UPDATE_TABLES and returns updated count", async () => {
    const updatedRow = { id: "t-1", name: "orders_v2" };
    const { client, tools } = makeTools(() => ({
      updateTables: [updatedRow],
    }));
    const tool = find(tools, "catalog_update_table_metadata");

    const res = await tool.handler({
      data: [{ id: "t-1", name: "orders_v2" }],
    });

    expect(res.isError).toBeUndefined();
    expect(client.calls[0].document).toBe(UPDATE_TABLES);
    expect(client.calls[0].variables).toEqual({
      data: [{ id: "t-1", name: "orders_v2" }],
    });
    expect(parseResult(res)).toEqual({
      updated: 1,
      data: [updatedRow],
    });
  });

  it("returns correct count for batch updates", async () => {
    const rows = [
      { id: "t-1", name: "a" },
      { id: "t-2", name: "b" },
      { id: "t-3", name: "c" },
    ];
    const { tools } = makeTools(() => ({ updateTables: rows }));
    const tool = find(tools, "catalog_update_table_metadata");

    const res = await tool.handler({ data: rows });

    expect(parseResult(res)).toMatchObject({ updated: 3 });
  });
});

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

describe("table tools error handling", () => {
  it("surfaces transport errors as isError results", async () => {
    const { tools } = makeTools(() => {
      throw new Error("network failure");
    });
    const tool = find(tools, "catalog_search_tables");

    const res = await tool.handler({});

    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/network failure/);
  });
});
