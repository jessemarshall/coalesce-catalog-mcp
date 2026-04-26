import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineColumnTools } from "../../src/mcp/columns.js";
import {
  GET_COLUMNS_SUMMARY,
  GET_COLUMN_DETAIL,
  GET_COLUMN_JOINS,
  UPDATE_COLUMNS_METADATA,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const client = makeMockClient(responder ?? (() => ({})));
  const tools = defineColumnTools(client);
  return { client, tools };
}

function find(tools: ReturnType<typeof defineColumnTools>, name: string) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

// ── catalog_search_columns ─────────────────────────────────────────────────

describe("catalog_search_columns handler", () => {
  const emptyPage = {
    getColumns: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("uses the summary operation", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_columns");
    await tool.handler({ nameContains: "order" });
    expect(client.calls[0].document).toBe(GET_COLUMNS_SUMMARY);
  });

  it("passes scope filters to the query", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_columns");
    await tool.handler({ nameContains: "order", tableId: "t-1", isPii: true });
    const vars = client.calls[0].variables as Record<string, unknown>;
    const scope = vars.scope as Record<string, unknown>;
    expect(scope.nameContains).toBe("order");
    expect(scope.tableId).toBe("t-1");
    expect(scope.isPii).toBe(true);
  });

  it("passes sorting through", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_columns");
    await tool.handler({
      nameContains: "x",
      sortBy: "name",
      sortDirection: "DESC",
      nullsPriority: "LAST",
    });
    const vars = client.calls[0].variables as Record<string, unknown>;
    const sorting = vars.sorting as Array<Record<string, unknown>>;
    expect(sorting).toEqual([
      { sortingKey: "name", direction: "DESC", nullsPriority: "LAST" },
    ]);
  });

  it("returns a listEnvelope with hasMore=true when more pages exist", async () => {
    const { tools } = makeTools(() => ({
      getColumns: {
        page: 0,
        nbPerPage: 2,
        totalCount: 5,
        data: [{ id: "c-1" }, { id: "c-2" }],
      },
    }));
    const tool = find(tools, "catalog_search_columns");
    const res = await tool.handler({ nameContains: "x", nbPerPage: 2 });
    const parsed = JSON.parse(res.content[0].text);
    expect(parsed.pagination.hasMore).toBe(true);
    expect(parsed.pagination.totalCount).toBe(5);
    expect(parsed.data).toHaveLength(2);
  });

  it("returns hasMore=false on the last page", async () => {
    const { tools } = makeTools(() => ({
      getColumns: {
        page: 2,
        nbPerPage: 2,
        totalCount: 5,
        data: [{ id: "c-5" }],
      },
    }));
    const tool = find(tools, "catalog_search_columns");
    const res = await tool.handler({ nameContains: "x", nbPerPage: 2, page: 2 });
    const parsed = JSON.parse(res.content[0].text);
    expect(parsed.pagination.hasMore).toBe(false);
  });

  it("omits scope when no filters are provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_columns");
    await tool.handler({});
    const vars = client.calls[0].variables as Record<string, unknown>;
    expect(vars.scope).toBeUndefined();
  });
});

// ── catalog_get_column ─────────────────────────────────────────────────────

describe("catalog_get_column handler", () => {
  it("uses GET_COLUMN_DETAIL and passes id as a single-element ids array", async () => {
    const { client, tools } = makeTools(() => ({
      getColumns: { data: [{ id: "c-1", name: "order_id" }] },
    }));
    const tool = find(tools, "catalog_get_column");
    await tool.handler({ id: "c-1" });
    expect(client.calls[0].document).toBe(GET_COLUMN_DETAIL);
    const vars = client.calls[0].variables as Record<string, unknown>;
    expect(vars.ids).toEqual(["c-1"]);
  });

  it("returns the column wrapped as { column: <row> }", async () => {
    const row = { id: "c-1", name: "order_id", tableId: "t-1" };
    const { tools } = makeTools(() => ({
      getColumns: { data: [row] },
    }));
    const tool = find(tools, "catalog_get_column");
    const res = await tool.handler({ id: "c-1" });
    const parsed = JSON.parse(res.content[0].text);
    expect(parsed.column).toEqual(row);
  });

  it("returns { column: null } when no match", async () => {
    const { tools } = makeTools(() => ({
      getColumns: { data: [] },
    }));
    const tool = find(tools, "catalog_get_column");
    const res = await tool.handler({ id: "nonexistent" });
    const parsed = JSON.parse(res.content[0].text);
    expect(parsed.column).toBeNull();
  });
});

// ── catalog_get_column_joins ───────────────────────────────────────────────

describe("catalog_get_column_joins handler", () => {
  const emptyPage = {
    getColumnJoins: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("uses GET_COLUMN_JOINS operation", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_column_joins");
    await tool.handler({ columnIds: ["c-1"] });
    expect(client.calls[0].document).toBe(GET_COLUMN_JOINS);
  });

  it("passes columnIds and tableIds as scope", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_column_joins");
    await tool.handler({ columnIds: ["c-1", "c-2"], tableIds: ["t-1"] });
    const vars = client.calls[0].variables as Record<string, unknown>;
    const scope = vars.scope as Record<string, unknown>;
    expect(scope.columnIds).toEqual(["c-1", "c-2"]);
    expect(scope.tableIds).toEqual(["t-1"]);
  });

  it("passes sorting and pagination", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_column_joins");
    await tool.handler({
      columnIds: ["c-1"],
      sortBy: "count",
      sortDirection: "DESC",
      nbPerPage: 50,
      page: 1,
    });
    const vars = client.calls[0].variables as Record<string, unknown>;
    expect(vars.sorting).toEqual([{ sortingKey: "count", direction: "DESC" }]);
    expect(vars.pagination).toEqual({ nbPerPage: 50, page: 1 });
  });

  it("returns a listEnvelope with join records", async () => {
    const joinRow = {
      id: "j-1",
      firstColumnId: "c-1",
      secondColumnId: "c-2",
      count: 42,
    };
    const { tools } = makeTools(() => ({
      getColumnJoins: {
        page: 0,
        nbPerPage: 100,
        totalCount: 1,
        data: [joinRow],
      },
    }));
    const tool = find(tools, "catalog_get_column_joins");
    const res = await tool.handler({ columnIds: ["c-1"] });
    const parsed = JSON.parse(res.content[0].text);
    expect(parsed.pagination.totalCount).toBe(1);
    expect(parsed.data).toEqual([joinRow]);
  });
});

// ── catalog_update_column_metadata ─────────────────────────────────────────

describe("catalog_update_column_metadata handler", () => {
  it("uses UPDATE_COLUMNS_METADATA and passes data through", async () => {
    const returned = [
      { id: "c-1", descriptionRaw: "new desc", isPii: false },
    ];
    const { client, tools } = makeTools(() => ({
      updateColumnsMetadata: returned,
    }));
    const tool = find(tools, "catalog_update_column_metadata");
    await tool.handler({
      data: [{ id: "c-1", descriptionRaw: "new desc" }],
    });
    expect(client.calls[0].document).toBe(UPDATE_COLUMNS_METADATA);
    const vars = client.calls[0].variables as Record<string, unknown>;
    expect(vars.data).toEqual([{ id: "c-1", descriptionRaw: "new desc" }]);
  });

  it("returns the updated rows with a count", async () => {
    const returned = [
      { id: "c-1", descriptionRaw: "d1" },
      { id: "c-2", descriptionRaw: "d2" },
    ];
    const { tools } = makeTools(() => ({
      updateColumnsMetadata: returned,
    }));
    const tool = find(tools, "catalog_update_column_metadata");
    const res = await tool.handler({
      data: [
        { id: "c-1", descriptionRaw: "d1" },
        { id: "c-2", descriptionRaw: "d2" },
      ],
    });
    const parsed = JSON.parse(res.content[0].text);
    expect(parsed.updated).toBe(2);
    expect(parsed.data).toEqual(returned);
  });

  it("rejects empty data array at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_update_column_metadata").config
        .inputSchema
    );
    expect(() => schema.parse({ data: [] })).toThrow();
  });

  it("rejects items missing required id at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_update_column_metadata").config
        .inputSchema
    );
    expect(() =>
      schema.parse({ data: [{ descriptionRaw: "x" }] })
    ).toThrow();
  });
});

// ── Schema-level validation ────────────────────────────────────────────────

describe("column tool schema validation", () => {
  it("catalog_get_column rejects empty id", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_get_column").config.inputSchema
    );
    expect(() => schema.parse({ id: "" })).toThrow();
  });

  it("catalog_search_columns rejects invalid sortBy", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_search_columns").config.inputSchema
    );
    expect(() => schema.parse({ sortBy: "invalid_key" })).toThrow();
  });

  it("catalog_get_column_joins accepts empty object (no scope)", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_get_column_joins").config.inputSchema
    );
    expect(() => schema.parse({})).not.toThrow();
  });
});
