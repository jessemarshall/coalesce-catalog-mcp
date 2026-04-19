import { describe, it, expect } from "vitest";
import {
  defineFindAssetByPath,
  parsePath,
} from "../../src/workflows/find-asset-by-path.js";
import {
  GET_DATABASES,
  GET_SCHEMAS,
  GET_TABLES_SUMMARY,
  GET_COLUMNS_SUMMARY,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface Row {
  id: string;
  name: string;
}

interface FixturePage {
  totalCount: number;
  data: Row[];
}

interface Fixture {
  databases?: FixturePage;
  schemas?: FixturePage;
  tables?: FixturePage;
  columns?: FixturePage | Array<FixturePage>;
}

// Slice the fixture according to the requested nbPerPage/page so handler-side
// pagination behaves the same as against the real server.
function paginate(page: FixturePage, nbPerPage: number, index: number) {
  const start = index * nbPerPage;
  return {
    totalCount: page.totalCount,
    data: page.data.slice(start, start + nbPerPage),
  };
}

function routerFor(fixture: Fixture) {
  return makeMockClient((document, variables) => {
    const vars = variables as { pagination: { page: number; nbPerPage: number } };
    if (document === GET_DATABASES) {
      if (!fixture.databases) throw new Error("no databases fixture");
      return {
        getDatabases: paginate(
          fixture.databases,
          vars.pagination.nbPerPage,
          vars.pagination.page
        ),
      };
    }
    if (document === GET_SCHEMAS) {
      if (!fixture.schemas) throw new Error("no schemas fixture");
      return {
        getSchemas: paginate(
          fixture.schemas,
          vars.pagination.nbPerPage,
          vars.pagination.page
        ),
      };
    }
    if (document === GET_TABLES_SUMMARY) {
      if (!fixture.tables) throw new Error("no tables fixture");
      return {
        getTables: paginate(
          fixture.tables,
          vars.pagination.nbPerPage,
          vars.pagination.page
        ),
      };
    }
    if (document === GET_COLUMNS_SUMMARY) {
      if (!fixture.columns) throw new Error("no columns fixture");
      const page = Array.isArray(fixture.columns)
        ? fixture.columns[vars.pagination.page]
        : fixture.columns;
      if (!page) return { getColumns: { totalCount: 0, data: [] } };
      return {
        getColumns: paginate(
          page,
          vars.pagination.nbPerPage,
          Array.isArray(fixture.columns) ? 0 : vars.pagination.page
        ),
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 40)}…`);
  });
}

describe("parsePath", () => {
  it("splits a 3-part unquoted path", () => {
    expect(parsePath("DB.SCHEMA.TABLE")).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("splits a 4-part path with column", () => {
    expect(parsePath("DB.SCHEMA.TABLE.COL")).toEqual([
      "DB",
      "SCHEMA",
      "TABLE",
      "COL",
    ]);
  });

  it("unwraps double-quoted identifiers", () => {
    expect(parsePath('"DB"."SCHEMA"."TABLE"')).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("unwraps backtick-quoted identifiers", () => {
    expect(parsePath("`DB`.`SCHEMA`.`TABLE`")).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("handles mixed quoting", () => {
    expect(parsePath('"DB".SCHEMA.`TABLE`')).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("preserves dots inside quoted identifiers", () => {
    expect(parsePath('"weird.name".schema.table')).toEqual([
      "weird.name",
      "schema",
      "table",
    ]);
  });

  it("preserves spaces inside identifiers", () => {
    expect(parsePath('"Databricks Demo".coalesce.sample_data.orders')).toEqual([
      "Databricks Demo",
      "coalesce",
      "sample_data",
      "orders",
    ]);
  });

  it("trims surrounding whitespace per component", () => {
    expect(parsePath("  db  .  schema  .  table  ")).toEqual([
      "db",
      "schema",
      "table",
    ]);
  });

  it("drops empty components from leading/trailing dots", () => {
    expect(parsePath(".db.schema.table.")).toEqual(["db", "schema", "table"]);
  });

  it("returns empty for an empty string", () => {
    expect(parsePath("")).toEqual([]);
  });

  it("handles 2 parts (caller detects too-few, tool returns error)", () => {
    expect(parsePath("db.schema")).toHaveLength(2);
  });
});

describe("catalog_find_asset_by_path — handler", () => {
  it("returns a structured notFound when the path has fewer than 3 parts", async () => {
    const tool = defineFindAssetByPath(routerFor({}));
    const res = await tool.handler({ path: "db.schema" });
    const out = parseResult(res);
    expect(out.notFound).toBe(true);
    expect(out.parsedParts).toEqual(["db", "schema"]);
  });

  it("resolves a 3-part table path and returns a TABLE UUID", async () => {
    const tool = defineFindAssetByPath(
      routerFor({
        databases: {
          totalCount: 1,
          data: [{ id: "db-1", name: "PROD" }],
        },
        schemas: {
          totalCount: 1,
          data: [{ id: "sch-1", name: "SALES" }],
        },
        tables: {
          totalCount: 1,
          data: [{ id: "t-1", name: "ORDERS" }],
        },
      })
    );
    const res = await tool.handler({ path: "PROD.SALES.ORDERS" });
    const out = parseResult(res);
    const resolved = out.resolved as Record<string, unknown>;
    expect(resolved).toMatchObject({
      kind: "TABLE",
      id: "t-1",
      fullPath: "PROD.SALES.ORDERS",
    });
  });

  it("returns ambiguous when two databases match the same name", async () => {
    const tool = defineFindAssetByPath(
      routerFor({
        databases: {
          totalCount: 2,
          data: [
            { id: "db-1", name: "PROD" },
            { id: "db-2", name: "PROD" },
          ],
        },
      })
    );
    const out = parseResult(
      await tool.handler({ path: "PROD.SALES.ORDERS" })
    );
    expect(out.ambiguous).toBe(true);
    expect(out.at).toBe("database");
  });

  it("returns notFound at the schema step with the resolved database preserved", async () => {
    const tool = defineFindAssetByPath(
      routerFor({
        databases: {
          totalCount: 1,
          data: [{ id: "db-1", name: "PROD" }],
        },
        schemas: { totalCount: 0, data: [] },
      })
    );
    const out = parseResult(await tool.handler({ path: "PROD.SALES.ORDERS" }));
    expect(out.notFound).toBe(true);
    expect(out.database).toMatchObject({ id: "db-1", name: "PROD" });
    expect(out.reason).toMatch(/No schema/i);
  });

  it("folds case when caseSensitive is false (default) — resolves ORDER_ID to order_id", async () => {
    // Server returns the lowercase-named column; we search case-insensitively.
    const tool = defineFindAssetByPath(
      routerFor({
        databases: { totalCount: 1, data: [{ id: "db-1", name: "PROD" }] },
        schemas: { totalCount: 1, data: [{ id: "sch-1", name: "SALES" }] },
        tables: { totalCount: 1, data: [{ id: "t-1", name: "ORDERS" }] },
        columns: {
          totalCount: 1,
          data: [{ id: "c-1", name: "order_id" }],
        },
      })
    );
    const out = parseResult(
      await tool.handler({ path: "PROD.SALES.ORDERS.ORDER_ID" })
    );
    const resolved = out.resolved as Record<string, unknown>;
    expect(resolved).toMatchObject({
      kind: "COLUMN",
      id: "c-1",
      column: { id: "c-1", name: "order_id" },
    });
  });

  it("honors caseSensitive=true — does NOT match order_id when searching for ORDER_ID", async () => {
    const tool = defineFindAssetByPath(
      routerFor({
        databases: { totalCount: 1, data: [{ id: "db-1", name: "PROD" }] },
        schemas: { totalCount: 1, data: [{ id: "sch-1", name: "SALES" }] },
        tables: { totalCount: 1, data: [{ id: "t-1", name: "ORDERS" }] },
        columns: {
          totalCount: 1,
          data: [{ id: "c-1", name: "order_id" }],
        },
      })
    );
    const out = parseResult(
      await tool.handler({
        path: "PROD.SALES.ORDERS.ORDER_ID",
        caseSensitive: true,
      })
    );
    expect(out.notFound).toBe(true);
  });

  it("paginates past a short-substring cliff — finds the target on page 2", async () => {
    // 201 databases all contain "DB"; the target is at index 150 — past the
    // old 50-row cap and past page 0 at 200/page. Walks into page 1 to find it.
    const manyDbs: Row[] = Array.from({ length: 201 }, (_, i) => ({
      id: `db-${i}`,
      name: i === 150 ? "DB" : `DB_${i}`,
    }));
    const tool = defineFindAssetByPath(
      routerFor({
        databases: { totalCount: manyDbs.length, data: manyDbs },
        schemas: { totalCount: 1, data: [{ id: "sch-1", name: "SALES" }] },
        tables: { totalCount: 1, data: [{ id: "t-1", name: "ORDERS" }] },
      })
    );
    const out = parseResult(await tool.handler({ path: "DB.SALES.ORDERS" }));
    expect(out.resolved).toMatchObject({
      kind: "TABLE",
      id: "t-1",
    });
  });
});
