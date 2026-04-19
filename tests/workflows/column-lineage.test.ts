import { describe, it, expect } from "vitest";
import { defineColumnLineage } from "../../src/workflows/column-lineage.js";
import {
  GET_COLUMNS_SUMMARY,
  GET_DASHBOARDS_SUMMARY,
  GET_DATABASES,
  GET_FIELD_LINEAGES,
  GET_SCHEMAS,
  GET_TABLES_SUMMARY,
  GET_TABLES_WITH_SCHEMA_CHAIN,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface FieldEdgeFixture {
  id: string;
  parentColumnId?: string;
  childColumnId?: string;
  parentDashboardFieldId?: string;
  childDashboardFieldId?: string;
  childDashboardId?: string;
  lineageType?: "AUTOMATIC" | "MANUAL_CUSTOMER" | "MANUAL_OPS" | "OTHER_TECHNOS";
}

interface ColumnFixture {
  id: string;
  name: string;
  tableId: string;
}

interface TableFixture {
  id: string;
  name: string;
  schemaId: string;
}

interface SchemaFixture {
  id: string;
  name: string;
  databaseId: string;
}

interface DatabaseFixture {
  id: string;
  name: string;
}

interface DashboardFixture {
  id: string;
  name: string;
}

interface WorldFixture {
  edges: FieldEdgeFixture[];
  columns: ColumnFixture[];
  tables: TableFixture[];
  schemas: SchemaFixture[];
  databases: DatabaseFixture[];
  dashboards?: DashboardFixture[];
}

// Build a mock client that answers the BFS field-lineage queries scoped by
// parent/child column id, plus the batched enrichment queries (columns,
// tables, schemas, databases, dashboards).
function buildWorld(fixture: WorldFixture) {
  return makeMockClient((document, variables) => {
    const vars = variables as Record<string, unknown>;
    const scope = vars.scope as Record<string, unknown> | undefined;

    if (document === GET_FIELD_LINEAGES) {
      const parentColumnId = scope?.parentColumnId as string | undefined;
      const childColumnId = scope?.childColumnId as string | undefined;
      const parentDashboardFieldId = scope?.parentDashboardFieldId as string | undefined;
      const childDashboardFieldId = scope?.childDashboardFieldId as string | undefined;

      const matched = fixture.edges.filter((e) => {
        if (parentColumnId) return e.parentColumnId === parentColumnId;
        if (childColumnId) return e.childColumnId === childColumnId;
        if (parentDashboardFieldId) return e.parentDashboardFieldId === parentDashboardFieldId;
        if (childDashboardFieldId) return e.childDashboardFieldId === childDashboardFieldId;
        return false;
      });
      return {
        getFieldLineages: {
          totalCount: matched.length,
          nbPerPage: 500,
          page: 0,
          data: matched,
        },
      };
    }

    if (document === GET_COLUMNS_SUMMARY) {
      const ids = scope?.ids as string[] | undefined;
      const matched = ids
        ? fixture.columns.filter((c) => ids.includes(c.id))
        : fixture.columns;
      return {
        getColumns: {
          totalCount: matched.length,
          nbPerPage: matched.length || 1,
          page: 0,
          data: matched,
        },
      };
    }

    // Handlers below support catalog_find_asset_by_path, which
    // column-lineage calls internally whenever the user passes a columnFQN
    // instead of a columnId. Uses nameContains / scoped filters — never
    // `ids` on getSchemas / getDatabases (neither scope accepts it).
    if (document === GET_TABLES_SUMMARY) {
      const nameContains = scope?.nameContains as string | undefined;
      const schemaId = scope?.schemaId as string | undefined;
      let matched = fixture.tables;
      if (nameContains) matched = matched.filter((t) => t.name.toLowerCase().includes(nameContains.toLowerCase()));
      if (schemaId) matched = matched.filter((t) => t.schemaId === schemaId);
      return {
        getTables: {
          totalCount: matched.length,
          nbPerPage: matched.length || 1,
          page: 0,
          data: matched,
        },
      };
    }

    if (document === GET_SCHEMAS) {
      // The real `GetSchemasScope` has NO `ids` field — calling with `ids`
      // makes the server 400. Earlier iterations of column-lineage did
      // exactly that, and the mock (which accepted anything) failed to
      // catch it. Now the mock enforces the same rejection as the server.
      if (scope && "ids" in scope) {
        throw new Error(
          "mock: GetSchemasScope does not accept `ids` — real server returns 400"
        );
      }
      const nameContains = scope?.nameContains as string | undefined;
      const databaseIds = scope?.databaseIds as string[] | undefined;
      let matched = fixture.schemas;
      if (nameContains) matched = matched.filter((s) => s.name.toLowerCase().includes(nameContains.toLowerCase()));
      if (databaseIds) matched = matched.filter((s) => databaseIds.includes(s.databaseId));
      return {
        getSchemas: {
          totalCount: matched.length,
          nbPerPage: matched.length || 1,
          page: 0,
          data: matched,
        },
      };
    }

    if (document === GET_DATABASES) {
      // Same as GetSchemasScope: no `ids` filter. Mirror the server's 400.
      if (scope && "ids" in scope) {
        throw new Error(
          "mock: GetDatabasesScope does not accept `ids` — real server returns 400"
        );
      }
      const nameContains = scope?.nameContains as string | undefined;
      let matched = fixture.databases;
      if (nameContains) matched = matched.filter((d) => d.name.toLowerCase().includes(nameContains.toLowerCase()));
      return {
        getDatabases: {
          totalCount: matched.length,
          nbPerPage: matched.length || 1,
          page: 0,
          data: matched,
        },
      };
    }

    if (document === GET_TABLES_WITH_SCHEMA_CHAIN) {
      // The enrichment path that resolves tableIds → FQN components. The
      // real server accepts only `scope.ids` for batched lookup; surfacing
      // `schema { id name databaseId database { id name } }` via the
      // relation chain.
      const ids = scope?.ids as string[] | undefined;
      const matched = ids
        ? fixture.tables.filter((t) => ids.includes(t.id))
        : fixture.tables;
      return {
        getTables: {
          totalCount: matched.length,
          nbPerPage: matched.length || 1,
          page: 0,
          data: matched.map((t) => {
            const schema = fixture.schemas.find((s) => s.id === t.schemaId);
            const database = schema
              ? fixture.databases.find((d) => d.id === schema.databaseId)
              : undefined;
            return {
              id: t.id,
              name: t.name,
              schemaId: t.schemaId,
              schema: schema
                ? {
                    id: schema.id,
                    name: schema.name,
                    databaseId: schema.databaseId,
                    database: database
                      ? { id: database.id, name: database.name }
                      : null,
                  }
                : null,
            };
          }),
        },
      };
    }

    if (document === GET_DASHBOARDS_SUMMARY) {
      const ids = scope?.ids as string[] | undefined;
      const matched = ids
        ? (fixture.dashboards ?? []).filter((d) => ids.includes(d.id))
        : fixture.dashboards ?? [];
      return {
        getDashboards: {
          totalCount: matched.length,
          nbPerPage: matched.length || 1,
          page: 0,
          data: matched,
        },
      };
    }

    throw new Error(`unexpected document: ${document.slice(0, 40)}…`);
  });
}

// Baseline fixture: a simple 4-node linear graph A → B → C with one dashboard-
// field D hanging off C. Only the types necessary to resolve FQNs are added.
const LINEAR_WORLD: WorldFixture = {
  edges: [
    { id: "e-ab", parentColumnId: "col-a", childColumnId: "col-b", lineageType: "AUTOMATIC" },
    { id: "e-bc", parentColumnId: "col-b", childColumnId: "col-c", lineageType: "AUTOMATIC" },
    {
      id: "e-cd",
      parentColumnId: "col-c",
      childDashboardFieldId: "df-d",
      childDashboardId: "dash-1",
      lineageType: "AUTOMATIC",
    },
  ],
  columns: [
    { id: "col-a", name: "a_col", tableId: "tbl-a" },
    { id: "col-b", name: "b_col", tableId: "tbl-b" },
    { id: "col-c", name: "c_col", tableId: "tbl-c" },
  ],
  tables: [
    { id: "tbl-a", name: "TBL_A", schemaId: "sch-1" },
    { id: "tbl-b", name: "TBL_B", schemaId: "sch-1" },
    { id: "tbl-c", name: "TBL_C", schemaId: "sch-1" },
  ],
  schemas: [{ id: "sch-1", name: "PUBLIC", databaseId: "db-1" }],
  databases: [{ id: "db-1", name: "WH" }],
  dashboards: [{ id: "dash-1", name: "Exec Sales" }],
};

describe("catalog_get_column_lineage", () => {
  it("walks downstream to exhaustion from a columnId and resolves every FQN", async () => {
    const client = buildWorld(LINEAR_WORLD);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({ columnId: "col-a", direction: "downstream" });
    const out = parseResult(res);
    expect(out.root).toMatchObject({ columnId: "col-a", name: "a_col" });
    const downstream = out.downstream as { nodes: Array<Record<string, unknown>>; edges: unknown[] };
    const columnNodes = downstream.nodes.filter((n) => n.assetType === "COLUMN");
    expect(columnNodes.map((n) => n.id).sort()).toEqual(["col-b", "col-c"]);
    for (const n of columnNodes) {
      expect(n.fqn).toBeTruthy();
      expect(n.fqn).toMatch(/^WH\.PUBLIC\.TBL_/);
    }
    const dashboardFieldNode = downstream.nodes.find((n) => n.assetType === "DASHBOARD_FIELD");
    expect(dashboardFieldNode).toMatchObject({
      id: "df-d",
      dashboardId: "dash-1",
      dashboardName: "Exec Sales",
      hydrationUnavailable: true,
    });
  });

  it("walks upstream and downstream when direction is 'both'", async () => {
    const client = buildWorld(LINEAR_WORLD);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({ columnId: "col-b", direction: "both" });
    const out = parseResult(res);
    const upstream = out.upstream as { nodes: Array<Record<string, unknown>> };
    const downstream = out.downstream as { nodes: Array<Record<string, unknown>> };
    expect(upstream.nodes.map((n) => n.id)).toContain("col-a");
    expect(downstream.nodes.map((n) => n.id)).toContain("col-c");
  });

  it("filters out dashboard-field endpoints when includeDashboardFields is false", async () => {
    const client = buildWorld(LINEAR_WORLD);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({
      columnId: "col-c",
      direction: "downstream",
      includeDashboardFields: false,
    });
    const out = parseResult(res);
    const downstream = out.downstream as { nodes: Array<Record<string, unknown>> };
    expect(downstream.nodes.find((n) => n.assetType === "DASHBOARD_FIELD")).toBeUndefined();
  });

  it("returns a structured error for an unknown FQN", async () => {
    const client = buildWorld(LINEAR_WORLD);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({ columnFQN: "WH.PUBLIC.MISSING_TABLE.MISSING_COL" });
    const out = parseResult(res);
    expect(out.error).toBe("column_not_found");
  });

  it("refuses to start with neither columnFQN nor columnId", async () => {
    const client = buildWorld(LINEAR_WORLD);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({});
    const out = parseResult(res);
    expect(out.error).toMatch(/columnFQN|columnId/);
  });

  it("filters lineageTypes post-fetch and returns an empty graph when no edges match", async () => {
    const client = buildWorld(LINEAR_WORLD);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({
      columnId: "col-a",
      direction: "downstream",
      lineageTypes: ["MANUAL_CUSTOMER"],
    });
    const out = parseResult(res);
    const downstream = out.downstream as { nodes: Array<Record<string, unknown>>; edges: unknown[] };
    expect(downstream.nodes).toHaveLength(0);
    expect(downstream.edges).toHaveLength(0);
  });

  it("throws when total reached nodes exceeds maxNodes (runaway safety)", async () => {
    // Build a world with 10 children off col-a and set maxNodes=3 so the
    // safety ceiling fires before the traversal completes.
    const world: WorldFixture = {
      edges: Array.from({ length: 10 }, (_, i) => ({
        id: `e-${i}`,
        parentColumnId: "col-a",
        childColumnId: `col-${i}`,
        lineageType: "AUTOMATIC" as const,
      })),
      columns: [
        { id: "col-a", name: "a_col", tableId: "tbl-a" },
        ...Array.from({ length: 10 }, (_, i) => ({
          id: `col-${i}`,
          name: `c${i}`,
          tableId: "tbl-a",
        })),
      ],
      tables: [{ id: "tbl-a", name: "TBL_A", schemaId: "sch-1" }],
      schemas: [{ id: "sch-1", name: "PUBLIC", databaseId: "db-1" }],
      databases: [{ id: "db-1", name: "WH" }],
    };
    const client = buildWorld(world);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({
      columnId: "col-a",
      direction: "downstream",
      maxNodes: 3,
    });
    const out = parseResult(res);
    expect(out.error).toMatch(/exceeded maxNodes/);
  });

  it("handles cycles via the visited set — a ↔ b does not infinite-loop", async () => {
    const world: WorldFixture = {
      edges: [
        { id: "e-ab", parentColumnId: "col-a", childColumnId: "col-b", lineageType: "AUTOMATIC" },
        { id: "e-ba", parentColumnId: "col-b", childColumnId: "col-a", lineageType: "AUTOMATIC" },
      ],
      columns: [
        { id: "col-a", name: "a", tableId: "tbl-a" },
        { id: "col-b", name: "b", tableId: "tbl-a" },
      ],
      tables: [{ id: "tbl-a", name: "T", schemaId: "s" }],
      schemas: [{ id: "s", name: "S", databaseId: "d" }],
      databases: [{ id: "d", name: "D" }],
    };
    const client = buildWorld(world);
    const tool = defineColumnLineage(client);
    const res = await tool.handler({ columnId: "col-a", direction: "downstream" });
    const out = parseResult(res);
    const downstream = out.downstream as { nodes: Array<Record<string, unknown>>; edges: unknown[] };
    // Root (col-a) is excluded from the `nodes` list — only col-b appears.
    expect(downstream.nodes.map((n) => n.id)).toEqual(["col-b"]);
    // Both edges are followed (a→b discovered expanding a; b→a discovered
    // expanding b), but the visited set stops further expansion back to a.
    expect(downstream.edges).toHaveLength(2);
  });
});
