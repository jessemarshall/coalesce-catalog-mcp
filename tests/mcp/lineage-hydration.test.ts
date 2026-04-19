import { describe, it, expect } from "vitest";
import type { CatalogClient } from "../../src/client.js";
import { defineLineageTools } from "../../src/mcp/lineage.js";
import {
  GET_LINEAGES,
  GET_FIELD_LINEAGES,
  GET_TABLES_SUMMARY,
  GET_DASHBOARDS_SUMMARY,
  GET_COLUMNS_SUMMARY,
} from "../../src/catalog/operations.js";

// ── Mock client factory ────────────────────────────────────────────────────
// Returns canned responses keyed by GraphQL document string. This lets us
// control exactly what the hydration map receives (empty → forces fallback).

function mockClient(
  responses: Map<string, unknown>
): CatalogClient {
  return {
    endpoint: "https://mock.invalid/graphql",
    region: "eu",
    async execute<TData>(document: string): Promise<TData> {
      for (const [key, value] of responses) {
        if (document.includes(key)) return value as TData;
      }
      throw new Error(`Unmocked query: ${document.slice(0, 80)}`);
    },
  };
}

function parseHandlerResult(result: {
  content: Array<{ type: string; text: string }>;
}): unknown {
  return JSON.parse(result.content[0].text);
}

// ── enrichAssetEdge fallback kind tests ────────────────────────────────────

describe("catalog_get_lineages hydration fallback kind", () => {
  it("falls back to DASHBOARD for a dashboard-parent edge when hydration misses", async () => {
    const client = mockClient(
      new Map([
        // getLineages returns one edge: dashboard parent → table child
        [
          "getLineages",
          {
            getLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "edge-1",
                  parentTableId: null,
                  parentDashboardId: "dash-parent-unknown",
                  childTableId: "table-child-1",
                  childDashboardId: null,
                  createdAt: 1700000000000,
                  refreshedAt: 1700000000000,
                },
              ],
            },
          },
        ],
        // Hydration: getTables returns the child table but NOT the dashboard
        [
          "CatalogGetTablesSummary",
          {
            getTables: {
              totalCount: 1,
              nbPerPage: 500,
              page: 0,
              data: [{ id: "table-child-1", name: "orders" }],
            },
          },
        ],
        // Hydration: getDashboards returns empty — the dashboard ID is unknown
        [
          "CatalogGetDashboardsSummary",
          {
            getDashboards: {
              totalCount: 0,
              nbPerPage: 500,
              page: 0,
              data: [],
            },
          },
        ],
      ])
    );

    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler({
      parentDashboardId: "dash-parent-unknown",
      hydrate: true,
    });
    const parsed = parseHandlerResult(result) as {
      data: Array<{ parent: { id: string; kind: string }; child: { id: string; kind: string } }>;
    };

    expect(parsed.data).toHaveLength(1);
    // Parent is a dashboard whose ID wasn't in the hydration map — must fall back to DASHBOARD
    expect(parsed.data[0].parent).toEqual({
      id: "dash-parent-unknown",
      kind: "DASHBOARD",
    });
    // Child table WAS hydrated
    expect(parsed.data[0].child.kind).toBe("TABLE");
    expect(parsed.data[0].child.name).toBe("orders");
  });

  it("falls back to TABLE for a table-parent edge when hydration misses", async () => {
    const client = mockClient(
      new Map([
        [
          "getLineages",
          {
            getLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "edge-2",
                  parentTableId: "table-unknown",
                  parentDashboardId: null,
                  childDashboardId: "dash-child-1",
                  childTableId: null,
                  createdAt: null,
                  refreshedAt: null,
                },
              ],
            },
          },
        ],
        [
          "CatalogGetTablesSummary",
          {
            getTables: {
              totalCount: 0,
              nbPerPage: 500,
              page: 0,
              data: [],
            },
          },
        ],
        [
          "CatalogGetDashboardsSummary",
          {
            getDashboards: {
              totalCount: 1,
              nbPerPage: 500,
              page: 0,
              data: [{ id: "dash-child-1", name: "Revenue Dashboard" }],
            },
          },
        ],
      ])
    );

    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler({
      parentTableId: "table-unknown",
      hydrate: true,
    });
    const parsed = parseHandlerResult(result) as {
      data: Array<{ parent: { id: string; kind: string }; child: { id: string; kind: string } }>;
    };

    expect(parsed.data).toHaveLength(1);
    // Parent is a table whose ID wasn't hydrated — must fall back to TABLE
    expect(parsed.data[0].parent).toEqual({
      id: "table-unknown",
      kind: "TABLE",
    });
    // Child dashboard WAS hydrated
    expect(parsed.data[0].child.kind).toBe("DASHBOARD");
    expect(parsed.data[0].child.name).toBe("Revenue Dashboard");
  });
});

// ── enrichFieldEdge fallback kind tests ────────────────────────────────────

describe("catalog_get_field_lineages hydration fallback kind", () => {
  it("falls back to DASHBOARD_FIELD for a dashboard-field child when hydration misses", async () => {
    const client = mockClient(
      new Map([
        [
          "getFieldLineages",
          {
            getFieldLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "fedge-1",
                  parentColumnId: "col-parent-1",
                  parentDashboardFieldId: null,
                  childColumnId: null,
                  childDashboardFieldId: "dashfield-unknown",
                  childDashboardId: null,
                  createdAt: 1700000000000,
                  refreshedAt: null,
                },
              ],
            },
          },
        ],
        // Columns: returns the parent column
        [
          "CatalogGetColumnsSummary",
          {
            getColumns: {
              totalCount: 1,
              nbPerPage: 500,
              page: 0,
              data: [
                { id: "col-parent-1", name: "user_id", tableId: "t1" },
              ],
            },
          },
        ],
        // Dashboards: empty (no dashboard to hydrate)
        [
          "CatalogGetDashboardsSummary",
          {
            getDashboards: {
              totalCount: 0,
              nbPerPage: 500,
              page: 0,
              data: [],
            },
          },
        ],
      ])
    );

    const tools = defineLineageTools(client);
    const handler = tools.find(
      (t) => t.name === "catalog_get_field_lineages"
    )!.handler;
    const result = await handler({
      parentColumnId: "col-parent-1",
      hydrate: true,
    });
    const parsed = parseHandlerResult(result) as {
      data: Array<{
        parent: { id: string; kind: string };
        child: { id: string; kind: string; hydrationUnavailable?: boolean };
      }>;
    };

    expect(parsed.data).toHaveLength(1);
    // Parent column WAS hydrated
    expect(parsed.data[0].parent.kind).toBe("COLUMN");
    expect(parsed.data[0].parent.name).toBe("user_id");
    // Child is a dashboard-field — must fall back to DASHBOARD_FIELD (not COLUMN)
    // Dashboard fields get the hydrationUnavailable placeholder from the hydration map
    expect(parsed.data[0].child.kind).toBe("DASHBOARD_FIELD");
  });

  it("falls back to DASHBOARD for a dashboard-child edge when hydration misses", async () => {
    const client = mockClient(
      new Map([
        [
          "getFieldLineages",
          {
            getFieldLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "fedge-2",
                  parentColumnId: "col-p2",
                  parentDashboardFieldId: null,
                  childColumnId: null,
                  childDashboardFieldId: null,
                  childDashboardId: "dash-unknown",
                  createdAt: null,
                  refreshedAt: null,
                },
              ],
            },
          },
        ],
        [
          "CatalogGetColumnsSummary",
          {
            getColumns: {
              totalCount: 1,
              nbPerPage: 500,
              page: 0,
              data: [{ id: "col-p2", name: "amount", tableId: "t2" }],
            },
          },
        ],
        // Dashboards: empty — the dashboard ID is unknown
        [
          "CatalogGetDashboardsSummary",
          {
            getDashboards: {
              totalCount: 0,
              nbPerPage: 500,
              page: 0,
              data: [],
            },
          },
        ],
      ])
    );

    const tools = defineLineageTools(client);
    const handler = tools.find(
      (t) => t.name === "catalog_get_field_lineages"
    )!.handler;
    const result = await handler({
      parentColumnId: "col-p2",
      hydrate: true,
    });
    const parsed = parseHandlerResult(result) as {
      data: Array<{
        parent: { id: string; kind: string };
        child: { id: string; kind: string };
      }>;
    };

    expect(parsed.data).toHaveLength(1);
    expect(parsed.data[0].parent.kind).toBe("COLUMN");
    // Child is a dashboard whose ID wasn't hydrated — must fall back to DASHBOARD (not COLUMN)
    expect(parsed.data[0].child).toEqual({
      id: "dash-unknown",
      kind: "DASHBOARD",
    });
  });

  it("falls back to DASHBOARD_FIELD for a dashboard-field parent when hydration misses", async () => {
    const client = mockClient(
      new Map([
        [
          "getFieldLineages",
          {
            getFieldLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "fedge-3",
                  parentColumnId: null,
                  parentDashboardFieldId: "dashfield-parent-unknown",
                  childColumnId: "col-child-1",
                  childDashboardFieldId: null,
                  childDashboardId: null,
                  createdAt: 1700000000000,
                  refreshedAt: 1700000000000,
                },
              ],
            },
          },
        ],
        [
          "CatalogGetColumnsSummary",
          {
            getColumns: {
              totalCount: 1,
              nbPerPage: 500,
              page: 0,
              data: [{ id: "col-child-1", name: "email", tableId: "t3" }],
            },
          },
        ],
        [
          "CatalogGetDashboardsSummary",
          {
            getDashboards: {
              totalCount: 0,
              nbPerPage: 500,
              page: 0,
              data: [],
            },
          },
        ],
      ])
    );

    const tools = defineLineageTools(client);
    const handler = tools.find(
      (t) => t.name === "catalog_get_field_lineages"
    )!.handler;
    const result = await handler({
      childColumnId: "col-child-1",
      hydrate: true,
    });
    const parsed = parseHandlerResult(result) as {
      data: Array<{
        parent: { id: string; kind: string; hydrationUnavailable?: boolean };
        child: { id: string; kind: string };
      }>;
    };

    expect(parsed.data).toHaveLength(1);
    // Parent is a dashboard-field — must fall back to DASHBOARD_FIELD (not COLUMN)
    // The hydration map sets hydrationUnavailable for dashboard fields
    expect(parsed.data[0].parent.kind).toBe("DASHBOARD_FIELD");
    expect(parsed.data[0].parent.hydrationUnavailable).toBe(true);
    // Child column WAS hydrated
    expect(parsed.data[0].child.kind).toBe("COLUMN");
  });
});
