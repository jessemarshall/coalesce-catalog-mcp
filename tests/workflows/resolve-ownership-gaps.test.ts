import { describe, it, expect } from "vitest";
import { defineResolveOwnershipGaps } from "../../src/workflows/resolve-ownership-gaps.js";
import {
  GET_TABLES_DETAIL_BATCH,
  GET_LINEAGES,
  GET_TABLE_QUERIES,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
  isError?: boolean;
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockTable {
  id: string;
  name: string;
  popularity?: number | null;
  schemaId?: string | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
}

interface MockQuery {
  author?: string | null;
  queryType?: string;
}

interface MockEdge {
  parentTableId?: string;
  childTableId?: string;
  parentDashboardId?: string;
  childDashboardId?: string;
}

interface RouterOpts {
  tablesByScope: (scope: Record<string, unknown>) => MockTable[];
  queriesByTableId?: Map<string, MockQuery[]>;
  // Map of `tableId|direction` (downstream/upstream) → edges
  edgesByKey?: Map<string, MockEdge[]>;
  detailByIds?: (ids: string[]) => MockTable[];
  totalTablesOverride?: number;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as {
        scope?: {
          ids?: string[];
          schemaId?: string;
          databaseId?: string;
        };
        pagination: { nbPerPage: number; page: number };
      };
      const sc = vars.scope ?? {};
      // Enrichment calls pass `ids`. Scoped calls pass schemaId/databaseId/ids.
      if (sc.ids && opts.detailByIds) {
        const rows = opts.detailByIds(sc.ids);
        return {
          getTables: {
            totalCount: rows.length,
            nbPerPage: vars.pagination.nbPerPage,
            page: 0,
            data: rows,
          },
        };
      }
      const all = opts.tablesByScope(sc as Record<string, unknown>);
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = all.slice(start, start + vars.pagination.nbPerPage);
      return {
        getTables: {
          totalCount: opts.totalTablesOverride ?? all.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_TABLE_QUERIES) {
      const vars = variables as {
        scope?: { tableIds?: string[] };
        pagination: { nbPerPage: number; page: number };
      };
      const id = vars.scope?.tableIds?.[0] ?? "";
      const rows = opts.queriesByTableId?.get(id) ?? [];
      return {
        getTableQueries: {
          totalCount: rows.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: 0,
          data: rows,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: {
          parentTableId?: string;
          childTableId?: string;
        };
      };
      const direction = vars.scope?.parentTableId
        ? "downstream"
        : "upstream";
      const tid = vars.scope?.parentTableId ?? vars.scope?.childTableId ?? "";
      const edges = opts.edgesByKey?.get(`${tid}|${direction}`) ?? [];
      return {
        getLineages: {
          totalCount: edges.length,
          nbPerPage: 100,
          page: 0,
          data: edges,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_resolve_ownership_gaps — scope validation", () => {
  it("refuses when no scope is provided", async () => {
    const client = makeRouter({ tablesByScope: () => [] });
    const tool = defineResolveOwnershipGaps(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/Scope required/);
  });

  it("refuses when multiple scope fields are passed", async () => {
    const client = makeMockClient(() => {
      throw new Error("unreachable");
    });
    const tool = defineResolveOwnershipGaps(client);
    const res = await tool.handler({
      databaseId: "db-1",
      tableIds: ["t-1"],
    });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/Multiple scope fields/i);
  });

  it("refuses when tableIds count exceeds the 500-table scope cap", async () => {
    const client = makeRouter({ tablesByScope: () => [] });
    const tool = defineResolveOwnershipGaps(client);
    const ids = Array.from({ length: 501 }, (_, i) => `t-${i}`);
    const res = await tool.handler({ tableIds: ids });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/501.*500-table/);
  });
});

describe("catalog_resolve_ownership_gaps — scan completeness", () => {
  it("refuses when the scope resolves to > scope cap (500)", async () => {
    const tables = Array.from({ length: 500 }, (_, i) => ({
      id: `t-${i}`,
      name: `T${i}`,
    }));
    const client = makeRouter({
      tablesByScope: () => tables,
      totalTablesOverride: 750,
    });
    const tool = defineResolveOwnershipGaps(client);
    const res = await tool.handler({ databaseId: "db-big" });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/750 tables/);
    expect(parseResult(res).error).toMatch(/500-table/);
  });

  it("refuses when unowned count exceeds the 200-unowned cap", async () => {
    // 201 tables, all unowned → past the 200 cap.
    const tables = Array.from({ length: 201 }, (_, i) => ({
      id: `t-${i}`,
      name: `T${i}`,
      ownerEntities: [],
      teamOwnerEntities: [],
    }));
    const client = makeRouter({
      tablesByScope: () => tables,
    });
    const tool = defineResolveOwnershipGaps(client);
    const res = await tool.handler({ schemaId: "sch-wide" });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/201 unowned/);
    expect(parseResult(res).error).toMatch(/200-table evidence-gathering cap/);
  });
});

describe("catalog_resolve_ownership_gaps — happy path", () => {
  it("emits per-table evidence with query-author grouping and neighbor owners", async () => {
    const tables: MockTable[] = [
      // Owned — should be filtered out
      {
        id: "t-owned",
        name: "OWNED",
        popularity: 0.9,
        ownerEntities: [{ id: "o", userId: "u1" }],
        teamOwnerEntities: [],
      },
      // Unowned — evidence expected
      {
        id: "t-orphan-a",
        name: "ORPHAN_A",
        popularity: 0.5,
        schemaId: "sch-1",
        ownerEntities: [],
        teamOwnerEntities: [],
      },
      // Unowned + orphaned binding (userId: null) — still counts as unowned
      {
        id: "t-orphan-b",
        name: "ORPHAN_B",
        popularity: 0.3,
        schemaId: "sch-1",
        ownerEntities: [{ id: "o-dead", userId: null }],
        teamOwnerEntities: [],
      },
    ];
    const queriesByTableId = new Map<string, MockQuery[]>([
      [
        "t-orphan-a",
        [
          { author: "alice@example.com", queryType: "SELECT" },
          { author: "alice@example.com", queryType: "SELECT" },
          { author: "alice@example.com", queryType: "WRITE" },
          { author: "bob@example.com", queryType: "SELECT" },
          { author: null }, // null author is dropped
        ],
      ],
      ["t-orphan-b", []],
    ]);
    const edgesByKey = new Map<string, MockEdge[]>([
      [
        "t-orphan-a|downstream",
        [
          { parentTableId: "t-orphan-a", childTableId: "t-downstream-1" },
          { parentTableId: "t-orphan-a", childDashboardId: "d-1" },
        ],
      ],
      [
        "t-orphan-a|upstream",
        [{ parentTableId: "t-upstream-1", childTableId: "t-orphan-a" }],
      ],
      ["t-orphan-b|downstream", []],
      ["t-orphan-b|upstream", []],
    ]);
    const detailByIds = (ids: string[]): MockTable[] => {
      const all: Record<string, MockTable> = {
        "t-upstream-1": {
          id: "t-upstream-1",
          name: "UPSTREAM_1",
          ownerEntities: [
            {
              id: "o",
              userId: "u-carol",
              user: {
                id: "u-carol",
                email: "carol@example.com",
                fullName: "Carol",
              },
            },
          ],
          teamOwnerEntities: [
            { id: "to", teamId: "team-x", team: { name: "Finance" } },
          ],
        },
        "t-downstream-1": {
          id: "t-downstream-1",
          name: "DOWNSTREAM_1",
          ownerEntities: [],
          teamOwnerEntities: [],
        },
      };
      return ids.map((id) => all[id]).filter((x): x is MockTable => !!x);
    };
    const client = makeRouter({
      tablesByScope: () => tables,
      queriesByTableId,
      edgesByKey,
      detailByIds,
    });
    const tool = defineResolveOwnershipGaps(client);
    const out = parseResult(
      await tool.handler({
        schemaId: "sch-1",
        queryAuthorLimit: 5,
      })
    );

    expect(out.scopedBy).toBe("schemaId");
    expect(out.scanned).toEqual({ tablesInScope: 3, unownedCount: 2 });

    const rows = out.tables as Array<Record<string, unknown>>;
    expect(rows).toHaveLength(2);
    // Popularity DESC → t-orphan-a (0.5) before t-orphan-b (0.3)
    expect(rows[0].id).toBe("t-orphan-a");
    expect(rows[1].id).toBe("t-orphan-b");

    const a = rows[0] as {
      queryAuthors: {
        totalQueriesSeen: number;
        authors: Array<{
          author: string;
          queryCount: number;
          queryTypeBreakdown: Record<string, number>;
        }>;
      };
      upstreamNeighbors: Array<{
        id: string;
        kind: string;
        owners: Array<Record<string, unknown>>;
      }>;
      downstreamNeighbors: Array<{
        id: string;
        kind: string;
        owners: Array<Record<string, unknown>>;
      }>;
    };
    expect(a.queryAuthors.totalQueriesSeen).toBe(5);
    expect(a.queryAuthors.authors).toEqual([
      {
        author: "alice@example.com",
        queryCount: 3,
        queryTypeBreakdown: { SELECT: 2, WRITE: 1 },
      },
      {
        author: "bob@example.com",
        queryCount: 1,
        queryTypeBreakdown: { SELECT: 1 },
      },
    ]);
    expect(a.upstreamNeighbors).toHaveLength(1);
    expect(a.upstreamNeighbors[0]).toMatchObject({
      id: "t-upstream-1",
      kind: "TABLE",
      name: "UPSTREAM_1",
    });
    expect(a.upstreamNeighbors[0].owners).toEqual([
      {
        type: "user",
        userId: "u-carol",
        email: "carol@example.com",
        name: "Carol",
      },
      {
        type: "team",
        teamId: "team-x",
        name: "Finance",
      },
    ]);
    expect(a.downstreamNeighbors).toHaveLength(2);
    const downTable = a.downstreamNeighbors.find((n) => n.kind === "TABLE");
    const downDash = a.downstreamNeighbors.find((n) => n.kind === "DASHBOARD");
    expect(downTable?.owners).toEqual([]);
    expect(downDash?.owners).toEqual([]); // dashboards surface but owners aren't fetched
  });

  it("respects includeQueryAuthors=false and includeLineageNeighbors=false (selective evidence)", async () => {
    const tables: MockTable[] = [
      {
        id: "t-orphan",
        name: "ORPHAN",
        ownerEntities: [],
        teamOwnerEntities: [],
      },
    ];
    const client = makeRouter({ tablesByScope: () => tables });
    const tool = defineResolveOwnershipGaps(client);
    await tool.handler({
      schemaId: "s",
      includeQueryAuthors: false,
      includeLineageNeighbors: false,
    });
    const documents = client.calls.map((c) => c.document);
    // Only the scope fetch should fire — no query/lineage calls.
    expect(documents).toEqual([GET_TABLES_DETAIL_BATCH]);
  });
});

describe("catalog_resolve_ownership_gaps — enrichment completeness", () => {
  it("throws if a lineage-reached table neighbor is missing from the enrichment response", async () => {
    const tables: MockTable[] = [
      {
        id: "t-orphan",
        name: "ORPHAN",
        popularity: 0.1,
        ownerEntities: [],
        teamOwnerEntities: [],
      },
    ];
    const edgesByKey = new Map<string, MockEdge[]>([
      [
        "t-orphan|upstream",
        [{ parentTableId: "t-missing", childTableId: "t-orphan" }],
      ],
      ["t-orphan|downstream", []],
    ]);
    // detailByIds returns empty — neighbor enrichment deliberately misses.
    const client = makeRouter({
      tablesByScope: () => tables,
      queriesByTableId: new Map(),
      edgesByKey,
      detailByIds: () => [],
    });
    const tool = defineResolveOwnershipGaps(client);
    const res = await tool.handler({ schemaId: "sch" });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/Neighbor enrichment returned no row/);
    expect(parseResult(res).error).toMatch(/t-missing/);
  });
});

describe("catalog_resolve_ownership_gaps — empty results", () => {
  it("returns unownedCount=0 when every in-scope table is owned", async () => {
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "T",
        ownerEntities: [{ id: "o", userId: "u" }],
      },
    ];
    const client = makeRouter({ tablesByScope: () => tables });
    const tool = defineResolveOwnershipGaps(client);
    const out = parseResult(await tool.handler({ schemaId: "s" }));
    expect(out.scanned).toEqual({ tablesInScope: 1, unownedCount: 0 });
    expect(out.tables).toEqual([]);
  });
});
