import { describe, it, expect } from "vitest";
import { defineTriageQualityFailures } from "../../src/workflows/triage-quality-failures.js";
import {
  GET_TABLES_DETAIL_BATCH,
  GET_DATA_QUALITIES,
  GET_LINEAGES,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockTable {
  id: string;
  name: string;
  popularity?: number | null;
  numberOfQueries?: number | null;
  description?: string | null;
  isVerified?: boolean;
  isDeprecated?: boolean;
  schemaId?: string;
  ownerEntities?: unknown[];
  teamOwnerEntities?: unknown[];
  tagEntities?: unknown[];
}

interface MockQualityCheck {
  id: string;
  name: string;
  status: "ALERT" | "WARNING" | "SUCCESS";
  result?: string | null;
  source?: string | null;
  ownerEmail?: string | null;
  externalId?: string;
  tableId: string;
  columnId?: string | null;
  runAt?: string | null;
  url?: string | null;
}

interface MockLineageEdge {
  id: string;
  parentTableId?: string | null;
  parentDashboardId?: string | null;
  childTableId?: string | null;
  childDashboardId?: string | null;
}

interface RouterOpts {
  tablesByScope: (
    scope: Record<string, unknown>
  ) => MockTable[];
  qualityChecksByTableId: Map<string, MockQualityCheck[]>;
  lineageByChildTableId?: Map<string, MockLineageEdge[]>;
  totalTablesOverride?: number;
  upstreamTableDetails?: Map<string, MockTable>;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as {
        scope?: Record<string, unknown>;
        pagination: { nbPerPage: number; page: number };
      };
      // If fetching by ids (upstream hydration), return from upstreamTableDetails
      const ids = vars.scope?.ids as string[] | undefined;
      if (ids && opts.upstreamTableDetails) {
        const data = ids
          .map((id) => opts.upstreamTableDetails!.get(id))
          .filter(Boolean);
        return {
          getTables: {
            totalCount: data.length,
            nbPerPage: vars.pagination.nbPerPage,
            page: vars.pagination.page,
            data,
          },
        };
      }
      const all = opts.tablesByScope(vars.scope ?? {});
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
    if (document === GET_DATA_QUALITIES) {
      const vars = variables as {
        scope?: { tableId?: string };
        pagination: { nbPerPage: number; page: number };
      };
      const tableId = vars.scope?.tableId ?? "";
      const all = opts.qualityChecksByTableId.get(tableId) ?? [];
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = all.slice(start, start + vars.pagination.nbPerPage);
      return {
        getDataQualities: {
          totalCount: all.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: { childTableId?: string };
        pagination: { nbPerPage: number; page: number };
      };
      const childId = vars.scope?.childTableId ?? "";
      const all = opts.lineageByChildTableId?.get(childId) ?? [];
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = all.slice(start, start + vars.pagination.nbPerPage);
      return {
        getLineages: {
          totalCount: all.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_triage_quality_failures — scope validation", () => {
  it("refuses when no scope is provided", async () => {
    const client = makeRouter({
      tablesByScope: () => [],
      qualityChecksByTableId: new Map(),
    });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/Scope required/);
  });

  it("refuses when multiple scope fields are passed", async () => {
    const client = makeMockClient(() => {
      throw new Error("should not be called");
    });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({
      databaseId: "db-1",
      schemaId: "sch-1",
    });
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/Multiple scope fields/);
  });

  it("refuses when tableIds exceeds the 500 cap", async () => {
    const client = makeRouter({
      tablesByScope: () => [],
      qualityChecksByTableId: new Map(),
    });
    const tool = defineTriageQualityFailures(client);
    const ids = Array.from({ length: 501 }, (_, i) => `t-${i}`);
    const res = await tool.handler({ tableIds: ids });
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/501/);
    expect(out.error).toMatch(/500-table cap/);
  });
});

describe("catalog_triage_quality_failures — empty results", () => {
  it("returns empty triage when scope resolves to no tables", async () => {
    const client = makeRouter({
      tablesByScope: () => [],
      qualityChecksByTableId: new Map(),
    });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({ schemaId: "empty" });
    const out = parseResult(res);
    expect(out.scopedBy).toBe("schemaId");
    const stats = out.stats as Record<string, number>;
    expect(stats.tablesInScope).toBe(0);
    expect(stats.tablesWithFailures).toBe(0);
    expect(out.triageQueue).toEqual([]);
    expect(out.rankedTables).toEqual([]);
  });

  it("returns empty triage when no quality checks are failing", async () => {
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "ORDERS",
        popularity: 0.9,
        ownerEntities: [{ id: "o1", userId: "u1", user: { id: "u1", email: "alice@co.io", fullName: "Alice" } }],
      },
    ];
    const checks: MockQualityCheck[] = [
      { id: "qc-1", name: "not_null", status: "SUCCESS", externalId: "ext-1", tableId: "t-1" },
    ];
    const client = makeRouter({
      tablesByScope: () => tables,
      qualityChecksByTableId: new Map([["t-1", checks]]),
    });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({ schemaId: "sch" });
    const out = parseResult(res);
    const stats = out.stats as Record<string, number>;
    expect(stats.tablesInScope).toBe(1);
    expect(stats.tablesWithFailures).toBe(0);
    expect(stats.totalFailingChecks).toBe(0);
  });
});

describe("catalog_triage_quality_failures — happy path", () => {
  it("triages ALERT/WARNING checks with ownership and upstream lineage", async () => {
    const tables: MockTable[] = [
      {
        id: "t-hot",
        name: "ORDERS",
        popularity: 0.95,
        numberOfQueries: 1200,
        ownerEntities: [
          { id: "o1", userId: "u1", user: { id: "u1", email: "alice@co.io", fullName: "Alice" } },
        ],
        teamOwnerEntities: [],
      },
      {
        id: "t-cold",
        name: "STAGING",
        popularity: 0.1,
        numberOfQueries: 5,
        ownerEntities: [],
        teamOwnerEntities: [],
      },
      {
        id: "t-clean",
        name: "CLEAN_TABLE",
        popularity: 0.5,
        ownerEntities: [
          { id: "o2", userId: "u2", user: { id: "u2", email: "bob@co.io", fullName: "Bob" } },
        ],
      },
    ];

    const checks = new Map<string, MockQualityCheck[]>([
      [
        "t-hot",
        [
          { id: "qc-1", name: "not_null_order_id", status: "ALERT", externalId: "ext-1", tableId: "t-hot", result: "5 nulls found" },
          { id: "qc-2", name: "row_count", status: "WARNING", externalId: "ext-2", tableId: "t-hot" },
          { id: "qc-3", name: "freshness", status: "SUCCESS", externalId: "ext-3", tableId: "t-hot" },
        ],
      ],
      [
        "t-cold",
        [
          { id: "qc-4", name: "not_null_id", status: "ALERT", externalId: "ext-4", tableId: "t-cold" },
        ],
      ],
      [
        "t-clean",
        [
          { id: "qc-5", name: "unique_key", status: "SUCCESS", externalId: "ext-5", tableId: "t-clean" },
        ],
      ],
    ]);

    const lineage = new Map<string, MockLineageEdge[]>([
      [
        "t-hot",
        [
          { id: "l-1", parentTableId: "t-upstream-1", childTableId: "t-hot" },
        ],
      ],
    ]);

    const upstreamDetails = new Map<string, MockTable>([
      [
        "t-upstream-1",
        {
          id: "t-upstream-1",
          name: "RAW_ORDERS",
          ownerEntities: [
            { id: "o3", userId: "u3", user: { id: "u3", email: "carol@co.io", fullName: "Carol" } },
          ],
          teamOwnerEntities: [],
        },
      ],
    ]);

    const client = makeRouter({
      tablesByScope: () => tables,
      qualityChecksByTableId: checks,
      lineageByChildTableId: lineage,
      upstreamTableDetails: upstreamDetails,
    });

    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({ schemaId: "sch" });
    const out = parseResult(res);

    // Stats
    const stats = out.stats as Record<string, number>;
    expect(stats.tablesInScope).toBe(3);
    expect(stats.tablesWithFailures).toBe(2);
    expect(stats.totalFailingChecks).toBe(3);
    expect(stats.alertCount).toBe(2);
    expect(stats.warningCount).toBe(1);

    // Ranked tables: t-hot should be first (higher popularity + more failures)
    const ranked = out.rankedTables as Array<Record<string, unknown>>;
    expect(ranked).toHaveLength(2);
    expect(ranked[0].id).toBe("t-hot");
    expect(ranked[0].failureCount).toBe(2);
    expect(ranked[0].alertCount).toBe(1);
    expect(ranked[0].warningCount).toBe(1);
    expect(ranked[1].id).toBe("t-cold");
    expect(ranked[1].failureCount).toBe(1);

    // t-hot has upstream lineage
    const hotUpstream = ranked[0].upstreamSources as Array<
      Record<string, unknown>
    >;
    expect(hotUpstream).toHaveLength(1);
    expect(hotUpstream[0].id).toBe("t-upstream-1");
    expect(hotUpstream[0].name).toBe("RAW_ORDERS");

    // t-clean should NOT be in the results (all checks passed)
    expect(ranked.find((r) => r.id === "t-clean")).toBeUndefined();

    // Triage queue: grouped by owner
    const queue = out.triageQueue as Array<Record<string, unknown>>;
    expect(queue.length).toBeGreaterThanOrEqual(2);
    // Alice owns t-hot (2 failures), unowned group has t-cold (1 failure)
    const aliceGroup = queue.find((g) => g.ownerType === "user");
    const unownedGroup = queue.find((g) => g.ownerType === "unowned");
    expect(aliceGroup).toBeDefined();
    expect(aliceGroup!.totalFailures).toBe(2);
    expect(unownedGroup).toBeDefined();
    expect(unownedGroup!.totalFailures).toBe(1);
  });

  it("filters out SUCCESS checks and only includes ALERT/WARNING", async () => {
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "T1",
        popularity: 0.5,
        ownerEntities: [{ id: "o1", userId: "u1", user: { id: "u1", email: "a@co.io", fullName: "A" } }],
      },
    ];
    const checks = new Map<string, MockQualityCheck[]>([
      [
        "t-1",
        [
          { id: "qc-ok-1", name: "check_a", status: "SUCCESS", externalId: "e1", tableId: "t-1" },
          { id: "qc-ok-2", name: "check_b", status: "SUCCESS", externalId: "e2", tableId: "t-1" },
          { id: "qc-fail", name: "check_c", status: "ALERT", externalId: "e3", tableId: "t-1" },
        ],
      ],
    ]);
    const client = makeRouter({
      tablesByScope: () => tables,
      qualityChecksByTableId: checks,
    });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({ schemaId: "sch" });
    const out = parseResult(res);
    const ranked = out.rankedTables as Array<Record<string, unknown>>;
    expect(ranked).toHaveLength(1);
    expect(ranked[0].failureCount).toBe(1);
    const checksOut = ranked[0].checks as Array<Record<string, unknown>>;
    expect(checksOut).toHaveLength(1);
    expect(checksOut[0].status).toBe("ALERT");
  });
});

describe("catalog_triage_quality_failures — capacity gate", () => {
  it("refuses when failing checks exceed the 500 cap", async () => {
    // 10 tables, each with 60 ALERT checks = 600 total failing
    const tables: MockTable[] = Array.from({ length: 10 }, (_, i) => ({
      id: `t-${i}`,
      name: `T${i}`,
      popularity: 0.5,
      ownerEntities: [],
    }));
    const checks = new Map<string, MockQualityCheck[]>();
    for (const t of tables) {
      checks.set(
        t.id,
        Array.from({ length: 60 }, (_, j) => ({
          id: `qc-${t.id}-${j}`,
          name: `check_${j}`,
          status: "ALERT" as const,
          externalId: `ext-${t.id}-${j}`,
          tableId: t.id,
        }))
      );
    }
    const client = makeRouter({
      tablesByScope: () => tables,
      qualityChecksByTableId: checks,
    });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({ schemaId: "sch" });
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/600 failing quality checks/);
    expect(out.error).toMatch(/500-check cap/);
  });
});

describe("catalog_triage_quality_failures — owner grouping", () => {
  it("groups tables by primary owner and sorts groups by total failures desc", async () => {
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "T1",
        popularity: 0.5,
        ownerEntities: [{ id: "o1", userId: "u1", user: { id: "u1", email: "alice@co.io", fullName: "Alice" } }],
        teamOwnerEntities: [],
      },
      {
        id: "t-2",
        name: "T2",
        popularity: 0.5,
        ownerEntities: [{ id: "o1", userId: "u1", user: { id: "u1", email: "alice@co.io", fullName: "Alice" } }],
        teamOwnerEntities: [],
      },
      {
        id: "t-3",
        name: "T3",
        popularity: 0.5,
        ownerEntities: [],
        teamOwnerEntities: [{ id: "to1", teamId: "team-1", team: { id: "team-1", name: "Data Team" } }],
      },
    ];
    const checks = new Map<string, MockQualityCheck[]>([
      ["t-1", [{ id: "qc-1", name: "c1", status: "ALERT" as const, externalId: "e1", tableId: "t-1" }]],
      ["t-2", [{ id: "qc-2", name: "c2", status: "ALERT" as const, externalId: "e2", tableId: "t-2" }]],
      ["t-3", [
        { id: "qc-3", name: "c3", status: "ALERT" as const, externalId: "e3", tableId: "t-3" },
        { id: "qc-4", name: "c4", status: "WARNING" as const, externalId: "e4", tableId: "t-3" },
        { id: "qc-5", name: "c5", status: "ALERT" as const, externalId: "e5", tableId: "t-3" },
      ]],
    ]);
    const client = makeRouter({
      tablesByScope: () => tables,
      qualityChecksByTableId: checks,
    });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({ schemaId: "sch" });
    const out = parseResult(res);

    const queue = out.triageQueue as Array<Record<string, unknown>>;
    // Data Team has 3 failures, Alice has 2
    expect(queue[0].ownerType).toBe("team");
    expect(queue[0].totalFailures).toBe(3);
    expect(queue[1].ownerType).toBe("user");
    expect(queue[1].totalFailures).toBe(2);
    expect((queue[1].tables as unknown[]).length).toBe(2);
  });
});
