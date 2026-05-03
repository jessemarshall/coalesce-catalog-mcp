import { describe, it, expect } from "vitest";
import { defineAssessQualityFailureDashboardImpact } from "../../src/workflows/assess-quality-failure-dashboard-impact.js";
import {
  GET_DATA_QUALITIES,
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_LINEAGES,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
  isError?: boolean;
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockQualityCheck {
  id: string;
  name: string;
  status: string;
  result?: string | null;
  externalId?: string;
  runAt?: string | null;
  url?: string | null;
  tableId: string;
}

interface MockTable {
  id: string;
  name?: string | null;
  numberOfQueries?: number | null;
  schema?: { name?: string } | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
}

interface MockDashboard {
  id: string;
  name?: string | null;
  popularity?: number | null;
  folderPath?: string | null;
  source?: { name?: string | null } | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
  tagEntities?: Array<Record<string, unknown>>;
}

interface RouterOpts {
  checks: MockQualityCheck[];
  tables?: Map<string, MockTable>;
  dashboards?: Map<string, MockDashboard>;
  // parent tableId -> { childTables: string[]; childDashboards: string[] }
  downstreamByTable?: Map<
    string,
    { childTables?: string[]; childDashboards?: string[] }
  >;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_DATA_QUALITIES) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = opts.checks.slice(
        start,
        start + vars.pagination.nbPerPage
      );
      return {
        getDataQualities: {
          totalCount: opts.checks.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice.map((c) => ({
            id: c.id,
            name: c.name,
            status: c.status,
            result: c.result ?? null,
            externalId: c.externalId ?? `ext-${c.id}`,
            runAt: c.runAt ?? null,
            url: c.url ?? null,
            tableId: c.tableId,
          })),
        },
      };
    }
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as { scope?: { ids?: string[] } };
      const ids = vars.scope?.ids ?? [];
      const rows = ids
        .map((id) => opts.tables?.get(id))
        .filter((t): t is MockTable => !!t);
      return {
        getTables: {
          totalCount: rows.length,
          nbPerPage: ids.length || 1,
          page: 0,
          data: rows,
        },
      };
    }
    if (document === GET_DASHBOARDS_DETAIL_BATCH) {
      const vars = variables as { scope?: { ids?: string[] } };
      const ids = vars.scope?.ids ?? [];
      const rows = ids
        .map((id) => opts.dashboards?.get(id))
        .filter((t): t is MockDashboard => !!t);
      return {
        getDashboards: {
          totalCount: rows.length,
          nbPerPage: ids.length || 1,
          page: 0,
          data: rows,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: { parentTableId?: string };
        pagination: { nbPerPage: number; page: number };
      };
      const parent = vars.scope?.parentTableId ?? "";
      const downstream = opts.downstreamByTable?.get(parent) ?? {};
      const tableEdges = (downstream.childTables ?? []).map((childTableId) => ({
        parentTableId: parent,
        childTableId,
      }));
      const dashEdges = (downstream.childDashboards ?? []).map(
        (childDashboardId) => ({
          parentTableId: parent,
          childDashboardId,
        })
      );
      const allEdges = [...tableEdges, ...dashEdges];
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = allEdges.slice(start, start + vars.pagination.nbPerPage);
      return {
        getLineages: {
          totalCount: allEdges.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_assess_quality_failure_dashboard_impact — empty paths", () => {
  it("returns empty triage and impact when no failing checks exist", async () => {
    const client = makeRouter({ checks: [] });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const out = parseResult(await tool.handler({}));
    expect(out.summary).toEqual({
      failingChecks: 0,
      affectedTables: 0,
      affectedDashboards: 0,
      intermediateTables: 0,
    });
    expect(out.triageQueue).toEqual([]);
    expect(out.dashboardImpact).toEqual([]);
  });

  it("returns the triage queue but empty dashboardImpact when failing tables have no downstream dashboards", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "n", status: "ALERT", tableId: "t1" }],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 5,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      downstreamByTable: new Map([
        ["t1", { childTables: [], childDashboards: [] }],
      ]),
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const out = parseResult(await tool.handler({}));
    expect((out.summary as Record<string, unknown>).affectedDashboards).toBe(0);
    expect((out.triageQueue as unknown[]).length).toBe(1);
    expect(out.dashboardImpact).toEqual([]);
  });
});

describe("catalog_assess_quality_failure_dashboard_impact — capacity gates", () => {
  it("refuses with an actionable message when failing checks exceed maxFailingChecks", async () => {
    const checks: MockQualityCheck[] = Array.from({ length: 10 }, (_, i) => ({
      id: `q${i}`,
      name: `c${i}`,
      status: "ALERT",
      tableId: "t1",
    }));
    const client = makeRouter({ checks });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const res = await tool.handler({ maxFailingChecks: 5 });
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/exceed the 5-check/);
  });

  it("refuses when reached dashboards exceed maxAffectedDashboards", async () => {
    // One failing table with 3 downstream dashboards, cap at 2.
    const dashIds = ["d1", "d2", "d3"];
    const client = makeRouter({
      checks: [{ id: "q1", name: "n", status: "ALERT", tableId: "t1" }],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 1,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      downstreamByTable: new Map([
        ["t1", { childTables: [], childDashboards: dashIds }],
      ]),
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const res = await tool.handler({ maxAffectedDashboards: 2 });
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/Reached more than 2 dashboards/);
  });
});

describe("catalog_assess_quality_failure_dashboard_impact — single hop", () => {
  it("aggregates failing tables that reach the same dashboard at depth 1", async () => {
    const client = makeRouter({
      checks: [
        { id: "q1", name: "n1", status: "ALERT", tableId: "t1" },
        { id: "q2", name: "n2", status: "WARNING", tableId: "t1" },
        { id: "q3", name: "n3", status: "ALERT", tableId: "t2" },
      ],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
        [
          "t2",
          {
            id: "t2",
            name: "T2",
            numberOfQueries: 20,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      dashboards: new Map([
        [
          "d1",
          {
            id: "d1",
            name: "Sales Overview",
            popularity: 100,
            folderPath: "Sales/Reports",
            source: { name: "Tableau" },
            ownerEntities: [],
            teamOwnerEntities: [],
            tagEntities: [],
          },
        ],
      ]),
      downstreamByTable: new Map([
        ["t1", { childDashboards: ["d1"] }],
        ["t2", { childDashboards: ["d1"] }],
      ]),
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const out = parseResult(await tool.handler({}));
    expect((out.summary as Record<string, unknown>).affectedDashboards).toBe(1);
    const impact = out.dashboardImpact as Array<Record<string, unknown>>;
    expect(impact).toHaveLength(1);
    expect(impact[0].dashboardId).toBe("d1");
    expect(impact[0].dashboardPath).toBe("Tableau/Sales/Reports/Sales Overview");
    const affected = impact[0].affectedByFailingTables as Array<
      Record<string, unknown>
    >;
    expect(affected).toHaveLength(2);
    // Aggregated failure count: 2 from t1 + 1 from t2 = 3
    expect(impact[0].totalFailureCount).toBe(3);
    // popularity 100 * totalFailureCount 3 * (1 + criticalityScore 0) = 300
    expect(impact[0].blastRadiusScore).toBe(300);
  });
});

describe("catalog_assess_quality_failure_dashboard_impact — multi-hop", () => {
  it("traverses through intermediate tables and records depth from each source", async () => {
    // Topology:  t1(failing) -> t-mid -> d1
    //            t2(failing) -> d1                (reaches d1 at depth 1)
    const client = makeRouter({
      checks: [
        { id: "q1", name: "n", status: "ALERT", tableId: "t1" },
        { id: "q2", name: "n", status: "ALERT", tableId: "t2" },
      ],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
        [
          "t2",
          {
            id: "t2",
            name: "T2",
            numberOfQueries: 5,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      dashboards: new Map([
        [
          "d1",
          {
            id: "d1",
            name: "D1",
            popularity: 50,
            ownerEntities: [],
            teamOwnerEntities: [],
            tagEntities: [],
          },
        ],
      ]),
      downstreamByTable: new Map([
        ["t1", { childTables: ["t-mid"] }],
        ["t-mid", { childDashboards: ["d1"] }],
        ["t2", { childDashboards: ["d1"] }],
      ]),
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const out = parseResult(await tool.handler({ maxDownstreamDepth: 2 }));
    const impact = out.dashboardImpact as Array<Record<string, unknown>>;
    const affected = impact[0].affectedByFailingTables as Array<
      Record<string, unknown>
    >;
    // Sorted by depth ASC, failureCount DESC. t2 reaches at depth 1 (closer),
    // t1 reaches via t-mid at depth 2.
    expect(affected[0].tableId).toBe("t2");
    expect(affected[0].depth).toBe(1);
    expect(affected[1].tableId).toBe("t1");
    expect(affected[1].depth).toBe(2);
    expect((out.summary as Record<string, unknown>).intermediateTables).toBe(1);
  });

  it("respects maxDownstreamDepth and excludes dashboards beyond the limit", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "n", status: "ALERT", tableId: "t1" }],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 1,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      dashboards: new Map([
        [
          "d-far",
          {
            id: "d-far",
            name: "Far",
            popularity: 1,
            ownerEntities: [],
            teamOwnerEntities: [],
            tagEntities: [],
          },
        ],
      ]),
      downstreamByTable: new Map([
        ["t1", { childTables: ["t-mid"] }],
        ["t-mid", { childDashboards: ["d-far"] }],
      ]),
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const out = parseResult(await tool.handler({ maxDownstreamDepth: 1 }));
    expect((out.summary as Record<string, unknown>).affectedDashboards).toBe(0);
  });
});

describe("catalog_assess_quality_failure_dashboard_impact — criticality + ranking", () => {
  it("boosts blastRadiusScore by criticality tag matches (case-insensitive substring)", async () => {
    const client = makeRouter({
      checks: [
        { id: "q1", name: "n", status: "ALERT", tableId: "t1" },
        { id: "q2", name: "n", status: "ALERT", tableId: "t2" },
      ],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
        [
          "t2",
          {
            id: "t2",
            name: "T2",
            numberOfQueries: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      dashboards: new Map([
        [
          "d-crit",
          {
            id: "d-crit",
            name: "Critical Dashboard",
            popularity: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
            tagEntities: [
              { tag: { id: "tag-1", label: "Production" } },
              { tag: { id: "tag-2", label: "TIER1" } },
            ],
          },
        ],
        [
          "d-plain",
          {
            id: "d-plain",
            name: "Plain Dashboard",
            popularity: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
            tagEntities: [{ tag: { id: "tag-3", label: "ad-hoc" } }],
          },
        ],
      ]),
      downstreamByTable: new Map([
        ["t1", { childDashboards: ["d-crit"] }],
        ["t2", { childDashboards: ["d-plain"] }],
      ]),
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const out = parseResult(await tool.handler({}));
    const impact = out.dashboardImpact as Array<Record<string, unknown>>;
    // d-crit: popularity 10 * failureCount 1 * (1 + criticalityScore 2) = 30
    // d-plain: popularity 10 * failureCount 1 * (1 + 0) = 10
    expect(impact[0].dashboardId).toBe("d-crit");
    expect(impact[0].criticalityScore).toBe(2);
    expect(impact[0].blastRadiusScore).toBe(30);
    expect(impact[1].dashboardId).toBe("d-plain");
    expect(impact[1].criticalityScore).toBe(0);
    expect(impact[1].blastRadiusScore).toBe(10);
  });

  it("treats empty criticalTagLabels as 'no boost' — ranks purely by popularity * failureCount", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "n", status: "ALERT", tableId: "t1" }],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 1,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      dashboards: new Map([
        [
          "d1",
          {
            id: "d1",
            name: "D1",
            popularity: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
            tagEntities: [{ tag: { id: "t", label: "production" } }],
          },
        ],
      ]),
      downstreamByTable: new Map([
        ["t1", { childDashboards: ["d1"] }],
      ]),
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const out = parseResult(await tool.handler({ criticalTagLabels: [] }));
    const impact = out.dashboardImpact as Array<Record<string, unknown>>;
    expect(impact[0].criticalityScore).toBe(0);
    expect(impact[0].blastRadiusScore).toBe(10);
  });
});

describe("catalog_assess_quality_failure_dashboard_impact — pagination ceiling", () => {
  it("refuses rather than silently truncating when quality-check pagination exceeds the per-call ceiling", async () => {
    // Simulate a workspace where every page is full and totalCount keeps
    // climbing past the 50-page * 100-row ceiling. The mock returns full
    // pages with a totalCount that never satisfies the page-fetched check,
    // forcing the loop to exhaust QUALITY_MAX_PAGES.
    const PAGE_SIZE = 100;
    const TOTAL_REPORTED = 99_999; // far above 50 * 100 = 5000
    const client = makeMockClient((document, variables) => {
      if (document === GET_DATA_QUALITIES) {
        const vars = variables as {
          pagination: { nbPerPage: number; page: number };
        };
        const data = Array.from({ length: PAGE_SIZE }, (_, i) => ({
          id: `q-${vars.pagination.page}-${i}`,
          name: `c-${vars.pagination.page}-${i}`,
          status: "SUCCESS",
          result: null,
          externalId: `ext-${vars.pagination.page}-${i}`,
          runAt: null,
          url: null,
          tableId: "t-misc",
        }));
        return {
          getDataQualities: {
            totalCount: TOTAL_REPORTED,
            nbPerPage: vars.pagination.nbPerPage,
            page: vars.pagination.page,
            data,
          },
        };
      }
      throw new Error(`unexpected document: ${document.slice(0, 60)}`);
    });
    const tool = defineAssessQualityFailureDashboardImpact(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/Quality check pagination exceeded/);
    expect(msg).toMatch(/Refusing to emit a partial dashboard-impact report/);
  });
});
