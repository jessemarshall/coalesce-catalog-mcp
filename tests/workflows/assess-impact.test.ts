import { describe, it, expect } from "vitest";
import { defineAssessImpact } from "../../src/workflows/assess-impact.js";
import {
  GET_TABLE_DETAIL,
  GET_DASHBOARD_DETAIL,
  GET_LINEAGES,
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_DATA_QUALITIES,
} from "../../src/catalog/operations.js";
import { CatalogGraphQLError } from "../../src/client.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface LineageEdge {
  childTableId?: string | null;
  childDashboardId?: string | null;
}

interface DownstreamAssetMock {
  id: string;
  name?: string;
  popularity?: number;
  isDeprecated?: boolean;
  isVerified?: boolean;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
}

interface RouterRoutes {
  tableDetail?: { id: string; row: Record<string, unknown> | null };
  dashboardDetail?: { id: string; row: Record<string, unknown> | null };
  // Returns the downstream edges for a given parent (table or dashboard).
  lineageByParent?: Map<string, LineageEdge[] | Error>;
  tableSummary?: Map<string, DownstreamAssetMock>;
  dashboardSummary?: Map<string, DownstreamAssetMock>;
  qualityForTable?: { tableId: string; rows: Array<{ status: string }>; totalCount?: number };
}

function makeRouter(routes: RouterRoutes) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLE_DETAIL) {
      const ids = (variables as { ids: string[] }).ids;
      const row =
        routes.tableDetail && ids.includes(routes.tableDetail.id)
          ? routes.tableDetail.row
          : null;
      return { getTables: { data: row ? [row] : [] } };
    }
    if (document === GET_DASHBOARD_DETAIL) {
      const ids = (variables as { ids: string[] }).ids;
      const row =
        routes.dashboardDetail && ids.includes(routes.dashboardDetail.id)
          ? routes.dashboardDetail.row
          : null;
      return { getDashboards: { data: row ? [row] : [] } };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: { parentTableId?: string; parentDashboardId?: string };
      };
      const parentId = vars.scope?.parentTableId ?? vars.scope?.parentDashboardId;
      if (!parentId) return { getLineages: { totalCount: 0, data: [] } };
      const edges = routes.lineageByParent?.get(parentId);
      if (edges instanceof Error) throw edges;
      const rows = edges ?? [];
      return { getLineages: { totalCount: rows.length, data: rows } };
    }
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as { scope?: { ids?: string[] } };
      const ids = vars.scope?.ids ?? [];
      const data = ids
        .map((id) => routes.tableSummary?.get(id))
        .filter((r): r is NonNullable<typeof r> => Boolean(r));
      return { getTables: { totalCount: data.length, data } };
    }
    if (document === GET_DASHBOARDS_DETAIL_BATCH) {
      const vars = variables as { scope?: { ids?: string[] } };
      const ids = vars.scope?.ids ?? [];
      const data = ids
        .map((id) => routes.dashboardSummary?.get(id))
        .filter((r): r is NonNullable<typeof r> => Boolean(r));
      return { getDashboards: { totalCount: data.length, data } };
    }
    if (document === GET_DATA_QUALITIES) {
      const vars = variables as { scope?: { tableId?: string } };
      if (vars.scope?.tableId !== routes.qualityForTable?.tableId) {
        return { getDataQualities: { totalCount: 0, data: [] } };
      }
      return {
        getDataQualities: {
          totalCount:
            routes.qualityForTable.totalCount ?? routes.qualityForTable.rows.length,
          data: routes.qualityForTable.rows,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

const TABLE_ROW = {
  id: "t-start",
  name: "ORDERS",
  description: "the orders table",
  popularity: 0.9,
  isVerified: true,
  isDeprecated: false,
  tableType: "TABLE",
  numberOfQueries: 1000,
  // Always-recent timestamp computed at test time so the recency band stays stable.
  lastQueriedAt: Date.now() - 2 * 24 * 60 * 60 * 1000, // 2 days ago
  ownerEntities: [{ id: "o1", userId: "u-1", user: { fullName: "Alice" } }],
  teamOwnerEntities: [
    { id: "to1", teamId: "tm-1", team: { name: "Sales Analytics" } },
  ],
  tagEntities: [{ id: "te1", tag: { id: "tg-1", label: "PII" } }],
};

describe("catalog_assess_impact — happy path", () => {
  it("returns asset + ownership + tags + downstream + severity at depth 1", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([
        [
          "t-start",
          [
            { childTableId: "t-d1", childDashboardId: null },
            { childTableId: null, childDashboardId: "d-d1" },
            { childTableId: "t-d2", childDashboardId: null },
          ],
        ],
      ]),
      tableSummary: new Map([
        ["t-d1", { id: "t-d1", name: "DAILY_ORDERS", popularity: 0.5, isDeprecated: false, isVerified: false }],
        ["t-d2", { id: "t-d2", name: "ARCHIVED_ORDERS", popularity: 0.1, isDeprecated: true, isVerified: false }],
      ]),
      dashboardSummary: new Map([
        ["d-d1", { id: "d-d1", name: "Sales Overview", popularity: 0.8, isDeprecated: false, isVerified: true }],
      ]),
      qualityForTable: {
        tableId: "t-start",
        rows: [{ status: "SUCCESS" }, { status: "SUCCESS" }, { status: "ALERT" }],
      },
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
    });

    expect(res.isError).toBeUndefined();
    const out = parseResult(res);

    expect(out.asset).toMatchObject({
      id: "t-start",
      kind: "TABLE",
      name: "ORDERS",
      numberOfQueries: 1000,
    });
    expect(out.ownership).toMatchObject({
      users: [{ userId: "u-1" }],
      teams: [{ teamId: "tm-1" }],
    });
    expect(out.tags).toEqual([
      { id: "te1", tag: { id: "tg-1", label: "PII" } },
    ]);

    const downstream = out.downstream as Record<string, unknown>;
    expect(downstream).toMatchObject({
      maxDepthRequested: 1,
      totalCount: 3,
      tableCount: 2,
      dashboardCount: 1,
      deprecatedCount: 1,
    });
    const assets = downstream.assets as Array<{
      id: string;
      depth: number;
      popularity: number | null;
    }>;
    expect(assets).toHaveLength(3);
    // All depth-1.
    expect(assets.every((a) => a.depth === 1)).toBe(true);
    // Sorted by popularity DESC within the same depth.
    expect(assets[0].popularity).toBeGreaterThanOrEqual(assets[1].popularity ?? 0);

    expect(out.qualityChecks).toEqual({
      totalCount: 3,
      byStatus: { SUCCESS: 2, WARNING: 0, ALERT: 1, OTHER: 0 },
      byStatusSampledFrom: 3,
      byStatusComplete: true,
    });

    const sev = out.severity as Record<string, unknown>;
    expect(sev.bucket).toBe("medium");
    expect(typeof sev.score).toBe("number");
    const rationale = sev.rationale as Array<{ component: string; points: number }>;
    expect(rationale.map((r) => r.component)).toEqual([
      "downstream_impact",
      "query_volume",
      "query_recency",
    ]);
    // 2-day-old lastQueriedAt → full recency band.
    expect(rationale.find((r) => r.component === "query_recency")?.points).toBe(20);
  });

  it("DASHBOARD path skips quality + reports n/a for query-volume signals", async () => {
    const client = makeRouter({
      dashboardDetail: {
        id: "d-start",
        row: {
          id: "d-start",
          name: "Exec Dashboard",
          popularity: 0.4,
          isVerified: false,
          isDeprecated: false,
          type: "LOOK",
          folderPath: "/Sales",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      },
      lineageByParent: new Map([["d-start", []]]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({ assetKind: "DASHBOARD", assetId: "d-start" });

    const out = parseResult(res);
    expect((out.asset as Record<string, unknown>).type).toBe("LOOK");
    expect(out.qualityChecks).toBeNull();
    const rationale = (out.severity as Record<string, unknown>).rationale as Array<{
      component: string;
      detail: string;
      points: number;
    }>;
    expect(rationale.find((r) => r.component === "query_volume")?.points).toBe(0);
    expect(rationale.find((r) => r.component === "query_volume")?.detail).toMatch(
      /DASHBOARD/
    );
  });
});

describe("catalog_assess_impact — depth-2 traversal", () => {
  it("reaches grandchildren at depth 2 and tags each asset's depth", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([
        ["t-start", [{ childTableId: "t-mid", childDashboardId: null }]],
        ["t-mid", [{ childTableId: null, childDashboardId: "d-leaf" }]],
      ]),
      tableSummary: new Map([
        ["t-mid", { id: "t-mid", name: "MID", popularity: 0.6 }],
      ]),
      dashboardSummary: new Map([
        ["d-leaf", { id: "d-leaf", name: "LEAF", popularity: 0.3 }],
      ]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      maxDepth: 2,
      includeQualityChecks: false,
    });

    const out = parseResult(res);
    const downstream = out.downstream as Record<string, unknown>;
    expect(downstream.totalCount).toBe(2);
    const assets = downstream.assets as Array<{ id: string; depth: number }>;
    const byId = new Map(assets.map((a) => [a.id, a]));
    expect(byId.get("t-mid")?.depth).toBe(1);
    expect(byId.get("d-leaf")?.depth).toBe(2);
  });

  it("dedupes nodes encountered via multiple paths (cycle / diamond)", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([
        // Diamond: t-start → {t-a, t-b}, both → t-shared
        [
          "t-start",
          [
            { childTableId: "t-a", childDashboardId: null },
            { childTableId: "t-b", childDashboardId: null },
          ],
        ],
        ["t-a", [{ childTableId: "t-shared", childDashboardId: null }]],
        ["t-b", [{ childTableId: "t-shared", childDashboardId: null }]],
        // Cycle: t-shared loops back to t-start (must not infinite-loop or recount).
        ["t-shared", [{ childTableId: "t-start", childDashboardId: null }]],
      ]),
      tableSummary: new Map([
        ["t-a", { id: "t-a", name: "A", popularity: 0.5 }],
        ["t-b", { id: "t-b", name: "B", popularity: 0.5 }],
        ["t-shared", { id: "t-shared", name: "SHARED", popularity: 0.7 }],
      ]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      maxDepth: 3,
      includeQualityChecks: false,
    });

    const out = parseResult(res);
    const downstream = out.downstream as Record<string, unknown>;
    // 3 unique downstream: t-a, t-b, t-shared. Cycle to t-start does NOT add it.
    expect(downstream.totalCount).toBe(3);
    const ids = (downstream.assets as Array<{ id: string }>).map((a) => a.id);
    expect(ids.sort()).toEqual(["t-a", "t-b", "t-shared"]);
  });
});

describe("catalog_assess_impact — completeness contract (refusal, not truncation)", () => {
  it("refuses depth 2 when the depth-1 frontier exceeds 2000 nodes", async () => {
    const wideChildren: LineageEdge[] = Array.from({ length: 2001 }, (_, i) => ({
      childTableId: `t-${i}`,
      childDashboardId: null,
    }));
    const summary = new Map(
      Array.from({ length: 2001 }, (_, i) => [
        `t-${i}`,
        { id: `t-${i}`, name: `T${i}`, popularity: 0.1 },
      ])
    );
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([["t-start", wideChildren]]),
      tableSummary: summary,
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      maxDepth: 2,
      includeQualityChecks: false,
    });

    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/Graph too wide/);
    expect(out.error).toMatch(/2001/);
    expect(out.error).toMatch(/2000-node cap/);
  });

  it("refuses depth 3 when the depth-2 frontier exceeds 500 nodes", async () => {
    // Single depth-1 child, but it fans out to 501 depth-2 nodes.
    const wideGrandchildren: LineageEdge[] = Array.from(
      { length: 501 },
      (_, i) => ({ childTableId: `g-${i}`, childDashboardId: null })
    );
    const summary = new Map([
      ["t-mid", { id: "t-mid", name: "MID", popularity: 0.5 }],
      ...Array.from({ length: 501 }, (_, i) => [
        `g-${i}`,
        { id: `g-${i}`, name: `G${i}`, popularity: 0.1 },
      ]) as Array<[string, { id: string; name: string; popularity: number }]>,
    ]);
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([
        ["t-start", [{ childTableId: "t-mid", childDashboardId: null }]],
        ["t-mid", wideGrandchildren],
      ]),
      tableSummary: summary,
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      maxDepth: 3,
      includeQualityChecks: false,
    });

    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/depth 3/);
    expect(out.error).toMatch(/501/);
    expect(out.error).toMatch(/500-node cap/);
  });

  it("never refuses at depth 1 even with thousands of immediate children", async () => {
    const wideChildren: LineageEdge[] = Array.from({ length: 3000 }, (_, i) => ({
      childTableId: `t-${i}`,
      childDashboardId: null,
    }));
    const summary = new Map(
      Array.from({ length: 3000 }, (_, i) => [
        `t-${i}`,
        { id: `t-${i}`, name: `T${i}`, popularity: 0 },
      ])
    );
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([["t-start", wideChildren]]),
      tableSummary: summary,
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      maxDepth: 1,
      includeQualityChecks: false,
    });

    expect(res.isError).toBeUndefined();
    const out = parseResult(res);
    expect((out.downstream as Record<string, unknown>).totalCount).toBe(3000);
  });
});

describe("catalog_assess_impact — completeness contract: pagination + missing-row refusal", () => {
  it("paginates per-node lineage when a single parent's downstream exceeds the page size", async () => {
    // 750 children off one node. Page size is 500, so this requires a second
    // page. Without pagination the cap check would think there are only 500
    // children, which is the whole bug we're fixing.
    const wide: LineageEdge[] = Array.from({ length: 750 }, (_, i) => ({
      childTableId: `t-${i}`,
      childDashboardId: null,
    }));
    const summary = new Map(
      Array.from({ length: 750 }, (_, i) => [
        `t-${i}`,
        { id: `t-${i}`, name: `T${i}`, popularity: 0.1 },
      ])
    );
    // We need the mock to return paginated results. Custom router below
    // (the default helper returns everything in one shot, which is exactly
    // what we want NOT to do here).
    let callsForT: Array<{ page: number; nbPerPage: number }> = [];
    const client = makeMockClient((document, variables) => {
      if (document === GET_TABLE_DETAIL) {
        return { getTables: { data: [TABLE_ROW] } };
      }
      if (document === GET_LINEAGES) {
        const vars = variables as {
          scope?: { parentTableId?: string };
          pagination: { nbPerPage: number; page: number };
        };
        if (vars.scope?.parentTableId === "t-start") {
          callsForT.push({ page: vars.pagination.page, nbPerPage: vars.pagination.nbPerPage });
          const start = vars.pagination.page * vars.pagination.nbPerPage;
          const slice = wide.slice(start, start + vars.pagination.nbPerPage);
          return {
            getLineages: { totalCount: wide.length, data: slice },
          };
        }
        return { getLineages: { totalCount: 0, data: [] } };
      }
      if (document === GET_TABLES_DETAIL_BATCH) {
        const vars = variables as { scope?: { ids?: string[] } };
        const ids = vars.scope?.ids ?? [];
        return {
          getTables: {
            totalCount: ids.length,
            data: ids
              .map((id) => summary.get(id))
              .filter((r): r is NonNullable<typeof r> => Boolean(r)),
          },
        };
      }
      return { getDataQualities: { totalCount: 0, data: [] } };
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      maxDepth: 1,
      includeQualityChecks: false,
    });

    expect(res.isError).toBeUndefined();
    const out = parseResult(res);
    expect((out.downstream as Record<string, unknown>).totalCount).toBe(750);
    // Two paginated calls for the lineage.
    expect(callsForT.length).toBe(2);
    expect(callsForT[0].page).toBe(0);
    expect(callsForT[1].page).toBe(1);
  });

  it("refuses with a clear error when enrichment returns no row for a known downstream id", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([
        [
          "t-start",
          [
            { childTableId: "t-known", childDashboardId: null },
            { childTableId: "t-vanished", childDashboardId: null },
          ],
        ],
      ]),
      // Only enrich one of the two — t-vanished is omitted from the response.
      tableSummary: new Map([
        ["t-known", { id: "t-known", name: "KNOWN", popularity: 0.5 }],
      ]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      includeQualityChecks: false,
    });

    const out = parseResult(res);
    // Error returned in the body, not silent partial report.
    expect(out.error).toMatch(/Detail enrichment returned no row/);
    expect(out.error).toMatch(/t-vanished/);
    expect(out.missingCount).toBe(1);
    // No partial downstream populated.
    expect(out.downstream).toBeUndefined();
  });
});

describe("catalog_assess_impact — error & not-found paths", () => {
  it("returns notFound when the starting asset detail is empty", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-missing", row: null },
      lineageByParent: new Map([["t-missing", []]]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({ assetKind: "TABLE", assetId: "t-missing" });

    expect(parseResult(res)).toEqual({
      notFound: true,
      assetKind: "TABLE",
      assetId: "t-missing",
    });
  });

  it("hard-fails (isError) when a lineage call rejects mid-traversal", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map<string, LineageEdge[] | Error>([
        ["t-start", new CatalogGraphQLError([{ message: "lineage backend down" }])],
      ]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
    });

    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/lineage backend down/);
  });
});

describe("catalog_assess_impact — downstream ownership enrichment", () => {
  it("populates teams + ownerCounts per downstream asset and aggregates distinct teams", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([
        [
          "t-start",
          [
            { childTableId: "t-d1", childDashboardId: null },
            { childTableId: "t-d2", childDashboardId: null },
            { childTableId: null, childDashboardId: "d-d1" },
            { childTableId: "t-orphan", childDashboardId: null },
          ],
        ],
      ]),
      tableSummary: new Map([
        [
          "t-d1",
          {
            id: "t-d1",
            name: "T_D1",
            popularity: 0.5,
            // Same team as the dashboard — should de-dupe.
            teamOwnerEntities: [
              { id: "to-1", teamId: "tm-sales", team: { name: "Sales" } },
            ],
            ownerEntities: [{ id: "o-1", userId: "u-1" }],
          },
        ],
        [
          "t-d2",
          {
            id: "t-d2",
            name: "T_D2",
            popularity: 0.4,
            teamOwnerEntities: [
              { id: "to-2", teamId: "tm-finance", team: { name: "Finance" } },
            ],
          },
        ],
        [
          "t-orphan",
          {
            id: "t-orphan",
            name: "T_ORPHAN",
            popularity: 0.1,
            // No owners.
          },
        ],
      ]),
      dashboardSummary: new Map([
        [
          "d-d1",
          {
            id: "d-d1",
            name: "D_D1",
            popularity: 0.7,
            // Same team as t-d1.
            teamOwnerEntities: [
              { id: "to-3", teamId: "tm-sales", team: { name: "Sales" } },
            ],
          },
        ],
      ]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      includeQualityChecks: false,
    });

    const out = parseResult(res);
    const downstream = out.downstream as Record<string, unknown>;
    expect(downstream.totalCount).toBe(4);
    expect(downstream.distinctOwnerTeamCount).toBe(2);
    expect(downstream.unownedCount).toBe(1);

    const assets = downstream.assets as Array<Record<string, unknown>>;
    const td1 = assets.find((a) => a.id === "t-d1") as Record<string, unknown>;
    expect(td1.ownerUserCount).toBe(1);
    expect(td1.ownerTeamCount).toBe(1);
    expect(td1.teams).toEqual([
      { id: "to-1", teamId: "tm-sales", name: "Sales" },
    ]);

    const orphan = assets.find((a) => a.id === "t-orphan") as Record<string, unknown>;
    expect(orphan.ownerUserCount).toBe(0);
    expect(orphan.ownerTeamCount).toBe(0);
    expect(orphan.teams).toEqual([]);
  });

  it("counts an asset as unowned only when both ownerEntities and teamOwnerEntities are empty", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-start", row: TABLE_ROW },
      lineageByParent: new Map([
        [
          "t-start",
          [
            { childTableId: "t-user-only", childDashboardId: null },
            { childTableId: "t-team-only", childDashboardId: null },
            { childTableId: "t-no-owners", childDashboardId: null },
          ],
        ],
      ]),
      tableSummary: new Map([
        [
          "t-user-only",
          {
            id: "t-user-only",
            name: "USER_ONLY",
            popularity: 0.3,
            ownerEntities: [{ id: "o-1", userId: "u-1" }],
          },
        ],
        [
          "t-team-only",
          {
            id: "t-team-only",
            name: "TEAM_ONLY",
            popularity: 0.3,
            teamOwnerEntities: [{ id: "to-1", teamId: "tm-1", team: { name: "T" } }],
          },
        ],
        [
          "t-no-owners",
          { id: "t-no-owners", name: "NO_OWNERS", popularity: 0.1 },
        ],
      ]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      includeQualityChecks: false,
    });
    const out = parseResult(res);
    expect((out.downstream as Record<string, unknown>).unownedCount).toBe(1);
  });
});

describe("catalog_assess_impact — severity rubric", () => {
  it("scores into the 'low' bucket for an unused leaf table with no downstream", async () => {
    const oldRow = {
      ...TABLE_ROW,
      numberOfQueries: 0,
      lastQueriedAt: null,
    };
    const client = makeRouter({
      tableDetail: { id: "t-start", row: oldRow },
      lineageByParent: new Map([["t-start", []]]),
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      includeQualityChecks: false,
    });

    const sev = parseResult(res).severity as { score: number; bucket: string };
    expect(sev.bucket).toBe("low");
    expect(sev.score).toBe(0);
  });

  it("scores into the 'high' bucket for a hot table with broad downstream", async () => {
    const hotRow = {
      ...TABLE_ROW,
      numberOfQueries: 100000,
      lastQueriedAt: Date.now() - 60 * 60 * 1000, // 1 hour ago
    };
    // 100 immediate consumers — log10(101)*30 ≈ 60 pts (saturates downstream)
    const wideChildren: LineageEdge[] = Array.from({ length: 100 }, (_, i) => ({
      childTableId: `t-${i}`,
      childDashboardId: null,
    }));
    const summary = new Map(
      Array.from({ length: 100 }, (_, i) => [
        `t-${i}`,
        { id: `t-${i}`, name: `T${i}`, popularity: 0.5 },
      ])
    );
    const client = makeRouter({
      tableDetail: { id: "t-start", row: hotRow },
      lineageByParent: new Map([["t-start", wideChildren]]),
      tableSummary: summary,
    });

    const tool = defineAssessImpact(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t-start",
      includeQualityChecks: false,
    });

    const sev = parseResult(res).severity as { score: number; bucket: string };
    expect(sev.bucket).toBe("high");
    expect(sev.score).toBeGreaterThanOrEqual(60);
  });
});
