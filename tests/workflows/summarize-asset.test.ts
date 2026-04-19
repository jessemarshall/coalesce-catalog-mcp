import { describe, it, expect } from "vitest";
import { defineSummarizeAsset } from "../../src/workflows/summarize-asset.js";
import {
  GET_TABLE_DETAIL,
  GET_DASHBOARD_DETAIL,
  GET_LINEAGES,
  GET_COLUMNS_SUMMARY,
  GET_DATA_QUALITIES,
} from "../../src/catalog/operations.js";
import { CatalogGraphQLError } from "../../src/client.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

// Routes responses (or errors) by GraphQL document so the test reads naturally
// regardless of Promise.allSettled scheduling.
function makeRouter(
  routes: Record<string, unknown | Error>,
  lineageRoutes?: { upstream: unknown | Error; downstream: unknown | Error }
) {
  let lineageCalls = 0;
  return makeMockClient((document, variables) => {
    if (document === GET_LINEAGES && lineageRoutes) {
      const vars = variables as { scope?: Record<string, unknown> };
      const isUpstream =
        vars.scope?.childTableId !== undefined ||
        vars.scope?.childDashboardId !== undefined;
      lineageCalls += 1;
      const r = isUpstream ? lineageRoutes.upstream : lineageRoutes.downstream;
      if (r instanceof Error) throw r;
      return r;
    }
    if (document in routes) {
      const r = routes[document];
      if (r instanceof Error) throw r;
      return r;
    }
    throw new Error(
      `unexpected document in test (lineageCalls=${lineageCalls}): ${document.slice(0, 60)}…`
    );
  });
}

const TABLE_ROW = {
  id: "t-1",
  name: "ORDERS",
  description: "the orders table",
  externalId: "snowflake.PROD.SALES.ORDERS",
  url: "https://catalog/example",
  popularity: 0.5,
  isVerified: true,
  isDeprecated: false,
  deletedAt: null,
  deprecatedAt: null,
  verifiedAt: "2026-01-01",
  createdAt: "2025-12-01",
  updatedAt: "2026-04-01",
  tableType: "TABLE",
  tableSize: 1234,
  numberOfQueries: 10,
  lastRefreshedAt: "2026-04-17",
  lastQueriedAt: "2026-04-18",
  transformationSource: "DBT",
  schema: "SALES",
  schemaId: "s-1",
  ownerEntities: [{ id: "o1", userId: "u-1", user: { name: "Alice" } }],
  teamOwnerEntities: [],
  tagEntities: [{ id: "te1", tag: { id: "tg-1", name: "PII" } }],
  externalLinks: [{ id: "el-1", url: "https://example" }],
};

const DASHBOARD_ROW = {
  id: "d-1",
  name: "Sales Overview",
  description: "exec dashboard",
  externalId: "looker.123",
  url: "https://looker/123",
  popularity: 0.7,
  isVerified: false,
  isDeprecated: false,
  deletedAt: null,
  deprecatedAt: null,
  verifiedAt: null,
  createdAt: "2025-11-01",
  updatedAt: "2026-04-01",
  type: "LOOK",
  folderPath: "/Sales",
  folderUrl: "https://looker/folder/sales",
  sourceId: "src-1",
  ownerEntities: [],
  teamOwnerEntities: [],
  tagEntities: [],
  externalLinks: [],
};

const okLineage = (count: number, sample: unknown[] = []) => ({
  getLineages: { totalCount: count, data: sample },
});

const okColumns = (count: number, sample: unknown[] = []) => ({
  getColumns: { totalCount: count, data: sample },
});

const okQuality = (count: number, sample: unknown[] = []) => ({
  getDataQualities: { totalCount: count, data: sample },
});

describe("catalog_summarize_asset — happy path", () => {
  it("returns a full TABLE summary stitched from all 5 sub-queries", async () => {
    const client = makeRouter(
      {
        [GET_TABLE_DETAIL]: { getTables: { data: [TABLE_ROW] } },
        [GET_COLUMNS_SUMMARY]: okColumns(3, [
          { id: "c-1", name: "ID" },
          { id: "c-2", name: "TOTAL" },
        ]),
        [GET_DATA_QUALITIES]: okQuality(0, []),
      },
      {
        upstream: okLineage(2, [{ id: "l-up-1" }]),
        downstream: okLineage(5, [{ id: "l-dn-1" }]),
      }
    );
    const tool = defineSummarizeAsset(client);

    const res = await tool.handler({ kind: "TABLE", id: "t-1" });

    expect(res.isError).toBeUndefined();
    const out = parseResult(res);
    expect(out).toMatchObject({
      kind: "TABLE",
      core: { id: "t-1", name: "ORDERS", schema: "SALES" },
      ownership: { users: [{ userId: "u-1" }], teams: [] },
      annotations: { tags: [{ tag: { name: "PII" } }] },
      lineage: {
        upstream: { totalCount: 2, returned: 1, hasMore: true },
        downstream: { totalCount: 5, returned: 1, hasMore: true },
      },
      columns: { totalCount: 3, returned: 2, hasMore: true },
      qualityChecks: { totalCount: 0, returned: 0, hasMore: false },
    });
    expect(client.calls).toHaveLength(5);
  });

  it("returns a DASHBOARD summary without firing columns/quality queries", async () => {
    const client = makeRouter(
      {
        [GET_DASHBOARD_DETAIL]: { getDashboards: { data: [DASHBOARD_ROW] } },
      },
      {
        upstream: okLineage(0, []),
        downstream: okLineage(0, []),
      }
    );
    const tool = defineSummarizeAsset(client);

    const res = await tool.handler({ kind: "DASHBOARD", id: "d-1" });

    const out = parseResult(res);
    expect(out).toMatchObject({
      kind: "DASHBOARD",
      core: { id: "d-1", name: "Sales Overview", type: "LOOK" },
    });
    expect(out.columns).toBeUndefined();
    expect(out.qualityChecks).toBeUndefined();
    // detail + 2 lineage queries; columns + quality skipped for dashboards
    expect(client.calls).toHaveLength(3);
  });
});

describe("catalog_summarize_asset — partial-failure behavior", () => {
  it("returns notFound when detail succeeds but the table does not exist", async () => {
    const client = makeRouter(
      {
        [GET_TABLE_DETAIL]: { getTables: { data: [] } },
        [GET_COLUMNS_SUMMARY]: okColumns(0, []),
        [GET_DATA_QUALITIES]: okQuality(0, []),
      },
      {
        upstream: okLineage(0, []),
        downstream: okLineage(0, []),
      }
    );
    const tool = defineSummarizeAsset(client);

    const res = await tool.handler({ kind: "TABLE", id: "missing" });

    expect(parseResult(res)).toEqual({ kind: "TABLE", id: "missing", notFound: true });
  });

  it("hard-fails (top-level error) when the detail query itself rejects", async () => {
    const client = makeRouter(
      {
        [GET_TABLE_DETAIL]: new CatalogGraphQLError([
          { message: "detail blew up" },
        ]),
        [GET_COLUMNS_SUMMARY]: okColumns(0, []),
        [GET_DATA_QUALITIES]: okQuality(0, []),
      },
      {
        upstream: okLineage(0, []),
        downstream: okLineage(0, []),
      }
    );
    const tool = defineSummarizeAsset(client);

    const res = await tool.handler({ kind: "TABLE", id: "t-1" });

    expect(parseResult(res)).toMatchObject({
      error: "Failed to fetch asset detail",
      detail: expect.stringMatching(/detail blew up/),
    });
  });

  it("partial failure: upstream lineage rejects, other 4 sections still populate", async () => {
    const client = makeRouter(
      {
        [GET_TABLE_DETAIL]: { getTables: { data: [TABLE_ROW] } },
        [GET_COLUMNS_SUMMARY]: okColumns(2, [{ id: "c-1" }, { id: "c-2" }]),
        [GET_DATA_QUALITIES]: okQuality(1, [{ id: "q-1" }]),
      },
      {
        upstream: new CatalogGraphQLError([{ message: "upstream timeout" }]),
        downstream: okLineage(3, [{ id: "l-1" }]),
      }
    );
    const tool = defineSummarizeAsset(client);

    const res = await tool.handler({ kind: "TABLE", id: "t-1" });
    const out = parseResult(res);

    // Other sections are intact.
    expect(out.core).toMatchObject({ id: "t-1", name: "ORDERS" });
    expect((out.lineage as Record<string, unknown>).downstream).toMatchObject({
      totalCount: 3,
      returned: 1,
    });
    expect(out.columns).toMatchObject({ totalCount: 2 });
    expect(out.qualityChecks).toMatchObject({ totalCount: 1 });
    // Failing section is surfaced inline, not at the top level.
    expect((out.lineage as Record<string, unknown>).upstream).toMatchObject({
      error: expect.stringMatching(/upstream timeout/),
    });
    expect(out.error).toBeUndefined();
  });

  it("partial failure: columns + quality both reject; lineage still rendered", async () => {
    const client = makeRouter(
      {
        [GET_TABLE_DETAIL]: { getTables: { data: [TABLE_ROW] } },
        [GET_COLUMNS_SUMMARY]: new Error("columns endpoint down"),
        [GET_DATA_QUALITIES]: new Error("quality endpoint down"),
      },
      {
        upstream: okLineage(1, [{ id: "u-1" }]),
        downstream: okLineage(1, [{ id: "d-1" }]),
      }
    );
    const tool = defineSummarizeAsset(client);

    const res = await tool.handler({ kind: "TABLE", id: "t-1" });
    const out = parseResult(res);

    expect(out.lineage).toMatchObject({
      upstream: { totalCount: 1, returned: 1 },
      downstream: { totalCount: 1, returned: 1 },
    });
    expect(out.columns).toMatchObject({
      error: expect.stringMatching(/columns endpoint down/),
    });
    expect(out.qualityChecks).toMatchObject({
      error: expect.stringMatching(/quality endpoint down/),
    });
  });

  it("renders skipped sections when a limit is set to 0", async () => {
    const client = makeRouter(
      {
        [GET_TABLE_DETAIL]: { getTables: { data: [TABLE_ROW] } },
      },
      {
        upstream: okLineage(0, []),
        downstream: okLineage(0, []),
      }
    );
    const tool = defineSummarizeAsset(client);

    const res = await tool.handler({
      kind: "TABLE",
      id: "t-1",
      columnsLimit: 0,
      qualityLimit: 0,
    });
    const out = parseResult(res);

    expect(out.columns).toEqual({ skipped: true });
    expect(out.qualityChecks).toEqual({ skipped: true });
    // Only detail + 2 lineage calls; columns + quality not requested.
    expect(client.calls).toHaveLength(3);
    expect(client.calls.some((c) => c.document === GET_COLUMNS_SUMMARY)).toBe(false);
    expect(client.calls.some((c) => c.document === GET_DATA_QUALITIES)).toBe(false);
  });
});

describe("catalog_summarize_asset — pagination flags", () => {
  it("hasMore=false when returned == totalCount on every section", async () => {
    const client = makeRouter(
      {
        [GET_TABLE_DETAIL]: { getTables: { data: [TABLE_ROW] } },
        [GET_COLUMNS_SUMMARY]: okColumns(2, [{ id: "c-1" }, { id: "c-2" }]),
        [GET_DATA_QUALITIES]: okQuality(1, [{ id: "q-1" }]),
      },
      {
        upstream: okLineage(1, [{ id: "u-1" }]),
        downstream: okLineage(2, [{ id: "d-1" }, { id: "d-2" }]),
      }
    );
    const tool = defineSummarizeAsset(client);

    const out = parseResult(await tool.handler({ kind: "TABLE", id: "t-1" }));

    const lineage = out.lineage as Record<string, Record<string, unknown>>;
    expect(lineage.upstream.hasMore).toBe(false);
    expect(lineage.downstream.hasMore).toBe(false);
    expect((out.columns as Record<string, unknown>).hasMore).toBe(false);
    expect((out.qualityChecks as Record<string, unknown>).hasMore).toBe(false);
  });
});
