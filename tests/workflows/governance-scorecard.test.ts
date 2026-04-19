import { describe, it, expect } from "vitest";
import { defineGovernanceScorecard } from "../../src/workflows/governance-scorecard.js";
import {
  GET_TABLES_DETAIL_BATCH,
  GET_COLUMNS_SUMMARY,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockTable {
  id: string;
  name: string;
  popularity?: number | null;
  description?: string | null;
  isVerified?: boolean;
  isDeprecated?: boolean;
  ownerEntities?: unknown[];
  teamOwnerEntities?: unknown[];
  tagEntities?: unknown[];
}

interface MockColumn {
  id: string;
  tableId: string;
  name: string;
  description?: string | null;
}

interface RouterOpts {
  tablesByScope: (scope: { ids?: string[]; schemaId?: string; databaseId?: string }) => MockTable[];
  columnsByTableIds: Map<string, MockColumn[]>;
  totalTablesOverride?: number;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as { scope?: { ids?: string[]; schemaId?: string; databaseId?: string } };
      const data = opts.tablesByScope(vars.scope ?? {});
      return {
        getTables: {
          totalCount: opts.totalTablesOverride ?? data.length,
          nbPerPage: data.length,
          page: 0,
          data,
        },
      };
    }
    if (document === GET_COLUMNS_SUMMARY) {
      const vars = variables as {
        scope?: { tableIds?: string[] };
        pagination: { nbPerPage: number; page: number };
      };
      const ids = vars.scope?.tableIds ?? [];
      const all: MockColumn[] = [];
      for (const id of ids) {
        const cols = opts.columnsByTableIds.get(id) ?? [];
        all.push(...cols);
      }
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = all.slice(start, start + vars.pagination.nbPerPage);
      return {
        getColumns: {
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

function makeColumns(tableId: string, total: number, describedCount: number): MockColumn[] {
  return Array.from({ length: total }, (_, i) => ({
    id: `${tableId}-c${i}`,
    tableId,
    name: `col_${i}`,
    description: i < describedCount ? "documented" : null,
  }));
}

describe("catalog_governance_scorecard — scope validation", () => {
  it("refuses when no scope is provided (would attempt to load every table)", async () => {
    const client = makeRouter({
      tablesByScope: () => [],
      columnsByTableIds: new Map(),
    });
    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({});
    const out = parseResult(res);
    expect(out.error).toMatch(/Scope required/);
  });

  it("most-specific scope wins when multiple are passed (tableIds > schemaId > databaseId)", async () => {
    let capturedScope: Record<string, unknown> = {};
    const client = makeMockClient((document, variables) => {
      if (document === GET_TABLES_DETAIL_BATCH) {
        capturedScope = (variables as { scope: Record<string, unknown> }).scope;
        return {
          getTables: { totalCount: 0, nbPerPage: 0, page: 0, data: [] },
        };
      }
      throw new Error("unexpected");
    });
    const tool = defineGovernanceScorecard(client);
    await tool.handler({
      databaseId: "db-1",
      schemaId: "sch-1",
      tableIds: ["t-1"],
    });
    expect(capturedScope).toEqual({ ids: ["t-1"] });
    const out = parseResult(
      await tool.handler({ databaseId: "db-1", schemaId: "sch-1" })
    );
    expect(out.scopedBy).toBe("schemaId");
  });
});

describe("catalog_governance_scorecard — happy path coverage matrix", () => {
  it("computes hasOwner, hasDescription, columnDocCoverage, tagCount per table", async () => {
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "ORDERS",
        popularity: 0.9,
        description: "the orders table",
        isVerified: true,
        isDeprecated: false,
        ownerEntities: [{ id: "o1", userId: "u1" }],
        teamOwnerEntities: [],
        tagEntities: [{ id: "te1", tag: { label: "PII" } }],
      },
      {
        id: "t-2",
        name: "STAGING_RAW",
        popularity: 0.1,
        description: null,
        isDeprecated: false,
        ownerEntities: [],
        teamOwnerEntities: [],
        tagEntities: [],
      },
    ];
    const columns = new Map<string, MockColumn[]>([
      // 4/5 documented = 80%
      ["t-1", makeColumns("t-1", 5, 4)],
      // 0/3 documented = 0%
      ["t-2", makeColumns("t-2", 3, 0)],
    ]);
    const client = makeRouter({
      tablesByScope: () => tables,
      columnsByTableIds: columns,
    });

    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ schemaId: "sch-x" });
    const out = parseResult(res);

    expect(out.scopedBy).toBe("schemaId");
    expect(out.tableCount).toBe(2);
    const rows = out.tables as Array<Record<string, unknown>>;
    expect(rows).toHaveLength(2);

    const t1 = rows.find((r) => r.id === "t-1") as Record<string, unknown>;
    expect(t1.hasOwner).toBe(true);
    expect(t1.hasDescription).toBe(true);
    expect(t1.tagCount).toBe(1);
    expect(t1.columnDocCoverage).toEqual({
      described: 4,
      total: 5,
      pct: 80,
      sampled: false,
    });

    const t2 = rows.find((r) => r.id === "t-2") as Record<string, unknown>;
    expect(t2.hasOwner).toBe(false);
    expect(t2.hasDescription).toBe(false);
    expect(t2.tagCount).toBe(0);
    expect(t2.columnDocCoverage).toEqual({
      described: 0,
      total: 3,
      pct: 0,
      sampled: false,
    });
  });

  it("flags sampled: true when a table exceeds perTableColumnCap", async () => {
    const tables: MockTable[] = [
      {
        id: "t-wide",
        name: "WIDE_STAGE",
        popularity: 0.5,
        description: "wide table",
        ownerEntities: [{ id: "o1", userId: "u1" }],
      },
    ];
    // 250 cols, all documented; cap=200 → expect sampled flag, total=200, described=200
    const columns = new Map<string, MockColumn[]>([
      ["t-wide", makeColumns("t-wide", 250, 250)],
    ]);
    const client = makeRouter({
      tablesByScope: () => tables,
      columnsByTableIds: columns,
    });

    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ schemaId: "sch", perTableColumnCap: 200 });
    const out = parseResult(res);
    const cov = (out.tables as Array<Record<string, unknown>>)[0]
      .columnDocCoverage as Record<string, unknown>;
    expect(cov).toMatchObject({ described: 200, total: 200, pct: 100, sampled: true });
  });
});

describe("catalog_governance_scorecard — aggregate weighting", () => {
  // Three tables: hot+governed, hot+ungoverned, cold+governed.
  // popularity-weighted should reflect that the cold table contributes little;
  // equal-weighted should treat them all the same.
  function tablesAndColumns() {
    const tables: MockTable[] = [
      {
        id: "hot-good",
        name: "HOT_GOOD",
        popularity: 1.0,
        description: "hot governed",
        ownerEntities: [{ id: "o", userId: "u" }],
        tagEntities: [{ id: "te", tag: { label: "T" } }],
      },
      {
        id: "hot-bad",
        name: "HOT_BAD",
        popularity: 1.0,
        description: null,
        ownerEntities: [],
        tagEntities: [],
      },
      {
        id: "cold-good",
        name: "COLD_GOOD",
        popularity: 0.0,
        description: "cold governed",
        ownerEntities: [{ id: "o", userId: "u" }],
        tagEntities: [{ id: "te", tag: { label: "T" } }],
      },
    ];
    const columns = new Map<string, MockColumn[]>([
      ["hot-good", makeColumns("hot-good", 10, 10)], // 100%
      ["hot-bad", makeColumns("hot-bad", 10, 0)], // 0%
      ["cold-good", makeColumns("cold-good", 10, 10)], // 100%
    ]);
    return { tables, columns };
  }

  it("popularity-weighted aggregate ignores the cold table's contribution", async () => {
    const { tables, columns } = tablesAndColumns();
    const client = makeRouter({
      tablesByScope: () => tables,
      columnsByTableIds: columns,
    });
    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ schemaId: "sch" });
    const agg = parseResult(res).aggregate as Record<string, number>;

    // Two hot tables share 100% of weight (popularity 1.0 each).
    // 1/2 owned, 1/2 described, 1/2 tagged → 50% on each axis.
    // columnDoc: hot-good=100, hot-bad=0 → mean 50.
    expect(agg.weighting).toBe("popularity");
    expect(agg.ownedPct).toBe(50);
    expect(agg.describedPct).toBe(50);
    expect(agg.taggedPct).toBe(50);
    expect(agg.columnDocPct).toBe(50);
    expect(agg.governanceScore).toBe(50);
  });

  it("equal-weighted aggregate counts the cold table fully", async () => {
    const { tables, columns } = tablesAndColumns();
    const client = makeRouter({
      tablesByScope: () => tables,
      columnsByTableIds: columns,
    });
    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ schemaId: "sch", weighting: "equal" });
    const agg = parseResult(res).aggregate as Record<string, number>;

    // 2/3 owned, 2/3 described, 2/3 tagged → 67%.
    // columnDoc: hot-good=100, hot-bad=0, cold-good=100 → mean ≈ 67.
    expect(agg.weighting).toBe("equal");
    expect(agg.ownedPct).toBe(67);
    expect(agg.describedPct).toBe(67);
    expect(agg.taggedPct).toBe(67);
    expect(agg.columnDocPct).toBe(67);
  });

  it("falls back to equal-weighting when every table has popularity 0/null", async () => {
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "T1",
        popularity: 0,
        description: "desc",
        ownerEntities: [{ id: "o", userId: "u" }],
      },
      {
        id: "t-2",
        name: "T2",
        popularity: null,
        description: null,
        ownerEntities: [],
      },
    ];
    const columns = new Map<string, MockColumn[]>([
      ["t-1", makeColumns("t-1", 4, 4)],
      ["t-2", makeColumns("t-2", 4, 0)],
    ]);
    const client = makeRouter({
      tablesByScope: () => tables,
      columnsByTableIds: columns,
    });
    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ schemaId: "sch" });
    const agg = parseResult(res).aggregate as Record<string, number>;
    // Without the fallback this would be NaN. Equal-weighted: 1/2 owned, 1/2 described.
    expect(agg.ownedPct).toBe(50);
    expect(agg.describedPct).toBe(50);
  });
});

describe("catalog_governance_scorecard — refusal on oversized scope", () => {
  it("refuses when totalCount exceeds the 500-table hard cap", async () => {
    const tables: MockTable[] = Array.from({ length: 500 }, (_, i) => ({
      id: `t-${i}`,
      name: `T${i}`,
      popularity: 0.1,
    }));
    const client = makeRouter({
      tablesByScope: () => tables,
      columnsByTableIds: new Map(),
      totalTablesOverride: 750,
    });
    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ databaseId: "db-large" });
    const out = parseResult(res);
    expect(out.error).toMatch(/750 tables/);
    expect(out.error).toMatch(/500-table cap/);
    expect(out.tableCount).toBe(750);
    expect(out.scopedBy).toBe("databaseId");
  });

  it("processes exactly 500 tables (at the cap, not over)", async () => {
    const tables: MockTable[] = Array.from({ length: 500 }, (_, i) => ({
      id: `t-${i}`,
      name: `T${i}`,
      popularity: 0.5,
      description: "x",
      ownerEntities: [{ id: "o", userId: "u" }],
    }));
    const columns = new Map<string, MockColumn[]>(
      tables.map((t) => [t.id, makeColumns(t.id, 2, 2)])
    );
    const client = makeRouter({
      tablesByScope: () => tables,
      columnsByTableIds: columns,
    });
    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ databaseId: "db" });
    const out = parseResult(res);
    expect(out.error).toBeUndefined();
    expect(out.tableCount).toBe(500);
  });
});

describe("catalog_governance_scorecard — empty scope", () => {
  it("returns a zeroed aggregate when the scope resolves to no tables", async () => {
    const client = makeRouter({
      tablesByScope: () => [],
      columnsByTableIds: new Map(),
    });
    const tool = defineGovernanceScorecard(client);
    const res = await tool.handler({ schemaId: "empty" });
    const out = parseResult(res);
    expect(out.tableCount).toBe(0);
    expect(out.tables).toEqual([]);
    expect(out.aggregate).toMatchObject({
      tableCount: 0,
      ownedPct: 0,
      describedPct: 0,
      governanceScore: 0,
    });
  });
});
