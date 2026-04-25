import { describe, it, expect } from "vitest";
import { defineAuditGovernanceFreshness } from "../../src/workflows/audit-governance-freshness.js";
import { GET_TABLES_DETAIL_BATCH } from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
  isError?: boolean;
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockTable {
  id: string;
  name?: string | null;
  numberOfQueries?: number | null;
  isVerified?: boolean;
  isDeprecated?: boolean;
  verifiedAt?: string | null;
  updatedAt?: string | null;
  schema?: { name?: string } | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
  tagEntities?: Array<Record<string, unknown>>;
}

interface RouterOpts {
  tables: MockTable[];
  // If set, scope must match this filter; defaults to no validation.
  expectedScope?: Record<string, unknown>;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as {
        scope?: Record<string, unknown>;
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = opts.tables.slice(
        start,
        start + vars.pagination.nbPerPage
      );
      return {
        getTables: {
          totalCount: opts.tables.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_audit_governance_freshness — scope validation", () => {
  it("refuses when no scope is provided", async () => {
    const client = makeRouter({ tables: [] });
    const tool = defineAuditGovernanceFreshness(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/Scope required/);
  });

  it("refuses when multiple scope fields are provided", async () => {
    const client = makeRouter({ tables: [] });
    const tool = defineAuditGovernanceFreshness(client);
    const res = await tool.handler({
      databaseId: "db-1",
      schemaId: "schema-1",
    });
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/Multiple scope fields/);
  });

  it("refuses when tableIds exceeds 500", async () => {
    const client = makeRouter({ tables: [] });
    const tool = defineAuditGovernanceFreshness(client);
    const res = await tool.handler({
      tableIds: Array.from({ length: 501 }, (_, i) => `t${i}`),
    });
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/exceeds the 500-table audit cap/);
  });
});

describe("catalog_audit_governance_freshness — staleness computation", () => {
  it("uses verifiedAt when isVerified=true and computes daysSinceReview", async () => {
    // asOf = 2026-04-25, verifiedAt = 2026-01-25 → 90 days
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          numberOfQueries: 10,
          isVerified: true,
          verifiedAt: "2026-01-25T00:00:00.000Z",
          updatedAt: "2026-04-20T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 365 },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].lastReviewedSource).toBe("verifiedAt");
    expect(tables[0].daysSinceReview).toBe(90);
    expect(tables[0].requiredCadenceDays).toBe(365);
    expect(tables[0].isOverdue).toBe(false);
    expect(tables[0].bucket).toBe("ok");
  });

  it("falls back to updatedAt when isVerified=false", async () => {
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          isVerified: false,
          verifiedAt: null,
          updatedAt: "2026-01-25T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 365 },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].lastReviewedSource).toBe("updatedAt");
    expect(tables[0].daysSinceReview).toBe(90);
  });

  it("buckets a table with no timestamps as 'neverReviewed' and treats it as overdue", async () => {
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          numberOfQueries: 5,
          isVerified: false,
          verifiedAt: null,
          updatedAt: null,
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].bucket).toBe("neverReviewed");
    expect(tables[0].isOverdue).toBe(true);
    expect(tables[0].lastReviewedAt).toBeNull();
    expect(tables[0].lastReviewedSource).toBeNull();
    expect(tables[0].daysSinceReview).toBeNull();
  });

  it("flags a table as overdue when daysSinceReview > requiredCadenceDays", async () => {
    // 200 days since verified, default cadence 90 → 110 days overdue
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          numberOfQueries: 100,
          isVerified: true,
          verifiedAt: "2025-10-07T00:00:00.000Z", // 200 days before asOf
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 90 },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].isOverdue).toBe(true);
    expect(tables[0].bucket).toBe("overdue");
    expect(tables[0].stalenessDays).toBe(110);
    expect(tables[0].priorityScore).toBe(110 * 100);
  });

  it("buckets dueSoon when within 30 days of cadence", async () => {
    // 75 days since verified, cadence 90 → due in 15 days
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          isVerified: true,
          verifiedAt: "2026-02-09T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 90 },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].bucket).toBe("dueSoon");
    expect(tables[0].isOverdue).toBe(false);
  });
});

describe("catalog_audit_governance_freshness — sensitivity policy", () => {
  it("applies tighter cadence from a matching tag (case-insensitive substring)", async () => {
    // 100 days since verified, default 365 (would be ok), pii: 90 (overdue)
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          isVerified: true,
          verifiedAt: "2026-01-15T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [
            { tag: { id: "tg-1", label: "PII-Restricted" } },
          ],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 365, byTag: { pii: 90 } },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].requiredCadenceDays).toBe(90);
    expect(tables[0].matchedSensitivityTags).toEqual(["PII-Restricted"]);
    expect(tables[0].isOverdue).toBe(true);
  });

  it("picks the tightest cadence when a table matches multiple sensitivity tags", async () => {
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          isVerified: true,
          verifiedAt: "2026-04-01T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [
            { tag: { id: "tg-1", label: "Reference" } }, // 730d
            { tag: { id: "tg-2", label: "PII" } }, // 90d (winner)
          ],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: {
          defaultDays: 365,
          byTag: { reference: 730, pii: 90 },
        },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].requiredCadenceDays).toBe(90);
    expect(tables[0].matchedSensitivityTags).toEqual(["Reference", "PII"]);
  });

  it("uses defaultDays when no sensitivity tag matches", async () => {
    const client = makeRouter({
      tables: [
        {
          id: "t1",
          name: "T1",
          isVerified: true,
          verifiedAt: "2026-04-01T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [{ tag: { id: "tg-1", label: "ad-hoc" } }],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 365, byTag: { pii: 90 } },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables[0].requiredCadenceDays).toBe(365);
    expect(tables[0].matchedSensitivityTags).toEqual([]);
  });
});

describe("catalog_audit_governance_freshness — aggregate + ordering", () => {
  it("sorts tables by priorityScore DESC and rolls up bucket counts + popularity-weighted overdue %", async () => {
    const client = makeRouter({
      tables: [
        // ok: 30 days, default cadence 365
        {
          id: "ok-t",
          name: "OK_TABLE",
          numberOfQueries: 5,
          isVerified: true,
          verifiedAt: "2026-03-26T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
        // overdue: 200 days verified, 90 default → staleness 110
        {
          id: "overdue-t",
          name: "OVERDUE",
          numberOfQueries: 100,
          isVerified: true,
          verifiedAt: "2025-10-07T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
        // neverReviewed: no timestamps
        {
          id: "never-t",
          name: "NEVER",
          numberOfQueries: 50,
          isVerified: false,
          verifiedAt: null,
          updatedAt: null,
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 90 },
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    // OVERDUE: 110 * 100 = 11000; NEVER: 90 * 50 = 4500; OK_TABLE: 0 * 5 = 0.
    expect(tables.map((t) => t.tableId)).toEqual([
      "overdue-t",
      "never-t",
      "ok-t",
    ]);
    const aggregate = out.aggregate as Record<string, unknown>;
    expect(aggregate.tableCount).toBe(3);
    expect(aggregate.buckets).toEqual({
      neverReviewed: 1,
      overdue: 1,
      dueSoon: 0,
      ok: 1,
    });
    // overdue + neverReviewed = 2/3 → 67%
    expect(aggregate.overduePct).toBe(67);
    // Popularity-weighted: (100 + 50) / (5 + 100 + 50) = 150/155 → 97%
    expect(aggregate.popularityWeightedOverduePct).toBe(97);
    expect(aggregate.worstStalenessDays).toBe(110);
  });

  it("filters output to overdue tables only when overdueOnly=true (aggregate still reflects full scope)", async () => {
    const client = makeRouter({
      tables: [
        {
          id: "ok-t",
          name: "OK",
          numberOfQueries: 1,
          isVerified: true,
          verifiedAt: "2026-04-01T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
        {
          id: "overdue-t",
          name: "OVERDUE",
          numberOfQueries: 1,
          isVerified: true,
          verifiedAt: "2025-01-01T00:00:00.000Z",
          ownerEntities: [],
          teamOwnerEntities: [],
          tagEntities: [],
        },
      ],
    });
    const tool = defineAuditGovernanceFreshness(client);
    const out = parseResult(
      await tool.handler({
        databaseId: "db-1",
        asOf: "2026-04-25T00:00:00.000Z",
        cadencePolicy: { defaultDays: 90 },
        overdueOnly: true,
      })
    );
    const tables = out.tables as Array<Record<string, unknown>>;
    expect(tables).toHaveLength(1);
    expect(tables[0].tableId).toBe("overdue-t");
    // Aggregate counts the full universe regardless of overdueOnly.
    const aggregate = out.aggregate as Record<string, unknown>;
    expect(aggregate.tableCount).toBe(2);
  });
});

describe("catalog_audit_governance_freshness — input validation", () => {
  it("rejects an unparseable asOf string", async () => {
    const client = makeRouter({ tables: [] });
    const tool = defineAuditGovernanceFreshness(client);
    // Bypass zod's datetime validator by passing a malformed value; the
    // tool's runtime check is the second gate.
    const res = await tool.handler({
      databaseId: "db-1",
      asOf: "1969-13-99T99:99:99Z",
    });
    expect(res.isError).toBe(true);
  });
});
