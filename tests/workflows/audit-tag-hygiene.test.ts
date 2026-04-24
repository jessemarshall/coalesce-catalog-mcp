import { describe, it, expect } from "vitest";
import { defineAuditTagHygiene } from "../../src/workflows/audit-tag-hygiene.js";
import {
  GET_TAGS,
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
  isError?: boolean;
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockTag {
  id: string;
  label: string;
  color?: string | null;
  linkedTermId?: string | null;
}

interface MockAsset {
  id: string;
  // Each tag reference uses { tag: { id, label } } shape.
  tagIds: string[];
}

interface RouterOpts {
  tags: MockTag[];
  tables?: MockAsset[];
  dashboards?: MockAsset[];
  // Override totalCounts to simulate capacity-gate scenarios or
  // non-numeric returns.
  tagsTotalOverride?: number | null;
  tablesTotalOverride?: number | null;
  dashboardsTotalOverride?: number | null;
}

function tagRefs(tags: MockTag[], ids: string[]) {
  return ids
    .map((id) => tags.find((t) => t.id === id))
    .filter((t): t is MockTag => !!t)
    .map((t, i) => ({
      id: `te-${i}-${t.id}`,
      tag: { id: t.id, label: t.label },
    }));
}

function makeRouter(opts: RouterOpts) {
  const tagRowById = new Map<string, MockTag>(
    opts.tags.map((t) => [t.id, t])
  );
  return makeMockClient((document, variables) => {
    if (document === GET_TAGS) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = opts.tags.slice(start, start + vars.pagination.nbPerPage);
      const totalCount =
        opts.tagsTotalOverride === undefined
          ? opts.tags.length
          : opts.tagsTotalOverride;
      return {
        getTags: {
          totalCount,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice.map((t) => ({
            id: t.id,
            label: t.label,
            color: t.color ?? null,
            linkedTermId: t.linkedTermId ?? null,
          })),
        },
      };
    }
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const tables = opts.tables ?? [];
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = tables.slice(start, start + vars.pagination.nbPerPage);
      const totalCount =
        opts.tablesTotalOverride === undefined
          ? tables.length
          : opts.tablesTotalOverride;
      return {
        getTables: {
          totalCount,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice.map((row) => ({
            id: row.id,
            tagEntities: tagRefs(
              row.tagIds.map((id) => tagRowById.get(id)).filter(
                (t): t is MockTag => !!t
              ),
              row.tagIds
            ),
          })),
        },
      };
    }
    if (document === GET_DASHBOARDS_DETAIL_BATCH) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const dashboards = opts.dashboards ?? [];
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = dashboards.slice(start, start + vars.pagination.nbPerPage);
      const totalCount =
        opts.dashboardsTotalOverride === undefined
          ? dashboards.length
          : opts.dashboardsTotalOverride;
      return {
        getDashboards: {
          totalCount,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice.map((row) => ({
            id: row.id,
            tagEntities: tagRefs(
              row.tagIds.map((id) => tagRowById.get(id)).filter(
                (t): t is MockTag => !!t
              ),
              row.tagIds
            ),
          })),
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_audit_tag_hygiene — empty workspace", () => {
  it("returns zero counts across the board when there are no tags", async () => {
    const client = makeRouter({ tags: [] });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    expect(out.summary).toMatchObject({
      totalTags: 0,
      orphanedCount: 0,
      unlinkedCount: 0,
      skewedCount: 0,
      nearDuplicateGroupCount: 0,
    });
    expect(out.findings).toMatchObject({
      orphaned: [],
      unlinked: [],
      skewed: [],
      nearDuplicates: [],
    });
    expect(out.tagUsage).toEqual([]);
  });
});

describe("catalog_audit_tag_hygiene — capacity gates", () => {
  it("refuses when the workspace has more tags than maxTags", async () => {
    const client = makeRouter({
      tags: [{ id: "t1", label: "pii" }],
      // Lie about totalCount to trigger the gate while returning a small page.
      tagsTotalOverride: 1500,
    });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({ maxTags: 1000 });
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/1500 tags, exceeding the maxTags/);
  });

  it("throws (isError) when getTags returns a non-numeric totalCount", async () => {
    const client = makeRouter({
      tags: [{ id: "t1", label: "pii" }],
      tagsTotalOverride: null,
    });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/non-numeric totalCount/);
  });

  it("throws (isError) when getTables returns a non-numeric totalCount", async () => {
    const client = makeRouter({
      tags: [{ id: "t1", label: "pii" }],
      tables: [{ id: "tbl-1", tagIds: [] }],
      tablesTotalOverride: null,
    });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/non-numeric totalCount/);
  });
});

describe("catalog_audit_tag_hygiene — finding detection", () => {
  it("flags tags with zero usage as orphaned", async () => {
    const client = makeRouter({
      tags: [
        { id: "t1", label: "pii", linkedTermId: "term-1" },
        { id: "t2", label: "unused-tag", linkedTermId: "term-2" },
      ],
      tables: [{ id: "tbl-1", tagIds: ["t1"] }],
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    const orphaned = (out.findings as Record<string, unknown>)
      .orphaned as Array<Record<string, unknown>>;
    expect(orphaned).toHaveLength(1);
    expect(orphaned[0]).toMatchObject({ tagId: "t2", label: "unused-tag" });
  });

  it("flags used tags without a linkedTermId as unlinked", async () => {
    const client = makeRouter({
      tags: [
        { id: "t1", label: "pii", linkedTermId: "term-1" },
        { id: "t2", label: "free-floating", linkedTermId: null },
      ],
      tables: [
        { id: "tbl-1", tagIds: ["t1"] },
        { id: "tbl-2", tagIds: ["t2"] },
      ],
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    const unlinked = (out.findings as Record<string, unknown>)
      .unlinked as Array<Record<string, unknown>>;
    expect(unlinked).toEqual([
      { tagId: "t2", label: "free-floating", usageCount: 1 },
    ]);
  });

  it("does not flag an orphaned tag as unlinked (unlinked requires usage > 0)", async () => {
    const client = makeRouter({
      tags: [{ id: "t1", label: "zero-use", linkedTermId: null }],
      tables: [],
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    const findings = out.findings as Record<string, unknown>;
    expect((findings.orphaned as Array<unknown>).length).toBe(1);
    expect((findings.unlinked as Array<unknown>).length).toBe(0);
  });

  it("flags a tag as TABLE-skewed when >80% of usage is on tables and totalUsage >= 5", async () => {
    // 9 table uses, 1 dashboard use -> 90% table, above 80% threshold.
    const client = makeRouter({
      tags: [{ id: "t1", label: "pii", linkedTermId: "term-1" }],
      tables: Array.from({ length: 9 }, (_, i) => ({
        id: `tbl-${i}`,
        tagIds: ["t1"],
      })),
      dashboards: [{ id: "d1", tagIds: ["t1"] }],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    const skewed = (out.findings as Record<string, unknown>)
      .skewed as Array<Record<string, unknown>>;
    expect(skewed).toHaveLength(1);
    expect(skewed[0]).toMatchObject({
      tagId: "t1",
      dominantType: "TABLE",
      dominantPercent: 90,
      totalUsage: 10,
    });
  });

  it("flags a tag as DASHBOARD-skewed when >80% of usage is on dashboards", async () => {
    const client = makeRouter({
      tags: [{ id: "t1", label: "exec" }],
      tables: [{ id: "tbl-1", tagIds: ["t1"] }],
      dashboards: Array.from({ length: 9 }, (_, i) => ({
        id: `d-${i}`,
        tagIds: ["t1"],
      })),
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    const skewed = (out.findings as Record<string, unknown>)
      .skewed as Array<Record<string, unknown>>;
    expect(skewed).toHaveLength(1);
    expect(skewed[0]).toMatchObject({
      dominantType: "DASHBOARD",
      dominantPercent: 90,
    });
  });

  it("does not flag skew below the 5-use minimum even when 100% concentrated", async () => {
    // 4 uses, all on tables — 100% concentration but below the 5-use floor.
    const client = makeRouter({
      tags: [{ id: "t1", label: "rare" }],
      tables: Array.from({ length: 4 }, (_, i) => ({
        id: `tbl-${i}`,
        tagIds: ["t1"],
      })),
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    const findings = out.findings as Record<string, unknown>;
    expect((findings.skewed as Array<unknown>).length).toBe(0);
  });

  it("detects near-duplicate tag labels within the Levenshtein threshold", async () => {
    const client = makeRouter({
      tags: [
        { id: "t1", label: "customer" },
        { id: "t2", label: "customers" }, // distance 1
        { id: "t3", label: "product" }, // distance >= 3 from customer/s
      ],
      tables: [],
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(
      await tool.handler({ nearDuplicateThreshold: 1 })
    );
    const nd = (out.findings as Record<string, unknown>)
      .nearDuplicates as Array<Record<string, unknown>>;
    expect(nd).toHaveLength(1);
    expect(nd[0]).toMatchObject({
      group: ["customer", "customers"],
      distance: 1,
    });
  });

  it("skips near-duplicate detection entirely when threshold is 0", async () => {
    const client = makeRouter({
      tags: [
        { id: "t1", label: "customer" },
        { id: "t2", label: "customers" },
      ],
      tables: [],
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(
      await tool.handler({ nearDuplicateThreshold: 0 })
    );
    const nd = (out.findings as Record<string, unknown>)
      .nearDuplicates as Array<unknown>;
    expect(nd).toHaveLength(0);
  });

  it("near-duplicate match is case-insensitive", async () => {
    const client = makeRouter({
      tags: [
        { id: "t1", label: "PII" },
        { id: "t2", label: "pii" }, // distance 0 after lowercasing — not a dup
        { id: "t3", label: "Pii_" }, // distance 1 after lowercasing
      ],
      tables: [],
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(
      await tool.handler({ nearDuplicateThreshold: 1 })
    );
    const nd = (out.findings as Record<string, unknown>)
      .nearDuplicates as Array<Record<string, unknown>>;
    // Both pairs whose distance > 0 AND <= 1 after lowercasing: PII/Pii_
    // and pii/Pii_. PII/pii is excluded because distance === 0 is filtered.
    expect(nd.length).toBe(2);
  });
});

describe("catalog_audit_tag_hygiene — tagUsage ordering", () => {
  it("sorts tagUsage by totalUsage DESC", async () => {
    const client = makeRouter({
      tags: [
        { id: "t1", label: "low" },
        { id: "t2", label: "high" },
      ],
      tables: [
        { id: "tbl-1", tagIds: ["t1"] },
        { id: "tbl-2", tagIds: ["t2"] },
        { id: "tbl-3", tagIds: ["t2"] },
        { id: "tbl-4", tagIds: ["t2"] },
      ],
      dashboards: [],
    });
    const tool = defineAuditTagHygiene(client);
    const out = parseResult(await tool.handler({}));
    const usage = out.tagUsage as Array<Record<string, unknown>>;
    expect(usage.map((u) => u.tagId)).toEqual(["t2", "t1"]);
    expect(usage[0].totalUsage).toBe(3);
    expect(usage[1].totalUsage).toBe(1);
  });
});
