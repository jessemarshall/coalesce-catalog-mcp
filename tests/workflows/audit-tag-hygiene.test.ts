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
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockTag {
  id: string;
  label: string;
  linkedTermId?: string | null;
  color?: string;
  slug?: string;
  createdAt?: string;
  updatedAt?: string;
}

interface MockTable {
  id: string;
  name: string;
  tagEntities?: Array<{ id: string; tag: { id: string; label: string } }>;
  ownerEntities?: unknown[];
  teamOwnerEntities?: unknown[];
}

interface MockDashboard {
  id: string;
  name: string;
  tagEntities?: Array<{ id: string; tag: { id: string; label: string } }>;
  ownerEntities?: unknown[];
  teamOwnerEntities?: unknown[];
}

interface RouterOpts {
  tags: MockTag[];
  tables: MockTable[];
  dashboards: MockDashboard[];
  totalTagsOverride?: number;
  totalTablesOverride?: number;
  totalDashboardsOverride?: number;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TAGS) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = opts.tags.slice(start, start + vars.pagination.nbPerPage);
      return {
        getTags: {
          totalCount: opts.totalTagsOverride ?? opts.tags.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = opts.tables.slice(
        start,
        start + vars.pagination.nbPerPage
      );
      return {
        getTables: {
          totalCount: opts.totalTablesOverride ?? opts.tables.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_DASHBOARDS_DETAIL_BATCH) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = opts.dashboards.slice(
        start,
        start + vars.pagination.nbPerPage
      );
      return {
        getDashboards: {
          totalCount: opts.totalDashboardsOverride ?? opts.dashboards.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_audit_tag_hygiene — scope validation", () => {
  it("refuses when both databaseId and schemaId are provided", async () => {
    const client = makeRouter({ tags: [], tables: [], dashboards: [] });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({
      databaseId: "db-1",
      schemaId: "sch-1",
    });
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/databaseId.*schemaId/i);
  });

  it("refuses when tags exceed the 1000 cap", async () => {
    const client = makeRouter({
      tags: Array.from({ length: 500 }, (_, i) => ({
        id: `tag-${i}`,
        label: `tag_${i}`,
      })),
      tables: [],
      dashboards: [],
      totalTagsOverride: 1500,
    });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/1500 tags/);
    expect(out.error).toMatch(/1000-tag cap/);
  });

  it("refuses when tables exceed the 500 cap", async () => {
    const client = makeRouter({
      tags: [{ id: "tag-1", label: "test" }],
      tables: [],
      dashboards: [],
      totalTablesOverride: 600,
    });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/600 tables/);
    expect(out.error).toMatch(/500-table/);
  });
});

describe("catalog_audit_tag_hygiene — empty workspace", () => {
  it("returns zeroed aggregates when no tags exist", async () => {
    const client = makeRouter({ tags: [], tables: [], dashboards: [] });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    const out = parseResult(res);
    expect(out.tagCount).toBe(0);
    const agg = out.aggregate as Record<string, number>;
    expect(agg.orphanedCount).toBe(0);
    expect(agg.unlinkedCount).toBe(0);
    expect(agg.healthyCount).toBe(0);
  });
});

describe("catalog_audit_tag_hygiene — issue detection", () => {
  it("detects orphaned tags (zero entity attachments)", async () => {
    const tags: MockTag[] = [
      { id: "tag-orphan", label: "orphan_tag", linkedTermId: "term-1" },
      { id: "tag-used", label: "used_tag", linkedTermId: "term-2" },
    ];
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "T1",
        tagEntities: [
          { id: "te-1", tag: { id: "tag-used", label: "used_tag" } },
        ],
      },
    ];
    const client = makeRouter({ tags, tables, dashboards: [] });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    const out = parseResult(res);

    const tagRecords = out.tags as Array<Record<string, unknown>>;
    const orphan = tagRecords.find((t) => t.id === "tag-orphan")!;
    expect(orphan.entityCount).toBe(0);
    expect(orphan.issues).toContain("orphaned");

    const used = tagRecords.find((t) => t.id === "tag-used")!;
    expect(used.entityCount).toBe(1);
    expect(used.issues).not.toContain("orphaned");

    const agg = out.aggregate as Record<string, number>;
    expect(agg.orphanedCount).toBe(1);
  });

  it("detects unlinked tags (no glossary term)", async () => {
    const tags: MockTag[] = [
      { id: "tag-linked", label: "linked", linkedTermId: "term-1" },
      { id: "tag-unlinked", label: "unlinked", linkedTermId: null },
    ];
    const client = makeRouter({ tags, tables: [], dashboards: [] });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    const out = parseResult(res);

    const tagRecords = out.tags as Array<Record<string, unknown>>;
    const linked = tagRecords.find((t) => t.id === "tag-linked")!;
    expect(linked.issues).not.toContain("unlinked");

    const unlinked = tagRecords.find((t) => t.id === "tag-unlinked")!;
    expect(unlinked.issues).toContain("unlinked");

    const agg = out.aggregate as Record<string, number>;
    expect(agg.unlinkedCount).toBe(1);
  });

  it("detects skewed tags (95%+ one entity type)", async () => {
    const tags: MockTag[] = [
      { id: "tag-skewed", label: "skewed_tag", linkedTermId: "term-1" },
      { id: "tag-balanced", label: "balanced_tag", linkedTermId: "term-2" },
    ];
    // tag-skewed: 20 tables, 0 dashboards → 100% tables → skewed
    const tables: MockTable[] = Array.from({ length: 20 }, (_, i) => ({
      id: `t-${i}`,
      name: `T${i}`,
      tagEntities: [
        { id: `te-${i}`, tag: { id: "tag-skewed", label: "skewed_tag" } },
        ...(i < 10
          ? [
              {
                id: `te-b-${i}`,
                tag: { id: "tag-balanced", label: "balanced_tag" },
              },
            ]
          : []),
      ],
    }));
    // tag-balanced: 10 tables + 10 dashboards → 50/50 → not skewed
    const dashboards: MockDashboard[] = Array.from({ length: 10 }, (_, i) => ({
      id: `d-${i}`,
      name: `D${i}`,
      tagEntities: [
        {
          id: `dte-${i}`,
          tag: { id: "tag-balanced", label: "balanced_tag" },
        },
      ],
    }));

    const client = makeRouter({ tags, tables, dashboards });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    const out = parseResult(res);

    const tagRecords = out.tags as Array<Record<string, unknown>>;
    const skewed = tagRecords.find((t) => t.id === "tag-skewed")!;
    expect(skewed.issues).toContain("skewed");
    const skewDetail = skewed.skewDetail as Record<string, number>;
    expect(skewDetail.tablePct).toBe(100);

    const balanced = tagRecords.find((t) => t.id === "tag-balanced")!;
    expect(balanced.issues).not.toContain("skewed");
    expect(balanced.entityCount).toBe(20);
  });

  it("detects near-duplicate tags via Levenshtein distance", async () => {
    const tags: MockTag[] = [
      { id: "tag-1", label: "customer_id", linkedTermId: "t1" },
      { id: "tag-2", label: "customer_Id", linkedTermId: "t2" }, // edit distance 1
      { id: "tag-3", label: "order_total", linkedTermId: "t3" },
      { id: "tag-4", label: "pii", linkedTermId: "t4" }, // too short for dup detection (default min 5)
      { id: "tag-5", label: "PII", linkedTermId: "t5" }, // also short
    ];
    const client = makeRouter({ tags, tables: [], dashboards: [] });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    const out = parseResult(res);

    const tagRecords = out.tags as Array<Record<string, unknown>>;
    const tag1 = tagRecords.find((t) => t.id === "tag-1")!;
    const tag2 = tagRecords.find((t) => t.id === "tag-2")!;
    expect(tag1.issues).toContain("near_duplicate");
    expect(tag2.issues).toContain("near_duplicate");
    expect(tag1.nearDuplicateGroup).toBe(tag2.nearDuplicateGroup);

    const tag3 = tagRecords.find((t) => t.id === "tag-3")!;
    expect(tag3.issues).not.toContain("near_duplicate");

    // Short tags excluded from near-duplicate detection
    const tag4 = tagRecords.find((t) => t.id === "tag-4")!;
    expect(tag4.issues).not.toContain("near_duplicate");

    const groups = out.nearDuplicateGroups as Array<Record<string, unknown>>;
    expect(groups.length).toBeGreaterThanOrEqual(1);
    const custGroup = groups.find((g) => {
      const labels = g.labels as string[];
      return labels.includes("customer_id");
    });
    expect(custGroup).toBeDefined();
  });

  it("identifies healthy tags (no issues)", async () => {
    const tags: MockTag[] = [
      { id: "tag-healthy", label: "healthy_tag", linkedTermId: "term-1" },
    ];
    const tables: MockTable[] = [
      {
        id: "t-1",
        name: "T1",
        tagEntities: [
          { id: "te-1", tag: { id: "tag-healthy", label: "healthy_tag" } },
        ],
      },
    ];
    const dashboards: MockDashboard[] = [
      {
        id: "d-1",
        name: "D1",
        tagEntities: [
          { id: "dte-1", tag: { id: "tag-healthy", label: "healthy_tag" } },
        ],
      },
    ];
    const client = makeRouter({ tags, tables, dashboards });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    const out = parseResult(res);

    const tagRecords = out.tags as Array<Record<string, unknown>>;
    const healthy = tagRecords.find((t) => t.id === "tag-healthy")!;
    expect(healthy.issues).toEqual([]);
    expect(healthy.entityCount).toBe(2);
    expect(healthy.tableCount).toBe(1);
    expect(healthy.dashboardCount).toBe(1);

    const agg = out.aggregate as Record<string, number>;
    expect(agg.healthyCount).toBe(1);
  });
});

describe("catalog_audit_tag_hygiene — multiple issues per tag", () => {
  it("tags can have multiple issues simultaneously", async () => {
    const tags: MockTag[] = [
      {
        id: "tag-bad",
        label: "orphaned_unlinked",
        linkedTermId: null,
      },
    ];
    // No tables or dashboards reference this tag
    const client = makeRouter({ tags, tables: [], dashboards: [] });
    const tool = defineAuditTagHygiene(client);
    const res = await tool.handler({});
    const out = parseResult(res);

    const tagRecords = out.tags as Array<Record<string, unknown>>;
    const bad = tagRecords.find((t) => t.id === "tag-bad")!;
    expect(bad.issues).toContain("orphaned");
    expect(bad.issues).toContain("unlinked");
  });
});

describe("catalog_audit_tag_hygiene — near-duplicate with custom min length", () => {
  it("respects nearDuplicateMinLength parameter", async () => {
    const tags: MockTag[] = [
      { id: "tag-1", label: "pii", linkedTermId: "t1" },
      { id: "tag-2", label: "PII", linkedTermId: "t2" },
    ];
    // Default min length 5 → "pii" (3 chars) excluded from dup detection
    const client1 = makeRouter({ tags, tables: [], dashboards: [] });
    const tool = defineAuditTagHygiene(client1);
    const res1 = await tool.handler({});
    const out1 = parseResult(res1);
    const tags1 = out1.tags as Array<Record<string, unknown>>;
    expect(tags1[0].issues).not.toContain("near_duplicate");

    // With min length 3 → "pii" included → should detect near-duplicate
    const client2 = makeRouter({ tags, tables: [], dashboards: [] });
    const tool2 = defineAuditTagHygiene(client2);
    const res2 = await tool2.handler({ nearDuplicateMinLength: 3 });
    const out2 = parseResult(res2);
    const tags2 = out2.tags as Array<Record<string, unknown>>;
    const piiTag = tags2.find((t) => t.id === "tag-1")!;
    expect(piiTag.issues).toContain("near_duplicate");
  });
});
