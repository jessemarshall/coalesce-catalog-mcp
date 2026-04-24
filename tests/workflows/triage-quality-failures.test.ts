import { describe, it, expect } from "vitest";
import { defineTriageQualityFailures } from "../../src/workflows/triage-quality-failures.js";
import {
  GET_DATA_QUALITIES,
  GET_TABLES_DETAIL_BATCH,
  GET_TABLES_SUMMARY,
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
  status: string; // "ALERT" | "WARNING" | "OK" etc
  result?: string | null;
  externalId?: string;
  runAt?: string | null;
  url?: string | null;
  tableId: string;
}

interface MockTable {
  id: string;
  name: string;
  numberOfQueries?: number | null;
  schema?: { name?: string } | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
}

interface RouterOpts {
  checks: MockQualityCheck[];
  tables?: Map<string, MockTable>;
  // parent tableId -> upstream parent ids (i.e. who feeds into this table)
  upstreamByTable?: Map<string, string[]>;
  upstreamNamesById?: Map<string, string>;
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
    if (document === GET_TABLES_SUMMARY) {
      // Used for upstream name hydration.
      const vars = variables as { scope?: { ids?: string[] } };
      const ids = vars.scope?.ids ?? [];
      const rows = ids.map((id) => ({
        id,
        name: opts.upstreamNamesById?.get(id) ?? null,
      }));
      return {
        getTables: {
          totalCount: rows.length,
          nbPerPage: ids.length || 1,
          page: 0,
          data: rows,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: { childTableId?: string };
        pagination: { nbPerPage: number; page: number };
      };
      const child = vars.scope?.childTableId ?? "";
      const parents = opts.upstreamByTable?.get(child) ?? [];
      // Return all parents on page 0 — tests use page sizes well above
      // the upstream fan-out so a single page always suffices.
      return {
        getLineages: {
          totalCount: parents.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data:
            vars.pagination.page === 0
              ? parents.map((parentTableId) => ({
                  parentTableId,
                  childTableId: child,
                }))
              : [],
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_triage_quality_failures — empty input paths", () => {
  it("returns an empty triage result when no checks exist", async () => {
    const client = makeRouter({ checks: [] });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    expect(out.summary).toEqual({
      matchedChecks: 0,
      failingChecks: 0,
      affectedTables: 0,
      totalOwners: 0,
    });
    expect(out.triageQueue).toEqual([]);
    expect(out.byOwner).toEqual({});
  });

  it("returns empty when every check is OK (none match the status filter)", async () => {
    const client = makeRouter({
      checks: [
        { id: "q1", name: "not_null", status: "OK", tableId: "t1" },
        { id: "q2", name: "unique", status: "OK", tableId: "t1" },
      ],
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    expect((out.summary as Record<string, unknown>).failingChecks).toBe(0);
    expect(out.triageQueue).toEqual([]);
  });
});

describe("catalog_triage_quality_failures — status filter", () => {
  it("defaults to ALERT + WARNING and excludes OK checks", async () => {
    const client = makeRouter({
      checks: [
        { id: "q1", name: "a", status: "ALERT", tableId: "t1" },
        { id: "q2", name: "b", status: "WARNING", tableId: "t1" },
        { id: "q3", name: "c", status: "OK", tableId: "t1" },
      ],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "ORDERS",
            numberOfQueries: 10,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    expect((out.summary as Record<string, unknown>).failingChecks).toBe(2);
  });

  it("honours statusFilter: ['ALERT'] and drops WARNINGs", async () => {
    const client = makeRouter({
      checks: [
        { id: "q1", name: "a", status: "ALERT", tableId: "t1" },
        { id: "q2", name: "b", status: "WARNING", tableId: "t1" },
      ],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T",
            numberOfQueries: 1,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({
        statusFilter: ["ALERT"],
        includeUpstreamPointers: false,
      })
    );
    expect((out.summary as Record<string, unknown>).failingChecks).toBe(1);
  });
});

describe("catalog_triage_quality_failures — capacity gate", () => {
  it("refuses with an actionable message when failing checks exceed maxFailingChecks", async () => {
    const checks: MockQualityCheck[] = Array.from({ length: 10 }, (_, i) => ({
      id: `q${i}`,
      name: `check_${i}`,
      status: "ALERT",
      tableId: "t1",
    }));
    const client = makeRouter({ checks });
    const tool = defineTriageQualityFailures(client);
    const res = await tool.handler({ maxFailingChecks: 5 });
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/exceed the 5-check/);
    expect(msg).toMatch(/statusFilter/);
  });
});

describe("catalog_triage_quality_failures — triage scoring & ordering", () => {
  it("ranks entries by popularity * failureCount DESC", async () => {
    const client = makeRouter({
      checks: [
        // t-low: 1 failure, popularity 100 -> score 100
        { id: "q1", name: "a", status: "ALERT", tableId: "t-low" },
        // t-high: 2 failures, popularity 100 -> score 200
        { id: "q2", name: "a", status: "ALERT", tableId: "t-high" },
        { id: "q3", name: "b", status: "WARNING", tableId: "t-high" },
      ],
      tables: new Map([
        [
          "t-low",
          {
            id: "t-low",
            name: "LOW",
            numberOfQueries: 100,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
        [
          "t-high",
          {
            id: "t-high",
            name: "HIGH",
            numberOfQueries: 100,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    const queue = out.triageQueue as Array<Record<string, unknown>>;
    expect(queue.map((e) => e.tableId)).toEqual(["t-high", "t-low"]);
    expect(queue[0].triageScore).toBe(200);
    expect(queue[1].triageScore).toBe(100);
  });

  it("derives tablePath from schema.name when present", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "a", status: "ALERT", tableId: "t1" }],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "ORDERS",
            numberOfQueries: 1,
            schema: { name: "PUBLIC" },
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    const queue = out.triageQueue as Array<Record<string, unknown>>;
    expect(queue[0].tablePath).toBe("PUBLIC.ORDERS");
  });

  it("defaults popularity to 0 when table detail is missing", async () => {
    // Table id from the quality check row has no matching detail — happens
    // on deleted/hidden tables. Triage should still emit an entry with
    // tableName 'unknown' and popularity 0 rather than swallowing the
    // failure.
    const client = makeRouter({
      checks: [{ id: "q1", name: "a", status: "ALERT", tableId: "t-ghost" }],
      tables: new Map(),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    const queue = out.triageQueue as Array<Record<string, unknown>>;
    expect(queue).toHaveLength(1);
    expect(queue[0].tableName).toBe("unknown");
    expect(queue[0].popularity).toBe(0);
    expect(queue[0].triageScore).toBe(0);
  });
});

describe("catalog_triage_quality_failures — owner grouping", () => {
  it("groups by each user owner and each team owner independently", async () => {
    const client = makeRouter({
      checks: [
        { id: "q1", name: "a", status: "ALERT", tableId: "t1" },
        { id: "q2", name: "b", status: "ALERT", tableId: "t2" },
      ],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 1,
            ownerEntities: [
              {
                id: "o1",
                userId: "u1",
                user: { id: "u1", email: "alice@x.com", fullName: "Alice" },
              },
            ],
            teamOwnerEntities: [
              { id: "to1", teamId: "team-1", team: { name: "Data Platform" } },
            ],
          },
        ],
        [
          "t2",
          {
            id: "t2",
            name: "T2",
            numberOfQueries: 1,
            ownerEntities: [
              {
                id: "o2",
                userId: "u2",
                user: { id: "u2", email: "bob@x.com", fullName: "Bob" },
              },
            ],
            teamOwnerEntities: [],
          },
        ],
      ]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    const byOwner = out.byOwner as Record<string, Record<string, unknown>>;
    expect(byOwner["alice@x.com"]).toMatchObject({
      ownerEmail: "alice@x.com",
      tableCount: 1,
      totalFailures: 1,
    });
    expect(byOwner["bob@x.com"]).toMatchObject({
      ownerEmail: "bob@x.com",
      tableCount: 1,
      totalFailures: 1,
    });
    expect(byOwner["team:Data Platform"]).toMatchObject({
      ownerName: "Data Platform",
      tableCount: 1,
    });
    // totalOwners excludes the synthetic __unowned__ bucket.
    expect((out.summary as Record<string, unknown>).totalOwners).toBe(3);
  });

  it("places unowned tables in the synthetic __unowned__ bucket and excludes it from totalOwners", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "a", status: "ALERT", tableId: "t1" }],
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
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    const byOwner = out.byOwner as Record<string, Record<string, unknown>>;
    expect(byOwner.__unowned__).toMatchObject({
      ownerEmail: null,
      ownerName: null,
      tableCount: 1,
      totalFailures: 1,
    });
    expect((out.summary as Record<string, unknown>).totalOwners).toBe(0);
  });

  it("treats orphaned owner rows (userId:null) as unowned", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "a", status: "ALERT", tableId: "t1" }],
      tables: new Map([
        [
          "t1",
          {
            id: "t1",
            name: "T1",
            numberOfQueries: 1,
            ownerEntities: [{ id: "o1", userId: null, user: null }],
            teamOwnerEntities: [],
          },
        ],
      ]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    const byOwner = out.byOwner as Record<string, Record<string, unknown>>;
    expect(byOwner.__unowned__).toBeDefined();
  });
});

describe("catalog_triage_quality_failures — upstream pointers", () => {
  it("includes upstreamPointers with hydrated names when includeUpstreamPointers=true (default)", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "a", status: "ALERT", tableId: "t-child" }],
      tables: new Map([
        [
          "t-child",
          {
            id: "t-child",
            name: "CHILD",
            numberOfQueries: 1,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      upstreamByTable: new Map([["t-child", ["t-parent-1", "t-parent-2"]]]),
      upstreamNamesById: new Map([
        ["t-parent-1", "PARENT_ONE"],
        ["t-parent-2", "PARENT_TWO"],
      ]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(await tool.handler({}));
    const queue = out.triageQueue as Array<Record<string, unknown>>;
    expect(queue[0].upstreamPointers).toEqual([
      { tableId: "t-parent-1", tableName: "PARENT_ONE" },
      { tableId: "t-parent-2", tableName: "PARENT_TWO" },
    ]);
  });

  it("omits upstreamPointers (and skips the lineage fan-out) when includeUpstreamPointers=false", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "a", status: "ALERT", tableId: "t1" }],
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
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(
      await tool.handler({ includeUpstreamPointers: false })
    );
    const queue = out.triageQueue as Array<Record<string, unknown>>;
    expect(queue[0].upstreamPointers).toBeUndefined();
    // No GET_LINEAGES / GET_TABLES_SUMMARY calls when upstream is disabled.
    const documents = client.calls.map((c) => c.document);
    expect(documents).not.toContain(GET_LINEAGES);
    expect(documents).not.toContain(GET_TABLES_SUMMARY);
  });

  it("emits upstreamPointers: [] when a table has no upstream parents", async () => {
    const client = makeRouter({
      checks: [{ id: "q1", name: "a", status: "ALERT", tableId: "t-root" }],
      tables: new Map([
        [
          "t-root",
          {
            id: "t-root",
            name: "ROOT",
            numberOfQueries: 1,
            ownerEntities: [],
            teamOwnerEntities: [],
          },
        ],
      ]),
      upstreamByTable: new Map([["t-root", []]]),
    });
    const tool = defineTriageQualityFailures(client);
    const out = parseResult(await tool.handler({}));
    const queue = out.triageQueue as Array<Record<string, unknown>>;
    expect(queue[0].upstreamPointers).toEqual([]);
  });
});
