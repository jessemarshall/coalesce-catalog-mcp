import { describe, it, expect } from "vitest";
import { defineOwnerScorecard } from "../../src/workflows/owner-scorecard.js";
import {
  GET_USERS,
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_TERMS_DETAIL_BATCH,
  GET_LINEAGES,
  GET_PINNED_ASSETS,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

// ── Fixtures ────────────────────────────────────────────────────────────────

interface MockUser {
  id: string;
  firstName: string;
  lastName: string;
  email: string;
  role: string;
  status?: string;
  isEmailValidated?: boolean;
  createdAt?: string;
  ownedAssetIds: string[];
}

interface MockTable {
  id: string;
  name?: string | null;
  description?: string | null;
  createdAt?: string;
  isVerified?: boolean | null;
  ownerEntities?: unknown[];
  teamOwnerEntities?: unknown[];
  tagEntities?: Array<{ tag?: { label?: string | null } }>;
}

interface MockDashboard extends MockTable {}

interface MockTerm {
  id: string;
  description?: string | null;
  createdAt?: string;
  isVerified?: boolean | null;
  linkedTag?: { id: string; label: string } | null;
  ownerEntities?: unknown[];
  teamOwnerEntities?: unknown[];
  tagEntities?: Array<{ tag?: { label?: string | null } }>;
}

interface RouterOpts {
  users: MockUser[];
  tables: MockTable[];
  dashboards: MockDashboard[];
  terms: MockTerm[];
  lineageByTable?: Map<string, { upstream: number; downstream: number }>;
  termsWithOutboundPins?: Set<string>;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_USERS) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = opts.users.slice(start, start + vars.pagination.nbPerPage);
      return { getUsers: slice };
    }
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as {
        scope?: { ids?: string[] };
        pagination: { nbPerPage: number; page: number };
      };
      const ids = vars.scope?.ids ?? [];
      const matching = opts.tables.filter((t) => ids.includes(t.id));
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = matching.slice(start, start + vars.pagination.nbPerPage);
      return {
        getTables: {
          totalCount: matching.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_DASHBOARDS_DETAIL_BATCH) {
      const vars = variables as {
        scope?: { ids?: string[] };
        pagination: { nbPerPage: number; page: number };
      };
      const ids = vars.scope?.ids ?? [];
      const matching = opts.dashboards.filter((d) => ids.includes(d.id));
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = matching.slice(start, start + vars.pagination.nbPerPage);
      return {
        getDashboards: {
          totalCount: matching.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_TERMS_DETAIL_BATCH) {
      const vars = variables as {
        scope?: { ids?: string[] };
        pagination: { nbPerPage: number; page: number };
      };
      const ids = vars.scope?.ids ?? [];
      const matching = opts.terms.filter((t) => ids.includes(t.id));
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = matching.slice(start, start + vars.pagination.nbPerPage);
      return {
        getTerms: {
          totalCount: matching.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: { parentTableId?: string; childTableId?: string };
      };
      const edges =
        opts.lineageByTable?.get(
          vars.scope?.parentTableId ?? vars.scope?.childTableId ?? ""
        ) ?? { upstream: 0, downstream: 0 };
      // Tool counts edges returned; fabricate N rows of a direction, but only
      // in response to the matching scope.
      if (vars.scope?.parentTableId) {
        return {
          getLineages: {
            data: Array.from({ length: edges.downstream }, (_, i) => ({
              id: `l-down-${i}`,
              parentTableId: vars.scope?.parentTableId,
              childTableId: `child-${i}`,
            })),
          },
        };
      }
      return {
        getLineages: {
          data: Array.from({ length: edges.upstream }, (_, i) => ({
            id: `l-up-${i}`,
            parentTableId: `parent-${i}`,
            childTableId: vars.scope?.childTableId,
          })),
        },
      };
    }
    if (document === GET_PINNED_ASSETS) {
      const vars = variables as { scope?: { fromTermIds?: string[] } };
      const candidates = vars.scope?.fromTermIds ?? [];
      const data = candidates
        .filter((id) => opts.termsWithOutboundPins?.has(id) === true)
        .map((id, i) => ({
          id: `pa-${i}`,
          fromTermId: id,
        }));
      return {
        getPinnedAssets: {
          totalCount: data.length,
          nbPerPage: 500,
          page: 0,
          data,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 80)}`);
  });
}

const ADA: MockUser = {
  id: "u-ada",
  firstName: "Ada",
  lastName: "Lovelace",
  email: "ada@example.com",
  role: "ADMIN",
  ownedAssetIds: ["tbl-1", "dash-1", "term-1"],
};

function findTool() {
  const client = makeMockClient(() => ({}));
  return defineOwnerScorecard(client);
}

// ── Input schema ────────────────────────────────────────────────────────────

describe("catalog_owner_scorecard input schema", () => {
  it("registers with the expected name", () => {
    expect(findTool().name).toBe("catalog_owner_scorecard");
  });

  it("is a read-only tool", () => {
    expect(findTool().config.annotations?.readOnlyHint).toBe(true);
  });

  it("description mentions the paired prompt", () => {
    expect(findTool().config.description).toMatch(/catalog-daily-guide/);
  });

  it("description mentions the no-silent-truncation contract", () => {
    expect(findTool().config.description).toMatch(/refuses|refusal/i);
  });
});

// ── User lookup ─────────────────────────────────────────────────────────────

describe("catalog_owner_scorecard user lookup", () => {
  it("resolves the user by email (case-insensitive)", async () => {
    const router = makeRouter({
      users: [ADA],
      tables: [],
      dashboards: [],
      terms: [],
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ADA@Example.COM" });
    const parsed = parseResult(res);
    expect(parsed.identity).toMatchObject({
      userId: "u-ada",
      email: "ada@example.com",
      firstName: "Ada",
      lastName: "Lovelace",
      ownedAssetCount: 3,
    });
  });

  it("returns notFound when the email does not appear in any page (short page confirms absence)", async () => {
    const router = makeRouter({
      users: [ADA],
      tables: [],
      dashboards: [],
      terms: [],
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ghost@example.com" });
    const parsed = parseResult(res);
    expect(parsed).toMatchObject({ notFound: true, email: "ghost@example.com" });
    expect(parsed.reason).toMatch(/confirmed absent/i);
  });

  it("throws (isError) when the user-scan ceiling is hit without a match", async () => {
    // Return a full page every time so the ceiling triggers — simulates a
    // tenant larger than USER_LOOKUP_PAGE_SIZE * USER_LOOKUP_MAX_PAGES users.
    const client = makeMockClient((document, variables) => {
      if (document === GET_USERS) {
        const vars = variables as {
          pagination: { nbPerPage: number; page: number };
        };
        return {
          getUsers: Array.from({ length: vars.pagination.nbPerPage }, (_, i) => ({
            id: `u-page${vars.pagination.page}-${i}`,
            firstName: "Filler",
            lastName: "User",
            email: `filler-${vars.pagination.page}-${i}@example.com`,
            role: "VIEWER",
            ownedAssetIds: [],
          })),
        };
      }
      throw new Error(`unexpected document: ${document.slice(0, 60)}`);
    });
    const tool = defineOwnerScorecard(client);
    const res = await tool.handler({ email: "ghost@example.com" });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/did not reach the end of the user directory/i);
  });

  it("emits an empty scorecard for an owner with zero owned assets", async () => {
    const empty: MockUser = { ...ADA, ownedAssetIds: [] };
    const router = makeRouter({
      users: [empty],
      tables: [],
      dashboards: [],
      terms: [],
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res);
    expect(parsed.identity).toMatchObject({
      ownedAssetCount: 0,
      ownedAssetUniqueCount: 0,
    });
    expect(parsed.tables).toMatchObject({ total: 0 });
    expect(parsed.dashboards).toMatchObject({ total: 0 });
    expect(parsed.terms).toMatchObject({ total: 0 });
    expect(parsed.unclassified_owned_ids).toEqual([]);
  });
});

// ── Categorisation: tables ──────────────────────────────────────────────────

describe("catalog_owner_scorecard table categorisation", () => {
  // Compute timestamps relative to actual Date.now() — the tool stamps `asOf`
  // off real-time, so a fixed-NOW fixture would silently age out of the
  // newAssetDays window as wall-clock time advances.
  const NOW = Date.now();
  const twoDaysAgo = new Date(NOW - 2 * 24 * 3600 * 1000).toISOString();
  const tenDaysAgo = new Date(NOW - 10 * 24 * 3600 * 1000).toISOString();
  const longAgo = new Date(NOW - 365 * 24 * 3600 * 1000).toISOString();

  const tbls: MockTable[] = [
    {
      id: "tbl-new-thin",
      description: "",
      createdAt: twoDaysAgo,
      isVerified: false,
      tagEntities: [],
    },
    {
      id: "tbl-old-verified-domain",
      description: "Plenty of descriptive prose here to exceed the threshold.",
      createdAt: longAgo,
      isVerified: true,
      tagEntities: [
        { tag: { label: "domain:sales" } },
        { tag: { label: "pii" } },
      ],
    },
    {
      id: "tbl-old-uncertified",
      description: "Has description but not verified yet, still plenty long.",
      createdAt: tenDaysAgo,
      isVerified: false,
      tagEntities: [{ tag: { label: "domain:marketing" } }],
    },
  ];

  it("flags thin descriptions, PII, new, uncertified, and no-domain-tag correctly", async () => {
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: tbls.map((t) => t.id) }],
      tables: tbls,
      dashboards: [],
      terms: [],
      lineageByTable: new Map(
        tbls.map((t) => [t.id, { upstream: 1, downstream: 1 }])
      ),
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const table = parsed.tables as { findings: Record<string, string[]> };
    const f = table.findings;
    expect(f.thin_description_ids).toEqual(["tbl-new-thin"]);
    expect(f.pii_tagged_ids).toEqual(["tbl-old-verified-domain"]);
    expect(f.new_asset_ids).toEqual(["tbl-new-thin"]);
    expect(f.uncertified_ids.sort()).toEqual(
      ["tbl-new-thin", "tbl-old-uncertified"].sort()
    );
    expect(f.no_domain_tag_ids).toEqual(["tbl-new-thin"]);
  });

  it("sorts ID arrays newest-first by createdAt DESC", async () => {
    const ids = [
      { id: "t-old", createdAt: longAgo },
      { id: "t-mid", createdAt: tenDaysAgo },
      { id: "t-new", createdAt: twoDaysAgo },
    ].map(
      (s): MockTable => ({
        id: s.id,
        description: "",
        createdAt: s.createdAt,
        isVerified: false,
        tagEntities: [],
      })
    );
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: ids.map((t) => t.id) }],
      tables: ids,
      dashboards: [],
      terms: [],
      lineageByTable: new Map(
        ids.map((t) => [t.id, { upstream: 1, downstream: 1 }])
      ),
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.tables as { findings: Record<string, string[]> }).findings;
    expect(f.thin_description_ids).toEqual(["t-new", "t-mid", "t-old"]);
  });

  it("classifies lineage isolation / upstream-only / downstream-only exactly", async () => {
    const tbs: MockTable[] = [
      {
        id: "t-isolated",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "domain:x" } }],
      },
      {
        id: "t-up-only",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "domain:x" } }],
      },
      {
        id: "t-down-only",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "domain:x" } }],
      },
      {
        id: "t-both",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "domain:x" } }],
      },
    ];
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: tbs.map((t) => t.id) }],
      tables: tbs,
      dashboards: [],
      terms: [],
      lineageByTable: new Map([
        ["t-isolated", { upstream: 0, downstream: 0 }],
        ["t-up-only", { upstream: 3, downstream: 0 }],
        ["t-down-only", { upstream: 0, downstream: 2 }],
        ["t-both", { upstream: 1, downstream: 4 }],
      ]),
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.tables as { findings: Record<string, string[]> }).findings;
    expect(f.lineage_isolated_ids).toEqual(["t-isolated"]);
    expect(f.lineage_upstream_only_ids).toEqual(["t-up-only"]);
    expect(f.lineage_downstream_only_ids).toEqual(["t-down-only"]);
  });

  it("honours a custom domainTagPrefix override", async () => {
    const tbs: MockTable[] = [
      {
        id: "t-has-biz",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "BusinessUnit:sales" } }],
      },
      {
        id: "t-has-domain",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "domain:sales" } }],
      },
    ];
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: tbs.map((t) => t.id) }],
      tables: tbs,
      dashboards: [],
      terms: [],
      lineageByTable: new Map(
        tbs.map((t) => [t.id, { upstream: 1, downstream: 1 }])
      ),
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({
      email: "ada@example.com",
      domainTagPrefix: "BusinessUnit:",
    });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.tables as { findings: Record<string, string[]> }).findings;
    expect(f.no_domain_tag_ids).toEqual(["t-has-domain"]);
  });

  it("honours a custom piiTagPattern override", async () => {
    const tbs: MockTable[] = [
      {
        id: "t-gdpr",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "regulation:gdpr" } }],
      },
      {
        id: "t-plain",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [{ tag: { label: "domain:x" } }],
      },
    ];
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: tbs.map((t) => t.id) }],
      tables: tbs,
      dashboards: [],
      terms: [],
      lineageByTable: new Map(
        tbs.map((t) => [t.id, { upstream: 1, downstream: 1 }])
      ),
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({
      email: "ada@example.com",
      piiTagPattern: "gdpr|hipaa",
    });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.tables as { findings: Record<string, string[]> }).findings;
    expect(f.pii_tagged_ids).toEqual(["t-gdpr"]);
  });
});

// ── Categorisation: dashboards ──────────────────────────────────────────────

describe("catalog_owner_scorecard dashboard categorisation", () => {
  it("flags dashboards on the five applicable categories (no lineage)", async () => {
    const dashes: MockDashboard[] = [
      {
        id: "d-thin",
        description: "",
        isVerified: false,
        tagEntities: [],
      },
      {
        id: "d-ok",
        description: "A proper long description for the dashboard.",
        isVerified: true,
        tagEntities: [{ tag: { label: "domain:product" } }],
      },
    ];
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: dashes.map((d) => d.id) }],
      tables: [],
      dashboards: dashes,
      terms: [],
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.dashboards as { findings: Record<string, string[]> }).findings;
    expect(f.thin_description_ids).toEqual(["d-thin"]);
    expect(f.uncertified_ids).toEqual(["d-thin"]);
    expect(f.no_domain_tag_ids).toEqual(["d-thin"]);
    // Dashboard findings never include lineage keys.
    expect("lineage_isolated_ids" in f).toBe(false);
  });
});

// ── Categorisation: terms ───────────────────────────────────────────────────

describe("catalog_owner_scorecard term categorisation", () => {
  it("detects orphaned terms only when linkedTag + tagEntities + outbound-pins are all empty", async () => {
    const termRows: MockTerm[] = [
      {
        id: "tm-orphan",
        description: "definition of x".repeat(5),
        isVerified: true,
        linkedTag: null,
        tagEntities: [],
        ownerEntities: [{ id: "oe1", userId: "u-1" }],
      },
      {
        id: "tm-has-linked-tag",
        description: "definition of y".repeat(5),
        isVerified: true,
        linkedTag: { id: "tag-y", label: "linked" },
        tagEntities: [],
        ownerEntities: [{ id: "oe2", userId: "u-1" }],
      },
      {
        id: "tm-has-attached-tag",
        description: "definition of z".repeat(5),
        isVerified: true,
        linkedTag: null,
        tagEntities: [{ tag: { label: "domain:foo" } }],
        ownerEntities: [{ id: "oe3", userId: "u-1" }],
      },
      {
        id: "tm-has-outbound-pin",
        description: "definition of w".repeat(5),
        isVerified: true,
        linkedTag: null,
        tagEntities: [],
        ownerEntities: [{ id: "oe4", userId: "u-1" }],
      },
    ];
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: termRows.map((t) => t.id) }],
      tables: [],
      dashboards: [],
      terms: termRows,
      termsWithOutboundPins: new Set(["tm-has-outbound-pin"]),
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.terms as { findings: Record<string, string[]> }).findings;
    expect(f.orphaned_ids).toEqual(["tm-orphan"]);
  });

  it("flags missing_owner when both ownerEntities and teamOwnerEntities are empty or null-bound", async () => {
    const termRows: MockTerm[] = [
      {
        id: "tm-no-owner",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [],
        ownerEntities: [],
        teamOwnerEntities: [],
      },
      {
        id: "tm-null-bindings",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [],
        // Owner/team entities exist but their FK points at nothing — same as
        // unowned for audit purposes.
        ownerEntities: [{ id: "oe-a", userId: null }],
        teamOwnerEntities: [{ id: "toe-b", teamId: null }],
      },
      {
        id: "tm-has-owner",
        description: "x".repeat(50),
        isVerified: true,
        tagEntities: [],
        ownerEntities: [{ id: "oe-c", userId: "u-1" }],
      },
    ];
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: termRows.map((t) => t.id) }],
      tables: [],
      dashboards: [],
      terms: termRows,
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.terms as { findings: Record<string, string[]> }).findings;
    expect(f.missing_owner_ids.sort()).toEqual(
      ["tm-no-owner", "tm-null-bindings"].sort()
    );
  });

  it("flags thin term descriptions on the description field", async () => {
    const termRows: MockTerm[] = [
      {
        id: "tm-thin",
        description: "short",
        isVerified: true,
        tagEntities: [],
        ownerEntities: [{ id: "oe", userId: "u-1" }],
      },
    ];
    const router = makeRouter({
      users: [{ ...ADA, ownedAssetIds: termRows.map((t) => t.id) }],
      tables: [],
      dashboards: [],
      terms: termRows,
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const f = (parsed.terms as { findings: Record<string, string[]> }).findings;
    expect(f.thin_description_ids).toEqual(["tm-thin"]);
  });
});

// ── Large owners (>500 owned assets per type) ───────────────────────────────

describe("catalog_owner_scorecard large-owner hydration", () => {
  it("chunks ownedAssetIds into batches of 500 for the `ids` scope filter", async () => {
    // 650 owned tables — the scope.ids filter must never carry more than 500
    // at a time, otherwise the server would silently drop the tail.
    const ids = Array.from({ length: 650 }, (_, i) => `tbl-${i}`);
    const tbls: MockTable[] = ids.map((id) => ({
      id,
      description: "x".repeat(50),
      isVerified: true,
      tagEntities: [{ tag: { label: "domain:x" } }],
    }));
    const idsSeen: number[] = [];
    const client = makeMockClient((document, variables) => {
      if (document === GET_USERS) {
        const vars = variables as {
          pagination: { nbPerPage: number; page: number };
        };
        if (vars.pagination.page === 0) {
          return { getUsers: [{ ...ADA, ownedAssetIds: ids }] };
        }
        return { getUsers: [] };
      }
      if (document === GET_TABLES_DETAIL_BATCH) {
        const vars = variables as { scope?: { ids?: string[] } };
        const batchIds = vars.scope?.ids ?? [];
        idsSeen.push(batchIds.length);
        const matching = tbls.filter((t) => batchIds.includes(t.id));
        return {
          getTables: {
            totalCount: matching.length,
            nbPerPage: batchIds.length,
            page: 0,
            data: matching,
          },
        };
      }
      if (document === GET_DASHBOARDS_DETAIL_BATCH) {
        return {
          getDashboards: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      if (document === GET_TERMS_DETAIL_BATCH) {
        return {
          getTerms: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      if (document === GET_LINEAGES) {
        return { getLineages: { data: [{ id: "edge" }] } };
      }
      if (document === GET_PINNED_ASSETS) {
        return {
          getPinnedAssets: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      throw new Error(`unexpected document: ${document.slice(0, 60)}`);
    });
    const tool = defineOwnerScorecard(client);
    const res = await tool.handler({ email: "ada@example.com" });
    expect(res.isError).toBeUndefined();
    expect(idsSeen.length).toBe(2);
    expect(Math.max(...idsSeen)).toBeLessThanOrEqual(500);
    expect(idsSeen.reduce((a, b) => a + b, 0)).toBe(650);
    const parsed = parseResult(res) as Record<string, unknown>;
    expect((parsed.tables as { total: number }).total).toBe(650);
  });
});

// ── Fan-out + pagination ceilings ───────────────────────────────────────────

describe("catalog_owner_scorecard failure modes", () => {
  it("throws (isError) if lineage pagination ceiling is exceeded for a single table", async () => {
    // A lineage response saturating the page size forever will trip the
    // per-node page ceiling. Simulate by always returning PAGE_SIZE rows.
    const client = makeMockClient((document, variables) => {
      if (document === GET_USERS) {
        const vars = variables as {
          pagination: { page: number; nbPerPage: number };
        };
        if (vars.pagination.page === 0) {
          return {
            getUsers: [{ ...ADA, ownedAssetIds: ["tbl-hot"] }],
          };
        }
        return { getUsers: [] };
      }
      if (document === GET_TABLES_DETAIL_BATCH) {
        return {
          getTables: {
            totalCount: 1,
            nbPerPage: 500,
            page: 0,
            data: [
              {
                id: "tbl-hot",
                description: "x".repeat(50),
                isVerified: true,
                tagEntities: [{ tag: { label: "domain:x" } }],
              },
            ],
          },
        };
      }
      if (document === GET_DASHBOARDS_DETAIL_BATCH) {
        return {
          getDashboards: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      if (document === GET_TERMS_DETAIL_BATCH) {
        return {
          getTerms: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      if (document === GET_LINEAGES) {
        return {
          getLineages: {
            data: Array.from({ length: 500 }, (_, i) => ({
              id: `l-${i}`,
            })),
          },
        };
      }
      if (document === GET_PINNED_ASSETS) {
        return {
          getPinnedAssets: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      throw new Error(`unexpected document: ${document.slice(0, 60)}`);
    });
    const tool = defineOwnerScorecard(client);
    const res = await tool.handler({ email: "ada@example.com" });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/Lineage pagination exceeded/);
  });

  it("throws (isError) if pinned-assets pagination ceiling is exceeded", async () => {
    const client = makeMockClient((document, variables) => {
      if (document === GET_USERS) {
        const vars = variables as {
          pagination: { page: number; nbPerPage: number };
        };
        if (vars.pagination.page === 0) {
          return {
            getUsers: [{ ...ADA, ownedAssetIds: ["tm-hot"] }],
          };
        }
        return { getUsers: [] };
      }
      if (document === GET_TABLES_DETAIL_BATCH) {
        return {
          getTables: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      if (document === GET_DASHBOARDS_DETAIL_BATCH) {
        return {
          getDashboards: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      if (document === GET_TERMS_DETAIL_BATCH) {
        return {
          getTerms: {
            totalCount: 1,
            nbPerPage: 500,
            page: 0,
            data: [
              {
                id: "tm-hot",
                description: "x".repeat(50),
                isVerified: true,
                ownerEntities: [{ id: "oe", userId: "u-1" }],
              },
            ],
          },
        };
      }
      if (document === GET_PINNED_ASSETS) {
        // Always a full page of results → trips the pagination ceiling.
        return {
          getPinnedAssets: {
            totalCount: 99999,
            nbPerPage: 500,
            page: 0,
            data: Array.from({ length: 500 }, (_, i) => ({
              id: `pa-${i}`,
              fromTermId: "tm-hot",
            })),
          },
        };
      }
      throw new Error(`unexpected document: ${document.slice(0, 60)}`);
    });
    const tool = defineOwnerScorecard(client);
    const res = await tool.handler({ email: "ada@example.com" });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/Pinned-asset pagination exceeded/);
  });

  it("surfaces transport errors as structured isError results", async () => {
    const client = makeMockClient(() => {
      throw new Error("graphql transport failed");
    });
    const tool = defineOwnerScorecard(client);
    const res = await tool.handler({ email: "ada@example.com" });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/graphql transport failed/);
  });
});

// ── Response shape ──────────────────────────────────────────────────────────

describe("catalog_owner_scorecard response shape", () => {
  it("returns identity, asOf, params, all three asset buckets, and unclassified_owned_ids", async () => {
    const router = makeRouter({
      users: [ADA],
      tables: [
        {
          id: "tbl-1",
          description: "x".repeat(50),
          isVerified: true,
          tagEntities: [{ tag: { label: "domain:x" } }],
        },
      ],
      dashboards: [
        {
          id: "dash-1",
          description: "x".repeat(50),
          isVerified: true,
          tagEntities: [{ tag: { label: "domain:x" } }],
        },
      ],
      terms: [
        {
          id: "term-1",
          description: "x".repeat(50),
          isVerified: true,
          tagEntities: [],
          ownerEntities: [{ id: "oe", userId: "u-1" }],
          linkedTag: { id: "tag-linked", label: "linked" },
        },
      ],
      lineageByTable: new Map([["tbl-1", { upstream: 1, downstream: 1 }]]),
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed.identity).toBeDefined();
    expect(typeof parsed.asOf).toBe("string");
    expect(parsed.params).toMatchObject({
      domainTagPrefix: "domain:",
      newAssetDays: 7,
      piiTagPattern: "pii|phi|pci",
    });
    expect(parsed.tables).toMatchObject({ total: 1 });
    expect(parsed.dashboards).toMatchObject({ total: 1 });
    expect(parsed.terms).toMatchObject({ total: 1 });
    expect(parsed.unclassified_owned_ids).toEqual([]);
  });

  it("surfaces owned IDs that don't resolve as table/dashboard/term in unclassified_owned_ids", async () => {
    // Owner holds a column UUID + a stale reference alongside one real term.
    // Hydrators silently filter these out; scorecard must reconcile the gap.
    const owner: MockUser = {
      ...ADA,
      ownedAssetIds: ["col-legacy", "term-real", "ghost-deleted-asset"],
    };
    const router = makeRouter({
      users: [owner],
      tables: [],
      dashboards: [],
      terms: [
        {
          id: "term-real",
          description: "x".repeat(50),
          isVerified: true,
          tagEntities: [],
          ownerEntities: [{ id: "oe", userId: "u-1" }],
          linkedTag: { id: "tag-y", label: "linked" },
        },
      ],
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    expect((parsed.tables as { total: number }).total).toBe(0);
    expect((parsed.dashboards as { total: number }).total).toBe(0);
    expect((parsed.terms as { total: number }).total).toBe(1);
    expect(parsed.unclassified_owned_ids).toEqual(
      expect.arrayContaining(["col-legacy", "ghost-deleted-asset"])
    );
    expect((parsed.unclassified_owned_ids as string[]).length).toBe(2);
    const identity = parsed.identity as {
      ownedAssetCount: number;
      ownedAssetUniqueCount: number;
    };
    expect(identity.ownedAssetCount).toBe(3);
    expect(identity.ownedAssetUniqueCount).toBe(3);
    // Arithmetic reconciliation must close exactly.
    const total =
      (parsed.tables as { total: number }).total +
      (parsed.dashboards as { total: number }).total +
      (parsed.terms as { total: number }).total +
      (parsed.unclassified_owned_ids as string[]).length;
    expect(total).toBe(identity.ownedAssetUniqueCount);
  });

  it("reconciles ownedAssetUniqueCount (deduped) vs ownedAssetCount (raw) when the API emits duplicates", async () => {
    // Defensive: if `ownedAssetIds` ever contains a duplicate UUID, the raw
    // count inflates but unique reconciliation still closes.
    const owner: MockUser = {
      ...ADA,
      ownedAssetIds: ["term-real", "term-real", "col-legacy"],
    };
    const router = makeRouter({
      users: [owner],
      tables: [],
      dashboards: [],
      terms: [
        {
          id: "term-real",
          description: "x".repeat(50),
          isVerified: true,
          tagEntities: [],
          ownerEntities: [{ id: "oe", userId: "u-1" }],
          linkedTag: { id: "tag-y", label: "linked" },
        },
      ],
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({ email: "ada@example.com" });
    const parsed = parseResult(res) as Record<string, unknown>;
    const identity = parsed.identity as {
      ownedAssetCount: number;
      ownedAssetUniqueCount: number;
    };
    expect(identity.ownedAssetCount).toBe(3);
    expect(identity.ownedAssetUniqueCount).toBe(2);
    expect((parsed.terms as { total: number }).total).toBe(1);
    expect((parsed.unclassified_owned_ids as string[]).length).toBe(1);
    const total =
      (parsed.tables as { total: number }).total +
      (parsed.dashboards as { total: number }).total +
      (parsed.terms as { total: number }).total +
      (parsed.unclassified_owned_ids as string[]).length;
    expect(total).toBe(identity.ownedAssetUniqueCount);
  });

  it("reflects overridden params back in the response", async () => {
    const router = makeRouter({
      users: [ADA],
      tables: [],
      dashboards: [],
      terms: [],
    });
    const tool = defineOwnerScorecard(router);
    const res = await tool.handler({
      email: "ada@example.com",
      domainTagPrefix: "business:",
      newAssetDays: 30,
      piiTagPattern: "gdpr|ccpa",
    });
    const parsed = parseResult(res) as Record<string, unknown>;
    expect(parsed.params).toEqual({
      domainTagPrefix: "business:",
      newAssetDays: 30,
      piiTagPattern: "gdpr|ccpa",
    });
  });
});
