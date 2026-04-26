import { describe, it, expect } from "vitest";
import { defineReconcileOwnershipHandoff } from "../../src/workflows/reconcile-ownership-handoff.js";
import {
  GET_USERS,
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_TERMS_DETAIL_BATCH,
  GET_TEAMS,
  GET_LINEAGES,
  GET_TABLE_QUERIES,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
  isError?: boolean;
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

// ── Fixture shapes ──────────────────────────────────────────────────────────

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
  teamIds?: string[];
}

interface MockTable {
  id: string;
  name?: string | null;
  popularity?: number | null;
  numberOfQueries?: number | null;
  lastQueriedAt?: number | null;
  schemaId?: string | null;
  createdAt?: string;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
}

interface MockDashboard {
  id: string;
  name?: string | null;
  popularity?: number | null;
  createdAt?: string;
}

interface MockTerm {
  id: string;
  name?: string | null;
  description?: string | null;
  linkedTag?: { id?: string; label?: string };
}

interface MockTeam {
  id: string;
  name?: string;
  memberIds: string[];
  createdAt?: string;
}

interface MockQuery {
  author?: string | null;
  queryType?: string;
}

interface MockEdge {
  id?: string;
  parentTableId?: string;
  childTableId?: string;
  parentDashboardId?: string;
  childDashboardId?: string;
}

interface RouterOpts {
  users: MockUser[];
  tables: MockTable[];
  dashboards: MockDashboard[];
  terms: MockTerm[];
  teams: MockTeam[];
  // keyed as `${tableId}|${direction}` where direction is 'upstream'|'downstream'
  edgesByKey?: Map<string, MockEdge[]>;
  // keyed as `${dashboardId}|downstream`
  dashboardEdgesByKey?: Map<string, MockEdge[]>;
  // keyed as `${dashboardId}|upstream` (table parents of a dashboard)
  dashboardUpstreamEdgesByKey?: Map<string, MockEdge[]>;
  queriesByTableId?: Map<string, MockQuery[]>;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_USERS) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      return {
        getUsers: opts.users.slice(start, start + vars.pagination.nbPerPage),
      };
    }
    if (document === GET_TEAMS) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      return {
        getTeams: opts.teams.slice(start, start + vars.pagination.nbPerPage),
      };
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
        scope?: {
          parentTableId?: string;
          childTableId?: string;
          parentDashboardId?: string;
          childDashboardId?: string;
        };
        pagination: { nbPerPage: number; page: number };
      };
      if (vars.scope?.parentDashboardId) {
        const edges =
          opts.dashboardEdgesByKey?.get(
            `${vars.scope.parentDashboardId}|downstream`
          ) ?? [];
        return {
          getLineages: {
            totalCount: edges.length,
            nbPerPage: vars.pagination.nbPerPage,
            page: vars.pagination.page,
            data: edges,
          },
        };
      }
      if (vars.scope?.childDashboardId) {
        const edges =
          opts.dashboardUpstreamEdgesByKey?.get(
            `${vars.scope.childDashboardId}|upstream`
          ) ?? [];
        return {
          getLineages: {
            totalCount: edges.length,
            nbPerPage: vars.pagination.nbPerPage,
            page: vars.pagination.page,
            data: edges,
          },
        };
      }
      const direction = vars.scope?.parentTableId ? "downstream" : "upstream";
      const tid =
        vars.scope?.parentTableId ?? vars.scope?.childTableId ?? "";
      const edges = opts.edgesByKey?.get(`${tid}|${direction}`) ?? [];
      return {
        getLineages: {
          totalCount: edges.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: edges,
        },
      };
    }
    if (document === GET_TABLE_QUERIES) {
      const vars = variables as {
        scope?: { tableIds?: string[] };
        pagination: { nbPerPage: number; page: number };
      };
      const id = vars.scope?.tableIds?.[0] ?? "";
      const rows = opts.queriesByTableId?.get(id) ?? [];
      return {
        getTableQueries: {
          totalCount: rows.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: 0,
          data: rows,
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 80)}`);
  });
}

function findTool() {
  const client = makeMockClient(() => ({}));
  return defineReconcileOwnershipHandoff(client);
}

// ── Registration / schema ───────────────────────────────────────────────────

describe("catalog_reconcile_ownership_handoff — registration", () => {
  it("registers with the expected name", () => {
    expect(findTool().name).toBe("catalog_reconcile_ownership_handoff");
  });
  it("is a read-only tool", () => {
    expect(findTool().config.annotations?.readOnlyHint).toBe(true);
  });
  it("description mentions the no-silent-truncation contract", () => {
    expect(findTool().config.description).toMatch(/refuses|refusal/i);
  });
  it("description mentions how it differs from neighboring tools", () => {
    const desc = findTool().config.description;
    expect(desc).toMatch(/catalog_assess_impact/);
    expect(desc).toMatch(/catalog_resolve_ownership_gaps/);
  });
});

// ── User lookup ─────────────────────────────────────────────────────────────

describe("catalog_reconcile_ownership_handoff — user lookup", () => {
  it("returns notFound when the user does not exist (short page reached)", async () => {
    const client = makeRouter({
      users: [],
      tables: [],
      dashboards: [],
      terms: [],
      teams: [],
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(
      await tool.handler({ email: "ghost@example.com" })
    );
    expect(out.notFound).toBe(true);
    expect(out.email).toBe("ghost@example.com");
  });

  it("resolves the user by email case-insensitively", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "Ada",
      lastName: "L",
      email: "Ada.L@example.com",
      role: "MEMBER",
      ownedAssetIds: [],
    };
    const client = makeRouter({
      users: [ada],
      tables: [],
      dashboards: [],
      terms: [],
      teams: [],
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(await tool.handler({ email: "ADA.L@example.com" }));
    expect(out.notFound).toBeUndefined();
    const identity = out.identity as Record<string, unknown>;
    expect(identity.userId).toBe("u-ada");
    expect(identity.email).toBe("Ada.L@example.com");
  });
});

// ── Capacity gate ───────────────────────────────────────────────────────────

describe("catalog_reconcile_ownership_handoff — capacity gate", () => {
  it("refuses when unique owned-asset count exceeds the 200 cap", async () => {
    const ownedIds = Array.from({ length: 201 }, (_, i) => `a-${i}`);
    const user: MockUser = {
      id: "u-big",
      firstName: "Big",
      lastName: "Owner",
      email: "big@example.com",
      role: "MEMBER",
      ownedAssetIds: ownedIds,
    };
    const client = makeRouter({
      users: [user],
      tables: [],
      dashboards: [],
      terms: [],
      teams: [],
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const res = await tool.handler({ email: "big@example.com" });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/201 unique owned assets/);
    expect(parseResult(res).error).toMatch(/200-asset handoff cap/);
  });

  it("dedupes repeated IDs before applying the cap", async () => {
    // 201 raw IDs but only 1 unique → below cap, should succeed
    const rawIds = Array.from({ length: 201 }, () => "a-only");
    const user: MockUser = {
      id: "u-dup",
      firstName: "Dup",
      lastName: "Owner",
      email: "dup@example.com",
      role: "MEMBER",
      ownedAssetIds: rawIds,
    };
    const client = makeRouter({
      users: [user],
      tables: [
        {
          id: "a-only",
          name: "Only",
          popularity: 0.1,
          numberOfQueries: 0,
        },
      ],
      dashboards: [],
      terms: [],
      teams: [],
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(
      await tool.handler({
        email: "dup@example.com",
        includeQueryAuthors: false,
        includeLineageNeighbors: false,
        includeTeamContext: false,
      })
    );
    const identity = out.identity as Record<string, unknown>;
    expect(identity.ownedAssetCount).toBe(201);
    expect(identity.ownedAssetUniqueCount).toBe(1);
  });
});

// ── Happy path ──────────────────────────────────────────────────────────────

describe("catalog_reconcile_ownership_handoff — happy path composition", () => {
  it("emits a ranked handoff queue with evidence and candidate aggregation", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "Ada",
      lastName: "Lovelace",
      email: "ada@example.com",
      role: "MEMBER",
      ownedAssetIds: ["tbl-A", "tbl-B", "dash-1", "term-1", "col-ghost"],
    };
    const carol: MockUser = {
      id: "u-carol",
      firstName: "Carol",
      lastName: "Q",
      email: "carol@example.com",
      role: "MEMBER",
      ownedAssetIds: [],
    };
    // tbl-A: popular, many queries, downstream-heavy
    // tbl-B: niche, rarely queried, no downstream
    const tables: MockTable[] = [
      {
        id: "tbl-A",
        name: "TBL_A",
        popularity: 0.9,
        numberOfQueries: 1000,
        lastQueriedAt: Date.now(),
      },
      {
        id: "tbl-B",
        name: "TBL_B",
        popularity: 0.1,
        numberOfQueries: 2,
        lastQueriedAt: Date.now(),
      },
    ];
    const dashboards: MockDashboard[] = [
      { id: "dash-1", name: "Revenue", popularity: 0.7 },
    ];
    const terms: MockTerm[] = [
      {
        id: "term-1",
        name: "CustomerLifetimeValue",
        description: "Business term",
        linkedTag: { id: "tag-1", label: "pii" },
      },
    ];
    const teams: MockTeam[] = [
      { id: "team-fin", name: "Finance", memberIds: ["u-carol"] },
    ];
    // Edges: tbl-A has 3 downstream consumers (2 tables + 1 dashboard) and 1
    // upstream table (owned by Carol). tbl-B has no consumers and no upstream.
    // dash-1 has 1 downstream embed.
    const edgesByKey = new Map<string, MockEdge[]>([
      [
        "tbl-A|downstream",
        [
          { id: "l1", parentTableId: "tbl-A", childTableId: "t-c1" },
          { id: "l2", parentTableId: "tbl-A", childTableId: "t-c2" },
          { id: "l3", parentTableId: "tbl-A", childDashboardId: "d-c1" },
        ],
      ],
      [
        "tbl-A|upstream",
        [{ id: "l4", parentTableId: "t-up", childTableId: "tbl-A" }],
      ],
      ["tbl-B|downstream", []],
      ["tbl-B|upstream", []],
    ]);
    const dashboardEdgesByKey = new Map<string, MockEdge[]>([
      [
        "dash-1|downstream",
        [{ id: "d-l1", parentDashboardId: "dash-1", childDashboardId: "d-c2" }],
      ],
    ]);
    const queriesByTableId = new Map<string, MockQuery[]>([
      [
        "tbl-A",
        [
          { author: "carol@example.com", queryType: "SELECT" },
          { author: "carol@example.com", queryType: "SELECT" },
          { author: "dan@example.com", queryType: "WRITE" },
          { author: null }, // null author is dropped
        ],
      ],
      ["tbl-B", []],
    ]);

    // Neighbor enrichment: t-up is a table owned by Carol, t-c1 and t-c2 are
    // unowned, upstream/downstream details fall back to owner-less rows.
    const tablesAugmented: MockTable[] = [
      ...tables,
      {
        id: "t-up",
        name: "UPSTREAM",
        ownerEntities: [
          {
            id: "oe-carol",
            userId: "u-carol",
            user: {
              id: "u-carol",
              email: "carol@example.com",
              fullName: "Carol Q",
            },
          },
        ],
        teamOwnerEntities: [
          { id: "te-fin", teamId: "team-fin", team: { name: "Finance" } },
        ],
      },
      { id: "t-c1", name: "CONSUMER_1", ownerEntities: [], teamOwnerEntities: [] },
      { id: "t-c2", name: "CONSUMER_2", ownerEntities: [], teamOwnerEntities: [] },
    ];

    const client = makeRouter({
      users: [ada, carol],
      tables: tablesAugmented,
      dashboards,
      terms,
      teams,
      edgesByKey,
      dashboardEdgesByKey,
      queriesByTableId,
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(
      await tool.handler({
        email: "ada@example.com",
        queryAuthorLimit: 5,
      })
    );

    // Identity
    const identity = out.identity as Record<string, unknown>;
    expect(identity.userId).toBe("u-ada");
    expect(identity.ownedAssetCount).toBe(5);
    expect(identity.ownedAssetUniqueCount).toBe(5);

    // Classification
    const scanned = out.scanned as Record<string, unknown>;
    expect(scanned.tablesCount).toBe(2);
    expect(scanned.dashboardsCount).toBe(1);
    expect(scanned.termsCount).toBe(1);
    // col-ghost isn't a table/dashboard/term — should land in unclassified
    expect(scanned.unclassified_owned_ids).toEqual(["col-ghost"]);

    // Handoff queue — sorted by blast radius DESC
    const queue = out.handoffQueue as Array<Record<string, unknown>>;
    expect(queue).toHaveLength(3);
    // tbl-A should be first (popular, heavy downstream, many queries)
    expect(queue[0].id).toBe("tbl-A");
    expect(queue[0].kind).toBe("TABLE");
    expect(queue[0].downstreamConsumerCount).toBe(3);
    // dash-1 or tbl-B depending on popularity vs downstream weighting;
    // both scores can be computed — just assert ordering is by blastRadiusScore
    const scores = queue.map((q) => q.blastRadiusScore as number);
    expect(scores[0]).toBeGreaterThanOrEqual(scores[1]);
    expect(scores[1]).toBeGreaterThanOrEqual(scores[2]);

    // Evidence bundle on tbl-A
    const a = queue.find((q) => q.id === "tbl-A") as Record<string, unknown>;
    const evidence = a.evidence as Record<string, unknown>;
    const qa = evidence.queryAuthors as Record<string, unknown>;
    expect(qa.totalQueriesSeen).toBe(4);
    expect(qa.queriesWithoutAuthor).toBe(1);
    const authors = qa.authors as Array<Record<string, unknown>>;
    expect(authors[0].author).toBe("carol@example.com");
    expect(authors[0].queryCount).toBe(2);

    const upstream = evidence.upstreamNeighbors as Array<Record<string, unknown>>;
    expect(upstream).toHaveLength(1);
    expect(upstream[0].id).toBe("t-up");
    const upOwners = upstream[0].owners as Array<Record<string, unknown>>;
    expect(upOwners).toHaveLength(2);
    expect(upOwners.find((o) => o.type === "user")?.userId).toBe("u-carol");
    expect(upOwners.find((o) => o.type === "team")?.teamId).toBe("team-fin");

    // tbl-B has no evidence beyond zero counts
    const b = queue.find((q) => q.id === "tbl-B") as Record<string, unknown>;
    expect(b.downstreamConsumerCount).toBe(0);
    const bEvidence = b.evidence as Record<string, unknown>;
    expect((bEvidence.upstreamNeighbors as unknown[]).length).toBe(0);
    expect((bEvidence.downstreamNeighbors as unknown[]).length).toBe(0);

    // Terms
    const termList = out.terms as Array<Record<string, unknown>>;
    expect(termList).toHaveLength(1);
    expect(termList[0].id).toBe("term-1");
    expect(termList[0].name).toBe("CustomerLifetimeValue");

    // Candidate summary — Carol appears as query author (tbl-A) AND neighbor
    // owner (upstream of tbl-A); she should be aggregated as TWO distinct
    // candidate entries: the synthetic email candidate (queryAuthor) and the
    // real userId candidate (neighbor). Finance team appears once.
    const candidates = out.candidateSummary as Array<Record<string, unknown>>;
    const userCandidates = candidates.filter(
      (c) => c.candidateType === "user"
    );
    const teamCandidates = candidates.filter(
      (c) => c.candidateType === "team"
    );
    // Two users seen as neighbor owners (actually just one: u-carol) plus
    // query-author email candidates (carol + dan). So: 1 neighbor-userId
    // candidate + 2 email candidates = 3 user candidates.
    expect(userCandidates).toHaveLength(3);
    expect(teamCandidates).toHaveLength(1);
    expect(teamCandidates[0].teamId).toBe("team-fin");
    // The real-userId Carol candidate (u-carol) should be tagged with the
    // Finance team via the team-context index.
    const carolByUserId = userCandidates.find(
      (c) => c.userId === "u-carol"
    ) as Record<string, unknown>;
    expect(carolByUserId).toBeDefined();
    const carolTeams = carolByUserId.teams as Array<Record<string, unknown>>;
    expect(carolTeams).toEqual([{ teamId: "team-fin", name: "Finance" }]);
    // Candidates are sorted by assetCount DESC — each candidate here only
    // touches one asset (tbl-A), so ordering falls back to totalBlastRadius
    // then userId/teamId. Just assert the coverage/score invariants.
    for (const c of candidates) {
      expect(c.assetCount).toBeGreaterThan(0);
      const ids = c.assetIds as string[];
      expect(new Set(ids).size).toBe(ids.length);
    }
  });

  it("skips team context when includeTeamContext=false (no getTeams call)", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "Ada",
      lastName: "L",
      email: "ada@example.com",
      role: "MEMBER",
      ownedAssetIds: ["tbl-1"],
    };
    const tables: MockTable[] = [
      { id: "tbl-1", name: "T", popularity: 0.1, numberOfQueries: 0 },
    ];
    const client = makeRouter({
      users: [ada],
      tables,
      dashboards: [],
      terms: [],
      teams: [],
    });
    const tool = defineReconcileOwnershipHandoff(client);
    await tool.handler({
      email: "ada@example.com",
      includeQueryAuthors: false,
      includeLineageNeighbors: false,
      includeTeamContext: false,
    });
    const documents = client.calls.map((c) => c.document);
    // Should NOT contain GET_TEAMS, GET_TABLE_QUERIES, or extra GET_LINEAGES
    // calls besides the downstream-count leg for the one owned table.
    expect(documents).not.toContain(GET_TEAMS);
    expect(documents).not.toContain(GET_TABLE_QUERIES);
  });
});

// ── Defensive guards ────────────────────────────────────────────────────────

describe("catalog_reconcile_ownership_handoff — defensive guards", () => {
  it("throws when a lineage-reached table neighbor is missing from enrichment", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "A",
      lastName: "L",
      email: "ada@example.com",
      role: "MEMBER",
      ownedAssetIds: ["tbl-orphan"],
    };
    const tables: MockTable[] = [
      {
        id: "tbl-orphan",
        name: "ORPHAN",
        popularity: 0.1,
        numberOfQueries: 0,
      },
    ];
    const edgesByKey = new Map<string, MockEdge[]>([
      [
        "tbl-orphan|upstream",
        [{ id: "l", parentTableId: "t-missing", childTableId: "tbl-orphan" }],
      ],
      ["tbl-orphan|downstream", []],
    ]);
    // Note: t-missing is NOT in `tables`, so enrichment returns no row.
    const client = makeRouter({
      users: [ada],
      tables,
      dashboards: [],
      terms: [],
      teams: [],
      edgesByKey,
      queriesByTableId: new Map([["tbl-orphan", []]]),
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const res = await tool.handler({ email: "ada@example.com" });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/Neighbor enrichment returned no row/);
    expect(parseResult(res).error).toMatch(/t-missing/);
  });

  it("refuses when downstream lineage pagination exceeds its ceiling", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "A",
      lastName: "L",
      email: "ada@example.com",
      role: "MEMBER",
      ownedAssetIds: ["tbl-1"],
    };
    const tables: MockTable[] = [
      { id: "tbl-1", name: "T", popularity: 0.5, numberOfQueries: 0 },
    ];
    // Return 500 full-page edges every time for downstream lineage counting.
    // The count helper ceiling is 20 pages * 500 = 10000; after 20 full
    // pages we throw.
    let nextId = 0;
    const client = makeMockClient((document, variables) => {
      if (document === GET_USERS) {
        const vars = variables as {
          pagination: { nbPerPage: number; page: number };
        };
        return {
          getUsers: vars.pagination.page === 0 ? [ada] : [],
        };
      }
      if (document === GET_TABLES_DETAIL_BATCH) {
        const vars = variables as { scope?: { ids?: string[] } };
        const ids = vars.scope?.ids ?? [];
        return {
          getTables: {
            totalCount: 1,
            nbPerPage: 500,
            page: 0,
            data: tables.filter((t) => ids.includes(t.id)),
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
        const vars = variables as {
          scope?: { parentTableId?: string; childTableId?: string };
        };
        // Downstream-count scope (parentTableId) — return full page forever
        // to trigger the ceiling.
        if (vars.scope?.parentTableId) {
          const data = Array.from({ length: 500 }, () => ({
            id: `e-${nextId++}`,
            parentTableId: vars.scope?.parentTableId,
            childTableId: `c-${nextId}`,
          }));
          return {
            getLineages: {
              totalCount: 99999,
              nbPerPage: 500,
              page: 0,
              data,
            },
          };
        }
        return {
          getLineages: { totalCount: 0, nbPerPage: 500, page: 0, data: [] },
        };
      }
      return {};
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const res = await tool.handler({
      email: "ada@example.com",
      includeQueryAuthors: false,
      includeLineageNeighbors: false,
      includeTeamContext: false,
    });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(
      /Lineage pagination exceeded 20 pages/
    );
  });
});

// ── Self-exclusion by email (fix: departing owner not a candidate) ──────────

describe("catalog_reconcile_ownership_handoff — self-exclusion", () => {
  it("filters the departing owner's email out of query-author candidates", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "Ada",
      lastName: "L",
      email: "Ada@Example.com",
      role: "MEMBER",
      ownedAssetIds: ["tbl-1"],
    };
    const tables: MockTable[] = [
      { id: "tbl-1", name: "T", popularity: 0.1, numberOfQueries: 10 },
    ];
    // Ada is the #1 query author on her own table; a few other emails
    // appear too so we can verify those still show up.
    const queriesByTableId = new Map<string, MockQuery[]>([
      [
        "tbl-1",
        [
          // Case-mismatch on purpose — filter is case-insensitive
          { author: "ada@example.com", queryType: "SELECT" },
          { author: "ada@example.com", queryType: "SELECT" },
          { author: "ada@example.com", queryType: "SELECT" },
          { author: "eve@example.com", queryType: "SELECT" },
          { author: "dan@example.com", queryType: "WRITE" },
        ],
      ],
    ]);
    const client = makeRouter({
      users: [ada],
      tables,
      dashboards: [],
      terms: [],
      teams: [],
      queriesByTableId,
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(
      await tool.handler({
        email: "ada@example.com",
        includeLineageNeighbors: false,
      })
    );
    const candidates = out.candidateSummary as Array<Record<string, unknown>>;
    const userIds = candidates
      .filter((c) => c.candidateType === "user")
      .map((c) => c.userId as string);
    // Ada must NOT appear (filtered by email self-exclusion)
    expect(userIds).not.toContain("email:ada@example.com");
    // Other authors must still appear
    expect(userIds).toContain("email:eve@example.com");
    expect(userIds).toContain("email:dan@example.com");
  });
});

// ── Dashboard upstream evidence (fix: dashboards contribute candidates) ─────

describe("catalog_reconcile_ownership_handoff — dashboard upstream evidence", () => {
  it("surfaces owners of upstream tables as candidates for a dashboard handoff", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "Ada",
      lastName: "L",
      email: "ada@example.com",
      role: "MEMBER",
      // Dashboard-only portfolio so this test isolates the dashboard path
      ownedAssetIds: ["dash-1"],
    };
    const carol: MockUser = {
      id: "u-carol",
      firstName: "Carol",
      lastName: "Q",
      email: "carol@example.com",
      role: "MEMBER",
      ownedAssetIds: [],
    };
    const dashboards: MockDashboard[] = [
      { id: "dash-1", name: "Revenue", popularity: 0.7 },
    ];
    // dash-1's upstream table is tbl-up, owned by Carol.
    const dashboardUpstreamEdgesByKey = new Map<string, MockEdge[]>([
      [
        "dash-1|upstream",
        [{ id: "l1", parentTableId: "tbl-up", childDashboardId: "dash-1" }],
      ],
    ]);
    const tablesAugmented: MockTable[] = [
      {
        id: "tbl-up",
        name: "UPSTREAM_TABLE",
        ownerEntities: [
          {
            id: "oe-carol",
            userId: "u-carol",
            user: {
              id: "u-carol",
              email: "carol@example.com",
              fullName: "Carol Q",
            },
          },
        ],
        teamOwnerEntities: [],
      },
    ];
    const client = makeRouter({
      users: [ada, carol],
      tables: tablesAugmented,
      dashboards,
      terms: [],
      teams: [],
      dashboardUpstreamEdgesByKey,
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(
      await tool.handler({
        email: "ada@example.com",
        includeQueryAuthors: false,
        includeTeamContext: false,
      })
    );
    const queue = out.handoffQueue as Array<Record<string, unknown>>;
    expect(queue).toHaveLength(1);
    expect(queue[0].id).toBe("dash-1");
    const evidence = queue[0].evidence as Record<string, unknown>;
    const upstream = evidence.upstreamNeighbors as Array<
      Record<string, unknown>
    >;
    expect(upstream).toHaveLength(1);
    expect(upstream[0].id).toBe("tbl-up");
    const upOwners = upstream[0].owners as Array<Record<string, unknown>>;
    expect(upOwners).toHaveLength(1);
    expect(upOwners[0].userId).toBe("u-carol");
    // And Carol shows up in candidateSummary via the dashboard's upstream path
    const candidates = out.candidateSummary as Array<Record<string, unknown>>;
    const carolCand = candidates.find(
      (c) => c.candidateType === "user" && c.userId === "u-carol"
    );
    expect(carolCand).toBeDefined();
    const evidenceTypes = carolCand!.evidenceTypes as Record<string, number>;
    expect(evidenceTypes.upstreamNeighbor).toBe(1);
  });

  it("surfaces parent dashboards as upstream neighbors with owners:[] (embedded-dashboard pattern)", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "Ada",
      lastName: "L",
      email: "ada@example.com",
      role: "MEMBER",
      ownedAssetIds: ["dash-child"],
    };
    const carol: MockUser = {
      id: "u-carol",
      firstName: "Carol",
      lastName: "Q",
      email: "carol@example.com",
      role: "MEMBER",
      ownedAssetIds: [],
    };
    const dashboards: MockDashboard[] = [
      { id: "dash-child", name: "Embedded", popularity: 0.4 },
    ];
    // dash-child is fed by one parent TABLE (tbl-up, owned by Carol) AND
    // one parent DASHBOARD (dash-parent). The fix must surface both.
    const dashboardUpstreamEdgesByKey = new Map<string, MockEdge[]>([
      [
        "dash-child|upstream",
        [
          { id: "l1", parentTableId: "tbl-up", childDashboardId: "dash-child" },
          {
            id: "l2",
            parentDashboardId: "dash-parent",
            childDashboardId: "dash-child",
          },
        ],
      ],
    ]);
    const tablesAugmented: MockTable[] = [
      {
        id: "tbl-up",
        name: "UPSTREAM_TABLE",
        ownerEntities: [
          {
            id: "oe-carol",
            userId: "u-carol",
            user: {
              id: "u-carol",
              email: "carol@example.com",
              fullName: "Carol Q",
            },
          },
        ],
        teamOwnerEntities: [],
      },
    ];
    const client = makeRouter({
      users: [ada, carol],
      tables: tablesAugmented,
      dashboards,
      terms: [],
      teams: [],
      dashboardUpstreamEdgesByKey,
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(
      await tool.handler({
        email: "ada@example.com",
        includeQueryAuthors: false,
        includeTeamContext: false,
      })
    );
    const queue = out.handoffQueue as Array<Record<string, unknown>>;
    expect(queue).toHaveLength(1);
    const evidence = queue[0].evidence as Record<string, unknown>;
    const upstream = evidence.upstreamNeighbors as Array<
      Record<string, unknown>
    >;
    // Both the parent table AND the parent dashboard must appear — the
    // pre-fix code silently dropped the parent dashboard.
    expect(upstream).toHaveLength(2);
    const tableUp = upstream.find((n) => n.kind === "TABLE");
    const dashUp = upstream.find((n) => n.kind === "DASHBOARD");
    expect(tableUp).toBeDefined();
    expect(tableUp!.id).toBe("tbl-up");
    expect(dashUp).toBeDefined();
    expect(dashUp!.id).toBe("dash-parent");
    // Dashboard parents carry owners:[] by convention (dashboard ownership
    // isn't used as an attribution signal — mirrors enrichTableNeighbors).
    expect(dashUp!.owners).toEqual([]);
    expect(dashUp!.name).toBeNull();
    // Carol still appears as a candidate via the table-parent path.
    const candidates = out.candidateSummary as Array<Record<string, unknown>>;
    expect(
      candidates.find((c) => c.userId === "u-carol")
    ).toBeDefined();
  });

  it("refuses when a dashboard's upstream table is missing from enrichment", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "A",
      lastName: "L",
      email: "ada@example.com",
      role: "MEMBER",
      ownedAssetIds: ["dash-orphan"],
    };
    const dashboards: MockDashboard[] = [
      { id: "dash-orphan", name: "Orphan", popularity: 0.1 },
    ];
    const dashboardUpstreamEdgesByKey = new Map<string, MockEdge[]>([
      [
        "dash-orphan|upstream",
        [
          {
            id: "l",
            parentTableId: "tbl-missing",
            childDashboardId: "dash-orphan",
          },
        ],
      ],
    ]);
    // tbl-missing is NOT present in `tables`
    const client = makeRouter({
      users: [ada],
      tables: [],
      dashboards,
      terms: [],
      teams: [],
      dashboardUpstreamEdgesByKey,
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const res = await tool.handler({
      email: "ada@example.com",
      includeQueryAuthors: false,
      includeTeamContext: false,
    });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(
      /Neighbor enrichment returned no row/
    );
    expect(parseResult(res).error).toMatch(/upstream of dashboard dash-orphan/);
  });
});

// ── Description promises ────────────────────────────────────────────────────

describe("catalog_reconcile_ownership_handoff — description promises", () => {
  it("discloses that terms contribute no candidate evidence", () => {
    const desc = findTool().config.description;
    expect(desc).toMatch(/term/i);
    expect(desc).toMatch(/no candidate evidence|picked manually/i);
  });
  it("discloses the departing owner's self-exclusion", () => {
    const desc = findTool().config.description;
    expect(desc).toMatch(/self-exclusion|departing owner is filtered/i);
  });
  it("discloses that dashboards have no query-author signal", () => {
    const desc = findTool().config.description;
    expect(desc).toMatch(/dashboards? don't author queries|no query-author/i);
  });
});

// ── Empty portfolio ─────────────────────────────────────────────────────────

describe("catalog_reconcile_ownership_handoff — empty portfolio", () => {
  it("returns empty handoffQueue/candidateSummary when the owner holds nothing", async () => {
    const ada: MockUser = {
      id: "u-ada",
      firstName: "A",
      lastName: "L",
      email: "ada@example.com",
      role: "MEMBER",
      ownedAssetIds: [],
    };
    const client = makeRouter({
      users: [ada],
      tables: [],
      dashboards: [],
      terms: [],
      teams: [],
    });
    const tool = defineReconcileOwnershipHandoff(client);
    const out = parseResult(await tool.handler({ email: "ada@example.com" }));
    expect((out.handoffQueue as unknown[]).length).toBe(0);
    expect((out.candidateSummary as unknown[]).length).toBe(0);
    expect((out.terms as unknown[]).length).toBe(0);
    const scanned = out.scanned as Record<string, unknown>;
    expect(scanned.tablesCount).toBe(0);
  });
});
