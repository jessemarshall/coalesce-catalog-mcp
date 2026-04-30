import { describe, it, expect } from "vitest";
import {
  GET_LINEAGES,
  GET_TABLE_QUERIES,
  GET_USERS,
} from "../../src/catalog/operations.js";
import {
  USER_PAGE_SIZE,
  USER_LOOKUP_MAX_PAGES,
  ENRICHMENT_BATCH_SIZE,
  isNonEmptyString,
  extractOwners,
  extractTagLabels,
  hasOwner,
  chunk,
  resolveUserByEmail,
  extractNeighborOwners,
  fetchTableQueryAuthors,
  fetchOneHopNeighborIds,
  countDownstreamEdges,
} from "../../src/workflows/shared.js";
import { makeMockClient } from "../helpers/mock-client.js";

// Helpers ───────────────────────────────────────────────────────────────────

function makeUser(
  id: string,
  email: string,
  ownedAssetIds: string[] = [],
  teamIds?: string[]
) {
  return {
    id,
    firstName: "First",
    lastName: "Last",
    email,
    role: "MEMBER",
    status: "ACTIVE",
    isEmailValidated: true,
    createdAt: "2025-01-01T00:00:00Z",
    ownedAssetIds,
    ...(teamIds ? { teamIds } : {}),
  };
}

function fillerUsers(count: number) {
  return Array.from({ length: count }, (_, i) =>
    makeUser(`u-filler-${i}`, `filler-${i}@example.com`)
  );
}

// ── Pure helpers ────────────────────────────────────────────────────────────

describe("isNonEmptyString", () => {
  it("accepts non-empty strings", () => {
    expect(isNonEmptyString("hello")).toBe(true);
    expect(isNonEmptyString(" x ")).toBe(true);
  });

  it("rejects empty / whitespace-only strings", () => {
    expect(isNonEmptyString("")).toBe(false);
    expect(isNonEmptyString("   ")).toBe(false);
    expect(isNonEmptyString("\t\n")).toBe(false);
  });

  it("rejects non-string values", () => {
    expect(isNonEmptyString(null)).toBe(false);
    expect(isNonEmptyString(undefined)).toBe(false);
    expect(isNonEmptyString(0)).toBe(false);
    expect(isNonEmptyString(123)).toBe(false);
    expect(isNonEmptyString({})).toBe(false);
    expect(isNonEmptyString([])).toBe(false);
    expect(isNonEmptyString(true)).toBe(false);
  });
});

describe("chunk", () => {
  it("splits an array into evenly-sized chunks", () => {
    expect(chunk([1, 2, 3, 4, 5, 6], 2)).toEqual([
      [1, 2],
      [3, 4],
      [5, 6],
    ]);
  });

  it("includes a smaller final chunk when the count doesn't divide evenly", () => {
    expect(chunk([1, 2, 3, 4, 5], 2)).toEqual([[1, 2], [3, 4], [5]]);
  });

  it("returns an empty array for empty input", () => {
    expect(chunk([], 5)).toEqual([]);
  });

  it("handles size larger than the input length", () => {
    expect(chunk([1, 2], 10)).toEqual([[1, 2]]);
  });

  it("preserves element order", () => {
    const items = ["a", "b", "c", "d", "e"];
    const flat = chunk(items, 3).flat();
    expect(flat).toEqual(items);
  });
});

// ── extractOwners / hasOwner ────────────────────────────────────────────────

describe("extractOwners", () => {
  it("returns empty arrays when neither owner field is present", () => {
    expect(extractOwners({})).toEqual({ userOwners: [], teamOwners: [] });
  });

  it("returns empty arrays when ownerEntities is not an array", () => {
    expect(
      extractOwners({ ownerEntities: null, teamOwnerEntities: undefined })
    ).toEqual({ userOwners: [], teamOwners: [] });
  });

  it("extracts user owners with email + fullName from nested user objects", () => {
    const result = extractOwners({
      ownerEntities: [
        { userId: "u-1", user: { email: "alice@x.com", fullName: "Alice" } },
        { userId: "u-2", user: { email: "bob@x.com", fullName: "Bob" } },
      ],
    });
    expect(result.userOwners).toEqual([
      { userId: "u-1", email: "alice@x.com", fullName: "Alice" },
      { userId: "u-2", email: "bob@x.com", fullName: "Bob" },
    ]);
  });

  it("filters out user owner rows with null userId", () => {
    const result = extractOwners({
      ownerEntities: [
        { userId: null, user: { email: "ghost@x.com" } },
        { userId: "u-1", user: { email: "alice@x.com", fullName: "Alice" } },
      ],
    });
    expect(result.userOwners.map((o) => o.userId)).toEqual(["u-1"]);
  });

  it("falls back to null when nested user object is missing", () => {
    const result = extractOwners({
      ownerEntities: [{ userId: "u-1" }],
    });
    expect(result.userOwners).toEqual([
      { userId: "u-1", email: null, fullName: null },
    ]);
  });

  it("extracts team owners with name from nested team objects", () => {
    const result = extractOwners({
      teamOwnerEntities: [
        { teamId: "t-1", team: { name: "Data Eng" } },
        { teamId: "t-2", team: { name: null } },
      ],
    });
    expect(result.teamOwners).toEqual([
      { teamId: "t-1", name: "Data Eng" },
      { teamId: "t-2", name: null },
    ]);
  });

  it("filters out team rows with null teamId", () => {
    const result = extractOwners({
      teamOwnerEntities: [{ teamId: null }, { teamId: "t-1" }],
    });
    expect(result.teamOwners.map((t) => t.teamId)).toEqual(["t-1"]);
  });
});

describe("hasOwner", () => {
  it("returns false for empty / missing fields", () => {
    expect(hasOwner({})).toBe(false);
    expect(hasOwner({ ownerEntities: [] })).toBe(false);
    expect(hasOwner({ ownerEntities: [], teamOwnerEntities: [] })).toBe(false);
  });

  it("returns true when at least one user owner is present", () => {
    expect(
      hasOwner({ ownerEntities: [{ userId: "u-1" }] })
    ).toBe(true);
  });

  it("returns true when at least one team owner is present", () => {
    expect(
      hasOwner({ teamOwnerEntities: [{ teamId: "t-1" }] })
    ).toBe(true);
  });

  it("returns false when all owner rows have null IDs", () => {
    expect(
      hasOwner({
        ownerEntities: [{ userId: null }],
        teamOwnerEntities: [{ teamId: null }],
      })
    ).toBe(false);
  });

  it("returns true when one valid owner is mixed with null-id rows", () => {
    expect(
      hasOwner({
        ownerEntities: [{ userId: null }, { userId: "u-1" }],
      })
    ).toBe(true);
  });
});

// ── extractTagLabels ────────────────────────────────────────────────────────

describe("extractTagLabels", () => {
  it("returns empty array when tagEntities is missing", () => {
    expect(extractTagLabels({})).toEqual([]);
  });

  it("returns empty array when tagEntities is not an array", () => {
    expect(extractTagLabels({ tagEntities: null })).toEqual([]);
    expect(extractTagLabels({ tagEntities: "nope" })).toEqual([]);
  });

  it("extracts non-empty string labels in order", () => {
    expect(
      extractTagLabels({
        tagEntities: [
          { tag: { label: "pii" } },
          { tag: { label: "domain:finance" } },
        ],
      })
    ).toEqual(["pii", "domain:finance"]);
  });

  it("skips entries with missing tag, missing label, non-string label, or empty label", () => {
    expect(
      extractTagLabels({
        tagEntities: [
          { tag: { label: "pii" } },
          { tag: undefined },
          { tag: { label: null } },
          { tag: { label: 7 } },
          { tag: { label: "" } },
          { tag: { label: "ok" } },
        ],
      })
    ).toEqual(["pii", "ok"]);
  });
});

// ── resolveUserByEmail ──────────────────────────────────────────────────────

describe("resolveUserByEmail", () => {
  it("returns kind=found with case-insensitive email match", async () => {
    const target = makeUser("u-1", "Alice@Example.com", ["a-1"]);
    const client = makeMockClient(() => ({ getUsers: [target] }));

    const result = await resolveUserByEmail(client, "alice@example.com");
    expect(result.kind).toBe("found");
    if (result.kind === "found") {
      expect(result.owner.userId).toBe("u-1");
      expect(result.owner.email).toBe("Alice@Example.com");
      expect(result.owner.ownedAssetIds).toEqual(["a-1"]);
    }
  });

  it("forwards teamIds when present on the user record", async () => {
    const target = makeUser("u-1", "alice@example.com", [], ["t-1", "t-2"]);
    const client = makeMockClient(() => ({ getUsers: [target] }));

    const result = await resolveUserByEmail(client, "alice@example.com");
    expect(result.kind).toBe("found");
    if (result.kind === "found") {
      expect(result.owner.teamIds).toEqual(["t-1", "t-2"]);
    }
  });

  it("returns kind=absent when a partial page closes the scan with no match", async () => {
    const client = makeMockClient(() => ({
      getUsers: fillerUsers(5),
    }));
    const result = await resolveUserByEmail(client, "missing@example.com");
    expect(result.kind).toBe("absent");
    expect(client.calls).toHaveLength(1);
  });

  it("paginates across multiple pages until a match is found", async () => {
    const target = makeUser("u-target", "match@example.com");
    let calls = 0;
    const client = makeMockClient(() => {
      calls += 1;
      if (calls === 1) return { getUsers: fillerUsers(USER_PAGE_SIZE) };
      return { getUsers: [target, ...fillerUsers(USER_PAGE_SIZE - 1)] };
    });

    const result = await resolveUserByEmail(client, "match@example.com");
    expect(result.kind).toBe("found");
    expect(client.calls).toHaveLength(2);
    // page 0 then page 1
    expect((client.calls[0].variables as { pagination: { page: number } }).pagination.page).toBe(0);
    expect((client.calls[1].variables as { pagination: { page: number } }).pagination.page).toBe(1);
    expect(client.calls[0].document).toBe(GET_USERS);
  });

  it("returns kind=ceiling with usersScanned count when scan exhausts the cap", async () => {
    const client = makeMockClient(() => ({
      getUsers: fillerUsers(USER_PAGE_SIZE),
    }));
    const result = await resolveUserByEmail(client, "missing@example.com");
    expect(result.kind).toBe("ceiling");
    if (result.kind === "ceiling") {
      expect(result.usersScanned).toBe(USER_PAGE_SIZE * USER_LOOKUP_MAX_PAGES);
    }
    expect(client.calls).toHaveLength(USER_LOOKUP_MAX_PAGES);
  });

  it("defaults ownedAssetIds to empty array when missing from the API row", async () => {
    const partial = {
      ...makeUser("u-1", "alice@example.com"),
      ownedAssetIds: undefined,
    };
    const client = makeMockClient(() => ({ getUsers: [partial] }));
    const result = await resolveUserByEmail(client, "alice@example.com");
    expect(result.kind).toBe("found");
    if (result.kind === "found") {
      expect(result.owner.ownedAssetIds).toEqual([]);
    }
  });
});

// ── extractNeighborOwners ───────────────────────────────────────────────────

describe("extractNeighborOwners", () => {
  it("returns an empty array for undefined input", () => {
    expect(extractNeighborOwners(undefined)).toEqual([]);
  });

  it("returns an empty array when neither owner field is present", () => {
    expect(extractNeighborOwners({})).toEqual([]);
  });

  it("extracts user owners with type='user'", () => {
    const owners = extractNeighborOwners({
      ownerEntities: [
        { userId: "u-1", user: { email: "alice@x.com", fullName: "Alice" } },
      ],
    });
    expect(owners).toEqual([
      {
        type: "user",
        userId: "u-1",
        email: "alice@x.com",
        name: "Alice",
      },
    ]);
  });

  it("extracts team owners with type='team' and no email field", () => {
    const owners = extractNeighborOwners({
      teamOwnerEntities: [{ teamId: "t-1", team: { name: "Data Eng" } }],
    });
    expect(owners).toEqual([
      { type: "team", teamId: "t-1", name: "Data Eng" },
    ]);
  });

  it("preserves order: users first, then teams", () => {
    const owners = extractNeighborOwners({
      ownerEntities: [{ userId: "u-1", user: { email: "a@x.com" } }],
      teamOwnerEntities: [{ teamId: "t-1", team: { name: "T" } }],
    });
    expect(owners.map((o) => o.type)).toEqual(["user", "team"]);
  });

  it("filters out rows with null userId or teamId", () => {
    const owners = extractNeighborOwners({
      ownerEntities: [{ userId: null }, { userId: "u-1", user: {} }],
      teamOwnerEntities: [{ teamId: null }, { teamId: "t-1", team: {} }],
    });
    expect(owners).toHaveLength(2);
    expect(owners[0].userId).toBe("u-1");
    expect(owners[1].teamId).toBe("t-1");
  });

  it("falls back to null name when the nested user/team object is missing", () => {
    const owners = extractNeighborOwners({
      ownerEntities: [{ userId: "u-1" }],
      teamOwnerEntities: [{ teamId: "t-1" }],
    });
    expect(owners).toEqual([
      { type: "user", userId: "u-1", email: null, name: null },
      { type: "team", teamId: "t-1", name: null },
    ]);
  });
});

// ── fetchTableQueryAuthors ──────────────────────────────────────────────────

describe("fetchTableQueryAuthors", () => {
  it("issues a single GET_TABLE_QUERIES call with timestamp DESC", async () => {
    const client = makeMockClient(() => ({
      getTableQueries: { data: [] },
    }));
    await fetchTableQueryAuthors(client, "tab-1", { topN: 5, probeSize: 100 });
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_TABLE_QUERIES);
    expect(client.calls[0].variables).toMatchObject({
      scope: { tableIds: ["tab-1"] },
      sorting: [{ sortingKey: "timestamp", direction: "DESC" }],
      pagination: { nbPerPage: 100, page: 0 },
    });
  });

  it("aggregates query counts per author and breaks down by queryType", async () => {
    const client = makeMockClient(() => ({
      getTableQueries: {
        data: [
          { author: "alice@x.com", queryType: "SELECT" },
          { author: "alice@x.com", queryType: "SELECT" },
          { author: "alice@x.com", queryType: "WRITE" },
          { author: "bob@x.com", queryType: "SELECT" },
        ],
      },
    }));
    const probe = await fetchTableQueryAuthors(client, "tab-1", {
      topN: 10,
      probeSize: 100,
    });
    expect(probe.totalQueriesSeen).toBe(4);
    expect(probe.queriesWithoutAuthor).toBe(0);
    expect(probe.authors).toEqual([
      {
        author: "alice@x.com",
        queryCount: 3,
        queryTypeBreakdown: { SELECT: 2, WRITE: 1 },
      },
      {
        author: "bob@x.com",
        queryCount: 1,
        queryTypeBreakdown: { SELECT: 1 },
      },
    ]);
  });

  it("counts rows whose author is missing/blank into queriesWithoutAuthor", async () => {
    const client = makeMockClient(() => ({
      getTableQueries: {
        data: [
          { author: "alice@x.com", queryType: "SELECT" },
          { author: "", queryType: "SELECT" },
          { author: "   ", queryType: "SELECT" },
          { author: null, queryType: "SELECT" },
        ],
      },
    }));
    const probe = await fetchTableQueryAuthors(client, "tab-1", {
      topN: 5,
      probeSize: 100,
    });
    expect(probe.totalQueriesSeen).toBe(4);
    expect(probe.queriesWithoutAuthor).toBe(3);
    expect(probe.authors.map((a) => a.author)).toEqual(["alice@x.com"]);
  });

  it("uses 'UNKNOWN' bucket when queryType is missing", async () => {
    const client = makeMockClient(() => ({
      getTableQueries: {
        data: [
          { author: "alice@x.com" },
          { author: "alice@x.com", queryType: null },
        ],
      },
    }));
    const probe = await fetchTableQueryAuthors(client, "tab-1", {
      topN: 5,
      probeSize: 100,
    });
    expect(probe.authors[0].queryTypeBreakdown).toEqual({ UNKNOWN: 2 });
  });

  it("caps result to topN and orders by queryCount DESC then author ASC", async () => {
    const client = makeMockClient(() => ({
      getTableQueries: {
        data: [
          { author: "carol", queryType: "SELECT" },
          { author: "alice", queryType: "SELECT" },
          { author: "alice", queryType: "SELECT" },
          { author: "bob", queryType: "SELECT" },
          { author: "bob", queryType: "SELECT" },
          { author: "dave", queryType: "SELECT" },
        ],
      },
    }));
    const probe = await fetchTableQueryAuthors(client, "tab-1", {
      topN: 2,
      probeSize: 100,
    });
    // 2-count tie between alice and bob → sorted alphabetically.
    expect(probe.authors.map((a) => a.author)).toEqual(["alice", "bob"]);
  });

  it("returns probeCap reflecting the requested probeSize", async () => {
    const client = makeMockClient(() => ({ getTableQueries: { data: [] } }));
    const probe = await fetchTableQueryAuthors(client, "tab-1", {
      topN: 5,
      probeSize: 250,
    });
    expect(probe.probeCap).toBe(250);
  });
});

// ── fetchOneHopNeighborIds ──────────────────────────────────────────────────

describe("fetchOneHopNeighborIds", () => {
  it("downstream: paginates with parentTableId scope and tags neighbors with kind", async () => {
    const client = makeMockClient(() => ({
      getLineages: {
        data: [
          { childTableId: "tab-down", childDashboardId: null },
          { childTableId: null, childDashboardId: "dash-down" },
        ],
      },
    }));
    const out = await fetchOneHopNeighborIds(client, "tab-1", "downstream", {
      pageSize: 10,
      maxPages: 5,
    });
    expect(client.calls[0].variables).toMatchObject({
      scope: { parentTableId: "tab-1" },
      pagination: { nbPerPage: 10, page: 0 },
    });
    expect(client.calls[0].document).toBe(GET_LINEAGES);
    expect(out).toEqual([
      { id: "tab-down", kind: "TABLE" },
      { id: "dash-down", kind: "DASHBOARD" },
    ]);
  });

  it("upstream: uses childTableId scope and tags every neighbor as TABLE", async () => {
    const client = makeMockClient(() => ({
      getLineages: {
        data: [
          // Upstream against a childTableId scope: even if the row has no
          // parentTableId in the fixture, the helper must mark it TABLE.
          { parentTableId: "tab-up-1" },
          { parentTableId: "tab-up-2" },
        ],
      },
    }));
    const out = await fetchOneHopNeighborIds(client, "tab-1", "upstream", {
      pageSize: 10,
      maxPages: 5,
    });
    expect(client.calls[0].variables).toMatchObject({
      scope: { childTableId: "tab-1" },
    });
    expect(out).toEqual([
      { id: "tab-up-1", kind: "TABLE" },
      { id: "tab-up-2", kind: "TABLE" },
    ]);
  });

  it("dedupes neighbor IDs across pages", async () => {
    let call = 0;
    const client = makeMockClient(() => {
      call += 1;
      if (call === 1) {
        // Page 0 — full page so iteration continues
        return {
          getLineages: {
            data: [
              { childTableId: "tab-a" },
              { childTableId: "tab-b" },
              ...Array.from({ length: 8 }, (_, i) => ({
                childTableId: `tab-${i}`,
              })),
            ],
          },
        };
      }
      // Page 1 — partial page closes the scan; reuses tab-a + new tab-c
      return {
        getLineages: {
          data: [{ childTableId: "tab-a" }, { childTableId: "tab-c" }],
        },
      };
    });
    const out = await fetchOneHopNeighborIds(client, "tab-1", "downstream", {
      pageSize: 10,
      maxPages: 5,
    });
    const uniqueIds = new Set(out.map((n) => n.id));
    // No duplicate of tab-a even though it appears on both pages.
    expect(uniqueIds.size).toBe(out.length);
    expect(out.some((n) => n.id === "tab-c")).toBe(true);
  });

  it("returns the partial result when the last page is short of the page size", async () => {
    const client = makeMockClient(() => ({
      getLineages: { data: [{ childTableId: "tab-only" }] },
    }));
    const out = await fetchOneHopNeighborIds(client, "tab-1", "downstream", {
      pageSize: 50,
      maxPages: 5,
    });
    expect(out).toEqual([{ id: "tab-only", kind: "TABLE" }]);
    expect(client.calls).toHaveLength(1);
  });

  it("skips edge rows that have no neighbor ID on either side", async () => {
    const client = makeMockClient(() => ({
      getLineages: {
        data: [
          { childTableId: null, childDashboardId: null },
          { childTableId: "tab-real" },
        ],
      },
    }));
    const out = await fetchOneHopNeighborIds(client, "tab-1", "downstream", {
      pageSize: 10,
      maxPages: 5,
    });
    expect(out).toEqual([{ id: "tab-real", kind: "TABLE" }]);
  });

  it("throws when pagination exhausts maxPages without a partial page closing the scan", async () => {
    const client = makeMockClient(() => ({
      getLineages: {
        data: Array.from({ length: 10 }, (_, i) => ({
          childTableId: `tab-${Date.now()}-${Math.random()}-${i}`,
        })),
      },
    }));
    await expect(
      fetchOneHopNeighborIds(client, "tab-1", "downstream", {
        pageSize: 10,
        maxPages: 3,
      })
    ).rejects.toThrow(/exceeded 3 pages/i);
    expect(client.calls).toHaveLength(3);
  });
});

// ── countDownstreamEdges ────────────────────────────────────────────────────

describe("countDownstreamEdges", () => {
  it("uses parentTableId scope when parentKind is TABLE", async () => {
    const client = makeMockClient(() => ({ getLineages: { data: [] } }));
    await countDownstreamEdges(client, "tab-1", "TABLE", {
      pageSize: 50,
      maxPages: 5,
    });
    expect(client.calls[0].document).toBe(GET_LINEAGES);
    expect(client.calls[0].variables).toMatchObject({
      scope: { parentTableId: "tab-1" },
      pagination: { nbPerPage: 50, page: 0 },
    });
  });

  it("uses parentDashboardId scope when parentKind is DASHBOARD", async () => {
    const client = makeMockClient(() => ({ getLineages: { data: [] } }));
    await countDownstreamEdges(client, "dash-1", "DASHBOARD", {
      pageSize: 50,
      maxPages: 5,
    });
    expect(client.calls[0].variables).toMatchObject({
      scope: { parentDashboardId: "dash-1" },
    });
  });

  it("returns 0 when the first page is empty", async () => {
    const client = makeMockClient(() => ({ getLineages: { data: [] } }));
    const count = await countDownstreamEdges(client, "tab-1", "TABLE", {
      pageSize: 50,
      maxPages: 5,
    });
    expect(count).toBe(0);
    expect(client.calls).toHaveLength(1);
  });

  it("sums rows across pages until a short page closes the scan", async () => {
    let call = 0;
    const client = makeMockClient(() => {
      call += 1;
      // Page 0: full; Page 1: full; Page 2: partial (closes scan)
      if (call === 1) return { getLineages: { data: new Array(10).fill({}) } };
      if (call === 2) return { getLineages: { data: new Array(10).fill({}) } };
      return { getLineages: { data: new Array(3).fill({}) } };
    });
    const count = await countDownstreamEdges(client, "tab-1", "TABLE", {
      pageSize: 10,
      maxPages: 5,
    });
    expect(count).toBe(23);
    expect(client.calls).toHaveLength(3);
  });

  it("throws rather than returning a partial count when maxPages is exhausted", async () => {
    const client = makeMockClient(() => ({
      getLineages: { data: new Array(10).fill({}) },
    }));
    await expect(
      countDownstreamEdges(client, "tab-1", "TABLE", {
        pageSize: 10,
        maxPages: 3,
      })
    ).rejects.toThrow(/exceeded 3 pages.*Refusing to produce a partial count/i);
    expect(client.calls).toHaveLength(3);
  });
});

// ── Sanity: exported constants ──────────────────────────────────────────────

describe("exported constants", () => {
  it("ENRICHMENT_BATCH_SIZE matches the API per-page maximum", () => {
    // Documented behavior: every workflow that hydrates a heterogeneous ID
    // list chunks at this size to stay under the 500-row API ceiling.
    expect(ENRICHMENT_BATCH_SIZE).toBe(500);
  });

  it("USER_PAGE_SIZE * USER_LOOKUP_MAX_PAGES gives the documented 10k user ceiling", () => {
    expect(USER_PAGE_SIZE * USER_LOOKUP_MAX_PAGES).toBe(10000);
  });
});
