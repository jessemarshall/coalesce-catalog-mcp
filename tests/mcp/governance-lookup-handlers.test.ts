import { describe, it, expect } from "vitest";
import { defineGovernanceTools } from "../../src/mcp/governance.js";
import { GET_USERS, GET_TEAMS } from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

// These three tools all share the same iteration shape — page through
// getUsers / getTeams (500 per page, max 20 pages) until the target id is
// found, then slice the corresponding array of asset / member IDs by the
// caller's pagination input. They expose three branches the handler-level
// tests need to cover:
//   1. found-in-first-page (the common path)
//   2. found-after-multi-page-iteration
//   3. notFound — short-circuit when a partial page comes back
//   4. notFound — ceiling hit after 20 full pages
// plus the sliceAssetIds pagination math (totalCount, hasMore, partial last
// page, empty array).

const LOOKUP_PAGE_SIZE = 500;
const LOOKUP_MAX_PAGES = 20;

function makeTools(responder: Parameters<typeof makeMockClient>[0]) {
  const mock = makeMockClient(responder);
  return { client: mock, tools: defineGovernanceTools(mock) };
}

function findIn(
  toolSet: ReturnType<typeof defineGovernanceTools>,
  name: string
) {
  const match = toolSet.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }): unknown {
  return JSON.parse(r.content[0].text);
}

function makeUser(id: string, ownedAssetIds: string[] = []) {
  return {
    id,
    firstName: "First",
    lastName: "Last",
    email: `${id}@example.com`,
    role: "MEMBER",
    status: "ACTIVE",
    isEmailValidated: true,
    createdAt: "2025-01-01T00:00:00Z",
    ownedAssetIds,
  };
}

function makeTeam(id: string, memberIds: string[] = [], ownedAssetIds: string[] = []) {
  return {
    id,
    name: `team-${id}`,
    description: "",
    email: `${id}@example.com`,
    slackChannel: null,
    slackGroup: null,
    memberIds,
    ownedAssetIds,
    createdAt: "2025-01-01T00:00:00Z",
  };
}

// Builds a full-page filler so iteration continues to the next page without
// a match. The handler short-circuits when it sees a page with fewer than
// LOOKUP_PAGE_SIZE rows, so each filler page is exactly LOOKUP_PAGE_SIZE.
function fillerUsers(count: number): ReturnType<typeof makeUser>[] {
  return Array.from({ length: count }, (_, i) => makeUser(`filler-u-${i}`));
}

function fillerTeams(count: number): ReturnType<typeof makeTeam>[] {
  return Array.from({ length: count }, (_, i) => makeTeam(`filler-t-${i}`));
}

// ── catalog_get_user_owned_assets handler ──────────────────────────────────

describe("catalog_get_user_owned_assets handler", () => {
  it("finds the user on the first page and slices ownedAssetIds", async () => {
    const target = makeUser("u-1", ["a-1", "a-2", "a-3"]);
    const { tools } = makeTools(() => ({ getUsers: [target] }));
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-1", nbPerPage: 10, page: 0 });
    const parsed = parseResult(res) as {
      pagination: {
        page: number;
        nbPerPage: number;
        totalCount: number;
        hasMore: boolean;
      };
      data: string[];
    };
    expect(parsed.data).toEqual(["a-1", "a-2", "a-3"]);
    expect(parsed.pagination).toEqual({
      page: 0,
      nbPerPage: 10,
      totalCount: 3,
      hasMore: false,
    });
  });

  it("iterates pages until the user is found", async () => {
    const target = makeUser("u-target", ["a-1", "a-2"]);
    let calls = 0;
    const { client, tools } = makeTools(() => {
      calls += 1;
      // First two pages: full filler with no match. Third page contains
      // the target plus filler so the iterator must go past page 0.
      if (calls <= 2) return { getUsers: fillerUsers(LOOKUP_PAGE_SIZE) };
      return { getUsers: [target, ...fillerUsers(LOOKUP_PAGE_SIZE - 1)] };
    });
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-target" });
    const parsed = parseResult(res) as { data: string[] };
    expect(parsed.data).toEqual(["a-1", "a-2"]);
    expect(client.calls).toHaveLength(3);
    // Each iteration call hits GET_USERS with a 500-row page and the next
    // 0-indexed page number.
    for (let i = 0; i < client.calls.length; i++) {
      const vars = client.calls[i].variables as Record<string, unknown>;
      const pagination = vars.pagination as Record<string, unknown>;
      expect(client.calls[i].document).toBe(GET_USERS);
      expect(pagination.nbPerPage).toBe(LOOKUP_PAGE_SIZE);
      expect(pagination.page).toBe(i);
    }
  });

  it("returns notFound (absent — full directory scanned) when a partial page comes back without a match", async () => {
    const { client, tools } = makeTools(() => ({
      getUsers: fillerUsers(10),
    }));
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-missing" });
    const parsed = parseResult(res) as {
      notFound: true;
      userId: string;
      reason: string;
      scanCeilingHit?: boolean;
      usersScanned?: number;
    };
    expect(parsed.notFound).toBe(true);
    expect(parsed.userId).toBe("u-missing");
    // Absent branch must NOT advertise itself as a ceiling hit — the user
    // really doesn't exist. The reason should call out a complete scan.
    expect(parsed.scanCeilingHit).toBeUndefined();
    expect(parsed.usersScanned).toBeUndefined();
    expect(parsed.reason).toMatch(/full user directory was scanned/i);
    // A partial page (< 500 rows) is sufficient evidence that the user
    // doesn't exist — the iterator must stop after that single call.
    expect(client.calls).toHaveLength(1);
  });

  it("returns notFound (ceiling) after the 10k-user ceiling without a match", async () => {
    const { client, tools } = makeTools(() => ({
      getUsers: fillerUsers(LOOKUP_PAGE_SIZE),
    }));
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-missing" });
    const parsed = parseResult(res) as {
      notFound: true;
      reason: string;
      scanCeilingHit?: boolean;
      usersScanned?: number;
    };
    expect(parsed.notFound).toBe(true);
    expect(parsed.reason).toMatch(/10,?000/);
    expect(parsed.reason).toMatch(/scan ceiling/i);
    expect(parsed.scanCeilingHit).toBe(true);
    expect(parsed.usersScanned).toBe(LOOKUP_PAGE_SIZE * LOOKUP_MAX_PAGES);
    expect(client.calls).toHaveLength(LOOKUP_MAX_PAGES);
  });

  it("paginates ownedAssetIds correctly past the first page", async () => {
    const ids = Array.from({ length: 25 }, (_, i) => `a-${i}`);
    const target = makeUser("u-1", ids);
    const { tools } = makeTools(() => ({ getUsers: [target] }));
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-1", nbPerPage: 10, page: 1 });
    const parsed = parseResult(res) as {
      pagination: { page: number; totalCount: number; hasMore: boolean };
      data: string[];
    };
    expect(parsed.data).toEqual(ids.slice(10, 20));
    expect(parsed.pagination.page).toBe(1);
    expect(parsed.pagination.totalCount).toBe(25);
    // Page 1 of 25 with nbPerPage 10 → indices 10-19 → 5 more remain
    expect(parsed.pagination.hasMore).toBe(true);
  });

  it("returns hasMore=false on the final partial page", async () => {
    const ids = Array.from({ length: 25 }, (_, i) => `a-${i}`);
    const target = makeUser("u-1", ids);
    const { tools } = makeTools(() => ({ getUsers: [target] }));
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-1", nbPerPage: 10, page: 2 });
    const parsed = parseResult(res) as {
      pagination: { hasMore: boolean };
      data: string[];
    };
    // Page 2 with nbPerPage 10 starts at index 20; only 5 rows left.
    expect(parsed.data).toHaveLength(5);
    expect(parsed.pagination.hasMore).toBe(false);
  });

  it("handles a user with empty ownedAssetIds", async () => {
    const target = makeUser("u-1", []);
    const { tools } = makeTools(() => ({ getUsers: [target] }));
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-1" });
    const parsed = parseResult(res) as {
      pagination: { totalCount: number; hasMore: boolean };
      data: string[];
    };
    expect(parsed.data).toEqual([]);
    expect(parsed.pagination.totalCount).toBe(0);
    expect(parsed.pagination.hasMore).toBe(false);
  });

  it("handles a user with a missing (undefined) ownedAssetIds field", async () => {
    // Defensive: the API typing says ownedAssetIds is non-optional, but
    // older payloads have surfaced without it. The slice helper coerces
    // undefined to [].
    const partialUser = { ...makeUser("u-1"), ownedAssetIds: undefined };
    const { tools } = makeTools(() => ({ getUsers: [partialUser] }));
    const tool = findIn(tools, "catalog_get_user_owned_assets");

    const res = await tool.handler({ userId: "u-1" });
    const parsed = parseResult(res) as {
      pagination: { totalCount: number };
      data: string[];
    };
    expect(parsed.data).toEqual([]);
    expect(parsed.pagination.totalCount).toBe(0);
  });
});

// ── catalog_get_team_members handler ──────────────────────────────────────

describe("catalog_get_team_members handler", () => {
  it("finds the team on the first page and slices memberIds", async () => {
    const team = makeTeam("t-1", ["u-1", "u-2", "u-3"], []);
    const { client, tools } = makeTools(() => ({ getTeams: [team] }));
    const tool = findIn(tools, "catalog_get_team_members");

    const res = await tool.handler({ teamId: "t-1", nbPerPage: 25, page: 0 });
    const parsed = parseResult(res) as {
      pagination: { totalCount: number; hasMore: boolean };
      data: string[];
    };
    expect(parsed.data).toEqual(["u-1", "u-2", "u-3"]);
    expect(parsed.pagination.totalCount).toBe(3);
    expect(parsed.pagination.hasMore).toBe(false);
    expect(client.calls[0].document).toBe(GET_TEAMS);
  });

  it("iterates pages until the team is found", async () => {
    const target = makeTeam("t-target", ["u-1", "u-2"]);
    let calls = 0;
    const { client, tools } = makeTools(() => {
      calls += 1;
      if (calls === 1) return { getTeams: fillerTeams(LOOKUP_PAGE_SIZE) };
      return { getTeams: [target, ...fillerTeams(LOOKUP_PAGE_SIZE - 1)] };
    });
    const tool = findIn(tools, "catalog_get_team_members");

    const res = await tool.handler({ teamId: "t-target" });
    const parsed = parseResult(res) as { data: string[] };
    expect(parsed.data).toEqual(["u-1", "u-2"]);
    expect(client.calls).toHaveLength(2);
  });

  it("returns notFound (absent) when a partial page comes back without a match", async () => {
    const { client, tools } = makeTools(() => ({ getTeams: fillerTeams(5) }));
    const tool = findIn(tools, "catalog_get_team_members");

    const res = await tool.handler({ teamId: "t-missing" });
    const parsed = parseResult(res) as {
      notFound: true;
      teamId: string;
      reason: string;
      scanCeilingHit?: boolean;
      teamsScanned?: number;
    };
    expect(parsed.notFound).toBe(true);
    expect(parsed.teamId).toBe("t-missing");
    expect(parsed.scanCeilingHit).toBeUndefined();
    expect(parsed.teamsScanned).toBeUndefined();
    expect(parsed.reason).toMatch(/full team directory was scanned/i);
    expect(client.calls).toHaveLength(1);
  });

  it("returns notFound (ceiling) after the 10k-team ceiling without a match", async () => {
    const { client, tools } = makeTools(() => ({
      getTeams: fillerTeams(LOOKUP_PAGE_SIZE),
    }));
    const tool = findIn(tools, "catalog_get_team_members");

    const res = await tool.handler({ teamId: "t-missing" });
    const parsed = parseResult(res) as {
      notFound: true;
      reason: string;
      scanCeilingHit?: boolean;
      teamsScanned?: number;
    };
    expect(parsed.notFound).toBe(true);
    expect(parsed.reason).toMatch(/10,?000/);
    expect(parsed.reason).toMatch(/scan ceiling/i);
    expect(parsed.scanCeilingHit).toBe(true);
    expect(parsed.teamsScanned).toBe(LOOKUP_PAGE_SIZE * LOOKUP_MAX_PAGES);
    expect(client.calls).toHaveLength(LOOKUP_MAX_PAGES);
  });

  it("handles a team with empty memberIds", async () => {
    const team = makeTeam("t-1", [], []);
    const { tools } = makeTools(() => ({ getTeams: [team] }));
    const tool = findIn(tools, "catalog_get_team_members");

    const res = await tool.handler({ teamId: "t-1" });
    const parsed = parseResult(res) as {
      pagination: { totalCount: number; hasMore: boolean };
      data: string[];
    };
    expect(parsed.data).toEqual([]);
    expect(parsed.pagination.totalCount).toBe(0);
    expect(parsed.pagination.hasMore).toBe(false);
  });
});

// ── catalog_get_team_owned_assets handler ─────────────────────────────────

describe("catalog_get_team_owned_assets handler", () => {
  it("slices ownedAssetIds (not memberIds) when both are populated", async () => {
    // Regression guard: the team_owned_assets and team_members handlers
    // both look up the same team, but slice different fields. Mixing them
    // up would silently return the wrong ID list.
    const team = makeTeam("t-1", ["u-1", "u-2"], ["a-1", "a-2", "a-3", "a-4"]);
    const { tools } = makeTools(() => ({ getTeams: [team] }));
    const tool = findIn(tools, "catalog_get_team_owned_assets");

    const res = await tool.handler({ teamId: "t-1", nbPerPage: 100, page: 0 });
    const parsed = parseResult(res) as {
      pagination: { totalCount: number };
      data: string[];
    };
    expect(parsed.data).toEqual(["a-1", "a-2", "a-3", "a-4"]);
    expect(parsed.pagination.totalCount).toBe(4);
  });

  it("paginates past the first page", async () => {
    const ids = Array.from({ length: 30 }, (_, i) => `a-${i}`);
    const team = makeTeam("t-1", [], ids);
    const { tools } = makeTools(() => ({ getTeams: [team] }));
    const tool = findIn(tools, "catalog_get_team_owned_assets");

    const res = await tool.handler({ teamId: "t-1", nbPerPage: 10, page: 1 });
    const parsed = parseResult(res) as {
      pagination: { page: number; hasMore: boolean };
      data: string[];
    };
    expect(parsed.data).toEqual(ids.slice(10, 20));
    expect(parsed.pagination.page).toBe(1);
    expect(parsed.pagination.hasMore).toBe(true);
  });

  it("returns notFound (absent) when the team is missing", async () => {
    const { tools } = makeTools(() => ({ getTeams: fillerTeams(3) }));
    const tool = findIn(tools, "catalog_get_team_owned_assets");

    const res = await tool.handler({ teamId: "t-missing" });
    const parsed = parseResult(res) as {
      notFound: true;
      teamId: string;
      reason: string;
      scanCeilingHit?: boolean;
    };
    expect(parsed.notFound).toBe(true);
    expect(parsed.teamId).toBe("t-missing");
    expect(parsed.scanCeilingHit).toBeUndefined();
    expect(parsed.reason).toMatch(/full team directory was scanned/i);
  });

  it("returns notFound (ceiling) when the team scan exhausts the cap", async () => {
    const { tools } = makeTools(() => ({
      getTeams: fillerTeams(LOOKUP_PAGE_SIZE),
    }));
    const tool = findIn(tools, "catalog_get_team_owned_assets");

    const res = await tool.handler({ teamId: "t-missing" });
    const parsed = parseResult(res) as {
      notFound: true;
      reason: string;
      scanCeilingHit?: boolean;
      teamsScanned?: number;
    };
    expect(parsed.scanCeilingHit).toBe(true);
    expect(parsed.teamsScanned).toBe(LOOKUP_PAGE_SIZE * LOOKUP_MAX_PAGES);
    expect(parsed.reason).toMatch(/scan ceiling/i);
  });
});
