import { describe, it, expect } from "vitest";
import { definePropagateMetadata } from "../../src/workflows/propagate-metadata.js";
import {
  GET_TABLE_DETAIL,
  GET_TABLES_DETAIL_BATCH,
  GET_LINEAGES,
  UPDATE_TABLES,
  ATTACH_TAGS,
  UPSERT_USER_OWNERS,
  UPSERT_TEAM_OWNERS,
} from "../../src/catalog/operations.js";
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
  description?: string | null;
  externalDescription?: string | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
  tagEntities?: Array<Record<string, unknown>>;
}

interface MockEdge {
  parentTableId?: string;
  childTableId?: string;
  childDashboardId?: string;
}

interface RouterOpts {
  sourceDetail?: MockTable | null;
  tablesByIds?: Map<string, MockTable>;
  // Map from parent tableId -> downstream table edges
  downstreamTableEdges?: Map<string, string[]>;
  // Map from parent tableId -> number of downstream dashboards
  downstreamDashboardCounts?: Map<string, number>;
  // Record of mutation responses / behaviour
  updateTablesResponse?: (
    input: Array<{ id: string; externalDescription: string }>
  ) => unknown;
  attachTagsResponse?: (input: unknown) => boolean;
  upsertUserOwnersResponse?: (input: {
    userId: string;
    targetEntities: unknown[];
  }) => unknown;
  upsertTeamOwnersResponse?: (input: {
    teamId: string;
    targetEntities: unknown[];
  }) => unknown;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLE_DETAIL) {
      return {
        getTables: { data: opts.sourceDetail ? [opts.sourceDetail] : [] },
      };
    }
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as { scope?: { ids?: string[] } };
      const ids = vars.scope?.ids ?? [];
      const rows = ids
        .map((id) => opts.tablesByIds?.get(id))
        .filter((r): r is MockTable => !!r);
      return {
        getTables: {
          totalCount: rows.length,
          nbPerPage: ids.length,
          page: 0,
          data: rows,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: {
          parentTableId?: string;
          withChildAssetType?: string;
        };
        pagination: { nbPerPage: number; page: number };
      };
      const parent = vars.scope?.parentTableId ?? "";
      if (vars.scope?.withChildAssetType === "DASHBOARD") {
        const count = opts.downstreamDashboardCounts?.get(parent) ?? 0;
        return {
          getLineages: {
            totalCount: count,
            nbPerPage: 1,
            page: 0,
            data: [],
          },
        };
      }
      // Table→table edges. Only page 0 is exercised in these tests.
      const childIds = opts.downstreamTableEdges?.get(parent) ?? [];
      const edges: MockEdge[] = childIds.map((childTableId) => ({
        parentTableId: parent,
        childTableId,
      }));
      return {
        getLineages: {
          totalCount: edges.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: edges,
        },
      };
    }
    if (document === UPDATE_TABLES) {
      const vars = variables as {
        data: Array<{ id: string; externalDescription: string }>;
      };
      const response = opts.updateTablesResponse
        ? opts.updateTablesResponse(vars.data)
        : vars.data.map((row) => ({
            id: row.id,
            name: null,
            externalDescription: row.externalDescription,
          }));
      return { updateTables: response };
    }
    if (document === ATTACH_TAGS) {
      const success = opts.attachTagsResponse
        ? opts.attachTagsResponse(variables)
        : true;
      return { attachTags: success };
    }
    if (document === UPSERT_USER_OWNERS) {
      const vars = variables as {
        data: { userId: string; targetEntities: unknown[] };
      };
      const response = opts.upsertUserOwnersResponse
        ? opts.upsertUserOwnersResponse(vars.data)
        : vars.data.targetEntities.map((_, i) => ({
            id: `ue-${i}`,
            userId: vars.data.userId,
          }));
      return { upsertUserOwners: response };
    }
    if (document === UPSERT_TEAM_OWNERS) {
      const vars = variables as {
        data: { teamId: string; targetEntities: unknown[] };
      };
      const response = opts.upsertTeamOwnersResponse
        ? opts.upsertTeamOwnersResponse(vars.data)
        : vars.data.targetEntities.map((_, i) => ({
            id: `te-${i}`,
            teamId: vars.data.teamId,
          }));
      return { upsertTeamOwners: response };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_propagate_metadata — source not found", () => {
  it("returns notFound when the source table id doesn't resolve", async () => {
    const client = makeRouter({ sourceDetail: null });
    const tool = definePropagateMetadata(client);
    const out = parseResult(
      await tool.handler({ sourceTableId: "missing" })
    );
    expect(out).toEqual({ notFound: true, sourceTableId: "missing" });
  });
});

describe("catalog_propagate_metadata — dry-run plan (default)", () => {
  it("emits an add plan for description when target has none and never mutates", async () => {
    const source: MockTable = {
      id: "src",
      name: "SOURCE",
      description: "Canonical orders fact, one row per order.",
    };
    const target: MockTable = {
      id: "tgt",
      name: "DOWNSTREAM",
      description: null,
    };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
      downstreamDashboardCounts: new Map([
        ["src", 0],
        ["tgt", 0],
      ]),
    });
    const tool = definePropagateMetadata(client);
    const out = parseResult(
      await tool.handler({ sourceTableId: "src" })
    );

    // Dry run: no UPDATE_TABLES / ATTACH_TAGS / UPSERT_*_OWNERS calls.
    const documents = client.calls.map((c) => c.document);
    expect(documents).not.toContain(UPDATE_TABLES);
    expect(documents).not.toContain(ATTACH_TAGS);
    expect(documents).not.toContain(UPSERT_USER_OWNERS);
    expect(documents).not.toContain(UPSERT_TEAM_OWNERS);

    const plan = out.plan as Array<{
      tableId: string;
      depth: number;
      changes: { description?: { action: string; after: string } };
    }>;
    expect(plan).toHaveLength(1);
    expect(plan[0].tableId).toBe("tgt");
    expect(plan[0].depth).toBe(1);
    expect(plan[0].changes.description).toMatchObject({
      action: "add",
      after: "Canonical orders fact, one row per order.",
    });

    const summary = out.summary as {
      actionsByAxis: { description: { add: number; update: number; skip: number } };
      plannedTablesWithMutations: number;
    };
    expect(summary.actionsByAxis.description).toEqual({
      add: 1,
      update: 0,
      skip: 0,
    });
    expect(summary.plannedTablesWithMutations).toBe(1);
  });

  it("skips description under ifEmpty when target already has one", async () => {
    const source: MockTable = {
      id: "src",
      description: "Source description",
    };
    const target: MockTable = {
      id: "tgt",
      description: "Target already documented",
    };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
    });
    const tool = definePropagateMetadata(client);
    const out = parseResult(await tool.handler({ sourceTableId: "src" }));
    const plan = out.plan as Array<{
      changes: { description: { action: string; reason: string } };
    }>;
    expect(plan[0].changes.description.action).toBe("skip");
    expect(plan[0].changes.description.reason).toMatch(/ifEmpty/);
  });

  it("emits an update plan when overwritePolicy=overwrite and descriptions differ", async () => {
    const source: MockTable = { id: "src", description: "Source description" };
    const target: MockTable = {
      id: "tgt",
      description: "Old target description",
    };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
    });
    const tool = definePropagateMetadata(client);
    const out = parseResult(
      await tool.handler({
        sourceTableId: "src",
        overwritePolicy: "overwrite",
      })
    );
    const plan = out.plan as Array<{
      changes: { description: { action: string; before: string; after: string } };
    }>;
    expect(plan[0].changes.description).toMatchObject({
      action: "update",
      before: "Old target description",
      after: "Source description",
    });
  });

  it("computes tag diffs (additive) with alreadyPresent tracking", async () => {
    const source: MockTable = {
      id: "src",
      description: "x",
      tagEntities: [
        { id: "t1", tag: { label: "pii" } },
        { id: "t2", tag: { label: "finance" } },
      ],
    };
    const target: MockTable = {
      id: "tgt",
      description: "y",
      tagEntities: [{ id: "t1", tag: { label: "pii" } }],
    };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
    });
    const tool = definePropagateMetadata(client);
    const out = parseResult(
      await tool.handler({
        sourceTableId: "src",
        axes: ["tags"],
        overwritePolicy: "overwrite",
      })
    );
    const plan = out.plan as Array<{
      changes: {
        tags: { action: string; added: string[]; alreadyPresent: string[] };
      };
    }>;
    expect(plan[0].changes.tags).toEqual({
      action: "add",
      reason: expect.any(String),
      added: ["finance"],
      alreadyPresent: ["pii"],
    });
  });

  it("computes owners diffs (additive) with alreadyOwnedBy tracking", async () => {
    const source: MockTable = {
      id: "src",
      description: "x",
      ownerEntities: [
        {
          id: "o1",
          userId: "u-alice",
          user: { email: "alice@a.com", fullName: "Alice" },
        },
        {
          id: "o2",
          userId: "u-bob",
          user: { email: "bob@a.com", fullName: "Bob" },
        },
      ],
      teamOwnerEntities: [
        { id: "to1", teamId: "team-x", team: { name: "X" } },
      ],
    };
    const target: MockTable = {
      id: "tgt",
      ownerEntities: [
        {
          id: "o1",
          userId: "u-alice",
          user: { email: "alice@a.com", fullName: "Alice" },
        },
      ],
      teamOwnerEntities: [],
    };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
    });
    const tool = definePropagateMetadata(client);
    const out = parseResult(
      await tool.handler({
        sourceTableId: "src",
        axes: ["owners"],
        overwritePolicy: "overwrite",
      })
    );
    const plan = out.plan as Array<{
      changes: {
        owners: {
          action: string;
          addedUsers: Array<{ userId: string }>;
          addedTeams: Array<{ teamId: string }>;
          alreadyOwnedBy: { userIds: string[]; teamIds: string[] };
        };
      };
    }>;
    expect(plan[0].changes.owners.action).toBe("add");
    expect(plan[0].changes.owners.addedUsers.map((u) => u.userId)).toEqual([
      "u-bob",
    ]);
    expect(plan[0].changes.owners.addedTeams.map((t) => t.teamId)).toEqual([
      "team-x",
    ]);
    expect(plan[0].changes.owners.alreadyOwnedBy).toEqual({
      userIds: ["u-alice"],
      teamIds: [],
    });
  });
});

describe("catalog_propagate_metadata — execution path", () => {
  // The SKIP env var lets us exercise the mutation path without a transport
  // that implements elicitation. We set it per-test and tear it down in a
  // finally to avoid leaking state across describes.
  function withSkipConfirmations<T>(fn: () => Promise<T>): Promise<T> {
    const prev = process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = "true";
    return fn().finally(() => {
      if (prev === undefined)
        delete process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
      else process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = prev;
    });
  }

  it("executes description updates and reports applied count", async () => {
    const source: MockTable = {
      id: "src",
      description: "New source description",
    };
    const targets = new Map<string, MockTable>([
      ["a", { id: "a", description: null }],
      ["b", { id: "b", description: null }],
    ]);
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: targets,
      downstreamTableEdges: new Map([["src", ["a", "b"]]]),
    });
    const tool = definePropagateMetadata(client);

    const out = await withSkipConfirmations(async () =>
      parseResult(
        await tool.handler({ sourceTableId: "src", dryRun: false })
      )
    );
    const execution = out.execution as {
      description: { applied: number; planned: number };
    };
    expect(execution.description).toEqual({ applied: 2, planned: 2 });
    const updateCalls = client.calls.filter((c) => c.document === UPDATE_TABLES);
    expect(updateCalls).toHaveLength(1);
    const payload = updateCalls[0].variables as {
      data: Array<{ id: string; externalDescription: string }>;
    };
    expect(payload.data).toEqual([
      { id: "a", externalDescription: "New source description" },
      { id: "b", externalDescription: "New source description" },
    ]);
  });

  it("flags partialFailure when UPDATE_TABLES returns fewer rows than sent", async () => {
    const source: MockTable = { id: "src", description: "desc" };
    const targets = new Map<string, MockTable>([
      ["a", { id: "a" }],
      ["b", { id: "b" }],
    ]);
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: targets,
      downstreamTableEdges: new Map([["src", ["a", "b"]]]),
      updateTablesResponse: (input) => [
        { id: input[0].id, name: null, externalDescription: "desc" },
      ], // only 1 of 2 returned
    });
    const tool = definePropagateMetadata(client);
    const out = await withSkipConfirmations(async () =>
      parseResult(
        await tool.handler({ sourceTableId: "src", dryRun: false })
      )
    );
    const execution = out.execution as {
      description: { applied: number; planned: number; partialFailure?: boolean };
    };
    expect(execution.description.applied).toBe(1);
    expect(execution.description.planned).toBe(2);
    expect(execution.description.partialFailure).toBe(true);
  });

  it("executes owner upserts grouped by userId / teamId", async () => {
    const source: MockTable = {
      id: "src",
      description: "x",
      ownerEntities: [
        {
          id: "o",
          userId: "u1",
          user: { email: "u1@a.com", fullName: "U1" },
        },
      ],
      teamOwnerEntities: [
        { id: "to", teamId: "team-1", team: { name: "T" } },
      ],
    };
    const targets = new Map<string, MockTable>([
      ["a", { id: "a", ownerEntities: [], teamOwnerEntities: [] }],
      ["b", { id: "b", ownerEntities: [], teamOwnerEntities: [] }],
    ]);
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: targets,
      downstreamTableEdges: new Map([["src", ["a", "b"]]]),
    });
    const tool = definePropagateMetadata(client);
    await withSkipConfirmations(async () =>
      tool.handler({
        sourceTableId: "src",
        axes: ["owners"],
        dryRun: false,
      })
    );
    const userCalls = client.calls.filter(
      (c) => c.document === UPSERT_USER_OWNERS
    );
    const teamCalls = client.calls.filter(
      (c) => c.document === UPSERT_TEAM_OWNERS
    );
    expect(userCalls).toHaveLength(1);
    expect(teamCalls).toHaveLength(1);
    expect(userCalls[0].variables).toEqual({
      data: {
        userId: "u1",
        targetEntities: [
          { entityType: "TABLE", entityId: "a" },
          { entityType: "TABLE", entityId: "b" },
        ],
      },
    });
    expect(teamCalls[0].variables).toEqual({
      data: {
        teamId: "team-1",
        targetEntities: [
          { entityType: "TABLE", entityId: "a" },
          { entityType: "TABLE", entityId: "b" },
        ],
      },
    });
  });

  it("executes tag attaches as one batched call with (tableId, label) rows", async () => {
    const source: MockTable = {
      id: "src",
      description: "x",
      tagEntities: [{ id: "t1", tag: { label: "pii" } }],
    };
    const targets = new Map<string, MockTable>([
      ["a", { id: "a", tagEntities: [] }],
      ["b", { id: "b", tagEntities: [] }],
    ]);
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: targets,
      downstreamTableEdges: new Map([["src", ["a", "b"]]]),
    });
    const tool = definePropagateMetadata(client);
    await withSkipConfirmations(async () =>
      tool.handler({
        sourceTableId: "src",
        axes: ["tags"],
        dryRun: false,
      })
    );
    const calls = client.calls.filter((c) => c.document === ATTACH_TAGS);
    expect(calls).toHaveLength(1);
    expect(calls[0].variables).toEqual({
      data: [
        { entityType: "TABLE", entityId: "a", label: "pii" },
        { entityType: "TABLE", entityId: "b", label: "pii" },
      ],
    });
  });
});

describe("catalog_propagate_metadata — traversal safety", () => {
  it("counts downstream dashboards as dashboardsSkipped (never mutates them)", async () => {
    // With maxDepth=2, the BFS expands src at depth=1 (counts 3 dashboards)
    // and tgt at depth=2 (counts 2 dashboards). tgt has no further table
    // children so the traversal naturally ends.
    const source: MockTable = { id: "src", description: "x" };
    const target: MockTable = { id: "tgt" };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
      downstreamDashboardCounts: new Map([
        ["src", 3],
        ["tgt", 2],
      ]),
    });
    const tool = definePropagateMetadata(client);
    const out = parseResult(
      await tool.handler({ sourceTableId: "src", maxDepth: 2 })
    );
    const traversal = out.traversal as { dashboardsSkipped: number };
    expect(traversal.dashboardsSkipped).toBe(5);
  });

  it("refuses when enrichment misses a downstream table reached via lineage", async () => {
    const source: MockTable = { id: "src", description: "x" };
    const client = makeRouter({
      sourceDetail: source,
      // tgt-missing isn't in tablesByIds — will not enrich
      tablesByIds: new Map(),
      downstreamTableEdges: new Map([["src", ["tgt-missing"]]]),
    });
    const tool = definePropagateMetadata(client);
    const res = await tool.handler({ sourceTableId: "src" });
    expect(res.isError).toBe(true);
    const out = parseResult(res);
    expect(out.error).toMatch(/Detail enrichment returned no row/);
    expect(out.error).toMatch(/tgt-missing/);
  });
});
