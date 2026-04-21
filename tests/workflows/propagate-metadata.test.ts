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
      // The implementation now does a single unscoped-by-type scan and
      // counts child dashboards inline — mirror that by returning both
      // table-child and dashboard-child edges in the same page.
      const childTableIds = opts.downstreamTableEdges?.get(parent) ?? [];
      const dashCount = opts.downstreamDashboardCounts?.get(parent) ?? 0;
      const allEdges: MockEdge[] = [
        ...childTableIds.map((childTableId) => ({
          parentTableId: parent,
          childTableId,
        })),
        ...Array.from({ length: dashCount }, (_, i) => ({
          parentTableId: parent,
          childDashboardId: `${parent}-d${i}`,
        })),
      ];
      // Respect pagination — the implementation's completeness contract
      // pages until a short page, so returning the full list on every
      // page would either loop forever or mask width-cap checks.
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = allEdges.slice(start, start + vars.pagination.nbPerPage);
      return {
        getLineages: {
          totalCount: allEdges.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
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

  it("counts plannedTablesWithMutations as unique tables, not mutation actions", async () => {
    const source: MockTable = {
      id: "src",
      description: "Source description",
      tagEntities: [{ id: "t1", tag: { label: "pii" } }],
      ownerEntities: [
        {
          id: "o1",
          userId: "u-alice",
          user: { email: "alice@a.com", fullName: "Alice" },
        },
      ],
      teamOwnerEntities: [],
    };
    const target: MockTable = {
      id: "tgt",
      description: null,
      tagEntities: [],
      ownerEntities: [],
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
        axes: ["description", "tags", "owners"],
      })
    );

    const plan = out.plan as Array<{
      changes: {
        description?: { action: string };
        tags?: { action: string };
        owners?: { action: string };
      };
    }>;
    expect(plan).toHaveLength(1);
    // All three axes produce "add" mutations on the same table.
    expect(plan[0].changes.description?.action).toBe("add");
    expect(plan[0].changes.tags?.action).toBe("add");
    expect(plan[0].changes.owners?.action).toBe("add");

    const summary = out.summary as {
      plannedTablesWithMutations: number;
    };
    // One table with mutations across 3 axes should count as 1, not 3.
    expect(summary.plannedTablesWithMutations).toBe(1);
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

  it("refuses when the depth-1 frontier exceeds the depth-2 width cap (2000 nodes)", async () => {
    // Build a src → 2001 direct children graph and request maxDepth:2.
    // At depth 2 the frontier (the 2001 depth-1 children) exceeds the
    // 2000-node cap; the tool must refuse rather than try to expand.
    const childIds = Array.from({ length: 2001 }, (_, i) => `t-${i}`);
    const tablesByIds = new Map<string, MockTable>(
      childIds.map((id) => [id, { id }])
    );
    const source: MockTable = { id: "src", description: "x" };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds,
      downstreamTableEdges: new Map([["src", childIds]]),
    });
    const tool = definePropagateMetadata(client);
    const res = await tool.handler({ sourceTableId: "src", maxDepth: 2 });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/Graph too wide/);
    expect(parseResult(res).error).toMatch(/2001.*2000-node cap/);
  });

  it("refuses when the depth-2 frontier exceeds the depth-3 width cap (500 nodes)", async () => {
    // src → 1 child at depth 1 → 501 children at depth 2. At depth 3 the
    // frontier exceeds the 500-node cap.
    const deepChildren = Array.from({ length: 501 }, (_, i) => `t-d2-${i}`);
    const tablesByIds = new Map<string, MockTable>([
      ["t-d1", { id: "t-d1" }],
      ...deepChildren.map(
        (id): [string, MockTable] => [id, { id }]
      ),
    ]);
    const source: MockTable = { id: "src", description: "x" };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds,
      downstreamTableEdges: new Map([
        ["src", ["t-d1"]],
        ["t-d1", deepChildren],
      ]),
    });
    const tool = definePropagateMetadata(client);
    const res = await tool.handler({ sourceTableId: "src", maxDepth: 3 });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/Graph too wide/);
    expect(parseResult(res).error).toMatch(/501.*500-node cap/);
  });

  it("throws when lineage pagination exceeds its hard per-node ceiling", async () => {
    // Return full pages of unique table edges forever — loop must throw
    // at LINEAGE_PAGES_PER_NODE_MAX rather than silently return partial.
    let nextChild = 0;
    const source: MockTable = { id: "src", description: "x" };
    const client = makeMockClient((document) => {
      if (document === GET_TABLE_DETAIL) {
        return { getTables: { data: [source] } };
      }
      if (document === GET_LINEAGES) {
        const data = Array.from({ length: 500 }, () => ({
          id: `e-${nextChild}`,
          parentTableId: "src",
          childTableId: `t-${nextChild++}`,
        }));
        return {
          getLineages: { totalCount: 999999, nbPerPage: 500, page: 0, data },
        };
      }
      return {};
    });
    const tool = definePropagateMetadata(client);
    const res = await tool.handler({ sourceTableId: "src" });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/Lineage pagination exceeded/);
  });
});

describe("catalog_propagate_metadata — defaults", () => {
  it("dryRun defaults to true when the caller omits it", async () => {
    // The mutation-safety contract hinges on this default. Pin it so a
    // future refactor that flips it has to update the test deliberately.
    const source: MockTable = { id: "src", description: "x" };
    const target: MockTable = { id: "tgt" };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
    });
    const tool = definePropagateMetadata(client);
    const out = parseResult(await tool.handler({ sourceTableId: "src" }));
    const config = out.config as { dryRun: boolean };
    expect(config.dryRun).toBe(true);
    // And: no mutation calls fired.
    expect(
      client.calls.filter((c) =>
        [UPDATE_TABLES, ATTACH_TAGS, UPSERT_USER_OWNERS, UPSERT_TEAM_OWNERS].includes(
          c.document
        )
      )
    ).toHaveLength(0);
  });
});

describe("catalog_propagate_metadata — confirmation routing", () => {
  it("fail-closes with confirmation_unavailable when dryRun:false and extra.sendRequest is missing", async () => {
    const source: MockTable = { id: "src", description: "x" };
    const target: MockTable = { id: "tgt" };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
    });
    const tool = definePropagateMetadata(client);
    // Clear skip-confirmations env var so the wrapper requires elicitation.
    const prev = process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    delete process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    try {
      // Handler invoked with no `extra` arg — wrapper must refuse.
      const res = await tool.handler({
        sourceTableId: "src",
        dryRun: false,
      });
      expect(res.isError).toBe(true);
      const out = parseResult(res);
      expect(out.error).toMatch(/requires interactive confirmation/i);
      expect((out.detail as { kind: string })?.kind).toBe(
        "confirmation_unavailable"
      );
      // No mutations fired.
      expect(
        client.calls.filter((c) => c.document === UPDATE_TABLES)
      ).toHaveLength(0);
    } finally {
      if (prev !== undefined)
        process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = prev;
    }
  });

  it("returns a decline (non-error) when the user declines the elicitation", async () => {
    const source: MockTable = { id: "src", description: "x" };
    const target: MockTable = { id: "tgt" };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
    });
    const tool = definePropagateMetadata(client);
    const prev = process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    delete process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    try {
      const res = await tool.handler(
        { sourceTableId: "src", dryRun: false },
        {
          sendRequest: async () => ({
            action: "decline",
          }),
        }
      );
      // Decline is user behaviour, not a tool failure — isError stays false.
      expect(res.isError).toBeUndefined();
      const out = parseResult(res);
      expect(out.error).toMatch(/did not confirm/i);
      expect((out.detail as { kind: string }).kind).toBe("user_declined");
      // No mutations fired.
      expect(
        client.calls.filter((c) => c.document === UPDATE_TABLES)
      ).toHaveLength(0);
    } finally {
      if (prev !== undefined)
        process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = prev;
    }
  });
});

describe("catalog_propagate_metadata — partial-failure tracking (tags + owners)", () => {
  function withSkipConfirmations<T>(fn: () => Promise<T>): Promise<T> {
    const prev = process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = "true";
    return fn().finally(() => {
      if (prev === undefined)
        delete process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
      else process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = prev;
    });
  }

  it("executeTags flags partialFailure and records failedAttachments when a batch is rejected", async () => {
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
      // API returns false — the whole batch is treated as rejected.
      attachTagsResponse: () => false,
    });
    const tool = definePropagateMetadata(client);
    const out = await withSkipConfirmations(async () =>
      parseResult(
        await tool.handler({
          sourceTableId: "src",
          axes: ["tags"],
          dryRun: false,
        })
      )
    );
    const tagsExec = (out.execution as { tags: Record<string, unknown> }).tags;
    expect(tagsExec.partialFailure).toBe(true);
    expect(tagsExec.batchesRejected).toBe(1);
    expect(tagsExec.applied).toBeNull();
    expect(tagsExec.failedAttachments).toEqual([
      { entityType: "TABLE", entityId: "a", label: "pii" },
      { entityType: "TABLE", entityId: "b", label: "pii" },
    ]);
  });

  it("executeOwners records per-owner failures when one upsert throws without aborting the rest", async () => {
    const source: MockTable = {
      id: "src",
      description: "x",
      ownerEntities: [
        {
          id: "o1",
          userId: "u-ok",
          user: { email: "ok@a.com", fullName: "OK" },
        },
        {
          id: "o2",
          userId: "u-bad",
          user: { email: "bad@a.com", fullName: "BAD" },
        },
      ],
    };
    const target: MockTable = {
      id: "tgt",
      ownerEntities: [],
      teamOwnerEntities: [],
    };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
      upsertUserOwnersResponse: ({ userId, targetEntities }) => {
        if (userId === "u-bad") {
          throw new Error("synthetic-upsert-failure-for-u-bad");
        }
        return targetEntities.map((_, i) => ({ id: `ue-${i}`, userId }));
      },
    });
    const tool = definePropagateMetadata(client);
    const out = await withSkipConfirmations(async () =>
      parseResult(
        await tool.handler({
          sourceTableId: "src",
          axes: ["owners"],
          dryRun: false,
        })
      )
    );
    const ownersExec = (out.execution as { owners: Record<string, unknown> })
      .owners;
    expect(ownersExec.partialFailure).toBe(true);
    const userFailures = ownersExec.userFailures as Array<{
      userId: string;
      error: string;
    }>;
    expect(userFailures).toHaveLength(1);
    expect(userFailures[0].userId).toBe("u-bad");
    expect(userFailures[0].error).toMatch(/synthetic-upsert-failure-for-u-bad/);
    // The OK upsert still landed — partial progress is preserved, not
    // discarded by the one bad call.
    expect(ownersExec.applied).toBe(1);
  });
});

describe("catalog_propagate_metadata — defensive guards", () => {
  it("throws when UPDATE_TABLES returns a non-array payload (schema drift)", async () => {
    const source: MockTable = { id: "src", description: "x" };
    const target: MockTable = { id: "tgt", description: null };
    const client = makeRouter({
      sourceDetail: source,
      tablesByIds: new Map([["tgt", target]]),
      downstreamTableEdges: new Map([["src", ["tgt"]]]),
      // Simulate schema drift: API returns non-array where the client
      // expects Table[]. Must surface as isError, not collapse to "0 applied".
      updateTablesResponse: () =>
        ({ unexpected: "payload" } as unknown as undefined),
    });
    const tool = definePropagateMetadata(client);
    const prev = process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = "true";
    try {
      const res = await tool.handler({ sourceTableId: "src", dryRun: false });
      expect(res.isError).toBe(true);
      expect(parseResult(res).error).toMatch(/non-array payload/);
    } finally {
      if (prev === undefined)
        delete process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
      else process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = prev;
    }
  });
});
