import { describe, it, expect } from "vitest";
import type { CatalogClient } from "../../src/client.js";
import { defineLineageTools } from "../../src/mcp/lineage.js";

/**
 * Coverage for lineage hydration's multi-batch fetch loop, direction
 * inference, ISO timestamp conversion, and the hydrate=false path. The
 * existing lineage-hydration.test.ts covers fallback-kind cases when
 * hydration misses a single ID; this file exercises the surrounding
 * batching, direction, and timestamp logic.
 */

interface MockCallLog {
  document: string;
  variables: Record<string, unknown> | undefined;
}

function recordingClient(
  responder: (
    document: string,
    variables: Record<string, unknown> | undefined
  ) => unknown
): { client: CatalogClient; calls: MockCallLog[] } {
  const calls: MockCallLog[] = [];
  const client: CatalogClient = {
    endpoint: "https://mock.invalid/graphql",
    region: "eu",
    async execute<TData>(
      document: string,
      variables?: Record<string, unknown>
    ): Promise<TData> {
      calls.push({ document, variables });
      return responder(document, variables) as TData;
    },
    async executeRaw() {
      throw new Error("executeRaw not used by lineage tools");
    },
  };
  return { client, calls };
}

function parseHandlerResult(result: {
  content: Array<{ type: string; text: string }>;
}): unknown {
  return JSON.parse(result.content[0].text);
}

// ── Multi-batch hydration ──────────────────────────────────────────────────
//
// HYDRATION_BATCH_SIZE = 500. A single 500-row lineage page can reference
// 1000 distinct table IDs (parent + child per edge), so hydration must
// page the table-id list rather than blowing past the GraphQL ids[]
// payload cap. Without this, IDs past the first 500 silently fell back
// to anonymous { id, kind } with no name, producing a hydration result
// that looked successful but was missing names.

describe("catalog_get_lineages multi-batch hydration", () => {
  it("splits >500 distinct table IDs into multiple getTables hydration calls and resolves every name", async () => {
    // 600 distinct edges, each with a unique parent + child table ID →
    // 1200 distinct table IDs total. Forces 3 hydration batches (500/500/200).
    const edges: Array<Record<string, unknown>> = [];
    for (let i = 0; i < 600; i++) {
      edges.push({
        id: `edge-${i}`,
        parentTableId: `parent-${i}`,
        parentDashboardId: null,
        childTableId: `child-${i}`,
        childDashboardId: null,
        createdAt: 1700000000000,
        refreshedAt: 1700000000001,
      });
    }

    const { client, calls } = recordingClient((document, variables) => {
      if (document.includes("getLineages")) {
        return {
          getLineages: {
            totalCount: 600,
            nbPerPage: 600,
            page: 0,
            data: edges,
          },
        };
      }
      if (document.includes("CatalogGetTablesSummary")) {
        const ids =
          (variables?.scope as { ids?: string[] } | undefined)?.ids ?? [];
        return {
          getTables: {
            totalCount: ids.length,
            nbPerPage: ids.length,
            page: 0,
            data: ids.map((id) => ({ id, name: `name-${id}` })),
          },
        };
      }
      throw new Error(`unexpected query: ${document.slice(0, 80)}`);
    });

    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler({
      parentTableId: "any-parent",
      hydrate: true,
      nbPerPage: 600,
    });

    const tableHydrationCalls = calls.filter((c) =>
      c.document.includes("CatalogGetTablesSummary")
    );
    // 1200 distinct table IDs / 500 batch = 3 batches (500 + 500 + 200)
    expect(tableHydrationCalls).toHaveLength(3);
    const batchSizes = tableHydrationCalls
      .map(
        (c) =>
          ((c.variables?.scope as { ids?: string[] } | undefined)?.ids ?? [])
            .length
      )
      .sort((a, b) => b - a);
    expect(batchSizes).toEqual([500, 500, 200]);

    const parsed = parseHandlerResult(result) as {
      data: Array<{
        parent: { id: string; name?: string };
        child: { id: string; name?: string };
      }>;
    };
    // Every edge across the 3 batches must have hydrated names — no silent
    // fallback to nameless `{ id, kind }`. This is the regression guard.
    expect(parsed.data).toHaveLength(600);
    for (const edge of parsed.data) {
      expect(edge.parent.name).toBe(`name-${edge.parent.id}`);
      expect(edge.child.name).toBe(`name-${edge.child.id}`);
    }
  });

  it("fans table-id batches and dashboard-id batches in parallel without cross-contaminating", async () => {
    const edges: Array<Record<string, unknown>> = [];
    for (let i = 0; i < 100; i++) {
      // Mix of table parents + dashboard children
      edges.push({
        id: `edge-${i}`,
        parentTableId: `t-${i}`,
        parentDashboardId: null,
        childTableId: null,
        childDashboardId: `d-${i}`,
        createdAt: null,
        refreshedAt: null,
      });
    }
    const { client, calls } = recordingClient((document, variables) => {
      if (document.includes("getLineages")) {
        return {
          getLineages: {
            totalCount: 100,
            nbPerPage: 100,
            page: 0,
            data: edges,
          },
        };
      }
      if (document.includes("CatalogGetTablesSummary")) {
        const ids =
          (variables?.scope as { ids?: string[] } | undefined)?.ids ?? [];
        return {
          getTables: {
            totalCount: ids.length,
            nbPerPage: ids.length,
            page: 0,
            data: ids.map((id) => ({ id, name: `T:${id}` })),
          },
        };
      }
      if (document.includes("CatalogGetDashboardsSummary")) {
        const ids =
          (variables?.scope as { ids?: string[] } | undefined)?.ids ?? [];
        return {
          getDashboards: {
            totalCount: ids.length,
            nbPerPage: ids.length,
            page: 0,
            data: ids.map((id) => ({ id, name: `D:${id}` })),
          },
        };
      }
      throw new Error(`unexpected query: ${document.slice(0, 80)}`);
    });

    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler({
      parentTableId: "any-parent",
      hydrate: true,
      nbPerPage: 100,
    });

    expect(
      calls.filter((c) => c.document.includes("CatalogGetTablesSummary"))
    ).toHaveLength(1);
    expect(
      calls.filter((c) => c.document.includes("CatalogGetDashboardsSummary"))
    ).toHaveLength(1);

    const parsed = parseHandlerResult(result) as {
      data: Array<{
        parent: { id: string; name?: string; kind: string };
        child: { id: string; name?: string; kind: string };
      }>;
    };
    expect(parsed.data).toHaveLength(100);
    for (const edge of parsed.data) {
      expect(edge.parent.kind).toBe("TABLE");
      expect(edge.parent.name).toBe(`T:${edge.parent.id}`);
      expect(edge.child.kind).toBe("DASHBOARD");
      expect(edge.child.name).toBe(`D:${edge.child.id}`);
    }
  });
});

// ── hydrate=false skips all hydration calls ─────────────────────────────────

describe("catalog_get_lineages hydrate=false", () => {
  it("does not issue any hydration queries when hydrate is false", async () => {
    const { client, calls } = recordingClient((document) => {
      if (document.includes("getLineages")) {
        return {
          getLineages: {
            totalCount: 1,
            nbPerPage: 20,
            page: 0,
            data: [
              {
                id: "edge-1",
                parentTableId: "t1",
                parentDashboardId: null,
                childTableId: "t2",
                childDashboardId: null,
                createdAt: 1700000000000,
                refreshedAt: null,
              },
            ],
          },
        };
      }
      throw new Error(`unexpected query: ${document.slice(0, 80)}`);
    });

    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler({ parentTableId: "t1" });

    // Only one call — no hydration follow-ups
    expect(calls).toHaveLength(1);
    const parsed = parseHandlerResult(result) as {
      data: Array<Record<string, unknown>>;
    };
    expect(parsed.data).toHaveLength(1);
    // Without hydrate, there's no `parent` / `child` enrichment object
    expect(parsed.data[0]).not.toHaveProperty("parent");
    expect(parsed.data[0]).not.toHaveProperty("child");
  });

  it("omits hydration queries when hydrate is unset (default behavior)", async () => {
    const { client, calls } = recordingClient((document) => {
      if (document.includes("getLineages")) {
        return {
          getLineages: {
            totalCount: 0,
            nbPerPage: 20,
            page: 0,
            data: [],
          },
        };
      }
      throw new Error(`unexpected query: ${document.slice(0, 80)}`);
    });

    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    await handler({ parentTableId: "t-empty" });
    expect(calls).toHaveLength(1);
  });
});

// ── Direction inference ────────────────────────────────────────────────────
//
// The `direction` field is derived from the scope:
//   parent-only filter → downstream (we're walking children)
//   child-only filter  → upstream   (we're walking parents)
//   parent + child     → specific
//   neither            → undefined  (no direction stamped)

describe("catalog_get_lineages direction inference", () => {
  function singleEdgeClient(): CatalogClient {
    return {
      endpoint: "https://mock.invalid/graphql",
      region: "eu",
      async execute<TData>(document: string): Promise<TData> {
        if (document.includes("getLineages")) {
          return {
            getLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "edge-1",
                  parentTableId: "t1",
                  parentDashboardId: null,
                  childTableId: "t2",
                  childDashboardId: null,
                  createdAt: null,
                  refreshedAt: null,
                },
              ],
            },
          } as TData;
        }
        throw new Error(`unexpected: ${document.slice(0, 60)}`);
      },
      async executeRaw() {
        throw new Error("executeRaw not used");
      },
    };
  }

  async function run(args: Record<string, unknown>): Promise<string | undefined> {
    const tools = defineLineageTools(singleEdgeClient());
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler(args);
    const parsed = parseHandlerResult(result) as {
      data: Array<{ direction?: string }>;
    };
    return parsed.data[0]?.direction;
  }

  it("stamps downstream when only a parent filter is set", async () => {
    expect(await run({ parentTableId: "t1" })).toBe("downstream");
  });

  it("stamps downstream for parentDashboardId only", async () => {
    expect(await run({ parentDashboardId: "d1" })).toBe("downstream");
  });

  it("stamps upstream when only a child filter is set", async () => {
    expect(await run({ childTableId: "t1" })).toBe("upstream");
  });

  it("stamps specific when parent and child are both set", async () => {
    expect(
      await run({ parentTableId: "t1", childTableId: "t2" })
    ).toBe("specific");
  });

  it("omits direction when neither parent nor child is set", async () => {
    expect(await run({ lineageType: "AUTOMATIC" })).toBeUndefined();
  });
});

// ── ISO timestamp enrichment ───────────────────────────────────────────────

describe("catalog_get_lineages ISO timestamp conversion", () => {
  function timestampClient(
    createdAt: unknown,
    refreshedAt: unknown
  ): CatalogClient {
    return {
      endpoint: "https://mock.invalid/graphql",
      region: "eu",
      async execute<TData>(document: string): Promise<TData> {
        if (document.includes("getLineages")) {
          return {
            getLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "edge-1",
                  parentTableId: "t1",
                  parentDashboardId: null,
                  childTableId: "t2",
                  childDashboardId: null,
                  createdAt,
                  refreshedAt,
                },
              ],
            },
          } as TData;
        }
        throw new Error("unexpected");
      },
      async executeRaw() {
        throw new Error("executeRaw not used");
      },
    };
  }

  async function getEdge(client: CatalogClient): Promise<Record<string, unknown>> {
    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler({ parentTableId: "t1" });
    const parsed = parseHandlerResult(result) as {
      data: Array<Record<string, unknown>>;
    };
    return parsed.data[0];
  }

  it("converts numeric createdAt and refreshedAt to ISO strings", async () => {
    const edge = await getEdge(timestampClient(1700000000000, 1700000003000));
    expect(edge.createdAtIso).toBe("2023-11-14T22:13:20.000Z");
    expect(edge.refreshedAtIso).toBe("2023-11-14T22:13:23.000Z");
  });

  it("emits createdAtIso=undefined when createdAt is null", async () => {
    const edge = await getEdge(timestampClient(null, 1700000000000));
    // JSON serialization drops undefined fields entirely
    expect(edge).not.toHaveProperty("createdAtIso");
    expect(edge.refreshedAtIso).toBe("2023-11-14T22:13:20.000Z");
  });

  it("emits no Iso fields when both timestamps are missing", async () => {
    const edge = await getEdge(timestampClient(null, null));
    expect(edge).not.toHaveProperty("createdAtIso");
    expect(edge).not.toHaveProperty("refreshedAtIso");
  });

  it("emits no Iso field for non-finite numeric timestamps", async () => {
    const edge = await getEdge(timestampClient(Number.NaN, Number.POSITIVE_INFINITY));
    expect(edge).not.toHaveProperty("createdAtIso");
    expect(edge).not.toHaveProperty("refreshedAtIso");
  });
});

// ── Empty edges array ──────────────────────────────────────────────────────

describe("catalog_get_lineages empty result", () => {
  it("does not issue hydration calls when the lineage query returns no edges", async () => {
    const { client, calls } = recordingClient((document) => {
      if (document.includes("getLineages")) {
        return {
          getLineages: {
            totalCount: 0,
            nbPerPage: 20,
            page: 0,
            data: [],
          },
        };
      }
      throw new Error(`unexpected: ${document.slice(0, 60)}`);
    });
    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_lineages")!.handler;
    const result = await handler({ parentTableId: "t1", hydrate: true });
    // Only the lineages call — no follow-up table/dashboard hydration on empty edges
    expect(calls).toHaveLength(1);
    const parsed = parseHandlerResult(result) as {
      data: unknown[];
      pagination: { hasMore: boolean };
    };
    expect(parsed.data).toHaveLength(0);
    expect(parsed.pagination.hasMore).toBe(false);
  });
});
