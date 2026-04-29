import { describe, it, expect } from "vitest";
import type { CatalogClient } from "../../src/client.js";
import { defineLineageTools } from "../../src/mcp/lineage.js";

/**
 * Field-level mirror of lineage-hydration-batching.test.ts. The asset variant
 * (catalog_get_lineages) and the field variant (catalog_get_field_lineages)
 * share the same multi-batch fetch loop, direction inference, and ISO
 * timestamp enrichment — but each runs its own underlying query and its own
 * hydrationMap shape, so the asset-side tests don't catch a regression in
 * hydrateFieldLineages / inferFieldDirection / enrichFieldEdge. This file
 * pins those code paths.
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
// HYDRATION_BATCH_SIZE = 500. Field lineage edges that link column→column
// contribute 2 distinct column IDs each, so a 500-row page can reference
// up to 1000 IDs. hydrateFieldLineages must split the column-id list into
// 500-sized batches the same way hydrateAssetLineages does for tables.

describe("catalog_get_field_lineages multi-batch hydration", () => {
  it("splits >500 distinct column IDs into multiple getColumns hydration calls and resolves every name", async () => {
    // 600 distinct edges × (parent + child column) = 1200 distinct column IDs
    // → 3 hydration batches of [500, 500, 200].
    const edges: Array<Record<string, unknown>> = [];
    for (let i = 0; i < 600; i++) {
      edges.push({
        id: `edge-${i}`,
        parentColumnId: `parent-col-${i}`,
        parentDashboardFieldId: null,
        childColumnId: `child-col-${i}`,
        childDashboardFieldId: null,
        childDashboardId: null,
        createdAt: 1700000000000,
        refreshedAt: 1700000000001,
      });
    }

    const { client, calls } = recordingClient((document, variables) => {
      if (document.includes("getFieldLineages")) {
        return {
          getFieldLineages: {
            totalCount: 600,
            nbPerPage: 600,
            page: 0,
            data: edges,
          },
        };
      }
      if (document.includes("CatalogGetColumnsSummary")) {
        const ids =
          (variables?.scope as { ids?: string[] } | undefined)?.ids ?? [];
        return {
          getColumns: {
            totalCount: ids.length,
            nbPerPage: ids.length,
            page: 0,
            data: ids.map((id) => ({
              id,
              name: `name-${id}`,
              tableId: `table-of-${id}`,
            })),
          },
        };
      }
      throw new Error(`unexpected query: ${document.slice(0, 80)}`);
    });

    const tools = defineLineageTools(client);
    const handler = tools.find((t) => t.name === "catalog_get_field_lineages")!.handler;
    const result = await handler({
      parentColumnId: "any-parent",
      hydrate: true,
      nbPerPage: 600,
    });

    const columnHydrationCalls = calls.filter((c) =>
      c.document.includes("CatalogGetColumnsSummary")
    );
    expect(columnHydrationCalls).toHaveLength(3);
    const batchSizes = columnHydrationCalls
      .map(
        (c) =>
          ((c.variables?.scope as { ids?: string[] } | undefined)?.ids ?? [])
            .length
      )
      .sort((a, b) => b - a);
    expect(batchSizes).toEqual([500, 500, 200]);

    const parsed = parseHandlerResult(result) as {
      data: Array<{
        parent: { id: string; name?: string; kind: string; parentId?: string };
        child: { id: string; name?: string; kind: string; parentId?: string };
      }>;
    };
    expect(parsed.data).toHaveLength(600);
    for (const edge of parsed.data) {
      expect(edge.parent.kind).toBe("COLUMN");
      expect(edge.parent.name).toBe(`name-${edge.parent.id}`);
      expect(edge.parent.parentId).toBe(`table-of-${edge.parent.id}`);
      expect(edge.child.kind).toBe("COLUMN");
      expect(edge.child.name).toBe(`name-${edge.child.id}`);
      expect(edge.child.parentId).toBe(`table-of-${edge.child.id}`);
    }
  });

  it("fans column-id batches and dashboard-id batches in parallel without cross-contaminating, and stamps DASHBOARD_FIELD endpoints with hydrationUnavailable", async () => {
    // 100 edges: column parent → dashboard-field child + dashboard child.
    // Exercises every branch in hydrateFieldLineages: column hydration,
    // dashboard hydration, and dashboard-field placeholder population.
    const edges: Array<Record<string, unknown>> = [];
    for (let i = 0; i < 100; i++) {
      edges.push({
        id: `edge-${i}`,
        parentColumnId: `c-${i}`,
        parentDashboardFieldId: null,
        childColumnId: null,
        childDashboardFieldId: `df-${i}`,
        childDashboardId: `d-${i}`,
        createdAt: null,
        refreshedAt: null,
      });
    }
    const { client, calls } = recordingClient((document, variables) => {
      if (document.includes("getFieldLineages")) {
        return {
          getFieldLineages: {
            totalCount: 100,
            nbPerPage: 100,
            page: 0,
            data: edges,
          },
        };
      }
      if (document.includes("CatalogGetColumnsSummary")) {
        const ids =
          (variables?.scope as { ids?: string[] } | undefined)?.ids ?? [];
        return {
          getColumns: {
            totalCount: ids.length,
            nbPerPage: ids.length,
            page: 0,
            data: ids.map((id) => ({
              id,
              name: `C:${id}`,
              tableId: `T:${id}`,
            })),
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
    const handler = tools.find((t) => t.name === "catalog_get_field_lineages")!.handler;
    const result = await handler({
      parentColumnId: "any-parent",
      hydrate: true,
      nbPerPage: 100,
    });

    expect(
      calls.filter((c) => c.document.includes("CatalogGetColumnsSummary"))
    ).toHaveLength(1);
    expect(
      calls.filter((c) => c.document.includes("CatalogGetDashboardsSummary"))
    ).toHaveLength(1);

    const parsed = parseHandlerResult(result) as {
      data: Array<{
        parent: {
          id: string;
          name?: string;
          kind: string;
          parentId?: string;
        };
        child: {
          id: string;
          name?: string;
          kind: string;
          hydrationUnavailable?: boolean;
        };
      }>;
    };
    expect(parsed.data).toHaveLength(100);
    for (const edge of parsed.data) {
      expect(edge.parent.kind).toBe("COLUMN");
      expect(edge.parent.name).toBe(`C:${edge.parent.id}`);
      expect(edge.parent.parentId).toBe(`T:${edge.parent.id}`);
      // The childId resolves to the dashboard-field id first per
      // enrichFieldEdge precedence (childColumnId ?? childDashboardFieldId
      // ?? childDashboardId). Dashboard-fields have no public hydration
      // endpoint, so the placeholder is stamped instead.
      expect(edge.child.kind).toBe("DASHBOARD_FIELD");
      expect(edge.child.hydrationUnavailable).toBe(true);
      expect(edge.child.name).toBeUndefined();
    }
  });
});

// ── hydrate=false skips all hydration calls ─────────────────────────────────

describe("catalog_get_field_lineages hydrate=false", () => {
  it("does not issue any hydration queries when hydrate is false", async () => {
    const { client, calls } = recordingClient((document) => {
      if (document.includes("getFieldLineages")) {
        return {
          getFieldLineages: {
            totalCount: 1,
            nbPerPage: 20,
            page: 0,
            data: [
              {
                id: "edge-1",
                parentColumnId: "c1",
                parentDashboardFieldId: null,
                childColumnId: "c2",
                childDashboardFieldId: null,
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
    const handler = tools.find((t) => t.name === "catalog_get_field_lineages")!.handler;
    const result = await handler({ parentColumnId: "c1" });

    expect(calls).toHaveLength(1);
    const parsed = parseHandlerResult(result) as {
      data: Array<Record<string, unknown>>;
    };
    expect(parsed.data).toHaveLength(1);
    expect(parsed.data[0]).not.toHaveProperty("parent");
    expect(parsed.data[0]).not.toHaveProperty("child");
  });

  it("omits hydration queries when hydrate is unset (default behavior)", async () => {
    const { client, calls } = recordingClient((document) => {
      if (document.includes("getFieldLineages")) {
        return {
          getFieldLineages: {
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
    const handler = tools.find((t) => t.name === "catalog_get_field_lineages")!.handler;
    await handler({ parentColumnId: "c-empty" });
    expect(calls).toHaveLength(1);
  });
});

// ── Direction inference ────────────────────────────────────────────────────
//
// inferFieldDirection considers parent: parentColumnId | parentDashboardFieldId
// and child: childColumnId | childDashboardFieldId | childDashboardSourceId
// | childDashboardFieldSourceId. Mirrors the asset-side test set.

describe("catalog_get_field_lineages direction inference", () => {
  function singleEdgeClient(): CatalogClient {
    return {
      endpoint: "https://mock.invalid/graphql",
      region: "eu",
      async execute<TData>(document: string): Promise<TData> {
        if (document.includes("getFieldLineages")) {
          return {
            getFieldLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "edge-1",
                  parentColumnId: "c1",
                  parentDashboardFieldId: null,
                  childColumnId: "c2",
                  childDashboardFieldId: null,
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
    const handler = tools.find((t) => t.name === "catalog_get_field_lineages")!.handler;
    const result = await handler(args);
    const parsed = parseHandlerResult(result) as {
      data: Array<{ direction?: string }>;
    };
    return parsed.data[0]?.direction;
  }

  it("stamps downstream when only a parent filter is set (parentColumnId)", async () => {
    expect(await run({ parentColumnId: "c1" })).toBe("downstream");
  });

  it("stamps downstream for parentDashboardFieldId only", async () => {
    expect(await run({ parentDashboardFieldId: "df1" })).toBe("downstream");
  });

  it("stamps upstream when only a child filter is set (childColumnId)", async () => {
    expect(await run({ childColumnId: "c1" })).toBe("upstream");
  });

  it("stamps upstream for childDashboardSourceId only", async () => {
    expect(await run({ childDashboardSourceId: "ds1" })).toBe("upstream");
  });

  it("stamps specific when parent and child are both set", async () => {
    expect(
      await run({ parentColumnId: "c1", childColumnId: "c2" })
    ).toBe("specific");
  });

  it("omits direction when neither parent nor child is set", async () => {
    expect(await run({ lineageType: "AUTOMATIC" })).toBeUndefined();
  });
});

// ── ISO timestamp enrichment ───────────────────────────────────────────────

describe("catalog_get_field_lineages ISO timestamp conversion", () => {
  function timestampClient(
    createdAt: unknown,
    refreshedAt: unknown
  ): CatalogClient {
    return {
      endpoint: "https://mock.invalid/graphql",
      region: "eu",
      async execute<TData>(document: string): Promise<TData> {
        if (document.includes("getFieldLineages")) {
          return {
            getFieldLineages: {
              totalCount: 1,
              nbPerPage: 20,
              page: 0,
              data: [
                {
                  id: "edge-1",
                  parentColumnId: "c1",
                  parentDashboardFieldId: null,
                  childColumnId: "c2",
                  childDashboardFieldId: null,
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
    const handler = tools.find((t) => t.name === "catalog_get_field_lineages")!.handler;
    const result = await handler({ parentColumnId: "c1" });
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

describe("catalog_get_field_lineages empty result", () => {
  it("does not issue hydration calls when the field-lineage query returns no edges", async () => {
    const { client, calls } = recordingClient((document) => {
      if (document.includes("getFieldLineages")) {
        return {
          getFieldLineages: {
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
    const handler = tools.find((t) => t.name === "catalog_get_field_lineages")!.handler;
    const result = await handler({ parentColumnId: "c1", hydrate: true });
    expect(calls).toHaveLength(1);
    const parsed = parseHandlerResult(result) as {
      data: unknown[];
      pagination: { hasMore: boolean };
    };
    expect(parsed.data).toHaveLength(0);
    expect(parsed.pagination.hasMore).toBe(false);
  });
});
