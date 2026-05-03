import { describe, it, expect } from "vitest";
import { defineLineageTools } from "../../src/mcp/lineage.js";
import {
  GET_LINEAGES,
  GET_FIELD_LINEAGES,
  UPSERT_LINEAGES,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const client = makeMockClient(responder ?? (() => ({})));
  const tools = defineLineageTools(client);
  return { client, tools };
}

function find(
  tools: ReturnType<typeof defineLineageTools>,
  name: string
) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }) {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

// ---------------------------------------------------------------------------
// catalog_get_lineages — handler
// ---------------------------------------------------------------------------

describe("catalog_get_lineages handler — operation + variable wiring", () => {
  const emptyPage = {
    getLineages: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("invokes GET_LINEAGES", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_lineages");
    await tool.handler({ parentTableId: "t-1" });
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_LINEAGES);
  });

  it("builds scope from parent + lineageType", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_lineages");
    await tool.handler({
      parentTableId: "t-1",
      lineageType: "MANUAL_CUSTOMER",
      withChildAssetType: "DASHBOARD",
    });
    const vars = client.calls[0].variables as { scope?: Record<string, unknown> };
    expect(vars.scope).toEqual({
      parentTableId: "t-1",
      lineageType: "MANUAL_CUSTOMER",
      withChildAssetType: "DASHBOARD",
    });
  });

  it("omits scope when no filter is provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_lineages");
    await tool.handler({});
    const vars = client.calls[0].variables as { scope?: unknown };
    expect(vars.scope).toBeUndefined();
  });

  it("builds sorting when sortBy is provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_lineages");
    await tool.handler({
      parentTableId: "t-1",
      sortBy: "popularity",
      sortDirection: "DESC",
      nullsPriority: "FIRST",
    });
    const vars = client.calls[0].variables as {
      sorting?: Array<Record<string, unknown>>;
    };
    expect(vars.sorting).toEqual([
      { sortingKey: "popularity", direction: "DESC", nullsPriority: "FIRST" },
    ]);
  });

  it("forwards pagination", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_lineages");
    await tool.handler({ parentTableId: "t-1", nbPerPage: 25, page: 3 });
    const vars = client.calls[0].variables as {
      pagination: Record<string, unknown>;
    };
    expect(vars.pagination).toMatchObject({ nbPerPage: 25, page: 3 });
  });
});

describe("catalog_get_lineages handler — output shaping", () => {
  it("annotates each row with `direction: downstream` when only parent scope is set", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ parentTableId: "t-1" });
    const parsed = parseResult(res);
    const data = parsed.data as Array<Record<string, unknown>>;
    expect(data).toHaveLength(1);
    expect(data[0].direction).toBe("downstream");
  });

  it("annotates `direction: upstream` when only child scope is set", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ childTableId: "t-2" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBe("upstream");
  });

  it("annotates `direction: specific` when both parent and child scope are set", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ parentTableId: "t-1", childTableId: "t-2" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBe("specific");
  });

  it("omits `direction` when no parent or child scope is set", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ lineageType: "AUTOMATIC" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBeUndefined();
  });

  it("annotates `direction: downstream` when only parentSourceId is set", async () => {
    // parentSourceId is a valid GetLineagesScope filter but was previously
    // ignored by the direction-inference logic — a caller scoping by source
    // alone got `direction: undefined` even though the field-lineage
    // equivalent (childDashboardSourceId etc.) already produced upstream/
    // downstream. Aligned to mirror that behaviour.
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ parentSourceId: "src-1" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBe("downstream");
  });

  it("annotates `direction: upstream` when only childSourceId is set", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ childSourceId: "src-2" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBe("upstream");
  });

  it("omits `direction` when only non-side scope filters are set (lineageIds, withChildAssetType)", async () => {
    // GetLineagesScope accepts filters that don't pin a side (lineageIds,
    // withChildAssetType, etc.). These narrow the result set without
    // implying upstream/downstream — emitting a `direction` for them
    // would be a contract bug. Pinning the negative case here so a future
    // "always emit a direction" change can't slip through.
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");

    const lineageIdsRes = await tool.handler({ lineageIds: ["edge-1"] });
    const lineageIdsData = parseResult(lineageIdsRes).data as Array<Record<string, unknown>>;
    expect(lineageIdsData[0].direction).toBeUndefined();

    const assetTypeRes = await tool.handler({ withChildAssetType: "DASHBOARD" });
    const assetTypeData = parseResult(assetTypeRes).data as Array<Record<string, unknown>>;
    expect(assetTypeData[0].direction).toBeUndefined();
  });

  it("annotates `direction: specific` when both parentSourceId and childSourceId are set", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({
      parentSourceId: "src-1",
      childSourceId: "src-2",
    });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBe("specific");
  });

  it("converts createdAt + refreshedAt epoch millis to ISO timestamps", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700001000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ parentTableId: "t-1" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].createdAtIso).toBe("2023-11-14T22:13:20.000Z");
    expect(data[0].refreshedAtIso).toBe("2023-11-14T22:30:00.000Z");
  });

  it("returns no createdAtIso when createdAt is missing or non-numeric", async () => {
    const { tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: null,
            refreshedAt: null,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ parentTableId: "t-1" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].createdAtIso).toBeUndefined();
    expect(data[0].refreshedAtIso).toBeUndefined();
  });

  it("does NOT inject parent / child fields when hydrate is false (default)", async () => {
    const { client, tools } = makeTools(() => ({
      getLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "edge-1",
            parentTableId: "t-1",
            childTableId: "t-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_lineages");
    const res = await tool.handler({ parentTableId: "t-1" });
    // Single GraphQL call (no hydration fan-out)
    expect(client.calls).toHaveLength(1);
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].parent).toBeUndefined();
    expect(data[0].child).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// catalog_get_field_lineages — handler
// ---------------------------------------------------------------------------

describe("catalog_get_field_lineages handler — operation + variable wiring", () => {
  const emptyPage = {
    getFieldLineages: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("invokes GET_FIELD_LINEAGES", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_field_lineages");
    await tool.handler({ parentColumnId: "c-1" });
    expect(client.calls[0].document).toBe(GET_FIELD_LINEAGES);
  });

  it("builds scope from parentColumnId + hasDashboardChild", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_get_field_lineages");
    await tool.handler({ parentColumnId: "c-1", hasDashboardChild: true });
    const vars = client.calls[0].variables as { scope?: Record<string, unknown> };
    expect(vars.scope).toEqual({
      parentColumnId: "c-1",
      hasDashboardChild: true,
    });
  });

  it("annotates `direction: downstream` for a parentColumnId-only query", async () => {
    const { tools } = makeTools(() => ({
      getFieldLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "f-edge-1",
            parentColumnId: "c-1",
            childColumnId: "c-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_field_lineages");
    const res = await tool.handler({ parentColumnId: "c-1" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBe("downstream");
  });

  it("annotates `direction: upstream` for a childColumnId-only query", async () => {
    const { tools } = makeTools(() => ({
      getFieldLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "f-edge-1",
            parentColumnId: "c-1",
            childColumnId: "c-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_field_lineages");
    const res = await tool.handler({ childColumnId: "c-2" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].direction).toBe("upstream");
  });

  it("converts millis timestamps to ISO on field-lineage rows", async () => {
    const { tools } = makeTools(() => ({
      getFieldLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "f-edge-1",
            parentColumnId: "c-1",
            childColumnId: "c-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_field_lineages");
    const res = await tool.handler({ parentColumnId: "c-1" });
    const data = parseResult(res).data as Array<Record<string, unknown>>;
    expect(data[0].createdAtIso).toBe("2023-11-14T22:13:20.000Z");
  });

  it("does not fan out when hydrate is false", async () => {
    const { client, tools } = makeTools(() => ({
      getFieldLineages: {
        page: 0,
        nbPerPage: 20,
        totalCount: 1,
        data: [
          {
            id: "f-edge-1",
            parentColumnId: "c-1",
            childColumnId: "c-2",
            createdAt: 1700000000000,
            refreshedAt: 1700000000000,
          },
        ],
      },
    }));
    const tool = find(tools, "catalog_get_field_lineages");
    await tool.handler({ parentColumnId: "c-1" });
    expect(client.calls).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// catalog_upsert_lineages — handler
// ---------------------------------------------------------------------------

describe("catalog_upsert_lineages handler", () => {
  it("calls UPSERT_LINEAGES with the data array", async () => {
    const returned = [
      { id: "edge-1", parentTableId: "t-1", childTableId: "t-2" },
    ];
    const { client, tools } = makeTools(() => ({
      upsertLineages: returned,
    }));
    const tool = find(tools, "catalog_upsert_lineages");
    await tool.handler({
      data: [{ parentTableId: "t-1", childTableId: "t-2" }],
    });
    expect(client.calls[0].document).toBe(UPSERT_LINEAGES);
    const vars = client.calls[0].variables as { data: unknown };
    expect(vars.data).toEqual([
      { parentTableId: "t-1", childTableId: "t-2" },
    ]);
  });

  it("returns batchResult shape with count + data", async () => {
    const returned = [
      { id: "edge-1", parentTableId: "t-1", childTableId: "t-2" },
      { id: "edge-2", parentTableId: "t-1", childDashboardId: "d-1" },
    ];
    const { tools } = makeTools(() => ({
      upsertLineages: returned,
    }));
    const tool = find(tools, "catalog_upsert_lineages");
    const res = await tool.handler({
      data: [
        { parentTableId: "t-1", childTableId: "t-2" },
        { parentTableId: "t-1", childDashboardId: "d-1" },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.upserted).toBe(2);
    expect(parsed.data).toEqual(returned);
    expect(parsed.partialFailure).toBeUndefined();
  });

  it("flags partialFailure when fewer rows return than were submitted", async () => {
    // The Catalog API silently drops rows it couldn't upsert; batchResult's
    // job is to surface this discrepancy so the agent can react.
    const returned = [
      { id: "edge-1", parentTableId: "t-1", childTableId: "t-2" },
    ];
    const { tools } = makeTools(() => ({
      upsertLineages: returned,
    }));
    const tool = find(tools, "catalog_upsert_lineages");
    const res = await tool.handler({
      data: [
        { parentTableId: "t-1", childTableId: "t-2" },
        { parentTableId: "t-1", childDashboardId: "d-1" },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.partialFailure).toBe(true);
    expect(parsed.expectedCount).toBe(2);
    expect(parsed.upserted).toBe(1);
  });

  it("surfaces transport errors as isError results without calling the API a second time", async () => {
    const { client, tools } = makeTools(() => {
      throw new Error("network failure");
    });
    const tool = find(tools, "catalog_upsert_lineages");
    const res = await tool.handler({
      data: [{ parentTableId: "t-1", childTableId: "t-2" }],
    });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/network failure/);
    expect(client.calls).toHaveLength(1);
  });
});
