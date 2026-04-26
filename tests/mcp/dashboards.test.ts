import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineDashboardTools } from "../../src/mcp/dashboards.js";
import {
  GET_DASHBOARDS_SUMMARY,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_DASHBOARD_DETAIL,
} from "../../src/catalog/operations.js";
import { CatalogGraphQLError } from "../../src/client.js";
import { makeMockClient } from "../helpers/mock-client.js";

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const client = makeMockClient(responder ?? (() => ({})));
  const tools = defineDashboardTools(client);
  return { client, tools };
}

function find(tools: ReturnType<typeof defineDashboardTools>, name: string) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }): unknown {
  return JSON.parse(r.content[0].text);
}

const dashboardFixture = {
  id: "d-1",
  name: "Revenue overview",
  externalId: "tableau::wb-42::view-7",
  externalSlug: "revenue-overview",
  description: "Daily revenue across all products",
  type: "WORKBOOK",
  url: "https://tableau.example/#/views/wb-42/view-7",
  folderPath: "root/finance/revenue",
  sourceId: "src-tableau",
  popularity: 87,
  isVerified: true,
  isDeprecated: false,
  deletedAt: null,
  deprecatedAt: null,
};

describe("catalog_search_dashboards projection", () => {
  const emptyPage = {
    getDashboards: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("defaults to the summary operation when projection is omitted", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({});
    expect(client.calls[0].document).toBe(GET_DASHBOARDS_SUMMARY);
  });

  it("uses summary operation when projection is 'summary'", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ projection: "summary" });
    expect(client.calls[0].document).toBe(GET_DASHBOARDS_SUMMARY);
  });

  it("uses detail-batch operation when projection is 'detailed'", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ projection: "detailed" });
    expect(client.calls[0].document).toBe(GET_DASHBOARDS_DETAIL_BATCH);
  });

  it("rejects invalid projection values at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_search_dashboards").config.inputSchema
    );
    expect(() => schema.parse({ projection: "full" })).toThrow();
  });
});

describe("catalog_search_dashboards scope filters", () => {
  const emptyPage = {
    getDashboards: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("omits scope entirely when no scope fields are provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({});
    const vars = client.calls[0].variables as { scope?: unknown };
    expect(vars.scope).toBeUndefined();
  });

  it("forwards nameContains into the scope", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ nameContains: "revenue" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { nameContains: "revenue" },
    });
  });

  it("forwards folderPath into the scope", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ folderPath: "root/finance" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { folderPath: "root/finance" },
    });
  });

  it("forwards sourceId into the scope", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ sourceId: "src-tableau" });
    expect(client.calls[0].variables).toMatchObject({
      scope: { sourceId: "src-tableau" },
    });
  });

  it("forwards ids into the scope", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ ids: ["d-1", "d-2"] });
    expect(client.calls[0].variables).toMatchObject({
      scope: { ids: ["d-1", "d-2"] },
    });
  });

  it("forwards withDeleted=true when explicitly set", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ withDeleted: true });
    expect(client.calls[0].variables).toMatchObject({
      scope: { withDeleted: true },
    });
  });

  it("combines multiple scope filters into a single scope object", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({
      nameContains: "revenue",
      sourceId: "src-tableau",
      withDeleted: false,
    });
    expect(client.calls[0].variables).toMatchObject({
      scope: {
        nameContains: "revenue",
        sourceId: "src-tableau",
        withDeleted: false,
      },
    });
  });
});

describe("catalog_search_dashboards sorting", () => {
  const emptyPage = {
    getDashboards: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
  };

  it("omits sorting when sortBy is not provided", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({});
    const vars = client.calls[0].variables as { sorting?: unknown };
    expect(vars.sorting).toBeUndefined();
  });

  it("forwards sortBy alone (no direction/nulls) as a single-entry sorting array", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({ sortBy: "name" });
    expect(client.calls[0].variables).toMatchObject({
      sorting: [{ sortingKey: "name" }],
    });
  });

  it("forwards sortBy + direction + nullsPriority", async () => {
    const { client, tools } = makeTools(() => emptyPage);
    const tool = find(tools, "catalog_search_dashboards");
    await tool.handler({
      sortBy: "popularity",
      sortDirection: "DESC",
      nullsPriority: "LAST",
    });
    expect(client.calls[0].variables).toMatchObject({
      sorting: [
        { sortingKey: "popularity", direction: "DESC", nullsPriority: "LAST" },
      ],
    });
  });

  it("rejects an unknown sortBy value at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_search_dashboards").config.inputSchema
    );
    expect(() => schema.parse({ sortBy: "createdAt" })).toThrow();
  });

  it("accepts all three documented sort keys", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_search_dashboards").config.inputSchema
    );
    for (const key of ["name", "popularity", "ownersAndTeamOwnersCount"]) {
      expect(() => schema.parse({ sortBy: key })).not.toThrow();
    }
  });
});

describe("catalog_search_dashboards pagination + envelope", () => {
  it("defaults pagination to nbPerPage=100, page=0 and forwards explicit values", async () => {
    const { client, tools } = makeTools(() => ({
      getDashboards: { page: 0, nbPerPage: 100, totalCount: 0, data: [] },
    }));
    const tool = find(tools, "catalog_search_dashboards");

    await tool.handler({});
    expect(client.calls[0].variables).toMatchObject({
      pagination: { nbPerPage: 100, page: 0 },
    });

    await tool.handler({ nbPerPage: 25, page: 3 });
    expect(client.calls[1].variables).toMatchObject({
      pagination: { nbPerPage: 25, page: 3 },
    });
  });

  it("rejects nbPerPage above the 500 ceiling at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_search_dashboards").config.inputSchema
    );
    expect(() => schema.parse({ nbPerPage: 501 })).toThrow();
  });

  it("returns the listEnvelope shape with hasMore derived from totalCount", async () => {
    const { tools } = makeTools(() => ({
      getDashboards: {
        page: 0,
        nbPerPage: 2,
        totalCount: 5,
        data: [dashboardFixture, { ...dashboardFixture, id: "d-2" }],
      },
    }));
    const tool = find(tools, "catalog_search_dashboards");
    const res = await tool.handler({ nbPerPage: 2, page: 0 });
    const parsed = parseResult(res) as {
      pagination: { page: number; nbPerPage: number; totalCount: number; hasMore: boolean };
      data: Array<{ id: string }>;
    };
    expect(parsed.pagination).toEqual({
      page: 0,
      nbPerPage: 2,
      totalCount: 5,
      hasMore: true,
    });
    expect(parsed.data.map((d) => d.id)).toEqual(["d-1", "d-2"]);
  });

  it("reports hasMore=false when page × nbPerPage + data.length >= totalCount", async () => {
    const { tools } = makeTools(() => ({
      getDashboards: {
        page: 2,
        nbPerPage: 2,
        totalCount: 5,
        data: [{ ...dashboardFixture, id: "d-5" }],
      },
    }));
    const tool = find(tools, "catalog_search_dashboards");
    const res = await tool.handler({ nbPerPage: 2, page: 2 });
    const parsed = parseResult(res) as {
      pagination: { hasMore: boolean };
    };
    expect(parsed.pagination.hasMore).toBe(false);
  });
});

describe("catalog_search_dashboards error handling", () => {
  it("surfaces GraphQL errors as isError tool results", async () => {
    const { tools } = makeTools(() => {
      throw new CatalogGraphQLError([{ message: "scope validation failed" }]);
    });
    const tool = find(tools, "catalog_search_dashboards");
    const res = await tool.handler({ nameContains: "x" });
    expect(res.isError).toBe(true);
    expect(parseResult(res)).toMatchObject({
      error: expect.stringMatching(/scope validation failed/),
      detail: { kind: "graphql_error" },
    });
  });
});

describe("catalog_get_dashboard handler", () => {
  it("calls GET_DASHBOARD_DETAIL with { ids: [id] }", async () => {
    const { client, tools } = makeTools(() => ({
      getDashboards: { data: [dashboardFixture] },
    }));
    const tool = find(tools, "catalog_get_dashboard");
    await tool.handler({ id: "d-1" });
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(GET_DASHBOARD_DETAIL);
    expect(client.calls[0].variables).toEqual({ ids: ["d-1"] });
  });

  it("returns { dashboard: row } on hit", async () => {
    const { tools } = makeTools(() => ({
      getDashboards: { data: [dashboardFixture] },
    }));
    const tool = find(tools, "catalog_get_dashboard");
    const res = await tool.handler({ id: "d-1" });
    const parsed = parseResult(res) as { dashboard: Record<string, unknown> };
    expect(parsed.dashboard).toMatchObject({
      id: "d-1",
      name: "Revenue overview",
      type: "WORKBOOK",
      folderPath: "root/finance/revenue",
      isVerified: true,
    });
  });

  it("returns { dashboard: null } on miss (empty data array)", async () => {
    const { tools } = makeTools(() => ({
      getDashboards: { data: [] },
    }));
    const tool = find(tools, "catalog_get_dashboard");
    const res = await tool.handler({ id: "d-missing" });
    expect(parseResult(res)).toEqual({ dashboard: null });
  });

  it("rejects missing or empty id at the schema layer", () => {
    const schema = z.object(
      find(makeTools().tools, "catalog_get_dashboard").config.inputSchema
    );
    expect(() => schema.parse({})).toThrow();
    expect(() => schema.parse({ id: "" })).toThrow();
  });

  it("surfaces GraphQL errors as isError tool results", async () => {
    const { tools } = makeTools(() => {
      throw new CatalogGraphQLError([{ message: "invalid uuid" }]);
    });
    const tool = find(tools, "catalog_get_dashboard");
    const res = await tool.handler({ id: "bad" });
    expect(res.isError).toBe(true);
    expect(parseResult(res)).toMatchObject({
      error: expect.stringMatching(/invalid uuid/),
    });
  });
});

describe("dashboard tool annotations", () => {
  it("both dashboard tools are marked read-only", () => {
    const tools = defineDashboardTools(
      makeMockClient(() => ({}))
    );
    for (const name of ["catalog_search_dashboards", "catalog_get_dashboard"]) {
      const def = tools.find((t) => t.name === name)!;
      expect(def.config.annotations?.readOnlyHint).toBe(true);
      expect(def.config.annotations?.idempotentHint).toBe(true);
      expect(def.config.annotations?.destructiveHint).toBe(false);
    }
  });
});
