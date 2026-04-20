import { describe, it, expect } from "vitest";
import { z } from "zod";
import { defineDashboardTools } from "../../src/mcp/dashboards.js";
import {
  GET_DASHBOARDS_SUMMARY,
  GET_DASHBOARDS_DETAIL_BATCH,
} from "../../src/catalog/operations.js";
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
