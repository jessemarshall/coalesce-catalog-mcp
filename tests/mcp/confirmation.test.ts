import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { defineLineageTools } from "../../src/mcp/lineage.js";
import { defineAnnotationTools } from "../../src/mcp/annotations.js";
import { defineGovernanceTools } from "../../src/mcp/governance.js";
import { makeMockClient, type MockClient } from "../helpers/mock-client.js";
import type { ToolHandlerExtra } from "../../src/catalog/types.js";
import { SKIP_CONFIRMATIONS_ENV_VAR } from "../../src/constants.js";

function findTool(tools: Array<{ name: string; handler: unknown }>, name: string) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match as {
    name: string;
    handler: (
      args: Record<string, unknown>,
      extra?: ToolHandlerExtra
    ) => Promise<{
      content: Array<{ type: string; text: string }>;
      isError?: boolean;
    }>;
  };
}

function parse(result: { content: Array<{ type: string; text: string }> }) {
  return JSON.parse(result.content[0].text);
}

interface ElicitCall {
  request: { method: string; params?: unknown };
}

function makeExtra(
  reply: { action: "accept" | "decline" | "cancel"; content?: Record<string, unknown> } | Error
): { extra: ToolHandlerExtra; calls: ElicitCall[] } {
  const calls: ElicitCall[] = [];
  const extra: ToolHandlerExtra = {
    async sendRequest(request) {
      calls.push({ request });
      if (reply instanceof Error) throw reply;
      return reply;
    },
  };
  return { extra, calls };
}

let savedSkip: string | undefined;
beforeEach(() => {
  savedSkip = process.env[SKIP_CONFIRMATIONS_ENV_VAR];
  delete process.env[SKIP_CONFIRMATIONS_ENV_VAR];
});
afterEach(() => {
  if (savedSkip === undefined) delete process.env[SKIP_CONFIRMATIONS_ENV_VAR];
  else process.env[SKIP_CONFIRMATIONS_ENV_VAR] = savedSkip;
});

describe("withConfirmation — happy path (accept)", () => {
  it("executes the destructive call and forwards the elicitation request", async () => {
    const client: MockClient = makeMockClient(() => ({ deleteLineages: true }));
    const tool = findTool(defineLineageTools(client), "catalog_delete_lineages");
    const { extra, calls } = makeExtra({
      action: "accept",
      content: { confirm: true },
    });

    const result = await tool.handler(
      { data: [{ parentTableId: "t-1", childTableId: "t-2" }] },
      extra
    );

    expect(result.isError).toBeFalsy();
    expect(parse(result)).toEqual({ success: true, requestedCount: 1 });
    expect(client.calls).toHaveLength(1);
    expect(calls).toHaveLength(1);
    expect(calls[0].request.method).toBe("elicitation/create");
    const params = calls[0].request.params as {
      message: string;
      requestedSchema: { properties: Record<string, unknown> };
    };
    expect(params.message).toContain("Delete lineage edges");
    expect(params.message).toContain("1 lineage edge");
    expect(params.requestedSchema.properties.confirm).toBeDefined();
  });
});

describe("withConfirmation — decline / cancel", () => {
  it("does not execute the destructive call when the user declines", async () => {
    const client: MockClient = makeMockClient(() => ({ deleteLineages: true }));
    const tool = findTool(defineLineageTools(client), "catalog_delete_lineages");
    const { extra } = makeExtra({ action: "decline" });

    const result = await tool.handler(
      { data: [{ parentTableId: "t-1", childTableId: "t-2" }] },
      extra
    );

    // Decline is user-expected behaviour, not a tool failure — no isError.
    expect(result.isError).toBeFalsy();
    expect(parse(result).error).toMatch(/did not confirm/i);
    expect(parse(result).detail).toMatchObject({ kind: "user_declined" });
    expect(client.calls).toHaveLength(0);
  });

  it("treats action=accept without confirm=true as decline", async () => {
    const client: MockClient = makeMockClient(() => ({ detachTags: true }));
    const tool = findTool(
      defineAnnotationTools(client),
      "catalog_detach_tags"
    );
    const { extra } = makeExtra({
      action: "accept",
      content: { confirm: false },
    });

    const result = await tool.handler(
      { data: [{ entityType: "TABLE", entityId: "t-1", label: "pii" }] },
      extra
    );

    expect(result.isError).toBeFalsy();
    expect(parse(result).detail).toMatchObject({ kind: "user_declined" });
    expect(client.calls).toHaveLength(0);
  });
});

describe("withConfirmation — fail closed", () => {
  it("returns a tool error when extra.sendRequest is missing", async () => {
    const client: MockClient = makeMockClient(() => ({ deleteLineages: true }));
    const tool = findTool(defineLineageTools(client), "catalog_delete_lineages");

    const result = await tool.handler({
      data: [{ parentTableId: "t-1", childTableId: "t-2" }],
    });

    expect(result.isError).toBe(true);
    expect(parse(result).error).toMatch(/requires interactive confirmation/i);
    expect(client.calls).toHaveLength(0);
  });

  it("returns a tool error when the elicitation request itself fails", async () => {
    const client: MockClient = makeMockClient(() => ({ deleteTerm: true }));
    const tool = findTool(defineAnnotationTools(client), "catalog_delete_term");
    const { extra } = makeExtra(new Error("Method not found"));

    const result = await tool.handler({ id: "term-1" }, extra);

    expect(result.isError).toBe(true);
    expect(parse(result).error).toMatch(/elicitation/i);
    expect(parse(result).detail).toMatchObject({
      kind: "elicitation_failed",
    });
    expect(client.calls).toHaveLength(0);
  });
});

describe("withConfirmation — env-var bypass", () => {
  it("skips the prompt entirely when COALESCE_CATALOG_SKIP_CONFIRMATIONS=true", async () => {
    process.env[SKIP_CONFIRMATIONS_ENV_VAR] = "true";
    const client: MockClient = makeMockClient(() => ({ removeUserOwners: true }));
    const tool = findTool(
      defineGovernanceTools(client),
      "catalog_remove_user_owners"
    );

    const result = await tool.handler({ userId: "u-1" });

    expect(result.isError).toBeFalsy();
    expect(parse(result)).toEqual({ success: true, userId: "u-1" });
    expect(client.calls).toHaveLength(1);
  });
});

describe("withConfirmation — coverage across all destructive tools", () => {
  const cases: Array<{
    define: (c: MockClient) => Array<{ name: string; handler: unknown }>;
    name: string;
    args: Record<string, unknown>;
    expectedSummary: RegExp;
    response: Record<string, unknown>;
  }> = [
    {
      define: (c) => defineLineageTools(c),
      name: "catalog_delete_lineages",
      args: { data: [{ parentTableId: "t-1", childTableId: "t-2" }] },
      expectedSummary: /1 lineage edge/i,
      response: { deleteLineages: true },
    },
    {
      define: (c) => defineAnnotationTools(c),
      name: "catalog_detach_tags",
      args: { data: [{ entityType: "TABLE", entityId: "t-1", label: "pii" }] },
      expectedSummary: /1 tag binding/i,
      response: { detachTags: true },
    },
    {
      define: (c) => defineAnnotationTools(c),
      name: "catalog_delete_term",
      args: { id: "term-1" },
      expectedSummary: /Permanently delete term term-1/i,
      response: { deleteTerm: true },
    },
    {
      define: (c) => defineGovernanceTools(c),
      name: "catalog_delete_external_links",
      args: { data: [{ id: "link-1" }, { id: "link-2" }] },
      expectedSummary: /2 external link/i,
      response: { deleteExternalLinks: true },
    },
    {
      define: (c) => defineGovernanceTools(c),
      name: "catalog_remove_data_qualities",
      args: { qualityChecks: [{ tableId: "t-1", externalId: "ext-1" }] },
      expectedSummary: /1 quality-check/i,
      response: { removeDataQualities: true },
    },
    {
      define: (c) => defineGovernanceTools(c),
      name: "catalog_remove_user_owners",
      args: { userId: "u-1" },
      expectedSummary: /ALL owned assets/i,
      response: { removeUserOwners: true },
    },
    {
      define: (c) => defineGovernanceTools(c),
      name: "catalog_remove_team_owners",
      args: {
        teamId: "team-1",
        targetEntities: [{ entityType: "TABLE", entityId: "t-1" }],
      },
      expectedSummary: /Strip team team-1 from 1 asset/i,
      response: { removeTeamOwners: true },
    },
    {
      define: (c) => defineGovernanceTools(c),
      name: "catalog_remove_team_users",
      args: { id: "team-1", emails: ["a@x.com", "b@x.com"] },
      expectedSummary: /Remove 2 user/i,
      response: { removeTeamUsers: true },
    },
    {
      define: (c) => defineGovernanceTools(c),
      name: "catalog_remove_pinned_assets",
      args: {
        data: [
          {
            from: { id: "a-1", type: "TABLE" },
            to: { id: "b-1", type: "DASHBOARD" },
          },
        ],
      },
      expectedSummary: /1 pinned-asset link/i,
      response: { removePinnedAssets: true },
    },
  ];

  for (const c of cases) {
    it(`${c.name} — sends elicitation with a meaningful summary`, async () => {
      const client: MockClient = makeMockClient(() => c.response);
      const tool = findTool(c.define(client), c.name);
      const { extra, calls } = makeExtra({
        action: "accept",
        content: { confirm: true },
      });

      const result = await tool.handler(c.args, extra);

      expect(result.isError, `${c.name} unexpectedly errored`).toBeFalsy();
      expect(client.calls).toHaveLength(1);
      expect(calls).toHaveLength(1);
      const params = calls[0].request.params as { message: string };
      expect(params.message).toMatch(c.expectedSummary);
    });

    it(`${c.name} — blocks the destructive call when no extra is supplied`, async () => {
      const client: MockClient = makeMockClient(() => c.response);
      const tool = findTool(c.define(client), c.name);

      const result = await tool.handler(c.args);

      expect(result.isError).toBe(true);
      expect(client.calls).toHaveLength(0);
    });
  }
});
