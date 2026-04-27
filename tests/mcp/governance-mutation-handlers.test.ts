import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { defineGovernanceTools } from "../../src/mcp/governance.js";
import {
  CREATE_EXTERNAL_LINKS,
  UPDATE_EXTERNAL_LINKS,
  DELETE_EXTERNAL_LINKS,
  UPSERT_DATA_QUALITIES,
  REMOVE_DATA_QUALITIES,
  UPSERT_USER_OWNERS,
  REMOVE_USER_OWNERS,
  UPSERT_TEAM_OWNERS,
  REMOVE_TEAM_OWNERS,
  UPSERT_TEAM,
  ADD_TEAM_USERS,
  REMOVE_TEAM_USERS,
  UPSERT_PINNED_ASSETS,
  REMOVE_PINNED_ASSETS,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";
import type { ToolHandlerExtra } from "../../src/catalog/types.js";
import { SKIP_CONFIRMATIONS_ENV_VAR } from "../../src/constants.js";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function makeTools(responder?: Parameters<typeof makeMockClient>[0]) {
  const client = makeMockClient(responder ?? (() => ({})));
  const tools = defineGovernanceTools(client);
  return { client, tools };
}

function find(
  tools: ReturnType<typeof defineGovernanceTools>,
  name: string
) {
  const match = tools.find((t) => t.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

function parseResult(r: { content: { text: string }[] }) {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

// Bypasses the elicitation prompt for destructive tools — tests that exercise
// the elicitation behaviour itself live in confirmation.test.ts. Here we
// only care that the underlying handler wires the right operation/variables
// and shapes the response correctly.
let savedSkip: string | undefined;
beforeEach(() => {
  savedSkip = process.env[SKIP_CONFIRMATIONS_ENV_VAR];
  process.env[SKIP_CONFIRMATIONS_ENV_VAR] = "true";
});
afterEach(() => {
  if (savedSkip === undefined) delete process.env[SKIP_CONFIRMATIONS_ENV_VAR];
  else process.env[SKIP_CONFIRMATIONS_ENV_VAR] = savedSkip;
});

const NO_EXTRA: ToolHandlerExtra | undefined = undefined;

// ---------------------------------------------------------------------------
// External links — create / update / delete
// ---------------------------------------------------------------------------

describe("catalog_create_external_links handler", () => {
  it("calls CREATE_EXTERNAL_LINKS with the data array", async () => {
    const returned = [
      { id: "el-1", tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" },
    ];
    const { client, tools } = makeTools(() => ({
      createExternalLinks: returned,
    }));
    const tool = find(tools, "catalog_create_external_links");
    await tool.handler({
      data: [{ tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" }],
    });
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(CREATE_EXTERNAL_LINKS);
    const vars = client.calls[0].variables as { data: unknown };
    expect(vars.data).toEqual([
      { tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" },
    ]);
  });

  it("returns batchResult shape with created count + data", async () => {
    const returned = [
      { id: "el-1", tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" },
      { id: "el-2", tableId: "t-2", technology: "AIRFLOW", url: "https://example.com/b" },
    ];
    const { tools } = makeTools(() => ({ createExternalLinks: returned }));
    const tool = find(tools, "catalog_create_external_links");
    const res = await tool.handler({
      data: [
        { tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" },
        { tableId: "t-2", technology: "AIRFLOW", url: "https://example.com/b" },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.created).toBe(2);
    expect(parsed.data).toEqual(returned);
    expect(parsed.partialFailure).toBeUndefined();
  });

  it("flags partialFailure when fewer rows return than were submitted", async () => {
    const returned = [
      { id: "el-1", tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" },
    ];
    const { tools } = makeTools(() => ({ createExternalLinks: returned }));
    const tool = find(tools, "catalog_create_external_links");
    const res = await tool.handler({
      data: [
        { tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" },
        { tableId: "t-2", technology: "AIRFLOW", url: "https://example.com/b" },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.partialFailure).toBe(true);
    expect(parsed.expectedCount).toBe(2);
    expect(parsed.created).toBe(1);
  });

  it("surfaces transport errors as isError", async () => {
    const { tools } = makeTools(() => {
      throw new Error("network failure");
    });
    const tool = find(tools, "catalog_create_external_links");
    const res = await tool.handler({
      data: [{ tableId: "t-1", technology: "OTHER", url: "https://example.com/a" }],
    });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/network failure/);
  });
});

describe("catalog_update_external_links handler", () => {
  it("calls UPDATE_EXTERNAL_LINKS with the data array", async () => {
    const returned = [
      { id: "el-1", tableId: "t-1", technology: "GITHUB", url: "https://example.com/new" },
    ];
    const { client, tools } = makeTools(() => ({
      updateExternalLinks: returned,
    }));
    const tool = find(tools, "catalog_update_external_links");
    await tool.handler({
      data: [{ id: "el-1", url: "https://example.com/new" }],
    });
    expect(client.calls[0].document).toBe(UPDATE_EXTERNAL_LINKS);
    const vars = client.calls[0].variables as { data: unknown };
    expect(vars.data).toEqual([{ id: "el-1", url: "https://example.com/new" }]);
  });

  it("flags partialFailure when fewer rows return than were submitted", async () => {
    const { tools } = makeTools(() => ({
      updateExternalLinks: [
        { id: "el-1", tableId: "t-1", technology: "GITHUB", url: "https://example.com/x" },
      ],
    }));
    const tool = find(tools, "catalog_update_external_links");
    const res = await tool.handler({
      data: [
        { id: "el-1", url: "https://example.com/x" },
        { id: "el-2", url: "https://example.com/y" },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.partialFailure).toBe(true);
    expect(parsed.expectedCount).toBe(2);
    expect(parsed.updated).toBe(1);
  });
});

describe("catalog_delete_external_links handler", () => {
  it("calls DELETE_EXTERNAL_LINKS and returns success + requestedCount", async () => {
    const { client, tools } = makeTools(() => ({ deleteExternalLinks: true }));
    const tool = find(tools, "catalog_delete_external_links");
    const res = await tool.handler(
      { data: [{ id: "el-1" }, { id: "el-2" }] },
      NO_EXTRA
    );
    expect(client.calls[0].document).toBe(DELETE_EXTERNAL_LINKS);
    const parsed = parseResult(res);
    expect(parsed.success).toBe(true);
    expect(parsed.requestedCount).toBe(2);
  });

  it("forwards a false success flag when the API rejects the batch", async () => {
    const { tools } = makeTools(() => ({ deleteExternalLinks: false }));
    const tool = find(tools, "catalog_delete_external_links");
    const res = await tool.handler({ data: [{ id: "el-1" }] }, NO_EXTRA);
    const parsed = parseResult(res);
    expect(parsed.success).toBe(false);
    expect(parsed.requestedCount).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Quality checks — upsert / remove
// ---------------------------------------------------------------------------

describe("catalog_upsert_data_qualities handler", () => {
  it("calls UPSERT_DATA_QUALITIES with the nested input shape (single tableId, array of checks)", async () => {
    const returned = [
      {
        id: "q-1",
        externalId: "ext-1",
        name: "row count",
        status: "SUCCESS",
        runAt: "2026-04-25T00:00:00Z",
      },
    ];
    const { client, tools } = makeTools(() => ({
      upsertDataQualities: returned,
    }));
    const tool = find(tools, "catalog_upsert_data_qualities");
    await tool.handler({
      tableId: "t-1",
      qualityChecks: [
        {
          externalId: "ext-1",
          name: "row count",
          status: "SUCCESS",
          runAt: "2026-04-25T00:00:00Z",
        },
      ],
    });
    expect(client.calls[0].document).toBe(UPSERT_DATA_QUALITIES);
    const vars = client.calls[0].variables as { data: { tableId: string; qualityChecks: unknown[] } };
    expect(vars.data.tableId).toBe("t-1");
    expect(vars.data.qualityChecks).toHaveLength(1);
  });

  it("returns batchResult with upserted count keyed off qualityChecks input length", async () => {
    const returned = [
      { id: "q-1", externalId: "ext-1", name: "n1", status: "SUCCESS", runAt: "2026-04-25T00:00:00Z" },
      { id: "q-2", externalId: "ext-2", name: "n2", status: "ALERT",   runAt: "2026-04-25T00:00:00Z" },
    ];
    const { tools } = makeTools(() => ({ upsertDataQualities: returned }));
    const tool = find(tools, "catalog_upsert_data_qualities");
    const res = await tool.handler({
      tableId: "t-1",
      qualityChecks: [
        { externalId: "ext-1", name: "n1", status: "SUCCESS", runAt: "2026-04-25T00:00:00Z" },
        { externalId: "ext-2", name: "n2", status: "ALERT",   runAt: "2026-04-25T00:00:00Z" },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.upserted).toBe(2);
    expect(parsed.partialFailure).toBeUndefined();
  });
});

describe("catalog_remove_data_qualities handler", () => {
  it("calls REMOVE_DATA_QUALITIES with the composite-key array", async () => {
    const { client, tools } = makeTools(() => ({ removeDataQualities: true }));
    const tool = find(tools, "catalog_remove_data_qualities");
    const res = await tool.handler(
      {
        qualityChecks: [
          { tableId: "t-1", externalId: "ext-1" },
          { tableId: "t-2", externalId: "ext-2" },
        ],
      },
      NO_EXTRA
    );
    expect(client.calls[0].document).toBe(REMOVE_DATA_QUALITIES);
    const vars = client.calls[0].variables as { data: { qualityChecks: unknown[] } };
    expect(vars.data.qualityChecks).toHaveLength(2);
    const parsed = parseResult(res);
    expect(parsed.success).toBe(true);
    expect(parsed.requestedCount).toBe(2);
  });
});

// ---------------------------------------------------------------------------
// Ownership writes — users
// ---------------------------------------------------------------------------

describe("catalog_upsert_user_owners handler", () => {
  it("calls UPSERT_USER_OWNERS with userId + targetEntities", async () => {
    const returned = [
      { userId: "u-1", entityType: "TABLE", entityId: "t-1" },
    ];
    const { client, tools } = makeTools(() => ({
      upsertUserOwners: returned,
    }));
    const tool = find(tools, "catalog_upsert_user_owners");
    await tool.handler({
      userId: "u-1",
      targetEntities: [{ entityType: "TABLE", entityId: "t-1" }],
    });
    expect(client.calls[0].document).toBe(UPSERT_USER_OWNERS);
    const vars = client.calls[0].variables as { data: { userId: string; targetEntities: unknown[] } };
    expect(vars.data.userId).toBe("u-1");
    expect(vars.data.targetEntities).toHaveLength(1);
  });

  it("flags partialFailure when fewer ownership rows are returned than targets submitted", async () => {
    const { tools } = makeTools(() => ({
      upsertUserOwners: [{ userId: "u-1", entityType: "TABLE", entityId: "t-1" }],
    }));
    const tool = find(tools, "catalog_upsert_user_owners");
    const res = await tool.handler({
      userId: "u-1",
      targetEntities: [
        { entityType: "TABLE", entityId: "t-1" },
        { entityType: "DASHBOARD", entityId: "d-1" },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.partialFailure).toBe(true);
    expect(parsed.expectedCount).toBe(2);
    expect(parsed.upserted).toBe(1);
  });
});

describe("catalog_remove_user_owners handler", () => {
  it("forwards targetEntities when supplied (scoped removal)", async () => {
    const { client, tools } = makeTools(() => ({ removeUserOwners: true }));
    const tool = find(tools, "catalog_remove_user_owners");
    const res = await tool.handler(
      {
        userId: "u-1",
        targetEntities: [{ entityType: "TABLE", entityId: "t-1" }],
      },
      NO_EXTRA
    );
    expect(client.calls[0].document).toBe(REMOVE_USER_OWNERS);
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({
      userId: "u-1",
      targetEntities: [{ entityType: "TABLE", entityId: "t-1" }],
    });
    const parsed = parseResult(res);
    expect(parsed.success).toBe(true);
    expect(parsed.userId).toBe("u-1");
  });

  it("omits targetEntities when not supplied (strip-from-all semantics)", async () => {
    // The summarize() in withConfirmation uses absence of targetEntities to
    // produce "Strip user X from ALL owned assets." — handler must mirror
    // that by leaving targetEntities out of the GraphQL variables entirely.
    const { client, tools } = makeTools(() => ({ removeUserOwners: true }));
    const tool = find(tools, "catalog_remove_user_owners");
    await tool.handler({ userId: "u-1" }, NO_EXTRA);
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({ userId: "u-1" });
    expect("targetEntities" in vars.data).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Ownership writes — teams
// ---------------------------------------------------------------------------

describe("catalog_upsert_team_owners handler", () => {
  it("calls UPSERT_TEAM_OWNERS with teamId + targetEntities", async () => {
    const returned = [
      { teamId: "team-1", entityType: "TABLE", entityId: "t-1" },
    ];
    const { client, tools } = makeTools(() => ({
      upsertTeamOwners: returned,
    }));
    const tool = find(tools, "catalog_upsert_team_owners");
    const res = await tool.handler({
      teamId: "team-1",
      targetEntities: [{ entityType: "TABLE", entityId: "t-1" }],
    });
    expect(client.calls[0].document).toBe(UPSERT_TEAM_OWNERS);
    const parsed = parseResult(res);
    expect(parsed.upserted).toBe(1);
  });
});

describe("catalog_remove_team_owners handler", () => {
  it("omits targetEntities when not supplied (strip-from-all semantics)", async () => {
    const { client, tools } = makeTools(() => ({ removeTeamOwners: true }));
    const tool = find(tools, "catalog_remove_team_owners");
    await tool.handler({ teamId: "team-1" }, NO_EXTRA);
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({ teamId: "team-1" });
    expect("targetEntities" in vars.data).toBe(false);
  });

  it("forwards targetEntities when supplied (scoped removal)", async () => {
    const { client, tools } = makeTools(() => ({ removeTeamOwners: true }));
    const tool = find(tools, "catalog_remove_team_owners");
    const res = await tool.handler(
      {
        teamId: "team-1",
        targetEntities: [{ entityType: "DASHBOARD", entityId: "d-1" }],
      },
      NO_EXTRA
    );
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({
      teamId: "team-1",
      targetEntities: [{ entityType: "DASHBOARD", entityId: "d-1" }],
    });
    const parsed = parseResult(res);
    expect(parsed.teamId).toBe("team-1");
  });
});

// ---------------------------------------------------------------------------
// Team CRUD
// ---------------------------------------------------------------------------

describe("catalog_upsert_team handler", () => {
  it("calls UPSERT_TEAM with only the populated fields", async () => {
    const returned = { id: "team-1", name: "Data Eng" };
    const { client, tools } = makeTools(() => ({ upsertTeam: returned }));
    const tool = find(tools, "catalog_upsert_team");
    const res = await tool.handler({
      name: "Data Eng",
      slackChannel: "#data-eng",
    });
    expect(client.calls[0].document).toBe(UPSERT_TEAM);
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({ name: "Data Eng", slackChannel: "#data-eng" });
    expect("description" in vars.data).toBe(false);
    expect("email" in vars.data).toBe(false);
    expect("slackGroup" in vars.data).toBe(false);
    const parsed = parseResult(res);
    expect(parsed.team).toEqual(returned);
  });

  it("forwards description / email / slackGroup when supplied", async () => {
    const { client, tools } = makeTools(() => ({
      upsertTeam: { id: "team-1", name: "Data Eng" },
    }));
    const tool = find(tools, "catalog_upsert_team");
    await tool.handler({
      name: "Data Eng",
      description: "Owns the warehouse",
      email: "data-eng@example.com",
      slackGroup: "@data-eng-team",
    });
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({
      name: "Data Eng",
      description: "Owns the warehouse",
      email: "data-eng@example.com",
      slackGroup: "@data-eng-team",
    });
  });
});

describe("catalog_add_team_users handler", () => {
  it("calls ADD_TEAM_USERS with id + emails", async () => {
    const { client, tools } = makeTools(() => ({ addTeamUsers: true }));
    const tool = find(tools, "catalog_add_team_users");
    const res = await tool.handler({
      id: "team-1",
      emails: ["a@example.com", "b@example.com"],
    });
    expect(client.calls[0].document).toBe(ADD_TEAM_USERS);
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({
      id: "team-1",
      emails: ["a@example.com", "b@example.com"],
    });
    const parsed = parseResult(res);
    expect(parsed.success).toBe(true);
    expect(parsed.requestedCount).toBe(2);
  });
});

describe("catalog_remove_team_users handler", () => {
  it("calls REMOVE_TEAM_USERS with id + emails", async () => {
    const { client, tools } = makeTools(() => ({ removeTeamUsers: true }));
    const tool = find(tools, "catalog_remove_team_users");
    const res = await tool.handler(
      {
        id: "team-1",
        emails: ["a@example.com"],
      },
      NO_EXTRA
    );
    expect(client.calls[0].document).toBe(REMOVE_TEAM_USERS);
    const vars = client.calls[0].variables as { data: Record<string, unknown> };
    expect(vars.data).toEqual({ id: "team-1", emails: ["a@example.com"] });
    const parsed = parseResult(res);
    expect(parsed.success).toBe(true);
    expect(parsed.requestedCount).toBe(1);
  });

  it("forwards a false success flag from the API", async () => {
    const { tools } = makeTools(() => ({ removeTeamUsers: false }));
    const tool = find(tools, "catalog_remove_team_users");
    const res = await tool.handler(
      { id: "team-1", emails: ["a@example.com"] },
      NO_EXTRA
    );
    const parsed = parseResult(res);
    expect(parsed.success).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Pinned assets — upsert / remove
// ---------------------------------------------------------------------------

describe("catalog_upsert_pinned_assets handler", () => {
  it("calls UPSERT_PINNED_ASSETS with the data array", async () => {
    const returned = [
      {
        id: "p-1",
        from: { id: "t-1", type: "TABLE" },
        to: { id: "d-1", type: "DASHBOARD" },
      },
    ];
    const { client, tools } = makeTools(() => ({
      upsertPinnedAssets: returned,
    }));
    const tool = find(tools, "catalog_upsert_pinned_assets");
    const res = await tool.handler({
      data: [
        {
          from: { id: "t-1", type: "TABLE" },
          to: { id: "d-1", type: "DASHBOARD" },
        },
      ],
    });
    expect(client.calls[0].document).toBe(UPSERT_PINNED_ASSETS);
    const parsed = parseResult(res);
    expect(parsed.upserted).toBe(1);
    expect(parsed.partialFailure).toBeUndefined();
  });

  it("flags partialFailure when fewer rows return than were submitted", async () => {
    const { tools } = makeTools(() => ({
      upsertPinnedAssets: [
        {
          id: "p-1",
          from: { id: "t-1", type: "TABLE" },
          to: { id: "d-1", type: "DASHBOARD" },
        },
      ],
    }));
    const tool = find(tools, "catalog_upsert_pinned_assets");
    const res = await tool.handler({
      data: [
        { from: { id: "t-1", type: "TABLE" }, to: { id: "d-1", type: "DASHBOARD" } },
        { from: { id: "t-2", type: "TABLE" }, to: { id: "d-2", type: "DASHBOARD" } },
      ],
    });
    const parsed = parseResult(res);
    expect(parsed.partialFailure).toBe(true);
    expect(parsed.expectedCount).toBe(2);
    expect(parsed.upserted).toBe(1);
  });
});

describe("catalog_remove_pinned_assets handler", () => {
  it("calls REMOVE_PINNED_ASSETS and returns success + requestedCount", async () => {
    const { client, tools } = makeTools(() => ({ removePinnedAssets: true }));
    const tool = find(tools, "catalog_remove_pinned_assets");
    const res = await tool.handler(
      {
        data: [
          { from: { id: "t-1", type: "TABLE" }, to: { id: "d-1", type: "DASHBOARD" } },
        ],
      },
      NO_EXTRA
    );
    expect(client.calls[0].document).toBe(REMOVE_PINNED_ASSETS);
    const parsed = parseResult(res);
    expect(parsed.success).toBe(true);
    expect(parsed.requestedCount).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Cross-cutting — error propagation through withErrorHandling
// ---------------------------------------------------------------------------

describe("governance mutations — error propagation", () => {
  // Every governance mutation handler must route thrown errors through
  // withErrorHandling instead of letting them propagate as protocol failures.
  // Each entry is `[toolName, args]` — args use minimal valid shapes so the
  // throw originates at c.execute (the production failure site), not at
  // shallower arg-coercion paths.
  const cases: Array<[string, Record<string, unknown>]> = [
    [
      "catalog_create_external_links",
      { data: [{ tableId: "t-1", technology: "GITHUB", url: "https://example.com/a" }] },
    ],
    [
      "catalog_update_external_links",
      { data: [{ id: "el-1", url: "https://example.com/b" }] },
    ],
    [
      "catalog_delete_external_links",
      { data: [{ id: "el-1" }] },
    ],
    [
      "catalog_upsert_data_qualities",
      {
        tableId: "t-1",
        qualityChecks: [
          {
            externalId: "ext-1",
            name: "row count",
            status: "SUCCESS",
            runAt: "2026-04-27T00:00:00Z",
          },
        ],
      },
    ],
    [
      "catalog_remove_data_qualities",
      { data: [{ id: "qc-1" }] },
    ],
    [
      "catalog_upsert_user_owners",
      { userId: "u-1", targetEntities: [{ entityType: "TABLE", entityId: "t-1" }] },
    ],
    [
      "catalog_remove_user_owners",
      { userId: "u-1" },
    ],
    [
      "catalog_upsert_team_owners",
      { teamId: "team-1", targetEntities: [{ entityType: "TABLE", entityId: "t-1" }] },
    ],
    [
      "catalog_remove_team_owners",
      { teamId: "team-1" },
    ],
    [
      "catalog_upsert_team",
      { name: "Data Eng" },
    ],
    [
      "catalog_add_team_users",
      { id: "team-1", emails: ["a@example.com"] },
    ],
    [
      "catalog_remove_team_users",
      { id: "team-1", emails: ["a@example.com"] },
    ],
    [
      "catalog_upsert_pinned_assets",
      { data: [{ entityType: "TABLE", entityId: "t-1" }] },
    ],
    [
      "catalog_remove_pinned_assets",
      { data: [{ id: "pa-1" }] },
    ],
  ];

  it.each(cases)("%s surfaces transport errors as isError", async (name, args) => {
    const { tools } = makeTools(() => {
      throw new Error(`boom ${name}`);
    });
    const tool = find(tools, name);
    const res = await tool.handler(args, NO_EXTRA);
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(new RegExp(`boom ${name}`));
  });
});
