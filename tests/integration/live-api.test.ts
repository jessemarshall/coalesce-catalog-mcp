/**
 * Live-API integration tests. Exercise the real Castor GraphQL endpoint end
 * to end through the built dist/index.js over stdio.
 *
 * Gated by COALESCE_CATALOG_API_KEY — skipped silently when absent so
 * contributors without a token can still run `npm test`. CI excludes this
 * file via `vitest run --exclude tests/integration/live-api.test.ts`.
 *
 * The suite assumes:
 *   - EU region (or COALESCE_CATALOG_REGION=us if you're on US)
 *   - A Castor account with at least one table populated
 *
 * Known fixtures below are from the coalesce-shared EU demo account. If
 * running against a different account, override with env vars:
 *   CATALOG_TEST_TABLE_PATH=<db.schema.table>
 *   CATALOG_TEST_COLUMN_PATH=<db.schema.table.column>
 */
import { describe, it, expect } from "vitest";
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const serverPath = join(__dirname, "..", "..", "dist", "index.js");

const HAS_TOKEN = !!process.env.COALESCE_CATALOG_API_KEY;
const describeLive = HAS_TOKEN ? describe : describe.skip;

const TABLE_PATH =
  process.env.CATALOG_TEST_TABLE_PATH ?? "coalesce.sample_data.orders";
const COLUMN_PATH =
  process.env.CATALOG_TEST_COLUMN_PATH ??
  "coalesce.sample_data.orders.o_orderstatus";

interface Client {
  send: (msg: Record<string, unknown>) => void;
  nextResponse: () => Promise<Record<string, unknown>>;
  close: () => void;
}

function launch(): Client {
  const child = spawn("node", [serverPath], {
    env: { ...process.env },
    stdio: ["pipe", "pipe", "inherit"],
  });
  const buffer: string[] = [];
  const pending: Array<(v: string) => void> = [];
  let tail = "";
  child.stdout.on("data", (chunk: Buffer) => {
    tail += chunk.toString("utf-8");
    let idx: number;
    while ((idx = tail.indexOf("\n")) >= 0) {
      const line = tail.slice(0, idx).trim();
      tail = tail.slice(idx + 1);
      if (!line) continue;
      if (pending.length > 0) pending.shift()!(line);
      else buffer.push(line);
    }
  });
  return {
    send: (msg) => child.stdin.write(JSON.stringify(msg) + "\n"),
    nextResponse: () =>
      new Promise((resolve) => {
        const take = (line: string) => resolve(JSON.parse(line));
        if (buffer.length > 0) take(buffer.shift()!);
        else pending.push(take);
      }),
    close: () => child.kill(),
  };
}

async function callTool(
  name: string,
  args: Record<string, unknown>
): Promise<{
  isError?: boolean;
  content: Array<{ type: string; text: string }>;
}> {
  const client = launch();
  client.send({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
      protocolVersion: "2024-11-05",
      capabilities: {},
      clientInfo: { name: "live-test", version: "0.0.0" },
    },
  });
  await client.nextResponse();
  client.send({
    jsonrpc: "2.0",
    method: "notifications/initialized",
    params: {},
  });
  client.send({
    jsonrpc: "2.0",
    id: 2,
    method: "tools/call",
    params: { name, arguments: args },
  });
  const resp = (await client.nextResponse()) as {
    result: { isError?: boolean; content: Array<{ type: string; text: string }> };
  };
  client.close();
  return resp.result;
}

describeLive("live API integration (requires COALESCE_CATALOG_API_KEY)", () => {
  it("resolves a 3-part warehouse path to a TABLE uuid", async () => {
    const res = await callTool("catalog_find_asset_by_path", {
      path: TABLE_PATH,
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.resolved).toBeDefined();
    expect(obj.resolved.kind).toBe("TABLE");
    expect(obj.resolved.id).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
    );
  }, 30000);

  it("resolves a 4-part path to a COLUMN uuid", async () => {
    const res = await callTool("catalog_find_asset_by_path", {
      path: COLUMN_PATH,
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.resolved?.kind).toBe("COLUMN");
    expect(obj.resolved.column).toBeDefined();
  }, 30000);

  it("returns notFound for a nonsense path", async () => {
    const res = await callTool("catalog_find_asset_by_path", {
      path: "no.such.thing.here",
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    // 4 parts → column lookup; database won't resolve so notFound surfaces
    expect(obj.notFound ?? obj.ambiguous).toBeTruthy();
  }, 30000);

  it("paginates catalog_search_tables with hasMore metadata", async () => {
    const res = await callTool("catalog_search_tables", { nbPerPage: 2 });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.pagination).toBeDefined();
    expect(obj.pagination.nbPerPage).toBe(2);
    expect(Array.isArray(obj.data)).toBe(true);
    if (obj.pagination.totalCount > 2) {
      expect(obj.pagination.hasMore).toBe(true);
    }
  }, 30000);

  it("catalog_summarize_asset bundles core/ownership/tags/lineage sections", async () => {
    const resolve = await callTool("catalog_find_asset_by_path", {
      path: TABLE_PATH,
    });
    const id = JSON.parse(resolve.content[0].text).resolved.id;

    const summary = await callTool("catalog_summarize_asset", {
      kind: "TABLE",
      id,
      columnsLimit: 3,
      upstreamLimit: 3,
      downstreamLimit: 3,
      qualityLimit: 3,
    });
    expect(summary.isError).not.toBe(true);
    const obj = JSON.parse(summary.content[0].text);
    expect(obj.core).toBeDefined();
    expect(obj.core.name).toBeTruthy();
    expect(obj.ownership).toBeDefined();
    expect(obj.annotations).toBeDefined();
    expect(obj.lineage.upstream).toBeDefined();
    expect(obj.lineage.downstream).toBeDefined();
    expect(obj.columns).toBeDefined();
  }, 60000);

  it("catalog_trace_missing_lineage returns structured findings", async () => {
    const resolve = await callTool("catalog_find_asset_by_path", {
      path: TABLE_PATH,
    });
    const id = JSON.parse(resolve.content[0].text).resolved.id;

    const trace = await callTool("catalog_trace_missing_lineage", {
      tableId: id,
      columnSampleSize: 3,
    });
    expect(trace.isError).not.toBe(true);
    const obj = JSON.parse(trace.content[0].text);
    expect(obj.table?.id).toBe(id);
    expect(obj.tableLineage).toBeDefined();
    expect(Array.isArray(obj.findings)).toBe(true);
    expect(obj.summary.severityCounts).toBeDefined();
  }, 60000);

  it("handles a malformed UUID without crashing — either structured isError or empty table", async () => {
    const res = await callTool("catalog_get_table", { id: "definitely-not-a-uuid" });
    // Two acceptable outcomes (depending on the API's tolerance): a
    // structured isError, or a successful response with table=null. Anything
    // else (truthy body that's neither shape) is a regression.
    if (res.isError === true) {
      const body = JSON.parse(res.content[0].text);
      expect(body.error).toBeTruthy();
    } else {
      const body = JSON.parse(res.content[0].text);
      // Successful path: table should be explicitly null/absent.
      expect(body.table === null || body.table === undefined).toBe(true);
    }
  }, 30000);

  // Smoke tests for hand-written GraphQL operations introduced for the
  // composed workflow tools (GET_TABLES_DETAIL_BATCH, GET_DASHBOARDS_DETAIL_BATCH).
  // Unit tests cover the handler logic; these confirm the field selections
  // are valid against the real Castor schema.

  it("catalog_assess_impact returns a populated severity report at depth 2", async () => {
    const resolve = await callTool("catalog_find_asset_by_path", {
      path: TABLE_PATH,
    });
    const id = JSON.parse(resolve.content[0].text).resolved.id;

    const impact = await callTool("catalog_assess_impact", {
      assetKind: "TABLE",
      assetId: id,
      maxDepth: 2,
      includeQualityChecks: true,
    });
    expect(impact.isError).not.toBe(true);
    const obj = JSON.parse(impact.content[0].text);

    // Asset core wired up.
    expect(obj.asset?.id).toBe(id);
    expect(obj.asset?.kind).toBe("TABLE");
    expect(obj.ownership).toBeDefined();
    expect(obj.tags).toBeDefined();

    // Downstream summary always present, even when count is 0.
    expect(obj.downstream).toBeDefined();
    expect(typeof obj.downstream.totalCount).toBe("number");
    expect(typeof obj.downstream.distinctOwnerTeamCount).toBe("number");
    expect(typeof obj.downstream.unownedCount).toBe("number");
    expect(Array.isArray(obj.downstream.assets)).toBe(true);
    // Shape of each enriched asset (only assert when downstream is non-empty;
    // a leaf table is a valid universe state we shouldn't fail on).
    if (obj.downstream.assets.length > 0) {
      const first = obj.downstream.assets[0];
      expect(first.id).toBeTruthy();
      expect(["TABLE", "DASHBOARD"]).toContain(first.kind);
      expect(typeof first.ownerUserCount).toBe("number");
      expect(typeof first.ownerTeamCount).toBe("number");
      expect(Array.isArray(first.teams)).toBe(true);
      expect(typeof first.depth).toBe("number");
    }

    // Severity scoring deterministic + transparent.
    expect(obj.severity?.bucket).toMatch(/^(low|medium|high)$/);
    expect(typeof obj.severity?.score).toBe("number");
    expect(obj.severity.score).toBeGreaterThanOrEqual(0);
    expect(obj.severity.score).toBeLessThanOrEqual(100);
    expect(Array.isArray(obj.severity?.rationale)).toBe(true);
    expect(obj.severity.rationale).toHaveLength(3);

    // Quality coverage requested → present (may be empty).
    expect(obj.qualityChecks).not.toBeNull();
  }, 60000);

  it("catalog_governance_scorecard returns a coverage matrix scoped to the test schema", async () => {
    // Resolve the test table → grab its schemaId via catalog_get_table.
    const resolve = await callTool("catalog_find_asset_by_path", {
      path: TABLE_PATH,
    });
    const tableId = JSON.parse(resolve.content[0].text).resolved.id;

    const detail = await callTool("catalog_get_table", { id: tableId });
    const tableObj = JSON.parse(detail.content[0].text).table;
    const schemaId = tableObj?.schema?.id ?? tableObj?.schemaId;
    expect(schemaId).toBeTruthy();

    const scorecard = await callTool("catalog_governance_scorecard", {
      schemaId,
      includeQualityCoverage: true,
    });
    expect(scorecard.isError).not.toBe(true);
    const obj = JSON.parse(scorecard.content[0].text);

    expect(obj.scopedBy).toBe("schemaId");
    expect(typeof obj.tableCount).toBe("number");
    expect(Array.isArray(obj.tables)).toBe(true);
    expect(obj.tables.length).toBe(obj.tableCount);

    // Aggregate well-formed under the requested 5-axis matrix.
    expect(obj.aggregate).toBeDefined();
    expect(obj.aggregate.axes).toEqual([
      "owned",
      "described",
      "tagged",
      "columnDoc",
      "checked",
    ]);
    expect(obj.aggregate.governanceScore).toBeGreaterThanOrEqual(0);
    expect(obj.aggregate.governanceScore).toBeLessThanOrEqual(100);

    if (obj.tables.length > 0) {
      const row = obj.tables[0];
      expect(row.id).toBeTruthy();
      expect(typeof row.hasOwner).toBe("boolean");
      expect(typeof row.hasDescription).toBe("boolean");
      expect(typeof row.tagCount).toBe("number");
      expect(typeof row.qualityCheckCount).toBe("number");
      expect(typeof row.hasQualityCheck).toBe("boolean");
      // columnDocCoverage is the per-table object; either coverage data or an error envelope.
      expect(row.columnDocCoverage).toBeDefined();
    }
  }, 90000);

  // Smoke test for catalog_describe_type — catches regressions in the
  // GraphQL type-introspection query's field selection.
  it("catalog_describe_type returns a flattened type reference for FieldLineage", async () => {
    const res = await callTool("catalog_describe_type", {
      typeName: "FieldLineage",
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.type?.name).toBe("FieldLineage");
    expect(obj.type?.kind).toBe("OBJECT");
    const fieldNames = (obj.type.fields as Array<{ name: string }>).map((f) => f.name);
    expect(fieldNames).toEqual(expect.arrayContaining(["parentColumnId", "childColumnId"]));
  }, 30000);

  // Smoke test for catalog_get_column_lineage — critical regression guard
  // for the FQN → UUID walk + batched enrichment. Earlier iterations of the
  // enrichment path tried scope: { ids: [...] } on getSchemas / getDatabases,
  // which the server rejects (neither scope accepts `ids`). Exercising the
  // full workflow against a live endpoint makes that class of bug fail here
  // rather than silently in Sharon's terminal.
  it("catalog_get_column_lineage walks, resolves FQNs, and returns a structured graph", async () => {
    const res = await callTool("catalog_get_column_lineage", {
      columnFQN: COLUMN_PATH,
      direction: "both",
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);

    // Root resolved + fully named.
    expect(obj.root?.columnId).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
    );
    expect(obj.root.name).toBeTruthy();
    // FQN may legitimately be null only if the server dropped an ancestor row;
    // under normal conditions every ancestor resolves cleanly.
    expect(obj.root.databaseName).toBeTruthy();
    expect(obj.root.schemaName).toBeTruthy();
    expect(obj.root.tableName).toBeTruthy();

    // Both sides present since direction: "both".
    expect(obj.upstream).toBeDefined();
    expect(obj.downstream).toBeDefined();
    expect(typeof obj.upstream.nodeCount).toBe("number");
    expect(typeof obj.downstream.nodeCount).toBe("number");
    expect(Array.isArray(obj.upstream.nodes)).toBe(true);
    expect(Array.isArray(obj.downstream.edges)).toBe(true);

    // Reached column nodes must have fully-resolved FQNs. This is the
    // exact symptom the getSchemas/getDatabases `ids`-scope bug produced:
    // handler threw mid-enrichment, whole call 4'd out. With the fix in
    // place, ancestors resolve cleanly through the getTables relation
    // chain.
    const allColumnNodes = [
      ...(obj.upstream.nodes as Array<Record<string, unknown>>),
      ...(obj.downstream.nodes as Array<Record<string, unknown>>),
    ].filter((n) => n.assetType === "COLUMN");

    // Guard against a "green test" where the chosen column has no lineage:
    // the enrichment path wouldn't exercise at all. Pick a column fixture
    // (via CATALOG_TEST_COLUMN_PATH) that has at least one reachable neighbor.
    expect(allColumnNodes.length).toBeGreaterThan(0);

    for (const n of allColumnNodes) {
      expect(n.id).toBeTruthy();
      expect(n.name).toBeTruthy();
      // fqn header: databaseName . schemaName . tableName . name
      expect(n.databaseName).toBeTruthy();
      expect(n.schemaName).toBeTruthy();
      expect(n.tableName).toBeTruthy();
      expect(n.fqn).toMatch(/^.+\..+\..+\..+$/);
    }

    // Stats.totalColumnsReached must equal the count of non-root COLUMN
    // nodes in the graph. A mismatch would mean either the walk dropped a
    // reached column or enrichment silently dropped its entry — both
    // partial-failure modes the aggregate stats should surface.
    expect(obj.stats?.totalColumnsReached).toBe(allColumnNodes.length);

    // Every edge endpoint must resolve to a node in the graph (no dangling
    // edges that point at an unknown id — that would be a BFS correctness
    // bug, since we both visit and record on the same pass).
    const nodeIds = new Set<string>([
      obj.root.columnId as string,
      ...allColumnNodes.map((n) => n.id as string),
      ...[
        ...(obj.upstream.nodes as Array<Record<string, unknown>>),
        ...(obj.downstream.nodes as Array<Record<string, unknown>>),
      ]
        .filter((n) => n.assetType === "DASHBOARD_FIELD")
        .map((n) => n.id as string),
    ]);
    const allEdges = [
      ...(obj.upstream.edges as Array<Record<string, unknown>>),
      ...(obj.downstream.edges as Array<Record<string, unknown>>),
    ];
    for (const e of allEdges) {
      expect(nodeIds.has(e.parentId as string)).toBe(true);
      expect(nodeIds.has(e.childId as string)).toBe(true);
    }
  }, 90000);

  // Additional catalog_describe_type coverage: INPUT_OBJECT shape +
  // notFound-with-suggestions path. The OBJECT case is covered above.
  it("catalog_describe_type surfaces INPUT_OBJECT input fields for GetFieldLineagesScope", async () => {
    const res = await callTool("catalog_describe_type", {
      typeName: "GetFieldLineagesScope",
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.type?.kind).toBe("INPUT_OBJECT");
    expect(obj.type?.fields).toBeUndefined();
    const inputNames = (obj.type.inputFields as Array<{ name: string }>).map((f) => f.name);
    // The scope accepts at least these four filters.
    expect(inputNames).toEqual(
      expect.arrayContaining([
        "parentColumnId",
        "childColumnId",
        "parentDashboardFieldId",
        "childDashboardFieldId",
      ])
    );
  }, 30000);

  it("catalog_describe_type returns notFound with near-match suggestions for a typo", async () => {
    const res = await callTool("catalog_describe_type", {
      typeName: "FieldLinage", // intentional typo
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.notFound).toBe(true);
    expect(obj.typeName).toBe("FieldLinage");
    expect(Array.isArray(obj.suggestions)).toBe(true);
    // Levenshtein ≤ 2 should surface the correct type.
    expect(obj.suggestions).toEqual(expect.arrayContaining(["FieldLineage"]));
  }, 30000);

  // catalog_run_graphql smoke tests — exercises executeRaw, the mutation
  // guardrail, and the verbatim-errors contract against the live server.
  it("catalog_run_graphql executes a query and returns data verbatim", async () => {
    const res = await callTool("catalog_run_graphql", {
      query: "query { getSources(pagination: { nbPerPage: 1, page: 0 }) { totalCount data { id name } } }",
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.data?.getSources).toBeDefined();
    expect(typeof obj.data.getSources.totalCount).toBe("number");
    expect(Array.isArray(obj.data.getSources.data)).toBe(true);
    expect(obj.errors).toBeUndefined();
  }, 30000);

  it("catalog_run_graphql surfaces GraphQL validation errors without re-mapping them", async () => {
    const res = await callTool("catalog_run_graphql", {
      query: "query { getLineages { thisFieldDoesNotExist } }",
    });
    // withErrorHandling doesn't flip isError here — the envelope contract
    // is that validation errors come through in `errors[]`, not as tool
    // errors, so the agent can reason about them.
    const obj = JSON.parse(res.content[0].text);
    expect(Array.isArray(obj.errors)).toBe(true);
    expect(obj.errors[0]?.message).toMatch(/thisFieldDoesNotExist/);
  }, 30000);

  it("catalog_run_graphql blocks mutations by default and never hits the network", async () => {
    const res = await callTool("catalog_run_graphql", {
      query: "mutation { deleteLineages(data: []) }",
    });
    expect(res.isError).not.toBe(true);
    const obj = JSON.parse(res.content[0].text);
    expect(obj.blocked).toBe("mutation");
  }, 15000);
});
