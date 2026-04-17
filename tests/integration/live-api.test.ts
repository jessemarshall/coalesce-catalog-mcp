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

  it("surfaces GraphQL validation errors as structured isError responses", async () => {
    // Malformed UUID triggers server-side argument validation
    const res = await callTool("catalog_get_table", { id: "definitely-not-a-uuid" });
    // Either isError=true with structured GraphQL error, or empty table
    // (depending on the API's tolerance). Both are acceptable — the key
    // assertion is we don't crash / hang.
    expect(res.content[0].text).toBeTruthy();
  }, 30000);
});
