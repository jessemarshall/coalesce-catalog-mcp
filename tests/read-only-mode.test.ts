import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const serverPath = join(__dirname, "..", "dist", "index.js");

interface InitialisedClient {
  send: (payload: Record<string, unknown>) => void;
  nextResponse: () => Promise<Record<string, unknown>>;
  close: () => void;
}

function launch(env: Record<string, string>): InitialisedClient {
  const child = spawn("node", [serverPath], {
    env: { ...process.env, ...env },
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
      if (pending.length > 0) {
        pending.shift()!(line);
      } else {
        buffer.push(line);
      }
    }
  });
  return {
    send: (payload) => {
      child.stdin.write(JSON.stringify(payload) + "\n");
    },
    nextResponse: () =>
      new Promise<Record<string, unknown>>((resolve) => {
        const take = (line: string) => resolve(JSON.parse(line));
        if (buffer.length > 0) take(buffer.shift()!);
        else pending.push(take);
      }),
    close: () => child.kill(),
  };
}

async function listTools(env: Record<string, string>): Promise<string[]> {
  const client = launch(env);
  client.send({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: {
      protocolVersion: "2024-11-05",
      capabilities: {},
      clientInfo: { name: "test", version: "0.0.0" },
    },
  });
  await client.nextResponse(); // init ack
  client.send({
    jsonrpc: "2.0",
    method: "notifications/initialized",
    params: {},
  });
  client.send({
    jsonrpc: "2.0",
    id: 2,
    method: "tools/list",
    params: {},
  });
  const resp = (await client.nextResponse()) as {
    result?: { tools?: Array<{ name: string }> };
  };
  client.close();
  return (resp.result?.tools ?? []).map((t) => t.name);
}

describe("COALESCE_CATALOG_READ_ONLY", () => {
  it("normal mode registers 58 tools", async () => {
    const names = await listTools({ COALESCE_CATALOG_API_KEY: "dummy" });
    expect(names).toHaveLength(58);
  }, 10000);

  it("read-only mode registers only 35 tools (all reads)", async () => {
    const names = await listTools({
      COALESCE_CATALOG_API_KEY: "dummy",
      COALESCE_CATALOG_READ_ONLY: "true",
    });
    expect(names).toHaveLength(35);
    // No write/destructive tool names leak through.
    // Note: `add` is excluded from the forbidden list because the schema
    // exposes `add_team_users` as a write, but we also have read tools
    // whose names start with get_ / search_ / find_ only — no read tool
    // begins with `add`. `get_user_owned_assets` etc. are reads and keep
    // working, since they don't match any write prefix.
    const forbiddenPrefixes = ["attach", "detach", "update", "create", "upsert", "delete", "remove", "add"];
    for (const name of names) {
      const trimmed = name.replace(/^catalog_/, "");
      for (const prefix of forbiddenPrefixes) {
        expect(trimmed).not.toMatch(new RegExp(`^${prefix}_`));
      }
    }
  }, 10000);
});
