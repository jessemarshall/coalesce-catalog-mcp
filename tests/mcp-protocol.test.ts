import { describe, it, expect } from "vitest";
import { spawn } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const serverPath = join(__dirname, "..", "dist", "index.js");

interface Client {
  send: (msg: Record<string, unknown>) => void;
  nextResponse: () => Promise<Record<string, unknown>>;
  close: () => void;
}

function launchWithDummyKey(): Client {
  const child = spawn("node", [serverPath], {
    env: { ...process.env, COALESCE_CATALOG_API_KEY: "dummy" },
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

describe("MCP protocol round-trip (no live API)", () => {
  it("responds to initialize with our serverInfo", async () => {
    const client = launchWithDummyKey();
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
    const resp = (await client.nextResponse()) as {
      result: {
        serverInfo: { name: string; version: string };
        instructions: string;
      };
    };
    client.close();

    expect(resp.result.serverInfo.name).toBe("coalesce-catalog");
    expect(resp.result.serverInfo.version).toMatch(/^\d+\.\d+\.\d+/);
    expect(resp.result.instructions).toMatch(/coalesce-catalog-mcp/);
  }, 10000);

  it("lists 5 resources", async () => {
    const client = launchWithDummyKey();
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
    await client.nextResponse();
    client.send({
      jsonrpc: "2.0",
      method: "notifications/initialized",
      params: {},
    });
    client.send({
      jsonrpc: "2.0",
      id: 2,
      method: "resources/list",
      params: {},
    });
    const resp = (await client.nextResponse()) as {
      result: { resources: Array<{ uri: string }> };
    };
    client.close();
    expect(resp.result.resources).toHaveLength(5);
    const uris = resp.result.resources.map((r) => r.uri);
    expect(uris).toEqual(
      expect.arrayContaining([
        "catalog://context/overview",
        "catalog://context/tool-routing",
        "catalog://context/ecosystem-boundaries",
        "catalog://context/investigation-playbook",
        "catalog://context/governance-rollout",
      ])
    );
  }, 10000);

  it("lists 5 prompts", async () => {
    const client = launchWithDummyKey();
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
    await client.nextResponse();
    client.send({
      jsonrpc: "2.0",
      method: "notifications/initialized",
      params: {},
    });
    client.send({
      jsonrpc: "2.0",
      id: 2,
      method: "prompts/list",
      params: {},
    });
    const resp = (await client.nextResponse()) as {
      result: { prompts: Array<{ name: string }> };
    };
    client.close();
    expect(resp.result.prompts).toHaveLength(6);
    const names = resp.result.prompts.map((p) => p.name);
    expect(names).toEqual(
      expect.arrayContaining([
        "catalog-start-here",
        "catalog-asset-summary",
        "catalog-find-consumers",
        "catalog-investigate-lineage-gaps",
        "catalog-audit-documentation",
        "catalog-governance-rollout",
      ])
    );
  }, 10000);
});
