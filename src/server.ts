import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CatalogClient } from "./client.js";
import { SERVER_NAME, SERVER_VERSION, READ_ONLY_ENV_VAR } from "./constants.js";

export function isReadOnlyMode(): boolean {
  return process.env[READ_ONLY_ENV_VAR] === "true";
}

const SERVER_INSTRUCTIONS = `
coalesce-catalog-mcp — Coalesce Catalog (Castor) Public GraphQL API, wrapped as
MCP. Use this server for data catalog discovery, lineage, governance metadata,
and asset annotations across your warehouse + BI tools.

ECOSYSTEM BOUNDARIES
- coalesce-transform-mcp — pipeline/node authoring inside the Coalesce Transform
  product. Reach for Transform when the user is building, running, or debugging
  nodes, pipelines, jobs, or environments.
- coalesce-catalog-mcp (this server) — discovery, lineage, governance across the
  already-materialized warehouse and BI layer. Reach for Catalog when the user
  is asking "what exists / who owns it / what feeds into it / what depends on
  it / how is it described" about tables, columns, dashboards, terms, or tags.

WORKFLOW SEAM
- A Coalesce node materialises a warehouse table → that table appears in the
  Catalog. When a user needs end-to-end context (node definition + downstream
  dashboards), call both servers and stitch results.

THIS SERVER IS A PHASE-0 SCAFFOLD. Tools will be added in subsequent phases.
Authentication is already wired; GraphQL endpoint is resolved per region.
`.trim();

export interface ServerHandle {
  server: McpServer;
}

export function createCoalesceCatalogMcpServer(
  _client: CatalogClient
): McpServer {
  const server = new McpServer(
    { name: SERVER_NAME, version: SERVER_VERSION },
    { instructions: SERVER_INSTRUCTIONS }
  );

  // Tool registration will happen here in Phase 2+.
  // Read-only mode gating will be added alongside the first write tools.

  return server;
}
