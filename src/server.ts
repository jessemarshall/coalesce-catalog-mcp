import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CatalogClient } from "./client.js";
import { SERVER_NAME, SERVER_VERSION, READ_ONLY_ENV_VAR } from "./constants.js";
import type { CatalogToolDefinition } from "./catalog/types.js";
import { defineTableTools } from "./mcp/tables.js";
import { defineLineageTools } from "./mcp/lineage.js";
import { defineColumnTools } from "./mcp/columns.js";
import { defineDashboardTools } from "./mcp/dashboards.js";
import { defineDiscoveryTools } from "./mcp/discovery.js";
import { defineAnnotationTools } from "./mcp/annotations.js";
import { defineGovernanceTools } from "./mcp/governance.js";
import { defineAiTools } from "./mcp/ai.js";

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

TOOLING NOTES
- All list tools paginate server-side; responses include \`pagination.hasMore\`.
  Start with nbPerPage=25-100 and page=0; only fetch deeper pages on demand.
- Read-only mode: set COALESCE_CATALOG_READ_ONLY=true to drop all mutation
  tools at registration time. Default is read-write.
`.trim();

/**
 * Whether a tool should be available in read-only mode. Tools default to
 * "write" (excluded) unless they declare readOnlyHint: true, matching the
 * transform MCP's convention.
 */
function isReadOnlyTool(def: CatalogToolDefinition): boolean {
  return def.config.annotations?.readOnlyHint === true;
}

export function createCoalesceCatalogMcpServer(
  client: CatalogClient
): McpServer {
  const server = new McpServer(
    { name: SERVER_NAME, version: SERVER_VERSION },
    { instructions: SERVER_INSTRUCTIONS }
  );

  const readOnly = isReadOnlyMode();
  const definitions: CatalogToolDefinition[] = [
    ...defineTableTools(client),
    ...defineLineageTools(client),
    ...defineColumnTools(client),
    ...defineDashboardTools(client),
    ...defineDiscoveryTools(client),
    ...defineAnnotationTools(client),
    ...defineGovernanceTools(client),
    ...defineAiTools(client),
  ];

  for (const def of definitions) {
    if (readOnly && !isReadOnlyTool(def)) continue;
    // SDK's handler type demands an index signature on the return; our
    // narrower ToolResult is structurally compatible but needs a cast.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    server.registerTool(def.name, def.config, def.handler as any);
  }

  return server;
}
