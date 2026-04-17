import { createClient } from "../../src/client.js";
import { createCoalesceCatalogMcpServer } from "../../src/server.js";

/**
 * Build an in-process MCP server wired to a fake client that never touches
 * the network. Use this in contract tests that only inspect registration
 * shape (tool list, resource list, input schemas, etc.).
 *
 * Caller can override env vars on `process.env` *before* importing this,
 * e.g. set COALESCE_CATALOG_READ_ONLY=true to test filtering.
 */
export function makeServer() {
  const client = createClient({
    apiKey: "dummy-not-used",
    region: "eu",
    endpoint: "https://example.invalid/public/graphql",
  });
  return createCoalesceCatalogMcpServer(client);
}
