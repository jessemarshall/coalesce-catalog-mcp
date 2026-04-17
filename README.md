# coalesce-catalog-mcp

MCP server for the [Coalesce Catalog](https://coalesce.io/catalog) Public GraphQL API. Covers tables, columns, dashboards, lineage, tags, terms, governance, and asset metadata across the warehouse + BI layer.

> **Status:** Phase 0 scaffold. The server starts, authenticates, and responds to the MCP `initialize` handshake. Tool surface lands in Phase 2.

## Why this exists

The existing remote Castor MCP (`https://api.castordoc.com/mcp/server`) exposes only 8 tools (4 search + 4 metadata) and no lineage / mutations. This server targets the full GraphQL surface — 20 queries + 33 mutations across 13 domains — with rich descriptions, composite workflow tools, and a read-only toggle.

Built as the catalog-side companion to [`coalesce-transform-mcp`](https://www.npmjs.com/package/coalesce-transform-mcp).

## Configuration

| Env var | Required | Default | Notes |
| --- | --- | --- | --- |
| `COALESCE_CATALOG_API_KEY` | yes | — | Castor API token. |
| `COALESCE_CATALOG_REGION` | no | `eu` | `eu` or `us`. |
| `COALESCE_CATALOG_API_URL` | no | region-derived | Override full base URL. |
| `COALESCE_CATALOG_READ_ONLY` | no | `false` | Drops mutation tools when `true`. |

## Install & run (local dev)

```bash
npm install
npm run build
COALESCE_CATALOG_API_KEY=... node dist/index.js
```

## MCP client config (after publish)

```jsonc
{
  "mcpServers": {
    "coalesce-catalog": {
      "command": "npx",
      "args": ["-y", "coalesce-catalog-mcp@alpha"],
      "env": {
        "COALESCE_CATALOG_API_KEY": "${COALESCE_CATALOG_API_KEY}",
        "COALESCE_CATALOG_REGION": "eu"
      }
    }
  }
}
```

## Architecture

Mirrors [`coalesce-transform-mcp`](https://github.com/jessemarshall/coalesce-transform-mcp):

- `src/index.ts` — entry, wires stdio transport
- `src/client.ts` — GraphQL client (raw `fetch`, timeouts, abort, typed errors)
- `src/server.ts` — `McpServer` construction + `SERVER_INSTRUCTIONS`
- `src/services/config/credentials.ts` — env + region resolution
- `src/mcp/*.ts` — one file per API-reference domain (tables, lineage, …) *(Phase 2+)*
- `src/workflows/*.ts` — composite cross-domain tools *(Phase 3+)*
- `src/generated/*` — `graphql-codegen` output *(Phase 1+)*

## License

MIT
