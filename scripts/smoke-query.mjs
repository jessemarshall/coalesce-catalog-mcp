#!/usr/bin/env node
/**
 * End-to-end smoke test: drives the compiled CatalogClient against the live
 * API and runs getTables with nbPerPage:1. Fails loudly on HTTP, GraphQL, or
 * unexpected-shape errors. Requires COALESCE_CATALOG_API_KEY in env.
 *
 * Run with:  npm run build && node scripts/smoke-query.mjs
 */
import { validateConfig, createClient } from "../dist/client.js";

const GET_TABLES = /* GraphQL */ `
  query SmokeGetTables($pagination: Pagination) {
    getTables(pagination: $pagination) {
      totalCount
      nbPerPage
      page
      data {
        id
        name
      }
    }
  }
`;

async function main() {
  const config = validateConfig();
  const client = createClient(config);
  console.error(`[smoke] region=${config.region} endpoint=${config.endpoint}`);

  const data = await client.query(GET_TABLES, {
    pagination: { nbPerPage: 1, page: 0 },
  });

  const { totalCount, nbPerPage, page, data: rows } = data.getTables;
  console.error(
    `[smoke] totalCount=${totalCount} nbPerPage=${nbPerPage} page=${page} rows=${rows.length}`
  );
  if (rows.length !== 1) {
    throw new Error(`expected 1 row, got ${rows.length}`);
  }
  console.error(`[smoke] first table: ${rows[0].name} (${rows[0].id})`);
  console.error("[smoke] OK");
}

main().catch((err) => {
  console.error("[smoke] FAIL");
  console.error(err);
  process.exit(1);
});
