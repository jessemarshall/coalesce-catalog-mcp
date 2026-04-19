import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_DATABASES,
  GET_SCHEMAS,
  GET_TABLES_SUMMARY,
  GET_COLUMNS_SUMMARY,
} from "../catalog/operations.js";
import type {
  GetColumnsOutput,
  GetDatabasesOutput,
  GetSchemasOutput,
  GetTablesOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

interface NamedRow {
  id: string;
  name: string;
}

/**
 * Split a dotted warehouse path into components, trimming whitespace and
 * unwrapping common identifier quoting (double quotes, backticks).
 */
export function parsePath(path: string): string[] {
  const parts: string[] = [];
  let current = "";
  let i = 0;
  let inQuote: string | null = null;
  while (i < path.length) {
    const ch = path[i];
    if (inQuote) {
      if (ch === inQuote) {
        inQuote = null;
      } else {
        current += ch;
      }
    } else if (ch === '"' || ch === "`") {
      inQuote = ch;
    } else if (ch === ".") {
      parts.push(current.trim());
      current = "";
    } else {
      current += ch;
    }
    i++;
  }
  parts.push(current.trim());
  return parts.filter((p) => p.length > 0);
}

function matchByName<T extends NamedRow>(
  rows: T[],
  expected: string,
  caseSensitive: boolean
): T[] {
  if (caseSensitive) return rows.filter((r) => r.name === expected);
  const lc = expected.toLowerCase();
  return rows.filter((r) => (r.name ?? "").toLowerCase() === lc);
}

// Step page size + bounded pagination ceiling. At 200 rows/page * 10 pages =
// 2000 substring-matching names at the deepest step, which covers every
// realistic catalog. Without this, short common substrings like "db" against
// a 60-database account silently returned notFound from the first-50 window.
const STEP_PAGE_SIZE = 200;
const STEP_MAX_PAGES = 10;

/**
 * Paginate a step query exhaustively (up to STEP_MAX_PAGES) collecting every
 * exact-name match. Each step already narrows with `nameContains`, so the
 * pagination exists only to cover the case where many rows share a common
 * substring. Throws rather than silently truncating if the ceiling is hit.
 */
async function findExactMatches<T extends NamedRow>(
  fetchPage: (page: number, nbPerPage: number) => Promise<{ data: T[]; totalCount: number }>,
  expected: string,
  caseSensitive: boolean,
  stepLabel: string
): Promise<T[]> {
  const matches: T[] = [];
  for (let page = 0; page < STEP_MAX_PAGES; page++) {
    const resp = await fetchPage(page, STEP_PAGE_SIZE);
    matches.push(...matchByName(resp.data, expected, caseSensitive));
    if (resp.data.length < STEP_PAGE_SIZE) return matches;
    if ((page + 1) * STEP_PAGE_SIZE >= resp.totalCount) return matches;
  }
  throw new Error(
    `${stepLabel} search for "${expected}" spans more than ${STEP_MAX_PAGES * STEP_PAGE_SIZE} ` +
      `substring-matching rows. Pass a more specific ${stepLabel.toLowerCase()} name or ` +
      `resolve this asset via UUID directly.`
  );
}

const FindAssetByPathInputShape = {
  path: z
    .string()
    .min(1)
    .describe(
      "Dotted warehouse path. 3 parts = table (DATABASE.SCHEMA.TABLE); 4 parts = column (DATABASE.SCHEMA.TABLE.COLUMN). Double-quoted or backtick-quoted identifiers are unwrapped."
    ),
  caseSensitive: z
    .boolean()
    .optional()
    .describe(
      "Whether name comparisons are case-sensitive. Default: false (matches typical Snowflake/Postgres behavior of folding unquoted identifiers)."
    ),
};

export function defineFindAssetByPath(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_find_asset_by_path",
    config: {
      title: "Find Asset By Warehouse Path",
      description:
        "Resolve a fully-qualified warehouse path (DATABASE.SCHEMA.TABLE or DATABASE.SCHEMA.TABLE.COLUMN) to its Catalog UUID. This is the gateway workflow: use its output as `id` on any downstream catalog_get_* tool.\n\n" +
        "The tool walks the catalog tree (database → schema → table → [column]) rather than relying on path-substring search, which is more robust across warehouses that format paths differently. On ambiguity (multiple matches at any level), returns all candidates and `ambiguous: true` so the caller can disambiguate.\n\n" +
        "Returns: { resolved: { kind, id, database, schema, table, column? } } on success, or { notFound: true, reason, candidates? } / { ambiguous: true, candidates } otherwise.",
      inputSchema: FindAssetByPathInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const path = args.path as string;
      const caseSensitive = (args.caseSensitive as boolean | undefined) ?? false;

      const parts = parsePath(path);
      if (parts.length < 3 || parts.length > 4) {
        return {
          notFound: true,
          reason:
            `Expected 3 or 4 path components (got ${parts.length}). ` +
            `Format: DATABASE.SCHEMA.TABLE or DATABASE.SCHEMA.TABLE.COLUMN.`,
          parsedParts: parts,
        };
      }
      const [dbName, schemaName, tableName, columnName] = parts;

      // Step 1: database
      const dbMatches = await findExactMatches(
        async (page, nbPerPage) => {
          const resp = await c.execute<{ getDatabases: GetDatabasesOutput }>(
            GET_DATABASES,
            {
              scope: { nameContains: dbName },
              pagination: { nbPerPage, page },
            }
          );
          return resp.getDatabases;
        },
        dbName,
        caseSensitive,
        "Database"
      );
      if (dbMatches.length === 0) {
        return { notFound: true, reason: `No database matched "${dbName}".` };
      }
      if (dbMatches.length > 1) {
        return {
          ambiguous: true,
          at: "database",
          candidates: dbMatches.map((d) => ({ id: d.id, name: d.name })),
        };
      }
      const database = dbMatches[0];

      // Step 2: schema
      const schemaMatches = await findExactMatches(
        async (page, nbPerPage) => {
          const resp = await c.execute<{ getSchemas: GetSchemasOutput }>(
            GET_SCHEMAS,
            {
              scope: { databaseIds: [database.id], nameContains: schemaName },
              pagination: { nbPerPage, page },
            }
          );
          return resp.getSchemas;
        },
        schemaName,
        caseSensitive,
        "Schema"
      );
      if (schemaMatches.length === 0) {
        return {
          notFound: true,
          reason: `No schema named "${schemaName}" in database "${database.name}".`,
          database: { id: database.id, name: database.name },
        };
      }
      if (schemaMatches.length > 1) {
        return {
          ambiguous: true,
          at: "schema",
          database: { id: database.id, name: database.name },
          candidates: schemaMatches.map((s) => ({ id: s.id, name: s.name })),
        };
      }
      const schema = schemaMatches[0];

      // Step 3: table
      const tableMatches = await findExactMatches(
        async (page, nbPerPage) => {
          const resp = await c.execute<{ getTables: GetTablesOutput }>(
            GET_TABLES_SUMMARY,
            {
              scope: { schemaId: schema.id, nameContains: tableName },
              pagination: { nbPerPage, page },
            }
          );
          return resp.getTables;
        },
        tableName,
        caseSensitive,
        "Table"
      );
      if (tableMatches.length === 0) {
        return {
          notFound: true,
          reason: `No table named "${tableName}" in schema "${database.name}.${schema.name}".`,
          database: { id: database.id, name: database.name },
          schema: { id: schema.id, name: schema.name },
        };
      }
      if (tableMatches.length > 1) {
        return {
          ambiguous: true,
          at: "table",
          database: { id: database.id, name: database.name },
          schema: { id: schema.id, name: schema.name },
          candidates: tableMatches.map((t) => ({ id: t.id, name: t.name })),
        };
      }
      const table = tableMatches[0];

      if (!columnName) {
        return {
          resolved: {
            kind: "TABLE",
            id: table.id,
            fullPath: `${database.name}.${schema.name}.${table.name}`,
            database: { id: database.id, name: database.name },
            schema: { id: schema.id, name: schema.name },
            table: { id: table.id, name: table.name },
          },
        };
      }

      // Step 4: column. Use nameContains (not name) so the server filter is
      // substring-based and the client-side matchByName applies the
      // caseSensitive option — otherwise a case-folded column like `order_id`
      // would never match `ORDER_ID` regardless of caseSensitive.
      const colMatches = await findExactMatches(
        async (page, nbPerPage) => {
          const resp = await c.execute<{ getColumns: GetColumnsOutput }>(
            GET_COLUMNS_SUMMARY,
            {
              scope: { tableId: table.id, nameContains: columnName },
              pagination: { nbPerPage, page },
            }
          );
          return resp.getColumns;
        },
        columnName,
        caseSensitive,
        "Column"
      );
      if (colMatches.length === 0) {
        return {
          notFound: true,
          reason: `No column named "${columnName}" on table "${database.name}.${schema.name}.${table.name}".`,
          database: { id: database.id, name: database.name },
          schema: { id: schema.id, name: schema.name },
          table: { id: table.id, name: table.name },
        };
      }
      if (colMatches.length > 1) {
        return {
          ambiguous: true,
          at: "column",
          database: { id: database.id, name: database.name },
          schema: { id: schema.id, name: schema.name },
          table: { id: table.id, name: table.name },
          candidates: colMatches.map((col) => ({ id: col.id, name: col.name })),
        };
      }
      const column = colMatches[0];

      return {
        resolved: {
          kind: "COLUMN",
          id: column.id,
          fullPath: `${database.name}.${schema.name}.${table.name}.${column.name}`,
          database: { id: database.id, name: database.name },
          schema: { id: schema.id, name: schema.name },
          table: { id: table.id, name: table.name },
          column: { id: column.id, name: column.name },
        },
      };
    }, client),
  };
}
