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
function parsePath(path: string): string[] {
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
      const dbsResp = await c.query<{ getDatabases: GetDatabasesOutput }>(
        GET_DATABASES,
        {
          scope: { nameContains: dbName },
          pagination: { nbPerPage: 50, page: 0 },
        }
      );
      const dbMatches = matchByName(dbsResp.getDatabases.data, dbName, caseSensitive);
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
      const schemasResp = await c.query<{ getSchemas: GetSchemasOutput }>(
        GET_SCHEMAS,
        {
          scope: { databaseIds: [database.id], nameContains: schemaName },
          pagination: { nbPerPage: 50, page: 0 },
        }
      );
      const schemaMatches = matchByName(
        schemasResp.getSchemas.data,
        schemaName,
        caseSensitive
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
      const tablesResp = await c.query<{ getTables: GetTablesOutput }>(
        GET_TABLES_SUMMARY,
        {
          scope: { schemaId: schema.id, nameContains: tableName },
          pagination: { nbPerPage: 50, page: 0 },
        }
      );
      const tableMatches = matchByName(
        tablesResp.getTables.data,
        tableName,
        caseSensitive
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

      // Step 4: column
      const colsResp = await c.query<{ getColumns: GetColumnsOutput }>(
        GET_COLUMNS_SUMMARY,
        {
          scope: { tableId: table.id, name: columnName },
          pagination: { nbPerPage: 50, page: 0 },
        }
      );
      const colMatches = matchByName(
        colsResp.getColumns.data,
        columnName,
        caseSensitive
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
