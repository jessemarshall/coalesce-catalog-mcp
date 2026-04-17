import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLES_SUMMARY,
  GET_TABLE_DETAIL,
  GET_TABLE_QUERIES,
  UPDATE_TABLES,
} from "../catalog/operations.js";
import type {
  GetTablesOutput,
  GetTableQueriesOutput,
  Pagination,
  GetTablesScope,
  GetTableQueriesScope,
  TableSorting,
  QuerySorting,
  TableSortingKey,
  QuerySortingKey,
  QueryType,
  SearchArrayFilterMode,
  Table,
  TableType,
  UpdateTableInput,
} from "../generated/types.js";
import {
  PaginationInputShape,
  toGraphQLPagination,
  type PaginationInput,
} from "../schemas/pagination.js";
import {
  NullsPrioritySchema,
  SortDirectionSchema,
} from "../schemas/sorting.js";
import { listEnvelope, withErrorHandling } from "./tool-helpers.js";

// ── Table search ────────────────────────────────────────────────────────────

const TableSortingKeySchema = z.enum([
  "name",
  "nameLength",
  "schemaName",
  "popularity",
  "levelOfCompletion",
  "ownersAndTeamOwnersCount",
]) satisfies z.ZodType<TableSortingKey>;

const SearchTablesInputShape = {
  nameContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the table name."),
  pathContains: z
    .string()
    .optional()
    .describe(
      "Case-insensitive substring match against the fully-qualified table path (e.g. 'PROD_DB.ANALYTICS.')."
    ),
  databaseId: z
    .string()
    .optional()
    .describe("Scope results to a single database by UUID."),
  schemaId: z
    .string()
    .optional()
    .describe("Scope results to a single schema by UUID."),
  warehouseId: z
    .string()
    .optional()
    .describe("Scope results to a single warehouse by UUID."),
  ids: z
    .array(z.string())
    .optional()
    .describe("Fetch specific tables by UUID. Prefer catalog_get_table for a single lookup."),
  withDeleted: z
    .boolean()
    .optional()
    .describe("Include soft-deleted tables in results. Default: false."),
  withHidden: z
    .boolean()
    .optional()
    .describe("Include hidden tables in results. Default: false."),
  sortBy: TableSortingKeySchema.optional().describe(
    "Sort key. Options: name, nameLength, schemaName, popularity, levelOfCompletion, ownersAndTeamOwnersCount."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildTableSorting(
  sortBy: TableSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): TableSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: TableSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

function buildTablesScope(
  input: Record<string, unknown>
): GetTablesScope | undefined {
  const scope: GetTablesScope = {};
  if (typeof input.nameContains === "string") scope.nameContains = input.nameContains;
  if (typeof input.pathContains === "string") scope.pathContains = input.pathContains;
  if (typeof input.databaseId === "string") scope.databaseId = input.databaseId;
  if (typeof input.schemaId === "string") scope.schemaId = input.schemaId;
  if (typeof input.warehouseId === "string") scope.warehouseId = input.warehouseId;
  if (Array.isArray(input.ids)) scope.ids = input.ids as string[];
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  if (typeof input.withHidden === "boolean") scope.withHidden = input.withHidden;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Table queries (SQL usage on a table) ────────────────────────────────────

const QuerySortingKeySchema = z.enum([
  "hash",
  "queryType",
  "timestamp",
]) satisfies z.ZodType<QuerySortingKey>;

const QueryTypeSchema = z.enum(["SELECT", "WRITE"]) satisfies z.ZodType<QueryType>;

const FilterModeSchema = z.enum([
  "ALL",
  "ANY",
]) satisfies z.ZodType<SearchArrayFilterMode>;

const GetTableQueriesInputShape = {
  tableIds: z
    .array(z.string())
    .min(1)
    .max(50)
    .describe("Required. Up to 50 table UUIDs whose SQL usage you want to inspect."),
  tableIdsFilterMode: FilterModeSchema.optional().describe(
    "ALL = query must touch every table in tableIds; ANY = query must touch at least one. Default: ALL."
  ),
  queryType: QueryTypeSchema.optional().describe(
    "Restrict to SELECT (reads) or WRITE (inserts/updates/etc.)."
  ),
  databaseId: z.string().optional(),
  schemaId: z.string().optional(),
  warehouseId: z.string().optional(),
  sortBy: QuerySortingKeySchema.optional().describe(
    "Sort key. Options: hash, queryType, timestamp."
  ),
  sortDirection: SortDirectionSchema.optional(),
  ...PaginationInputShape,
};

function buildQuerySorting(
  sortBy: QuerySortingKey | undefined,
  direction: "ASC" | "DESC" | undefined
): QuerySorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: QuerySorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  return [entry];
}

function buildTableQueriesScope(
  input: Record<string, unknown>
): GetTableQueriesScope {
  const scope: GetTableQueriesScope = {
    tableIds: input.tableIds as string[],
  };
  if (typeof input.tableIdsFilterMode === "string") {
    scope.tableIdsFilterMode = input.tableIdsFilterMode as SearchArrayFilterMode;
  }
  if (typeof input.queryType === "string") {
    scope.queryType = input.queryType as QueryType;
  }
  if (typeof input.databaseId === "string") scope.databaseId = input.databaseId;
  if (typeof input.schemaId === "string") scope.schemaId = input.schemaId;
  if (typeof input.warehouseId === "string") scope.warehouseId = input.warehouseId;
  return scope;
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineTableTools(client: CatalogClient): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_search_tables",
      config: {
        title: "Search Catalog Tables",
        description:
          "Find warehouse/BI tables indexed in the Coalesce Catalog. Supports substring search on name or path, plus scoping by database/schema/warehouse. Returns a compact summary for each match (id, name, description, type, popularity, freshness). Use catalog_get_table for full detail on one row.\n\n" +
          "Default page size: 100, max: 500. Sorting is optional; omit for API default order.",
        inputSchema: SearchTablesInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildTablesScope(args),
          sorting: buildTableSorting(
            args.sortBy as TableSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getTables: GetTablesOutput }>(
          GET_TABLES_SUMMARY,
          variables
        );
        const out = data.getTables;
        return listEnvelope(
          out.page ?? 0,
          out.nbPerPage,
          out.totalCount,
          out.data
        );
      }, client),
    },

    {
      name: "catalog_get_table",
      config: {
        title: "Get Catalog Table Detail",
        description:
          "Fetch the full detail for a single table by its Catalog UUID, including description (both custom and external), ownership (users + teams), tags, external links, schema/database context, freshness, popularity, and verification/deprecation state. Returns null if no table matches.",
        inputSchema: {
          id: z
            .string()
            .min(1)
            .describe("Catalog UUID of the table."),
        },
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const id = args.id as string;
        const data = await c.query<{ getTables: { data: unknown[] } }>(
          GET_TABLE_DETAIL,
          { ids: [id] }
        );
        const row = data.getTables.data[0] ?? null;
        return { table: row };
      }, client),
    },

    {
      name: "catalog_get_table_queries",
      config: {
        title: "Get SQL Queries For Tables",
        description:
          "Retrieve the SQL queries that have touched one or more tables (up to 50 at a time). Useful for impact analysis, usage debugging, or understanding how a table is consumed. Each result includes the query text, author, query type (SELECT/WRITE), warehouse, duration, row count, and the set of tables it referenced.\n\n" +
          "Set tableIdsFilterMode=ANY to find queries that touch any of the listed tables (default: ALL = queries must touch every listed table).",
        inputSchema: GetTableQueriesInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildTableQueriesScope(args),
          sorting: buildQuerySorting(
            args.sortBy as QuerySortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getTableQueries: GetTableQueriesOutput }>(
          GET_TABLE_QUERIES,
          variables
        );
        const out = data.getTableQueries;
        return listEnvelope(
          out.page ?? 0,
          out.nbPerPage,
          out.totalCount,
          out.data
        );
      }, client),
    },

    // ── Mutations ──────────────────────────────────────────────────────────

    {
      name: "catalog_update_table_metadata",
      config: {
        title: "Update Table Metadata",
        description:
          "Update one or more tables in the Catalog. Accepts a batch (max 500 items per call); each item must include `id` plus the fields to change. Only human-editable metadata is exposed — table identity/schema bindings are not editable through this MCP (use the warehouse ingestion flow).\n\n" +
          "Requires a READ_WRITE API token. Returns the updated rows.",
        inputSchema: {
          data: z
            .array(
              z.object({
                id: z.string().min(1).describe("Catalog UUID of the table."),
                name: z.string().optional().describe("Override the table name."),
                externalDescription: z
                  .string()
                  .optional()
                  .describe(
                    "Source-style description (surfaces as `description` when no Catalog-native description is set)."
                  ),
                tableType: z
                  .enum(["TABLE", "VIEW", "EXTERNAL", "DYNAMIC_TABLE", "TOPIC"])
                  .optional()
                  .describe("Set the table's type."),
                url: z.string().url().optional().describe("External URL for the table."),
                externalId: z
                  .string()
                  .optional()
                  .describe("Technical identifier from the warehouse."),
              })
            )
            .min(1)
            .max(500)
            .describe("Batch of table updates (max 500)."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as Array<UpdateTableInput & { tableType?: TableType }>;
        const data = await c.query<{ updateTables: Table[] }>(UPDATE_TABLES, {
          data: input,
        });
        return { updated: data.updateTables.length, data: data.updateTables };
      }, client),
    },
  ];
}
