import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_COLUMNS_SUMMARY,
  GET_COLUMN_DETAIL,
  GET_COLUMN_JOINS,
  UPDATE_COLUMNS_METADATA,
} from "../catalog/operations.js";
import type {
  Column,
  ColumnSorting,
  ColumnSortingKey,
  ColumnJoinSorting,
  ColumnJoinSortingKey,
  GetColumnsOutput,
  GetColumnsScope,
  GetColumnJoinsOutput,
  GetColumnJoinsScope,
  Pagination,
  UpdateColumnsMetadataInput,
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

// ── Column search ───────────────────────────────────────────────────────────

const ColumnSortingKeySchema = z.enum([
  "name",
  "sourceOrder",
  "tableName",
  "tablePopularity",
]) satisfies z.ZodType<ColumnSortingKey>;

const SearchColumnsInputShape = {
  nameContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the column name."),
  name: z
    .string()
    .optional()
    .describe("Exact column name match (case-sensitive)."),
  description: z
    .string()
    .optional()
    .describe("Scope by description content (substring match)."),
  tableId: z.string().optional().describe("Scope results to a single table UUID."),
  tableIds: z
    .array(z.string())
    .optional()
    .describe("Scope results to multiple tables by UUID."),
  schemaId: z.string().optional().describe("Scope to a single schema UUID."),
  schemaIds: z.array(z.string()).optional(),
  databaseId: z.string().optional().describe("Scope to a single database UUID."),
  databaseIds: z.array(z.string()).optional(),
  sourceId: z.string().optional(),
  sourceIds: z.array(z.string()).optional(),
  ids: z
    .array(z.string())
    .optional()
    .describe("Fetch specific columns by UUID. Prefer catalog_get_column for one."),
  isPii: z.boolean().optional().describe("Filter by PII flag."),
  isPrimaryKey: z.boolean().optional().describe("Filter by primary-key flag."),
  isDocumented: z
    .boolean()
    .optional()
    .describe("Filter by documentation status (Catalog or external)."),
  hasColumnJoins: z
    .boolean()
    .optional()
    .describe("Restrict to columns that participate in at least one observed join."),
  withDeleted: z.boolean().optional(),
  withHidden: z.boolean().optional(),
  sortBy: ColumnSortingKeySchema.optional().describe(
    "Sort key. Options: name, sourceOrder, tableName, tablePopularity."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildColumnSorting(
  sortBy: ColumnSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): ColumnSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: ColumnSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

function buildColumnsScope(
  input: Record<string, unknown>
): GetColumnsScope | undefined {
  const scope: GetColumnsScope = {};
  const strFields = [
    "nameContains",
    "name",
    "description",
    "tableId",
    "schemaId",
    "databaseId",
    "sourceId",
  ] as const;
  for (const k of strFields) {
    if (typeof input[k] === "string") (scope as Record<string, unknown>)[k] = input[k];
  }
  const arrFields = [
    "tableIds",
    "schemaIds",
    "databaseIds",
    "sourceIds",
    "ids",
  ] as const;
  for (const k of arrFields) {
    if (Array.isArray(input[k])) (scope as Record<string, unknown>)[k] = input[k];
  }
  const boolFields = [
    "isPii",
    "isPrimaryKey",
    "isDocumented",
    "hasColumnJoins",
    "withDeleted",
    "withHidden",
  ] as const;
  for (const k of boolFields) {
    if (typeof input[k] === "boolean") (scope as Record<string, unknown>)[k] = input[k];
  }
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Column joins ────────────────────────────────────────────────────────────

const ColumnJoinSortingKeySchema = z.enum([
  "count",
]) satisfies z.ZodType<ColumnJoinSortingKey>;

const GetColumnJoinsInputShape = {
  columnIds: z
    .array(z.string())
    .optional()
    .describe("Filter to joins that involve any of these column UUIDs."),
  tableIds: z
    .array(z.string())
    .optional()
    .describe("Filter to joins observed on any of these table UUIDs."),
  ids: z
    .array(z.string())
    .optional()
    .describe("Fetch specific column-join records by UUID."),
  withDeleted: z.boolean().optional(),
  withHidden: z.boolean().optional(),
  sortBy: ColumnJoinSortingKeySchema.optional().describe("Sort key. Only option: count."),
  sortDirection: SortDirectionSchema.optional(),
  ...PaginationInputShape,
};

function buildColumnJoinSorting(
  sortBy: ColumnJoinSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined
): ColumnJoinSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: ColumnJoinSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  return [entry];
}

// ── Predicate-only guard for search_columns ─────────────────────────────────
// The public API does not index the boolean predicates globally. Callers who
// pass a predicate (isPii/isPrimaryKey/isDocumented/hasColumnJoins) with no
// scoping filter trigger a full-table scan and time out after ~30s. We reject
// those calls up-front with an actionable message so the model can retry with
// a scope instead of hanging the session.
const PREDICATE_FIELDS = [
  "isPii",
  "isPrimaryKey",
  "isDocumented",
  "hasColumnJoins",
] as const;

const SCOPE_FIELDS = [
  "ids",
  "nameContains",
  "name",
  "description",
  "tableId",
  "tableIds",
  "schemaId",
  "schemaIds",
  "databaseId",
  "databaseIds",
  "sourceId",
  "sourceIds",
] as const;

function assertSearchColumnsHasScope(input: Record<string, unknown>): void {
  const hasPredicate = PREDICATE_FIELDS.some(
    (k) => typeof input[k] === "boolean"
  );
  if (!hasPredicate) return;
  const hasScope = SCOPE_FIELDS.some((k) => input[k] !== undefined);
  if (!hasScope) {
    throw new Error(
      "Unscoped predicate-only catalog_search_columns queries time out on the Catalog API (~30s). " +
        "Add at least one of: tableId / tableIds, schemaId / schemaIds, databaseId / databaseIds, " +
        "sourceId / sourceIds, ids, nameContains, name, or description — alongside your isPii / " +
        "isPrimaryKey / isDocumented / hasColumnJoins filter."
    );
  }
}

function buildColumnJoinsScope(
  input: Record<string, unknown>
): GetColumnJoinsScope | undefined {
  const scope: GetColumnJoinsScope = {};
  if (Array.isArray(input.columnIds)) scope.columnIds = input.columnIds as string[];
  if (Array.isArray(input.tableIds)) scope.tableIds = input.tableIds as string[];
  if (Array.isArray(input.ids)) scope.ids = input.ids as string[];
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  if (typeof input.withHidden === "boolean") scope.withHidden = input.withHidden;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineColumnTools(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_search_columns",
      config: {
        title: "Search Catalog Columns",
        description:
          "Find warehouse/BI columns indexed in the Coalesce Catalog. Supports substring search on name, filters by table/schema/database/source, and boolean predicates for PII/primary-key/documented/has-joins status. Returns a compact summary (id, name, type, description, flags) per match.\n\n" +
          "Use for: finding all columns on a table, sweeping for undocumented or PII-flagged columns across a schema, or verifying primary-key coverage. Use catalog_get_column for full detail on a single column.\n\n" +
          "Note: boolean predicates (isPii / isPrimaryKey / isDocumented / hasColumnJoins) are not globally indexed — always combine them with a scope filter (tableId, schemaId, databaseId, sourceId, or nameContains). The tool rejects predicate-only calls up-front rather than letting them time out.",
        inputSchema: SearchColumnsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        assertSearchColumnsHasScope(args);
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildColumnsScope(args),
          sorting: buildColumnSorting(
            args.sortBy as ColumnSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.execute<{ getColumns: GetColumnsOutput }>(
          GET_COLUMNS_SUMMARY,
          variables
        );
        const out = data.getColumns;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_get_column",
      config: {
        title: "Get Catalog Column Detail",
        description:
          "Fetch the full detail for a single column by its Catalog UUID. Returns description provenance (raw + external), tags, source references, the parent tableId, and flags (PII, primary-key, nullable). Returns null if no column matches. To hydrate the parent table, pass tableId into catalog_get_table.",
        inputSchema: {
          id: z.string().min(1).describe("Catalog UUID of the column."),
        },
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const id = args.id as string;
        const data = await c.execute<{ getColumns: { data: unknown[] } }>(
          GET_COLUMN_DETAIL,
          { ids: [id] }
        );
        const row = data.getColumns.data[0] ?? null;
        return { column: row };
      }, client),
    },

    {
      name: "catalog_get_column_joins",
      config: {
        title: "Get Observed Column Joins",
        description:
          "List warehouse-observed JOIN relationships between columns. Each record pairs two columns (firstColumn, secondColumn) with a `count` of how many times the join has been seen. Useful for discovering de-facto foreign-key relationships that may not be declared at the schema level.\n\n" +
          "Pass `columnIds` to find joins involving specific columns, or `tableIds` to find all joins observed on given tables. Sort by count DESC to surface the most common joins first.",
        inputSchema: GetColumnJoinsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildColumnJoinsScope(args),
          sorting: buildColumnJoinSorting(
            args.sortBy as ColumnJoinSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.execute<{ getColumnJoins: GetColumnJoinsOutput }>(
          GET_COLUMN_JOINS,
          variables
        );
        const out = data.getColumnJoins;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    // ── Mutations ──────────────────────────────────────────────────────────

    {
      name: "catalog_update_column_metadata",
      config: {
        title: "Update Column Metadata",
        description:
          "Update descriptive metadata on one or more columns. Supports both `descriptionRaw` (Catalog-native markdown documentation) and `externalDescription` (source-pushed documentation), plus the boolean `isPii` and `isPrimaryKey` flags. Accepts a batch (max 500 items per call); each item must include `id`.\n\n" +
          "Requires a READ_WRITE API token. Returns the updated rows with both description fields + flags resolved.",
        inputSchema: {
          data: z
            .array(
              z.object({
                id: z.string().min(1).describe("Catalog UUID of the column."),
                descriptionRaw: z
                  .string()
                  .optional()
                  .describe(
                    "Catalog-native user documentation (markdown). Takes precedence over externalDescription in the computed `description` field."
                  ),
                externalDescription: z
                  .string()
                  .optional()
                  .describe("Source-system description."),
                isPii: z
                  .boolean()
                  .optional()
                  .describe("Mark the column as personally identifiable information."),
                isPrimaryKey: z
                  .boolean()
                  .optional()
                  .describe("Mark the column as a primary key."),
              })
            )
            .min(1)
            .max(500)
            .describe("Batch of column metadata updates (max 500)."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as UpdateColumnsMetadataInput[];
        const data = await c.execute<{ updateColumnsMetadata: Column[] }>(
          UPDATE_COLUMNS_METADATA,
          { data: input }
        );
        return {
          updated: data.updateColumnsMetadata.length,
          data: data.updateColumnsMetadata,
        };
      }, client),
    },
  ];
}
