import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_LINEAGES,
  GET_FIELD_LINEAGES,
  UPSERT_LINEAGES,
  DELETE_LINEAGES,
} from "../catalog/operations.js";
import type {
  DeleteLineageInput,
  GetFieldLineagesOutput,
  GetFieldLineagesScope,
  GetLineagesOutput,
  GetLineagesScope,
  FieldLineageSorting,
  FieldLineageSortingKey,
  Lineage,
  LineageAssetType,
  LineageSorting,
  LineageSortingKey,
  LineageType,
  Pagination,
  UpsertLineageInput,
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

// ── Shared lineage enums ────────────────────────────────────────────────────

const LineageTypeSchema = z.enum([
  "AUTOMATIC",
  "MANUAL_CUSTOMER",
  "MANUAL_OPS",
  "OTHER_TECHNOS",
]) satisfies z.ZodType<LineageType>;

const LineageAssetTypeSchema = z.enum([
  "COLUMN",
  "DASHBOARD",
  "DASHBOARD_FIELD",
  "TABLE",
]) satisfies z.ZodType<LineageAssetType>;

// ── Asset-level lineage (tables + dashboards) ───────────────────────────────

const LineageSortingKeySchema = z.enum([
  "id",
  "popularity",
  "type",
]) satisfies z.ZodType<LineageSortingKey>;

const GetLineagesInputShape = {
  parentTableId: z
    .string()
    .optional()
    .describe("Limit to edges whose parent asset is this table UUID."),
  childTableId: z
    .string()
    .optional()
    .describe("Limit to edges whose child asset is this table UUID."),
  parentDashboardId: z
    .string()
    .optional()
    .describe("Limit to edges whose parent asset is this dashboard UUID."),
  childDashboardId: z
    .string()
    .optional()
    .describe("Limit to edges whose child asset is this dashboard UUID."),
  parentSourceId: z.string().optional().describe("Scope by parent source ID."),
  childSourceId: z.string().optional().describe("Scope by child source ID."),
  lineageIds: z
    .array(z.string())
    .optional()
    .describe("Fetch specific lineage records by UUID."),
  lineageType: LineageTypeSchema.optional().describe(
    "Filter by lineage origin: AUTOMATIC (inferred), MANUAL_CUSTOMER (via public API), MANUAL_OPS (ops team), OTHER_TECHNOS (imported)."
  ),
  withChildAssetType: LineageAssetTypeSchema.optional().describe(
    "Filter to edges whose child asset is of this type (COLUMN, DASHBOARD, DASHBOARD_FIELD, TABLE)."
  ),
  withDeleted: z.boolean().optional().describe("Include soft-deleted edges. Default: false."),
  withHidden: z.boolean().optional().describe("Include hidden edges. Default: false."),
  sortBy: LineageSortingKeySchema.optional().describe(
    "Sort key. Options: id, popularity, type."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildLineageSorting(
  sortBy: LineageSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): LineageSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: LineageSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

function buildLineagesScope(
  input: Record<string, unknown>
): GetLineagesScope | undefined {
  const scope: GetLineagesScope = {};
  if (typeof input.parentTableId === "string") scope.parentTableId = input.parentTableId;
  if (typeof input.childTableId === "string") scope.childTableId = input.childTableId;
  if (typeof input.parentDashboardId === "string") scope.parentDashboardId = input.parentDashboardId;
  if (typeof input.childDashboardId === "string") scope.childDashboardId = input.childDashboardId;
  if (typeof input.parentSourceId === "string") scope.parentSourceId = input.parentSourceId;
  if (typeof input.childSourceId === "string") scope.childSourceId = input.childSourceId;
  if (Array.isArray(input.lineageIds)) scope.lineageIds = input.lineageIds as string[];
  if (typeof input.lineageType === "string") scope.lineageType = input.lineageType as LineageType;
  if (typeof input.withChildAssetType === "string") {
    scope.withChildAssetType = input.withChildAssetType as LineageAssetType;
  }
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  if (typeof input.withHidden === "boolean") scope.withHidden = input.withHidden;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Field-level lineage (columns + dashboard fields) ────────────────────────

const FieldLineageSortingKeySchema = z.enum([
  "id",
  "popularity",
  "type",
  "childDashboardPopularity",
]) satisfies z.ZodType<FieldLineageSortingKey>;

const GetFieldLineagesInputShape = {
  parentColumnId: z
    .string()
    .optional()
    .describe("Limit to edges whose parent is this column UUID."),
  childColumnId: z
    .string()
    .optional()
    .describe("Limit to edges whose child is this column UUID."),
  parentDashboardFieldId: z
    .string()
    .optional()
    .describe("Limit to edges whose parent is this dashboard-field UUID."),
  childDashboardFieldId: z
    .string()
    .optional()
    .describe("Limit to edges whose child is this dashboard-field UUID."),
  childDashboardFieldSourceId: z
    .string()
    .optional()
    .describe("Scope by child dashboard-field source ID."),
  childDashboardSourceId: z
    .string()
    .optional()
    .describe("Scope by child dashboard source ID."),
  columnSourceId: z
    .string()
    .optional()
    .describe("Scope by column source (PARENT or CHILD) ID."),
  hasDashboardChild: z
    .boolean()
    .optional()
    .describe("Restrict to edges whose child is a dashboard/dashboard-field."),
  lineageType: LineageTypeSchema.optional(),
  withChildAssetType: LineageAssetTypeSchema.optional(),
  sortBy: FieldLineageSortingKeySchema.optional().describe(
    "Sort key. Options: id, popularity, type, childDashboardPopularity."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildFieldLineageSorting(
  sortBy: FieldLineageSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): FieldLineageSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: FieldLineageSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

function buildFieldLineagesScope(
  input: Record<string, unknown>
): GetFieldLineagesScope {
  const scope: GetFieldLineagesScope = {};
  if (typeof input.parentColumnId === "string") scope.parentColumnId = input.parentColumnId;
  if (typeof input.childColumnId === "string") scope.childColumnId = input.childColumnId;
  if (typeof input.parentDashboardFieldId === "string") {
    scope.parentDashboardFieldId = input.parentDashboardFieldId;
  }
  if (typeof input.childDashboardFieldId === "string") {
    scope.childDashboardFieldId = input.childDashboardFieldId;
  }
  if (typeof input.childDashboardFieldSourceId === "string") {
    scope.childDashboardFieldSourceId = input.childDashboardFieldSourceId;
  }
  if (typeof input.childDashboardSourceId === "string") {
    scope.childDashboardSourceId = input.childDashboardSourceId;
  }
  if (typeof input.columnSourceId === "string") scope.columnSourceId = input.columnSourceId;
  if (typeof input.hasDashboardChild === "boolean") {
    scope.hasDashboardChild = input.hasDashboardChild;
  }
  if (typeof input.lineageType === "string") scope.lineageType = input.lineageType as LineageType;
  if (typeof input.withChildAssetType === "string") {
    scope.withChildAssetType = input.withChildAssetType as LineageAssetType;
  }
  return scope;
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineLineageTools(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_get_lineages",
      config: {
        title: "Get Asset Lineage",
        description:
          "List lineage edges between table and dashboard assets. Each row links exactly one parent (parentTableId OR parentDashboardId) to one child (childTableId OR childDashboardId).\n\n" +
          "Common patterns:\n" +
          "  - Downstream of a table: { parentTableId: '<uuid>' }\n" +
          "  - Upstream of a table:   { childTableId:  '<uuid>' }\n" +
          "  - Table→dashboard edges: { parentTableId, withChildAssetType: 'DASHBOARD' }\n\n" +
          "Returns edge records (not resolved asset names). Pair with catalog_get_table / catalog_get_dashboard (when available) to hydrate the endpoints. Edge provenance is exposed via lineageType: AUTOMATIC | MANUAL_CUSTOMER | MANUAL_OPS | OTHER_TECHNOS.",
        inputSchema: GetLineagesInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildLineagesScope(args),
          sorting: buildLineageSorting(
            args.sortBy as LineageSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getLineages: GetLineagesOutput }>(
          GET_LINEAGES,
          variables
        );
        const out = data.getLineages;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_get_field_lineages",
      config: {
        title: "Get Field (Column) Lineage",
        description:
          "List column-level and dashboard-field-level lineage edges. Each row links exactly one parent (column or dashboard-field) to one child.\n\n" +
          "Common patterns:\n" +
          "  - Downstream of a column:      { parentColumnId: '<uuid>' }\n" +
          "  - Upstream of a column:        { childColumnId:  '<uuid>' }\n" +
          "  - Columns feeding a dashboard: { hasDashboardChild: true, childDashboardId from getLineages }\n\n" +
          "This API requires at least one filter — an unscoped call will return a large, unpaginated payload or be rejected server-side. Edge records are intentionally slim (IDs + type); hydrate via catalog_get_column (when available) for names.",
        inputSchema: GetFieldLineagesInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildFieldLineagesScope(args),
          sorting: buildFieldLineageSorting(
            args.sortBy as FieldLineageSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getFieldLineages: GetFieldLineagesOutput }>(
          GET_FIELD_LINEAGES,
          variables
        );
        const out = data.getFieldLineages;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    // ── Mutations ──────────────────────────────────────────────────────────

    {
      name: "catalog_upsert_lineages",
      config: {
        title: "Upsert Asset Lineage Edges",
        description:
          "Create or update lineage edges between tables and/or dashboards. Each edge must specify exactly one parent (parentTableId OR parentDashboardId) and exactly one child (childTableId OR childDashboardId). Upserting an existing edge is a no-op.\n\n" +
          "Edges created through this API register as MANUAL_CUSTOMER lineage type. Use to patch gaps where automatic detection missed a dependency. Batches up to 500 per call; requires a READ_WRITE API token.",
        inputSchema: {
          data: z
            .array(
              z
                .object({
                  parentTableId: z.string().optional(),
                  parentDashboardId: z.string().optional(),
                  childTableId: z.string().optional(),
                  childDashboardId: z.string().optional(),
                })
                .refine(
                  (v) =>
                    Number(!!v.parentTableId) + Number(!!v.parentDashboardId) === 1 &&
                    Number(!!v.childTableId) + Number(!!v.childDashboardId) === 1,
                  {
                    message:
                      "Each edge requires exactly one parent (parentTableId XOR parentDashboardId) and exactly one child (childTableId XOR childDashboardId).",
                  }
                )
            )
            .min(1)
            .max(500)
            .describe("Batch of lineage edges to upsert (max 500)."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as UpsertLineageInput[];
        const data = await c.query<{ upsertLineages: Lineage[] }>(
          UPSERT_LINEAGES,
          { data: input }
        );
        return { upserted: data.upsertLineages.length, data: data.upsertLineages };
      }, client),
    },

    {
      name: "catalog_delete_lineages",
      config: {
        title: "Delete Asset Lineage Edges",
        description:
          "Delete lineage edges identified by their endpoints. Same shape as upsert: exactly one parent (parentTableId XOR parentDashboardId) and one child (childTableId XOR childDashboardId) per row. Irreversible — the edge is removed from Catalog.\n\n" +
          "Use to clean up incorrect automatic lineage or retire stale manual edges. Batches up to 500 per call; requires a READ_WRITE API token.",
        inputSchema: {
          data: z
            .array(
              z
                .object({
                  parentTableId: z.string().optional(),
                  parentDashboardId: z.string().optional(),
                  childTableId: z.string().optional(),
                  childDashboardId: z.string().optional(),
                })
                .refine(
                  (v) =>
                    Number(!!v.parentTableId) + Number(!!v.parentDashboardId) === 1 &&
                    Number(!!v.childTableId) + Number(!!v.childDashboardId) === 1,
                  {
                    message:
                      "Each edge requires exactly one parent (parentTableId XOR parentDashboardId) and exactly one child (childTableId XOR childDashboardId).",
                  }
                )
            )
            .min(1)
            .max(500)
            .describe("Batch of lineage edges to delete (max 500)."),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as DeleteLineageInput[];
        const data = await c.query<{ deleteLineages: boolean }>(
          DELETE_LINEAGES,
          { data: input }
        );
        return { success: data.deleteLineages, deleted: input.length };
      }, client),
    },
  ];
}
