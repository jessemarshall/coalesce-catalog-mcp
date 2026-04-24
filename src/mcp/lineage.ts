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
  GET_TABLES_SUMMARY,
  GET_DASHBOARDS_SUMMARY,
  GET_COLUMNS_SUMMARY,
} from "../catalog/operations.js";
import type {
  DeleteLineageInput,
  FieldLineage,
  GetColumnsOutput,
  GetDashboardsOutput,
  GetFieldLineagesOutput,
  GetFieldLineagesScope,
  GetLineagesOutput,
  GetLineagesScope,
  GetTablesOutput,
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
import { batchResult, listEnvelope, withErrorHandling } from "./tool-helpers.js";
import { withConfirmation } from "./confirmation.js";

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

// catalog_get_lineages returns edges between TABLE / DASHBOARD assets only —
// the underlying `Lineage` type carries only parent/child table + dashboard ids.
// Column / dashboard-field children live on `FieldLineage` via
// catalog_get_field_lineages. Narrowing the enum here turns a silent "filter
// accepted, zero rows returned" into an actionable schema rejection.
const AssetLevelLineageAssetTypeSchema = z.enum([
  "DASHBOARD",
  "TABLE",
]);

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
  withChildAssetType: AssetLevelLineageAssetTypeSchema.optional().describe(
    "Filter to edges whose child asset is of this type. Only TABLE and DASHBOARD are meaningful here — column and dashboard-field children live on field lineage, use catalog_get_field_lineages for those."
  ),
  withDeleted: z.boolean().optional().describe("Include soft-deleted edges. Default: false."),
  withHidden: z.boolean().optional().describe("Include hidden edges. Default: false."),
  sortBy: LineageSortingKeySchema.optional().describe(
    "Sort key. Options: id, popularity, type."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  hydrate: z
    .boolean()
    .optional()
    .describe(
      "When true, batch-resolves each edge's parent/child IDs to { id, name, kind } via one extra getTables + one extra getDashboards call. Use when presenting results to a user or doing multi-hop reasoning. Default: false (compact ID-only output)."
    ),
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
  hydrate: z
    .boolean()
    .optional()
    .describe(
      "When true, batch-resolves each edge's parent/child IDs to { id, name, kind } via one extra getColumns + one extra getDashboards call. Dashboard-field endpoints cannot be hydrated (no public API) and are surfaced with hydrationUnavailable: true. Default: false."
    ),
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

// ── Output enrichment: direction + ISO timestamps + endpoint hydration ──────

type Direction = "upstream" | "downstream" | "specific";

function toIso(millis: unknown): string | undefined {
  if (typeof millis !== "number" || !Number.isFinite(millis)) return undefined;
  try {
    return new Date(millis).toISOString();
  } catch {
    return undefined;
  }
}

function inferAssetDirection(input: Record<string, unknown>): Direction | undefined {
  const hasParent =
    typeof input.parentTableId === "string" ||
    typeof input.parentDashboardId === "string";
  const hasChild =
    typeof input.childTableId === "string" ||
    typeof input.childDashboardId === "string";
  if (hasParent && !hasChild) return "downstream";
  if (hasChild && !hasParent) return "upstream";
  if (hasParent && hasChild) return "specific";
  return undefined;
}

function inferFieldDirection(input: Record<string, unknown>): Direction | undefined {
  const hasParent =
    typeof input.parentColumnId === "string" ||
    typeof input.parentDashboardFieldId === "string";
  const hasChild =
    typeof input.childColumnId === "string" ||
    typeof input.childDashboardFieldId === "string" ||
    typeof input.childDashboardSourceId === "string" ||
    typeof input.childDashboardFieldSourceId === "string";
  if (hasParent && !hasChild) return "downstream";
  if (hasChild && !hasParent) return "upstream";
  if (hasParent && hasChild) return "specific";
  return undefined;
}

interface HydratedEndpoint {
  id: string;
  kind: "TABLE" | "DASHBOARD" | "COLUMN" | "DASHBOARD_FIELD";
  name?: string | null;
  /** For COLUMN: the parent tableId. For DASHBOARD_FIELD: null (no public endpoint). */
  parentId?: string | null;
  /** True when we attempted to hydrate but the endpoint doesn't exist in the public API (DASHBOARD_FIELD). */
  hydrationUnavailable?: boolean;
}

// A single 500-row page of lineage edges can reference 600–1000 distinct IDs
// (each edge contributes two endpoints). Fetch hydration in batches of this
// size so no ID past the first page is silently dropped to an unnamed fallback.
const HYDRATION_BATCH_SIZE = 500;

async function fetchHydrationBatches<T>(
  ids: string[],
  fetchBatch: (batch: string[]) => Promise<T[]>
): Promise<T[]> {
  const out: T[] = [];
  for (let i = 0; i < ids.length; i += HYDRATION_BATCH_SIZE) {
    const batch = ids.slice(i, i + HYDRATION_BATCH_SIZE);
    const rows = await fetchBatch(batch);
    out.push(...rows);
  }
  return out;
}

async function hydrateAssetLineages(
  client: CatalogClient,
  edges: Lineage[]
): Promise<Map<string, HydratedEndpoint>> {
  const tableIds = new Set<string>();
  const dashboardIds = new Set<string>();
  for (const e of edges) {
    if (e.parentTableId) tableIds.add(e.parentTableId);
    if (e.childTableId) tableIds.add(e.childTableId);
    if (e.parentDashboardId) dashboardIds.add(e.parentDashboardId);
    if (e.childDashboardId) dashboardIds.add(e.childDashboardId);
  }
  const map = new Map<string, HydratedEndpoint>();
  const tasks: Promise<unknown>[] = [];
  if (tableIds.size > 0) {
    tasks.push(
      fetchHydrationBatches([...tableIds], async (batch) => {
        const r = await client.execute<{ getTables: GetTablesOutput }>(
          GET_TABLES_SUMMARY,
          {
            scope: { ids: batch },
            pagination: { nbPerPage: batch.length, page: 0 },
          }
        );
        return r.getTables.data;
      }).then((rows) => {
        for (const t of rows) {
          map.set(t.id, { id: t.id, kind: "TABLE", name: t.name });
        }
      })
    );
  }
  if (dashboardIds.size > 0) {
    tasks.push(
      fetchHydrationBatches([...dashboardIds], async (batch) => {
        const r = await client.execute<{
          getDashboards: GetDashboardsOutput;
        }>(GET_DASHBOARDS_SUMMARY, {
          scope: { ids: batch },
          pagination: { nbPerPage: batch.length, page: 0 },
        });
        return r.getDashboards.data;
      }).then((rows) => {
        for (const d of rows) {
          map.set(d.id, { id: d.id, kind: "DASHBOARD", name: d.name });
        }
      })
    );
  }
  await Promise.all(tasks);
  return map;
}

async function hydrateFieldLineages(
  client: CatalogClient,
  edges: FieldLineage[]
): Promise<Map<string, HydratedEndpoint>> {
  const columnIds = new Set<string>();
  const dashboardIds = new Set<string>();
  const dashboardFieldIds = new Set<string>();
  for (const e of edges) {
    if (e.parentColumnId) columnIds.add(e.parentColumnId);
    if (e.childColumnId) columnIds.add(e.childColumnId);
    if (e.childDashboardId) dashboardIds.add(e.childDashboardId);
    if (e.parentDashboardFieldId)
      dashboardFieldIds.add(e.parentDashboardFieldId);
    if (e.childDashboardFieldId)
      dashboardFieldIds.add(e.childDashboardFieldId);
  }
  const map = new Map<string, HydratedEndpoint>();
  const tasks: Promise<unknown>[] = [];
  if (columnIds.size > 0) {
    tasks.push(
      fetchHydrationBatches([...columnIds], async (batch) => {
        const r = await client.execute<{ getColumns: GetColumnsOutput }>(
          GET_COLUMNS_SUMMARY,
          {
            scope: { ids: batch },
            pagination: { nbPerPage: batch.length, page: 0 },
          }
        );
        return r.getColumns.data;
      }).then((rows) => {
        for (const c of rows) {
          map.set(c.id, {
            id: c.id,
            kind: "COLUMN",
            name: c.name,
            parentId: c.tableId,
          });
        }
      })
    );
  }
  if (dashboardIds.size > 0) {
    tasks.push(
      fetchHydrationBatches([...dashboardIds], async (batch) => {
        const r = await client.execute<{
          getDashboards: GetDashboardsOutput;
        }>(GET_DASHBOARDS_SUMMARY, {
          scope: { ids: batch },
          pagination: { nbPerPage: batch.length, page: 0 },
        });
        return r.getDashboards.data;
      }).then((rows) => {
        for (const d of rows) {
          map.set(d.id, { id: d.id, kind: "DASHBOARD", name: d.name });
        }
      })
    );
  }
  // Dashboard fields can't be hydrated via the public API — there's no
  // getDashboardFields query. Record an unavailable placeholder so callers
  // see the shape is consistent.
  for (const id of dashboardFieldIds) {
    map.set(id, {
      id,
      kind: "DASHBOARD_FIELD",
      hydrationUnavailable: true,
    });
  }
  await Promise.all(tasks);
  return map;
}

function enrichAssetEdge(
  edge: Lineage,
  direction: Direction | undefined,
  hydrationMap: Map<string, HydratedEndpoint> | null
): Record<string, unknown> {
  const parentId = edge.parentTableId ?? edge.parentDashboardId ?? undefined;
  const parentKind: "TABLE" | "DASHBOARD" = edge.parentTableId ? "TABLE" : "DASHBOARD";
  const childId = edge.childTableId ?? edge.childDashboardId ?? undefined;
  const childKind: "TABLE" | "DASHBOARD" = edge.childTableId ? "TABLE" : "DASHBOARD";
  return {
    ...edge,
    ...(direction ? { direction } : {}),
    createdAtIso: toIso(edge.createdAt),
    refreshedAtIso: toIso(edge.refreshedAt),
    ...(hydrationMap && parentId
      ? { parent: hydrationMap.get(parentId) ?? { id: parentId, kind: parentKind } }
      : {}),
    ...(hydrationMap && childId
      ? { child: hydrationMap.get(childId) ?? { id: childId, kind: childKind } }
      : {}),
  };
}

function enrichFieldEdge(
  edge: FieldLineage,
  direction: Direction | undefined,
  hydrationMap: Map<string, HydratedEndpoint> | null
): Record<string, unknown> {
  const parentId =
    edge.parentColumnId ?? edge.parentDashboardFieldId ?? undefined;
  const parentKind: HydratedEndpoint["kind"] = edge.parentColumnId
    ? "COLUMN"
    : "DASHBOARD_FIELD";
  const childId =
    edge.childColumnId ??
    edge.childDashboardFieldId ??
    edge.childDashboardId ??
    undefined;
  const childKind: HydratedEndpoint["kind"] = edge.childColumnId
    ? "COLUMN"
    : edge.childDashboardFieldId
      ? "DASHBOARD_FIELD"
      : "DASHBOARD";
  return {
    ...edge,
    ...(direction ? { direction } : {}),
    createdAtIso: toIso(edge.createdAt),
    refreshedAtIso: toIso(edge.refreshedAt),
    ...(hydrationMap && parentId
      ? { parent: hydrationMap.get(parentId) ?? { id: parentId, kind: parentKind } }
      : {}),
    ...(hydrationMap && childId
      ? { child: hydrationMap.get(childId) ?? { id: childId, kind: childKind } }
      : {}),
  };
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
          "Each edge includes a derived `direction` field (upstream | downstream | specific) computed from the scope, and both epoch + ISO timestamps (`refreshedAtIso`, `createdAtIso`). Pass `hydrate: true` to enrich each row with `parent: { id, name, kind }` + `child: { id, name, kind }` — costs one extra batched call per direction but eliminates the N+1 pattern when the caller needs asset names.\n\n" +
          "**When presenting lineage to a user, render a compact ASCII tree** (see catalog://context/tool-routing for the expected shape) rather than dumping raw JSON — the edge list is structured for machine parsing, not human readability.\n\n" +
          "Edge provenance is exposed via lineageType: AUTOMATIC | MANUAL_CUSTOMER | MANUAL_OPS | OTHER_TECHNOS.",
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
        const data = await c.execute<{ getLineages: GetLineagesOutput }>(
          GET_LINEAGES,
          variables
        );
        const out = data.getLineages;
        const direction = inferAssetDirection(args);
        const hydrationMap = args.hydrate === true
          ? await hydrateAssetLineages(c, out.data)
          : null;
        const enriched = out.data.map((e) =>
          enrichAssetEdge(e, direction, hydrationMap)
        );
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, enriched);
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
          "This API requires at least one filter — an unscoped call will return a large, unpaginated payload or be rejected server-side.\n\n" +
          "Each edge includes a derived `direction` field + ISO timestamps (`refreshedAtIso`, `createdAtIso`). Pass `hydrate: true` to resolve column and dashboard endpoints to `{ id, name, kind, parentId? }`. Dashboard-field endpoints return `hydrationUnavailable: true` — the public API has no `getDashboardFields` query.\n\n" +
          "**When presenting field lineage to a user, render a compact ASCII tree** (see catalog://context/tool-routing) rather than dumping the raw JSON.",
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
        const data = await c.execute<{ getFieldLineages: GetFieldLineagesOutput }>(
          GET_FIELD_LINEAGES,
          variables
        );
        const out = data.getFieldLineages;
        const direction = inferFieldDirection(args);
        const hydrationMap = args.hydrate === true
          ? await hydrateFieldLineages(c, out.data)
          : null;
        const enriched = out.data.map((e) =>
          enrichFieldEdge(e, direction, hydrationMap)
        );
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, enriched);
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
        const data = await c.execute<{ upsertLineages: Lineage[] }>(
          UPSERT_LINEAGES,
          { data: input }
        );
        return batchResult("upserted", data.upsertLineages, input.length);
      }, client),
    },

    {
      name: "catalog_delete_lineages",
      config: {
        title: "Delete Asset Lineage Edges",
        description:
          "Delete lineage edges identified by their endpoints. Same shape as upsert: exactly one parent (parentTableId XOR parentDashboardId) and one child (childTableId XOR childDashboardId) per row. Irreversible — the edge is removed from Catalog.\n\n" +
          "Use to clean up incorrect automatic lineage or retire stale manual edges. Batches up to 500 per call; requires a READ_WRITE API token.\n\n" +
          "Returns a boolean success flag and `requestedCount` (echo of the input batch size — the API has no per-row result, so partial failures within the batch are not detectable).",
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
      handler: withErrorHandling(
        withConfirmation<{ data: DeleteLineageInput[] }>(
          {
            action: "Delete lineage edges",
            summarize: (a) => `Permanently delete ${a.data.length} lineage edge(s).`,
          },
          async (args, c) => {
            const input = args.data;
            const data = await c.execute<{ deleteLineages: boolean }>(
              DELETE_LINEAGES,
              { data: input }
            );
            return { success: data.deleteLineages, requestedCount: input.length };
          }
        ),
        client
      ),
    },
  ];
}
