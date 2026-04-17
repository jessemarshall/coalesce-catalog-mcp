import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_DASHBOARDS_SUMMARY,
  GET_DASHBOARD_DETAIL,
} from "../catalog/operations.js";
import type {
  DashboardSorting,
  DashboardSortingKey,
  GetDashboardsOutput,
  GetDashboardsScope,
  Pagination,
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

const DashboardSortingKeySchema = z.enum([
  "name",
  "popularity",
  "ownersAndTeamOwnersCount",
]) satisfies z.ZodType<DashboardSortingKey>;

const SearchDashboardsInputShape = {
  nameContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the dashboard name."),
  folderPath: z
    .string()
    .optional()
    .describe(
      "Filter by the dashboard folder path within the source tool (format: 'root/folder1/folder2')."
    ),
  sourceId: z
    .string()
    .optional()
    .describe("Scope to dashboards from a single data source UUID (e.g. a Tableau/Looker connection)."),
  ids: z
    .array(z.string())
    .optional()
    .describe("Fetch specific dashboards by UUID. Prefer catalog_get_dashboard for one."),
  withDeleted: z
    .boolean()
    .optional()
    .describe("Include soft-deleted dashboards. Default: false."),
  sortBy: DashboardSortingKeySchema.optional().describe(
    "Sort key. Options: name, popularity, ownersAndTeamOwnersCount."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildDashboardSorting(
  sortBy: DashboardSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): DashboardSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: DashboardSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

function buildDashboardsScope(
  input: Record<string, unknown>
): GetDashboardsScope | undefined {
  const scope: GetDashboardsScope = {};
  if (typeof input.nameContains === "string") scope.nameContains = input.nameContains;
  if (typeof input.folderPath === "string") scope.folderPath = input.folderPath;
  if (typeof input.sourceId === "string") scope.sourceId = input.sourceId;
  if (Array.isArray(input.ids)) scope.ids = input.ids as string[];
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

export function defineDashboardTools(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_search_dashboards",
      config: {
        title: "Search Catalog Dashboards",
        description:
          "Find BI dashboards (Tableau, Looker, Mode, etc.) indexed in the Coalesce Catalog. Supports substring search on the dashboard name, folder-path scoping, and filtering to a specific source/BI tool. Returns a compact summary per match (id, name, type, url, folder, popularity, verification/deprecation state).\n\n" +
          "Use for: finding which dashboards exist for a given business area, discovering consumers of a table (combine with catalog_get_lineages childDashboardId), or sweeping for unverified dashboards. Use catalog_get_dashboard for full detail on a single row.",
        inputSchema: SearchDashboardsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildDashboardsScope(args),
          sorting: buildDashboardSorting(
            args.sortBy as DashboardSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getDashboards: GetDashboardsOutput }>(
          GET_DASHBOARDS_SUMMARY,
          variables
        );
        const out = data.getDashboards;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_get_dashboard",
      config: {
        title: "Get Catalog Dashboard Detail",
        description:
          "Fetch the full detail for a single dashboard by its Catalog UUID, including description provenance, ownership (users + teams), tags, folder location, external slug/URL, and verification/deprecation state. Returns null if no dashboard matches. For the source/BI-tool metadata, use the returned sourceId with a future catalog_get_source call.",
        inputSchema: {
          id: z.string().min(1).describe("Catalog UUID of the dashboard."),
        },
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const id = args.id as string;
        const data = await c.query<{ getDashboards: { data: unknown[] } }>(
          GET_DASHBOARD_DETAIL,
          { ids: [id] }
        );
        const row = data.getDashboards.data[0] ?? null;
        return { dashboard: row };
      }, client),
    },
  ];
}
