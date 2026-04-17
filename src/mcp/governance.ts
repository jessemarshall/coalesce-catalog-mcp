import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_USERS,
  GET_TEAMS,
  GET_DATA_QUALITIES,
  GET_PINNED_ASSETS,
} from "../catalog/operations.js";
import type {
  EntitiesLinkSorting,
  EntitiesLinkSortingKey,
  GetEntitiesLinkOutput,
  GetEntitiesLinksScope,
  GetQualityChecksOutput,
  GetQualityChecksScope,
  GetTeamsOutput,
  GetUsersOutput,
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

// ── Users ───────────────────────────────────────────────────────────────────

const SearchUsersInputShape = {
  ...PaginationInputShape,
};

// ── Teams ───────────────────────────────────────────────────────────────────

const SearchTeamsInputShape = {
  ...PaginationInputShape,
};

// ── Quality checks ──────────────────────────────────────────────────────────

const SearchQualityChecksInputShape = {
  tableId: z
    .string()
    .optional()
    .describe("Scope to quality checks on a single table UUID."),
  withDeleted: z.boolean().optional().describe("Include soft-deleted checks."),
  ...PaginationInputShape,
};

function buildQualityChecksScope(
  input: Record<string, unknown>
): GetQualityChecksScope | undefined {
  const scope: GetQualityChecksScope = {};
  if (typeof input.tableId === "string") scope.tableId = input.tableId;
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Pinned assets (entity links) ────────────────────────────────────────────

const EntitiesLinkSortingKeySchema = z.enum([
  "createdAt",
]) satisfies z.ZodType<EntitiesLinkSortingKey>;

const SearchPinnedAssetsInputShape = {
  fromTableId: z
    .string()
    .optional()
    .describe("Scope to links issued from this table UUID."),
  fromDashboardId: z
    .string()
    .optional()
    .describe("Scope to links issued from this dashboard UUID."),
  fromTermId: z
    .string()
    .optional()
    .describe("Scope to links issued from this term UUID."),
  fromTermIds: z
    .array(z.string())
    .optional()
    .describe("Scope to links issued from any of these term UUIDs."),
  toTableId: z
    .string()
    .optional()
    .describe("Scope to links pointing to this table UUID."),
  toDashboardId: z
    .string()
    .optional()
    .describe("Scope to links pointing to this dashboard UUID."),
  toDashboardFieldId: z
    .string()
    .optional()
    .describe("Scope to links pointing to this dashboard-field UUID."),
  toColumnsOfTableId: z
    .string()
    .optional()
    .describe("Scope to links pointing to any column on this table UUID."),
  toFieldsOfDashboardId: z
    .string()
    .optional()
    .describe("Scope to links pointing to any field on this dashboard UUID."),
  sortBy: EntitiesLinkSortingKeySchema.optional().describe(
    "Sort key. Only option: createdAt."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildPinnedAssetsScope(
  input: Record<string, unknown>
): GetEntitiesLinksScope | undefined {
  const scope: GetEntitiesLinksScope = {};
  const strFields = [
    "fromTableId",
    "fromDashboardId",
    "fromTermId",
    "toTableId",
    "toDashboardId",
    "toDashboardFieldId",
    "toColumnsOfTableId",
    "toFieldsOfDashboardId",
  ] as const;
  for (const k of strFields) {
    if (typeof input[k] === "string") (scope as Record<string, unknown>)[k] = input[k];
  }
  if (Array.isArray(input.fromTermIds)) scope.fromTermIds = input.fromTermIds as string[];
  return Object.keys(scope).length > 0 ? scope : undefined;
}

function buildPinnedAssetsSorting(
  sortBy: EntitiesLinkSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): EntitiesLinkSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: EntitiesLinkSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineGovernanceTools(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_search_users",
      config: {
        title: "List Catalog Users",
        description:
          "List Catalog users (humans). Returns identity (id, email, firstName, lastName), role, email-validation status, and ownedAssetIds (the UUIDs of every asset this user owns, useful for stewardship queries). The API returns a flat array — no totalCount/hasMore metadata.",
        inputSchema: SearchUsersInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const data = await c.query<{ getUsers: GetUsersOutput[] }>(
          GET_USERS,
          { pagination: pagination as Pagination }
        );
        return {
          pagination: {
            page: pagination.page,
            nbPerPage: pagination.nbPerPage,
            returned: data.getUsers.length,
          },
          data: data.getUsers,
        };
      }, client),
    },

    {
      name: "catalog_search_teams",
      config: {
        title: "List Catalog Teams",
        description:
          "List Catalog teams (groups). Returns identity (id, name, description, email), Slack routing (slackChannel, slackGroup), memberIds (user UUIDs), and ownedAssetIds. The API returns a flat array — no totalCount/hasMore metadata.",
        inputSchema: SearchTeamsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const data = await c.query<{ getTeams: GetTeamsOutput[] }>(
          GET_TEAMS,
          { pagination: pagination as Pagination }
        );
        return {
          pagination: {
            page: pagination.page,
            nbPerPage: pagination.nbPerPage,
            returned: data.getTeams.length,
          },
          data: data.getTeams,
        };
      }, client),
    },

    {
      name: "catalog_search_quality_checks",
      config: {
        title: "Search Data Quality Checks",
        description:
          "List data quality test results attached to tables or columns (e.g. dbt tests, Monte Carlo monitors, Soda checks, Great Expectations). Each record includes status (SUCCESS/WARNING/ALERT), result text (e.g. failure reason), externalId, source system, runAt timestamp, and tableId/columnId pointers.\n\n" +
          "Scope by tableId to inspect a specific table's test coverage. Use catalog_get_table (when you have the id) to find assets lacking tests, then pair with this tool to grade existing coverage.",
        inputSchema: SearchQualityChecksInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildQualityChecksScope(args),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getDataQualities: GetQualityChecksOutput }>(
          GET_DATA_QUALITIES,
          variables
        );
        const out = data.getDataQualities;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_search_pinned_assets",
      config: {
        title: "Search Pinned Asset Links",
        description:
          "List curated 'pinned asset' links between catalog entities — hand-authored relationships where a table/dashboard/term pins other tables, dashboards, columns, dashboard-fields, or terms as important context. Each record links one `from` asset to one `to` asset via ID pointers.\n\n" +
          "Scope by fromTableId/fromDashboardId/fromTermId (find what an asset pins) or by toTableId/toDashboardId/... (find who pins an asset). toColumnsOfTableId and toFieldsOfDashboardId are useful when you want any column/field on a parent asset. Records are ID-only; hydrate via the asset tools.",
        inputSchema: SearchPinnedAssetsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildPinnedAssetsScope(args),
          sorting: buildPinnedAssetsSorting(
            args.sortBy as EntitiesLinkSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getPinnedAssets: GetEntitiesLinkOutput }>(
          GET_PINNED_ASSETS,
          variables
        );
        const out = data.getPinnedAssets;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },
  ];
}
