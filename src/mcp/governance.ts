import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_USERS,
  GET_TEAMS,
  GET_DATA_QUALITIES,
  GET_PINNED_ASSETS,
  CREATE_EXTERNAL_LINKS,
  UPDATE_EXTERNAL_LINKS,
  DELETE_EXTERNAL_LINKS,
  UPSERT_DATA_QUALITIES,
  REMOVE_DATA_QUALITIES,
  UPSERT_USER_OWNERS,
  REMOVE_USER_OWNERS,
  UPSERT_TEAM_OWNERS,
  REMOVE_TEAM_OWNERS,
  UPSERT_TEAM,
  ADD_TEAM_USERS,
  REMOVE_TEAM_USERS,
  UPSERT_PINNED_ASSETS,
  REMOVE_PINNED_ASSETS,
} from "../catalog/operations.js";
import type {
  CreateExternalLinkInput,
  DeleteExternalLinkInput,
  EntitiesLink,
  EntitiesLinkInput,
  EntitiesLinkSorting,
  EntitiesLinkSortingKey,
  EntitiesLinkTargetType,
  EntityTarget,
  EntityTargetType,
  ExternalLink,
  ExternalLinkTechnology,
  GetEntitiesLinkOutput,
  GetEntitiesLinksScope,
  GetQualityChecksOutput,
  GetQualityChecksScope,
  GetTeamsOutput,
  GetUsersOutput,
  OwnerEntity,
  OwnerInput,
  Pagination,
  QualityCheck,
  QualityStatus,
  Team,
  TeamOwnerEntity,
  TeamOwnerInput,
  TeamUsersInput,
  UpdateExternalLinkInput,
  UpsertQualityChecksInput,
  UpsertTeamInput,
  RemoveQualityChecksInput,
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

// ── Payload shaping for search_users / search_teams ─────────────────────────
// The public API inlines ownedAssetIds (and memberIds for teams) as
// unbounded UUID arrays on every user/team record. A single ADMIN user in a
// mature workspace can return ~500 IDs, inflating list payloads to 20 KB per
// row. We strip the arrays in list queries and expose them via dedicated
// paginated lookup tools instead.

type TrimmedUser = Omit<GetUsersOutput, "ownedAssetIds"> & {
  ownedAssetCount: number;
};

type TrimmedTeam = Omit<GetTeamsOutput, "memberIds" | "ownedAssetIds"> & {
  memberCount: number;
  ownedAssetCount: number;
};

function stripUserOwnedAssets(user: GetUsersOutput): TrimmedUser {
  const { ownedAssetIds, ...rest } = user;
  return { ...rest, ownedAssetCount: ownedAssetIds?.length ?? 0 };
}

function stripTeamArrays(team: GetTeamsOutput): TrimmedTeam {
  const { memberIds, ownedAssetIds, ...rest } = team;
  return {
    ...rest,
    memberCount: memberIds?.length ?? 0,
    ownedAssetCount: ownedAssetIds?.length ?? 0,
  };
}

// ── Find-by-id helpers for the dedicated lookup tools ───────────────────────
// The public API has no user-by-id or team-by-id endpoint. The dedicated
// lookup tools (catalog_get_user_owned_assets etc.) iterate pages until the
// target is found or the cap is hit.
const LOOKUP_PAGE_SIZE = 500;
const LOOKUP_MAX_PAGES = 20;

async function findUserById(
  client: CatalogClient,
  userId: string
): Promise<GetUsersOutput | null> {
  for (let page = 0; page < LOOKUP_MAX_PAGES; page++) {
    const data = await client.execute<{ getUsers: GetUsersOutput[] }>(
      GET_USERS,
      { pagination: { nbPerPage: LOOKUP_PAGE_SIZE, page } }
    );
    const match = data.getUsers.find((u) => u.id === userId);
    if (match) return match;
    if (data.getUsers.length < LOOKUP_PAGE_SIZE) return null;
  }
  return null;
}

async function findTeamById(
  client: CatalogClient,
  teamId: string
): Promise<GetTeamsOutput | null> {
  for (let page = 0; page < LOOKUP_MAX_PAGES; page++) {
    const data = await client.execute<{ getTeams: GetTeamsOutput[] }>(
      GET_TEAMS,
      { pagination: { nbPerPage: LOOKUP_PAGE_SIZE, page } }
    );
    const match = data.getTeams.find((t) => t.id === teamId);
    if (match) return match;
    if (data.getTeams.length < LOOKUP_PAGE_SIZE) return null;
  }
  return null;
}

function sliceAssetIds(
  ids: readonly string[] | undefined,
  page: number,
  nbPerPage: number
): {
  pagination: {
    page: number;
    nbPerPage: number;
    totalCount: number;
    hasMore: boolean;
  };
  data: string[];
} {
  const all = ids ?? [];
  const start = page * nbPerPage;
  const slice = all.slice(start, start + nbPerPage);
  return {
    pagination: {
      page,
      nbPerPage,
      totalCount: all.length,
      hasMore: start + slice.length < all.length,
    },
    data: slice,
  };
}

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
          "List Catalog users (humans). Returns identity (id, email, firstName, lastName), role, email-validation status, and `ownedAssetCount` — a scalar count of assets this user owns.\n\n" +
          "For the full list of owned asset UUIDs, call catalog_get_user_owned_assets(userId). The count was substituted for the raw UUID array to keep list responses small on accounts with senior admins who own hundreds of assets — a ~500-asset ADMIN could otherwise inflate a single user row to 20 KB.",
        inputSchema: SearchUsersInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const data = await c.execute<{ getUsers: GetUsersOutput[] }>(
          GET_USERS,
          { pagination: pagination as Pagination }
        );
        return listEnvelope(
          pagination.page,
          pagination.nbPerPage,
          null,
          data.getUsers.map(stripUserOwnedAssets)
        );
      }, client),
    },

    {
      name: "catalog_search_teams",
      config: {
        title: "List Catalog Teams",
        description:
          "List Catalog teams (groups). Returns identity (id, name, description, email), Slack routing (slackChannel, slackGroup), `memberCount`, and `ownedAssetCount`.\n\n" +
          "For the full list of member UUIDs, call catalog_get_team_members(teamId). For the full list of owned asset UUIDs, call catalog_get_team_owned_assets(teamId). The counts were substituted for the raw UUID arrays to keep list responses small — a team of 30 people owning 200 assets would otherwise inflate a single team row.",
        inputSchema: SearchTeamsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const data = await c.execute<{ getTeams: GetTeamsOutput[] }>(
          GET_TEAMS,
          { pagination: pagination as Pagination }
        );
        return listEnvelope(
          pagination.page,
          pagination.nbPerPage,
          null,
          data.getTeams.map(stripTeamArrays)
        );
      }, client),
    },

    {
      name: "catalog_get_user_owned_assets",
      config: {
        title: "Get Paginated Owned Assets For a User",
        description:
          "Return the paginated list of asset UUIDs owned by a given user. Pair with catalog_search_users (which only exposes `ownedAssetCount`) when you need the full list — e.g. 'list every table Zach owns'.\n\n" +
          "Implementation note: the public API has no direct user-by-id endpoint, so this tool iterates `getUsers` pages of 500 until it finds the target. The cap is 10k users (20 pages); beyond that it returns notFound with a hint. The returned IDs are heterogeneous (tables, dashboards, terms) — hydrate via catalog_get_table / catalog_get_dashboard / catalog_search_terms{ids} as needed.",
        inputSchema: {
          userId: z.string().min(1).describe("Catalog UUID of the user."),
          ...PaginationInputShape,
        },
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const user = await findUserById(c, args.userId as string);
        if (!user) {
          return {
            notFound: true,
            userId: args.userId,
            reason:
              "User not found within the first 10,000 accounts scanned. The API has no per-id lookup, so this tool may miss very large tenants — narrow via catalog_search_users if needed.",
          };
        }
        return sliceAssetIds(
          user.ownedAssetIds,
          pagination.page,
          pagination.nbPerPage
        );
      }, client),
    },

    {
      name: "catalog_get_team_members",
      config: {
        title: "Get Paginated Members For a Team",
        description:
          "Return the paginated list of user UUIDs belonging to a team. Pair with catalog_search_teams (which only exposes `memberCount`) when you need the full membership.\n\n" +
          "Same iteration strategy as catalog_get_user_owned_assets — scans up to 10k teams.",
        inputSchema: {
          teamId: z.string().min(1).describe("Catalog UUID of the team."),
          ...PaginationInputShape,
        },
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const team = await findTeamById(c, args.teamId as string);
        if (!team) {
          return {
            notFound: true,
            teamId: args.teamId,
            reason:
              "Team not found within the first 10,000 accounts scanned.",
          };
        }
        return sliceAssetIds(
          team.memberIds,
          pagination.page,
          pagination.nbPerPage
        );
      }, client),
    },

    {
      name: "catalog_get_team_owned_assets",
      config: {
        title: "Get Paginated Owned Assets For a Team",
        description:
          "Return the paginated list of asset UUIDs owned by a given team. Pair with catalog_search_teams (which only exposes `ownedAssetCount`). Same iteration strategy as catalog_get_user_owned_assets.",
        inputSchema: {
          teamId: z.string().min(1).describe("Catalog UUID of the team."),
          ...PaginationInputShape,
        },
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const team = await findTeamById(c, args.teamId as string);
        if (!team) {
          return {
            notFound: true,
            teamId: args.teamId,
            reason:
              "Team not found within the first 10,000 accounts scanned.",
          };
        }
        return sliceAssetIds(
          team.ownedAssetIds,
          pagination.page,
          pagination.nbPerPage
        );
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
        const data = await c.execute<{ getDataQualities: GetQualityChecksOutput }>(
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
        const data = await c.execute<{ getPinnedAssets: GetEntitiesLinkOutput }>(
          GET_PINNED_ASSETS,
          variables
        );
        const out = data.getPinnedAssets;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    // ── External links mutations ──────────────────────────────────────────

    {
      name: "catalog_create_external_links",
      config: {
        title: "Create External Links on Tables",
        description:
          "Attach external URLs (runbooks, repo links, workflow runs, etc.) to tables. Each input row specifies a tableId, a technology (GITHUB/GITLAB/AIRFLOW/OTHER), and a url. Useful for linking owned tables to their source SQL, runbooks, or orchestration runs.\n\n" +
          "Batches up to 500; requires READ_WRITE token. Returns the created links with their generated ids.",
        inputSchema: {
          data: z
            .array(
              z.object({
                tableId: z.string().min(1).describe("Catalog UUID of the table."),
                technology: z
                  .enum(["AIRFLOW", "GITHUB", "GITLAB", "OTHER"])
                  .describe("Where the link points."),
                url: z.string().url().describe("The external URL."),
              })
            )
            .min(1)
            .max(500),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as Array<{
          tableId: string;
          technology: ExternalLinkTechnology;
          url: string;
        }>;
        const data = await c.execute<{ createExternalLinks: ExternalLink[] }>(
          CREATE_EXTERNAL_LINKS,
          { data: input satisfies CreateExternalLinkInput[] }
        );
        return { created: data.createExternalLinks.length, data: data.createExternalLinks };
      }, client),
    },

    {
      name: "catalog_update_external_links",
      config: {
        title: "Update External Link URLs",
        description:
          "Update the URL of an existing external link. Identity + technology stay the same; only `url` is editable. Batches up to 500; requires READ_WRITE token.",
        inputSchema: {
          data: z
            .array(
              z.object({
                id: z.string().min(1).describe("Catalog UUID of the external link."),
                url: z.string().url().optional().describe("New URL (omit to leave unchanged)."),
              })
            )
            .min(1)
            .max(500),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as UpdateExternalLinkInput[];
        const data = await c.execute<{ updateExternalLinks: ExternalLink[] }>(
          UPDATE_EXTERNAL_LINKS,
          { data: input }
        );
        return { updated: data.updateExternalLinks.length, data: data.updateExternalLinks };
      }, client),
    },

    {
      name: "catalog_delete_external_links",
      config: {
        title: "Delete External Links",
        description:
          "Remove external links by id. Irreversible. Batches up to 500; requires READ_WRITE token.",
        inputSchema: {
          data: z
            .array(
              z.object({
                id: z.string().min(1).describe("Catalog UUID of the link to delete."),
              })
            )
            .min(1)
            .max(500),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as DeleteExternalLinkInput[];
        const data = await c.execute<{ deleteExternalLinks: boolean }>(
          DELETE_EXTERNAL_LINKS,
          { data: input }
        );
        return { success: data.deleteExternalLinks, deleted: input.length };
      }, client),
    },

    // ── Quality checks mutations ──────────────────────────────────────────

    {
      name: "catalog_upsert_data_qualities",
      config: {
        title: "Upsert Data Quality Checks",
        description:
          "Register or update quality-check results for a single table. Unlike most mutations this one takes a single tableId with a nested array of checks (not a flat batch). Each check carries an externalId (stable identifier from the source tool), name, status, runAt, and optional description/url/columnId.\n\n" +
          "Designed for pushing results from dbt-tests, Monte Carlo monitors, Soda checks, Great Expectations, etc. Requires READ_WRITE token. Returns the resulting QualityCheck rows.",
        inputSchema: {
          tableId: z.string().min(1).describe("Catalog UUID of the table."),
          qualityChecks: z
            .array(
              z.object({
                externalId: z
                  .string()
                  .min(1)
                  .describe("Stable identifier from the source test tool."),
                name: z.string().min(1).describe("Human-readable check name."),
                status: z
                  .enum(["SUCCESS", "WARNING", "ALERT"])
                  .describe("Outcome of the check."),
                runAt: z
                  .string()
                  .describe("ISO-8601 timestamp when the check ran."),
                description: z.string().optional(),
                url: z.string().url().optional().describe("Link to the source tool's run."),
                columnId: z
                  .string()
                  .optional()
                  .describe("Scope the check to a specific column on the table."),
              })
            )
            .min(1)
            .max(500)
            .describe("Array of checks associated with this tableId."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: UpsertQualityChecksInput = {
          tableId: args.tableId as string,
          qualityChecks: args.qualityChecks as Array<{
            externalId: string;
            name: string;
            status: QualityStatus;
            runAt: string;
            description?: string;
            url?: string;
            columnId?: string;
          }>,
        };
        const data = await c.execute<{ upsertDataQualities: QualityCheck[] }>(
          UPSERT_DATA_QUALITIES,
          { data: input }
        );
        return {
          upserted: data.upsertDataQualities.length,
          data: data.upsertDataQualities,
        };
      }, client),
    },

    {
      name: "catalog_remove_data_qualities",
      config: {
        title: "Remove Data Quality Checks",
        description:
          "Remove quality-check rows by (tableId, externalId) composite keys. Irreversible. Accepts up to 500 keys per call; requires READ_WRITE token.",
        inputSchema: {
          qualityChecks: z
            .array(
              z.object({
                tableId: z.string().min(1),
                externalId: z.string().min(1),
              })
            )
            .min(1)
            .max(500)
            .describe("Composite keys (tableId + externalId) of checks to remove."),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: RemoveQualityChecksInput = {
          qualityChecks: args.qualityChecks as Array<{
            tableId: string;
            externalId: string;
          }>,
        };
        const data = await c.execute<{ removeDataQualities: boolean }>(
          REMOVE_DATA_QUALITIES,
          { data: input }
        );
        return {
          success: data.removeDataQualities,
          removed: input.qualityChecks.length,
        };
      }, client),
    },

    // ── Ownership writes ───────────────────────────────────────────────────

    {
      name: "catalog_upsert_user_owners",
      config: {
        title: "Assign User As Owner",
        description:
          "Mark a user as owner of one or more target assets (tables, dashboards, terms). Single userId, multiple targetEntities per call. Upserting an existing ownership is a no-op.\n\n" +
          "Requires READ_WRITE token. Returns the resulting OwnerEntity records.",
        inputSchema: {
          userId: z.string().min(1).describe("Catalog UUID of the user."),
          targetEntities: z
            .array(
              z.object({
                entityType: z.enum(["TABLE", "DASHBOARD", "TERM"]),
                entityId: z.string().min(1),
              })
            )
            .min(1)
            .describe("Assets to attribute to this user."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: OwnerInput = {
          userId: args.userId as string,
          targetEntities: args.targetEntities as EntityTarget[],
        };
        const data = await c.execute<{ upsertUserOwners: OwnerEntity[] }>(
          UPSERT_USER_OWNERS,
          { data: input }
        );
        return { upserted: data.upsertUserOwners.length, data: data.upsertUserOwners };
      }, client),
    },

    {
      name: "catalog_remove_user_owners",
      config: {
        title: "Remove User Ownership",
        description:
          "Strip a user's ownership of specified assets (or of all their assets if targetEntities is omitted). Irreversible within the scope of the removal.",
        inputSchema: {
          userId: z.string().min(1).describe("Catalog UUID of the user."),
          targetEntities: z
            .array(
              z.object({
                entityType: z.enum(["TABLE", "DASHBOARD", "TERM"]),
                entityId: z.string().min(1),
              })
            )
            .optional()
            .describe(
              "Specific assets to strip ownership from. Omit to remove the user from ALL owned assets."
            ),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: OwnerInput = {
          userId: args.userId as string,
          ...(Array.isArray(args.targetEntities)
            ? { targetEntities: args.targetEntities as EntityTarget[] }
            : {}),
        };
        const data = await c.execute<{ removeUserOwners: boolean }>(
          REMOVE_USER_OWNERS,
          { data: input }
        );
        return { success: data.removeUserOwners, userId: input.userId };
      }, client),
    },

    {
      name: "catalog_upsert_team_owners",
      config: {
        title: "Assign Team As Owner",
        description:
          "Mark a team as owner of one or more target assets. Single teamId, multiple targetEntities per call. Upserting an existing ownership is a no-op.",
        inputSchema: {
          teamId: z.string().min(1).describe("Catalog UUID of the team."),
          targetEntities: z
            .array(
              z.object({
                entityType: z.enum(["TABLE", "DASHBOARD", "TERM"]),
                entityId: z.string().min(1),
              })
            )
            .min(1)
            .describe("Assets to attribute to this team."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: TeamOwnerInput = {
          teamId: args.teamId as string,
          targetEntities: args.targetEntities as EntityTarget[],
        };
        const data = await c.execute<{ upsertTeamOwners: TeamOwnerEntity[] }>(
          UPSERT_TEAM_OWNERS,
          { data: input }
        );
        return { upserted: data.upsertTeamOwners.length, data: data.upsertTeamOwners };
      }, client),
    },

    {
      name: "catalog_remove_team_owners",
      config: {
        title: "Remove Team Ownership",
        description:
          "Strip a team's ownership of specified assets (or of all their assets if targetEntities is omitted). Irreversible within the scope of the removal.",
        inputSchema: {
          teamId: z.string().min(1).describe("Catalog UUID of the team."),
          targetEntities: z
            .array(
              z.object({
                entityType: z.enum(["TABLE", "DASHBOARD", "TERM"]),
                entityId: z.string().min(1),
              })
            )
            .optional()
            .describe(
              "Specific assets to strip ownership from. Omit to remove the team from ALL owned assets."
            ),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: TeamOwnerInput = {
          teamId: args.teamId as string,
          ...(Array.isArray(args.targetEntities)
            ? { targetEntities: args.targetEntities as EntityTarget[] }
            : {}),
        };
        const data = await c.execute<{ removeTeamOwners: boolean }>(
          REMOVE_TEAM_OWNERS,
          { data: input }
        );
        return { success: data.removeTeamOwners, teamId: input.teamId };
      }, client),
    },

    // ── Team management ────────────────────────────────────────────────────

    {
      name: "catalog_upsert_team",
      config: {
        title: "Create or Update Team",
        description:
          "Create a team (identified by its unique name) or update an existing one in place. Pass the name you want; if a team with that name already exists it is updated, otherwise one is created. Slack channels must start with '#'; Slack groups must start with '@'.\n\n" +
          "Single-row mutation (not batched). Requires READ_WRITE token. Returns the resulting team.",
        inputSchema: {
          name: z.string().min(1).describe("Unique team name across the account."),
          description: z.string().optional(),
          email: z.string().email().optional(),
          slackChannel: z
            .string()
            .startsWith("#")
            .optional()
            .describe("Slack channel, must start with '#'."),
          slackGroup: z
            .string()
            .startsWith("@")
            .optional()
            .describe("Slack group, must start with '@'."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: UpsertTeamInput = { name: args.name as string };
        if (typeof args.description === "string") input.description = args.description;
        if (typeof args.email === "string") input.email = args.email;
        if (typeof args.slackChannel === "string") input.slackChannel = args.slackChannel;
        if (typeof args.slackGroup === "string") input.slackGroup = args.slackGroup;
        const data = await c.execute<{ upsertTeam: Team }>(UPSERT_TEAM, {
          data: input,
        });
        return { team: data.upsertTeam };
      }, client),
    },

    {
      name: "catalog_add_team_users",
      config: {
        title: "Add Users to Team",
        description:
          "Add users (by email) to a team. Emails must belong to existing Catalog users. Single-row (one team, many emails). Requires READ_WRITE token.",
        inputSchema: {
          id: z.string().min(1).describe("Catalog UUID of the team."),
          emails: z
            .array(z.string().email())
            .min(1)
            .describe("Emails of users to add to the team."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: TeamUsersInput = {
          id: args.id as string,
          emails: args.emails as string[],
        };
        const data = await c.execute<{ addTeamUsers: boolean }>(ADD_TEAM_USERS, {
          data: input,
        });
        return { success: data.addTeamUsers, added: input.emails.length };
      }, client),
    },

    {
      name: "catalog_remove_team_users",
      config: {
        title: "Remove Users from Team",
        description:
          "Remove users (by email) from a team. Single-row. Requires READ_WRITE token.",
        inputSchema: {
          id: z.string().min(1).describe("Catalog UUID of the team."),
          emails: z
            .array(z.string().email())
            .min(1)
            .describe("Emails of users to remove from the team."),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: TeamUsersInput = {
          id: args.id as string,
          emails: args.emails as string[],
        };
        const data = await c.execute<{ removeTeamUsers: boolean }>(
          REMOVE_TEAM_USERS,
          { data: input }
        );
        return { success: data.removeTeamUsers, removed: input.emails.length };
      }, client),
    },

    // ── Pinned assets mutations ────────────────────────────────────────────

    {
      name: "catalog_upsert_pinned_assets",
      config: {
        title: "Upsert Pinned Asset Links",
        description:
          "Create or refresh pinned-asset relationships — curated 'see also' pointers from one catalog entity to another. Each input row is { from: {id, type}, to: {id, type} } where type is COLUMN | DASHBOARD | DASHBOARD_FIELD | TABLE | TERM.\n\n" +
          "Batches up to 500. Requires READ_WRITE token.",
        inputSchema: {
          data: z
            .array(
              z.object({
                from: z.object({
                  id: z.string().min(1),
                  type: z.enum([
                    "COLUMN",
                    "DASHBOARD",
                    "DASHBOARD_FIELD",
                    "TABLE",
                    "TERM",
                  ]),
                }),
                to: z.object({
                  id: z.string().min(1),
                  type: z.enum([
                    "COLUMN",
                    "DASHBOARD",
                    "DASHBOARD_FIELD",
                    "TABLE",
                    "TERM",
                  ]),
                }),
              })
            )
            .min(1)
            .max(500)
            .describe("Batch of pinned-asset links (max 500)."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as EntitiesLinkInput[];
        const data = await c.execute<{ upsertPinnedAssets: EntitiesLink[] }>(
          UPSERT_PINNED_ASSETS,
          { data: input }
        );
        return { upserted: data.upsertPinnedAssets.length, data: data.upsertPinnedAssets };
      }, client),
    },

    {
      name: "catalog_remove_pinned_assets",
      config: {
        title: "Remove Pinned Asset Links",
        description:
          "Remove pinned-asset links identified by endpoints. Same shape as upsert. Irreversible.",
        inputSchema: {
          data: z
            .array(
              z.object({
                from: z.object({
                  id: z.string().min(1),
                  type: z.enum([
                    "COLUMN",
                    "DASHBOARD",
                    "DASHBOARD_FIELD",
                    "TABLE",
                    "TERM",
                  ]),
                }),
                to: z.object({
                  id: z.string().min(1),
                  type: z.enum([
                    "COLUMN",
                    "DASHBOARD",
                    "DASHBOARD_FIELD",
                    "TABLE",
                    "TERM",
                  ]),
                }),
              })
            )
            .min(1)
            .max(500)
            .describe("Batch of pinned-asset links to remove (max 500)."),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as EntitiesLinkInput[];
        const data = await c.execute<{ removePinnedAssets: boolean }>(
          REMOVE_PINNED_ASSETS,
          { data: input }
        );
        return { success: data.removePinnedAssets, removed: input.length };
      }, client),
    },
  ];
}
