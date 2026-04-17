import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TAGS,
  GET_TERMS,
  GET_DATA_PRODUCTS,
  ATTACH_TAGS,
  DETACH_TAGS,
  CREATE_TERM,
  UPDATE_TERM,
  DELETE_TERM,
} from "../catalog/operations.js";
import type {
  BaseTagEntityInput,
  CreateTermInput,
  DataProductSorting,
  DataProductSortingKey,
  DeleteTermInput,
  EntityTargetType,
  GetDataProductOutput,
  GetDataProductScope,
  GetTagsOutput,
  GetTagsScope,
  GetTermsOutput,
  GetTermsScope,
  Pagination,
  TagEntityType,
  TagSorting,
  TagSortingKey,
  Term,
  TermSorting,
  TermSortingKey,
  UpdateTermInput,
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

// ── Tags ────────────────────────────────────────────────────────────────────

const TagSortingKeySchema = z.enum(["label"]) satisfies z.ZodType<TagSortingKey>;

const SearchTagsInputShape = {
  labelContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the tag label."),
  ids: z
    .array(z.string())
    .optional()
    .describe("Fetch specific tags by UUID."),
  sortBy: TagSortingKeySchema.optional().describe("Sort key. Only option: label."),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildTagsScope(
  input: Record<string, unknown>
): GetTagsScope | undefined {
  const scope: GetTagsScope = {};
  if (typeof input.labelContains === "string") scope.labelContains = input.labelContains;
  if (Array.isArray(input.ids)) scope.ids = input.ids as string[];
  return Object.keys(scope).length > 0 ? scope : undefined;
}

function buildTagSorting(
  sortBy: TagSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): TagSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: TagSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

// ── Terms ───────────────────────────────────────────────────────────────────

const TermSortingKeySchema = z.enum([
  "name",
  "ownersAndTeamOwnersCount",
]) satisfies z.ZodType<TermSortingKey>;

const SearchTermsInputShape = {
  nameContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the term name."),
  ids: z
    .array(z.string())
    .optional()
    .describe("Fetch specific terms by UUID."),
  sortBy: TermSortingKeySchema.optional().describe(
    "Sort key. Options: name, ownersAndTeamOwnersCount."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildTermsScope(
  input: Record<string, unknown>
): GetTermsScope | undefined {
  const scope: GetTermsScope = {};
  if (typeof input.nameContains === "string") scope.nameContains = input.nameContains;
  if (Array.isArray(input.ids)) scope.ids = input.ids as string[];
  return Object.keys(scope).length > 0 ? scope : undefined;
}

function buildTermSorting(
  sortBy: TermSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): TermSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: TermSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

// ── Data products ───────────────────────────────────────────────────────────

const EntityTargetTypeSchema = z.enum([
  "DASHBOARD",
  "TABLE",
  "TERM",
]) satisfies z.ZodType<EntityTargetType>;

const DataProductSortingKeySchema = z.enum([
  "dashboardName",
  "tableName",
  "termName",
]) satisfies z.ZodType<DataProductSortingKey>;

const SearchDataProductsInputShape = {
  entityType: EntityTargetTypeSchema.optional().describe(
    "Filter by the kind of asset marked as a data product: TABLE, DASHBOARD, or TERM."
  ),
  withTagId: z
    .string()
    .optional()
    .describe("Scope to data products whose tagged entity carries this tag (or domain) UUID."),
  sortBy: DataProductSortingKeySchema.optional().describe(
    "Sort key. Options: dashboardName, tableName, termName."
  ),
  sortDirection: SortDirectionSchema.optional(),
  nullsPriority: NullsPrioritySchema.optional(),
  ...PaginationInputShape,
};

function buildDataProductsScope(
  input: Record<string, unknown>
): GetDataProductScope | undefined {
  const scope: GetDataProductScope = {};
  if (typeof input.entityType === "string") {
    scope.entityType = input.entityType as EntityTargetType;
  }
  if (typeof input.withTagId === "string") scope.withTagId = input.withTagId;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

function buildDataProductSorting(
  sortBy: DataProductSortingKey | undefined,
  direction: "ASC" | "DESC" | undefined,
  nulls: "FIRST" | "LAST" | undefined
): DataProductSorting[] | undefined {
  if (!sortBy) return undefined;
  const entry: DataProductSorting = { sortingKey: sortBy };
  if (direction) entry.direction = direction;
  if (nulls) entry.nullsPriority = nulls;
  return [entry];
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineAnnotationTools(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_search_tags",
      config: {
        title: "Search Catalog Tags",
        description:
          "List tags defined in the Catalog. Tags are reusable labels that can be attached to tables, columns, dashboards, or terms (via the tag* mutation tools in Phase 4). Returns the tag identity (id, label, color) plus any linked term UUID (for glossary-backed tags).\n\n" +
          "Use for: discovering what tags already exist before creating a new one, looking up a tag UUID by label, or enumerating the color/taxonomy palette in use.",
        inputSchema: SearchTagsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildTagsScope(args),
          sorting: buildTagSorting(
            args.sortBy as TagSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getTags: GetTagsOutput }>(
          GET_TAGS,
          variables
        );
        const out = data.getTags;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_search_terms",
      config: {
        title: "Search Catalog Terms (Glossary)",
        description:
          "List glossary terms — the Catalog's business-language definitions that knit together technical assets (tables, columns, dashboards) with shared vocabulary. Terms form a hierarchy via parentTermId + depthLevel and can be linked 1:1 with a tag.\n\n" +
          "Returns name, description, hierarchy position, verification/deprecation flags, and linkedTag metadata. Use catalog_search_tags with the linkedTagId to find entities tagged with a given term's concept.",
        inputSchema: SearchTermsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildTermsScope(args),
          sorting: buildTermSorting(
            args.sortBy as TermSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getTerms: GetTermsOutput }>(
          GET_TERMS,
          variables
        );
        const out = data.getTerms;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_search_data_products",
      config: {
        title: "Search Catalog Data Products",
        description:
          "List assets (tables, dashboards, or terms) that have been promoted to 'data products' — the Catalog's curated, governance-approved surface. Each record points to exactly one asset via tableId, dashboardId, or termId.\n\n" +
          "Filter by entityType (TABLE/DASHBOARD/TERM) and/or withTagId (a tag or domain UUID). Hydrate the asset identity via catalog_get_table / catalog_get_dashboard.",
        inputSchema: SearchDataProductsInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildDataProductsScope(args),
          sorting: buildDataProductSorting(
            args.sortBy as DataProductSortingKey | undefined,
            args.sortDirection as "ASC" | "DESC" | undefined,
            args.nullsPriority as "FIRST" | "LAST" | undefined
          ),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getDataProducts: GetDataProductOutput }>(
          GET_DATA_PRODUCTS,
          variables
        );
        const out = data.getDataProducts;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    // ── Mutations ──────────────────────────────────────────────────────────

    {
      name: "catalog_attach_tags",
      config: {
        title: "Attach Tags to Entities",
        description:
          "Attach tags to one or more entities (tables, columns, dashboards, dashboard fields, or terms). Tags are addressed by *label* — if a tag with the given label does not exist, it is created automatically. Each input row binds one tag label to one entity.\n\n" +
          "Accepts up to 500 rows per call. Requires a READ_WRITE API token. Returns a boolean success flag (the underlying mutation has no per-row result).",
        inputSchema: {
          data: z
            .array(
              z.object({
                entityType: z
                  .enum(["COLUMN", "DASHBOARD", "DASHBOARD_FIELD", "TABLE", "TERM"])
                  .describe("What kind of entity to tag."),
                entityId: z.string().min(1).describe("Catalog UUID of the entity."),
                label: z
                  .string()
                  .min(1)
                  .describe("Tag label. Created if it doesn't already exist."),
              })
            )
            .min(1)
            .max(500)
            .describe("Batch of tag attachments (max 500)."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as Array<{
          entityType: TagEntityType;
          entityId: string;
          label: string;
        }>;
        const data = await c.query<{ attachTags: boolean }>(ATTACH_TAGS, {
          data: input satisfies BaseTagEntityInput[],
        });
        return { success: data.attachTags, attached: input.length };
      }, client),
    },

    {
      name: "catalog_detach_tags",
      config: {
        title: "Detach Tags from Entities",
        description:
          "Remove tag bindings from entities. Identifies the binding by (entityType, entityId, label) — the same shape as catalog_attach_tags. Does not delete the tag itself, only the association.\n\n" +
          "Accepts up to 500 rows per call. Requires a READ_WRITE API token. Returns a boolean success flag.",
        inputSchema: {
          data: z
            .array(
              z.object({
                entityType: z
                  .enum(["COLUMN", "DASHBOARD", "DASHBOARD_FIELD", "TABLE", "TERM"])
                  .describe("The entity the tag is currently attached to."),
                entityId: z.string().min(1).describe("Catalog UUID of the entity."),
                label: z.string().min(1).describe("Tag label to remove."),
              })
            )
            .min(1)
            .max(500)
            .describe("Batch of tag detachments (max 500)."),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input = args.data as Array<{
          entityType: TagEntityType;
          entityId: string;
          label: string;
        }>;
        const data = await c.query<{ detachTags: boolean }>(DETACH_TAGS, {
          data: input satisfies BaseTagEntityInput[],
        });
        return { success: data.detachTags, detached: input.length };
      }, client),
    },

    // ── Term CRUD ──────────────────────────────────────────────────────────

    {
      name: "catalog_create_term",
      config: {
        title: "Create Glossary Term",
        description:
          "Create a new glossary term. Single-row (not batched) — mirroring the API. Required: name + description (supports markdown). Optional: parentTermId to nest it beneath another term (omit for a root-level term), linkedTagId to associate with a tag so tagged entities inherit the term's context.\n\n" +
          "Requires READ_WRITE token. Returns the full created term.",
        inputSchema: {
          name: z.string().min(1).describe("Term name."),
          description: z
            .string()
            .min(1)
            .describe("Markdown-supported description of the term."),
          parentTermId: z
            .string()
            .optional()
            .describe("UUID of the parent term; omit to create at the root."),
          linkedTagId: z
            .string()
            .optional()
            .describe("UUID of a tag to link 1:1 with this term."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: CreateTermInput = {
          name: args.name as string,
          description: args.description as string,
          ...(typeof args.parentTermId === "string"
            ? { parentTermId: args.parentTermId }
            : {}),
          ...(typeof args.linkedTagId === "string"
            ? { linkedTagId: args.linkedTagId }
            : {}),
        };
        const data = await c.query<{ createTerm: Term }>(CREATE_TERM, {
          data: input,
        });
        return { term: data.createTerm };
      }, client),
    },

    {
      name: "catalog_update_term",
      config: {
        title: "Update Glossary Term",
        description:
          "Update a term by id. All fields except id are optional. To detach a linked tag pass linkedTagId: null; to move to the root pass parentTermId: null. Single-row (not batched).\n\n" +
          "Requires READ_WRITE token. Returns the updated term.",
        inputSchema: {
          id: z.string().min(1).describe("Catalog UUID of the term."),
          name: z.string().optional(),
          description: z.string().optional(),
          parentTermId: z
            .string()
            .nullable()
            .optional()
            .describe("Set to null to move to root, or a UUID to re-parent."),
          linkedTagId: z
            .string()
            .nullable()
            .optional()
            .describe("Set to null to unlink, or a UUID to link a tag."),
        },
        annotations: WRITE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: UpdateTermInput = { id: args.id as string };
        if (typeof args.name === "string") input.name = args.name;
        if (typeof args.description === "string") input.description = args.description;
        if (args.parentTermId !== undefined) {
          input.parentTermId = args.parentTermId as string | null;
        }
        if (args.linkedTagId !== undefined) {
          input.linkedTagId = args.linkedTagId as string | null;
        }
        const data = await c.query<{ updateTerm: Term }>(UPDATE_TERM, {
          data: input,
        });
        return { term: data.updateTerm };
      }, client),
    },

    {
      name: "catalog_delete_term",
      config: {
        title: "Delete Glossary Term",
        description:
          "Delete a term by id. Irreversible. Child terms become orphaned (their parentTermId still points at a now-deleted term). Single-row (not batched).\n\n" +
          "Requires READ_WRITE token. Returns a boolean success flag.",
        inputSchema: {
          id: z.string().min(1).describe("Catalog UUID of the term to delete."),
        },
        annotations: DESTRUCTIVE_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: DeleteTermInput = { id: args.id as string };
        const data = await c.query<{ deleteTerm: boolean }>(DELETE_TERM, {
          data: input,
        });
        return { success: data.deleteTerm, id: input.id };
      }, client),
    },
  ];
}
