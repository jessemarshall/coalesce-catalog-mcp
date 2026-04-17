import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_SOURCES,
  GET_DATABASES,
  GET_SCHEMAS,
} from "../catalog/operations.js";
import type {
  GetDatabasesOutput,
  GetDatabasesScope,
  GetSchemasOutput,
  GetSchemasScope,
  GetSourcesOutput,
  GetSourcesScope,
  Pagination,
  SourceOrigin,
  SourceType,
} from "../generated/types.js";
import {
  PaginationInputShape,
  toGraphQLPagination,
  type PaginationInput,
} from "../schemas/pagination.js";
import { listEnvelope, withErrorHandling } from "./tool-helpers.js";

// ── Shared source enums ─────────────────────────────────────────────────────

const SourceOriginSchema = z.enum([
  "API",
  "EXTRACTION",
]) satisfies z.ZodType<SourceOrigin>;

const SourceTypeSchema = z.enum([
  "COMMUNICATION",
  "KNOWLEDGE",
  "QUALITY",
  "TRANSFORMATION",
  "VISUALIZATION",
  "WAREHOUSE",
]) satisfies z.ZodType<SourceType>;

// SourceTechnology has ~65 enum values; enumerating them in Zod bloats the
// tool schema without much gain. Accept any string and let the API reject
// invalid values — the description lists the common techs so the LLM has
// strong priors.
const SourceTechnologySchema = z
  .string()
  .describe(
    "Source technology. Warehouses: SNOWFLAKE, BIGQUERY, DATABRICKS, REDSHIFT, POSTGRES, MYSQL, SQLSERVER, ORACLE, DELTALAKE. BI/viz: TABLEAU, LOOKER, POWERBI, SIGMA, MODE, METABASE, SUPERSET, LOOKER_STUDIO, THOUGHTSPOT. Transform: COALESCE, DBT, FIVETRAN. Quality: COALESCE_QUALITY, DBT_TEST, MONTE_CARLO, GREAT_EXPECTATIONS, SODA, SIFFLET, ANOMALO. Knowledge: CONFLUENCE, NOTION. Plus many more — see SourceTechnology enum in the generated schema."
  );

// ── Sources ─────────────────────────────────────────────────────────────────

const SearchSourcesInputShape = {
  nameContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the source name."),
  origin: SourceOriginSchema.optional().describe(
    "API = pushed via public API; EXTRACTION = auto-ingested by a Castor extractor."
  ),
  technology: SourceTechnologySchema.optional(),
  type: SourceTypeSchema.optional().describe(
    "High-level source category: WAREHOUSE, VISUALIZATION, TRANSFORMATION, QUALITY, KNOWLEDGE, COMMUNICATION."
  ),
  withDeleted: z.boolean().optional(),
  ...PaginationInputShape,
};

function buildSourcesScope(
  input: Record<string, unknown>
): GetSourcesScope | undefined {
  const scope: GetSourcesScope = {};
  if (typeof input.nameContains === "string") scope.nameContains = input.nameContains;
  if (typeof input.origin === "string") scope.origin = input.origin as SourceOrigin;
  if (typeof input.technology === "string") {
    // Cast: we accept free-text string but the GraphQL enum is narrower; the
    // server validates and rejects unknown values.
    scope.technology = input.technology as GetSourcesScope["technology"];
  }
  if (typeof input.type === "string") scope.type = input.type as SourceType;
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Databases ───────────────────────────────────────────────────────────────

const SearchDatabasesInputShape = {
  nameContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the database name."),
  sourceIds: z
    .array(z.string())
    .optional()
    .describe("Scope to databases belonging to any of these source UUIDs."),
  withDeleted: z.boolean().optional(),
  withHidden: z.boolean().optional(),
  ...PaginationInputShape,
};

function buildDatabasesScope(
  input: Record<string, unknown>
): GetDatabasesScope | undefined {
  const scope: GetDatabasesScope = {};
  if (typeof input.nameContains === "string") scope.nameContains = input.nameContains;
  if (Array.isArray(input.sourceIds)) scope.sourceIds = input.sourceIds as string[];
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  if (typeof input.withHidden === "boolean") scope.withHidden = input.withHidden;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Schemas ─────────────────────────────────────────────────────────────────

const SearchSchemasInputShape = {
  nameContains: z
    .string()
    .optional()
    .describe("Case-insensitive substring match against the schema name."),
  databaseIds: z
    .array(z.string())
    .optional()
    .describe("Scope to schemas belonging to any of these database UUIDs."),
  sourceIds: z
    .array(z.string())
    .optional()
    .describe("Scope to schemas belonging to any of these source UUIDs."),
  withDeleted: z.boolean().optional(),
  withHidden: z.boolean().optional(),
  ...PaginationInputShape,
};

function buildSchemasScope(
  input: Record<string, unknown>
): GetSchemasScope | undefined {
  const scope: GetSchemasScope = {};
  if (typeof input.nameContains === "string") scope.nameContains = input.nameContains;
  if (Array.isArray(input.databaseIds)) scope.databaseIds = input.databaseIds as string[];
  if (Array.isArray(input.sourceIds)) scope.sourceIds = input.sourceIds as string[];
  if (typeof input.withDeleted === "boolean") scope.withDeleted = input.withDeleted;
  if (typeof input.withHidden === "boolean") scope.withHidden = input.withHidden;
  return Object.keys(scope).length > 0 ? scope : undefined;
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineDiscoveryTools(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_search_sources",
      config: {
        title: "Search Catalog Sources",
        description:
          "List connected data sources — the roots of the catalog tree (warehouses like Snowflake, BI tools like Tableau, transform tools like Coalesce, etc.). Use this first when resolving IDs for downstream filters (sourceId on databases/tables/dashboards).\n\n" +
          "Supports substring search on name, plus filters by origin (API vs EXTRACTION), technology (SNOWFLAKE, TABLEAU, ...), and type (WAREHOUSE, VISUALIZATION, ...). No server-side sorting; results come back in API order.",
        inputSchema: SearchSourcesInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildSourcesScope(args),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getSources: GetSourcesOutput }>(
          GET_SOURCES,
          variables
        );
        const out = data.getSources;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_search_databases",
      config: {
        title: "Search Catalog Databases",
        description:
          "List warehouse databases (Snowflake DBs, BigQuery projects, etc.). Scope via sourceIds (from catalog_search_sources) or nameContains. Returns identity + description + isHidden. Use the resulting `id` as `databaseId` in catalog_search_tables / catalog_search_columns / catalog_search_schemas.",
        inputSchema: SearchDatabasesInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildDatabasesScope(args),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getDatabases: GetDatabasesOutput }>(
          GET_DATABASES,
          variables
        );
        const out = data.getDatabases;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },

    {
      name: "catalog_search_schemas",
      config: {
        title: "Search Catalog Schemas",
        description:
          "List warehouse schemas (Snowflake schemas, BigQuery datasets, etc.). Scope via databaseIds (from catalog_search_databases), sourceIds, or nameContains. Returns identity + databaseId + description + isHidden. Use the resulting `id` as `schemaId` in catalog_search_tables / catalog_search_columns.",
        inputSchema: SearchSchemasInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const pagination = toGraphQLPagination(args as PaginationInput);
        const variables = {
          scope: buildSchemasScope(args),
          pagination: pagination as Pagination,
        };
        const data = await c.query<{ getSchemas: GetSchemasOutput }>(
          GET_SCHEMAS,
          variables
        );
        const out = data.getSchemas;
        return listEnvelope(out.page ?? 0, out.nbPerPage, out.totalCount, out.data);
      }, client),
    },
  ];
}
