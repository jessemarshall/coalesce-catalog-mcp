import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLE_DETAIL,
  GET_DASHBOARD_DETAIL,
  GET_LINEAGES,
  GET_COLUMNS_SUMMARY,
  GET_DATA_QUALITIES,
} from "../catalog/operations.js";
import type {
  GetColumnsOutput,
  GetLineagesOutput,
  GetQualityChecksOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

type AssetKind = "TABLE" | "DASHBOARD";

const SummarizeAssetInputShape = {
  kind: z
    .enum(["TABLE", "DASHBOARD"])
    .describe("Asset type: TABLE or DASHBOARD."),
  id: z.string().min(1).describe("Catalog UUID of the asset."),
  columnsLimit: z
    .number()
    .int()
    .min(0)
    .max(500)
    .optional()
    .describe("Max columns to include for TABLE assets. Default: 50. Set 0 to skip."),
  upstreamLimit: z
    .number()
    .int()
    .min(0)
    .max(500)
    .optional()
    .describe("Max upstream lineage edges to include. Default: 25."),
  downstreamLimit: z
    .number()
    .int()
    .min(0)
    .max(500)
    .optional()
    .describe("Max downstream lineage edges to include. Default: 25."),
  qualityLimit: z
    .number()
    .int()
    .min(0)
    .max(500)
    .optional()
    .describe("Max quality-check rows for TABLE assets. Default: 25. Set 0 to skip."),
};

function truncate(text: unknown, max: number): string | null {
  if (typeof text !== "string") return null;
  if (text.length <= max) return text;
  return text.slice(0, max) + "…";
}

function owners(row: Record<string, unknown>): {
  users: unknown[];
  teams: unknown[];
} {
  return {
    users: Array.isArray(row.ownerEntities)
      ? (row.ownerEntities as Record<string, unknown>[]).map((o) => ({
          id: o.id,
          userId: o.userId,
          user: o.user ?? null,
        }))
      : [],
    teams: Array.isArray(row.teamOwnerEntities)
      ? (row.teamOwnerEntities as Record<string, unknown>[]).map((o) => ({
          id: o.id,
          teamId: o.teamId,
          team: o.team ?? null,
        }))
      : [],
  };
}

function tags(row: Record<string, unknown>): unknown[] {
  return Array.isArray(row.tagEntities)
    ? (row.tagEntities as Record<string, unknown>[]).map((t) => ({
        id: t.id,
        tag: t.tag,
      }))
    : [];
}

function lineageEnvelope(
  out: GetLineagesOutput,
  limit: number
): { totalCount: number; returned: number; hasMore: boolean; sample: unknown[] } {
  return {
    totalCount: out.totalCount,
    returned: out.data.length,
    hasMore: out.data.length < out.totalCount,
    sample: out.data.slice(0, limit),
  };
}

export function defineSummarizeAsset(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_summarize_asset",
    config: {
      title: "Summarize Asset (One-Call Overview)",
      description:
        "Produce a consolidated cross-domain summary for a TABLE or DASHBOARD asset in a single call: core identity + description, ownership (users + teams), tags, upstream + downstream lineage edges (with totalCount so you know whether to paginate), and — for tables — columns and recent quality-check rows.\n\n" +
        "Issues up to 5 underlying GraphQL queries in parallel. Use this to get full context on one asset without chaining 4-5 tool calls yourself. Limits are per-sub-query; set them to 0 to skip sections entirely.",
      inputSchema: SummarizeAssetInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const kind = args.kind as AssetKind;
      const id = args.id as string;
      const columnsLimit = (args.columnsLimit as number | undefined) ?? 50;
      const upstreamLimit = (args.upstreamLimit as number | undefined) ?? 25;
      const downstreamLimit = (args.downstreamLimit as number | undefined) ?? 25;
      const qualityLimit = (args.qualityLimit as number | undefined) ?? 25;

      // Fire the cross-cutting queries in parallel. Detail, lineage both
      // sides, columns (table-only), quality (table-only).
      const detailPromise =
        kind === "TABLE"
          ? c.execute<{ getTables: { data: Record<string, unknown>[] } }>(
              GET_TABLE_DETAIL,
              { ids: [id] }
            )
          : c.execute<{ getDashboards: { data: Record<string, unknown>[] } }>(
              GET_DASHBOARD_DETAIL,
              { ids: [id] }
            );

      const upstreamPromise =
        upstreamLimit > 0
          ? c.execute<{ getLineages: GetLineagesOutput }>(GET_LINEAGES, {
              scope:
                kind === "TABLE"
                  ? { childTableId: id }
                  : { childDashboardId: id },
              pagination: { nbPerPage: upstreamLimit, page: 0 },
            })
          : Promise.resolve(null);

      const downstreamPromise =
        downstreamLimit > 0
          ? c.execute<{ getLineages: GetLineagesOutput }>(GET_LINEAGES, {
              scope:
                kind === "TABLE"
                  ? { parentTableId: id }
                  : { parentDashboardId: id },
              pagination: { nbPerPage: downstreamLimit, page: 0 },
            })
          : Promise.resolve(null);

      const columnsPromise =
        kind === "TABLE" && columnsLimit > 0
          ? c.execute<{ getColumns: GetColumnsOutput }>(GET_COLUMNS_SUMMARY, {
              scope: { tableId: id },
              sorting: [{ sortingKey: "sourceOrder", direction: "ASC" }],
              pagination: { nbPerPage: columnsLimit, page: 0 },
            })
          : Promise.resolve(null);

      const qualityPromise =
        kind === "TABLE" && qualityLimit > 0
          ? c.execute<{ getDataQualities: GetQualityChecksOutput }>(
              GET_DATA_QUALITIES,
              {
                scope: { tableId: id },
                pagination: { nbPerPage: qualityLimit, page: 0 },
              }
            )
          : Promise.resolve(null);

      // allSettled so one sub-query failing (e.g. upstream lineage rejecting
      // an unknown id while detail returns empty) doesn't poison the others.
      const [
        detailRes,
        upstreamRes,
        downstreamRes,
        columnsRes,
        qualityRes,
      ] = await Promise.allSettled([
        detailPromise,
        upstreamPromise,
        downstreamPromise,
        columnsPromise,
        qualityPromise,
      ]);

      const unwrap = <T>(
        r: PromiseSettledResult<T>
      ): { value: T; error?: undefined } | { value?: undefined; error: string } => {
        if (r.status === "fulfilled") return { value: r.value };
        const e = r.reason;
        return {
          error: e instanceof Error ? e.message : String(e),
        };
      };

      const detail = unwrap(detailRes);
      const upstream = unwrap(upstreamRes);
      const downstream = unwrap(downstreamRes);
      const columns = unwrap(columnsRes);
      const quality = unwrap(qualityRes);

      if (detail.error) {
        return { error: "Failed to fetch asset detail", detail: detail.error };
      }

      const detailValue = detail.value!;
      const row =
        kind === "TABLE"
          ? (detailValue as { getTables: { data: Record<string, unknown>[] } })
              .getTables.data[0]
          : (detailValue as { getDashboards: { data: Record<string, unknown>[] } })
              .getDashboards.data[0];

      if (!row) return { notFound: true, kind, id };

      const core = {
        id: row.id,
        name: row.name,
        description: truncate(row.description, 800),
        externalId: row.externalId,
        url: row.url,
        popularity: row.popularity,
        isVerified: row.isVerified,
        isDeprecated: row.isDeprecated,
        deletedAt: row.deletedAt,
        deprecatedAt: row.deprecatedAt,
        verifiedAt: row.verifiedAt,
        createdAt: row.createdAt,
        updatedAt: row.updatedAt,
        ...(kind === "TABLE"
          ? {
              tableType: row.tableType,
              tableSize: row.tableSize,
              numberOfQueries: row.numberOfQueries,
              lastRefreshedAt: row.lastRefreshedAt,
              lastQueriedAt: row.lastQueriedAt,
              transformationSource: row.transformationSource,
              schema: row.schema,
              schemaId: row.schemaId,
            }
          : {
              type: row.type,
              folderPath: row.folderPath,
              folderUrl: row.folderUrl,
              sourceId: row.sourceId,
            }),
      };

      const summary: Record<string, unknown> = {
        kind,
        core,
        ownership: owners(row),
        annotations: {
          tags: tags(row),
          externalLinks: row.externalLinks ?? [],
        },
        lineage: {
          upstream: upstream.error
            ? { error: upstream.error }
            : upstream.value
              ? lineageEnvelope(upstream.value.getLineages, upstreamLimit)
              : { skipped: true },
          downstream: downstream.error
            ? { error: downstream.error }
            : downstream.value
              ? lineageEnvelope(downstream.value.getLineages, downstreamLimit)
              : { skipped: true },
        },
      };

      if (kind === "TABLE") {
        summary.columns = columns.error
          ? { error: columns.error }
          : columns.value
            ? {
                totalCount: columns.value.getColumns.totalCount,
                returned: columns.value.getColumns.data.length,
                hasMore:
                  columns.value.getColumns.data.length <
                  columns.value.getColumns.totalCount,
                sample: columns.value.getColumns.data,
              }
            : { skipped: true };
        summary.qualityChecks = quality.error
          ? { error: quality.error }
          : quality.value
            ? {
                totalCount: quality.value.getDataQualities.totalCount,
                returned: quality.value.getDataQualities.data.length,
                hasMore:
                  quality.value.getDataQualities.data.length <
                  quality.value.getDataQualities.totalCount,
                sample: quality.value.getDataQualities.data,
              }
            : { skipped: true };
      }

      return summary;
    }, client),
  };
}
