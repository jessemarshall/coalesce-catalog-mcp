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
  GET_TABLES_SUMMARY,
  GET_DASHBOARDS_SUMMARY,
  GET_DATA_QUALITIES,
} from "../catalog/operations.js";
import type {
  GetDashboardsOutput,
  GetLineagesOutput,
  GetQualityChecksOutput,
  GetTablesOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

type AssetKind = "TABLE" | "DASHBOARD";

const AssessImpactInputShape = {
  assetKind: z
    .enum(["TABLE", "DASHBOARD"])
    .describe("Asset type whose blast radius to assess."),
  assetId: z.string().min(1).describe("Catalog UUID of the asset."),
  maxDepth: z
    .number()
    .int()
    .min(1)
    .max(3)
    .optional()
    .describe(
      "How many lineage hops downstream to traverse. 1 = immediate consumers only (always complete; cheapest). 2 = consumers-of-consumers (refuses if >2000 distinct nodes encountered). 3 = full deprecation-grade walk (refuses if >500). Default 1."
    ),
  includeQualityChecks: z
    .boolean()
    .optional()
    .describe(
      "Fetch quality-check coverage for the starting TABLE. Ignored for DASHBOARD assets. Default true."
    ),
};

// Width caps per depth. Depth 1 is unbounded (a single paginated lineage call
// is always tractable). Depth 2/3 fan out one query per node in the previous
// frontier, so we refuse rather than silently truncate or burn a minute of
// runtime. The contract is "complete impact, or explicit refusal."
const WIDTH_CAPS: Record<number, number> = {
  2: 2000,
  3: 500,
};

const ENRICHMENT_BATCH_SIZE = 500;

interface ReachedNode {
  kind: AssetKind;
  depth: number;
}

interface EnrichedAsset {
  id: string;
  name: string | null;
  kind: AssetKind;
  popularity: number | null;
  isDeprecated: boolean | null;
  isVerified: boolean | null;
  depth: number;
}

interface SeverityComponent {
  component: string;
  points: number;
  max: number;
  detail: string;
}

interface Severity {
  score: number;
  bucket: "low" | "medium" | "high";
  rationale: SeverityComponent[];
}

function isTableEdge(edge: {
  childTableId?: string | null;
  childDashboardId?: string | null;
}): boolean {
  return Boolean(edge.childTableId);
}

function pickChildId(edge: {
  childTableId?: string | null;
  childDashboardId?: string | null;
}): { id: string; kind: AssetKind } | null {
  if (edge.childTableId) return { id: edge.childTableId, kind: "TABLE" };
  if (edge.childDashboardId)
    return { id: edge.childDashboardId, kind: "DASHBOARD" };
  return null;
}

async function traverseDownstream(
  client: CatalogClient,
  startId: string,
  startKind: AssetKind,
  maxDepth: number
): Promise<{ visited: Map<string, ReachedNode>; truncated: false }> {
  const visited = new Map<string, ReachedNode>();
  visited.set(startId, { kind: startKind, depth: 0 });
  let frontier: Array<{ id: string; kind: AssetKind }> = [
    { id: startId, kind: startKind },
  ];

  for (let depth = 1; depth <= maxDepth; depth++) {
    if (depth >= 2) {
      const cap = WIDTH_CAPS[depth];
      if (frontier.length > cap) {
        throw new Error(
          `Graph too wide for complete impact assessment at depth ${depth}: ` +
            `${frontier.length} nodes in the depth-${depth - 1} frontier exceeds the ` +
            `${cap}-node cap. Reduce maxDepth, or run depth=1 first to identify the ` +
            `widest hotspots and assess them individually.`
        );
      }
    }

    const edgesPerNode = await Promise.all(
      frontier.map((node) =>
        client.execute<{ getLineages: GetLineagesOutput }>(GET_LINEAGES, {
          scope:
            node.kind === "TABLE"
              ? { parentTableId: node.id }
              : { parentDashboardId: node.id },
          pagination: { nbPerPage: 500, page: 0 },
        })
      )
    );

    const nextFrontier: Array<{ id: string; kind: AssetKind }> = [];
    for (const result of edgesPerNode) {
      for (const edge of result.getLineages.data) {
        const child = pickChildId(edge);
        if (!child) continue;
        if (visited.has(child.id)) continue;
        visited.set(child.id, { kind: child.kind, depth });
        nextFrontier.push(child);
      }
    }

    if (nextFrontier.length === 0) break;
    frontier = nextFrontier;
  }

  return { visited, truncated: false };
}

async function enrichByKind(
  client: CatalogClient,
  ids: string[],
  kind: AssetKind
): Promise<Map<string, Record<string, unknown>>> {
  const map = new Map<string, Record<string, unknown>>();
  if (ids.length === 0) return map;

  for (let i = 0; i < ids.length; i += ENRICHMENT_BATCH_SIZE) {
    const slice = ids.slice(i, i + ENRICHMENT_BATCH_SIZE);
    if (kind === "TABLE") {
      const resp = await client.execute<{ getTables: GetTablesOutput }>(
        GET_TABLES_SUMMARY,
        {
          scope: { ids: slice },
          pagination: { nbPerPage: slice.length, page: 0 },
        }
      );
      for (const row of resp.getTables.data) {
        map.set(row.id, row as unknown as Record<string, unknown>);
      }
    } else {
      const resp = await client.execute<{ getDashboards: GetDashboardsOutput }>(
        GET_DASHBOARDS_SUMMARY,
        {
          scope: { ids: slice },
          pagination: { nbPerPage: slice.length, page: 0 },
        }
      );
      for (const row of resp.getDashboards.data) {
        map.set(row.id, row as unknown as Record<string, unknown>);
      }
    }
  }
  return map;
}

function computeSeverity(opts: {
  downstreamCount: number;
  numberOfQueries: number | null;
  lastQueriedAt: number | null;
  startKind: AssetKind;
  now: number;
}): Severity {
  // Downstream impact: log-scaled so that going from 0→10 children and from
  // 100→110 don't move the needle the same way. Cap at 60 so a lineage
  // explosion can't dominate the entire score on its own.
  const downstreamPts = Math.min(
    60,
    Math.log10(opts.downstreamCount + 1) * 30
  );

  // Active usage: split into volume + recency. Both are TABLE-only signals
  // (dashboards have no numberOfQueries / lastQueriedAt scalars in the API).
  let queryVolumePts = 0;
  let queryVolumeDetail = "n/a (DASHBOARD has no query-volume signal)";
  if (opts.startKind === "TABLE" && opts.numberOfQueries != null) {
    queryVolumePts = Math.min(20, Math.log10(opts.numberOfQueries + 1) * 5);
    queryVolumeDetail = `${opts.numberOfQueries} cumulative queries`;
  } else if (opts.startKind === "TABLE") {
    queryVolumeDetail = "never queried";
  }

  let recencyPts = 0;
  let recencyDetail = "n/a (DASHBOARD has no last-queried signal)";
  if (opts.startKind === "TABLE" && opts.lastQueriedAt != null) {
    const ageDays = (opts.now - opts.lastQueriedAt) / (1000 * 60 * 60 * 24);
    if (ageDays < 7) recencyPts = 20;
    else if (ageDays < 30) recencyPts = 12;
    else if (ageDays < 90) recencyPts = 6;
    recencyDetail = `last queried ${Math.round(ageDays)} days ago`;
  } else if (opts.startKind === "TABLE") {
    recencyDetail = "never queried";
  }

  const score = Math.round(downstreamPts + queryVolumePts + recencyPts);
  const bucket: Severity["bucket"] =
    score < 30 ? "low" : score < 60 ? "medium" : "high";

  return {
    score,
    bucket,
    rationale: [
      {
        component: "downstream_impact",
        points: Math.round(downstreamPts),
        max: 60,
        detail: `${opts.downstreamCount} downstream assets reached`,
      },
      {
        component: "query_volume",
        points: Math.round(queryVolumePts),
        max: 20,
        detail: queryVolumeDetail,
      },
      {
        component: "query_recency",
        points: recencyPts,
        max: 20,
        detail: recencyDetail,
      },
    ],
  };
}

function shapeOwnership(row: Record<string, unknown>): {
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

function shapeTags(row: Record<string, unknown>): unknown[] {
  return Array.isArray(row.tagEntities)
    ? (row.tagEntities as Record<string, unknown>[]).map((t) => ({
        id: t.id,
        tag: t.tag,
      }))
    : [];
}

function summariseQualityStatuses(
  data: GetQualityChecksOutput | null
): {
  totalCount: number;
  byStatus: { SUCCESS: number; WARNING: number; ALERT: number };
} | null {
  if (!data) return null;
  const counts = { SUCCESS: 0, WARNING: 0, ALERT: 0 };
  for (const row of data.data as Array<{ status?: string }>) {
    const s = row.status as keyof typeof counts | undefined;
    if (s && s in counts) counts[s] += 1;
  }
  return { totalCount: data.totalCount, byStatus: counts };
}

export function defineAssessImpact(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_assess_impact",
    config: {
      title: "Assess Blast Radius (Deprecation Impact Report)",
      description:
        "Composed deprecation-impact report for a TABLE or DASHBOARD: walks downstream lineage to the requested depth, batch-enriches every reached asset with name + popularity, attaches the starting asset's ownership + tags + quality-check coverage, and returns a 0-100 severity score with explicit per-component rationale.\n\n" +
        "**Completeness contract:** at any given depth, the report is exhaustive — there is no silent truncation. If the downstream graph at depth 2 exceeds 2000 distinct nodes (or 500 at depth 3), the tool refuses with an actionable error rather than returning a partial answer. Use depth=1 first on wide hubs to identify hotspots and assess them individually.\n\n" +
        "Severity rubric (deterministic, transparent in `rationale[]`):\n" +
        "  - downstream_impact: 0-60 pts, log-scaled by reached-asset count\n" +
        "  - query_volume: 0-20 pts, log-scaled by numberOfQueries (TABLE-only)\n" +
        "  - query_recency: 0-20 pts, banded by lastQueriedAt age (<7d / <30d / <90d)\n" +
        "Buckets: 0-30 low, 30-60 medium, 60-100 high.\n\n" +
        "One call replaces 5+ chained ones (lineage + per-asset enrichment + ownership + quality + scoring). Use before any deprecate / archive / restructure decision.",
      inputSchema: AssessImpactInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const assetKind = args.assetKind as AssetKind;
      const assetId = args.assetId as string;
      const maxDepth = (args.maxDepth as number | undefined) ?? 1;
      const includeQuality =
        (args.includeQualityChecks as boolean | undefined) ?? true;

      // Detail + lineage traversal + (optional) quality check fetch run in
      // parallel. Detail must succeed for the report to be meaningful;
      // traversal failure also fails the call (impact must be exhaustive).
      const detailPromise =
        assetKind === "TABLE"
          ? c.execute<{ getTables: { data: Record<string, unknown>[] } }>(
              GET_TABLE_DETAIL,
              { ids: [assetId] }
            )
          : c.execute<{ getDashboards: { data: Record<string, unknown>[] } }>(
              GET_DASHBOARD_DETAIL,
              { ids: [assetId] }
            );

      const traversalPromise = traverseDownstream(
        c,
        assetId,
        assetKind,
        maxDepth
      );

      const qualityPromise =
        assetKind === "TABLE" && includeQuality
          ? c.execute<{ getDataQualities: GetQualityChecksOutput }>(
              GET_DATA_QUALITIES,
              {
                scope: { tableId: assetId },
                pagination: { nbPerPage: 500, page: 0 },
              }
            )
          : Promise.resolve(null);

      const [detail, traversal, qualityRaw] = await Promise.all([
        detailPromise,
        traversalPromise,
        qualityPromise,
      ]);

      const detailRow =
        assetKind === "TABLE"
          ? (detail as { getTables: { data: Record<string, unknown>[] } })
              .getTables.data[0]
          : (detail as { getDashboards: { data: Record<string, unknown>[] } })
              .getDashboards.data[0];

      if (!detailRow) {
        return { notFound: true, assetKind, assetId };
      }

      // Enrich every reached node (excluding the starting asset, which we
      // already have full detail for). Split by kind so the API gets one
      // batched call per kind regardless of how mixed the graph is.
      const reachedTableIds: string[] = [];
      const reachedDashboardIds: string[] = [];
      for (const [id, node] of traversal.visited) {
        if (id === assetId) continue;
        if (node.kind === "TABLE") reachedTableIds.push(id);
        else reachedDashboardIds.push(id);
      }

      const [tableEnrichment, dashboardEnrichment] = await Promise.all([
        enrichByKind(c, reachedTableIds, "TABLE"),
        enrichByKind(c, reachedDashboardIds, "DASHBOARD"),
      ]);

      const enriched: EnrichedAsset[] = [];
      for (const [id, node] of traversal.visited) {
        if (id === assetId) continue;
        const row =
          node.kind === "TABLE"
            ? tableEnrichment.get(id)
            : dashboardEnrichment.get(id);
        enriched.push({
          id,
          name: (row?.name as string | null) ?? null,
          kind: node.kind,
          popularity: (row?.popularity as number | null) ?? null,
          isDeprecated: (row?.isDeprecated as boolean | null) ?? null,
          isVerified: (row?.isVerified as boolean | null) ?? null,
          depth: node.depth,
        });
      }
      enriched.sort(
        (a, b) =>
          a.depth - b.depth ||
          (b.popularity ?? -1) - (a.popularity ?? -1) ||
          (a.name ?? "").localeCompare(b.name ?? "")
      );

      const downstreamCount = enriched.length;
      const downstreamTables = enriched.filter((a) => a.kind === "TABLE");
      const downstreamDashboards = enriched.filter(
        (a) => a.kind === "DASHBOARD"
      );

      const severity = computeSeverity({
        downstreamCount,
        numberOfQueries:
          (detailRow.numberOfQueries as number | null | undefined) ?? null,
        lastQueriedAt:
          (detailRow.lastQueriedAt as number | null | undefined) ?? null,
        startKind: assetKind,
        now: Date.now(),
      });

      return {
        asset: {
          id: detailRow.id,
          kind: assetKind,
          name: detailRow.name,
          popularity: detailRow.popularity ?? null,
          isVerified: detailRow.isVerified ?? null,
          isDeprecated: detailRow.isDeprecated ?? null,
          ...(assetKind === "TABLE"
            ? {
                tableType: detailRow.tableType ?? null,
                numberOfQueries: detailRow.numberOfQueries ?? null,
                lastQueriedAt: detailRow.lastQueriedAt ?? null,
              }
            : {
                type: detailRow.type ?? null,
                folderPath: detailRow.folderPath ?? null,
              }),
        },
        ownership: shapeOwnership(detailRow),
        tags: shapeTags(detailRow),
        downstream: {
          maxDepthRequested: maxDepth,
          totalCount: downstreamCount,
          tableCount: downstreamTables.length,
          dashboardCount: downstreamDashboards.length,
          deprecatedCount: enriched.filter((a) => a.isDeprecated === true)
            .length,
          assets: enriched,
        },
        qualityChecks: summariseQualityStatuses(
          qualityRaw ? qualityRaw.getDataQualities : null
        ),
        severity,
      };
    }, client),
  };
}
