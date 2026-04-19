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
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
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
// Bound per-depth fanout so a 2000-node frontier doesn't open 2000 concurrent
// HTTP requests. Matches the scorecard's QUALITY_PARALLELISM convention.
const LINEAGE_FANOUT_PARALLELISM = 20;

interface ReachedNode {
  kind: AssetKind;
  depth: number;
}

interface OwnerTeamRef {
  id: string;
  teamId: string | null;
  name: string | null;
}

interface EnrichedAsset {
  id: string;
  name: string | null;
  kind: AssetKind;
  popularity: number | null;
  isDeprecated: boolean | null;
  isVerified: boolean | null;
  depth: number;
  ownerUserCount: number;
  ownerTeamCount: number;
  teams: OwnerTeamRef[];
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

function pickChildId(edge: {
  childTableId?: string | null;
  childDashboardId?: string | null;
}): { id: string; kind: AssetKind } | null {
  if (edge.childTableId) return { id: edge.childTableId, kind: "TABLE" };
  if (edge.childDashboardId)
    return { id: edge.childDashboardId, kind: "DASHBOARD" };
  return null;
}

const LINEAGE_PAGE_SIZE = 500;
// Hard ceiling per parent node so a misbehaving server (e.g. paginating
// without ever decrementing) can't wedge the BFS forever. 10000 edges off
// one node is already an extreme hub; throwing here is closer to the
// completeness contract than silently capping.
const LINEAGE_PAGES_PER_NODE_MAX = 20;

async function fetchAllDownstreamEdges(
  client: CatalogClient,
  parentId: string,
  parentKind: AssetKind
): Promise<Array<{ childTableId?: string | null; childDashboardId?: string | null }>> {
  // Paginates exhaustively. The "complete or refuse" contract requires every
  // edge — single-page fetches at nbPerPage=500 silently drop anything past
  // the first page on hub tables.
  const all: Array<{ childTableId?: string | null; childDashboardId?: string | null }> = [];
  for (let page = 0; page < LINEAGE_PAGES_PER_NODE_MAX; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope:
          parentKind === "TABLE"
            ? { parentTableId: parentId }
            : { parentDashboardId: parentId },
        pagination: { nbPerPage: LINEAGE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getLineages.data;
    for (const r of rows) all.push(r);
    // Only the short-page signal is load-bearing. An off-by-one or post-filter
    // totalCount from the server would silently drop trailing pages — the
    // completeness contract is worth the extra (empty) request per hub.
    if (rows.length < LINEAGE_PAGE_SIZE) return all;
  }
  throw new Error(
    `Lineage pagination exceeded ${LINEAGE_PAGES_PER_NODE_MAX} pages for ` +
      `${parentKind.toLowerCase()} ${parentId} (>${LINEAGE_PAGES_PER_NODE_MAX * LINEAGE_PAGE_SIZE} edges). ` +
      `Refusing to produce a partial impact report; investigate the lineage data for this node.`
  );
}

async function traverseDownstream(
  client: CatalogClient,
  startId: string,
  startKind: AssetKind,
  maxDepth: number
): Promise<Map<string, ReachedNode>> {
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

    const edgesPerNode: Array<
      Array<{ childTableId?: string | null; childDashboardId?: string | null }>
    > = [];
    for (let i = 0; i < frontier.length; i += LINEAGE_FANOUT_PARALLELISM) {
      const slice = frontier.slice(i, i + LINEAGE_FANOUT_PARALLELISM);
      const sliceEdges = await Promise.all(
        slice.map((node) => fetchAllDownstreamEdges(client, node.id, node.kind))
      );
      edgesPerNode.push(...sliceEdges);
    }

    const nextFrontier: Array<{ id: string; kind: AssetKind }> = [];
    for (const edges of edgesPerNode) {
      for (const edge of edges) {
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

  return visited;
}

async function enrichByKind(
  client: CatalogClient,
  ids: string[],
  kind: AssetKind
): Promise<Map<string, Record<string, unknown>>> {
  // Uses the *_DETAIL_BATCH variants so each downstream node carries its
  // ownerEntities + teamOwnerEntities — needed to compute distinct owner
  // counts for the blast-radius report. Cost is one batched call per kind
  // (paginated by ENRICHMENT_BATCH_SIZE), not per asset.
  const map = new Map<string, Record<string, unknown>>();
  if (ids.length === 0) return map;

  for (let i = 0; i < ids.length; i += ENRICHMENT_BATCH_SIZE) {
    const slice = ids.slice(i, i + ENRICHMENT_BATCH_SIZE);
    if (kind === "TABLE") {
      const resp = await client.execute<{ getTables: GetTablesOutput }>(
        GET_TABLES_DETAIL_BATCH,
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
        GET_DASHBOARDS_DETAIL_BATCH,
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

function extractTeams(row: Record<string, unknown> | undefined): OwnerTeamRef[] {
  if (!row || !Array.isArray(row.teamOwnerEntities)) return [];
  // Filter out rows whose teamId is null — the binding exists but doesn't
  // point at a team (orphaned binding). Keeps ownerTeamCount aligned with the
  // scorecard's hasOwner logic; otherwise unownedCount / distinctOwnerTeamCount
  // disagree with the scorecard on the same assets.
  return (row.teamOwnerEntities as Array<Record<string, unknown>>)
    .filter((t) => t.teamId != null)
    .map((t) => {
      const team = t.team as Record<string, unknown> | undefined;
      return {
        id: t.id as string,
        teamId: t.teamId as string,
        name: (team?.name as string | null) ?? null,
      };
    });
}

function userOwnerCount(row: Record<string, unknown> | undefined): number {
  // Filter out owner records whose userId is null — the relationship row exists
  // but doesn't actually point at a user (orphaned binding). For governance
  // purposes "userId is null" is functionally unowned, not silently owned.
  if (!row || !Array.isArray(row.ownerEntities)) return 0;
  return (row.ownerEntities as Array<Record<string, unknown>>).filter(
    (o) => o.userId != null
  ).length;
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
  if (
    opts.startKind === "TABLE" &&
    typeof opts.lastQueriedAt === "number" &&
    Number.isFinite(opts.lastQueriedAt)
  ) {
    // Clamp negative ages (clock skew on the extraction worker can leave a
    // future timestamp; we don't want to award full recency for that).
    const ageDays = Math.max(
      0,
      (opts.now - opts.lastQueriedAt) / (1000 * 60 * 60 * 24)
    );
    if (ageDays < 7) recencyPts = 20;
    else if (ageDays < 30) recencyPts = 12;
    else if (ageDays < 90) recencyPts = 6;
    recencyDetail = `last queried ${Math.round(ageDays)} days ago`;
  } else if (opts.startKind === "TABLE") {
    recencyDetail = "never queried";
  }

  const score = Math.round(downstreamPts + queryVolumePts + recencyPts);
  // Bucket boundaries: 0-29 low, 30-59 medium, 60-100 high.
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
  byStatus: {
    SUCCESS: number;
    WARNING: number;
    ALERT: number;
    OTHER: number;
  };
  byStatusSampledFrom: number;
  byStatusComplete: boolean;
} | null {
  if (!data) return null;
  // Always reconcile to totalCount: any new/unknown status (e.g. ERROR, MUTED)
  // lands in OTHER so the byStatus sum equals data.data.length, and
  // byStatusSampledFrom plus byStatusComplete tell the caller whether the
  // sample equals the full population. Without this, an LLM consumer reading
  // sum(byStatus) < totalCount infers missing checks that aren't actually
  // missing.
  const counts = { SUCCESS: 0, WARNING: 0, ALERT: 0, OTHER: 0 };
  for (const row of data.data as Array<{ status?: string }>) {
    const s = row.status;
    if (s === "SUCCESS" || s === "WARNING" || s === "ALERT") counts[s] += 1;
    else counts.OTHER += 1;
  }
  return {
    totalCount: data.totalCount,
    byStatus: counts,
    byStatusSampledFrom: data.data.length,
    byStatusComplete: data.data.length === data.totalCount,
  };
}

export function defineAssessImpact(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_assess_impact",
    config: {
      title: "Assess Blast Radius (Deprecation Impact Report)",
      description:
        "Composed deprecation-impact report for a TABLE or DASHBOARD: walks downstream lineage to the requested depth, batch-enriches every reached asset with name + popularity + ownership (teams + user count), attaches the starting asset's ownership + tags + quality-check coverage, and returns a 0-100 severity score with explicit per-component rationale.\n\n" +
        "Each downstream row carries `teams: [{id, teamId, name}]` plus `ownerUserCount` / `ownerTeamCount`. The aggregate exposes `distinctOwnerTeamCount` (how many independent teams need to be coordinated with for a deprecation) and `unownedCount` (orphaned downstream assets — likely no one will notice they break, but also no one will fix them).\n\n" +
        "**Completeness contract:** at any given depth, the report is exhaustive — there is no silent truncation. If the downstream graph at depth 2 exceeds 2000 distinct nodes (or 500 at depth 3), the tool refuses with an actionable error rather than returning a partial answer. Use depth=1 first on wide hubs to identify hotspots and assess them individually.\n\n" +
        "Severity rubric (deterministic, transparent in `rationale[]`):\n" +
        "  - downstream_impact: 0-60 pts, log-scaled by reached-asset count\n" +
        "  - query_volume: 0-20 pts, log-scaled by numberOfQueries (TABLE-only)\n" +
        "  - query_recency: 0-20 pts, banded by lastQueriedAt age (<7d / <30d / <90d)\n" +
        "Buckets: 0-29 low, 30-59 medium, 60-100 high.\n\n" +
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
      for (const [id, node] of traversal) {
        if (id === assetId) continue;
        if (node.kind === "TABLE") reachedTableIds.push(id);
        else reachedDashboardIds.push(id);
      }

      const [tableEnrichment, dashboardEnrichment] = await Promise.all([
        enrichByKind(c, reachedTableIds, "TABLE"),
        enrichByKind(c, reachedDashboardIds, "DASHBOARD"),
      ]);

      // Surface any downstream id we know exists (it came back from lineage)
      // but couldn't enrich. Conflating "no row returned" with "no owners"
      // would silently inflate unownedCount with assets the API simply
      // refused to detail. Throw so withErrorHandling sets isError: true —
      // the same shape the scorecard uses for its refusals, so consumers
      // filtering on isError handle both consistently.
      const missing: Array<{ id: string; kind: AssetKind }> = [];
      for (const [id, node] of traversal) {
        if (id === assetId) continue;
        const row =
          node.kind === "TABLE"
            ? tableEnrichment.get(id)
            : dashboardEnrichment.get(id);
        if (!row) missing.push({ id, kind: node.kind });
      }
      if (missing.length > 0) {
        const sample = missing
          .slice(0, 5)
          .map((m) => `${m.kind}:${m.id}`)
          .join(", ");
        throw new Error(
          `Detail enrichment returned no row for ${missing.length} downstream ` +
            `asset(s) reached via lineage (sample: ${sample}). The completeness ` +
            `contract requires every reached node to be enriched; re-run after ` +
            `the catalog catches up, or scope the assessment to a sub-tree.`
        );
      }

      const enriched: EnrichedAsset[] = [];
      for (const [id, node] of traversal) {
        if (id === assetId) continue;
        const row =
          node.kind === "TABLE"
            ? tableEnrichment.get(id)!
            : dashboardEnrichment.get(id)!;
        const teams = extractTeams(row);
        enriched.push({
          id,
          name: (row.name as string | null) ?? null,
          kind: node.kind,
          popularity: (row.popularity as number | null) ?? null,
          isDeprecated: (row.isDeprecated as boolean | null) ?? null,
          isVerified: (row.isVerified as boolean | null) ?? null,
          depth: node.depth,
          ownerUserCount: userOwnerCount(row),
          ownerTeamCount: teams.length,
          teams,
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

      // Distinct owner-team count = how many independent teams will be
      // affected by deprecating this asset. Counts each unique teamId once
      // across the entire downstream graph.
      const distinctTeamIds = new Set<string>();
      let unownedAssetCount = 0;
      for (const a of enriched) {
        if (a.ownerTeamCount === 0 && a.ownerUserCount === 0) unownedAssetCount += 1;
        for (const t of a.teams) {
          if (t.teamId) distinctTeamIds.add(t.teamId);
        }
      }

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
          unownedCount: unownedAssetCount,
          distinctOwnerTeamCount: distinctTeamIds.size,
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
