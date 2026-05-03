import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_DATA_QUALITIES,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_LINEAGES,
  GET_TABLES_DETAIL_BATCH,
} from "../catalog/operations.js";
import type {
  GetDashboardsOutput,
  GetLineagesOutput,
  GetQualityChecksOutput,
  GetTablesOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import {
  ENRICHMENT_BATCH_SIZE,
  chunk,
  extractOwners,
  type Owners,
} from "./shared.js";

// ── Input schema ────────────────────────────────────────────────────────────

const AssessImpactInputShape = {
  statusFilter: z
    .array(z.enum(["ALERT", "WARNING"]))
    .optional()
    .describe(
      'Which quality-check statuses to include. Default: ["ALERT", "WARNING"].'
    ),
  maxFailingChecks: z
    .number()
    .int()
    .min(1)
    .max(500)
    .optional()
    .describe(
      "Capacity gate on the failing-check input set. Default 500, max 500."
    ),
  maxDownstreamDepth: z
    .number()
    .int()
    .min(1)
    .max(3)
    .optional()
    .describe(
      "How many lineage hops downstream of each failing table to traverse looking for affected dashboards. 1 = immediate children only (cheapest, completes always). 2 = children-of-children (refuses if total frontier exceeds 2000 nodes). 3 = deep traversal (refuses if total frontier exceeds 500). Default 2 — most quality-failure → dashboard impact paths land within two hops."
    ),
  maxAffectedDashboards: z
    .number()
    .int()
    .min(1)
    .max(500)
    .optional()
    .describe(
      "Capacity gate on the output dashboard set. Refuses with an actionable message if the BFS reaches more dashboards than this cap. Default 200, max 500. If you hit this, narrow `statusFilter` to ['ALERT'] or reduce `maxDownstreamDepth`."
    ),
  criticalTagLabels: z
    .array(z.string())
    .optional()
    .describe(
      "Tag labels that should boost a dashboard's criticality score (case-insensitive substring match against each tag's label). Default ['critical','production','p0','tier1','executive']. Pass [] to disable criticality boosting and rank purely by popularity * failureCount."
    ),
};

// ── Constants ───────────────────────────────────────────────────────────────

const QUALITY_PAGE_SIZE = 100;
const QUALITY_MAX_PAGES = 50;
const DEFAULT_MAX_FAILING = 500;
const DEFAULT_MAX_AFFECTED_DASHBOARDS = 200;
const DEFAULT_MAX_DEPTH = 2;
const LINEAGE_PAGE_SIZE = 500;
const LINEAGE_PAGES_PER_NODE_MAX = 20;
const LINEAGE_FANOUT_PARALLELISM = 20;
// Width caps mirror propagate-metadata / assess-impact: refuse rather than
// silently truncating on hub nodes. The traversal aggregates frontiers across
// every failing-table tree, so the cap applies to the union not per-tree.
const FRONTIER_WIDTH_CAPS: Record<number, number> = {
  2: 2000,
  3: 500,
};
const DEFAULT_CRITICAL_TAGS = [
  "critical",
  "production",
  "p0",
  "tier1",
  "executive",
];

// ── Internal types ──────────────────────────────────────────────────────────

interface FailingCheck {
  id: string;
  name: string;
  status: string;
  result: string | null;
  externalId: string;
  runAt: number | null;
  url: string | null;
  tableId: string;
}

interface FailingTable {
  tableId: string;
  tableName: string;
  tablePath: string | null;
  popularity: number;
  failureCount: number;
  triageScore: number;
  owners: Owners;
  failures: Array<{
    id: string;
    name: string;
    status: string;
    result: string | null;
    externalId: string;
    runAt: number | null;
    url: string | null;
  }>;
}

interface DashboardReachedFrom {
  tableId: string;
  tableName: string;
  failureCount: number;
  depth: number;
}

interface DashboardImpactRow {
  dashboardId: string;
  dashboardName: string | null;
  dashboardPath: string | null;
  popularity: number;
  criticalityScore: number;
  criticalTags: string[];
  owners: Owners;
  affectedByFailingTables: DashboardReachedFrom[];
  totalFailureCount: number;
  blastRadiusScore: number;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

async function fetchAllFailingChecks(
  client: CatalogClient,
  statusSet: Set<string>
): Promise<FailingCheck[]> {
  const failing: FailingCheck[] = [];
  for (let page = 0; page < QUALITY_MAX_PAGES; page++) {
    const resp = await client.execute<{
      getDataQualities: GetQualityChecksOutput;
    }>(GET_DATA_QUALITIES, {
      pagination: { nbPerPage: QUALITY_PAGE_SIZE, page },
    });
    for (const row of resp.getDataQualities.data) {
      if (statusSet.has(row.status)) {
        failing.push({
          id: row.id,
          name: row.name,
          status: row.status,
          result: row.result ?? null,
          externalId: row.externalId,
          runAt: row.runAt ?? null,
          url: row.url ?? null,
          tableId: row.tableId,
        });
      }
    }
    const fetched = (page + 1) * QUALITY_PAGE_SIZE;
    const rows = resp.getDataQualities.data;
    if (rows.length < QUALITY_PAGE_SIZE) return failing;
    const total = resp.getDataQualities.totalCount;
    if (typeof total === "number" && Number.isFinite(total) && fetched >= total)
      return failing;
  }
  // Workspace has more quality checks than the per-call ceiling can reach.
  // Filtering happens client-side, so any failing checks past the ceiling
  // would silently fall out of the dashboard-impact report. Match the
  // "complete or refuse" contract used by triage-quality-failures rather
  // than emit a partial answer.
  //
  // The tool's input schema has no tableId/tableIds parameter, so the only
  // narrowing axis exposed is statusFilter. Per-table triage requires
  // dropping out of this tool entirely and using catalog_search_quality_checks
  // (which DOES accept a tableId scope).
  throw new Error(
    `Quality check pagination exceeded ${QUALITY_MAX_PAGES} pages ` +
      `(>${QUALITY_MAX_PAGES * QUALITY_PAGE_SIZE} total checks scanned). ` +
      `Refusing to emit a partial dashboard-impact report — failing checks past ` +
      `the ceiling would be silently dropped. Narrow with statusFilter: ['ALERT'], ` +
      `or triage manually with catalog_search_quality_checks per tableId ` +
      `instead of this tool.`
  );
}

async function enrichTables(
  client: CatalogClient,
  ids: string[]
): Promise<Map<string, Record<string, unknown>>> {
  const map = new Map<string, Record<string, unknown>>();
  if (ids.length === 0) return map;
  for (const batch of chunk(ids, ENRICHMENT_BATCH_SIZE)) {
    const resp = await client.execute<{ getTables: GetTablesOutput }>(
      GET_TABLES_DETAIL_BATCH,
      {
        scope: { ids: batch },
        pagination: { nbPerPage: batch.length, page: 0 },
      }
    );
    for (const row of resp.getTables.data) {
      map.set(row.id, row as unknown as Record<string, unknown>);
    }
  }
  return map;
}

async function enrichDashboards(
  client: CatalogClient,
  ids: string[]
): Promise<Map<string, Record<string, unknown>>> {
  const map = new Map<string, Record<string, unknown>>();
  if (ids.length === 0) return map;
  for (const batch of chunk(ids, ENRICHMENT_BATCH_SIZE)) {
    const resp = await client.execute<{ getDashboards: GetDashboardsOutput }>(
      GET_DASHBOARDS_DETAIL_BATCH,
      {
        scope: { ids: batch },
        pagination: { nbPerPage: batch.length, page: 0 },
      }
    );
    for (const row of resp.getDashboards.data) {
      map.set(row.id, row as unknown as Record<string, unknown>);
    }
  }
  return map;
}

interface FrontierEdges {
  childTableIds: string[];
  childDashboardIds: string[];
}

async function fetchAllDownstreamEdges(
  client: CatalogClient,
  parentTableId: string
): Promise<FrontierEdges> {
  const childTableIds: string[] = [];
  const childDashboardIds: string[] = [];
  for (let page = 0; page < LINEAGE_PAGES_PER_NODE_MAX; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope: { parentTableId },
        pagination: { nbPerPage: LINEAGE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getLineages.data;
    for (const e of rows) {
      if (e.childTableId) childTableIds.push(e.childTableId);
      else if (e.childDashboardId) childDashboardIds.push(e.childDashboardId);
    }
    if (rows.length < LINEAGE_PAGE_SIZE)
      return { childTableIds, childDashboardIds };
  }
  throw new Error(
    `Lineage pagination exceeded ${LINEAGE_PAGES_PER_NODE_MAX} pages for ` +
      `table ${parentTableId} (>${LINEAGE_PAGES_PER_NODE_MAX * LINEAGE_PAGE_SIZE} downstream edges). ` +
      `Refusing to produce a partial impact report.`
  );
}

interface TraversalResult {
  // dashboardId -> list of (failingTableId, depth) pairs that reach it
  dashboardReach: Map<string, Array<{ failingTableId: string; depth: number }>>;
  // total intermediate tables visited (excluding source failing tables)
  intermediateTableCount: number;
}

async function traverseDownstreamPerFailingTable(
  client: CatalogClient,
  failingTableIds: string[],
  maxDepth: number,
  maxAffectedDashboards: number
): Promise<TraversalResult> {
  const dashboardReach = new Map<
    string,
    Array<{ failingTableId: string; depth: number }>
  >();
  const intermediateTables = new Set<string>();

  // For each failing table, do a per-source BFS so we can attribute each
  // reached dashboard to the source(s) that flow into it.
  for (const sourceTableId of failingTableIds) {
    const visitedAtDepth = new Map<string, number>();
    visitedAtDepth.set(sourceTableId, 0);
    let frontier: string[] = [sourceTableId];

    for (let depth = 1; depth <= maxDepth; depth++) {
      if (depth >= 2) {
        const cap = FRONTIER_WIDTH_CAPS[depth];
        if (frontier.length > cap) {
          throw new Error(
            `Lineage frontier too wide at depth ${depth} for failing table ${sourceTableId}: ` +
              `${frontier.length} nodes exceeds the ${cap}-node cap. Reduce ` +
              `maxDownstreamDepth, or narrow statusFilter to ['ALERT'] to shrink the failing-table set.`
          );
        }
      }

      const edgesPerNode: FrontierEdges[] = [];
      for (let i = 0; i < frontier.length; i += LINEAGE_FANOUT_PARALLELISM) {
        const slice = frontier.slice(i, i + LINEAGE_FANOUT_PARALLELISM);
        const sliceResults = await Promise.all(
          slice.map((node) => fetchAllDownstreamEdges(client, node))
        );
        edgesPerNode.push(...sliceResults);
      }

      const nextFrontier: string[] = [];
      for (const r of edgesPerNode) {
        for (const dashId of r.childDashboardIds) {
          const list = dashboardReach.get(dashId) ?? [];
          list.push({ failingTableId: sourceTableId, depth });
          dashboardReach.set(dashId, list);
          if (dashboardReach.size > maxAffectedDashboards) {
            throw new Error(
              `Reached more than ${maxAffectedDashboards} dashboards while ` +
                `traversing downstream of ${failingTableIds.length} failing table(s). ` +
                `Narrow with statusFilter: ['ALERT'], reduce maxDownstreamDepth, ` +
                `or raise maxAffectedDashboards (max 500).`
            );
          }
        }
        for (const childId of r.childTableIds) {
          if (visitedAtDepth.has(childId)) continue;
          visitedAtDepth.set(childId, depth);
          // Track intermediate tables across all source trees for the summary.
          if (!failingTableIds.includes(childId))
            intermediateTables.add(childId);
          nextFrontier.push(childId);
        }
      }

      if (nextFrontier.length === 0) break;
      frontier = nextFrontier;
    }
  }

  return {
    dashboardReach,
    intermediateTableCount: intermediateTables.size,
  };
}

function extractDashboardPath(row: Record<string, unknown>): string | null {
  const folderPath = row.folderPath;
  const name = row.name;
  const source = row.source as Record<string, unknown> | undefined;
  const sourceName = source?.name;
  const parts: string[] = [];
  if (typeof sourceName === "string" && sourceName.length > 0)
    parts.push(sourceName);
  if (typeof folderPath === "string" && folderPath.length > 0)
    parts.push(folderPath);
  if (typeof name === "string" && name.length > 0) parts.push(name);
  return parts.length > 0 ? parts.join("/") : null;
}

function extractCriticalTags(
  row: Record<string, unknown>,
  criticalLabelsLower: string[]
): { criticalTags: string[]; allTags: string[] } {
  const allTags: string[] = [];
  const criticalTags: string[] = [];
  if (!Array.isArray(row.tagEntities)) return { criticalTags, allTags };
  for (const t of row.tagEntities as Array<Record<string, unknown>>) {
    const tag = t.tag as Record<string, unknown> | undefined;
    const label = tag?.label;
    if (typeof label !== "string" || label.length === 0) continue;
    allTags.push(label);
    const lower = label.toLowerCase();
    for (const needle of criticalLabelsLower) {
      if (lower.includes(needle)) {
        criticalTags.push(label);
        break;
      }
    }
  }
  return { criticalTags, allTags };
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineAssessQualityFailureDashboardImpact(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_assess_quality_failure_dashboard_impact",
    config: {
      title: "Assess Quality-Failure Dashboard Impact",
      description:
        "Extends `catalog_triage_quality_failures` by traversing forward through lineage to identify affected BI dashboards/reports. Answers \"which dashboards are reading data downstream of failing quality checks, and which steward needs to act first?\".\n\n" +
        "Composes: paginated `getDataQualities` (filter by status) → per-failing-table `getTables` enrichment (popularity + owners) → per-failing-table downstream lineage BFS (table-and-dashboard children, depth-limited) → `getDashboards` enrichment (popularity + tags + owners) → per-dashboard impact aggregation ranked by `blastRadiusScore = dashboardPopularity * totalFailureCount * (1 + criticalityScore)`.\n\n" +
        "Output: a `triageQueue` of failing tables (same shape as `catalog_triage_quality_failures` minus upstream pointers) and a new `dashboardImpact` array — each row pins the affected dashboard, the failing tables that reach it (with depth), the dashboard's popularity + critical-tag matches + owners, and a composite blast-radius score.\n\n" +
        "Capacity gates (refuse rather than truncate):\n" +
        "  - failing-check set size: `maxFailingChecks` (default/max 500)\n" +
        "  - lineage frontier width per source tree: 2000 at depth 2, 500 at depth 3\n" +
        "  - reached dashboard set size: `maxAffectedDashboards` (default 200, max 500)\n\n" +
        "Use this when a user asks \"which BI reports are at risk because of failing checks?\" or \"who do we need to email about the data quality issues right now?\". For root-cause analysis (which UPSTREAM tables feed failures), use `catalog_triage_quality_failures` with `includeUpstreamPointers: true` instead.",
      inputSchema: AssessImpactInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const statusFilter =
        (args.statusFilter as string[] | undefined) ?? ["ALERT", "WARNING"];
      const maxFailingChecks =
        (args.maxFailingChecks as number | undefined) ?? DEFAULT_MAX_FAILING;
      const maxDownstreamDepth =
        (args.maxDownstreamDepth as number | undefined) ?? DEFAULT_MAX_DEPTH;
      const maxAffectedDashboards =
        (args.maxAffectedDashboards as number | undefined) ??
        DEFAULT_MAX_AFFECTED_DASHBOARDS;
      const criticalTagLabels =
        (args.criticalTagLabels as string[] | undefined) ??
        DEFAULT_CRITICAL_TAGS;
      const criticalLabelsLower = criticalTagLabels
        .map((l) => l.toLowerCase())
        .filter((l) => l.length > 0);

      // Step 1: paginate all quality checks, filter client-side.
      const statusSet = new Set(statusFilter);
      const failing = await fetchAllFailingChecks(c, statusSet);

      // Step 2: capacity gate.
      if (failing.length > maxFailingChecks) {
        throw new Error(
          `${failing.length} failing quality checks exceed the ${maxFailingChecks}-check ` +
            `capacity gate. Narrow with statusFilter: ['ALERT'] or increase maxFailingChecks (max 500).`
        );
      }
      if (failing.length === 0) {
        return {
          summary: {
            failingChecks: 0,
            affectedTables: 0,
            affectedDashboards: 0,
            intermediateTables: 0,
          },
          triageQueue: [],
          dashboardImpact: [],
        };
      }

      // Step 3: group failures by tableId.
      const failuresByTable = new Map<string, FailingCheck[]>();
      for (const check of failing) {
        const list = failuresByTable.get(check.tableId);
        if (list) list.push(check);
        else failuresByTable.set(check.tableId, [check]);
      }
      const failingTableIds = Array.from(failuresByTable.keys());

      // Step 4: enrich failing tables with detail (owners + popularity + name).
      const failingTableDetails = await enrichTables(c, failingTableIds);
      const failingTables: FailingTable[] = [];
      for (const [tableId, checks] of failuresByTable) {
        const detail = failingTableDetails.get(tableId);
        const tableName = (detail?.name as string | null) ?? "unknown";
        const popularity = (detail?.numberOfQueries as number | null) ?? 0;
        const failureCount = checks.length;
        const schema = detail?.schema as Record<string, unknown> | undefined;
        const schemaName = schema?.name as string | undefined;
        const tablePath = schemaName ? `${schemaName}.${tableName}` : null;
        const owners: Owners = detail
          ? extractOwners(detail)
          : { userOwners: [], teamOwners: [] };
        failingTables.push({
          tableId,
          tableName,
          tablePath,
          popularity,
          failureCount,
          triageScore: popularity * failureCount,
          owners,
          failures: checks.map((ch) => ({
            id: ch.id,
            name: ch.name,
            status: ch.status,
            result: ch.result,
            externalId: ch.externalId,
            runAt: ch.runAt,
            url: ch.url,
          })),
        });
      }
      failingTables.sort((a, b) => b.triageScore - a.triageScore);

      // Step 5: BFS downstream from each failing table; collect reached
      // dashboards and the failing-tables-of-origin that reach each one.
      const { dashboardReach, intermediateTableCount } =
        await traverseDownstreamPerFailingTable(
          c,
          failingTableIds,
          maxDownstreamDepth,
          maxAffectedDashboards
        );

      if (dashboardReach.size === 0) {
        return {
          summary: {
            failingChecks: failing.length,
            affectedTables: failingTableIds.length,
            affectedDashboards: 0,
            intermediateTables: intermediateTableCount,
          },
          triageQueue: failingTables,
          dashboardImpact: [],
        };
      }

      // Step 6: enrich every reached dashboard with detail (popularity + tags
      // + owners + folderPath + source).
      const reachedDashboardIds = Array.from(dashboardReach.keys());
      const dashboardDetails = await enrichDashboards(c, reachedDashboardIds);

      // Build a quick lookup of failing-table summaries for impact rows.
      const failingByTableId = new Map<string, FailingTable>();
      for (const ft of failingTables) failingByTableId.set(ft.tableId, ft);

      // Step 7: build dashboardImpact rows.
      const dashboardImpact: DashboardImpactRow[] = [];
      for (const [dashboardId, reached] of dashboardReach) {
        const detail = dashboardDetails.get(dashboardId);
        const dashboardName = (detail?.name as string | null) ?? null;
        const dashboardPath = detail ? extractDashboardPath(detail) : null;
        const popularity = (detail?.popularity as number | null) ?? 0;
        const owners: Owners = detail
          ? extractOwners(detail)
          : { userOwners: [], teamOwners: [] };
        const { criticalTags } = detail
          ? extractCriticalTags(detail, criticalLabelsLower)
          : { criticalTags: [] as string[] };

        // De-duplicate (failingTableId, depth) — a dashboard reached via
        // multiple paths from the same source records only the shortest depth.
        const shortestByTable = new Map<string, number>();
        for (const r of reached) {
          const prev = shortestByTable.get(r.failingTableId);
          if (prev === undefined || r.depth < prev)
            shortestByTable.set(r.failingTableId, r.depth);
        }

        const affected: DashboardReachedFrom[] = [];
        let totalFailureCount = 0;
        for (const [failingTableId, depth] of shortestByTable) {
          const ft = failingByTableId.get(failingTableId);
          if (!ft) continue; // defensive — should always be present
          affected.push({
            tableId: failingTableId,
            tableName: ft.tableName,
            failureCount: ft.failureCount,
            depth,
          });
          totalFailureCount += ft.failureCount;
        }
        // Sort affected sources by depth ASC, failureCount DESC for readability.
        affected.sort(
          (a, b) => a.depth - b.depth || b.failureCount - a.failureCount
        );

        const criticalityScore = criticalTags.length;
        const blastRadiusScore =
          popularity * totalFailureCount * (1 + criticalityScore);

        dashboardImpact.push({
          dashboardId,
          dashboardName,
          dashboardPath,
          popularity,
          criticalityScore,
          criticalTags,
          owners,
          affectedByFailingTables: affected,
          totalFailureCount,
          blastRadiusScore,
        });
      }
      dashboardImpact.sort((a, b) => b.blastRadiusScore - a.blastRadiusScore);

      return {
        summary: {
          failingChecks: failing.length,
          affectedTables: failingTableIds.length,
          affectedDashboards: dashboardImpact.length,
          intermediateTables: intermediateTableCount,
        },
        triageQueue: failingTables,
        dashboardImpact,
      };
    }, client),
  };
}
