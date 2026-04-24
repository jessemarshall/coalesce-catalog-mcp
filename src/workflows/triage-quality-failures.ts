import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_DATA_QUALITIES,
  GET_TABLES_DETAIL_BATCH,
  GET_LINEAGES,
  GET_TABLES_SUMMARY,
} from "../catalog/operations.js";
import type {
  GetQualityChecksOutput,
  GetTablesOutput,
  GetLineagesOutput,
  Table,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import {
  ENRICHMENT_BATCH_SIZE,
  extractOwners,
  chunk,
  type Owners,
} from "./shared.js";

// ── Input schema ────────────────────────────────────────────────────────────

const TriageQualityFailuresInputShape = {
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
      "Capacity gate: refuse if the number of failing checks exceeds this limit. Default 500, max 500."
    ),
  includeUpstreamPointers: z
    .boolean()
    .optional()
    .describe(
      "Whether to fetch 1-hop upstream lineage per failing table to surface root-cause pointers. Default true."
    ),
};

// ── Constants ───────────────────────────────────────────────────────────────

const QUALITY_PAGE_SIZE = 100;
const QUALITY_MAX_PAGES = 50; // 100 * 50 = 5000 checks ceiling
const DEFAULT_MAX_FAILING = 500;
const LINEAGE_PAGE_SIZE = 500;
const LINEAGE_PAGES_MAX = 10;
const LINEAGE_FANOUT_PARALLELISM = 20;

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

interface TriageQueueEntry {
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
  upstreamPointers?: Array<{
    tableId: string;
    tableName: string | null;
  }>;
}

interface ByOwnerEntry {
  ownerEmail: string | null;
  ownerName: string | null;
  tableCount: number;
  totalFailures: number;
  tableIds: string[];
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
    if (rows.length < QUALITY_PAGE_SIZE) break;
    const total = resp.getDataQualities.totalCount;
    if (typeof total === "number" && Number.isFinite(total) && fetched >= total)
      break;
  }

  return failing;
}

async function fetchUpstreamParentIds(
  client: CatalogClient,
  tableId: string
): Promise<string[]> {
  const parentIds: string[] = [];
  for (let page = 0; page < LINEAGE_PAGES_MAX; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope: { childTableId: tableId },
        pagination: { nbPerPage: LINEAGE_PAGE_SIZE, page },
      }
    );
    for (const edge of resp.getLineages.data) {
      if (edge.parentTableId) parentIds.push(edge.parentTableId);
    }
    if (resp.getLineages.data.length < LINEAGE_PAGE_SIZE) break;
  }
  return parentIds;
}

async function fetchUpstreamForTables(
  client: CatalogClient,
  tableIds: string[]
): Promise<Map<string, string[]>> {
  const result = new Map<string, string[]>();
  const tasks = tableIds.map((id) => async () => {
    const parents = await fetchUpstreamParentIds(client, id);
    result.set(id, parents);
  });
  for (let i = 0; i < tasks.length; i += LINEAGE_FANOUT_PARALLELISM) {
    const slice = tasks.slice(i, i + LINEAGE_FANOUT_PARALLELISM);
    await Promise.all(slice.map((t) => t()));
  }
  return result;
}

async function hydrateTableNames(
  client: CatalogClient,
  tableIds: string[]
): Promise<Map<string, string | null>> {
  const names = new Map<string, string | null>();
  if (tableIds.length === 0) return names;
  const batches = chunk(tableIds, ENRICHMENT_BATCH_SIZE);
  for (const batch of batches) {
    const resp = await client.execute<{ getTables: GetTablesOutput }>(
      GET_TABLES_SUMMARY,
      {
        scope: { ids: batch },
        pagination: { nbPerPage: batch.length, page: 0 },
      }
    );
    for (const row of resp.getTables.data) {
      names.set(row.id, row.name ?? null);
    }
  }
  return names;
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineTriageQualityFailures(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_triage_quality_failures",
    config: {
      title: "Triage Quality Check Failures",
      description:
        "Triage all failing quality checks into a prioritised action queue. Fetches every quality check across the workspace (paginated, unscoped), filters for ALERT/WARNING status, then enriches each failing table with owner resolution, popularity (numberOfQueries), and optional 1-hop upstream lineage pointers for root-cause analysis.\n\n" +
        "Output: a `triageQueue` ranked by `triageScore` (popularity * failureCount) DESC, plus a `byOwner` grouping for \"who needs to act?\" routing. Upstream pointers surface the parent tables that feed into each failing table — useful for tracing failures to their root cause.\n\n" +
        "Capacity gate: refuses if failing checks exceed `maxFailingChecks` (default/max 500) with an actionable message. Use `statusFilter` to narrow to ALERT-only if the full set is too large.\n\n" +
        "One call replaces 4+ chained ones (quality list + table detail + lineage + owner resolution). Use when a user asks \"what quality failures matter most?\" or \"which tables are failing and who owns them?\".",
      inputSchema: TriageQualityFailuresInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const statusFilter =
        (args.statusFilter as string[] | undefined) ?? ["ALERT", "WARNING"];
      const maxFailingChecks =
        (args.maxFailingChecks as number | undefined) ?? DEFAULT_MAX_FAILING;
      const includeUpstream =
        (args.includeUpstreamPointers as boolean | undefined) ?? true;

      const statusSet = new Set(statusFilter);

      // Step 1+2: Paginate all quality checks, filter client-side
      const failing = await fetchAllFailingChecks(c, statusSet);

      // Step 3: Capacity gate
      if (failing.length > maxFailingChecks) {
        throw new Error(
          `${failing.length} failing quality checks exceed the ${maxFailingChecks}-check ` +
            `capacity gate. Narrow with statusFilter: ["ALERT"] to reduce volume, or ` +
            `increase maxFailingChecks (max 500).`
        );
      }

      if (failing.length === 0) {
        return {
          summary: {
            matchedChecks: 0,
            failingChecks: 0,
            affectedTables: 0,
            totalOwners: 0,
          },
          triageQueue: [],
          byOwner: {},
        };
      }

      // Step 4: Group failures by tableId
      const failuresByTable = new Map<string, FailingCheck[]>();
      for (const check of failing) {
        const existing = failuresByTable.get(check.tableId);
        if (existing) {
          existing.push(check);
        } else {
          failuresByTable.set(check.tableId, [check]);
        }
      }

      const affectedTableIds = Array.from(failuresByTable.keys());

      // Step 5: Batch-fetch table details for owner/popularity/name
      const tableDetails = new Map<string, Table>();
      const tableBatches = chunk(affectedTableIds, ENRICHMENT_BATCH_SIZE);
      for (const batch of tableBatches) {
        const resp = await c.execute<{ getTables: GetTablesOutput }>(
          GET_TABLES_DETAIL_BATCH,
          {
            scope: { ids: batch },
            pagination: { nbPerPage: batch.length, page: 0 },
          }
        );
        for (const row of resp.getTables.data) {
          tableDetails.set(row.id, row);
        }
      }

      // Step 6: Optionally fetch 1-hop upstream lineage per table
      let upstreamMap: Map<string, string[]> | null = null;
      let upstreamNames: Map<string, string | null> | null = null;
      if (includeUpstream) {
        upstreamMap = await fetchUpstreamForTables(c, affectedTableIds);

        // Collect all unique upstream table IDs for name hydration
        const allUpstreamIds = new Set<string>();
        for (const parentIds of upstreamMap.values()) {
          for (const pid of parentIds) allUpstreamIds.add(pid);
        }
        if (allUpstreamIds.size > 0) {
          upstreamNames = await hydrateTableNames(
            c,
            Array.from(allUpstreamIds)
          );
        }
      }

      // Step 7: Build triageQueue sorted by triageScore DESC
      const triageQueue: TriageQueueEntry[] = [];
      for (const [tableId, checks] of failuresByTable) {
        const detail = tableDetails.get(tableId);
        const tableName = detail?.name ?? "unknown";
        const popularity = detail?.numberOfQueries ?? 0;
        const failureCount = checks.length;
        const triageScore = popularity * failureCount;

        // Build tablePath from schema info if available
        let tablePath: string | null = null;
        if (detail?.schema?.name) {
          tablePath = `${detail.schema.name}.${tableName}`;
        }

        const owners: Owners = detail
          ? extractOwners(detail)
          : { userOwners: [], teamOwners: [] };

        const entry: TriageQueueEntry = {
          tableId,
          tableName,
          tablePath,
          popularity,
          failureCount,
          triageScore,
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
        };

        if (includeUpstream && upstreamMap) {
          const parentIds = upstreamMap.get(tableId) ?? [];
          entry.upstreamPointers = parentIds.map((pid) => ({
            tableId: pid,
            tableName: upstreamNames?.get(pid) ?? null,
          }));
        }

        triageQueue.push(entry);
      }

      triageQueue.sort((a, b) => b.triageScore - a.triageScore);

      // Step 8: Build byOwner grouping
      const byOwner: Record<string, ByOwnerEntry> = {};
      for (const entry of triageQueue) {
        // Group by each user owner
        if (entry.owners.userOwners.length > 0) {
          for (const owner of entry.owners.userOwners) {
            const key = owner.email ?? owner.userId;
            if (!byOwner[key]) {
              byOwner[key] = {
                ownerEmail: owner.email,
                ownerName: owner.fullName,
                tableCount: 0,
                totalFailures: 0,
                tableIds: [],
              };
            }
            byOwner[key].tableCount += 1;
            byOwner[key].totalFailures += entry.failureCount;
            byOwner[key].tableIds.push(entry.tableId);
          }
        }
        // Group by each team owner
        if (entry.owners.teamOwners.length > 0) {
          for (const team of entry.owners.teamOwners) {
            const key = `team:${team.name ?? team.teamId}`;
            if (!byOwner[key]) {
              byOwner[key] = {
                ownerEmail: null,
                ownerName: team.name,
                tableCount: 0,
                totalFailures: 0,
                tableIds: [],
              };
            }
            byOwner[key].tableCount += 1;
            byOwner[key].totalFailures += entry.failureCount;
            byOwner[key].tableIds.push(entry.tableId);
          }
        }
        // Track unowned tables under a synthetic key
        if (
          entry.owners.userOwners.length === 0 &&
          entry.owners.teamOwners.length === 0
        ) {
          const key = "__unowned__";
          if (!byOwner[key]) {
            byOwner[key] = {
              ownerEmail: null,
              ownerName: null,
              tableCount: 0,
              totalFailures: 0,
              tableIds: [],
            };
          }
          byOwner[key].tableCount += 1;
          byOwner[key].totalFailures += entry.failureCount;
          byOwner[key].tableIds.push(entry.tableId);
        }
      }

      // Count distinct owners (excluding __unowned__)
      const totalOwners = Object.keys(byOwner).filter(
        (k) => k !== "__unowned__"
      ).length;

      return {
        summary: {
          matchedChecks: failing.length,
          failingChecks: failing.length,
          affectedTables: affectedTableIds.length,
          totalOwners,
        },
        triageQueue,
        byOwner,
      };
    }, client),
  };
}
