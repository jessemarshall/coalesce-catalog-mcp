import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLES_DETAIL_BATCH,
  GET_DATA_QUALITIES,
  GET_LINEAGES,
} from "../catalog/operations.js";
import type {
  GetQualityChecksOutput,
  GetTablesOutput,
  GetLineagesOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import { extractOwners, hasOwner, chunk, type Owners } from "./shared.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TABLE_HARD_CAP = 500;
const TABLE_PAGE_SIZE = 100;
const FAILING_CHECKS_CAP = 500;
const QUALITY_PAGE_SIZE = 100;
const QUALITY_PAGES_PER_TABLE_MAX = 10;
const QUALITY_PARALLELISM = 20;
const LINEAGE_PARALLELISM = 20;
const LINEAGE_PAGE_SIZE = 500;
const LINEAGE_PAGES_MAX = 5;
const ENRICHMENT_BATCH_SIZE = 500;

// ---------------------------------------------------------------------------
// Input schema
// ---------------------------------------------------------------------------

const TriageInputShape = {
  databaseId: z
    .string()
    .optional()
    .describe("Scope the triage to tables in this database UUID."),
  schemaId: z
    .string()
    .optional()
    .describe("Scope the triage to tables in this schema UUID."),
  tableIds: z
    .array(z.string())
    .optional()
    .describe(
      "Scope the triage to an explicit list of table UUIDs (max 500). " +
        "Mutually exclusive with databaseId/schemaId."
    ),
};

// ---------------------------------------------------------------------------
// Scope validation (same pattern as governance-scorecard)
// ---------------------------------------------------------------------------

interface ScopedFilter {
  field: "tableIds" | "schemaId" | "databaseId";
  filter: { ids?: string[]; schemaId?: string; databaseId?: string };
}

function pickScope(args: {
  databaseId?: unknown;
  schemaId?: unknown;
  tableIds?: unknown;
}): ScopedFilter | null {
  const provided: string[] = [];
  const hasTableIds = Array.isArray(args.tableIds) && args.tableIds.length > 0;
  const hasSchemaId = typeof args.schemaId === "string";
  const hasDatabaseId = typeof args.databaseId === "string";
  if (hasTableIds) provided.push("tableIds");
  if (hasSchemaId) provided.push("schemaId");
  if (hasDatabaseId) provided.push("databaseId");
  if (provided.length > 1) {
    throw new Error(
      `Multiple scope fields supplied (${provided.join(", ")}); pass exactly one ` +
        `of databaseId, schemaId, or tableIds.`
    );
  }
  if (hasTableIds) {
    const ids = args.tableIds as string[];
    if (ids.length > TABLE_HARD_CAP) {
      throw new Error(
        `tableIds (${ids.length}) exceeds the ${TABLE_HARD_CAP}-table cap. ` +
          `Split into smaller batches.`
      );
    }
    return { field: "tableIds", filter: { ids } };
  }
  if (hasSchemaId) {
    return { field: "schemaId", filter: { schemaId: args.schemaId as string } };
  }
  if (hasDatabaseId) {
    return {
      field: "databaseId",
      filter: { databaseId: args.databaseId as string },
    };
  }
  return null;
}

// ---------------------------------------------------------------------------
// Quality check types
// ---------------------------------------------------------------------------

interface QualityCheckRecord {
  id: string;
  name: string;
  status: string;
  result: string | null;
  source: string | null;
  ownerEmail: string | null;
  externalId: string | null;
  columnId: string | null;
  runAt: string | null;
  url: string | null;
}

interface FailingTable {
  id: string;
  name: string | null;
  popularity: number | null;
  numberOfQueries: number | null;
  isVerified: boolean | null;
  isDeprecated: boolean | null;
  schemaId: string | null;
  owners: Owners;
  failureCount: number;
  alertCount: number;
  warningCount: number;
  checks: QualityCheckRecord[];
  upstreamSources: UpstreamSource[];
}

interface UpstreamSource {
  id: string;
  kind: "TABLE" | "DASHBOARD";
  name: string | null;
  owners: Owners;
}

interface OwnerGroup {
  ownerKey: string;
  ownerType: "user" | "team" | "unowned";
  ownerName: string | null;
  tables: Array<{
    tableId: string;
    tableName: string | null;
    failureCount: number;
    triageScore: number;
  }>;
  totalFailures: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function fetchAllTablesInScope(
  client: CatalogClient,
  scope: ScopedFilter
): Promise<Array<Record<string, unknown>>> {
  const firstPage = await client.execute<{ getTables: GetTablesOutput }>(
    GET_TABLES_DETAIL_BATCH,
    {
      scope: scope.filter,
      sorting: [{ sortingKey: "popularity", direction: "DESC" }],
      pagination: { nbPerPage: TABLE_PAGE_SIZE, page: 0 },
    }
  );
  const totalCount = firstPage.getTables.totalCount;
  if (typeof totalCount !== "number" || !Number.isFinite(totalCount)) {
    throw new Error(
      `getTables returned non-numeric totalCount (${String(totalCount)}) ` +
        `for triage scope=${scope.field}; cannot establish table universe.`
    );
  }
  if (totalCount > TABLE_HARD_CAP) {
    throw new Error(
      `Scope resolves to ${totalCount} tables (scoped by ${scope.field}), ` +
        `exceeding the ${TABLE_HARD_CAP}-table cap. ` +
        `Narrow via schemaId or split into smaller tableIds batches.`
    );
  }
  const tables: Array<Record<string, unknown>> = [
    ...(firstPage.getTables.data as Array<Record<string, unknown>>),
  ];
  const expectedPages = Math.ceil(totalCount / TABLE_PAGE_SIZE);
  for (let page = 1; page < expectedPages; page++) {
    const resp = await client.execute<{ getTables: GetTablesOutput }>(
      GET_TABLES_DETAIL_BATCH,
      {
        scope: scope.filter,
        sorting: [{ sortingKey: "popularity", direction: "DESC" }],
        pagination: { nbPerPage: TABLE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getTables.data as Array<Record<string, unknown>>;
    tables.push(...rows);
    if (rows.length < TABLE_PAGE_SIZE) break;
  }
  if (tables.length < Math.min(totalCount, TABLE_HARD_CAP)) {
    throw new Error(
      `Table pagination returned ${tables.length} rows for scope=${scope.field} ` +
        `but totalCount reported ${totalCount}. Refusing to emit a partial triage.`
    );
  }
  return tables;
}

async function fetchQualityChecksForTable(
  client: CatalogClient,
  tableId: string
): Promise<QualityCheckRecord[]> {
  const failing: QualityCheckRecord[] = [];
  for (let page = 0; page < QUALITY_PAGES_PER_TABLE_MAX; page++) {
    const resp = await client.execute<{
      getDataQualities: GetQualityChecksOutput;
    }>(GET_DATA_QUALITIES, {
      scope: { tableId },
      pagination: { nbPerPage: QUALITY_PAGE_SIZE, page },
    });
    const rows = resp.getDataQualities.data as Array<Record<string, unknown>>;
    for (const row of rows) {
      const status = row.status as string;
      if (status === "ALERT" || status === "WARNING") {
        failing.push({
          id: row.id as string,
          name: (row.name as string) ?? "",
          status,
          result: (row.result as string | null) ?? null,
          source: (row.source as string | null) ?? null,
          ownerEmail: (row.ownerEmail as string | null) ?? null,
          externalId: (row.externalId as string | null) ?? null,
          columnId: (row.columnId as string | null) ?? null,
          runAt: (row.runAt as string | null) ?? null,
          url: (row.url as string | null) ?? null,
        });
      }
    }
    if (rows.length < QUALITY_PAGE_SIZE) break;
    const total = resp.getDataQualities.totalCount;
    if (typeof total === "number" && Number.isFinite(total)) {
      const fetched = (page + 1) * QUALITY_PAGE_SIZE;
      if (fetched >= total) break;
    }
  }
  return failing;
}

async function fetchUpstreamSources(
  client: CatalogClient,
  tableId: string
): Promise<Array<{ id: string; kind: "TABLE" | "DASHBOARD" }>> {
  const upstreamIds: Array<{ id: string; kind: "TABLE" | "DASHBOARD" }> = [];
  for (let page = 0; page < LINEAGE_PAGES_MAX; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope: { childTableId: tableId },
        pagination: { nbPerPage: LINEAGE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getLineages.data as Array<Record<string, unknown>>;
    for (const edge of rows) {
      if (edge.parentTableId) {
        upstreamIds.push({
          id: edge.parentTableId as string,
          kind: "TABLE",
        });
      } else if (edge.parentDashboardId) {
        upstreamIds.push({
          id: edge.parentDashboardId as string,
          kind: "DASHBOARD",
        });
      }
    }
    if (rows.length < LINEAGE_PAGE_SIZE) break;
    const total = resp.getLineages.totalCount;
    if (typeof total === "number" && Number.isFinite(total)) {
      const fetched = (page + 1) * LINEAGE_PAGE_SIZE;
      if (fetched >= total) break;
    }
  }
  return upstreamIds;
}

function computeTriageScore(
  popularity: number | null,
  failureCount: number
): number {
  const pop = Math.max(0, popularity ?? 0);
  // Scale popularity to 0-50 range, failure count to 0-50 range
  const popScore = Math.min(50, pop * 50);
  const failScore = Math.min(50, Math.log10(failureCount + 1) * 25);
  return Math.round(popScore + failScore);
}

function groupByOwner(tables: FailingTable[]): OwnerGroup[] {
  const groups = new Map<string, OwnerGroup>();

  for (const t of tables) {
    const score = computeTriageScore(t.popularity, t.failureCount);
    const entry = {
      tableId: t.id,
      tableName: t.name,
      failureCount: t.failureCount,
      triageScore: score,
    };

    if (t.owners.userOwners.length === 0 && t.owners.teamOwners.length === 0) {
      const key = "__unowned__";
      const group = groups.get(key) ?? {
        ownerKey: key,
        ownerType: "unowned" as const,
        ownerName: null,
        tables: [],
        totalFailures: 0,
      };
      group.tables.push(entry);
      group.totalFailures += t.failureCount;
      groups.set(key, group);
    } else {
      // Assign to first user owner, or first team owner if no user owners
      const primaryOwner =
        t.owners.userOwners.length > 0
          ? {
              key: `user:${t.owners.userOwners[0].userId}`,
              type: "user" as const,
              name:
                t.owners.userOwners[0].email ??
                t.owners.userOwners[0].fullName,
            }
          : {
              key: `team:${t.owners.teamOwners[0].teamId}`,
              type: "team" as const,
              name: t.owners.teamOwners[0].name,
            };
      const group = groups.get(primaryOwner.key) ?? {
        ownerKey: primaryOwner.key,
        ownerType: primaryOwner.type,
        ownerName: primaryOwner.name,
        tables: [],
        totalFailures: 0,
      };
      group.tables.push(entry);
      group.totalFailures += t.failureCount;
      groups.set(primaryOwner.key, group);
    }
  }

  // Sort groups by totalFailures desc, within each group sort tables by triageScore desc
  const sorted = [...groups.values()].sort(
    (a, b) => b.totalFailures - a.totalFailures
  );
  for (const g of sorted) {
    g.tables.sort((a, b) => b.triageScore - a.triageScore);
  }
  return sorted;
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

export function defineTriageQualityFailures(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_triage_quality_failures",
    config: {
      title: "Triage Quality Check Failures",
      description:
        "Compose quality check failure results with ownership, popularity, and upstream lineage " +
        "context into an actionable remediation queue.\n\n" +
        "Scope is required — pass exactly one of `databaseId`, `schemaId`, or `tableIds`. " +
        "Refuses if scope exceeds 500 tables or yields more than 500 failing checks.\n\n" +
        "Process: fetches all quality checks per in-scope table, filters for ALERT/WARNING " +
        "status (no server-side status filter exists — `GetQualityChecksScope` only supports " +
        "`tableId`), enriches each failing table with ownership (from table detail), popularity, " +
        "and 1-hop upstream lineage sources (for root-cause investigation). Returns a triage " +
        "queue ranked by (popularity × failure count), grouped by primary owner.\n\n" +
        "Differs from `catalog_governance_scorecard` (counts quality check PRESENCE, not " +
        "RESULTS) and from raw `catalog_search_quality_checks` (flat check records with no " +
        "ownership/impact context). This tool answers: which quality failures matter most and " +
        "who needs to act?\n\n" +
        "Output shape: `{ scopedBy, stats, triageQueue: [{ ownerKey, ownerType, ownerName, " +
        "totalFailures, tables: [{ tableId, tableName, failureCount, triageScore, checks, " +
        "upstreamSources }] }], rankedTables: [{ id, name, popularity, failureCount, " +
        "alertCount, warningCount, owners, checks, upstreamSources }] }`.",
      inputSchema: TriageInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const scope = pickScope(args);
      if (!scope) {
        throw new Error(
          "Scope required: pass one of databaseId, schemaId, or tableIds. " +
            "Quality triage is not safe to run unscoped — it would attempt to scan " +
            "every table in the workspace."
        );
      }

      // 1. Fetch all tables in scope with detail fields (owners, tags, popularity)
      const tables = await fetchAllTablesInScope(c, scope);
      if (tables.length === 0) {
        return {
          scopedBy: scope.field,
          stats: {
            tablesInScope: 0,
            tablesWithFailures: 0,
            totalFailingChecks: 0,
            alertCount: 0,
            warningCount: 0,
          },
          triageQueue: [],
          rankedTables: [],
        };
      }

      // 2. Fetch quality checks for each table, filter for ALERT/WARNING
      const tableIds = tables.map((t) => t.id as string);
      const failingChecksByTable = new Map<string, QualityCheckRecord[]>();
      for (let i = 0; i < tableIds.length; i += QUALITY_PARALLELISM) {
        const slice = tableIds.slice(i, i + QUALITY_PARALLELISM);
        const results = await Promise.all(
          slice.map((id) => fetchQualityChecksForTable(c, id))
        );
        for (let j = 0; j < slice.length; j++) {
          if (results[j].length > 0) {
            failingChecksByTable.set(slice[j], results[j]);
          }
        }
      }

      // 3. Count total failing checks and enforce cap
      let totalFailingChecks = 0;
      for (const checks of failingChecksByTable.values()) {
        totalFailingChecks += checks.length;
      }
      if (totalFailingChecks > FAILING_CHECKS_CAP) {
        throw new Error(
          `Scope yields ${totalFailingChecks} failing quality checks ` +
            `(exceeds ${FAILING_CHECKS_CAP}-check cap). Narrow the scope with ` +
            `schemaId or a smaller tableIds list.`
        );
      }

      if (failingChecksByTable.size === 0) {
        return {
          scopedBy: scope.field,
          stats: {
            tablesInScope: tables.length,
            tablesWithFailures: 0,
            totalFailingChecks: 0,
            alertCount: 0,
            warningCount: 0,
          },
          triageQueue: [],
          rankedTables: [],
        };
      }

      // 4. Build table detail map from already-fetched data
      const tableMap = new Map<string, Record<string, unknown>>();
      for (const t of tables) {
        tableMap.set(t.id as string, t);
      }

      // 5. Fetch upstream lineage for each failing table
      const failingTableIds = [...failingChecksByTable.keys()];
      const upstreamByTable = new Map<
        string,
        Array<{ id: string; kind: "TABLE" | "DASHBOARD" }>
      >();
      for (let i = 0; i < failingTableIds.length; i += LINEAGE_PARALLELISM) {
        const slice = failingTableIds.slice(i, i + LINEAGE_PARALLELISM);
        const results = await Promise.all(
          slice.map((id) => fetchUpstreamSources(c, id))
        );
        for (let j = 0; j < slice.length; j++) {
          upstreamByTable.set(slice[j], results[j]);
        }
      }

      // 6. Hydrate upstream table sources with ownership
      const allUpstreamTableIds = new Set<string>();
      for (const sources of upstreamByTable.values()) {
        for (const s of sources) {
          if (s.kind === "TABLE" && !tableMap.has(s.id)) {
            allUpstreamTableIds.add(s.id);
          }
        }
      }
      const upstreamTableMap = new Map<string, Record<string, unknown>>();
      if (allUpstreamTableIds.size > 0) {
        const batches = chunk([...allUpstreamTableIds], ENRICHMENT_BATCH_SIZE);
        for (const batch of batches) {
          const resp = await c.execute<{ getTables: GetTablesOutput }>(
            GET_TABLES_DETAIL_BATCH,
            {
              scope: { ids: batch },
              pagination: { nbPerPage: batch.length, page: 0 },
            }
          );
          for (const row of resp.getTables.data as Array<
            Record<string, unknown>
          >) {
            upstreamTableMap.set(row.id as string, row);
          }
        }
      }

      // 7. Build failing table entries
      let globalAlertCount = 0;
      let globalWarningCount = 0;
      const failingTables: FailingTable[] = failingTableIds.map((tableId) => {
        const checks = failingChecksByTable.get(tableId)!;
        const tableRow = tableMap.get(tableId);
        const alertCount = checks.filter((c) => c.status === "ALERT").length;
        const warningCount = checks.filter(
          (c) => c.status === "WARNING"
        ).length;
        globalAlertCount += alertCount;
        globalWarningCount += warningCount;

        // Build upstream sources with ownership
        const rawUpstream = upstreamByTable.get(tableId) ?? [];
        const upstreamSources: UpstreamSource[] = rawUpstream.map((s) => {
          const row =
            s.kind === "TABLE"
              ? tableMap.get(s.id) ?? upstreamTableMap.get(s.id)
              : undefined;
          return {
            id: s.id,
            kind: s.kind,
            name: row ? ((row.name as string | null) ?? null) : null,
            owners: row ? extractOwners(row) : { userOwners: [], teamOwners: [] },
          };
        });

        return {
          id: tableId,
          name: tableRow
            ? ((tableRow.name as string | null) ?? null)
            : null,
          popularity: tableRow
            ? ((tableRow.popularity as number | null) ?? null)
            : null,
          numberOfQueries: tableRow
            ? ((tableRow.numberOfQueries as number | null) ?? null)
            : null,
          isVerified: tableRow
            ? ((tableRow.isVerified as boolean | null) ?? null)
            : null,
          isDeprecated: tableRow
            ? ((tableRow.isDeprecated as boolean | null) ?? null)
            : null,
          schemaId: tableRow
            ? ((tableRow.schemaId as string | null) ?? null)
            : null,
          owners: tableRow
            ? extractOwners(tableRow)
            : { userOwners: [], teamOwners: [] },
          failureCount: checks.length,
          alertCount,
          warningCount,
          checks,
          upstreamSources,
        };
      });

      // 8. Rank by triage score (popularity × failure count)
      failingTables.sort(
        (a, b) =>
          computeTriageScore(b.popularity, b.failureCount) -
          computeTriageScore(a.popularity, a.failureCount)
      );

      // 9. Group by owner
      const triageQueue = groupByOwner(failingTables);

      return {
        scopedBy: scope.field,
        stats: {
          tablesInScope: tables.length,
          tablesWithFailures: failingChecksByTable.size,
          totalFailingChecks,
          alertCount: globalAlertCount,
          warningCount: globalWarningCount,
        },
        triageQueue,
        rankedTables: failingTables.map((t) => ({
          id: t.id,
          name: t.name,
          popularity: t.popularity,
          numberOfQueries: t.numberOfQueries,
          isVerified: t.isVerified,
          isDeprecated: t.isDeprecated,
          schemaId: t.schemaId,
          failureCount: t.failureCount,
          alertCount: t.alertCount,
          warningCount: t.warningCount,
          triageScore: computeTriageScore(t.popularity, t.failureCount),
          owners: t.owners,
          checks: t.checks,
          upstreamSources: t.upstreamSources,
        })),
      };
    }, client),
  };
}
