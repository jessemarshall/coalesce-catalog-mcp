import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLES_DETAIL_BATCH,
} from "../catalog/operations.js";
import type {
  GetTablesOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import {
  hasOwner,
  ENRICHMENT_BATCH_SIZE,
  extractNeighborOwners,
  fetchTableQueryAuthors,
  fetchOneHopNeighborIds,
  type NeighborOwner,
  type NeighborRef,
  type QueryAuthorSignal,
} from "./shared.js";

const UNOWNED_HARD_CAP = 200;
const TABLE_PAGE_SIZE = 100;
const TABLE_SCOPE_HARD_CAP = 500;

// Query-author aggregation: fetch up to this many recent queries per table
// and group by `author` (email). Anything past this probe is unlikely to
// change the top-10 signal meaningfully — signal saturates well before
// QUERY_PROBE_SIZE for any real table.
const QUERY_PROBE_SIZE = 200;
// Each neighbor is one 1-hop edge. We only want ownership info from the
// immediate upstream/downstream, not the full graph. A single page of 100
// covers almost every real table (median table fan-out is <10).
const NEIGHBOR_PAGE_SIZE = 100;
const NEIGHBOR_PAGES_MAX = 5;
// Bounded per-table fanout so a 200-table scope doesn't open 200 × 4 =
// 800 concurrent HTTP requests. Matches assess-impact's convention.
const EVIDENCE_PARALLELISM = 10;

const ResolveOwnershipGapsInputShape = {
  databaseId: z
    .string()
    .optional()
    .describe("Scope to a single database UUID."),
  schemaId: z
    .string()
    .optional()
    .describe("Scope to a single schema UUID."),
  tableIds: z
    .array(z.string())
    .optional()
    .describe(
      `Scope to an explicit list of table UUIDs (max ${TABLE_SCOPE_HARD_CAP}). Mutually exclusive with databaseId/schemaId.`
    ),
  queryAuthorLimit: z
    .number()
    .int()
    .min(1)
    .max(50)
    .optional()
    .describe(
      "Max number of top query authors to return per unowned table (sorted by query count, descending). Default 10. Authors below the cut are dropped; the total query count per author is still computed from the full probe sample."
    ),
  includeQueryAuthors: z
    .boolean()
    .optional()
    .describe(
      "Whether to fetch query-author evidence per unowned table. Default true. Set false to skip the query-history probe when the workspace has no SQL usage data (pure Catalog-imported accounts)."
    ),
  includeLineageNeighbors: z
    .boolean()
    .optional()
    .describe(
      "Whether to fetch 1-hop upstream/downstream lineage neighbors + their owners per unowned table. Default true."
    ),
};

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
    if (ids.length > TABLE_SCOPE_HARD_CAP) {
      throw new Error(
        `tableIds (${ids.length}) exceeds the ${TABLE_SCOPE_HARD_CAP}-table ` +
          `scope cap. Split into smaller batches and merge the results client-side.`
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

async function fetchScopedTables(
  client: CatalogClient,
  scope: ScopedFilter
): Promise<Array<Record<string, unknown>>> {
  // Paginate exhaustively so a downstream filter (unowned) has the full
  // universe to work on. Completeness contract mirrors governance_scorecard:
  // if the scope resolves to >500 tables, refuse rather than silently
  // truncating the scan surface.
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
        `for scope=${scope.field}; cannot establish a complete universe of tables.`
    );
  }
  if (totalCount > TABLE_SCOPE_HARD_CAP) {
    throw new Error(
      `Scope resolves to ${totalCount} tables (scoped by ${scope.field}), ` +
        `exceeding the ${TABLE_SCOPE_HARD_CAP}-table scan cap. Narrow via schemaId ` +
        `or split into smaller tableIds batches.`
    );
  }
  const out: Array<Record<string, unknown>> = [
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
    out.push(...rows);
    if (rows.length < TABLE_PAGE_SIZE) break;
  }
  if (out.length < Math.min(totalCount, TABLE_SCOPE_HARD_CAP)) {
    throw new Error(
      `Table pagination returned ${out.length} rows for scope=${scope.field} ` +
        `but totalCount reported ${totalCount}. Refusing to emit a partial scan.`
    );
  }
  return out;
}

async function enrichTableNeighbors(
  client: CatalogClient,
  ids: string[]
): Promise<Map<string, Record<string, unknown>>> {
  // Only table neighbors are enriched for owners — dashboard ownership isn't
  // a useful signal for table-ownership attribution. Dashboards still appear
  // in the neighbor list (so the caller sees the real downstream surface),
  // but with `owners: []`.
  const map = new Map<string, Record<string, unknown>>();
  if (ids.length === 0) return map;
  for (let i = 0; i < ids.length; i += ENRICHMENT_BATCH_SIZE) {
    const slice = ids.slice(i, i + ENRICHMENT_BATCH_SIZE);
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
  }
  return map;
}

interface EvidenceBundle {
  id: string;
  name: string | null;
  popularity: number | null;
  schemaId: string | null;
  queryAuthors?: {
    totalQueriesSeen: number;
    queriesWithoutAuthor: number;
    probeCap: number;
    authors: QueryAuthorSignal[];
  };
  upstreamNeighbors?: Array<{
    id: string;
    kind: "TABLE" | "DASHBOARD";
    name: string | null;
    owners: NeighborOwner[];
  }>;
  downstreamNeighbors?: Array<{
    id: string;
    kind: "TABLE" | "DASHBOARD";
    name: string | null;
    owners: NeighborOwner[];
  }>;
}

async function gatherEvidenceForTable(
  client: CatalogClient,
  tableRow: Record<string, unknown>,
  opts: {
    includeQueryAuthors: boolean;
    includeLineageNeighbors: boolean;
    queryAuthorLimit: number;
  }
): Promise<EvidenceBundle> {
  const id = tableRow.id as string;
  const bundle: EvidenceBundle = {
    id,
    name: (tableRow.name as string | null) ?? null,
    popularity: (tableRow.popularity as number | null) ?? null,
    schemaId: (tableRow.schemaId as string | null) ?? null,
  };

  const tasks: Array<Promise<void>> = [];

  if (opts.includeQueryAuthors) {
    tasks.push(
      fetchTableQueryAuthors(client, id, {
        topN: opts.queryAuthorLimit,
        probeSize: QUERY_PROBE_SIZE,
      }).then((qa) => {
        bundle.queryAuthors = {
          totalQueriesSeen: qa.totalQueriesSeen,
          queriesWithoutAuthor: qa.queriesWithoutAuthor,
          probeCap: qa.probeCap,
          authors: qa.authors,
        };
      })
    );
  }

  if (opts.includeLineageNeighbors) {
    tasks.push(
      (async () => {
        const [upstream, downstream] = await Promise.all([
          fetchOneHopNeighborIds(client, id, "upstream", {
            pageSize: NEIGHBOR_PAGE_SIZE,
            maxPages: NEIGHBOR_PAGES_MAX,
          }),
          fetchOneHopNeighborIds(client, id, "downstream", {
            pageSize: NEIGHBOR_PAGE_SIZE,
            maxPages: NEIGHBOR_PAGES_MAX,
          }),
        ]);
        const tableNeighborIds = [
          ...upstream.filter((n) => n.kind === "TABLE").map((n) => n.id),
          ...downstream.filter((n) => n.kind === "TABLE").map((n) => n.id),
        ];
        const enrichment = await enrichTableNeighbors(
          client,
          [...new Set(tableNeighborIds)]
        );
        // Validate enrichment: every table neighbor we reached via lineage
        // must come back with a detail row. Conflating "no row returned"
        // with "no owners" would silently inflate the "neighbor is unowned"
        // signal and lead a caller to misattribute ownership. Throw loudly
        // (same contract as assess-impact) rather than emitting a partial
        // evidence bundle.
        const missing = [...new Set(tableNeighborIds)].filter(
          (nid) => !enrichment.has(nid)
        );
        if (missing.length > 0) {
          throw new Error(
            `Neighbor enrichment returned no row for ${missing.length} table(s) ` +
              `reached via lineage from ${id} (sample: ${missing.slice(0, 5).join(", ")}). ` +
              `Cannot produce complete ownership evidence.`
          );
        }
        bundle.upstreamNeighbors = upstream.map((n) => ({
          id: n.id,
          kind: n.kind,
          name:
            n.kind === "TABLE"
              ? ((enrichment.get(n.id)?.name as string | null) ?? null)
              : null,
          owners:
            n.kind === "TABLE"
              ? extractNeighborOwners(enrichment.get(n.id))
              : [],
        }));
        bundle.downstreamNeighbors = downstream.map((n) => ({
          id: n.id,
          kind: n.kind,
          name:
            n.kind === "TABLE"
              ? ((enrichment.get(n.id)?.name as string | null) ?? null)
              : null,
          owners:
            n.kind === "TABLE"
              ? extractNeighborOwners(enrichment.get(n.id))
              : [],
        }));
      })()
    );
  }

  await Promise.all(tasks);
  return bundle;
}

export function defineResolveOwnershipGaps(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_resolve_ownership_gaps",
    config: {
      title: "Resolve Ownership Gaps (Evidence Bundles)",
      description:
        "Scope an ownership audit: find every unowned table in a database / schema / table list, then gather per-table *evidence* that a human can review to pick an owner. No confidence scores, no heuristic ranking — just raw signals:\n" +
        "  - top N query authors (sorted by recent query count, grouped by email) from the last ~200 queries\n" +
        "  - 1-hop upstream + downstream lineage neighbors, each with their attached user/team owners\n\n" +
        `**Completeness contract:** the tool refuses if the scope resolves to >${TABLE_SCOPE_HARD_CAP} tables, or if more than ${UNOWNED_HARD_CAP} of them are unowned — those scans need to be split, not silently truncated. Scope via schemaId or a narrower tableIds batch. Every pagination loop has a hard ceiling with a loud error if exceeded.\n\n` +
        "**Scope routing:**\n" +
        "  - databaseId — audit an entire database (refuses if >500 tables)\n" +
        "  - schemaId — audit a single schema (usually the right grain for a data-steward review)\n" +
        "  - tableIds — audit an explicit list (max 500)\n\n" +
        "**Output shape:** `scanned.unownedCount`, plus per-unowned-table `{ id, name, popularity, queryAuthors, upstreamNeighbors, downstreamNeighbors }`. Use as input for a human/agent to decide ownership via catalog_upsert_user_owners or catalog_upsert_team_owners.\n\n" +
        "**When to reach for this instead of governance_scorecard:** the scorecard tells you *who's covered* (aggregate coverage % + per-table hasOwner flag); this tool tells you *who should own what's not covered* (evidence-per-gap). Pair them: scorecard first to size the gap, then this tool to close it.",
      inputSchema: ResolveOwnershipGapsInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const scope = pickScope(args);
      if (!scope) {
        throw new Error(
          "Scope required: pass one of databaseId, schemaId, or tableIds. " +
            "The ownership-gap scan is not safe to run unscoped — it would attempt " +
            "to load every table in the workspace."
        );
      }

      const queryAuthorLimit = (args.queryAuthorLimit as number | undefined) ?? 10;
      const includeQueryAuthors =
        (args.includeQueryAuthors as boolean | undefined) ?? true;
      const includeLineageNeighbors =
        (args.includeLineageNeighbors as boolean | undefined) ?? true;

      const tables = await fetchScopedTables(c, scope);
      const unowned = tables.filter((t) => !hasOwner(t));

      if (unowned.length > UNOWNED_HARD_CAP) {
        throw new Error(
          `Found ${unowned.length} unowned tables in scope (scoped by ${scope.field}), ` +
            `exceeding the ${UNOWNED_HARD_CAP}-table evidence-gathering cap. Narrow the ` +
            `scope (e.g. via schemaId) — the agent shouldn't be processing that many at once, ` +
            `and running uncapped would fan out to ~${unowned.length * 4} API calls.`
        );
      }

      // Bounded parallel fanout: slice the unowned list into groups of
      // EVIDENCE_PARALLELISM so we don't open 200 × 4 = 800 concurrent
      // requests at the top-end of the cap.
      const evidence: EvidenceBundle[] = [];
      for (let i = 0; i < unowned.length; i += EVIDENCE_PARALLELISM) {
        const slice = unowned.slice(i, i + EVIDENCE_PARALLELISM);
        const sliceResults = await Promise.all(
          slice.map((row) =>
            gatherEvidenceForTable(c, row, {
              includeQueryAuthors,
              includeLineageNeighbors,
              queryAuthorLimit,
            })
          )
        );
        evidence.push(...sliceResults);
      }
      // Preserve the popularity-DESC order from the fetch stage — the caller
      // sees the most-important gaps first.
      evidence.sort((a, b) => (b.popularity ?? -1) - (a.popularity ?? -1));

      return {
        scopedBy: scope.field,
        scanned: {
          tablesInScope: tables.length,
          unownedCount: unowned.length,
        },
        config: {
          queryAuthorLimit,
          includeQueryAuthors,
          includeLineageNeighbors,
          queryProbeCap: QUERY_PROBE_SIZE,
          neighborPageSize: NEIGHBOR_PAGE_SIZE,
        },
        tables: evidence,
      };
    }, client),
  };
}
