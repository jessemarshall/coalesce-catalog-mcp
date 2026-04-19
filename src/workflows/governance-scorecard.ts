import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLES_DETAIL_BATCH,
  GET_COLUMNS_SUMMARY,
  GET_DATA_QUALITIES,
} from "../catalog/operations.js";
import type {
  GetColumnsOutput,
  GetQualityChecksOutput,
  GetTablesOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

const ScorecardInputShape = {
  databaseId: z
    .string()
    .optional()
    .describe("Scope the scorecard to a single database UUID."),
  schemaId: z
    .string()
    .optional()
    .describe("Scope the scorecard to a single schema UUID."),
  tableIds: z
    .array(z.string())
    .optional()
    .describe(
      "Scope the scorecard to an explicit list of table UUIDs (max 500). Mutually exclusive with databaseId/schemaId — passing more than one scope field is refused."
    ),
  weighting: z
    .enum(["popularity", "equal"])
    .optional()
    .describe(
      "How to roll up per-table scores into the aggregate. 'popularity' (default) treats hot tables as more important — un-owned popular table moves the needle more than un-owned obscure one. 'equal' is one-table-one-vote, useful for compliance/audit reviews where the long tail matters."
    ),
  perTableColumnCap: z
    .number()
    .int()
    .min(10)
    .max(500)
    .optional()
    .describe(
      "Max columns to inspect per table when computing column-doc coverage. Default 200. Wide-table outliers (1000+ col staging tables) are sampled to this cap; their `columnDocCoverage` row reports `sampled: true`."
    ),
  includeQualityCoverage: z
    .boolean()
    .optional()
    .describe(
      "Add the 'checked' axis: per-table qualityCheckCount + hasQualityCheck flag, plus aggregate checkedPct. Costs one extra `getDataQualities` call per table (the API doesn't batch by tableIds), parallelised in groups of 20. Default false to keep the default scorecard fast; set true for a complete 5-axis coverage matrix."
    ),
};

const TABLE_HARD_CAP = 500;
const TABLE_PAGE_SIZE = 100;
const TABLE_BATCH_SIZE = 50;
const COLUMN_PAGE_SIZE = 500;
const QUALITY_PARALLELISM = 20;

interface TableScoreRow {
  id: string;
  name: string | null;
  popularity: number | null;
  isVerified: boolean | null;
  isDeprecated: boolean | null;
  hasOwner: boolean;
  hasDescription: boolean;
  tagCount: number;
  columnDocCoverage:
    | {
        described: number;
        total: number;
        pct: number;
        sampled: boolean;
      }
    | { error: string };
  // Only populated when includeQualityCoverage is true.
  qualityCheckCount?: number;
  hasQualityCheck?: boolean;
}

interface ScorecardAggregate {
  weighting: "popularity" | "equal";
  axes: string[];
  tableCount: number;
  ownedPct: number;
  describedPct: number;
  taggedPct: number;
  columnDocPct: number;
  // Present only when includeQualityCoverage is true.
  checkedPct?: number;
  governanceScore: number;
}

interface ScopedFilter {
  field: "tableIds" | "schemaId" | "databaseId";
  filter: { ids?: string[]; schemaId?: string; databaseId?: string };
}

function pickScope(args: {
  databaseId?: unknown;
  schemaId?: unknown;
  tableIds?: unknown;
}): ScopedFilter | null {
  // The docstring says these are mutually exclusive. Reject combinations
  // loudly rather than silently applying most-specific-wins — a caller who
  // passes both tableIds and databaseId almost certainly made a mistake, and
  // silently ignoring the databaseId would produce a scorecard scoped much
  // more narrowly than the caller intended.
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
      // Enforce the documented max here rather than relying on the post-fetch
      // totalCount check — a server-side ids cap could silently drop the tail.
      throw new Error(
        `tableIds (${ids.length}) exceeds the ${TABLE_HARD_CAP}-table ` +
          `scorecard cap. Split into smaller batches and merge the results client-side.`
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

function hasOwner(row: Record<string, unknown>): boolean {
  // Filter out owner entities whose userId/teamId is null — the binding row
  // exists but doesn't actually point at a user/team. For governance audit
  // purposes that's functionally unowned, not silently owned.
  const userOwners = Array.isArray(row.ownerEntities)
    ? (row.ownerEntities as Array<Record<string, unknown>>).filter(
        (o) => o.userId != null
      ).length
    : 0;
  const teamOwners = Array.isArray(row.teamOwnerEntities)
    ? (row.teamOwnerEntities as Array<Record<string, unknown>>).filter(
        (t) => t.teamId != null
      ).length
    : 0;
  return userOwners + teamOwners > 0;
}

function isNonEmptyString(v: unknown): boolean {
  return typeof v === "string" && v.trim().length > 0;
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function fetchColumnsForTableBatch(
  client: CatalogClient,
  tableIds: string[],
  perTableCap: number
): Promise<Map<string, { described: number; total: number; sampled: boolean }>> {
  // Returns per-table coverage. Paginate the columns endpoint scoped to the
  // tableIds batch until we've either seen everything or hit the per-table cap.
  // Per-table cap is enforced client-side as we process pages.
  const counts = new Map<
    string,
    { described: number; total: number; capped: boolean }
  >();
  for (const id of tableIds) {
    counts.set(id, { described: 0, total: 0, capped: false });
  }

  // Hard ceiling so a misbehaving server (e.g. nbPerPage not honoured, or
  // totalCount stuck at NaN/0 while data keeps flowing) can't wedge the call.
  // Theoretical max is tableIds.length * perTableCap columns; convert to
  // pages and add slack for response-size variance.
  const maxPages =
    Math.ceil((tableIds.length * perTableCap) / COLUMN_PAGE_SIZE) + 5;

  for (let page = 0; page < maxPages; page++) {
    const resp = await client.execute<{ getColumns: GetColumnsOutput }>(
      GET_COLUMNS_SUMMARY,
      {
        scope: { tableIds },
        pagination: { nbPerPage: COLUMN_PAGE_SIZE, page },
      }
    );
    const rows = resp.getColumns.data as Array<{
      tableId: string;
      description?: string | null;
    }>;
    for (const col of rows) {
      const entry = counts.get(col.tableId);
      if (!entry) continue;
      if (entry.total >= perTableCap) {
        entry.capped = true;
        continue;
      }
      entry.total += 1;
      if (isNonEmptyString(col.description)) entry.described += 1;
    }
    const fetchedSoFar = (page + 1) * COLUMN_PAGE_SIZE;
    if (rows.length < COLUMN_PAGE_SIZE) break;
    const columnsTotal = resp.getColumns.totalCount;
    if (typeof columnsTotal !== "number" || !Number.isFinite(columnsTotal)) {
      throw new Error(
        `getColumns returned non-numeric totalCount (${String(columnsTotal)}) ` +
          `for tableIds batch of ${tableIds.length}; cannot verify pagination completeness.`
      );
    }
    if (fetchedSoFar >= columnsTotal) break;
    // If every table in the batch has hit its cap, additional rows would all
    // be skipped — stop paginating.
    if ([...counts.values()].every((c) => c.capped)) break;
    if (page === maxPages - 1) {
      throw new Error(
        `Column pagination exceeded ${maxPages} pages for ${tableIds.length} ` +
          `tables (perTableCap=${perTableCap}). Likely server returning ` +
          `duplicate pages or non-numeric totalCount.`
      );
    }
  }

  const out = new Map<
    string,
    { described: number; total: number; sampled: boolean }
  >();
  for (const [id, c] of counts) {
    out.set(id, { described: c.described, total: c.total, sampled: c.capped });
  }
  return out;
}

function computeAggregate(
  rows: TableScoreRow[],
  weighting: "popularity" | "equal",
  qualityIncluded: boolean
): ScorecardAggregate {
  const baseAxes = ["owned", "described", "tagged", "columnDoc"];
  const axes = qualityIncluded ? [...baseAxes, "checked"] : baseAxes;

  if (rows.length === 0) {
    return {
      weighting,
      axes,
      tableCount: 0,
      ownedPct: 0,
      describedPct: 0,
      taggedPct: 0,
      columnDocPct: 0,
      ...(qualityIncluded ? { checkedPct: 0 } : {}),
      governanceScore: 0,
    };
  }

  const weights = rows.map((r) =>
    weighting === "equal" ? 1 : Math.max(0, r.popularity ?? 0)
  );
  const sumWeights = weights.reduce((a, b) => a + b, 0);
  // If popularity is 0 across the board (or unset), fall back to equal so the
  // aggregate still reflects something meaningful instead of NaN.
  const effectiveWeights = sumWeights > 0 ? weights : rows.map(() => 1);
  const effectiveSum = effectiveWeights.reduce((a, b) => a + b, 0);

  function weightedPct(predicate: (r: TableScoreRow) => boolean): number {
    let num = 0;
    for (let i = 0; i < rows.length; i++) {
      if (predicate(rows[i])) num += effectiveWeights[i];
    }
    return Math.round((num / effectiveSum) * 100);
  }

  function weightedColumnDocPct(): number {
    let weightedSum = 0;
    let weightSumForCols = 0;
    for (let i = 0; i < rows.length; i++) {
      const cov = rows[i].columnDocCoverage;
      if ("error" in cov) continue;
      if (cov.total === 0) continue;
      weightedSum += cov.pct * effectiveWeights[i];
      weightSumForCols += effectiveWeights[i];
    }
    if (weightSumForCols === 0) return 0;
    return Math.round(weightedSum / weightSumForCols);
  }

  const ownedPct = weightedPct((r) => r.hasOwner);
  const describedPct = weightedPct((r) => r.hasDescription);
  const taggedPct = weightedPct((r) => r.tagCount > 0);
  const columnDocPct = weightedColumnDocPct();
  const checkedPct = qualityIncluded
    ? weightedPct((r) => r.hasQualityCheck === true)
    : undefined;

  // Score adapts to the measured axes — a 4-axis report and a 5-axis report
  // are not directly comparable, but each is internally consistent.
  const partsForScore = qualityIncluded
    ? [ownedPct, describedPct, taggedPct, columnDocPct, checkedPct as number]
    : [ownedPct, describedPct, taggedPct, columnDocPct];
  const governanceScore = Math.round(
    partsForScore.reduce((a, b) => a + b, 0) / partsForScore.length
  );

  return {
    weighting,
    axes,
    tableCount: rows.length,
    ownedPct,
    describedPct,
    taggedPct,
    columnDocPct,
    ...(qualityIncluded ? { checkedPct } : {}),
    governanceScore,
  };
}

async function fetchQualityCountsForTables(
  client: CatalogClient,
  tableIds: string[]
): Promise<Map<string, number>> {
  // The quality endpoint only filters by single tableId — no tableIds batch
  // scope. Run N calls bounded by QUALITY_PARALLELISM so we don't open 500
  // sockets at once. Each call asks for one row to get the totalCount cheaply.
  const counts = new Map<string, number>();
  for (let i = 0; i < tableIds.length; i += QUALITY_PARALLELISM) {
    const slice = tableIds.slice(i, i + QUALITY_PARALLELISM);
    const results = await Promise.all(
      slice.map((id) =>
        client
          .execute<{ getDataQualities: GetQualityChecksOutput }>(
            GET_DATA_QUALITIES,
            {
              scope: { tableId: id },
              pagination: { nbPerPage: 1, page: 0 },
            }
          )
          .then((r) => {
            const total = r.getDataQualities.totalCount;
            if (typeof total !== "number" || !Number.isFinite(total)) {
              // Defending against schema drift: a non-numeric totalCount would
              // silently bucket every table as "no quality checks" and drop
              // checkedPct to 0 — surfacing an emergency that doesn't exist.
              throw new Error(
                `getDataQualities returned non-numeric totalCount (${String(total)}) ` +
                  `for tableId=${id}; cannot compute quality coverage.`
              );
            }
            return { id, total };
          })
      )
    );
    for (const r of results) counts.set(r.id, r.total);
  }
  return counts;
}

export function defineGovernanceScorecard(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_governance_scorecard",
    config: {
      title: "Governance Coverage Scorecard",
      description:
        "Compute a governance coverage matrix across a scoped set of tables: per-table flags for ownership, description, column-doc coverage %, and tag count, plus a popularity-weighted aggregate roll-up.\n\n" +
        "Scope is required — pass exactly one of `databaseId`, `schemaId`, or `tableIds`. Passing more than one is a refusal (explicit, not silent). The tool refuses if the scope resolves to >500 tables; narrow with `schemaId` or `tableIds` when scoping a large database.\n\n" +
        "Coverage methodology:\n" +
        "  - hasOwner: any user OR team owner attached.\n" +
        "  - hasDescription: non-empty `description` field (Catalog or external source).\n" +
        "  - columnDocCoverage: described columns / total columns. Inspects up to `perTableColumnCap` columns per table (default 200); wide outliers report `sampled: true`.\n" +
        "  - tagCount: count of attached tags.\n" +
        "  - hasQualityCheck (opt-in via `includeQualityCoverage: true`): any data-quality check attached. Adds N parallel calls (one per table); excluded by default to keep the scorecard fast.\n" +
        "Aggregate `governanceScore` = mean of the measured percentages under the chosen weighting (popularity-weighted by default; pass `weighting: 'equal'` for one-table-one-vote audits). The `axes` field on the aggregate lists exactly which axes contributed — a 4-axis report and a 5-axis report are not directly comparable, but each is internally consistent.\n\n" +
        "Per-table rows are returned ranked by popularity DESC. Use to drive Catalog 'Health' dashboards or governance-rollout playbooks.",
      inputSchema: ScorecardInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const scope = pickScope(args);
      if (!scope) {
        // Throw so withErrorHandling sets isError: true; clients filtering on
        // isError otherwise treat scope-required as a successful response with
        // an error key, which is the wrong shape for retries / pipelines.
        throw new Error(
          "Scope required: pass one of databaseId, schemaId, or tableIds. " +
            "The scorecard is not safe to run unscoped — it would attempt to load every table in the workspace."
        );
      }
      const weighting =
        (args.weighting as "popularity" | "equal" | undefined) ?? "popularity";
      const perTableColumnCap =
        (args.perTableColumnCap as number | undefined) ?? 200;
      const includeQualityCoverage =
        (args.includeQualityCoverage as boolean | undefined) ?? false;

      // Fetch the in-scope tables with detail fields (owners + tags + description).
      // Paginate because the server may clamp nbPerPage below TABLE_HARD_CAP;
      // a single-page fetch would silently truncate the scorecard universe and
      // break the "complete or refuse" contract.
      const firstPage = await c.execute<{ getTables: GetTablesOutput }>(
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
            `for scorecard scope=${scope.field}; cannot establish a complete universe.`
        );
      }
      if (totalCount > TABLE_HARD_CAP) {
        throw new Error(
          `Scope resolves to ${totalCount} tables (scoped by ${scope.field}), ` +
            `exceeding the ${TABLE_HARD_CAP}-table cap for one scorecard call. ` +
            `Narrow via schemaId or split into smaller tableIds batches.`
        );
      }

      // Copy rather than alias the response array — we mutate `tables` below
      // and aliasing would let a subsequent page's response double-count
      // anything that happens to share backing storage.
      const tables: Array<Record<string, unknown>> = [
        ...(firstPage.getTables.data as Array<Record<string, unknown>>),
      ];
      const expectedPages = Math.ceil(totalCount / TABLE_PAGE_SIZE);
      for (let page = 1; page < expectedPages; page++) {
        const resp = await c.execute<{ getTables: GetTablesOutput }>(
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
            `but totalCount reported ${totalCount}. Refusing to emit a partial scorecard.`
        );
      }
      if (tables.length === 0) {
        return {
          scopedBy: scope.field,
          tableCount: 0,
          aggregate: computeAggregate([], weighting, includeQualityCoverage),
          tables: [],
        };
      }

      // Fetch columns in table-batches and (optional) quality counts in
      // parallel — they target different endpoints so they don't compete.
      const tableIds = tables.map((t) => t.id as string);
      const batches = chunk(tableIds, TABLE_BATCH_SIZE);
      const [coverageMaps, qualityCounts] = await Promise.all([
        Promise.all(
          batches.map((batch) =>
            fetchColumnsForTableBatch(c, batch, perTableColumnCap)
          )
        ),
        includeQualityCoverage
          ? fetchQualityCountsForTables(c, tableIds)
          : Promise.resolve(null),
      ]);
      const coverage = new Map<
        string,
        { described: number; total: number; sampled: boolean }
      >();
      for (const m of coverageMaps) {
        for (const [id, v] of m) coverage.set(id, v);
      }

      const rows: TableScoreRow[] = tables.map((t) => {
        const id = t.id as string;
        const cov = coverage.get(id);
        const tagCount = Array.isArray(t.tagEntities)
          ? (t.tagEntities as unknown[]).length
          : 0;
        const qualityCount = qualityCounts?.get(id);
        return {
          id,
          name: (t.name as string | null) ?? null,
          popularity: (t.popularity as number | null) ?? null,
          isVerified: (t.isVerified as boolean | null) ?? null,
          isDeprecated: (t.isDeprecated as boolean | null) ?? null,
          hasOwner: hasOwner(t),
          hasDescription: isNonEmptyString(t.description),
          tagCount,
          columnDocCoverage: cov
            ? {
                described: cov.described,
                total: cov.total,
                pct:
                  cov.total === 0
                    ? 0
                    : Math.round((cov.described / cov.total) * 100),
                sampled: cov.sampled,
              }
            : { error: "column fetch returned no data for this table" },
          ...(qualityCounts
            ? {
                qualityCheckCount: qualityCount ?? 0,
                hasQualityCheck: (qualityCount ?? 0) > 0,
              }
            : {}),
        };
      });

      return {
        scopedBy: scope.field,
        tableCount: rows.length,
        aggregate: computeAggregate(rows, weighting, includeQualityCoverage),
        tables: rows,
      };
    }, client),
  };
}
