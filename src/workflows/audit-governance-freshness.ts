import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import { GET_TABLES_DETAIL_BATCH } from "../catalog/operations.js";
import type { GetTablesOutput } from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import { extractOwners, type Owners } from "./shared.js";

// ── Input schema ────────────────────────────────────────────────────────────

const FreshnessInputShape = {
  databaseId: z
    .string()
    .optional()
    .describe("Scope the audit to a single database UUID."),
  schemaId: z
    .string()
    .optional()
    .describe("Scope the audit to a single schema UUID."),
  tableIds: z
    .array(z.string())
    .optional()
    .describe(
      "Scope the audit to an explicit list of table UUIDs (max 500). Mutually exclusive with databaseId/schemaId."
    ),
  cadencePolicy: z
    .object({
      defaultDays: z
        .number()
        .int()
        .min(1)
        .max(36500)
        .describe(
          "Default required review cadence in days for tables with no matching sensitivity tag."
        ),
      byTag: z
        .record(
          z.string(),
          z
            .number()
            .int()
            .min(1)
            .max(36500)
        )
        .optional()
        .describe(
          "Per-tag-label cadence overrides (case-insensitive substring match against each tag's label). Tighter cadence wins when a table matches multiple labels — use for sensitivity tiers like 'pii: 90' or 'critical: 180'."
        ),
    })
    .optional()
    .describe(
      "Sensitivity-driven review cadence policy. Default: { defaultDays: 365 }. Customer ask (Telstra Health 2026-03-27): 'governance in Catalog could look complete at launch, then quietly become stale' — tag-driven cadence lets PII tables have a tighter review window than long-tail reference data."
    ),
  asOf: z
    .string()
    .datetime({ offset: true })
    .optional()
    .describe(
      "Compute staleness relative to this ISO timestamp instead of 'now'. Useful for backdating reports or deterministic test runs. Defaults to the server's current time."
    ),
  overdueOnly: z
    .boolean()
    .optional()
    .describe(
      "When true, the per-table list only contains tables with stalenessDays > 0 (overdue for review). Aggregate counts always reflect the full scope. Default false."
    ),
};

// ── Constants ───────────────────────────────────────────────────────────────

const TABLE_HARD_CAP = 500;
const TABLE_PAGE_SIZE = 100;
const DAY_MS = 24 * 60 * 60 * 1000;
const DEFAULT_CADENCE_DAYS = 365;

// ── Internal types ──────────────────────────────────────────────────────────

interface CadencePolicy {
  defaultDays: number;
  byTag: Array<{ labelLower: string; days: number }>;
}

interface FreshnessRow {
  tableId: string;
  tableName: string | null;
  tablePath: string | null;
  popularity: number;
  isVerified: boolean;
  isDeprecated: boolean;
  lastReviewedAt: string | null;
  lastReviewedSource: "verifiedAt" | "updatedAt" | null;
  daysSinceReview: number | null;
  matchedSensitivityTags: string[];
  requiredCadenceDays: number;
  stalenessDays: number;
  isOverdue: boolean;
  bucket: "neverReviewed" | "overdue" | "dueSoon" | "ok";
  priorityScore: number;
  owners: Owners;
}

interface ScopedFilter {
  field: "tableIds" | "schemaId" | "databaseId";
  filter: { ids?: string[]; schemaId?: string; databaseId?: string };
}

// ── Helpers ─────────────────────────────────────────────────────────────────

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
        `tableIds (${ids.length}) exceeds the ${TABLE_HARD_CAP}-table audit cap. ` +
          `Split into smaller batches and merge the results client-side.`
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

function normalisePolicy(raw: unknown): CadencePolicy {
  if (raw && typeof raw === "object") {
    const obj = raw as Record<string, unknown>;
    const defaultDays =
      typeof obj.defaultDays === "number" && Number.isFinite(obj.defaultDays)
        ? obj.defaultDays
        : DEFAULT_CADENCE_DAYS;
    const byTagRaw = obj.byTag as Record<string, unknown> | undefined;
    const byTag: Array<{ labelLower: string; days: number }> = [];
    if (byTagRaw && typeof byTagRaw === "object") {
      for (const [label, days] of Object.entries(byTagRaw)) {
        if (typeof days !== "number" || !Number.isFinite(days)) continue;
        byTag.push({ labelLower: label.toLowerCase(), days });
      }
    }
    return { defaultDays, byTag };
  }
  return { defaultDays: DEFAULT_CADENCE_DAYS, byTag: [] };
}

function tableTagLabels(row: Record<string, unknown>): string[] {
  if (!Array.isArray(row.tagEntities)) return [];
  const out: string[] = [];
  for (const t of row.tagEntities as Array<Record<string, unknown>>) {
    const tag = t.tag as Record<string, unknown> | undefined;
    const label = tag?.label;
    if (typeof label === "string" && label.length > 0) out.push(label);
  }
  return out;
}

function pickRequiredCadence(
  tagLabels: string[],
  policy: CadencePolicy
): { requiredDays: number; matched: string[] } {
  const matched: string[] = [];
  let tightest: number | null = null;
  for (const label of tagLabels) {
    const lower = label.toLowerCase();
    for (const entry of policy.byTag) {
      if (lower.includes(entry.labelLower)) {
        matched.push(label);
        // Tighter cadence wins on conflict — most-protective policy applies
        // when a table is tagged both 'pii' (90d) and 'reference' (730d).
        if (tightest === null || entry.days < tightest) tightest = entry.days;
        break;
      }
    }
  }
  if (tightest === null)
    return { requiredDays: policy.defaultDays, matched: [] };
  return { requiredDays: tightest, matched };
}

function parseTimestamp(v: unknown): number | null {
  if (typeof v === "number" && Number.isFinite(v)) return v;
  if (typeof v === "string") {
    const parsed = Date.parse(v);
    if (!Number.isNaN(parsed)) return parsed;
  }
  return null;
}

function tablePathFromRow(row: Record<string, unknown>): string | null {
  const name = row.name;
  const schema = row.schema as Record<string, unknown> | undefined;
  const schemaName = schema?.name;
  if (typeof name !== "string" || name.length === 0) return null;
  if (typeof schemaName === "string" && schemaName.length > 0)
    return `${schemaName}.${name}`;
  return name;
}

interface AggregateBuckets {
  neverReviewed: number;
  overdue: number;
  dueSoon: number;
  ok: number;
}

interface FreshnessAggregate {
  scopedBy: "tableIds" | "schemaId" | "databaseId";
  asOf: string;
  policy: { defaultDays: number; byTag: Record<string, number> };
  tableCount: number;
  buckets: AggregateBuckets;
  overduePct: number;
  worstStalenessDays: number;
  popularityWeightedOverduePct: number;
}

function computeAggregate(
  rows: FreshnessRow[],
  scopedBy: ScopedFilter["field"],
  asOfMs: number,
  policy: CadencePolicy
): FreshnessAggregate {
  const buckets: AggregateBuckets = {
    neverReviewed: 0,
    overdue: 0,
    dueSoon: 0,
    ok: 0,
  };
  let worstStalenessDays = 0;
  let popularitySum = 0;
  let popularityOverdueSum = 0;
  for (const r of rows) {
    buckets[r.bucket] += 1;
    if (r.stalenessDays > worstStalenessDays)
      worstStalenessDays = r.stalenessDays;
    const w = Math.max(0, r.popularity);
    popularitySum += w;
    if (r.isOverdue || r.bucket === "neverReviewed") popularityOverdueSum += w;
  }
  const overdueCount = buckets.overdue + buckets.neverReviewed;
  const overduePct =
    rows.length === 0 ? 0 : Math.round((overdueCount / rows.length) * 100);
  const popularityWeightedOverduePct =
    popularitySum === 0
      ? overduePct
      : Math.round((popularityOverdueSum / popularitySum) * 100);
  const byTagOut: Record<string, number> = {};
  for (const entry of policy.byTag) byTagOut[entry.labelLower] = entry.days;
  return {
    scopedBy,
    asOf: new Date(asOfMs).toISOString(),
    policy: { defaultDays: policy.defaultDays, byTag: byTagOut },
    tableCount: rows.length,
    buckets,
    overduePct,
    worstStalenessDays,
    popularityWeightedOverduePct,
  };
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineAuditGovernanceFreshness(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_audit_governance_freshness",
    config: {
      title: "Audit Governance Freshness",
      description:
        "Audit governance review-recency across a scoped set of tables. Extends the `catalog_governance_scorecard` model with `verifiedAt` + a sensitivity-driven cadence policy, computing per-table staleness (days-since-last-review minus required-cadence-days) and emitting an overdue report sorted by stalenessDays * popularity.\n\n" +
        "Composes: scoped `getTables` detail batch (verifiedAt, updatedAt, tagEntities, popularity, ownership) → per-table cadence resolution against the policy (sensitivity-tag substring match, tightest-wins) → staleness computation → bucketed aggregate roll-up.\n\n" +
        "Closes the seam Telstra Health raised 2026-03-27: 'current completeness checks only answer \"does documentation exist?\", not \"is it still current?\".' The scorecard tells you coverage; this tool tells you which covered metadata has gone stale and which sensitive tables are overdue for re-verification.\n\n" +
        "Methodology:\n" +
        "  - lastReviewedAt = `verifiedAt` when isVerified=true, else `updatedAt` (with `lastReviewedSource` recorded on every row).\n" +
        "  - requiredCadenceDays = policy.byTag tightest match (case-insensitive substring), else policy.defaultDays (365 by default).\n" +
        "  - bucket: neverReviewed (no timestamp), overdue (stalenessDays > 0), dueSoon (within 30 days of cadence), ok (otherwise).\n" +
        "  - priorityScore = stalenessDays * max(0, popularity).\n" +
        "  - aggregates: bucket counts, overduePct, popularity-weighted overdue %, worstStalenessDays.\n\n" +
        "Scope is required — pass exactly one of `databaseId`, `schemaId`, or `tableIds` (max 500). The tool refuses if the scope resolves to >500 tables.\n\n" +
        "Pair with `catalog_governance_scorecard` (coverage matrix) for a complete governance health view: scorecard answers \"is metadata present?\", this audit answers \"is metadata fresh?\".",
      inputSchema: FreshnessInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const scope = pickScope(args);
      if (!scope) {
        throw new Error(
          "Scope required: pass one of databaseId, schemaId, or tableIds. " +
            "The freshness audit is not safe to run unscoped — it would attempt to load every table in the workspace."
        );
      }
      const policy = normalisePolicy(args.cadencePolicy);
      const overdueOnly = (args.overdueOnly as boolean | undefined) ?? false;
      const asOfMs =
        typeof args.asOf === "string"
          ? Date.parse(args.asOf as string)
          : Date.now();
      if (!Number.isFinite(asOfMs)) {
        throw new Error(
          `Could not parse asOf timestamp '${String(args.asOf)}'; expected an ISO 8601 string.`
        );
      }

      // Fetch tables in scope with detail fields. Mirrors the scorecard's
      // pagination + capacity-gate contract — refuse rather than silently
      // truncating when the scope is too wide.
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
            `for freshness scope=${scope.field}; cannot establish a complete universe.`
        );
      }
      if (totalCount > TABLE_HARD_CAP) {
        throw new Error(
          `Scope resolves to ${totalCount} tables (scoped by ${scope.field}), ` +
            `exceeding the ${TABLE_HARD_CAP}-table cap for one freshness audit. ` +
            `Narrow via schemaId or split into smaller tableIds batches.`
        );
      }

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
            `but totalCount reported ${totalCount}. Refusing to emit a partial freshness audit.`
        );
      }

      const rows: FreshnessRow[] = tables.map((row) => {
        const tagLabels = tableTagLabels(row);
        const { requiredDays, matched } = pickRequiredCadence(tagLabels, policy);
        const isVerified = row.isVerified === true;
        const verifiedAt = parseTimestamp(row.verifiedAt);
        const updatedAt = parseTimestamp(row.updatedAt);
        let lastReviewedMs: number | null = null;
        let lastReviewedSource: FreshnessRow["lastReviewedSource"] = null;
        if (isVerified && verifiedAt !== null) {
          lastReviewedMs = verifiedAt;
          lastReviewedSource = "verifiedAt";
        } else if (updatedAt !== null) {
          lastReviewedMs = updatedAt;
          lastReviewedSource = "updatedAt";
        }
        const popularity = (row.numberOfQueries as number | null) ?? 0;
        const isDeprecated = row.isDeprecated === true;
        const owners = extractOwners(row);

        let daysSinceReview: number | null = null;
        let stalenessDays = 0;
        let isOverdue = false;
        let bucket: FreshnessRow["bucket"];
        if (lastReviewedMs === null) {
          bucket = "neverReviewed";
          isOverdue = true;
          // Treat never-reviewed as stale = required cadence (so neverReviewed
          // tables sort alongside other overdue tables of the same cadence).
          stalenessDays = requiredDays;
        } else {
          const elapsedMs = asOfMs - lastReviewedMs;
          daysSinceReview = Math.max(0, Math.floor(elapsedMs / DAY_MS));
          if (daysSinceReview > requiredDays) {
            stalenessDays = daysSinceReview - requiredDays;
            isOverdue = true;
            bucket = "overdue";
          } else if (daysSinceReview > requiredDays - 30) {
            bucket = "dueSoon";
          } else {
            bucket = "ok";
          }
        }
        const priorityScore = stalenessDays * Math.max(0, popularity);

        return {
          tableId: row.id as string,
          tableName: (row.name as string | null) ?? null,
          tablePath: tablePathFromRow(row),
          popularity,
          isVerified,
          isDeprecated,
          lastReviewedAt:
            lastReviewedMs !== null
              ? new Date(lastReviewedMs).toISOString()
              : null,
          lastReviewedSource,
          daysSinceReview,
          matchedSensitivityTags: matched,
          requiredCadenceDays: requiredDays,
          stalenessDays,
          isOverdue,
          bucket,
          priorityScore,
          owners,
        };
      });

      rows.sort((a, b) => b.priorityScore - a.priorityScore);
      const aggregate = computeAggregate(rows, scope.field, asOfMs, policy);
      const filtered = overdueOnly ? rows.filter((r) => r.isOverdue) : rows;

      return {
        scopedBy: scope.field,
        asOf: aggregate.asOf,
        aggregate,
        tables: filtered,
      };
    }, client),
  };
}
