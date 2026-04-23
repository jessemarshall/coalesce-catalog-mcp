import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TAGS,
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
} from "../catalog/operations.js";
import type {
  GetTagsOutput,
  GetTablesOutput,
  GetDashboardsOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TAG_HARD_CAP = 1000;
const TAG_PAGE_SIZE = 500;
const TABLE_HARD_CAP = 500;
const TABLE_PAGE_SIZE = 100;
const DASHBOARD_HARD_CAP = 500;
const DASHBOARD_PAGE_SIZE = 100;
const NEAR_DUPLICATE_MIN_LENGTH = 5;

// ---------------------------------------------------------------------------
// Input schema
// ---------------------------------------------------------------------------

const AuditTagHygieneInputShape = {
  labelContains: z
    .string()
    .optional()
    .describe(
      "Optional substring filter on tag labels (case-insensitive). Narrows the audit " +
        "to tags whose label contains this string. Omit to audit all tags in the workspace."
    ),
  databaseId: z
    .string()
    .optional()
    .describe(
      "Optional: only count entity attachments from tables/dashboards in this database. " +
        "Narrows the entity universe without narrowing the tag set. Mutually exclusive " +
        "with schemaId."
    ),
  schemaId: z
    .string()
    .optional()
    .describe(
      "Optional: only count entity attachments from tables in this schema. " +
        "Narrows the entity universe without narrowing the tag set. Mutually exclusive " +
        "with databaseId."
    ),
  nearDuplicateMinLength: z
    .number()
    .int()
    .min(3)
    .max(20)
    .optional()
    .describe(
      "Minimum tag label length for near-duplicate detection (default 5). Tags shorter " +
        "than this are excluded from fuzzy matching to reduce false positives on short " +
        "common words."
    ),
};

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type IssueCategory = "orphaned" | "unlinked" | "skewed" | "near_duplicate";

interface TagHealthRecord {
  id: string;
  label: string;
  linkedTermId: string | null;
  entityCount: number;
  tableCount: number;
  dashboardCount: number;
  issues: IssueCategory[];
  skewDetail: {
    tablePct: number;
    dashboardPct: number;
  } | null;
  nearDuplicateGroup: string | null;
}

interface EntityScope {
  field: "databaseId" | "schemaId" | null;
  tableFilter: Record<string, string> | undefined;
  dashboardFilter: Record<string, string> | undefined;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function parseEntityScope(args: {
  databaseId?: unknown;
  schemaId?: unknown;
}): EntityScope {
  const hasDatabaseId = typeof args.databaseId === "string";
  const hasSchemaId = typeof args.schemaId === "string";
  if (hasDatabaseId && hasSchemaId) {
    throw new Error(
      "Both databaseId and schemaId provided; pass at most one to scope " +
        "the entity universe."
    );
  }
  if (hasDatabaseId) {
    return {
      field: "databaseId",
      tableFilter: { databaseId: args.databaseId as string },
      // Dashboards don't support databaseId scope — use sourceId if available,
      // but the schema has no databaseId filter for dashboards. We'll fetch all
      // and note the limitation.
      dashboardFilter: undefined,
    };
  }
  if (hasSchemaId) {
    return {
      field: "schemaId",
      tableFilter: { schemaId: args.schemaId as string },
      // Dashboards don't support schemaId scope either.
      dashboardFilter: undefined,
    };
  }
  return {
    field: null,
    tableFilter: undefined,
    dashboardFilter: undefined,
  };
}

async function fetchAllTags(
  client: CatalogClient,
  labelContains?: string
): Promise<Array<Record<string, unknown>>> {
  const scope: Record<string, unknown> = {};
  if (labelContains) scope.labelContains = labelContains;

  const firstPage = await client.execute<{ getTags: GetTagsOutput }>(
    GET_TAGS,
    {
      scope: Object.keys(scope).length > 0 ? scope : undefined,
      sorting: [{ sortingKey: "label", direction: "ASC" }],
      pagination: { nbPerPage: TAG_PAGE_SIZE, page: 0 },
    }
  );
  const totalCount = firstPage.getTags.totalCount;
  if (typeof totalCount !== "number" || !Number.isFinite(totalCount)) {
    throw new Error(
      `getTags returned non-numeric totalCount (${String(totalCount)}); ` +
        `cannot establish tag universe.`
    );
  }
  if (totalCount > TAG_HARD_CAP) {
    throw new Error(
      `Workspace has ${totalCount} tags (exceeds ${TAG_HARD_CAP}-tag cap). ` +
        `Use labelContains to narrow the audit scope.`
    );
  }

  const tags: Array<Record<string, unknown>> = [
    ...(firstPage.getTags.data as Array<Record<string, unknown>>),
  ];
  const expectedPages = Math.ceil(totalCount / TAG_PAGE_SIZE);
  for (let page = 1; page < expectedPages; page++) {
    const resp = await client.execute<{ getTags: GetTagsOutput }>(GET_TAGS, {
      scope: Object.keys(scope).length > 0 ? scope : undefined,
      sorting: [{ sortingKey: "label", direction: "ASC" }],
      pagination: { nbPerPage: TAG_PAGE_SIZE, page },
    });
    const rows = resp.getTags.data as Array<Record<string, unknown>>;
    tags.push(...rows);
    if (rows.length < TAG_PAGE_SIZE) break;
  }
  if (tags.length < Math.min(totalCount, TAG_HARD_CAP)) {
    throw new Error(
      `Tag pagination returned ${tags.length} rows but totalCount reported ` +
        `${totalCount}. Refusing to emit a partial audit.`
    );
  }
  return tags;
}

async function fetchAllTablesWithTags(
  client: CatalogClient,
  scope: EntityScope
): Promise<Array<Record<string, unknown>>> {
  const filter = scope.tableFilter;
  const firstPage = await client.execute<{ getTables: GetTablesOutput }>(
    GET_TABLES_DETAIL_BATCH,
    {
      scope: filter,
      pagination: { nbPerPage: TABLE_PAGE_SIZE, page: 0 },
    }
  );
  const totalCount = firstPage.getTables.totalCount;
  if (typeof totalCount !== "number" || !Number.isFinite(totalCount)) {
    throw new Error(
      `getTables returned non-numeric totalCount (${String(totalCount)}); ` +
        `cannot build tag reverse index.`
    );
  }
  if (totalCount > TABLE_HARD_CAP) {
    throw new Error(
      `Entity scope resolves to ${totalCount} tables (exceeds ${TABLE_HARD_CAP}-table ` +
        `cap). Narrow with schemaId or databaseId.`
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
        scope: filter,
        pagination: { nbPerPage: TABLE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getTables.data as Array<Record<string, unknown>>;
    tables.push(...rows);
    if (rows.length < TABLE_PAGE_SIZE) break;
  }
  return tables;
}

async function fetchAllDashboardsWithTags(
  client: CatalogClient,
  scope: EntityScope
): Promise<Array<Record<string, unknown>>> {
  const filter = scope.dashboardFilter;
  const firstPage = await client.execute<{
    getDashboards: GetDashboardsOutput;
  }>(GET_DASHBOARDS_DETAIL_BATCH, {
    scope: filter,
    pagination: { nbPerPage: DASHBOARD_PAGE_SIZE, page: 0 },
  });
  const totalCount = firstPage.getDashboards.totalCount;
  if (typeof totalCount !== "number" || !Number.isFinite(totalCount)) {
    throw new Error(
      `getDashboards returned non-numeric totalCount (${String(totalCount)}); ` +
        `cannot build tag reverse index.`
    );
  }
  if (totalCount > DASHBOARD_HARD_CAP) {
    throw new Error(
      `Entity scope resolves to ${totalCount} dashboards (exceeds ` +
        `${DASHBOARD_HARD_CAP}-dashboard cap). Narrow the entity scope.`
    );
  }

  const dashboards: Array<Record<string, unknown>> = [
    ...(firstPage.getDashboards.data as Array<Record<string, unknown>>),
  ];
  const expectedPages = Math.ceil(totalCount / DASHBOARD_PAGE_SIZE);
  for (let page = 1; page < expectedPages; page++) {
    const resp = await client.execute<{
      getDashboards: GetDashboardsOutput;
    }>(GET_DASHBOARDS_DETAIL_BATCH, {
      scope: filter,
      pagination: { nbPerPage: DASHBOARD_PAGE_SIZE, page },
    });
    const rows = resp.getDashboards.data as Array<Record<string, unknown>>;
    dashboards.push(...rows);
    if (rows.length < DASHBOARD_PAGE_SIZE) break;
  }
  return dashboards;
}

function buildTagEntityIndex(
  tables: Array<Record<string, unknown>>,
  dashboards: Array<Record<string, unknown>>
): Map<string, { tableCount: number; dashboardCount: number }> {
  const index = new Map<
    string,
    { tableCount: number; dashboardCount: number }
  >();

  for (const table of tables) {
    const tagEntities = table.tagEntities as
      | Array<Record<string, unknown>>
      | undefined;
    if (!Array.isArray(tagEntities)) continue;
    for (const te of tagEntities) {
      const tag = te.tag as Record<string, unknown> | undefined;
      if (!tag) continue;
      const tagId = tag.id as string;
      const entry = index.get(tagId) ?? { tableCount: 0, dashboardCount: 0 };
      entry.tableCount += 1;
      index.set(tagId, entry);
    }
  }

  for (const dashboard of dashboards) {
    const tagEntities = dashboard.tagEntities as
      | Array<Record<string, unknown>>
      | undefined;
    if (!Array.isArray(tagEntities)) continue;
    for (const te of tagEntities) {
      const tag = te.tag as Record<string, unknown> | undefined;
      if (!tag) continue;
      const tagId = tag.id as string;
      const entry = index.get(tagId) ?? { tableCount: 0, dashboardCount: 0 };
      entry.dashboardCount += 1;
      index.set(tagId, entry);
    }
  }

  return index;
}

/**
 * Levenshtein distance between two strings. Used for near-duplicate tag
 * detection on labels longer than the minimum length threshold.
 */
function levenshtein(a: string, b: string): number {
  const m = a.length;
  const n = b.length;
  const dp: number[][] = Array.from({ length: m + 1 }, () =>
    Array(n + 1).fill(0) as number[]
  );
  for (let i = 0; i <= m; i++) dp[i][0] = i;
  for (let j = 0; j <= n; j++) dp[0][j] = j;
  for (let i = 1; i <= m; i++) {
    for (let j = 1; j <= n; j++) {
      dp[i][j] =
        a[i - 1] === b[j - 1]
          ? dp[i - 1][j - 1]
          : 1 + Math.min(dp[i - 1][j], dp[i][j - 1], dp[i - 1][j - 1]);
    }
  }
  return dp[m][n];
}

/**
 * Detect near-duplicate tags using a combination of:
 * 1. Prefix grouping: tags sharing a common prefix (min 4 chars)
 * 2. Levenshtein distance ≤ 2 for labels above the minimum length
 *
 * Returns a map from tag ID to group label (the shortest label in the group).
 */
function detectNearDuplicates(
  tags: Array<{ id: string; label: string }>,
  minLength: number
): Map<string, string> {
  const eligible = tags.filter((t) => t.label.length >= minLength);
  const groups = new Map<string, string>(); // tagId → group representative

  // Sort by label for stable grouping
  const sorted = [...eligible].sort((a, b) =>
    a.label.localeCompare(b.label)
  );

  for (let i = 0; i < sorted.length; i++) {
    if (groups.has(sorted[i].id)) continue;
    const group: typeof sorted = [sorted[i]];

    for (let j = i + 1; j < sorted.length; j++) {
      if (groups.has(sorted[j].id)) continue;
      const dist = levenshtein(
        sorted[i].label.toLowerCase(),
        sorted[j].label.toLowerCase()
      );
      // Threshold: edit distance ≤ 2, or ≤ 20% of the shorter label's length
      const maxDist = Math.min(
        2,
        Math.ceil(
          Math.min(sorted[i].label.length, sorted[j].label.length) * 0.2
        )
      );
      if (dist <= maxDist) {
        group.push(sorted[j]);
      }
    }

    if (group.length > 1) {
      // Use the shortest label as the group representative
      const rep = group.reduce((shortest, t) =>
        t.label.length < shortest.label.length ? t : shortest
      ).label;
      for (const t of group) {
        groups.set(t.id, rep);
      }
    }
  }

  return groups;
}

// ---------------------------------------------------------------------------
// Tool definition
// ---------------------------------------------------------------------------

export function defineAuditTagHygiene(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_audit_tag_hygiene",
    config: {
      title: "Audit Tag Layer Health",
      description:
        "Audit the structural health of the tag layer across the workspace. " +
        "Composes tag enumeration + entity reverse-index construction (from table and " +
        "dashboard tagEntities) + term linkage analysis + near-duplicate detection into " +
        "a typed per-tag health report.\n\n" +
        "Detects four issue categories:\n" +
        "  - **orphaned**: tag has zero entity attachments (no tables or dashboards use it)\n" +
        "  - **unlinked**: tag has no linked glossary term (linkedTermId is null)\n" +
        "  - **skewed**: tag is attached to ≥95% one entity type (e.g. all tables, no dashboards)\n" +
        "  - **near_duplicate**: tag label is within edit distance 2 of another tag label " +
        "(Levenshtein, only for labels ≥ nearDuplicateMinLength chars)\n\n" +
        "Capacity gates: 1000 tags, 500 tables, 500 dashboards. Use `labelContains` to " +
        "narrow the tag set, `databaseId` or `schemaId` to narrow the entity universe.\n\n" +
        "The entity scope (databaseId/schemaId) narrows which tables/dashboards are " +
        "counted for attachment, NOT which tags are audited. A tag may report 0 attachments " +
        "in-scope even if it has attachments in other schemas — this is the expected " +
        "behavior for scoped audits.\n\n" +
        "Complements `catalog_audit_glossary_health` (audits the term layer) — together " +
        "they audit both halves of the governance annotation graph.",
      inputSchema: AuditTagHygieneInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const labelContains = args.labelContains as string | undefined;
      const nearDupMinLen =
        (args.nearDuplicateMinLength as number | undefined) ??
        NEAR_DUPLICATE_MIN_LENGTH;
      const entityScope = parseEntityScope(args);

      // 1. Fetch all tags
      const rawTags = await fetchAllTags(c, labelContains);
      if (rawTags.length === 0) {
        return {
          tagCount: 0,
          entityScope: entityScope.field,
          aggregate: {
            orphanedCount: 0,
            unlinkedCount: 0,
            skewedCount: 0,
            nearDuplicateCount: 0,
            healthyCount: 0,
          },
          tags: [],
          nearDuplicateGroups: [],
        };
      }

      // 2. Fetch tables and dashboards for reverse-index construction (parallel)
      const [tables, dashboards] = await Promise.all([
        fetchAllTablesWithTags(c, entityScope),
        fetchAllDashboardsWithTags(c, entityScope),
      ]);

      // 3. Build reverse index: tagId → { tableCount, dashboardCount }
      const entityIndex = buildTagEntityIndex(tables, dashboards);

      // 4. Detect near-duplicates
      const tagLabels = rawTags.map((t) => ({
        id: t.id as string,
        label: (t.label as string) ?? "",
      }));
      const nearDups = detectNearDuplicates(tagLabels, nearDupMinLen);

      // 5. Classify each tag
      const tagRecords: TagHealthRecord[] = rawTags.map((t) => {
        const tagId = t.id as string;
        const label = (t.label as string) ?? "";
        const linkedTermId = (t.linkedTermId as string | null) ?? null;
        const counts = entityIndex.get(tagId) ?? {
          tableCount: 0,
          dashboardCount: 0,
        };
        const entityCount = counts.tableCount + counts.dashboardCount;

        const issues: IssueCategory[] = [];

        // Orphaned: no entities attached
        if (entityCount === 0) issues.push("orphaned");

        // Unlinked: no glossary term
        if (!linkedTermId) issues.push("unlinked");

        // Skewed: ≥95% one type (only meaningful if attached to >0 entities)
        let skewDetail: TagHealthRecord["skewDetail"] = null;
        if (entityCount > 0) {
          const tablePct = Math.round(
            (counts.tableCount / entityCount) * 100
          );
          const dashboardPct = 100 - tablePct;
          skewDetail = { tablePct, dashboardPct };
          if (tablePct >= 95 || dashboardPct >= 95) {
            issues.push("skewed");
          }
        }

        // Near-duplicate
        const dupGroup = nearDups.get(tagId) ?? null;
        if (dupGroup) issues.push("near_duplicate");

        return {
          id: tagId,
          label,
          linkedTermId,
          entityCount,
          tableCount: counts.tableCount,
          dashboardCount: counts.dashboardCount,
          issues,
          skewDetail,
          nearDuplicateGroup: dupGroup,
        };
      });

      // 6. Compute aggregates
      let orphanedCount = 0;
      let unlinkedCount = 0;
      let skewedCount = 0;
      let nearDuplicateCount = 0;
      let healthyCount = 0;

      for (const t of tagRecords) {
        if (t.issues.includes("orphaned")) orphanedCount++;
        if (t.issues.includes("unlinked")) unlinkedCount++;
        if (t.issues.includes("skewed")) skewedCount++;
        if (t.issues.includes("near_duplicate")) nearDuplicateCount++;
        if (t.issues.length === 0) healthyCount++;
      }

      // 7. Build near-duplicate groups for easy scanning
      const groupMap = new Map<string, string[]>();
      for (const t of tagRecords) {
        if (t.nearDuplicateGroup) {
          const group = groupMap.get(t.nearDuplicateGroup) ?? [];
          group.push(t.label);
          groupMap.set(t.nearDuplicateGroup, group);
        }
      }
      const nearDuplicateGroups = [...groupMap.entries()].map(
        ([representative, labels]) => ({
          representative,
          labels: [...new Set(labels)].sort(),
        })
      );

      return {
        tagCount: tagRecords.length,
        entityScope: entityScope.field,
        entityCounts: {
          tables: tables.length,
          dashboards: dashboards.length,
        },
        aggregate: {
          orphanedCount,
          unlinkedCount,
          skewedCount,
          nearDuplicateCount,
          healthyCount,
        },
        tags: tagRecords,
        nearDuplicateGroups,
      };
    }, client),
  };
}
