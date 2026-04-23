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

// ── Input schema ────────────────────────────────────────────────────────────

const AuditTagHygieneInputShape = {
  maxTags: z
    .number()
    .int()
    .min(1)
    .max(1000)
    .optional()
    .describe(
      "Capacity gate for tags. Refuses if the workspace has more tags than this limit. Default 1000, max 1000."
    ),
  maxAssets: z
    .number()
    .int()
    .min(1)
    .max(500)
    .optional()
    .describe(
      "Capacity gate for tables + dashboards to scan. Default 500, max 500."
    ),
  databaseId: z
    .string()
    .optional()
    .describe("Scope the table/dashboard scan to a specific database UUID."),
  schemaId: z
    .string()
    .optional()
    .describe("Scope the table/dashboard scan to a specific schema UUID."),
  nearDuplicateThreshold: z
    .number()
    .int()
    .min(0)
    .max(5)
    .optional()
    .describe(
      "Levenshtein distance threshold for near-duplicate tag detection. Default 2, max 5."
    ),
};

// ── Constants ───────────────────────────────────────────────────────────────

const TAG_PAGE_SIZE = 100;
const ASSET_PAGE_SIZE = 100;
const DEFAULT_MAX_TAGS = 1000;
const DEFAULT_MAX_ASSETS = 500;
const DEFAULT_NEAR_DUPLICATE_THRESHOLD = 2;
const SKEW_THRESHOLD = 0.8;
const SKEW_MIN_USAGE = 5;

// ── Levenshtein edit distance ───────────────────────────────────────────────

function editDistance(a: string, b: string): number {
  const la = a.length;
  const lb = b.length;
  if (la === 0) return lb;
  if (lb === 0) return la;

  // Single-row DP to keep memory O(min(la, lb)).
  let prev = Array.from({ length: lb + 1 }, (_, i) => i);
  let curr = new Array<number>(lb + 1);

  for (let i = 1; i <= la; i++) {
    curr[0] = i;
    for (let j = 1; j <= lb; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(
        prev[j] + 1, // deletion
        curr[j - 1] + 1, // insertion
        prev[j - 1] + cost // substitution
      );
    }
    [prev, curr] = [curr, prev];
  }
  return prev[lb];
}

// ── Types ───────────────────────────────────────────────────────────────────

interface TagRecord {
  id: string;
  label: string;
  color: string | null;
  linkedTermId: string | null;
}

interface TagUsageRow {
  tagId: string;
  label: string;
  linkedTermId: string | null;
  tableCount: number;
  dashboardCount: number;
  totalUsage: number;
}

// ── Tool definition ─────────────────────────────────────────────────────────

export function defineAuditTagHygiene(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_audit_tag_hygiene",
    config: {
      title: "Audit Tag Hygiene",
      description:
        "Audit the structural health of the tag layer. Composes GET_TAGS " +
        "(paginated), GET_TABLES_DETAIL_BATCH, and GET_DASHBOARDS_DETAIL_BATCH " +
        "to build a reverse index of tag usage across tables and dashboards, " +
        "then detects four classes of findings:\n\n" +
        "  - Orphaned: tags not attached to any table or dashboard.\n" +
        "  - Unlinked: tags in active use but not linked to a glossary term.\n" +
        "  - Skewed: tags where >80% of usage is concentrated on a single entity " +
        "type (TABLE or DASHBOARD) with at least 5 total uses.\n" +
        "  - Near-duplicates: pairs of tag labels within a configurable " +
        "Levenshtein distance threshold.\n\n" +
        "Returns a summary with counts, detailed findings arrays, and a full " +
        "tagUsage roster sorted by total usage DESC. Capacity-gated at 1000 " +
        "tags and 500 assets (tables + dashboards); refuses with an actionable " +
        "message if limits are exceeded. Scope table/dashboard scans with " +
        "databaseId or schemaId for large workspaces.",
      inputSchema: AuditTagHygieneInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const maxTags = (args.maxTags as number | undefined) ?? DEFAULT_MAX_TAGS;
      const maxAssets =
        (args.maxAssets as number | undefined) ?? DEFAULT_MAX_ASSETS;
      const nearDuplicateThreshold =
        (args.nearDuplicateThreshold as number | undefined) ??
        DEFAULT_NEAR_DUPLICATE_THRESHOLD;
      const databaseId = args.databaseId as string | undefined;
      const schemaId = args.schemaId as string | undefined;

      // ── 1. Fetch all tags (paginated) ─────────────────────────────────
      const tags: TagRecord[] = [];
      const maxTagPages = Math.ceil(maxTags / TAG_PAGE_SIZE) + 1;

      for (let page = 0; page < maxTagPages; page++) {
        const resp = await c.execute<{ getTags: GetTagsOutput }>(GET_TAGS, {
          pagination: { nbPerPage: TAG_PAGE_SIZE, page },
        });
        const totalCount = resp.getTags.totalCount;
        if (
          typeof totalCount !== "number" ||
          !Number.isFinite(totalCount)
        ) {
          throw new Error(
            `getTags returned non-numeric totalCount (${String(totalCount)}); ` +
              `cannot verify pagination completeness.`
          );
        }
        if (totalCount > maxTags) {
          throw new Error(
            `Workspace contains ${totalCount} tags, exceeding the maxTags ` +
              `capacity gate of ${maxTags}. Narrow the scope or increase ` +
              `maxTags (max 1000).`
          );
        }
        const rows = resp.getTags.data as Array<Record<string, unknown>>;
        for (const row of rows) {
          tags.push({
            id: row.id as string,
            label: row.label as string,
            color: (row.color as string | null) ?? null,
            linkedTermId: (row.linkedTermId as string | null) ?? null,
          });
        }
        if (rows.length < TAG_PAGE_SIZE) break;
        const fetchedSoFar = (page + 1) * TAG_PAGE_SIZE;
        if (fetchedSoFar >= totalCount) break;
      }

      // ── 2. Build tag-id lookup ────────────────────────────────────────
      const tagUsageMap = new Map<
        string,
        { tableCount: number; dashboardCount: number }
      >();
      // Index by label (lowercased) -> tag id, for reverse lookup from
      // tagEntities on tables/dashboards which carry tag.label.
      const labelToId = new Map<string, string>();
      for (const tag of tags) {
        tagUsageMap.set(tag.id, { tableCount: 0, dashboardCount: 0 });
        labelToId.set(tag.label.toLowerCase(), tag.id);
      }

      // ── 3. Fetch tables (paginated, scoped) ──────────────────────────
      const tableScope: Record<string, unknown> = {};
      if (databaseId) tableScope.databaseId = databaseId;
      if (schemaId) tableScope.schemaId = schemaId;

      let tablesScanned = 0;
      const tableMaxPages = Math.ceil(maxAssets / ASSET_PAGE_SIZE) + 1;

      for (let page = 0; page < tableMaxPages; page++) {
        const resp = await c.execute<{ getTables: GetTablesOutput }>(
          GET_TABLES_DETAIL_BATCH,
          {
            scope: tableScope,
            pagination: { nbPerPage: ASSET_PAGE_SIZE, page },
          }
        );
        const totalCount = resp.getTables.totalCount;
        if (
          typeof totalCount !== "number" ||
          !Number.isFinite(totalCount)
        ) {
          throw new Error(
            `getTables returned non-numeric totalCount (${String(totalCount)}).`
          );
        }
        const rows = resp.getTables.data as Array<Record<string, unknown>>;
        for (const row of rows) {
          if (tablesScanned >= maxAssets) break;
          tablesScanned++;
          const tagEntities = Array.isArray(row.tagEntities)
            ? (row.tagEntities as Array<Record<string, unknown>>)
            : [];
          for (const te of tagEntities) {
            const tag = te.tag as Record<string, unknown> | undefined;
            if (!tag) continue;
            const tagId = tag.id as string;
            const usage = tagUsageMap.get(tagId);
            if (usage) {
              usage.tableCount++;
            }
          }
        }
        if (tablesScanned >= maxAssets) break;
        if (rows.length < ASSET_PAGE_SIZE) break;
        const fetchedSoFar = (page + 1) * ASSET_PAGE_SIZE;
        if (fetchedSoFar >= totalCount) break;
      }

      // ── 4. Fetch dashboards (paginated) ───────────────────────────────
      const remainingAssetBudget = maxAssets - tablesScanned;
      let dashboardsScanned = 0;

      if (remainingAssetBudget > 0) {
        const dashboardMaxPages =
          Math.ceil(remainingAssetBudget / ASSET_PAGE_SIZE) + 1;

        for (let page = 0; page < dashboardMaxPages; page++) {
          const resp = await c.execute<{
            getDashboards: GetDashboardsOutput;
          }>(GET_DASHBOARDS_DETAIL_BATCH, {
            scope: {},
            pagination: { nbPerPage: ASSET_PAGE_SIZE, page },
          });
          const totalCount = resp.getDashboards.totalCount;
          if (
            typeof totalCount !== "number" ||
            !Number.isFinite(totalCount)
          ) {
            throw new Error(
              `getDashboards returned non-numeric totalCount (${String(totalCount)}).`
            );
          }
          const rows = resp.getDashboards.data as Array<
            Record<string, unknown>
          >;
          for (const row of rows) {
            if (dashboardsScanned >= remainingAssetBudget) break;
            dashboardsScanned++;
            const tagEntities = Array.isArray(row.tagEntities)
              ? (row.tagEntities as Array<Record<string, unknown>>)
              : [];
            for (const te of tagEntities) {
              const tag = te.tag as Record<string, unknown> | undefined;
              if (!tag) continue;
              const tagId = tag.id as string;
              const usage = tagUsageMap.get(tagId);
              if (usage) {
                usage.dashboardCount++;
              }
            }
          }
          if (dashboardsScanned >= remainingAssetBudget) break;
          if (rows.length < ASSET_PAGE_SIZE) break;
          const fetchedSoFar = (page + 1) * ASSET_PAGE_SIZE;
          if (fetchedSoFar >= totalCount) break;
        }
      }

      // ── 5. Build tagUsage array ───────────────────────────────────────
      const tagUsage: TagUsageRow[] = tags.map((tag) => {
        const usage = tagUsageMap.get(tag.id) ?? {
          tableCount: 0,
          dashboardCount: 0,
        };
        return {
          tagId: tag.id,
          label: tag.label,
          linkedTermId: tag.linkedTermId,
          tableCount: usage.tableCount,
          dashboardCount: usage.dashboardCount,
          totalUsage: usage.tableCount + usage.dashboardCount,
        };
      });
      tagUsage.sort((a, b) => b.totalUsage - a.totalUsage);

      // ── 6. Detect findings ────────────────────────────────────────────

      // Orphaned: tags with 0 total usage
      const orphaned = tagUsage
        .filter((t) => t.totalUsage === 0)
        .map((t) => {
          const tag = tags.find((tg) => tg.id === t.tagId)!;
          return { tagId: t.tagId, label: t.label, color: tag.color };
        });

      // Unlinked: tags with no linkedTermId and usage > 0
      const unlinked = tagUsage
        .filter((t) => t.linkedTermId === null && t.totalUsage > 0)
        .map((t) => ({
          tagId: t.tagId,
          label: t.label,
          usageCount: t.totalUsage,
        }));

      // Skewed: tags where >80% of usage is on a single entity type, totalUsage >= 5
      const skewed: Array<{
        tagId: string;
        label: string;
        totalUsage: number;
        dominantType: "TABLE" | "DASHBOARD";
        dominantPercent: number;
      }> = [];
      for (const t of tagUsage) {
        if (t.totalUsage < SKEW_MIN_USAGE) continue;
        const tablePct = t.tableCount / t.totalUsage;
        const dashPct = t.dashboardCount / t.totalUsage;
        if (tablePct > SKEW_THRESHOLD) {
          skewed.push({
            tagId: t.tagId,
            label: t.label,
            totalUsage: t.totalUsage,
            dominantType: "TABLE",
            dominantPercent: Math.round(tablePct * 100),
          });
        } else if (dashPct > SKEW_THRESHOLD) {
          skewed.push({
            tagId: t.tagId,
            label: t.label,
            totalUsage: t.totalUsage,
            dominantType: "DASHBOARD",
            dominantPercent: Math.round(dashPct * 100),
          });
        }
      }

      // Near-duplicates: pairs within Levenshtein distance <= threshold
      const nearDuplicates: Array<{
        group: string[];
        tagIds: string[];
        distance: number;
      }> = [];
      if (nearDuplicateThreshold > 0) {
        // Compare all pairs. For workspaces with <= 1000 tags this is at most
        // ~500K comparisons — fast enough for a single-shot audit.
        const visited = new Set<string>();
        for (let i = 0; i < tags.length; i++) {
          if (visited.has(tags[i].id)) continue;
          for (let j = i + 1; j < tags.length; j++) {
            if (visited.has(tags[j].id)) continue;
            const dist = editDistance(
              tags[i].label.toLowerCase(),
              tags[j].label.toLowerCase()
            );
            if (dist > 0 && dist <= nearDuplicateThreshold) {
              nearDuplicates.push({
                group: [tags[i].label, tags[j].label],
                tagIds: [tags[i].id, tags[j].id],
                distance: dist,
              });
              // Don't mark as visited — a tag can appear in multiple
              // near-duplicate pairs with different partners.
            }
          }
        }
      }

      // ── 7. Assemble result ────────────────────────────────────────────
      return {
        summary: {
          totalTags: tags.length,
          totalTablesScanned: tablesScanned,
          totalDashboardsScanned: dashboardsScanned,
          orphanedCount: orphaned.length,
          unlinkedCount: unlinked.length,
          skewedCount: skewed.length,
          nearDuplicateGroupCount: nearDuplicates.length,
        },
        findings: {
          orphaned,
          unlinked,
          skewed,
          nearDuplicates,
        },
        tagUsage,
      };
    }, client),
  };
}
