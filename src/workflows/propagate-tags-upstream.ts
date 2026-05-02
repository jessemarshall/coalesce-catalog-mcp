import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  WRITE_ANNOTATIONS,
  type CatalogToolDefinition,
  type ToolHandlerExtra,
} from "../catalog/types.js";
import {
  GET_TABLE_DETAIL,
  GET_DASHBOARD_DETAIL,
  GET_TABLES_DETAIL_BATCH,
  GET_LINEAGES,
  ATTACH_TAGS,
} from "../catalog/operations.js";
import type {
  BaseTagEntityInput,
  GetLineagesOutput,
  GetTablesOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import { withConfirmation } from "../mcp/confirmation.js";
import { ENRICHMENT_BATCH_SIZE, chunk, extractTagLabels } from "./shared.js";

type OverwritePolicy = "ifEmpty" | "overwrite";
type SourceAssetType = "TABLE" | "DASHBOARD";

// ── Input schema ────────────────────────────────────────────────────────────

const PropagateTagsUpstreamInputShape = {
  sourceAssetId: z
    .string()
    .min(1)
    .describe(
      "Catalog UUID of the source asset (table or dashboard) whose tags should propagate to upstream warehouse tables."
    ),
  sourceAssetType: z
    .enum(["TABLE", "DASHBOARD"])
    .describe(
      "Type of the source asset. DASHBOARD is the typical N-able-style use case ('this report is critical, mark its sources'); TABLE supports gold/presentation layer → upstream warehouse propagation."
    ),
  tagLabels: z
    .array(z.string())
    .optional()
    .describe(
      "Explicit list of tag labels to propagate (case-insensitive match against the source's attached tags). Default: every tag attached to the source. Useful when the source carries a mix of relevant ('Critical') and irrelevant ('Tableau-internal') tags."
    ),
  maxDepth: z
    .number()
    .int()
    .min(1)
    .max(3)
    .optional()
    .describe(
      "How many lineage hops upstream to propagate. 1 = immediate parents only (cheapest, completes always). 2 = parents-of-parents (refuses if frontier exceeds 2000 nodes). 3 = deep upstream (refuses if frontier exceeds 500). Default 1."
    ),
  overwritePolicy: z
    .enum(["ifEmpty", "overwrite"])
    .optional()
    .describe(
      "ifEmpty (default): only attach tags to upstream tables that currently have NO tags — most conservative; cannot clobber hand-curated metadata. overwrite: additively merge — adds source tags missing from each upstream table; never removes target-native tags."
    ),
  dryRun: z
    .boolean()
    .optional()
    .describe(
      "When true (default), compute the full diff plan and return without mutating anything. When false, execute the plan — additionally requires acknowledgeProvenanceSemantics=true and routes through MCP elicitation (or COALESCE_CATALOG_SKIP_CONFIRMATIONS=true)."
    ),
  acknowledgeProvenanceSemantics: z
    .boolean()
    .optional()
    .describe(
      "REQUIRED to be true when dryRun=false. Acknowledges the user understands UPSTREAM tag propagation is semantically different from downstream: a 'Critical' tag on a presentation dashboard does NOT necessarily mean the upstream warehouse table is critical — that table may feed many consumers, of which the source is one. Tag the source AND review the dry-run plan; only execute when the agent has validated the propagation is semantically correct for every upstream target."
    ),
};

// ── Constants ───────────────────────────────────────────────────────────────

const UPSTREAM_HARD_CAP = 200;
const LINEAGE_PAGE_SIZE = 500;
const LINEAGE_PAGES_PER_NODE_MAX = 20;
const LINEAGE_FANOUT_PARALLELISM = 20;
const TAG_ATTACH_BATCH_SIZE = 500;

// ── Internal types ──────────────────────────────────────────────────────────

interface SourceAsset {
  id: string;
  name: string | null;
  assetType: SourceAssetType;
  tagLabels: string[];
}

interface UpstreamTarget {
  id: string;
  name: string | null;
  depth: number;
  tagLabels: string[];
  // Path from source to this upstream node (table ids in BFS-discovery order
  // — the first id is the closest parent, NOT necessarily a unique shortest
  // path when multiple paths exist).
  pathFromSource: Array<{ tableId: string; tableName: string | null }>;
}

interface TagsChange {
  action: "add" | "skip";
  reason: string;
  added: string[];
  alreadyPresent: string[];
}

interface TargetPlan {
  tableId: string;
  tableName: string | null;
  depth: number;
  provenance: {
    sourceAssetId: string;
    sourceAssetType: SourceAssetType;
    sourceAssetName: string | null;
    pathFromSource: Array<{ tableId: string; tableName: string | null }>;
  };
  changes: { tags: TagsChange };
}

// ── Helpers ─────────────────────────────────────────────────────────────────

async function fetchSourceAsset(
  client: CatalogClient,
  id: string,
  assetType: SourceAssetType
): Promise<SourceAsset | null> {
  if (assetType === "TABLE") {
    const resp = await client.execute<{
      getTables: { data: Record<string, unknown>[] };
    }>(GET_TABLE_DETAIL, { ids: [id] });
    const row = resp.getTables.data[0];
    if (!row) return null;
    return {
      id: row.id as string,
      name: (row.name as string | null) ?? null,
      assetType: "TABLE",
      tagLabels: extractTagLabels(row),
    };
  }
  const resp = await client.execute<{
    getDashboards: { data: Record<string, unknown>[] };
  }>(GET_DASHBOARD_DETAIL, { ids: [id] });
  const row = resp.getDashboards.data[0];
  if (!row) return null;
  return {
    id: row.id as string,
    name: (row.name as string | null) ?? null,
    assetType: "DASHBOARD",
    tagLabels: extractTagLabels(row),
  };
}

async function fetchUpstreamParentTableIds(
  client: CatalogClient,
  childId: string,
  childKind: "TABLE" | "DASHBOARD"
): Promise<string[]> {
  const out: string[] = [];
  for (let page = 0; page < LINEAGE_PAGES_PER_NODE_MAX; page++) {
    const scope: Record<string, string> = {};
    if (childKind === "TABLE") scope.childTableId = childId;
    else scope.childDashboardId = childId;
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope,
        pagination: { nbPerPage: LINEAGE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getLineages.data;
    for (const e of rows) {
      // The lineage model in this catalog has table parents only — there is
      // no parent-dashboard edge shape. Filter defensively.
      if (e.parentTableId) out.push(e.parentTableId);
    }
    if (rows.length < LINEAGE_PAGE_SIZE) return out;
  }
  throw new Error(
    `Lineage pagination exceeded ${LINEAGE_PAGES_PER_NODE_MAX} pages upstream of ` +
      `${childKind.toLowerCase()} ${childId} (>${LINEAGE_PAGES_PER_NODE_MAX * LINEAGE_PAGE_SIZE} edges). ` +
      `Refusing to produce a partial propagation plan.`
  );
}

interface TraversalResult {
  // tableId -> { depth, pathFromSource[] }
  visitedAtDepth: Map<
    string,
    { depth: number; pathFromSource: string[] }
  >;
}

function throwUpstreamCapacityExceeded(cap: number): never {
  throw new Error(
    `Upstream traversal reached more than ${cap} tables ` +
      `(capacity gate). Reduce maxDepth, narrow the source asset's tag set, ` +
      `or split into separate runs by sourceAssetId.`
  );
}

async function traverseUpstream(
  client: CatalogClient,
  source: SourceAsset,
  maxDepth: number,
  upstreamHardCap: number
): Promise<TraversalResult> {
  const visitedAtDepth = new Map<
    string,
    { depth: number; pathFromSource: string[] }
  >();
  // Step 1: immediate parents — depends on source type.
  const firstParents = await fetchUpstreamParentTableIds(
    client,
    source.id,
    source.assetType
  );
  for (const pid of firstParents) {
    if (visitedAtDepth.has(pid)) continue;
    visitedAtDepth.set(pid, { depth: 1, pathFromSource: [pid] });
    if (visitedAtDepth.size > upstreamHardCap) {
      throwUpstreamCapacityExceeded(upstreamHardCap);
    }
  }
  let frontier = firstParents.slice();

  for (let depth = 2; depth <= maxDepth; depth++) {
    const nextFrontier: string[] = [];
    const edgesPerNode: Array<{ parent: string; children: string[] }> = [];
    for (let i = 0; i < frontier.length; i += LINEAGE_FANOUT_PARALLELISM) {
      const slice = frontier.slice(i, i + LINEAGE_FANOUT_PARALLELISM);
      const sliceResults = await Promise.all(
        slice.map(async (node) => ({
          parent: node,
          children: await fetchUpstreamParentTableIds(client, node, "TABLE"),
        }))
      );
      edgesPerNode.push(...sliceResults);
    }
    for (const r of edgesPerNode) {
      const parentInfo = visitedAtDepth.get(r.parent);
      if (!parentInfo) continue; // defensive — should always be present
      for (const grandparentId of r.children) {
        if (visitedAtDepth.has(grandparentId)) continue;
        const newPath = [...parentInfo.pathFromSource, grandparentId];
        visitedAtDepth.set(grandparentId, {
          depth,
          pathFromSource: newPath,
        });
        nextFrontier.push(grandparentId);
        if (visitedAtDepth.size > upstreamHardCap) {
          throwUpstreamCapacityExceeded(upstreamHardCap);
        }
      }
    }
    if (nextFrontier.length === 0) break;
    frontier = nextFrontier;
  }
  return { visitedAtDepth };
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

function planTags(
  source: SourceAsset,
  effectiveTagLabels: string[],
  target: UpstreamTarget,
  policy: OverwritePolicy
): TagsChange {
  if (effectiveTagLabels.length === 0) {
    return {
      action: "skip",
      reason: "no tags to propagate (source has none, or tagLabels filter excluded all)",
      added: [],
      alreadyPresent: [],
    };
  }
  if (policy === "ifEmpty" && target.tagLabels.length > 0) {
    return {
      action: "skip",
      reason: "target already has tags (overwritePolicy=ifEmpty)",
      added: [],
      alreadyPresent: [...target.tagLabels],
    };
  }
  const existing = new Set(target.tagLabels);
  const toAdd: string[] = [];
  const alreadyPresent: string[] = [];
  for (const label of effectiveTagLabels) {
    if (existing.has(label)) alreadyPresent.push(label);
    else toAdd.push(label);
  }
  if (toAdd.length === 0) {
    return {
      action: "skip",
      reason: "every selected source tag is already attached to the upstream target",
      added: [],
      alreadyPresent,
    };
  }
  return {
    action: "add",
    reason: `propagating ${toAdd.length} tag(s) upstream from ${source.assetType.toLowerCase()} '${source.name ?? source.id}'`,
    added: toAdd,
    alreadyPresent,
  };
}

function effectiveTagFilter(
  source: SourceAsset,
  tagLabelsArg: string[] | undefined
): string[] {
  if (!tagLabelsArg || tagLabelsArg.length === 0) return source.tagLabels;
  const wanted = new Set(tagLabelsArg.map((s) => s.toLowerCase()));
  return source.tagLabels.filter((label) => wanted.has(label.toLowerCase()));
}

async function executeTags(
  client: CatalogClient,
  plans: TargetPlan[]
): Promise<Record<string, unknown>> {
  const attach: BaseTagEntityInput[] = [];
  for (const p of plans) {
    const c = p.changes.tags;
    if (c.action !== "add") continue;
    for (const label of c.added) {
      attach.push({ entityType: "TABLE", entityId: p.tableId, label });
    }
  }
  if (attach.length === 0) {
    return { applied: 0, planned: 0, skipped: true };
  }
  let successBatches = 0;
  let failedBatches = 0;
  const failedAttachments: BaseTagEntityInput[] = [];
  const batches = chunk(attach, TAG_ATTACH_BATCH_SIZE);
  for (const batch of batches) {
    const resp = await client.execute<{ attachTags: boolean }>(ATTACH_TAGS, {
      data: batch,
    });
    if (resp.attachTags) successBatches += 1;
    else {
      failedBatches += 1;
      failedAttachments.push(...batch);
    }
  }
  const result: Record<string, unknown> = {
    applied: null,
    appliedReason:
      "ATTACH_TAGS returns a batch-level boolean; per-row outcomes are not observable. Compare batchesAccepted to batchesTotal for batch-level success, and use failedAttachments to re-issue rejected rows.",
    planned: attach.length,
    batchesTotal: batches.length,
    batchesAccepted: successBatches,
    batchesRejected: failedBatches,
  };
  if (failedBatches > 0) {
    result.partialFailure = true;
    result.failedAttachments = failedAttachments;
  }
  return result;
}

// ── Tool factory ────────────────────────────────────────────────────────────

interface PropagateInput extends Record<string, unknown> {
  sourceAssetId: string;
  sourceAssetType: SourceAssetType;
  tagLabels?: string[];
  maxDepth?: number;
  overwritePolicy?: OverwritePolicy;
  dryRun?: boolean;
  acknowledgeProvenanceSemantics?: boolean;
}

async function runPropagation(
  args: PropagateInput,
  client: CatalogClient,
  _extra?: ToolHandlerExtra
): Promise<unknown> {
  const maxDepth = args.maxDepth ?? 1;
  const overwritePolicy: OverwritePolicy = args.overwritePolicy ?? "ifEmpty";
  const dryRun = args.dryRun ?? true;

  const source = await fetchSourceAsset(
    client,
    args.sourceAssetId,
    args.sourceAssetType
  );
  if (!source) {
    return {
      notFound: true,
      sourceAssetId: args.sourceAssetId,
      sourceAssetType: args.sourceAssetType,
    };
  }

  const effectiveTags = effectiveTagFilter(source, args.tagLabels);

  // Traverse upstream — collect every reached upstream table id, with depth
  // and a (BFS-discovered) path from the source.
  const { visitedAtDepth } = await traverseUpstream(
    client,
    source,
    maxDepth,
    UPSTREAM_HARD_CAP
  );

  const upstreamIds = Array.from(visitedAtDepth.keys());
  const enrichment = await enrichTables(client, upstreamIds);
  // Completeness contract: every id we reached via lineage must be enriched.
  const missing = upstreamIds.filter((id) => !enrichment.has(id));
  if (missing.length > 0) {
    const sample = missing.slice(0, 5).join(", ");
    throw new Error(
      `Detail enrichment returned no row for ${missing.length} upstream table(s) ` +
        `reached via lineage (sample: ${sample}). Refusing to emit a partial propagation plan.`
    );
  }

  const targets: UpstreamTarget[] = upstreamIds.map((id) => {
    const row = enrichment.get(id)!;
    const info = visitedAtDepth.get(id)!;
    const pathFromSource = info.pathFromSource.map((pid) => {
      const r = enrichment.get(pid);
      return {
        tableId: pid,
        tableName: (r?.name as string | null) ?? null,
      };
    });
    return {
      id,
      name: (row.name as string | null) ?? null,
      depth: info.depth,
      tagLabels: extractTagLabels(row),
      pathFromSource,
    };
  });
  // Deterministic order — depth ASC, name ASC.
  targets.sort(
    (a, b) =>
      a.depth - b.depth || (a.name ?? "").localeCompare(b.name ?? "")
  );

  const plans: TargetPlan[] = targets.map((target) => ({
    tableId: target.id,
    tableName: target.name,
    depth: target.depth,
    provenance: {
      sourceAssetId: source.id,
      sourceAssetType: source.assetType,
      sourceAssetName: source.name,
      pathFromSource: target.pathFromSource,
    },
    changes: {
      tags: planTags(source, effectiveTags, target, overwritePolicy),
    },
  }));

  // Plan summary.
  let addCount = 0;
  let skipCount = 0;
  let plannedAttachments = 0;
  for (const p of plans) {
    if (p.changes.tags.action === "add") {
      addCount += 1;
      plannedAttachments += p.changes.tags.added.length;
    } else {
      skipCount += 1;
    }
  }

  const baseResponse: Record<string, unknown> = {
    source: {
      id: source.id,
      name: source.name,
      assetType: source.assetType,
      allTags: source.tagLabels,
      effectiveTags,
    },
    config: {
      maxDepth,
      overwritePolicy,
      dryRun,
      acknowledgeProvenanceSemantics:
        args.acknowledgeProvenanceSemantics === true,
    },
    traversal: {
      upstreamTablesReached: upstreamIds.length,
      maxDepthRequested: maxDepth,
    },
    plan: plans,
    summary: {
      tablesInPlan: plans.length,
      tablesWithMutations: addCount,
      tablesSkipped: skipCount,
      plannedAttachments,
    },
  };

  if (dryRun) {
    baseResponse.note =
      "Dry-run mode — no mutations executed. To execute, re-issue with " +
      "dryRun: false AND acknowledgeProvenanceSemantics: true after " +
      "reviewing the plan + provenance trail.";
    return baseResponse;
  }

  const execution = await executeTags(client, plans);
  return { ...baseResponse, execution };
}

function summarisePropagation(args: PropagateInput): string {
  const depth = args.maxDepth ?? 1;
  const policy = args.overwritePolicy ?? "ifEmpty";
  const labelsClause = args.tagLabels?.length
    ? `tags=[${args.tagLabels.join(", ")}]`
    : "all source tags";
  return (
    `Propagate ${labelsClause} UPSTREAM from ${args.sourceAssetType.toLowerCase()} ` +
    `${args.sourceAssetId} (depth=${depth}, overwritePolicy=${policy}). ` +
    `Provenance semantics ACKNOWLEDGED. Irreversible additive; review plan first.`
  );
}

export function definePropagateTagsUpstream(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_propagate_tags_upstream",
    config: {
      title: "Propagate Tags Upstream",
      description:
        "Propagate tag labels UPSTREAM through lineage from a presentation-layer source (typically a dashboard, optionally a gold-layer table) to the warehouse tables that feed it. Closes the seam N-able raised 2026-04-22 (#topic-feedback-catalog): \"Governance highlights critical fields starting from a report or the gold/presentation layer. Using lineage we should be able to propagate upstream tags that highlight these critical elements to be managed at source.\"\n\n" +
        "**Default behaviour is dry-run (`dryRun: true`)**: returns the full plan with per-upstream-table tag decisions and the lineage path each upstream table sits on, and never mutates anything. Set `dryRun: false` AND `acknowledgeProvenanceSemantics: true` to execute. Without the acknowledgment flag the tool refuses — by design.\n\n" +
        "**Provenance semantics — REQUIRED reading before execution**:\n" +
        "  Upstream tag propagation is NOT symmetric to downstream. A 'Critical' tag on a presentation dashboard does NOT mean the upstream warehouse table is critical: that table may feed many consumers, of which the source is one. Tagging the table 'Critical' may misdirect downstream owners, falsely escalate alerts, or shift on-call ownership. The acknowledgment flag forces the agent to review the dry-run plan + provenance trail (lineage path from source to each upstream table) and confirm the propagation is semantically correct for every upstream target before the mutation runs.\n\n" +
        "**Composes**: source-asset detail (table or dashboard) → upstream lineage BFS (depth 1-3) → upstream-table detail batch (current tags) → per-target diff plan with provenance trail → conditional `attachTags` mutations with batch-level partial-failure tracking.\n\n" +
        "**Capacity gate**: refuses with `Upstream traversal reached more than 200 tables` if the BFS reaches more than 200 distinct upstream tables (counted across all depths combined; tighter than `propagate_metadata`'s per-depth caps because upstream propagation is the higher-risk direction). The cap fires at the moment the 201st distinct table is enqueued. Narrow with `tagLabels`, reduce `maxDepth`, or split by `sourceAssetId`.\n\n" +
        "**Overwrite policy**:\n" +
        "  - `ifEmpty` (default) — only attach tags to upstream tables that currently have NO tags. Cannot clobber hand-curated metadata.\n" +
        "  - `overwrite` — additively merge: adds source tags missing from each upstream table; never removes target-native tags.\n" +
        "Neither policy ever removes metadata.\n\n" +
        "**Mutation surface**: `ATTACH_TAGS` returns a batch-level boolean only (no per-row outcomes), so `partialFailure` is reported per batch with the exact rejected `(tableId, label)` pairs in `failedAttachments` for selective retry.\n\n" +
        "Companion to `catalog_propagate_metadata` (downstream direction, all axes). Use this tool when the user asks to mark warehouse sources because of a downstream report's criticality.",
      inputSchema: PropagateTagsUpstreamInputShape,
      annotations: WRITE_ANNOTATIONS,
    },
    handler: withErrorHandling(async (rawArgs, c, extra) => {
      const args = rawArgs as PropagateInput;
      const dryRun = args.dryRun ?? true;
      if (dryRun) {
        return runPropagation(args, c, extra);
      }
      // Non-dry-run path: gate on the explicit provenance acknowledgment flag
      // BEFORE the elicitation dialog so we fail closed without bothering the
      // user with a dialog they shouldn't see.
      if (args.acknowledgeProvenanceSemantics !== true) {
        throw new Error(
          "acknowledgeProvenanceSemantics must be true to execute. Upstream tag " +
            "propagation has different semantics than downstream — review the dry-run " +
            "plan and the lineage path for every upstream target, confirm the tag's " +
            "meaning is correct upstream, then re-issue with both dryRun: false AND " +
            "acknowledgeProvenanceSemantics: true."
        );
      }
      const guarded = withConfirmation<PropagateInput>(
        {
          action: "Propagate tags upstream",
          summarize: summarisePropagation,
        },
        runPropagation
      );
      return guarded(args, c, extra);
    }, client),
  };
}
