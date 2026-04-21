import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  WRITE_ANNOTATIONS,
  type CatalogToolDefinition,
  type ToolHandlerExtra,
} from "../catalog/types.js";
import {
  GET_TABLE_DETAIL,
  GET_TABLES_DETAIL_BATCH,
  GET_LINEAGES,
  UPDATE_TABLES,
  ATTACH_TAGS,
  UPSERT_USER_OWNERS,
  UPSERT_TEAM_OWNERS,
} from "../catalog/operations.js";
import type {
  BaseTagEntityInput,
  EntityTarget,
  GetLineagesOutput,
  GetTablesOutput,
  OwnerEntity,
  OwnerInput,
  Table,
  TeamOwnerEntity,
  TeamOwnerInput,
  UpdateTableInput,
} from "../generated/types.js";
import { batchResult, withErrorHandling } from "../mcp/tool-helpers.js";
import { withConfirmation } from "../mcp/confirmation.js";
import {
  ENRICHMENT_BATCH_SIZE,
  isNonEmptyString,
  extractOwners,
  chunk,
  type Owners,
} from "./shared.js";

type Axis = "description" | "tags" | "owners";
type OverwritePolicy = "ifEmpty" | "overwrite";

// Traversal width caps mirror assess-impact's contract: the tool refuses
// rather than silently truncating on wide hubs. Propagation is more
// consequential than reading impact (mutations), so the same "complete or
// refuse" rule applies even more strongly.
const WIDTH_CAPS: Record<number, number> = {
  2: 2000,
  3: 500,
};

const LINEAGE_PAGE_SIZE = 500;
const LINEAGE_PAGES_PER_NODE_MAX = 20;
const LINEAGE_FANOUT_PARALLELISM = 20;
// Mutation batch sizes. APIs cap each at 500, but the upsert-owners shape
// is "one userId, many targetEntities" so the fanout there is per-user, not
// per-target-table. TAG_ATTACH_BATCH_SIZE stays below the API cap to keep
// individual requests small enough that a retry is cheap.
const UPDATE_TABLES_BATCH_SIZE = 500;
const TAG_ATTACH_BATCH_SIZE = 500;

const PropagateMetadataInputShape = {
  sourceTableId: z
    .string()
    .min(1)
    .describe("Catalog UUID of the source table whose metadata should propagate downstream."),
  axes: z
    .array(z.enum(["description", "tags", "owners"]))
    .optional()
    .describe(
      "Which metadata axes to propagate. Default: ['description'] only. Tags and owners are opt-in per-call — description propagation is low-risk (filling in a blank is additive), while owner propagation is high-trust and should only run when the caller has explicitly validated that the source's owners are the right owners for every reached downstream table."
    ),
  maxDepth: z
    .number()
    .int()
    .min(1)
    .max(3)
    .optional()
    .describe(
      "How many lineage hops downstream to propagate. 1 = immediate children only (always complete; cheapest). 2 = children-of-children (refuses if >2000 distinct nodes). 3 = deep propagation (refuses if >500). Default 1. Dashboards encountered in lineage are listed as `dashboardsSkipped` and never mutated — propagation is table-to-table only."
    ),
  overwritePolicy: z
    .enum(["ifEmpty", "overwrite"])
    .optional()
    .describe(
      "How to handle targets that already carry the axis's metadata:\n" +
        "  - 'ifEmpty' (default) — only write when the target has no value for that axis. Safest; cannot clobber hand-curated metadata. Tags + owners: 'ifEmpty' = skip if the target has ANY tags / ANY owners; merges are opt-in via 'overwrite'.\n" +
        "  - 'overwrite' — write the source's value regardless. For description: replaces the target's externalDescription. For tags + owners: additively merges (adds missing source tags/owners; never removes target-native ones).\n\n" +
        "Neither policy ever removes metadata from the target — propagation is write-add-or-overwrite, not mirror."
    ),
  dryRun: z
    .boolean()
    .optional()
    .describe(
      "When true (default), compute the full diff plan and return without executing any mutations. When false, execute the plan — requires interactive confirmation via MCP elicitation (or COALESCE_CATALOG_SKIP_CONFIRMATIONS=true)."
    ),
};

interface TargetMeta {
  id: string;
  name: string | null;
  depth: number;
  description: string | null;
  externalDescription: string | null;
  tagLabels: string[];
  owners: Owners;
}

interface SourceMeta {
  id: string;
  name: string | null;
  description: string | null;
  tagLabels: string[];
  owners: Owners;
}

interface DescriptionChange {
  action: "add" | "update" | "skip";
  reason: string;
  before: string | null;
  after: string | null;
}

interface TagsChange {
  action: "add" | "skip";
  reason: string;
  added: string[];
  alreadyPresent: string[];
}

interface OwnersChange {
  action: "add" | "skip";
  reason: string;
  addedUsers: Array<{ userId: string; email: string | null; fullName: string | null }>;
  addedTeams: Array<{ teamId: string; name: string | null }>;
  alreadyOwnedBy: {
    userIds: string[];
    teamIds: string[];
  };
}

interface TargetPlan {
  tableId: string;
  tableName: string | null;
  depth: number;
  changes: {
    description?: DescriptionChange;
    tags?: TagsChange;
    owners?: OwnersChange;
  };
}

function extractTagLabels(row: Record<string, unknown>): string[] {
  if (!Array.isArray(row.tagEntities)) return [];
  const out: string[] = [];
  for (const t of row.tagEntities as Array<Record<string, unknown>>) {
    const tag = t.tag as Record<string, unknown> | undefined;
    const label = tag?.label;
    if (typeof label === "string" && label.length > 0) out.push(label);
  }
  return out;
}

function extractSource(row: Record<string, unknown>): SourceMeta {
  // For propagation, `description` is the right read surface — it's the
  // merged value the consumer sees. We write back to `externalDescription`
  // on targets (the only source-style field UPDATE_TABLES exposes), but the
  // "ifEmpty" check on a target reads against `description` so we're
  // deciding against the displayed value, not just the writable field.
  const description = isNonEmptyString(row.description)
    ? (row.description as string)
    : null;
  return {
    id: row.id as string,
    name: (row.name as string | null) ?? null,
    description,
    tagLabels: extractTagLabels(row),
    owners: extractOwners(row),
  };
}

function extractTarget(
  row: Record<string, unknown>,
  depth: number
): TargetMeta {
  return {
    id: row.id as string,
    name: (row.name as string | null) ?? null,
    depth,
    description: isNonEmptyString(row.description)
      ? (row.description as string)
      : null,
    externalDescription: isNonEmptyString(row.externalDescription)
      ? (row.externalDescription as string)
      : null,
    tagLabels: extractTagLabels(row),
    owners: extractOwners(row),
  };
}

async function fetchAllDownstreamEdges(
  client: CatalogClient,
  parentId: string
): Promise<{ tableIds: string[]; dashboardCount: number }> {
  // One unscoped-by-type scan returns both table and dashboard children in
  // the same pages. We keep tableIds (which continue the BFS) and count
  // dashboard children (which are summarised as `dashboardsSkipped` for the
  // caller). Doing this in one scan rather than two halves lineage traffic
  // at every BFS node — meaningful at depth=2 with hundreds of frontier
  // nodes.
  const tableIds: string[] = [];
  let dashboardCount = 0;
  for (let page = 0; page < LINEAGE_PAGES_PER_NODE_MAX; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope: { parentTableId: parentId },
        pagination: { nbPerPage: LINEAGE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getLineages.data;
    for (const e of rows) {
      if (e.childTableId) tableIds.push(e.childTableId);
      else if (e.childDashboardId) dashboardCount += 1;
    }
    if (rows.length < LINEAGE_PAGE_SIZE) return { tableIds, dashboardCount };
  }
  throw new Error(
    `Lineage pagination exceeded ${LINEAGE_PAGES_PER_NODE_MAX} pages for ` +
      `table ${parentId} (>${LINEAGE_PAGES_PER_NODE_MAX * LINEAGE_PAGE_SIZE} downstream edges). ` +
      `Refusing to produce a partial propagation plan.`
  );
}

async function traverseDownstreamTables(
  client: CatalogClient,
  startId: string,
  maxDepth: number
): Promise<{
  visitedDepths: Map<string, number>;
  dashboardsSkipped: number;
}> {
  const visitedDepths = new Map<string, number>();
  visitedDepths.set(startId, 0);
  let frontier: string[] = [startId];
  let dashboardsSkipped = 0;

  for (let depth = 1; depth <= maxDepth; depth++) {
    if (depth >= 2) {
      const cap = WIDTH_CAPS[depth];
      if (frontier.length > cap) {
        throw new Error(
          `Graph too wide for complete propagation at depth ${depth}: ` +
            `${frontier.length} nodes in the depth-${depth - 1} frontier exceeds the ` +
            `${cap}-node cap. Reduce maxDepth, or run depth=1 first to pick a safer sub-tree.`
        );
      }
    }

    const edgesPerNode: Array<{ tableIds: string[]; dashboardCount: number }> =
      [];
    for (let i = 0; i < frontier.length; i += LINEAGE_FANOUT_PARALLELISM) {
      const slice = frontier.slice(i, i + LINEAGE_FANOUT_PARALLELISM);
      const sliceResults = await Promise.all(
        slice.map((node) => fetchAllDownstreamEdges(client, node))
      );
      edgesPerNode.push(...sliceResults);
    }

    const nextFrontier: string[] = [];
    for (const r of edgesPerNode) {
      dashboardsSkipped += r.dashboardCount;
      for (const childId of r.tableIds) {
        if (visitedDepths.has(childId)) continue;
        visitedDepths.set(childId, depth);
        nextFrontier.push(childId);
      }
    }

    if (nextFrontier.length === 0) break;
    frontier = nextFrontier;
  }

  return { visitedDepths, dashboardsSkipped };
}

async function enrichTables(
  client: CatalogClient,
  ids: string[]
): Promise<Map<string, Record<string, unknown>>> {
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

function planDescription(
  source: SourceMeta,
  target: TargetMeta,
  policy: OverwritePolicy
): DescriptionChange | null {
  if (!source.description) {
    // Nothing to propagate — source has no description. We still emit a
    // skip entry so the caller sees why description changes didn't land,
    // but only when description is in the requested axes set.
    return {
      action: "skip",
      reason: "source table has no description to propagate",
      before: target.description,
      after: target.description,
    };
  }
  if (!target.description) {
    return {
      action: "add",
      reason: "target has no description; filling in from source",
      before: null,
      after: source.description,
    };
  }
  if (policy === "ifEmpty") {
    return {
      action: "skip",
      reason: "target already has a description (overwritePolicy=ifEmpty)",
      before: target.description,
      after: target.description,
    };
  }
  if (target.description === source.description) {
    return {
      action: "skip",
      reason: "target description already matches source",
      before: target.description,
      after: target.description,
    };
  }
  return {
    action: "update",
    reason: "overwriting target description from source (overwritePolicy=overwrite)",
    before: target.description,
    after: source.description,
  };
}

function planTags(
  source: SourceMeta,
  target: TargetMeta,
  policy: OverwritePolicy
): TagsChange | null {
  if (source.tagLabels.length === 0) {
    return {
      action: "skip",
      reason: "source table has no tags to propagate",
      added: [],
      alreadyPresent: [],
    };
  }
  // ifEmpty for a list axis = "target has nothing" (no tags at all). Otherwise
  // the tool behaves additively (merge) regardless of policy for tags.
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
  for (const label of source.tagLabels) {
    if (existing.has(label)) alreadyPresent.push(label);
    else toAdd.push(label);
  }
  if (toAdd.length === 0) {
    return {
      action: "skip",
      reason: "every source tag is already attached to the target",
      added: [],
      alreadyPresent,
    };
  }
  return {
    action: "add",
    reason: `adding ${toAdd.length} missing tag(s) from source`,
    added: toAdd,
    alreadyPresent,
  };
}

function planOwners(
  source: SourceMeta,
  target: TargetMeta,
  policy: OverwritePolicy
): OwnersChange | null {
  const sourceUserIds = source.owners.userOwners.map((o) => o.userId);
  const sourceTeamIds = source.owners.teamOwners.map((o) => o.teamId);
  if (sourceUserIds.length === 0 && sourceTeamIds.length === 0) {
    return {
      action: "skip",
      reason: "source table has no owners to propagate",
      addedUsers: [],
      addedTeams: [],
      alreadyOwnedBy: {
        userIds: target.owners.userOwners.map((o) => o.userId),
        teamIds: target.owners.teamOwners.map((o) => o.teamId),
      },
    };
  }
  const targetUserIds = new Set(target.owners.userOwners.map((o) => o.userId));
  const targetTeamIds = new Set(target.owners.teamOwners.map((o) => o.teamId));
  if (
    policy === "ifEmpty" &&
    (targetUserIds.size > 0 || targetTeamIds.size > 0)
  ) {
    return {
      action: "skip",
      reason: "target already has owners (overwritePolicy=ifEmpty)",
      addedUsers: [],
      addedTeams: [],
      alreadyOwnedBy: {
        userIds: [...targetUserIds],
        teamIds: [...targetTeamIds],
      },
    };
  }
  const addedUsers = source.owners.userOwners.filter(
    (o) => !targetUserIds.has(o.userId)
  );
  const addedTeams = source.owners.teamOwners.filter(
    (o) => !targetTeamIds.has(o.teamId)
  );
  if (addedUsers.length === 0 && addedTeams.length === 0) {
    return {
      action: "skip",
      reason: "every source owner is already attached to the target",
      addedUsers: [],
      addedTeams: [],
      alreadyOwnedBy: {
        userIds: [...targetUserIds],
        teamIds: [...targetTeamIds],
      },
    };
  }
  return {
    action: "add",
    reason: `adding ${addedUsers.length} user-owner(s) and ${addedTeams.length} team-owner(s) from source`,
    addedUsers,
    addedTeams,
    alreadyOwnedBy: {
      userIds: [...targetUserIds],
      teamIds: [...targetTeamIds],
    },
  };
}

function computePlan(
  source: SourceMeta,
  targets: TargetMeta[],
  axes: Set<Axis>,
  policy: OverwritePolicy
): TargetPlan[] {
  const plans: TargetPlan[] = [];
  for (const target of targets) {
    const plan: TargetPlan = {
      tableId: target.id,
      tableName: target.name,
      depth: target.depth,
      changes: {},
    };
    if (axes.has("description")) {
      const c = planDescription(source, target, policy);
      if (c) plan.changes.description = c;
    }
    if (axes.has("tags")) {
      const c = planTags(source, target, policy);
      if (c) plan.changes.tags = c;
    }
    if (axes.has("owners")) {
      const c = planOwners(source, target, policy);
      if (c) plan.changes.owners = c;
    }
    plans.push(plan);
  }
  return plans;
}



async function executeDescription(
  client: CatalogClient,
  source: SourceMeta,
  plans: TargetPlan[]
): Promise<Record<string, unknown>> {
  // UPDATE_TABLES writes to externalDescription (the only writable surface);
  // the plan's "after" value is the source.description, sourced from the
  // merged display value. If the target later sets descriptionRaw, our
  // external-description write becomes shadowed — acceptable for the "fill
  // in a gap" use case and surfaced in the tool description.
  if (!source.description) {
    // planDescription already emits `action: "skip"` when source is empty,
    // so `pending` below would be empty. Early-return here replaces the
    // non-null assertion (source.description!) that the map would otherwise
    // need; the assertion becomes a correctness landmine if future edits
    // let planDescription produce an add/update row with no source value.
    return { applied: 0, planned: 0, skipped: true };
  }
  const pending = plans
    .filter(
      (p) =>
        p.changes.description &&
        (p.changes.description.action === "add" ||
          p.changes.description.action === "update")
    )
    .map((p) => ({
      id: p.tableId,
      externalDescription: source.description as string,
    }));
  if (pending.length === 0) {
    return { applied: 0, planned: 0, skipped: true };
  }
  let applied = 0;
  const failures: Array<{ batch: number; expected: number; returned: number }> = [];
  const batches = chunk(pending, UPDATE_TABLES_BATCH_SIZE);
  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    const resp = await client.execute<{ updateTables: Table[] }>(UPDATE_TABLES, {
      data: batch satisfies UpdateTableInput[],
    });
    if (!Array.isArray(resp.updateTables)) {
      // Defending against schema drift: a non-array response would otherwise
      // collapse to `applied = 0` via `?? 0` and silently look like a total
      // mutation failure, when in reality the client can no longer decode
      // the API's response shape. Throw so the caller sees isError:true.
      throw new Error(
        `updateTables returned a non-array payload (${typeof resp.updateTables}); ` +
          `cannot verify how many of batch ${i} (${batch.length} rows) were applied.`
      );
    }
    const returned = resp.updateTables.length;
    applied += returned;
    if (returned < batch.length) {
      failures.push({ batch: i, expected: batch.length, returned });
    }
  }
  const result: Record<string, unknown> = {
    applied,
    planned: pending.length,
  };
  if (failures.length > 0) {
    result.partialFailure = true;
    result.failures = failures;
  }
  return result;
}

async function executeTags(
  client: CatalogClient,
  plans: TargetPlan[]
): Promise<Record<string, unknown>> {
  // Flatten plans into one attach-row per (tableId, label) pair. The API
  // auto-creates tag labels that don't exist yet — safe for propagation.
  const attach: BaseTagEntityInput[] = [];
  for (const p of plans) {
    const c = p.changes.tags;
    if (!c || c.action !== "add") continue;
    for (const label of c.added) {
      attach.push({ entityType: "TABLE", entityId: p.tableId, label });
    }
  }
  if (attach.length === 0) {
    return { applied: 0, planned: 0, skipped: true };
  }
  let successBatches = 0;
  let failedBatches = 0;
  // Record the exact (entityId, label) pairs in any rejected batch so the
  // caller can re-issue selectively instead of re-running the entire plan.
  // Without this, partialFailure: true is indistinguishable from "try
  // everything again" — which risks flapping a half-failed batch.
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
    // ATTACH_TAGS returns a single boolean per batch — no per-row result.
    // We expose `applied: null` (not 0) so a caller comparing `.applied`
    // across execution axes can tell "unknown" apart from "zero applied."
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

async function executeOwners(
  client: CatalogClient,
  plans: TargetPlan[]
): Promise<Record<string, unknown>> {
  // Group by userId / teamId — the API shape is "one owner, many
  // targetEntities." We issue one call per unique owner with all targets
  // that need that owner attached.
  const userTargets = new Map<string, EntityTarget[]>();
  const teamTargets = new Map<string, EntityTarget[]>();
  for (const p of plans) {
    const c = p.changes.owners;
    if (!c || c.action !== "add") continue;
    const target: EntityTarget = { entityType: "TABLE", entityId: p.tableId };
    for (const u of c.addedUsers) {
      const list = userTargets.get(u.userId) ?? [];
      list.push(target);
      userTargets.set(u.userId, list);
    }
    for (const t of c.addedTeams) {
      const list = teamTargets.get(t.teamId) ?? [];
      list.push(target);
      teamTargets.set(t.teamId, list);
    }
  }

  // Per-owner try/catch so a single upsert failure doesn't abort the rest
  // of the batch — half-applied ownership is still useful to the caller as
  // long as we surface which owners succeeded and which threw. Letting the
  // first throw propagate would leave earlier attachments landed with no
  // record of them in the execution response.
  const userResults: Array<Record<string, unknown>> = [];
  const userFailures: Array<{ userId: string; error: string }> = [];
  for (const [userId, targets] of userTargets) {
    try {
      const resp = await client.execute<{ upsertUserOwners: OwnerEntity[] }>(
        UPSERT_USER_OWNERS,
        { data: { userId, targetEntities: targets } satisfies OwnerInput }
      );
      userResults.push({
        userId,
        ...batchResult("upserted", resp.upsertUserOwners, targets.length),
      });
    } catch (err) {
      userFailures.push({
        userId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
  const teamResults: Array<Record<string, unknown>> = [];
  const teamFailures: Array<{ teamId: string; error: string }> = [];
  for (const [teamId, targets] of teamTargets) {
    try {
      const resp = await client.execute<{ upsertTeamOwners: TeamOwnerEntity[] }>(
        UPSERT_TEAM_OWNERS,
        { data: { teamId, targetEntities: targets } satisfies TeamOwnerInput }
      );
      teamResults.push({
        teamId,
        ...batchResult("upserted", resp.upsertTeamOwners, targets.length),
      });
    } catch (err) {
      teamFailures.push({
        teamId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  const plannedUserAttachments = [...userTargets.values()].reduce(
    (a, b) => a + b.length,
    0
  );
  const plannedTeamAttachments = [...teamTargets.values()].reduce(
    (a, b) => a + b.length,
    0
  );
  const appliedUserAttachments = userResults.reduce(
    (a, r) => a + (r.upserted as number),
    0
  );
  const appliedTeamAttachments = teamResults.reduce(
    (a, r) => a + (r.upserted as number),
    0
  );
  const result: Record<string, unknown> = {
    planned: plannedUserAttachments + plannedTeamAttachments,
    applied: appliedUserAttachments + appliedTeamAttachments,
    byUser: userResults,
    byTeam: teamResults,
  };
  const hasFailure =
    userFailures.length > 0 ||
    teamFailures.length > 0 ||
    appliedUserAttachments < plannedUserAttachments ||
    appliedTeamAttachments < plannedTeamAttachments;
  if (hasFailure) {
    result.partialFailure = true;
    if (userFailures.length > 0) result.userFailures = userFailures;
    if (teamFailures.length > 0) result.teamFailures = teamFailures;
  }
  return result;
}

function summarisePlan(
  axes: Set<Axis>,
  plans: TargetPlan[]
): Record<string, unknown> {
  const byAxis: Record<string, { add: number; update: number; skip: number }> = {};
  if (axes.has("description"))
    byAxis.description = { add: 0, update: 0, skip: 0 };
  if (axes.has("tags")) byAxis.tags = { add: 0, update: 0, skip: 0 };
  if (axes.has("owners")) byAxis.owners = { add: 0, update: 0, skip: 0 };

  for (const p of plans) {
    // Guard with the bucket's presence rather than a non-null assertion:
    // plans may carry a change for an axis the caller didn't request
    // (future-proofing — computePlan gates on the axes set today, but a
    // refactor that widened that could silently crash this loop with a
    // non-null assertion).
    if (p.changes.description && byAxis.description) {
      byAxis.description[p.changes.description.action] += 1;
    }
    if (p.changes.tags && byAxis.tags) {
      byAxis.tags[p.changes.tags.action] += 1;
    }
    if (p.changes.owners && byAxis.owners) {
      byAxis.owners[p.changes.owners.action] += 1;
    }
  }

  let tablesWithMutations = 0;
  for (const p of plans) {
    const hasMutation =
      (p.changes.description?.action === "add" || p.changes.description?.action === "update") ||
      p.changes.tags?.action === "add" ||
      p.changes.owners?.action === "add";
    if (hasMutation) tablesWithMutations += 1;
  }
  return {
    tablesInPlan: plans.length,
    actionsByAxis: byAxis,
    plannedTablesWithMutations: tablesWithMutations,
  };
}

interface PropagateInput extends Record<string, unknown> {
  sourceTableId: string;
  axes?: Axis[];
  maxDepth?: number;
  overwritePolicy?: OverwritePolicy;
  dryRun?: boolean;
}

async function runPropagation(
  args: PropagateInput,
  client: CatalogClient,
  _extra?: ToolHandlerExtra
): Promise<unknown> {
  const axes = new Set<Axis>(args.axes ?? ["description"]);
  const maxDepth = args.maxDepth ?? 1;
  const overwritePolicy: OverwritePolicy = args.overwritePolicy ?? "ifEmpty";
  const dryRun = args.dryRun ?? true;

  const detailResp = await client.execute<{
    getTables: { data: Record<string, unknown>[] };
  }>(GET_TABLE_DETAIL, { ids: [args.sourceTableId] });
  const sourceRow = detailResp.getTables.data[0];
  if (!sourceRow) {
    return { notFound: true, sourceTableId: args.sourceTableId };
  }
  const source = extractSource(sourceRow);

  const { visitedDepths, dashboardsSkipped } = await traverseDownstreamTables(
    client,
    args.sourceTableId,
    maxDepth
  );

  const targetIds = [...visitedDepths.keys()].filter(
    (id) => id !== args.sourceTableId
  );
  const enrichment = await enrichTables(client, targetIds);
  // Completeness contract: every id we reached via lineage must be enriched.
  // Otherwise we'd silently skip a target we reached, producing an
  // under-inclusive plan that the user would accept in good faith.
  const missing = targetIds.filter((id) => !enrichment.has(id));
  if (missing.length > 0) {
    const sample = missing.slice(0, 5).join(", ");
    throw new Error(
      `Detail enrichment returned no row for ${missing.length} downstream ` +
        `table(s) reached via lineage (sample: ${sample}). Refusing to emit ` +
        `a partial propagation plan.`
    );
  }
  const targets: TargetMeta[] = targetIds.map((id) =>
    extractTarget(enrichment.get(id)!, visitedDepths.get(id)!)
  );
  // Sort by depth, then name — deterministic output order makes the dry-run
  // plan diff-friendly when the caller re-runs the tool.
  targets.sort(
    (a, b) => a.depth - b.depth || (a.name ?? "").localeCompare(b.name ?? "")
  );

  const plans = computePlan(source, targets, axes, overwritePolicy);

  const baseResponse: Record<string, unknown> = {
    source: {
      id: source.id,
      name: source.name,
      description: source.description,
      tagLabels: source.tagLabels,
      owners: source.owners,
    },
    config: {
      axes: [...axes],
      maxDepth,
      overwritePolicy,
      dryRun,
    },
    traversal: {
      tablesReached: targetIds.length,
      dashboardsSkipped,
      maxDepthRequested: maxDepth,
    },
    plan: plans,
    summary: summarisePlan(axes, plans),
  };

  if (dryRun) {
    return baseResponse;
  }

  const execution: Record<string, unknown> = {};
  if (axes.has("description")) {
    execution.description = await executeDescription(client, source, plans);
  }
  if (axes.has("tags")) {
    execution.tags = await executeTags(client, plans);
  }
  if (axes.has("owners")) {
    execution.owners = await executeOwners(client, plans);
  }
  return { ...baseResponse, execution };
}

function summarisePropagation(args: PropagateInput): string {
  const axes = args.axes ?? ["description"];
  const depth = args.maxDepth ?? 1;
  const policy = args.overwritePolicy ?? "ifEmpty";
  return (
    `Propagate [${axes.join(", ")}] downstream from table ${args.sourceTableId} ` +
    `(depth=${depth}, overwritePolicy=${policy}). Irreversible for added metadata — ` +
    `review the dry-run plan first.`
  );
}

export function definePropagateMetadata(client: CatalogClient): CatalogToolDefinition {
  return {
    name: "catalog_propagate_metadata",
    config: {
      title: "Propagate Metadata Downstream",
      description:
        "Propagate one or more metadata axes (description, tags, owners) from a source table downstream along lineage — one call produces a typed diff plan plus optional execution with partial-failure tracking.\n\n" +
        "**Default behaviour is dry-run (`dryRun: true`)**: returns the full plan with per-table per-axis decisions and never mutates anything. Set `dryRun: false` to execute; that path requests interactive confirmation via MCP elicitation (set COALESCE_CATALOG_SKIP_CONFIRMATIONS=true for vetted non-interactive callers).\n\n" +
        "**Axes are opt-in.** The default is `['description']` because filling in a blank description is low-risk; tags and owners require the caller to have *validated* that the source's owners/tags are the right ones for every downstream table. Passing `['owners']` without checking will attribute consumers to the source owner and misdirect on-call escalations.\n\n" +
        "**Completeness contract (mirrors catalog_assess_impact):** the tool refuses rather than silently truncating when the downstream graph is too wide — 2000 distinct nodes at depth 2, 500 at depth 3. Every pagination loop has a hard ceiling with a loud error if exceeded. Dashboards encountered downstream are counted in `traversal.dashboardsSkipped` and never mutated — propagation is table-to-table only.\n\n" +
        "**Overwrite policy:**\n" +
        "  - `ifEmpty` (default): only write if the target has no value on that axis. Tags/owners: skip if the target has ANY tags/owners.\n" +
        "  - `overwrite`: description is replaced; tags/owners are additively merged (add missing; never remove target-native values).\n" +
        "Neither policy ever removes metadata — propagation is write-or-merge, not mirror.\n\n" +
        "**Writable surface caveats:**\n" +
        "  - description is written to `externalDescription` (the only writable description field on UPDATE_TABLES). If the target later sets `descriptionRaw` in Catalog UI, the display surfaces `descriptionRaw` and the propagated externalDescription is shadowed. Acceptable for gap-filling.\n" +
        "  - tags use ATTACH_TAGS (auto-creates missing labels). The API returns a boolean per batch, not per row — `partialFailure` flags the batch, not the specific row.\n" +
        "  - owners use UPSERT_USER_OWNERS / UPSERT_TEAM_OWNERS per distinct owner, each carrying the full target list.\n\n" +
        "Pair with catalog_audit_data_product_readiness to grade the source before propagating, and catalog_resolve_ownership_gaps to pick the right owner before attaching.",
      inputSchema: PropagateMetadataInputShape,
      annotations: WRITE_ANNOTATIONS,
    },
    handler: withErrorHandling(
      async (rawArgs, c, extra) => {
        const args = rawArgs as PropagateInput;
        const dryRun = args.dryRun ?? true;
        if (dryRun) {
          // Dry-run bypasses the confirmation dialog entirely — no mutations
          // will fire. Callers preview the plan, then re-issue with
          // dryRun:false to commit.
          return runPropagation(args, c, extra);
        }
        // Non-dry-run: route through withConfirmation. The confirmation
        // dialog will fire before any API writes happen.
        const guarded = withConfirmation<PropagateInput>(
          {
            action: "Propagate metadata downstream",
            summarize: summarisePropagation,
          },
          runPropagation
        );
        return guarded(args, c, extra);
      },
      client
    ),
  };
}
