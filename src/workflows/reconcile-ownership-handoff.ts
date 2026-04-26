import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_TERMS_DETAIL_BATCH,
  GET_TEAMS,
  GET_LINEAGES,
} from "../catalog/operations.js";
import type {
  GetDashboardsOutput,
  GetLineagesOutput,
  GetTablesOutput,
  GetTeamsOutput,
  GetTermsOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import {
  ENRICHMENT_BATCH_SIZE,
  countDownstreamEdges,
  extractNeighborOwners,
  fetchOneHopNeighborIds,
  fetchTableQueryAuthors,
  resolveUserByEmail,
  USER_LOOKUP_MAX_PAGES,
  USER_PAGE_SIZE,
  type NeighborOwner,
  type NeighborRef,
  type QueryAuthorSignal,
} from "./shared.js";

// ── Input schema ────────────────────────────────────────────────────────────

// Capacity gate from the research-history.md 2026-04-23 approval. A departing
// owner with >200 assets is an organisational-level transition that needs a
// different workflow (bulk reassignment, not per-asset handoff).
const OWNED_ASSET_HARD_CAP = 200;

const ReconcileOwnershipHandoffInputShape = {
  email: z
    .string()
    .email()
    .describe(
      "Email of the departing Catalog user whose assets need a handoff plan. Matched case-insensitively."
    ),
  queryAuthorLimit: z
    .number()
    .int()
    .min(1)
    .max(50)
    .optional()
    .describe(
      "Max number of top query authors to return per owned table (sorted by recent query count, descending). Default 5. Authors below the cut are dropped; full probe counts still inform `totalQueriesSeen` / `queriesWithoutAuthor`."
    ),
  includeQueryAuthors: z
    .boolean()
    .optional()
    .describe(
      "Whether to fetch query-author evidence per owned table. Default true. Set false in workspaces with no SQL usage data (pure Catalog-imported accounts) to skip the probe."
    ),
  includeLineageNeighbors: z
    .boolean()
    .optional()
    .describe(
      "Whether to fetch 1-hop upstream/downstream lineage neighbors + their owners per owned table. Default true."
    ),
  includeTeamContext: z
    .boolean()
    .optional()
    .describe(
      "Whether to fetch the full team directory (`getTeams`) to tag each candidate with the teams they belong to. Default true. Set false when the workspace has no teams configured or when the caller wants to skip the single `getTeams` page-scan to save a round-trip."
    ),
};

// ── Constants ───────────────────────────────────────────────────────────────

// Asset hydration — owned IDs are heterogeneous (tables/dashboards/terms), so
// we fan each detail-batch query out in parallel using the `ids:` scope filter.
// Chunk client-side to stay under the API's 500-row-per-page ceiling.
const ASSET_HYDRATE_BATCH_SIZE = ENRICHMENT_BATCH_SIZE;

// Downstream-count pagination — same shape as owner-scorecard's lineage
// counter. Completeness over speed: refuse rather than emit a partial count.
const DOWNSTREAM_COUNT_PAGE_SIZE = 500;
const DOWNSTREAM_COUNT_PAGES_MAX = 20;

// Neighbor lookup (1-hop upstream/downstream per table). Matches the
// resolve-ownership-gaps convention — the caller only needs a handful of
// neighbors to see candidate ownership, but we paginate exhaustively.
const NEIGHBOR_PAGE_SIZE = 100;
const NEIGHBOR_PAGES_MAX = 5;

// Query-author probe — the same 200-query DESC window used elsewhere; saturates
// on any real table long before the cap.
const QUERY_PROBE_SIZE = 200;

// Bounded per-asset fanout so a 200-asset handoff doesn't open 200 × 4 = 800
// concurrent HTTP requests. Matches assess-impact / resolve-ownership-gaps.
const EVIDENCE_PARALLELISM = 10;

// Team directory page-scan — one page of 500 covers almost every workspace;
// paginate defensively up to a bounded ceiling so the tool refuses rather
// than silently truncating on enterprise tenants.
const TEAM_PAGE_SIZE = 500;
const TEAM_PAGES_MAX = 20;

// ── Internal row shapes ─────────────────────────────────────────────────────

interface DetailedTable {
  id: string;
  name?: string | null;
  popularity?: number | null;
  createdAt?: string;
  numberOfQueries?: number | null;
  lastQueriedAt?: number | null;
  schemaId?: string | null;
}

interface DetailedDashboard {
  id: string;
  name?: string | null;
  popularity?: number | null;
  createdAt?: string;
}

interface DetailedTerm {
  id: string;
  name?: string | null;
  description?: string | null;
  createdAt?: string;
  linkedTag?: { id?: string | null; label?: string | null } | null;
}

type OwnedAssetKind = "TABLE" | "DASHBOARD" | "TERM";

// ── Asset hydration ─────────────────────────────────────────────────────────

async function hydrateByIds<TRow>(
  client: CatalogClient,
  operation: string,
  allIds: string[],
  extractRows: (data: unknown) => TRow[]
): Promise<TRow[]> {
  if (allIds.length === 0) return [];
  const collected: TRow[] = [];
  for (let i = 0; i < allIds.length; i += ASSET_HYDRATE_BATCH_SIZE) {
    const slice = allIds.slice(i, i + ASSET_HYDRATE_BATCH_SIZE);
    const data = await client.execute<unknown>(operation, {
      scope: { ids: slice },
      pagination: { nbPerPage: slice.length, page: 0 },
    });
    collected.push(...extractRows(data));
  }
  return collected;
}

async function hydrateTables(
  client: CatalogClient,
  ids: string[]
): Promise<DetailedTable[]> {
  return hydrateByIds<DetailedTable>(
    client,
    GET_TABLES_DETAIL_BATCH,
    ids,
    (d) =>
      (d as { getTables: GetTablesOutput }).getTables.data as unknown as DetailedTable[]
  );
}

async function hydrateDashboards(
  client: CatalogClient,
  ids: string[]
): Promise<DetailedDashboard[]> {
  return hydrateByIds<DetailedDashboard>(
    client,
    GET_DASHBOARDS_DETAIL_BATCH,
    ids,
    (d) =>
      (d as { getDashboards: GetDashboardsOutput }).getDashboards
        .data as unknown as DetailedDashboard[]
  );
}

async function hydrateTerms(
  client: CatalogClient,
  ids: string[]
): Promise<DetailedTerm[]> {
  return hydrateByIds<DetailedTerm>(
    client,
    GET_TERMS_DETAIL_BATCH,
    ids,
    (d) =>
      (d as { getTerms: GetTermsOutput }).getTerms.data as unknown as DetailedTerm[]
  );
}

// ── Team directory (candidate team-membership context) ──────────────────────

interface TeamEntry {
  id: string;
  name: string | null;
  memberIds: string[];
}

async function fetchAllTeams(client: CatalogClient): Promise<TeamEntry[]> {
  // Single paginated scan, refuses at TEAM_PAGES_MAX * TEAM_PAGE_SIZE. We only
  // need id + name + memberIds to build the userId->teams[] index.
  const out: TeamEntry[] = [];
  for (let page = 0; page < TEAM_PAGES_MAX; page++) {
    const resp = await client.execute<{ getTeams: GetTeamsOutput[] }>(
      GET_TEAMS,
      { pagination: { nbPerPage: TEAM_PAGE_SIZE, page } }
    );
    for (const t of resp.getTeams) {
      out.push({
        id: t.id,
        name: t.name ?? null,
        memberIds: t.memberIds ?? [],
      });
    }
    if (resp.getTeams.length < TEAM_PAGE_SIZE) return out;
  }
  throw new Error(
    `Team directory exceeded the ${TEAM_PAGES_MAX * TEAM_PAGE_SIZE}-team ` +
      `scan cap. The tenant is larger than the handoff tool's ceiling; rerun ` +
      `with includeTeamContext=false to skip team enrichment.`
  );
}

function buildUserTeamsIndex(
  teams: TeamEntry[]
): Map<string, Array<{ teamId: string; name: string | null }>> {
  const index = new Map<string, Array<{ teamId: string; name: string | null }>>();
  for (const t of teams) {
    for (const uid of t.memberIds) {
      const existing = index.get(uid) ?? [];
      existing.push({ teamId: t.id, name: t.name });
      index.set(uid, existing);
    }
  }
  return index;
}

// ── Evidence shaping ────────────────────────────────────────────────────────

interface NeighborEntry {
  id: string;
  kind: "TABLE" | "DASHBOARD";
  name: string | null;
  owners: NeighborOwner[];
}

interface EvidenceBundle {
  queryAuthors?: {
    totalQueriesSeen: number;
    queriesWithoutAuthor: number;
    probeCap: number;
    authors: QueryAuthorSignal[];
  };
  upstreamNeighbors?: NeighborEntry[];
  downstreamNeighbors?: NeighborEntry[];
}

async function enrichTableNeighbors(
  client: CatalogClient,
  ids: string[]
): Promise<Map<string, Record<string, unknown>>> {
  // Table neighbors only — dashboard ownership isn't a useful signal for
  // table-ownership attribution (mirrors resolve-ownership-gaps convention).
  // Dashboards still appear in the neighbor list with `owners: []`.
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

// ── Per-asset orchestration ─────────────────────────────────────────────────

interface HandoffAssetBase {
  id: string;
  kind: "TABLE" | "DASHBOARD";
  name: string | null;
  popularity: number | null;
  downstreamConsumerCount: number;
  blastRadiusScore: number;
  evidence: EvidenceBundle;
}

interface TableHandoff extends HandoffAssetBase {
  kind: "TABLE";
  numberOfQueries: number | null;
  lastQueriedAt: number | null;
}

interface DashboardHandoff extends HandoffAssetBase {
  kind: "DASHBOARD";
}

type HandoffAsset = TableHandoff | DashboardHandoff;

interface PerAssetOpts {
  includeQueryAuthors: boolean;
  includeLineageNeighbors: boolean;
  queryAuthorLimit: number;
}

async function gatherTableHandoff(
  client: CatalogClient,
  row: DetailedTable,
  opts: PerAssetOpts
): Promise<TableHandoff> {
  const id = row.id;
  const popularity = row.popularity ?? null;

  // Downstream count always — the blast-radius signal is the whole point of
  // ranking. Evidence subcalls are optional (callers may opt out).
  const countP = countDownstreamEdges(client, id, "TABLE", {
    pageSize: DOWNSTREAM_COUNT_PAGE_SIZE,
    maxPages: DOWNSTREAM_COUNT_PAGES_MAX,
  });
  const tasks: Array<Promise<unknown>> = [countP];

  const evidence: EvidenceBundle = {};

  if (opts.includeQueryAuthors) {
    tasks.push(
      fetchTableQueryAuthors(client, id, {
        topN: opts.queryAuthorLimit,
        probeSize: QUERY_PROBE_SIZE,
      }).then((qa) => {
        evidence.queryAuthors = {
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
        const enrichment = await enrichTableNeighbors(client, [
          ...new Set(tableNeighborIds),
        ]);
        // Completeness guard — same contract as assess-impact /
        // resolve-ownership-gaps: a lineage-reached table neighbor that
        // doesn't come back in the enrichment batch is a real failure,
        // not a silently-unowned neighbor.
        const missing = [...new Set(tableNeighborIds)].filter(
          (nid) => !enrichment.has(nid)
        );
        if (missing.length > 0) {
          throw new Error(
            `Neighbor enrichment returned no row for ${missing.length} table(s) ` +
              `reached via lineage from ${id} (sample: ${missing.slice(0, 5).join(", ")}). ` +
              `Cannot produce complete handoff evidence.`
          );
        }
        evidence.upstreamNeighbors = upstream.map((n) => ({
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
        evidence.downstreamNeighbors = downstream.map((n) => ({
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
  const downstreamConsumerCount = await countP;

  return {
    id,
    kind: "TABLE",
    name: row.name ?? null,
    popularity,
    numberOfQueries: row.numberOfQueries ?? null,
    lastQueriedAt: row.lastQueriedAt ?? null,
    downstreamConsumerCount,
    blastRadiusScore: computeBlastRadius({
      popularity,
      downstreamConsumerCount,
      numberOfQueries: row.numberOfQueries ?? null,
    }),
    evidence,
  };
}

// Fetch upstream neighbors of a dashboard via the dashboard-child lineage
// scope. Dashboards can be fed by both parent TABLEs (the common case) and
// parent DASHBOARDs (embedded-dashboard pattern). We surface both kinds so
// the candidate-evidence promise in the tool description is honored — table
// parents contribute owner signal, dashboard parents are surfaced with
// owners:[] for shape-consistency with how table-handoff neighbors treat
// dashboards. Paginates exhaustively with the same refuse-on-ceiling
// contract used by fetchOneHopNeighborIds.
async function fetchDashboardUpstreamNeighbors(
  client: CatalogClient,
  dashboardId: string,
  opts: { pageSize: number; maxPages: number }
): Promise<NeighborRef[]> {
  const seen = new Set<string>();
  const out: NeighborRef[] = [];
  for (let page = 0; page < opts.maxPages; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope: { childDashboardId: dashboardId },
        pagination: { nbPerPage: opts.pageSize, page },
      }
    );
    const rows = resp.getLineages.data;
    for (const e of rows) {
      const parentId = e.parentTableId ?? e.parentDashboardId;
      if (!parentId) continue;
      if (seen.has(parentId)) continue;
      seen.add(parentId);
      const kind: "TABLE" | "DASHBOARD" = e.parentTableId ? "TABLE" : "DASHBOARD";
      out.push({ id: parentId, kind });
    }
    if (rows.length < opts.pageSize) return out;
  }
  throw new Error(
    `Lineage pagination exceeded ${opts.maxPages} pages for upstream of ` +
      `dashboard ${dashboardId} (>${opts.maxPages * opts.pageSize} edges). ` +
      `Refusing to produce partial neighbor evidence; investigate the lineage data for this node.`
  );
}

async function gatherDashboardHandoff(
  client: CatalogClient,
  row: DetailedDashboard,
  opts: PerAssetOpts
): Promise<DashboardHandoff> {
  const id = row.id;
  const popularity = row.popularity ?? null;

  // Dashboards don't author queries (no query-author probe) and don't have
  // downstream-table neighbors to surface owner evidence for. They DO have
  // upstream tables — a revenue dashboard built on top of someone else's
  // table is a real candidate-ownership signal. Downstream count is still
  // meaningful (dashboard→dashboard embeds) and feeds blast-radius.
  const countP = countDownstreamEdges(client, id, "DASHBOARD", {
    pageSize: DOWNSTREAM_COUNT_PAGE_SIZE,
    maxPages: DOWNSTREAM_COUNT_PAGES_MAX,
  });
  const tasks: Array<Promise<unknown>> = [countP];

  const evidence: EvidenceBundle = {};

  if (opts.includeLineageNeighbors) {
    tasks.push(
      (async () => {
        const upstream = await fetchDashboardUpstreamNeighbors(client, id, {
          pageSize: NEIGHBOR_PAGE_SIZE,
          maxPages: NEIGHBOR_PAGES_MAX,
        });
        const upstreamTableIds = upstream
          .filter((n) => n.kind === "TABLE")
          .map((n) => n.id);
        const enrichment = await enrichTableNeighbors(client, [
          ...new Set(upstreamTableIds),
        ]);
        // Same completeness guard as table handoffs — a lineage-reached
        // table parent that doesn't come back from enrichment is a
        // failure, not a silently-unowned neighbor. Dashboard parents
        // carry owners:[] intentionally (mirrors the table-handoff
        // treatment of dashboard neighbors).
        const missing = [...new Set(upstreamTableIds)].filter(
          (nid) => !enrichment.has(nid)
        );
        if (missing.length > 0) {
          throw new Error(
            `Neighbor enrichment returned no row for ${missing.length} table(s) ` +
              `reached via lineage upstream of dashboard ${id} ` +
              `(sample: ${missing.slice(0, 5).join(", ")}). ` +
              `Cannot produce complete handoff evidence.`
          );
        }
        evidence.upstreamNeighbors = upstream.map((n) => ({
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
        // Dashboards don't have downstream TABLE neighbors to surface
        // owner evidence for (downstream is always other dashboards), so
        // we leave the list empty for shape-consistency with tables.
        evidence.downstreamNeighbors = [];
      })()
    );
  }

  await Promise.all(tasks);
  const downstreamConsumerCount = await countP;

  return {
    id,
    kind: "DASHBOARD",
    name: row.name ?? null,
    popularity,
    downstreamConsumerCount,
    blastRadiusScore: computeBlastRadius({
      popularity,
      downstreamConsumerCount,
      numberOfQueries: null,
    }),
    evidence,
  };
}

// ── Blast-radius scoring ────────────────────────────────────────────────────

// Deterministic, transparent: log-scaled downstream impact + log-scaled query
// volume (tables only). The score is a *ranking* signal, not an opaque
// confidence — callers see `downstreamConsumerCount` and `popularity`
// alongside `blastRadiusScore` and can apply their own ordering if desired.
function computeBlastRadius(opts: {
  popularity: number | null;
  downstreamConsumerCount: number;
  numberOfQueries: number | null;
}): number {
  // downstream_impact: 0-60pts, log-scaled to match assess-impact
  const downstreamPts = Math.min(
    60,
    Math.log10(opts.downstreamConsumerCount + 1) * 30
  );
  // popularity: 0-20pts, API popularity is already a 0-1 float
  const popularityPts =
    opts.popularity != null ? Math.round(Math.min(20, opts.popularity * 20)) : 0;
  // query_volume: 0-20pts, log-scaled (tables only; dashboards have no volume)
  const queryVolumePts =
    opts.numberOfQueries != null
      ? Math.min(20, Math.log10(opts.numberOfQueries + 1) * 5)
      : 0;
  return Math.round(downstreamPts + popularityPts + queryVolumePts);
}

// ── Candidate aggregation ───────────────────────────────────────────────────

interface CandidateUser {
  candidateType: "user";
  userId: string;
  email: string | null;
  name: string | null;
  assetCount: number;
  totalBlastRadius: number;
  assetIds: string[];
  evidenceTypes: {
    queryAuthor: number;
    upstreamNeighbor: number;
    downstreamNeighbor: number;
  };
  teams: Array<{ teamId: string; name: string | null }>;
}

interface CandidateTeam {
  candidateType: "team";
  teamId: string;
  name: string | null;
  assetCount: number;
  totalBlastRadius: number;
  assetIds: string[];
  evidenceTypes: {
    queryAuthor: 0;
    upstreamNeighbor: number;
    downstreamNeighbor: number;
  };
}

type Candidate = CandidateUser | CandidateTeam;

function aggregateCandidates(
  assets: HandoffAsset[],
  departingUserId: string,
  departingEmail: string,
  userTeamsIndex: Map<string, Array<{ teamId: string; name: string | null }>> | null
): Candidate[] {
  // keyed by userId for users, by teamId for teams
  const userMap = new Map<string, CandidateUser>();
  const teamMap = new Map<string, CandidateTeam>();

  const ensureUser = (
    userId: string,
    email: string | null,
    name: string | null
  ): CandidateUser => {
    const existing = userMap.get(userId);
    if (existing) return existing;
    const teams = userTeamsIndex?.get(userId) ?? [];
    const fresh: CandidateUser = {
      candidateType: "user",
      userId,
      email,
      name,
      assetCount: 0,
      totalBlastRadius: 0,
      assetIds: [],
      evidenceTypes: { queryAuthor: 0, upstreamNeighbor: 0, downstreamNeighbor: 0 },
      teams,
    };
    userMap.set(userId, fresh);
    return fresh;
  };

  const departingEmailLower = departingEmail.toLowerCase();

  const ensureTeam = (teamId: string, name: string | null): CandidateTeam => {
    const existing = teamMap.get(teamId);
    if (existing) return existing;
    const fresh: CandidateTeam = {
      candidateType: "team",
      teamId,
      name,
      assetCount: 0,
      totalBlastRadius: 0,
      assetIds: [],
      evidenceTypes: { queryAuthor: 0, upstreamNeighbor: 0, downstreamNeighbor: 0 },
    };
    teamMap.set(teamId, fresh);
    return fresh;
  };

  for (const asset of assets) {
    // Track which candidate keys we've touched for this asset so we don't
    // double-count the same candidate (e.g. a user who is both a query
    // author and an upstream neighbor owner). `assetCount` and
    // `totalBlastRadius` accrue at most once per (candidate, asset) pair.
    const touchedUsers = new Set<string>();
    const touchedTeams = new Set<string>();

    // Query authors — only meaningful for TABLE assets; maps author email
    // to a CandidateUser WITH NO userId resolution (the API returns author
    // as an email string, not a user record).
    // We attribute by email so authors who don't have a Catalog user get
    // surfaced too. The departing owner is almost always a top query
    // author on their own tables, so skip their email match — they're the
    // ones leaving.
    if (asset.evidence.queryAuthors) {
      for (const a of asset.evidence.queryAuthors.authors) {
        const authorLower = a.author.toLowerCase();
        if (authorLower === departingEmailLower) continue;
        // Query-author "userId" synthetic key: email — distinct from real
        // Catalog userIds so the aggregation doesn't collide with real
        // userId-based candidates.
        const syntheticKey = `email:${authorLower}`;
        if (touchedUsers.has(syntheticKey)) continue;
        const cand = ensureUser(syntheticKey, a.author, null);
        cand.evidenceTypes.queryAuthor += 1;
        touchedUsers.add(syntheticKey);
      }
    }

    // Neighbor owners — map each owner entity to a candidate, tagged with
    // direction (upstream vs downstream). Skip the departing owner's own
    // userId if it shows up (it shouldn't, since we're looking at
    // neighbors of their owned assets, but guards against corrupt data).
    const accrueNeighbors = (
      list: NeighborEntry[] | undefined,
      direction: "upstreamNeighbor" | "downstreamNeighbor"
    ) => {
      if (!list) return;
      for (const n of list) {
        for (const owner of n.owners) {
          if (owner.type === "user") {
            if (!owner.userId) continue;
            if (owner.userId === departingUserId) continue;
            if (touchedUsers.has(owner.userId)) continue;
            const cand = ensureUser(
              owner.userId,
              owner.email ?? null,
              owner.name ?? null
            );
            cand.evidenceTypes[direction] += 1;
            touchedUsers.add(owner.userId);
          } else {
            if (!owner.teamId) continue;
            if (touchedTeams.has(owner.teamId)) continue;
            const cand = ensureTeam(owner.teamId, owner.name);
            cand.evidenceTypes[direction] += 1;
            touchedTeams.add(owner.teamId);
          }
        }
      }
    };
    accrueNeighbors(asset.evidence.upstreamNeighbors, "upstreamNeighbor");
    accrueNeighbors(asset.evidence.downstreamNeighbors, "downstreamNeighbor");

    for (const key of touchedUsers) {
      const cand = userMap.get(key)!;
      cand.assetCount += 1;
      cand.totalBlastRadius += asset.blastRadiusScore;
      cand.assetIds.push(asset.id);
    }
    for (const key of touchedTeams) {
      const cand = teamMap.get(key)!;
      cand.assetCount += 1;
      cand.totalBlastRadius += asset.blastRadiusScore;
      cand.assetIds.push(asset.id);
    }
  }

  const candidates: Candidate[] = [
    ...userMap.values(),
    ...teamMap.values(),
  ];
  // Sort by asset coverage first (broader reach wins), then blast radius
  // (bigger stakes wins), then stable by key.
  candidates.sort((a, b) => {
    if (b.assetCount !== a.assetCount) return b.assetCount - a.assetCount;
    if (b.totalBlastRadius !== a.totalBlastRadius)
      return b.totalBlastRadius - a.totalBlastRadius;
    const aKey = a.candidateType === "user" ? a.userId : a.teamId;
    const bKey = b.candidateType === "user" ? b.userId : b.teamId;
    return aKey.localeCompare(bKey);
  });
  return candidates;
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineReconcileOwnershipHandoff(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_reconcile_ownership_handoff",
    config: {
      title: "Reconcile Ownership Handoff (Departing-Owner Plan)",
      description:
        "Build a prioritised handoff plan for a departing Catalog owner. Given an email, enumerates every table/dashboard/term they own, scores each owned asset by blast radius (popularity × downstream consumer count × recent query volume), gathers candidate-owner evidence per asset (top query authors, 1-hop upstream/downstream neighbor owners, team membership context), and aggregates candidates across the portfolio into a ranked summary.\n\n" +
        `**Completeness contract.** The tool refuses if the departing owner holds more than ${OWNED_ASSET_HARD_CAP} unique owned assets — at that scale this is an org-level transition (bulk reassignment) not a per-asset handoff, and the fanout alone would open ${OWNED_ASSET_HARD_CAP * 5}+ concurrent calls. Narrow the handoff (e.g. by domain) and split into multiple runs. Every pagination loop has a hard ceiling with a loud error if exceeded — no silent truncation.\n\n` +
        "**Why this seam exists.** `catalog_assess_impact` is single-asset (no batch mode), and `catalog_resolve_ownership_gaps` semantically rejects owned tables (`!hasOwner()` filter). Composing them manually for a departing owner with 30+ assets is 60-80+ sequential calls with complex inter-call state. This tool does it in one call, bounded-concurrent.\n\n" +
        "**Output shape:** `{ identity, asOf, params, scanned: {tablesCount, dashboardsCount, termsCount, unclassified_owned_ids}, handoffQueue: [{ id, kind, name, popularity, downstreamConsumerCount, blastRadiusScore, evidence: { queryAuthors, upstreamNeighbors, downstreamNeighbors }, ... }], terms: [...], candidateSummary: [{ candidateType, userId|teamId, email, name, assetCount, totalBlastRadius, evidenceTypes, teams }] }`. `handoffQueue` is sorted by `blastRadiusScore` DESC (most important first). `candidateSummary` is sorted by `assetCount` DESC (broadest candidate first) then `totalBlastRadius` DESC. Terms are listed but not scored — they have no downstream consumer fan-out.\n\n" +
        "**Term handoff caveat.** Terms appear in `terms[]` for the departing owner but contribute NO candidate evidence to `candidateSummary` — glossary stewardship is not derivable from lineage or query history, so a glossary steward must be picked manually. The asset counts in `candidateSummary` reflect table + dashboard coverage only, never term coverage.\n\n" +
        "**Dashboard evidence caveat.** Dashboards get upstream-neighbor owner evidence (the tables the dashboard reads from) but no query-author signal — dashboards don't author queries. Parent dashboards (from embedded-dashboard patterns) are surfaced in `upstreamNeighbors` with `owners: []` for shape-consistency, mirroring how table-handoff neighbors treat dashboards. Candidate coverage for dashboard-heavy portfolios therefore leans on upstream-table owners.\n\n" +
        "**Self-exclusion.** The departing owner is filtered from candidate aggregation both by userId (neighbor owners) and by email (query authors), so they never appear as a candidate to take over their own assets.\n\n" +
        "**Differs from `catalog_owner_scorecard`** (grades the hygiene of what a user currently owns — thin descriptions, PII tags, lineage gaps) and **from `catalog_resolve_ownership_gaps`** (finds currently-unowned tables and suggests owners). This tool addresses the TRANSITION moment: assets ARE owned but the owner is departing — who should take each one, prioritized by blast radius?\n\n" +
        "Use for: leaver workflows, role-transition handoffs, and domain-migration reviews where one owner is offboarding a portfolio.",
      inputSchema: ReconcileOwnershipHandoffInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const email = args.email as string;
      const queryAuthorLimit = (args.queryAuthorLimit as number | undefined) ?? 5;
      const includeQueryAuthors =
        (args.includeQueryAuthors as boolean | undefined) ?? true;
      const includeLineageNeighbors =
        (args.includeLineageNeighbors as boolean | undefined) ?? true;
      const includeTeamContext =
        (args.includeTeamContext as boolean | undefined) ?? true;

      const asOfMs = Date.now();

      // 1. Resolve user by email
      const resolution = await resolveUserByEmail(c, email);
      if (resolution.kind === "ceiling") {
        throw new Error(
          `User lookup did not reach the end of the user directory: scanned ` +
            `${resolution.usersScanned} users without finding '${email}'. The tenant is ` +
            `larger than the ${USER_PAGE_SIZE * USER_LOOKUP_MAX_PAGES}-user scan ceiling; the ` +
            `public API has no user-by-email endpoint so this tool cannot distinguish ` +
            `"absent" from "beyond the ceiling" in this case. Retrying will not help.`
        );
      }
      if (resolution.kind === "absent") {
        return {
          notFound: true,
          email,
          reason:
            `User confirmed absent: scanned the entire user directory (short page returned) ` +
            `without finding '${email}'.`,
        };
      }
      const owner = resolution.owner;

      // 2. Enforce capacity gate on unique owned IDs
      const allIds = Array.from(new Set(owner.ownedAssetIds));
      if (allIds.length > OWNED_ASSET_HARD_CAP) {
        throw new Error(
          `Owner '${email}' holds ${allIds.length} unique owned assets, exceeding ` +
            `the ${OWNED_ASSET_HARD_CAP}-asset handoff cap. At that scale the handoff is ` +
            `an org-level transition (bulk reassignment) — fanning out per-asset evidence ` +
            `would open ${allIds.length * 5}+ concurrent calls and saturate the API. ` +
            `Narrow the handoff (e.g. by domain tag or schema) and run in batches.`
        );
      }

      // 3. Hydrate owned assets (tables, dashboards, terms in parallel)
      //    plus optionally fetch the team directory for candidate team
      //    membership context. One getTeams page-scan gives every future
      //    candidate their team roster.
      const [tables, dashboards, terms, teams] = await Promise.all([
        hydrateTables(c, allIds),
        hydrateDashboards(c, allIds),
        hydrateTerms(c, allIds),
        includeTeamContext ? fetchAllTeams(c) : Promise.resolve(null),
      ]);

      const userTeamsIndex =
        teams == null ? null : buildUserTeamsIndex(teams);

      // 4. Per-asset evidence gathering, bounded-concurrent. Tables and
      //    dashboards are orchestrated separately — tables get the full
      //    evidence bundle (query authors + neighbor owners), dashboards
      //    get downstream count + blast-radius only.
      const perAssetOpts: PerAssetOpts = {
        includeQueryAuthors,
        includeLineageNeighbors,
        queryAuthorLimit,
      };

      const tableWork = tables.map((t) => async () =>
        gatherTableHandoff(c, t, perAssetOpts)
      );
      const dashboardWork = dashboards.map((d) => async () =>
        gatherDashboardHandoff(c, d, perAssetOpts)
      );

      const tableHandoffs: TableHandoff[] = [];
      for (let i = 0; i < tableWork.length; i += EVIDENCE_PARALLELISM) {
        const slice = tableWork.slice(i, i + EVIDENCE_PARALLELISM);
        tableHandoffs.push(...(await Promise.all(slice.map((fn) => fn()))));
      }
      const dashboardHandoffs: DashboardHandoff[] = [];
      for (let i = 0; i < dashboardWork.length; i += EVIDENCE_PARALLELISM) {
        const slice = dashboardWork.slice(i, i + EVIDENCE_PARALLELISM);
        dashboardHandoffs.push(...(await Promise.all(slice.map((fn) => fn()))));
      }

      // 5. Merge + sort handoff queue by blast radius DESC (ties broken by
      //    popularity DESC then name).
      const handoffQueue: HandoffAsset[] = [
        ...tableHandoffs,
        ...dashboardHandoffs,
      ];
      handoffQueue.sort(
        (a, b) =>
          b.blastRadiusScore - a.blastRadiusScore ||
          (b.popularity ?? -1) - (a.popularity ?? -1) ||
          (a.name ?? "").localeCompare(b.name ?? "")
      );

      // 6. Candidate aggregation across all assets
      const candidateSummary = aggregateCandidates(
        handoffQueue,
        owner.userId,
        owner.email,
        userTeamsIndex
      );

      // 7. Terms — surface the list but don't score. Terms don't have
      //    downstream-asset fan-out in the same sense tables/dashboards
      //    do, and the handoff decision is usually "pick a glossary
      //    steward" not "compute blast radius."
      const termSummaries = terms.map((t) => ({
        id: t.id,
        name: t.name ?? null,
        description: t.description ?? null,
        linkedTag: t.linkedTag
          ? { id: t.linkedTag.id ?? null, label: t.linkedTag.label ?? null }
          : null,
      }));

      // Any owned ID that didn't resolve as table/dashboard/term —
      // typically column/query UUIDs or references to deleted assets.
      const classified = new Set<string>();
      for (const t of tables) classified.add(t.id);
      for (const d of dashboards) classified.add(d.id);
      for (const t of terms) classified.add(t.id);
      const unclassifiedOwnedIds = allIds.filter(
        (id) => !classified.has(id)
      );

      return {
        identity: {
          userId: owner.userId,
          email: owner.email,
          firstName: owner.firstName,
          lastName: owner.lastName,
          ownedAssetCount: owner.ownedAssetIds.length,
          ownedAssetUniqueCount: allIds.length,
        },
        asOf: new Date(asOfMs).toISOString(),
        params: {
          queryAuthorLimit,
          includeQueryAuthors,
          includeLineageNeighbors,
          includeTeamContext,
          queryProbeCap: QUERY_PROBE_SIZE,
          neighborPageSize: NEIGHBOR_PAGE_SIZE,
        },
        scanned: {
          tablesCount: tables.length,
          dashboardsCount: dashboards.length,
          termsCount: terms.length,
          unclassified_owned_ids: unclassifiedOwnedIds,
        },
        handoffQueue,
        terms: termSummaries,
        candidateSummary,
      };
    }, client),
  };
}
