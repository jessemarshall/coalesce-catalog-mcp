import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_USERS,
  GET_TABLES_DETAIL_BATCH,
  GET_DASHBOARDS_DETAIL_BATCH,
  GET_TERMS_DETAIL_BATCH,
  GET_LINEAGES,
  GET_PINNED_ASSETS,
} from "../catalog/operations.js";
import type {
  EntitiesLink,
  GetDashboardsOutput,
  GetEntitiesLinkOutput,
  GetLineagesOutput,
  GetTablesOutput,
  GetTermsOutput,
  GetUsersOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

// ── Input schema ────────────────────────────────────────────────────────────

const OwnerScorecardInputShape = {
  email: z
    .string()
    .email()
    .describe(
      "Email of the Catalog user whose owned assets to grade. Matched case-insensitively."
    ),
  domainTagPrefix: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Label prefix (case-insensitive) identifying a \"domain\" tag — by convention `domain:sales`, `domain:marketing`, etc. Default: \"domain:\". An asset is flagged `no_domain_tag` if none of its attached tag labels begin with this prefix."
    ),
  newAssetDays: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      "Window, in days, that counts an asset as \"new\". Measured from `createdAt` to `asOf`. Default: 7."
    ),
  piiTagPattern: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Regex (case-insensitive) over attached tag labels that flags an asset as PII/regulated. Default: \"pii|phi|pci\"."
    ),
};

// ── Constants ───────────────────────────────────────────────────────────────

// User page-scan ceiling — mirrors the governance.ts findUserById cap so the
// two lookups fail at the same boundary. Page size is 500 (API max).
const USER_LOOKUP_PAGE_SIZE = 500;
const USER_LOOKUP_MAX_PAGES = 20;

// Asset hydration: owned IDs are heterogeneous (tables/dashboards/terms), so
// we fan each search out in parallel using the `ids:` scope filter. 500 is
// the per-call batch ceiling (matches ENRICHMENT_BATCH_SIZE in
// assess-impact.ts) — owners with >500 owned assets of one type would have
// silently-truncated responses if we passed the full list in one call, so
// we chunk client-side and merge.
const ASSET_HYDRATE_BATCH_SIZE = 500;

// Lineage fan-out — one call per (direction × owned table). Same shape as
// assess-impact: slice the fan-out into parallel batches of N, bounded pages
// per node. Completeness over speed: throw if a single node exceeds the
// per-node page ceiling rather than emit a partial edge count.
const LINEAGE_PAGE_SIZE = 500;
const LINEAGE_PAGES_PER_NODE_MAX = 20;
const LINEAGE_FANOUT_PARALLELISM = 20;

// Pinned-asset outbound check (for term "orphaned"). One batched call with
// fromTermIds handles up to PINNED_PAGE_SIZE edges per page; paginate for
// heavy pinners.
const PINNED_PAGE_SIZE = 500;
const PINNED_PAGES_MAX = 20;

// Category boundary constants.
const THIN_DESCRIPTION_CHARS = 20;
const DEFAULT_DOMAIN_TAG_PREFIX = "domain:";
const DEFAULT_NEW_ASSET_DAYS = 7;
const DEFAULT_PII_TAG_PATTERN = "pii|phi|pci";

const DAY_MS = 24 * 60 * 60 * 1000;

// ── Internal row shapes ─────────────────────────────────────────────────────

interface DetailedTable {
  id: string;
  description?: string | null;
  createdAt?: string;
  isVerified?: boolean | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
  tagEntities?: Array<{ tag?: { label?: string | null } | null }>;
}

interface DetailedDashboard extends DetailedTable {}

interface DetailedTerm {
  id: string;
  description?: string | null;
  createdAt?: string;
  isVerified?: boolean | null;
  linkedTag?: { id: string; label: string } | null;
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
  tagEntities?: Array<{ tag?: { label?: string | null } | null }>;
}

interface LineageCounts {
  upstream: number;
  downstream: number;
}

// ── User lookup (email → userId + owned IDs) ────────────────────────────────

interface OwnerIdentity {
  userId: string;
  email: string;
  firstName: string;
  lastName: string;
  ownedAssetIds: string[];
}

// Distinguish "user confirmed absent" (short page reached without a match)
// from "scan ceiling hit" (20 full pages without a match). The first is a
// terminal notFound for the caller; the second means the scan is incomplete
// and the tool must refuse rather than silently claim the user doesn't exist.
type OwnerResolution =
  | { kind: "found"; owner: OwnerIdentity }
  | { kind: "absent" }
  | { kind: "ceiling"; usersScanned: number };

async function resolveOwnerByEmail(
  client: CatalogClient,
  email: string
): Promise<OwnerResolution> {
  const target = email.toLowerCase();
  for (let page = 0; page < USER_LOOKUP_MAX_PAGES; page++) {
    const resp = await client.execute<{ getUsers: GetUsersOutput[] }>(
      GET_USERS,
      { pagination: { nbPerPage: USER_LOOKUP_PAGE_SIZE, page } }
    );
    const match = resp.getUsers.find(
      (u) => u.email.toLowerCase() === target
    );
    if (match) {
      return {
        kind: "found",
        owner: {
          userId: match.id,
          email: match.email,
          firstName: match.firstName,
          lastName: match.lastName,
          ownedAssetIds: match.ownedAssetIds ?? [],
        },
      };
    }
    if (resp.getUsers.length < USER_LOOKUP_PAGE_SIZE) return { kind: "absent" };
  }
  return {
    kind: "ceiling",
    usersScanned: USER_LOOKUP_PAGE_SIZE * USER_LOOKUP_MAX_PAGES,
  };
}

// ── Asset hydration ─────────────────────────────────────────────────────────

async function hydrateByIds<TRow>(
  client: CatalogClient,
  operation: string,
  allIds: string[],
  extractRows: (data: unknown) => TRow[]
): Promise<TRow[]> {
  // Chunk the heterogeneous owned-ID list into batches of
  // ASSET_HYDRATE_BATCH_SIZE. Sending a 600-ID batch would be silently
  // truncated by the server (no pagination on a single ids scope). Asking
  // for exactly `slice.length` per page → one response per batch; if the
  // server returned fewer rows than requested that's a legitimate "some IDs
  // didn't match this type" outcome, not a truncation signal.
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
    (d) => (d as { getTables: GetTablesOutput }).getTables.data as unknown as DetailedTable[]
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
    (d) => (d as { getTerms: GetTermsOutput }).getTerms.data as unknown as DetailedTerm[]
  );
}

// ── Lineage fan-out (tables only) ───────────────────────────────────────────

async function countLineageEdges(
  client: CatalogClient,
  tableId: string,
  direction: "upstream" | "downstream"
): Promise<number> {
  let count = 0;
  for (let page = 0; page < LINEAGE_PAGES_PER_NODE_MAX; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope:
          direction === "upstream"
            ? { childTableId: tableId }
            : { parentTableId: tableId },
        pagination: { nbPerPage: LINEAGE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getLineages.data;
    count += rows.length;
    if (rows.length < LINEAGE_PAGE_SIZE) return count;
  }
  throw new Error(
    `Lineage pagination exceeded ${LINEAGE_PAGES_PER_NODE_MAX} pages ` +
      `(${direction}) for table ${tableId}. Refusing to emit a partial scorecard.`
  );
}

async function lineageCountsForTables(
  client: CatalogClient,
  tableIds: string[]
): Promise<Map<string, LineageCounts>> {
  const out = new Map<string, LineageCounts>();
  // Fan out both directions per table as one task; bound concurrency by the
  // parallelism knob so a 500-table owner opens at most N sockets.
  const tasks = tableIds.map((id) => async () => {
    const [upstream, downstream] = await Promise.all([
      countLineageEdges(client, id, "upstream"),
      countLineageEdges(client, id, "downstream"),
    ]);
    out.set(id, { upstream, downstream });
  });
  for (let i = 0; i < tasks.length; i += LINEAGE_FANOUT_PARALLELISM) {
    const slice = tasks.slice(i, i + LINEAGE_FANOUT_PARALLELISM);
    await Promise.all(slice.map((t) => t()));
  }
  return out;
}

// ── Pinned-asset outbound check (terms only) ────────────────────────────────

async function termIdsWithOutboundPinnedAssets(
  client: CatalogClient,
  termIds: string[]
): Promise<Set<string>> {
  // Full orphaned-check leg: for every owned term, does it pin anything? One
  // batched call with fromTermIds catches every edge; paginate until exhausted
  // or refuse if the page ceiling is hit.
  const seen = new Set<string>();
  if (termIds.length === 0) return seen;
  for (let page = 0; page < PINNED_PAGES_MAX; page++) {
    const resp = await client.execute<{
      getPinnedAssets: GetEntitiesLinkOutput;
    }>(GET_PINNED_ASSETS, {
      scope: { fromTermIds: termIds },
      pagination: { nbPerPage: PINNED_PAGE_SIZE, page },
    });
    const rows = resp.getPinnedAssets.data as EntitiesLink[];
    for (const row of rows) {
      if (row.fromTermId) seen.add(row.fromTermId);
    }
    if (rows.length < PINNED_PAGE_SIZE) return seen;
  }
  throw new Error(
    `Pinned-asset pagination exceeded ${PINNED_PAGES_MAX} pages for ` +
      `${termIds.length} owned terms. Refusing to emit a partial scorecard.`
  );
}

// ── Category predicates ─────────────────────────────────────────────────────

function isThinDescription(desc: string | null | undefined): boolean {
  if (typeof desc !== "string") return true;
  return desc.trim().length < THIN_DESCRIPTION_CHARS;
}

function hasTagMatching(
  row: { tagEntities?: Array<{ tag?: { label?: string | null } | null }> },
  pattern: RegExp
): boolean {
  if (!Array.isArray(row.tagEntities)) return false;
  return row.tagEntities.some((te) => {
    const label = te.tag?.label;
    return typeof label === "string" && pattern.test(label);
  });
}

function hasDomainTag(
  row: { tagEntities?: Array<{ tag?: { label?: string | null } | null }> },
  prefixLower: string
): boolean {
  if (!Array.isArray(row.tagEntities)) return false;
  return row.tagEntities.some((te) => {
    const label = te.tag?.label;
    return typeof label === "string" && label.toLowerCase().startsWith(prefixLower);
  });
}

function hasAssignedOwner(row: {
  ownerEntities?: Array<Record<string, unknown>>;
  teamOwnerEntities?: Array<Record<string, unknown>>;
}): boolean {
  const userOwners = Array.isArray(row.ownerEntities)
    ? row.ownerEntities.filter((o) => o.userId != null).length
    : 0;
  const teamOwners = Array.isArray(row.teamOwnerEntities)
    ? row.teamOwnerEntities.filter((t) => t.teamId != null).length
    : 0;
  return userOwners + teamOwners > 0;
}

function isWithinDays(
  createdAt: string | undefined,
  asOfMs: number,
  windowDays: number
): boolean {
  if (!createdAt) return false;
  const created = Date.parse(createdAt);
  if (Number.isNaN(created)) return false;
  return asOfMs - created <= windowDays * DAY_MS;
}

// Sort IDs newest-first by createdAt DESC, preserving the original order for
// rows without a parseable createdAt (puts them at the end).
function sortIdsByCreatedDesc<
  T extends { id: string; createdAt?: string | null | undefined }
>(rows: T[], pickIds: (r: T) => boolean): string[] {
  return rows
    .filter(pickIds)
    .slice()
    .sort((a, b) => {
      const at = a.createdAt ? Date.parse(a.createdAt) : NaN;
      const bt = b.createdAt ? Date.parse(b.createdAt) : NaN;
      const aValid = !Number.isNaN(at);
      const bValid = !Number.isNaN(bt);
      if (aValid && bValid) return bt - at;
      if (aValid) return -1;
      if (bValid) return 1;
      return 0;
    })
    .map((r) => r.id);
}

// ── Categorization ──────────────────────────────────────────────────────────

interface CategoryOpts {
  domainTagPrefixLower: string;
  newAssetDays: number;
  piiPattern: RegExp;
  asOfMs: number;
}

interface TableFindings {
  thin_description_ids: string[];
  pii_tagged_ids: string[];
  new_asset_ids: string[];
  uncertified_ids: string[];
  no_domain_tag_ids: string[];
  lineage_isolated_ids: string[];
  lineage_upstream_only_ids: string[];
  lineage_downstream_only_ids: string[];
}

interface DashboardFindings {
  thin_description_ids: string[];
  pii_tagged_ids: string[];
  new_asset_ids: string[];
  uncertified_ids: string[];
  no_domain_tag_ids: string[];
}

interface TermFindings {
  thin_description_ids: string[];
  missing_owner_ids: string[];
  orphaned_ids: string[];
  uncertified_ids: string[];
}

function categorizeTables(
  tables: DetailedTable[],
  lineage: Map<string, LineageCounts>,
  opts: CategoryOpts
): TableFindings {
  return {
    thin_description_ids: sortIdsByCreatedDesc(tables, (t) =>
      isThinDescription(t.description)
    ),
    pii_tagged_ids: sortIdsByCreatedDesc(tables, (t) =>
      hasTagMatching(t, opts.piiPattern)
    ),
    new_asset_ids: sortIdsByCreatedDesc(tables, (t) =>
      isWithinDays(t.createdAt, opts.asOfMs, opts.newAssetDays)
    ),
    uncertified_ids: sortIdsByCreatedDesc(tables, (t) => t.isVerified !== true),
    no_domain_tag_ids: sortIdsByCreatedDesc(
      tables,
      (t) => !hasDomainTag(t, opts.domainTagPrefixLower)
    ),
    lineage_isolated_ids: sortIdsByCreatedDesc(tables, (t) => {
      const c = lineage.get(t.id);
      return c !== undefined && c.upstream === 0 && c.downstream === 0;
    }),
    lineage_upstream_only_ids: sortIdsByCreatedDesc(tables, (t) => {
      const c = lineage.get(t.id);
      return c !== undefined && c.upstream > 0 && c.downstream === 0;
    }),
    lineage_downstream_only_ids: sortIdsByCreatedDesc(tables, (t) => {
      const c = lineage.get(t.id);
      return c !== undefined && c.upstream === 0 && c.downstream > 0;
    }),
  };
}

function categorizeDashboards(
  dashboards: DetailedDashboard[],
  opts: CategoryOpts
): DashboardFindings {
  return {
    thin_description_ids: sortIdsByCreatedDesc(dashboards, (d) =>
      isThinDescription(d.description)
    ),
    pii_tagged_ids: sortIdsByCreatedDesc(dashboards, (d) =>
      hasTagMatching(d, opts.piiPattern)
    ),
    new_asset_ids: sortIdsByCreatedDesc(dashboards, (d) =>
      isWithinDays(d.createdAt, opts.asOfMs, opts.newAssetDays)
    ),
    uncertified_ids: sortIdsByCreatedDesc(
      dashboards,
      (d) => d.isVerified !== true
    ),
    no_domain_tag_ids: sortIdsByCreatedDesc(
      dashboards,
      (d) => !hasDomainTag(d, opts.domainTagPrefixLower)
    ),
  };
}

function categorizeTerms(
  terms: DetailedTerm[],
  termIdsWithOutboundPins: Set<string>,
  opts: CategoryOpts
): TermFindings {
  const isOrphaned = (t: DetailedTerm): boolean => {
    const noLinkedTag = !t.linkedTag;
    const noAttachedTags =
      !Array.isArray(t.tagEntities) || t.tagEntities.length === 0;
    const noOutboundPins = !termIdsWithOutboundPins.has(t.id);
    return noLinkedTag && noAttachedTags && noOutboundPins;
  };
  return {
    thin_description_ids: sortIdsByCreatedDesc(terms, (t) =>
      isThinDescription(t.description)
    ),
    missing_owner_ids: sortIdsByCreatedDesc(terms, (t) => !hasAssignedOwner(t)),
    orphaned_ids: sortIdsByCreatedDesc(terms, isOrphaned),
    uncertified_ids: sortIdsByCreatedDesc(terms, (t) => t.isVerified !== true),
  };
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineOwnerScorecard(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_owner_scorecard",
    config: {
      title: "Owner Cleanup Scorecard",
      description:
        "Grade the hygiene of one owner's asset portfolio in a single call. Given a user email, enumerates every table/dashboard/term they own and categorises each asset by the issues that most often need cleanup: missing or thin description, PII tags, recent creation (possible unattended imports), uncertified assets, no domain tag, lineage gaps (isolated / upstream-only / downstream-only), and term-specific health (missing owner, orphaned, uncertified).\n\n" +
        "Output shape: `{ identity, asOf, params, tables: {total, findings{...}}, dashboards: {total, findings{...}}, terms: {total, findings{...}}, unclassified_owned_ids: [...] }`. Each `*_ids` array inside `findings` is sorted newest-first by `createdAt DESC`, ready to drive a daily-review walkthrough. `unclassified_owned_ids` holds owned UUIDs that resolved as none of table/dashboard/term — typically columns, queries, or references to deleted assets. Reconcile with `identity.ownedAssetUniqueCount == tables.total + dashboards.total + terms.total + unclassified_owned_ids.length`. (`ownedAssetCount` is the raw user-record count and may differ if the API emits duplicates; `ownedAssetUniqueCount` is the deduped denominator.)\n\n" +
        "**Complete picture or explicit refusal.** The scorecard fans out lineage calls uncapped across every owned table (~20 concurrent) and runs a full outbound-pinned-assets check for term-orphaned detection. If any pagination ceiling is hit or the user isn't found within the 10k-user API scan cap, the tool refuses rather than emit a partial grade — re-run if the failure was transient.\n\n" +
        "Use for: daily/weekly owner review, leaver scrubs, tier-1 asset grading per domain owner. Pair with the `catalog-daily-guide` prompt to render a markdown agenda and walk remediation steps.",
      inputSchema: OwnerScorecardInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const email = args.email as string;
      const domainTagPrefixLower = (
        (args.domainTagPrefix as string | undefined) ?? DEFAULT_DOMAIN_TAG_PREFIX
      ).toLowerCase();
      const newAssetDays =
        (args.newAssetDays as number | undefined) ?? DEFAULT_NEW_ASSET_DAYS;
      const piiPatternSrc =
        (args.piiTagPattern as string | undefined) ?? DEFAULT_PII_TAG_PATTERN;
      const piiPattern = new RegExp(piiPatternSrc, "i");

      const asOfMs = Date.now();

      const resolution = await resolveOwnerByEmail(c, email);
      if (resolution.kind === "ceiling") {
        // Scan ceiling hit — the user may or may not exist beyond page 20.
        // Emitting notFound here would be a false certainty, so refuse.
        throw new Error(
          `User lookup did not reach the end of the user directory: scanned ` +
            `${resolution.usersScanned} users without finding '${email}'. The tenant is ` +
            `larger than the scorecard's scan ceiling; the public API has no ` +
            `user-by-email endpoint so this tool cannot distinguish "absent" ` +
            `from "beyond the ceiling" in this case. Retrying will not help.`
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

      // Heterogeneous ownedAssetIds — send the full list to all three search
      // endpoints; non-matching IDs simply don't come back on a given type.
      // Dedupe defensively in case the API ever returns duplicates.
      const allIds = Array.from(new Set(owner.ownedAssetIds));

      const [tables, dashboards, terms] = await Promise.all([
        hydrateTables(c, allIds),
        hydrateDashboards(c, allIds),
        hydrateTerms(c, allIds),
      ]);

      const [lineage, termsWithPins] = await Promise.all([
        lineageCountsForTables(
          c,
          tables.map((t) => t.id)
        ),
        termIdsWithOutboundPinnedAssets(
          c,
          terms.map((t) => t.id)
        ),
      ]);

      const opts: CategoryOpts = {
        domainTagPrefixLower,
        newAssetDays,
        piiPattern,
        asOfMs,
      };

      // Any owned ID that didn't resolve as a table / dashboard / term —
      // typically column UUIDs, query UUIDs, or references to deleted assets.
      // Surfacing the residual keeps the caller from wondering why
      // `ownedAssetCount` exceeds the sum of `tables/dashboards/terms.total`.
      const classified = new Set<string>();
      for (const t of tables) classified.add(t.id);
      for (const d of dashboards) classified.add(d.id);
      for (const t of terms) classified.add(t.id);
      const unclassifiedOwnedIds = allIds.filter((id) => !classified.has(id));

      return {
        identity: {
          userId: owner.userId,
          email: owner.email,
          firstName: owner.firstName,
          lastName: owner.lastName,
          // Raw count from the user directory (may include duplicates if the
          // API ever emits them). Kept for fidelity to the user record.
          ownedAssetCount: owner.ownedAssetIds.length,
          // Post-dedupe count — this is the denominator callers should use
          // when reconciling against `tables.total + dashboards.total +
          // terms.total + unclassified_owned_ids.length`. Arithmetic adds up
          // exactly even if the raw list had duplicates.
          ownedAssetUniqueCount: allIds.length,
        },
        asOf: new Date(asOfMs).toISOString(),
        params: {
          domainTagPrefix:
            (args.domainTagPrefix as string | undefined) ??
            DEFAULT_DOMAIN_TAG_PREFIX,
          newAssetDays,
          piiTagPattern: piiPatternSrc,
        },
        tables: {
          total: tables.length,
          findings: categorizeTables(tables, lineage, opts),
        },
        dashboards: {
          total: dashboards.length,
          findings: categorizeDashboards(dashboards, opts),
        },
        terms: {
          total: terms.length,
          findings: categorizeTerms(terms, termsWithPins, opts),
        },
        unclassified_owned_ids: unclassifiedOwnedIds,
      };
    }, client),
  };
}
