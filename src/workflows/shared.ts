/**
 * Shared helpers used across multiple workflow files.
 *
 * Consolidated here so a schema drift in ownerEntities / teamOwnerEntities
 * only needs to be fixed in one place.
 */

import type { CatalogClient } from "../client.js";
import {
  GET_LINEAGES,
  GET_TABLE_QUERIES,
  GET_USERS,
} from "../catalog/operations.js";
import type {
  GetLineagesOutput,
  GetTableQueriesOutput,
  GetUsersOutput,
} from "../generated/types.js";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const ENRICHMENT_BATCH_SIZE = 500;

// Shared directory page-scan ceiling. The public API has no user-by-email
// or team-by-id endpoint, so any workflow or governance helper that needs
// to resolve identity → row iterates `getUsers` / `getTeams` pages until it
// finds a match or blows the cap. Used by `resolveUserByEmail` here, plus
// `findUserById` / `findTeamById` in `mcp/governance.ts`. Single source of
// truth so all lookups refuse at the same boundary by construction.
export const USER_PAGE_SIZE = 500;
export const USER_LOOKUP_MAX_PAGES = 20;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface Owners {
  userOwners: Array<{
    userId: string;
    email: string | null;
    fullName: string | null;
  }>;
  teamOwners: Array<{ teamId: string; name: string | null }>;
}

// ---------------------------------------------------------------------------
// Utility functions
// ---------------------------------------------------------------------------

export function isNonEmptyString(v: unknown): v is string {
  return typeof v === "string" && v.trim().length > 0;
}

export function extractOwners(row: Record<string, unknown>): Owners {
  const userOwners = Array.isArray(row.ownerEntities)
    ? (row.ownerEntities as Array<Record<string, unknown>>)
        .filter((o) => o.userId != null)
        .map((o) => {
          const u = (o.user as Record<string, unknown> | undefined) ?? {};
          return {
            userId: o.userId as string,
            email: (u.email as string | null) ?? null,
            fullName: (u.fullName as string | null) ?? null,
          };
        })
    : [];
  const teamOwners = Array.isArray(row.teamOwnerEntities)
    ? (row.teamOwnerEntities as Array<Record<string, unknown>>)
        .filter((t) => t.teamId != null)
        .map((t) => {
          const team = (t.team as Record<string, unknown> | undefined) ?? {};
          return {
            teamId: t.teamId as string,
            name: (team.name as string | null) ?? null,
          };
        })
    : [];
  return { userOwners, teamOwners };
}

export function hasOwner(row: Record<string, unknown>): boolean {
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

export function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

export function extractTagLabels(row: Record<string, unknown>): string[] {
  if (!Array.isArray(row.tagEntities)) return [];
  const out: string[] = [];
  for (const t of row.tagEntities as Array<Record<string, unknown>>) {
    const tag = t.tag as Record<string, unknown> | undefined;
    const label = tag?.label;
    if (typeof label === "string" && label.length > 0) out.push(label);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Owner identity resolution (email -> user record)
// ---------------------------------------------------------------------------

export interface OwnerIdentity {
  userId: string;
  email: string;
  firstName: string;
  lastName: string;
  ownedAssetIds: string[];
  teamIds?: string[];
}

// Distinguish "user confirmed absent" (short page reached without a match)
// from "scan ceiling hit" (20 full pages without a match). The first is a
// terminal notFound for the caller; the second means the scan is incomplete
// and the tool must refuse rather than silently claim the user doesn't exist.
export type OwnerResolution =
  | { kind: "found"; owner: OwnerIdentity }
  | { kind: "absent" }
  | { kind: "ceiling"; usersScanned: number };

export async function resolveUserByEmail(
  client: CatalogClient,
  email: string
): Promise<OwnerResolution> {
  const target = email.toLowerCase();
  for (let page = 0; page < USER_LOOKUP_MAX_PAGES; page++) {
    const resp = await client.execute<{ getUsers: GetUsersOutput[] }>(
      GET_USERS,
      { pagination: { nbPerPage: USER_PAGE_SIZE, page } }
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
          teamIds: (match as { teamIds?: string[] }).teamIds,
        },
      };
    }
    if (resp.getUsers.length < USER_PAGE_SIZE) return { kind: "absent" };
  }
  return {
    kind: "ceiling",
    usersScanned: USER_PAGE_SIZE * USER_LOOKUP_MAX_PAGES,
  };
}

// ---------------------------------------------------------------------------
// Neighbor owners (reused by evidence-bundle gatherers)
// ---------------------------------------------------------------------------

export interface NeighborOwner {
  type: "user" | "team";
  userId?: string | null;
  teamId?: string | null;
  email?: string | null;
  name: string | null;
}

export function extractNeighborOwners(
  row: Record<string, unknown> | undefined
): NeighborOwner[] {
  if (!row) return [];
  const owners: NeighborOwner[] = [];
  if (Array.isArray(row.ownerEntities)) {
    for (const o of row.ownerEntities as Array<Record<string, unknown>>) {
      if (o.userId == null) continue;
      const user = (o.user as Record<string, unknown> | undefined) ?? {};
      owners.push({
        type: "user",
        userId: o.userId as string,
        email: (user.email as string | null) ?? null,
        name: (user.fullName as string | null) ?? null,
      });
    }
  }
  if (Array.isArray(row.teamOwnerEntities)) {
    for (const t of row.teamOwnerEntities as Array<Record<string, unknown>>) {
      if (t.teamId == null) continue;
      const team = (t.team as Record<string, unknown> | undefined) ?? {};
      owners.push({
        type: "team",
        teamId: t.teamId as string,
        name: (team.name as string | null) ?? null,
      });
    }
  }
  return owners;
}

// ---------------------------------------------------------------------------
// Query-author probe (reused by ownership-evidence gatherers)
// ---------------------------------------------------------------------------

export interface QueryAuthorSignal {
  author: string;
  queryCount: number;
  queryTypeBreakdown: Record<string, number>;
}

export interface QueryAuthorProbe {
  authors: QueryAuthorSignal[];
  totalQueriesSeen: number;
  queriesWithoutAuthor: number;
  probeCap: number;
}

// One call, up to `probeSize` queries. The API sorts by timestamp DESC by
// default; the probe is "which humans touched this table recently" not
// "cumulative all-time volume," which is the correct framing for pointing
// at a likely owner.
export async function fetchTableQueryAuthors(
  client: CatalogClient,
  tableId: string,
  opts: { topN: number; probeSize: number }
): Promise<QueryAuthorProbe> {
  const resp = await client.execute<{
    getTableQueries: GetTableQueriesOutput;
  }>(GET_TABLE_QUERIES, {
    scope: { tableIds: [tableId] },
    sorting: [{ sortingKey: "timestamp", direction: "DESC" }],
    pagination: { nbPerPage: opts.probeSize, page: 0 },
  });
  const rows = resp.getTableQueries.data as Array<{
    author?: string | null;
    queryType?: string | null;
  }>;
  const counts = new Map<string, QueryAuthorSignal>();
  let queriesWithoutAuthor = 0;
  for (const row of rows) {
    const author = row.author;
    if (!author || author.trim().length === 0) {
      queriesWithoutAuthor += 1;
      continue;
    }
    const entry = counts.get(author) ?? {
      author,
      queryCount: 0,
      queryTypeBreakdown: {},
    };
    entry.queryCount += 1;
    const qt = row.queryType ?? "UNKNOWN";
    entry.queryTypeBreakdown[qt] = (entry.queryTypeBreakdown[qt] ?? 0) + 1;
    counts.set(author, entry);
  }
  const sorted = [...counts.values()].sort(
    (a, b) => b.queryCount - a.queryCount || a.author.localeCompare(b.author)
  );
  return {
    authors: sorted.slice(0, opts.topN),
    totalQueriesSeen: rows.length,
    queriesWithoutAuthor,
    probeCap: opts.probeSize,
  };
}

// ---------------------------------------------------------------------------
// 1-hop lineage neighbor lookup (reused by evidence-bundle gatherers)
// ---------------------------------------------------------------------------

export interface NeighborRef {
  id: string;
  kind: "TABLE" | "DASHBOARD";
}

// Paginate exhaustively up to `maxPages`; throw if we blow that ceiling.
// "Complete or refuse" contract — we don't want to silently drop neighbors
// past a ceiling even if the caller only cares about the first handful.
export async function fetchOneHopNeighborIds(
  client: CatalogClient,
  tableId: string,
  direction: "upstream" | "downstream",
  opts: { pageSize: number; maxPages: number }
): Promise<NeighborRef[]> {
  const seen = new Set<string>();
  const out: NeighborRef[] = [];
  for (let page = 0; page < opts.maxPages; page++) {
    const scope: Record<string, string> = {};
    if (direction === "downstream") scope.parentTableId = tableId;
    else scope.childTableId = tableId;
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope,
        pagination: { nbPerPage: opts.pageSize, page },
      }
    );
    const rows = resp.getLineages.data;
    for (const e of rows) {
      const neighborId =
        direction === "downstream"
          ? (e.childTableId ?? e.childDashboardId)
          : e.parentTableId;
      if (!neighborId) continue;
      if (seen.has(neighborId)) continue;
      seen.add(neighborId);
      // Upstream edges against a childTableId scope can only have table
      // parents (no parent-dashboard-of-a-table edge shape exists in the
      // API), so upstream is always TABLE. Downstream can be either.
      const kind: "TABLE" | "DASHBOARD" =
        direction === "upstream" || e.childTableId ? "TABLE" : "DASHBOARD";
      out.push({ id: neighborId, kind });
    }
    if (rows.length < opts.pageSize) return out;
  }
  throw new Error(
    `Lineage pagination exceeded ${opts.maxPages} pages for ${direction} ` +
      `of table ${tableId} (>${opts.maxPages * opts.pageSize} edges). ` +
      `Refusing to produce partial neighbor evidence; investigate the lineage data for this node.`
  );
}

// ---------------------------------------------------------------------------
// Downstream edge count (used for blast-radius scoring)
// ---------------------------------------------------------------------------

// Counts distinct downstream edges for a TABLE or DASHBOARD parent. Paginates
// exhaustively, refusing rather than silently truncating on hub nodes.
export async function countDownstreamEdges(
  client: CatalogClient,
  parentId: string,
  parentKind: "TABLE" | "DASHBOARD",
  opts: { pageSize: number; maxPages: number }
): Promise<number> {
  let count = 0;
  for (let page = 0; page < opts.maxPages; page++) {
    const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
      GET_LINEAGES,
      {
        scope:
          parentKind === "TABLE"
            ? { parentTableId: parentId }
            : { parentDashboardId: parentId },
        pagination: { nbPerPage: opts.pageSize, page },
      }
    );
    const rows = resp.getLineages.data;
    count += rows.length;
    if (rows.length < opts.pageSize) return count;
  }
  throw new Error(
    `Lineage pagination exceeded ${opts.maxPages} pages (downstream) for ` +
      `${parentKind.toLowerCase()} ${parentId} (>${opts.maxPages * opts.pageSize} edges). ` +
      `Refusing to produce a partial count.`
  );
}
