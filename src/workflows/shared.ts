/**
 * Shared helpers used across multiple workflow files.
 *
 * Consolidated here so a schema drift in ownerEntities / teamOwnerEntities
 * only needs to be fixed in one place.
 */

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

export const ENRICHMENT_BATCH_SIZE = 500;

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
