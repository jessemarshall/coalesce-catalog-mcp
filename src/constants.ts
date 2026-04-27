import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

export const SERVER_NAME = "coalesce-catalog";

// Read from package.json so the MCP handshake always advertises the shipped
// version — hard-coding drifted at every release. Works in both dev (src/)
// and prod (dist/) because ../package.json resolves identically from either.
const pkgUrl = new URL("../package.json", import.meta.url);
const pkg = JSON.parse(readFileSync(fileURLToPath(pkgUrl), "utf8")) as {
  version: string;
};
export const SERVER_VERSION = pkg.version;

export type CatalogRegion = "eu" | "us";

export const DEFAULT_REGION: CatalogRegion = "eu";

export const REGION_BASE_URLS: Record<CatalogRegion, string> = {
  eu: "https://api.castordoc.com",
  us: "https://api.us.castordoc.com",
};

export const GRAPHQL_PATH = "/public/graphql";

export const DEFAULT_REQUEST_TIMEOUT_MS = 60_000;

export const DEFAULT_PAGE_SIZE = 100;
export const MAX_PAGE_SIZE = 500;

/**
 * Maximum number of rows accepted in a single batch mutation. Applies
 * uniformly to every Catalog GraphQL mutation that takes an array input
 * (upsert/delete/attach/detach/etc.). Centralised so a platform-side change
 * to the cap only needs one edit.
 *
 * Scope: mutation input batches only. Workflow tools that cap output
 * enumeration (e.g. max columns to include in a summary, max edges to
 * traverse in a lineage walk) use their own per-workflow capacity gates and
 * intentionally do NOT route through this constant — they're shaped by
 * agent-context budget, not the API contract.
 */
export const MAX_BATCH_SIZE = 500;

export const READ_ONLY_ENV_VAR = "COALESCE_CATALOG_READ_ONLY";

/**
 * When set to "true", destructive tools skip the elicitation confirmation
 * step. Intended for non-interactive deployments (CI, scripted batch jobs)
 * where the operator has already vetted the call. Default behavior is to
 * require an interactive accept via the MCP elicitation protocol.
 */
export const SKIP_CONFIRMATIONS_ENV_VAR = "COALESCE_CATALOG_SKIP_CONFIRMATIONS";
