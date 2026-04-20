import { tmpdir } from "node:os";
import { join, normalize, relative, sep } from "node:path";

export const CACHE_DIR_NAME = "coalesce_catalog_mcp_cache";
export const CACHE_URI_SCHEME = "catalog://cache/";

/**
 * Process-scoped session directory component. Each server process gets its
 * own subdirectory under the cache root, so concurrent processes can't step
 * on each other's files and cleanup can purge sibling sessions by mtime.
 */
const SESSION_ID = `session-${Date.now()}-${process.pid}`;

export function getCacheBaseDir(): string {
  const fromEnv = process.env.COALESCE_CACHE_DIR?.trim();
  if (fromEnv) return fromEnv;
  return tmpdir();
}

export function getCacheRoot(): string {
  return join(getCacheBaseDir(), CACHE_DIR_NAME);
}

export function getSessionDir(): string {
  return join(getCacheRoot(), SESSION_ID);
}

/**
 * base64url-encode a UTF-8 string without padding. Lightweight wrapper so
 * call sites don't repeat the Buffer dance.
 */
function encodeBase64Url(input: string): string {
  return Buffer.from(input, "utf8").toString("base64url");
}

function decodeBase64Url(input: string): string {
  return Buffer.from(input, "base64url").toString("utf8");
}

/**
 * Build the relative cache path for a new artifact. The toolName + section
 * segments make the on-disk layout human-scannable when debugging; the uuid
 * guarantees uniqueness across concurrent calls with identical args.
 */
export function buildRelPath(
  toolName: string,
  section: string | undefined,
  uuid: string
): string {
  const safeTool = sanitizeSegment(toolName);
  const parts = [safeTool];
  if (section) parts.push(sanitizeSegment(section));
  parts.push(`${uuid}.json`);
  return parts.join("/");
}

export function encodeCacheKey(relPath: string): string {
  return encodeBase64Url(relPath);
}

/**
 * Decode a cache URI back to an absolute file path *inside the session dir*.
 * Returns null for malformed URIs or any path that attempts to escape the
 * session directory via `..`, absolute paths, or drive letters.
 */
export function resolveCacheUri(uri: string): { absPath: string; relPath: string } | null {
  if (!uri.startsWith(CACHE_URI_SCHEME)) return null;
  const key = uri.slice(CACHE_URI_SCHEME.length);
  if (!key || key.includes("/") || key.includes("\\")) return null;

  let relPath: string;
  try {
    relPath = decodeBase64Url(key);
  } catch {
    return null;
  }

  if (!relPath || relPath.length > 512) return null;
  if (relPath.includes("\0")) return null;
  // Reject absolute paths (POSIX `/foo` or Windows `C:\foo`) up front — on
  // POSIX, path.join treats them as a relative segment so they'd otherwise
  // slip past the traversal check below.
  if (relPath.startsWith("/") || /^[A-Za-z]:[\\/]/.test(relPath)) return null;

  const sessionDir = getSessionDir();
  const absPath = normalize(join(sessionDir, relPath));

  const rel = relative(sessionDir, absPath);
  if (rel.startsWith("..") || rel.startsWith(sep) || rel === "") return null;

  return { absPath, relPath };
}

export function buildCacheUri(relPath: string): string {
  return `${CACHE_URI_SCHEME}${encodeCacheKey(relPath)}`;
}

function sanitizeSegment(segment: string): string {
  const cleaned = segment.replace(/[^a-zA-Z0-9._-]/g, "_");
  return cleaned || "unnamed";
}
