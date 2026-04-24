import type { CatalogClient } from "../client.js";
import { CatalogApiError, CatalogGraphQLError } from "../client.js";
import {
  errorResult,
  textResult,
  type CatalogToolDefinition,
  type ToolHandlerExtra,
  type ToolResult,
} from "../catalog/types.js";
import {
  EXTERNALIZE_RESPONSE_THRESHOLD,
  externalizeIfLarge,
  isExternalizedPointer,
} from "../cache/externalize.js";

/**
 * Sentinel a wrapper (e.g. withConfirmation) can throw to short-circuit
 * with a non-error tool result — used so wrappers can return user-facing
 * messages without piggybacking on the error path.
 */
export class ToolEarlyReturn extends Error {
  constructor(public readonly result: ToolResult) {
    super("ToolEarlyReturn");
  }
}

/**
 * Wrap a tool implementation so that thrown Catalog errors become structured
 * MCP tool errors instead of propagating as MCP protocol failures. Keeps the
 * model able to see and reason about what went wrong rather than the tool
 * silently crashing.
 */
export function withErrorHandling<TArgs extends Record<string, unknown>>(
  impl: (
    args: TArgs,
    client: CatalogClient,
    extra?: ToolHandlerExtra
  ) => Promise<unknown>,
  client: CatalogClient
): (args: TArgs, extra?: ToolHandlerExtra) => Promise<ToolResult> {
  return async (args, extra) => {
    try {
      const result = await impl(args, client, extra);
      return textResult(result);
    } catch (err) {
      if (err instanceof ToolEarlyReturn) return err.result;
      if (err instanceof CatalogGraphQLError) {
        return errorResult(err.message, {
          kind: "graphql_error",
          errors: err.errors,
        });
      }
      if (err instanceof CatalogApiError) {
        return errorResult(err.message, {
          kind: "http_error",
          status: err.status,
          detail: err.detail,
        });
      }
      const message = err instanceof Error ? err.message : String(err);
      return errorResult(message, { kind: "unexpected" });
    }
  };
}

/**
 * Build a mutation result for batch operations where the API returns an
 * array of results. Compares the response count against the input count
 * and surfaces a `partialFailure` flag when fewer rows are returned than
 * were submitted — this lets the agent detect silent partial failures that
 * the raw GraphQL API does not signal.
 */
export function batchResult<T>(
  label: string,
  data: T[],
  expectedCount: number
): Record<string, unknown> {
  const result: Record<string, unknown> = {
    [label]: data.length,
    data,
  };
  if (data.length < expectedCount) {
    result.partialFailure = true;
    result.expectedCount = expectedCount;
  }
  return result;
}

/**
 * Wrap a tool handler so any non-error response whose serialised text exceeds
 * the externalization threshold gets written to the session cache and
 * replaced with a small `{ externalized, resourceUri, ... }` pointer. Error
 * results and declines pass through unchanged so the model still sees their
 * full message inline.
 *
 * Callers opt out per-tool by setting `neverExternalize: true` on the
 * CatalogToolDefinition — use sparingly (health checks, single-scalar tools).
 */
export function withResponseExternalization(
  handler: CatalogToolDefinition["handler"],
  opts: { toolName: string; neverExternalize?: boolean }
): CatalogToolDefinition["handler"] {
  if (opts.neverExternalize) return handler;
  return async (args, extra) => {
    const result = await handler(args, extra);
    if (result.isError) return result;
    const first = result.content[0];
    if (!first || first.type !== "text") return result;
    if (Buffer.byteLength(first.text, "utf8") <= EXTERNALIZE_RESPONSE_THRESHOLD) {
      return result;
    }
    let parsed: unknown;
    try {
      parsed = JSON.parse(first.text);
    } catch {
      return result;
    }
    const replaced = externalizeIfLarge(parsed, {
      toolName: opts.toolName,
      threshold: EXTERNALIZE_RESPONSE_THRESHOLD,
    });
    if (!isExternalizedPointer(replaced)) return result;
    return textResult(replaced);
  };
}

/**
 * Normalise a list-query GraphQL output into a uniform envelope for MCP
 * responses. Callers provide the raw data array and pagination metadata;
 * the envelope surfaces `hasMore` so the LLM can decide whether to paginate.
 *
 * When `totalCount` is `null` (the GraphQL endpoint doesn't return it),
 * `hasMore` is inferred from whether the page is full, and `totalCount`
 * is omitted from the output to avoid misleading the agent.
 *
 * Note: when `totalCount` is null and the last page happens to contain
 * exactly `nbPerPage` items, `hasMore` will be `true` — the agent will
 * fetch one more (empty) page. This is the correct heuristic when the
 * server provides no count; the alternative (under-fetching) is worse.
 */
export function listEnvelope<T>(
  page: number,
  nbPerPage: number,
  totalCount: number | null,
  data: T[]
): {
  pagination: { page: number; nbPerPage: number; totalCount?: number; hasMore: boolean };
  data: T[];
} {
  if (totalCount === null) {
    return {
      pagination: {
        page,
        nbPerPage,
        hasMore: data.length >= nbPerPage,
      },
      data,
    };
  }
  const seenSoFar = page * nbPerPage + data.length;
  return {
    pagination: {
      page,
      nbPerPage,
      totalCount,
      hasMore: seenSoFar < totalCount,
    },
    data,
  };
}
