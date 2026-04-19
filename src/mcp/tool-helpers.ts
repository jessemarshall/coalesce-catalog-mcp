import type { CatalogClient } from "../client.js";
import { CatalogApiError, CatalogGraphQLError } from "../client.js";
import { errorResult, textResult, type ToolResult } from "../catalog/types.js";

/**
 * Wrap a tool implementation so that thrown Catalog errors become structured
 * MCP tool errors instead of propagating as MCP protocol failures. Keeps the
 * model able to see and reason about what went wrong rather than the tool
 * silently crashing.
 */
export function withErrorHandling<TArgs extends Record<string, unknown>>(
  impl: (args: TArgs, client: CatalogClient) => Promise<unknown>,
  client: CatalogClient
): (args: TArgs) => Promise<ToolResult> {
  return async (args) => {
    try {
      const result = await impl(args, client);
      return textResult(result);
    } catch (err) {
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
 * Normalise a list-query GraphQL output into a uniform envelope for MCP
 * responses. Callers provide the raw data array and pagination metadata;
 * the envelope surfaces `hasMore` so the LLM can decide whether to paginate.
 *
 * When `totalCount` is `null` (the GraphQL endpoint doesn't return it),
 * `hasMore` is inferred from whether the page is full, and `totalCount`
 * is omitted from the output to avoid misleading the agent.
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
