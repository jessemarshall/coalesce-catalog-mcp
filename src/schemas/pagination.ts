import { z } from "zod";
import { DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE } from "../constants.js";

/**
 * Pagination input shared across all Catalog list queries.
 *
 * The GraphQL API's `Pagination` input has only { nbPerPage, page }; it is
 * 0-indexed. The API caps page size at 500.
 */
export const PaginationInputShape = {
  nbPerPage: z
    .number()
    .int()
    .min(1)
    .max(MAX_PAGE_SIZE)
    .optional()
    .describe(
      `Number of results per page (1-${MAX_PAGE_SIZE}). Default: ${DEFAULT_PAGE_SIZE}.`
    ),
  page: z
    .number()
    .int()
    .min(0)
    .optional()
    .describe("Page number, 0-indexed. Default: 0."),
};

export const PaginationInputSchema = z.object(PaginationInputShape);
export type PaginationInput = z.infer<typeof PaginationInputSchema>;

export interface GraphQLPagination {
  nbPerPage: number;
  page: number;
}

export function toGraphQLPagination(input: PaginationInput): GraphQLPagination {
  return {
    nbPerPage: input.nbPerPage ?? DEFAULT_PAGE_SIZE,
    page: input.page ?? 0,
  };
}
