import { z } from "zod";

export const SortDirectionSchema = z
  .enum(["ASC", "DESC"])
  .describe("Sort direction: ASC (ascending) or DESC (descending).");

export const NullsPrioritySchema = z
  .enum(["FIRST", "LAST"])
  .describe("Position of null values: FIRST or LAST.");

export type SortDirection = z.infer<typeof SortDirectionSchema>;
export type NullsPriority = z.infer<typeof NullsPrioritySchema>;
