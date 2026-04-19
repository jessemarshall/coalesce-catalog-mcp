import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  SEARCH_QUERIES,
  ADD_AI_ASSISTANT_JOB,
  GET_AI_ASSISTANT_JOB_RESULT,
} from "../catalog/operations.js";
import type {
  AddAiAssistantJobInput,
  AddAiAssistantJobOutput,
  FilterTablesMode,
  GetAiAssistantJobResultOutput,
  Origin,
  SearchQueriesOutput,
  SearchQueriesScope,
} from "../generated/types.js";
import { withErrorHandling } from "./tool-helpers.js";

const FilterTablesModeSchema = z.enum([
  "ALL",
  "ANY",
]) satisfies z.ZodType<FilterTablesMode>;

const OriginSchema = z.enum([
  "API",
  "APP",
  "DUST",
  "EXTENSION",
  "MS_TEAMS",
  "SLACK_BOT",
]) satisfies z.ZodType<Origin>;

// ── Semantic query search ───────────────────────────────────────────────────

const SearchQueriesInputShape = {
  question: z
    .string()
    .min(1)
    .max(1024)
    .describe(
      "Natural-language question describing the SQL queries you want to find (<=256 words, <=1024 chars). Returns the 10 most semantically relevant queries previously run against the warehouse."
    ),
  translateQuestionToEnglish: z
    .boolean()
    .optional()
    .describe("Force English translation of the question; improves accuracy for non-English questions at the cost of latency."),
  tableIds: z
    .array(z.string())
    .max(10)
    .optional()
    .describe("Narrow the semantic search to queries that reference these tables (max 10 UUIDs)."),
  filterMode: FilterTablesModeSchema.optional().describe(
    "ALL = query must touch every listed table; ANY = at least one. Default: ALL. Only meaningful when tableIds is provided."
  ),
};

function buildSearchQueriesScope(
  input: Record<string, unknown>
): SearchQueriesScope | undefined {
  if (!Array.isArray(input.tableIds) || input.tableIds.length === 0) return undefined;
  const scope: SearchQueriesScope = {
    tableIds: input.tableIds as string[],
  };
  if (typeof input.filterMode === "string") {
    scope.filterMode = input.filterMode as FilterTablesMode;
  }
  return scope;
}

// ── AI assistant (start + poll) ─────────────────────────────────────────────

const AskAssistantInputShape = {
  email: z
    .string()
    .email()
    .describe(
      "Email of an existing Catalog user (required). The assistant errors if the email is not in the account."
    ),
  question: z
    .string()
    .min(1)
    .max(10000)
    .describe("Question to ask the AI assistant (<=10000 characters)."),
  externalConversationId: z
    .string()
    .min(1)
    .describe(
      "Conversation key (any string) used to thread multi-turn context. Reuse across messages to maintain continuity; change to start a fresh conversation."
    ),
  origin: OriginSchema.optional().describe(
    "Where the question is coming from. Defaults to API when omitted. Allowed: API, APP, DUST, EXTENSION, MS_TEAMS, SLACK_BOT."
  ),
};

const GetAssistantResultInputShape = {
  jobId: z
    .string()
    .min(1)
    .describe("jobId returned from catalog_ask_assistant."),
  delaySeconds: z
    .number()
    .int()
    .min(0)
    .max(10)
    .optional()
    .describe(
      "Seconds to wait server-side before returning if the job is still ACTIVE (0-10). Values above ~10 are rejected by the server as BAD_USER_INPUT. If you need longer waits, omit this argument and poll client-side instead."
    ),
};

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineAiTools(client: CatalogClient): CatalogToolDefinition[] {
  return [
    {
      name: "catalog_search_queries",
      config: {
        title: "Semantic Search of SQL Queries",
        description:
          "Search ingested SQL queries by natural-language meaning, not keywords. Returns the 10 most semantically relevant queries with their author + referenced tableIds. Use for: finding example queries that compute a concept (\"active users per week\"), locating prior queries against a set of tables, or surfacing patterns before writing new SQL.\n\n" +
          "Narrow via `tableIds` (max 10) + `filterMode` (ALL/ANY) when you already know which tables to scope to.",
        inputSchema: SearchQueriesInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const variables = {
          data: {
            question: args.question as string,
            ...(typeof args.translateQuestionToEnglish === "boolean"
              ? { translateQuestionToEnglish: args.translateQuestionToEnglish }
              : {}),
          },
          scope: buildSearchQueriesScope(args),
        };
        const data = await c.query<{ searchQueries: SearchQueriesOutput }>(
          SEARCH_QUERIES,
          variables
        );
        return { data: data.searchQueries.data };
      }, client),
    },

    {
      name: "catalog_ask_assistant",
      config: {
        title: "Ask the Catalog AI Assistant (start job)",
        description:
          "Kick off an async AI Assistant job against the Catalog's RAG index (answers sourced from catalog descriptions, lineage, tags, etc.). Returns a jobId; poll it with catalog_get_assistant_result.\n\n" +
          "The assistant requires an `email` of an existing Catalog user (errors otherwise). Use `externalConversationId` to thread multiple turns; generate a fresh random string to start a new conversation.",
        inputSchema: AskAssistantInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: AddAiAssistantJobInput = {
          email: args.email as string,
          externalConversationId: args.externalConversationId as string,
          question: args.question as string,
          ...(typeof args.origin === "string" ? { origin: args.origin as Origin } : {}),
        };
        const data = await c.query<{ addAiAssistantJob: AddAiAssistantJobOutput }>(
          ADD_AI_ASSISTANT_JOB,
          { data: input }
        );
        return data.addAiAssistantJob.data;
      }, client),
    },

    {
      name: "catalog_get_assistant_result",
      config: {
        title: "Get AI Assistant Job Result (poll)",
        description:
          "Poll the result of an AI Assistant job started with catalog_ask_assistant. Returns { status: ADDED | ACTIVE | COMPLETED | FAILED | RETRIES_EXHAUSTED, answer, assets[] }.\n\n" +
          "Pass `delaySeconds` (0-10) to block server-side if the job is still ACTIVE — reduces polling overhead. When status is COMPLETED, `answer` is the final text and `assets` lists referenced catalog entities (tables, dashboards, terms) with internal + external links.",
        inputSchema: GetAssistantResultInputShape,
        annotations: READ_ONLY_ANNOTATIONS,
      },
      handler: withErrorHandling(async (args, c) => {
        const input: Record<string, unknown> = { id: args.jobId as string };
        if (typeof args.delaySeconds === "number") input.delaySeconds = args.delaySeconds;
        const data = await c.query<{
          getAiAssistantJobResult: GetAiAssistantJobResultOutput;
        }>(GET_AI_ASSISTANT_JOB_RESULT, { data: input });
        return data.getAiAssistantJobResult.data;
      }, client),
    },
  ];
}
