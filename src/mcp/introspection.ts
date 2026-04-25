import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import { isReadOnlyMode } from "../server.js";
import { withErrorHandling } from "./tool-helpers.js";

// ── catalog_describe_type ───────────────────────────────────────────────────

interface IntrospectedTypeRef {
  kind: string;
  name: string | null;
  ofType: IntrospectedTypeRef | null;
}

interface IntrospectedField {
  name: string;
  description: string | null;
  type: IntrospectedTypeRef;
}

interface IntrospectedEnumValue {
  name: string;
  description: string | null;
}

interface IntrospectedType {
  name: string;
  kind: string;
  description: string | null;
  fields: IntrospectedField[] | null;
  inputFields: IntrospectedField[] | null;
  enumValues: IntrospectedEnumValue[] | null;
}

const INTROSPECTION_QUERY = /* GraphQL */ `
  query CatalogDescribeType($name: String!) {
    __type(name: $name) {
      name
      kind
      description
      fields(includeDeprecated: true) {
        name
        description
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
      }
      inputFields {
        name
        description
        type {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
      }
      enumValues(includeDeprecated: true) {
        name
        description
      }
    }
  }
`;

const ALL_TYPE_NAMES_QUERY = /* GraphQL */ `
  query CatalogListTypeNames {
    __schema {
      types {
        name
      }
    }
  }
`;

/**
 * Render a GraphQL type reference as native SDL syntax — e.g. `[String!]!`,
 * `LIST[Column]`, `NON_NULL[...]`. Callers skim this as a one-liner instead of
 * unwrapping the nested ofType chain themselves.
 */
function renderTypeRef(ref: IntrospectedTypeRef | null): string {
  if (!ref) return "Unknown";
  if (ref.kind === "NON_NULL") {
    return renderTypeRef(ref.ofType) + "!";
  }
  if (ref.kind === "LIST") {
    return "[" + renderTypeRef(ref.ofType) + "]";
  }
  return ref.name ?? ref.kind;
}

/** Outermost NON_NULL wrapping tells us whether the field is required. */
function isFieldRequired(ref: IntrospectedTypeRef): boolean {
  return ref.kind === "NON_NULL";
}

function flattenField(f: IntrospectedField): {
  name: string;
  type: string;
  description: string | null;
  isRequired: boolean;
} {
  return {
    name: f.name,
    type: renderTypeRef(f.type),
    description: f.description,
    isRequired: isFieldRequired(f.type),
  };
}

/** Iterative Levenshtein edit distance (O(len * len) space/time). */
function editDistance(a: string, b: string): number {
  if (a === b) return 0;
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;
  const prev = new Array(b.length + 1);
  const curr = new Array(b.length + 1);
  for (let j = 0; j <= b.length; j++) prev[j] = j;
  for (let i = 1; i <= a.length; i++) {
    curr[0] = i;
    for (let j = 1; j <= b.length; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(
        curr[j - 1] + 1,
        prev[j] + 1,
        prev[j - 1] + cost
      );
    }
    for (let j = 0; j <= b.length; j++) prev[j] = curr[j];
  }
  return prev[b.length];
}

/**
 * Suggest near-match type names when `__type(name: $name)` returns null.
 * Accepts a name if (a) its lowercase form is a substring either way (covers
 * casing typos like "fieldlineage" → "FieldLineage"), or (b) its edit distance
 * to the needle is ≤ 2 (covers single-letter typos like "linage" → "Lineage").
 * Sorted by edit distance then by length delta; capped at 10 results.
 */
function findSuggestions(needle: string, haystack: string[]): string[] {
  const lcNeedle = needle.toLowerCase();
  const scored: Array<{ name: string; distance: number }> = [];
  for (const name of haystack) {
    const lcName = name.toLowerCase();
    if (lcName.includes(lcNeedle) || lcNeedle.includes(lcName)) {
      scored.push({ name, distance: 0 });
      continue;
    }
    const d = editDistance(lcName, lcNeedle);
    if (d <= 2) scored.push({ name, distance: d });
  }
  scored.sort((a, b) => {
    if (a.distance !== b.distance) return a.distance - b.distance;
    return (
      Math.abs(a.name.length - needle.length) -
      Math.abs(b.name.length - needle.length)
    );
  });
  return scored.slice(0, 10).map((s) => s.name);
}

const DescribeTypeInputShape = {
  typeName: z
    .string()
    .min(1)
    .describe(
      "Exact GraphQL type name, case-sensitive (e.g. 'Lineage', 'FieldLineage', 'GetFieldLineagesScope'). If the type doesn't exist, the tool returns null plus near-match suggestions."
    ),
};

function defineDescribeType(client: CatalogClient): CatalogToolDefinition {
  return {
    name: "catalog_describe_type",
    config: {
      title: "Describe GraphQL Type",
      description:
        "Introspect a type on the Catalog Public GraphQL API: returns its kind, description, fields (for OBJECT / INTERFACE), inputFields (for INPUT_OBJECT), or enumValues (for ENUM). Each field includes its rendered GraphQL type (e.g. `[String!]!`), description, and an `isRequired` flag computed from the outermost NON_NULL wrapping.\n\n" +
        "Use this when a customer or tool asks an API-shape question — e.g. 'does getLineages accept a column scope?', 'what fields does FieldLineage return?', 'what's in GetFieldLineagesScope?'. Cheaper and more reliable than guessing from docs. Pairs with catalog_run_graphql for one-shot API debugging.\n\n" +
        "Returns: { type: { name, kind, description?, fields?, inputFields?, enumValues? } } on hit, or { notFound: true, typeName, suggestions: [...] } when no type exists by that name.",
      inputSchema: DescribeTypeInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const typeName = args.typeName as string;
      const resp = await c.execute<{ __type: IntrospectedType | null }>(
        INTROSPECTION_QUERY,
        { name: typeName }
      );
      const t = resp.__type;
      if (!t) {
        const suggestions = await fetchSuggestions(c, typeName);
        return {
          notFound: true,
          typeName,
          suggestions,
        };
      }
      const flattened: Record<string, unknown> = {
        name: t.name,
        kind: t.kind,
      };
      if (t.description) flattened.description = t.description;
      if (t.fields) flattened.fields = t.fields.map(flattenField);
      if (t.inputFields) flattened.inputFields = t.inputFields.map(flattenField);
      if (t.enumValues) {
        flattened.enumValues = t.enumValues.map((e) => ({
          name: e.name,
          description: e.description,
        }));
      }
      return { type: flattened };
    }, client),
  };
}

async function fetchSuggestions(
  client: CatalogClient,
  typeName: string
): Promise<string[]> {
  try {
    const resp = await client.execute<{
      __schema: { types: { name: string | null }[] };
    }>(ALL_TYPE_NAMES_QUERY, {});
    const names = resp.__schema.types
      .map((t) => t.name)
      .filter((n): n is string => typeof n === "string" && !n.startsWith("__"));
    return findSuggestions(typeName, names);
  } catch {
    // Suggestion lookup is best-effort; don't let a failing schema scan
    // prevent the notFound response from getting back to the caller.
    return [];
  }
}

// ── catalog_run_graphql ─────────────────────────────────────────────────────

const RunGraphQLInputShape = {
  query: z
    .string()
    .min(1)
    .describe(
      "Raw GraphQL document. Queries run without ceremony; mutations are blocked unless allowMutations is true."
    ),
  variables: z
    .record(z.unknown())
    .optional()
    .describe("Optional variables map for the document."),
  operationName: z
    .string()
    .optional()
    .describe(
      "Selects an operation when the document contains multiple. Pair with a matching mutation-detection override if the chosen op is a mutation."
    ),
  allowMutations: z
    .boolean()
    .optional()
    .describe(
      "Opt-in override to allow mutation operations. Default false — protects against an agent accidentally calling deleteLineages / upsertLineages during exploration."
    ),
  timeoutMs: z
    .number()
    .int()
    .positive()
    .optional()
    .describe(
      "Per-call HTTP timeout in milliseconds for the upstream GraphQL request. Overrides the server's default (set via COALESCE_CATALOG_REQUEST_TIMEOUT_MS or DEFAULT_REQUEST_TIMEOUT_MS). Use a longer value (e.g. 120000) for paginated reads on big catalogs."
    ),
};

/**
 * Strip `#`-style GraphQL comments before doing keyword detection.
 * GraphQL's only comment syntax is `#` to end-of-line.
 */
function stripComments(doc: string): string {
  return doc.replace(/#[^\n]*/g, "");
}

/**
 * Heuristic mutation detection. Looks for a `mutation` keyword followed by
 * an optional name and an opening brace/paren — matches both `mutation Foo {`
 * and shorthand `mutation {`. Ignores `#` comments but does not strip string
 * literals, so a document that embeds the word `mutation` inside a string
 * argument can false-positive; callers can override via `allowMutations`.
 *
 * When `operationName` is provided, the detection scopes to that one named
 * operation — `query Foo { ... } mutation Bar { ... }` with operationName:
 * "Foo" is correctly identified as a query, not a mutation.
 */
function looksLikeMutation(doc: string, operationName?: string): boolean {
  const stripped = stripComments(doc);
  if (operationName) {
    // Matches the operation's definition header — `query Foo` / `mutation Foo`
    // / `subscription Foo` — optionally followed by variables `(...)` and the
    // opening `{`. Captures the keyword.
    const opPattern = new RegExp(
      `\\b(query|mutation|subscription)\\s+${escapeRegex(operationName)}\\b`
    );
    const match = opPattern.exec(stripped);
    if (match) return match[1] === "mutation";
    // Operation name didn't match any header — fall through to the broad
    // heuristic so an ambiguous doc still errs on the safe side.
  }
  return /\bmutation\b\s*\w*\s*[({]/.test(stripped);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function defineRunGraphQL(client: CatalogClient): CatalogToolDefinition {
  return {
    name: "catalog_run_graphql",
    config: {
      title: "Run Raw GraphQL Query",
      description:
        "Execute an arbitrary GraphQL document against the Catalog Public API for debugging or for questions not covered by the structured MCP tools. **Prefer the structured tools (catalog_summarize_asset, catalog_get_field_lineages, catalog_assess_impact, etc.) whenever they fit — this is the escape hatch, not the default path.** Use when: answering API-contract questions, validating a query before a customer runs it, reproducing a customer's failing query, or exploring a response shape the structured tools don't expose.\n\n" +
        "Returns the raw GraphQL response envelope unchanged: `{ data?, errors?, extensions? }`. Validation errors and execution errors are preserved verbatim (not re-mapped to tool errors) so the agent can reason about schema mistakes directly. On network or HTTP failure the tool returns a structured `{ error }` result instead.\n\n" +
        "Mutation guardrail: mutations are rejected by default. Pass `allowMutations: true` to opt in — required for upsert/delete operations. Detection is a keyword heuristic (`mutation` at the top of the document), so false positives are possible; re-submit with the override if you're sure the document is safe.",
      inputSchema: RunGraphQLInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const query = args.query as string;
      const variables = args.variables as Record<string, unknown> | undefined;
      const operationName = args.operationName as string | undefined;
      const allowMutations = args.allowMutations === true;
      const timeoutMs = typeof args.timeoutMs === "number" ? args.timeoutMs : undefined;

      // Read-only mode drops all mutation tools at registration; the
      // escape-hatch tool survives because it's read-by-default, but its
      // opt-in mutation flag would otherwise be a bypass. Force mutations
      // off when the server is configured read-only.
      if (allowMutations && isReadOnlyMode()) {
        return {
          error:
            "allowMutations rejected: server is running in read-only mode (COALESCE_CATALOG_READ_ONLY=true). " +
            "Unset the env var or use a READ_WRITE token + read-write server to execute mutations.",
          blocked: "read_only_mode",
        };
      }

      if (!allowMutations && looksLikeMutation(query, operationName)) {
        return {
          error:
            "Mutation blocked: document appears to contain a mutation operation. Re-submit with `allowMutations: true` to execute. " +
            "(Detection is a keyword heuristic — pass the flag if you believe this is a false positive.)",
          blocked: "mutation",
        };
      }

      // executeRaw returns the full GraphQL envelope (data, errors, extensions)
      // without throwing on `errors[]` — that's the whole point of an
      // escape-hatch tool, since the agent needs to see validation/execution
      // errors verbatim. Only transport-level failures (HTTP 4xx/5xx, auth)
      // propagate as errors via withErrorHandling.
      //
      // Pass `variables` through as-is (including the undefined case) so a
      // strict server that rejects `variables: {}` on variable-less documents
      // sees exactly what the caller sent.
      const execOptions: { operationName?: string; timeoutMs?: number } = {};
      if (operationName) execOptions.operationName = operationName;
      if (timeoutMs !== undefined) execOptions.timeoutMs = timeoutMs;
      const envelope = await c.executeRaw<Record<string, unknown>>(
        query,
        variables,
        Object.keys(execOptions).length > 0 ? execOptions : undefined
      );
      const out: Record<string, unknown> = {};
      if (envelope.data !== undefined) out.data = envelope.data;
      if (envelope.errors) out.errors = envelope.errors;
      if (envelope.extensions) out.extensions = envelope.extensions;
      return out;
    }, client),
  };
}

// ── Tool factory ────────────────────────────────────────────────────────────

export function defineIntrospectionTools(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [defineDescribeType(client), defineRunGraphQL(client)];
}
