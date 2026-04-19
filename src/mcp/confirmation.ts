import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  errorResult,
  type ToolHandlerExtra,
} from "../catalog/types.js";
import { SKIP_CONFIRMATIONS_ENV_VAR } from "../constants.js";
import { ToolEarlyReturn } from "./tool-helpers.js";

/**
 * Subset of MCP's ElicitResult schema that we need to validate when calling
 * the client back through `extra.sendRequest`. We intentionally don't import
 * the SDK's full ElicitResultSchema — its zod shape is awkwardly tied to
 * generated unions, and our minimal schema accepts the same wire payload.
 */
const ElicitResponseSchema = z.object({
  action: z.enum(["accept", "decline", "cancel"]),
  content: z
    .record(z.string(), z.union([z.string(), z.number(), z.boolean()]))
    .optional(),
});

export interface ConfirmationOptions<TArgs extends Record<string, unknown>> {
  /** Short label for the action — shown in the dialog header. */
  action: string;
  /**
   * Returns a one-line human summary of the impact (e.g. "Delete 12 lineage
   * edges"). The model already knows the args; the summary is for the user
   * who will click accept/decline.
   */
  summarize: (args: TArgs) => string;
}

/**
 * Wrap a destructive handler so it requests explicit user confirmation via
 * MCP elicitation before executing. Behaviour:
 *
 *   1. If `COALESCE_CATALOG_SKIP_CONFIRMATIONS=true`, the wrapper is a no-op
 *      (suitable for CI / scripted runs where the caller has pre-approved).
 *   2. Otherwise we send an `elicitation/create` request through the active
 *      transport. On `accept` we proceed; on `decline` / `cancel` we return
 *      a non-error tool result that explains nothing was changed.
 *   3. If `extra.sendRequest` is missing (legacy clients) or the elicitation
 *      itself errors (client doesn't implement the method), we fail closed:
 *      the destructive call does not run, and the user sees a message
 *      pointing at the env-var bypass.
 *
 * Fail-closed is the safe default for irreversible mutations. Callers who
 * want autonomy must opt out explicitly.
 */
export function withConfirmation<TArgs extends Record<string, unknown>>(
  options: ConfirmationOptions<TArgs>,
  impl: (
    args: TArgs,
    client: CatalogClient,
    extra?: ToolHandlerExtra
  ) => Promise<unknown>
): (
  args: Record<string, unknown>,
  client: CatalogClient,
  extra?: ToolHandlerExtra
) => Promise<unknown> {
  return async (rawArgs, client, extra) => {
    // Tool input has already been validated against the zod schema by the SDK
    // before reaching the handler — same trust-the-framework pattern used by
    // the rest of the codebase.
    const args = rawArgs as TArgs;
    if (process.env[SKIP_CONFIRMATIONS_ENV_VAR] === "true") {
      return impl(args, client, extra);
    }

    if (!extra?.sendRequest) {
      throw new ToolEarlyReturn(
        errorResult(
          `Destructive action "${options.action}" requires interactive confirmation, ` +
            `but the active MCP transport does not expose a request channel. ` +
            `Set ${SKIP_CONFIRMATIONS_ENV_VAR}=true to bypass (only safe for vetted, non-interactive callers).`,
          { kind: "confirmation_unavailable" }
        )
      );
    }

    const summary = options.summarize(args);
    let response: z.infer<typeof ElicitResponseSchema>;
    try {
      const raw = await extra.sendRequest(
        {
          method: "elicitation/create",
          params: {
            mode: "form",
            message: `${options.action}: ${summary}\n\nThis action is irreversible. Confirm to proceed.`,
            requestedSchema: {
              type: "object",
              properties: {
                confirm: {
                  type: "boolean",
                  title: "Confirm",
                  description: "Required to proceed.",
                },
              },
              required: ["confirm"],
            },
          },
        },
        ElicitResponseSchema
      );
      response = ElicitResponseSchema.parse(raw);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new ToolEarlyReturn(
        errorResult(
          `Could not obtain user confirmation for "${options.action}". The client likely does not ` +
            `support the MCP elicitation protocol. Set ${SKIP_CONFIRMATIONS_ENV_VAR}=true to ` +
            `bypass (only safe for vetted, non-interactive callers).`,
          { kind: "elicitation_failed", detail: message }
        )
      );
    }

    if (response.action !== "accept" || response.content?.confirm !== true) {
      throw new ToolEarlyReturn(
        errorResult(
          `User did not confirm "${options.action}" — no changes were made.`,
          { kind: "user_declined", action: response.action }
        )
      );
    }

    return impl(args, client, extra);
  };
}
