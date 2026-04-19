import type { z } from "zod";

/**
 * Subset of the SDK's RequestHandlerExtra surface we actually use, kept narrow
 * so handlers stay testable without spinning up a real Server.
 */
export interface ToolHandlerExtra {
  sendRequest?: (
    request: { method: string; params?: unknown },
    resultSchema: unknown,
    options?: unknown
  ) => Promise<unknown>;
  signal?: AbortSignal;
  [key: string]: unknown;
}

/**
 * Declarative tool definition consumed by server.ts. Shape matches the
 * McpServer.registerTool(name, config, handler) signature.
 *
 * Handler's params are explicitly typed `unknown` (not the tuple element 0
 * default) because McpServer passes the validated input as the first arg.
 * Callers cast/parse before use. The optional `extra` is forwarded by the
 * SDK and used by wrappers like withConfirmation to call elicitation.
 */
export interface CatalogToolDefinition {
  name: string;
  config: {
    title?: string;
    description: string;
    inputSchema: z.ZodRawShape;
    annotations?: ToolAnnotations;
  };
  handler: (
    args: Record<string, unknown>,
    extra?: ToolHandlerExtra
  ) => Promise<ToolResult>;
}

export interface ToolAnnotations {
  readOnlyHint?: boolean;
  destructiveHint?: boolean;
  idempotentHint?: boolean;
  openWorldHint?: boolean;
}

export interface ToolResult {
  content: Array<{ type: "text"; text: string }>;
  isError?: boolean;
  structuredContent?: Record<string, unknown>;
}

// ── Shared annotation presets ────────────────────────────────────────────────

export const READ_ONLY_ANNOTATIONS = {
  readOnlyHint: true,
  idempotentHint: true,
  destructiveHint: false,
  openWorldHint: true,
} as const satisfies ToolAnnotations;

export const WRITE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: false,
  openWorldHint: true,
} as const satisfies ToolAnnotations;

export const DESTRUCTIVE_ANNOTATIONS = {
  readOnlyHint: false,
  idempotentHint: false,
  destructiveHint: true,
  openWorldHint: true,
} as const satisfies ToolAnnotations;

// ── Shared response helpers ─────────────────────────────────────────────────

export function textResult(payload: unknown): ToolResult {
  return {
    content: [{ type: "text", text: JSON.stringify(payload, null, 2) }],
  };
}

export function errorResult(message: string, detail?: unknown): ToolResult {
  const payload = detail === undefined ? { error: message } : { error: message, detail };
  return {
    content: [{ type: "text", text: JSON.stringify(payload, null, 2) }],
    isError: true,
  };
}
