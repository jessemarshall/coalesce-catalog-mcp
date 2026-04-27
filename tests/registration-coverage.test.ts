import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { createClient } from "../src/client.js";
import { buildAllToolDefinitions } from "../src/server.js";
import { makeMockClient } from "./helpers/mock-client.js";
import { SKIP_CONFIRMATIONS_ENV_VAR } from "../src/constants.js";

// Two structural assertions over the registered tool list:
//
// 1. README docs coverage — every tool registered by buildAllToolDefinitions
//    appears in README.md as a `catalog_<name>` token. Catches the doc-drift
//    class permanently: shipping a tool now requires updating README,
//    otherwise this fails CI.
//
// 2. Error-envelope contract — every tool routes thrown errors through
//    withErrorHandling so the agent sees `{ isError: true }` instead of a
//    protocol-level failure. Catches a missing wrapper on any new tool.

const dummyClient = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

describe("README docs coverage", () => {
  const readmeUrl = new URL("../README.md", import.meta.url);
  const readme = readFileSync(fileURLToPath(readmeUrl), "utf8");
  const definitions = buildAllToolDefinitions(dummyClient);

  it.each(definitions.map((d) => [d.name]))(
    "%s appears in README.md as `catalog_<name>`",
    (name) => {
      const token = `\`${name}\``;
      expect(readme).toContain(token);
    }
  );
});

describe("error envelope contract", () => {
  // Build tools wired to a client that throws on every call. The contract
  // every tool must satisfy: thrown errors from c.execute / c.executeRaw
  // are caught and surfaced as a structured ToolResult — never propagated
  // as a rejected promise that crashes the MCP protocol layer.
  //
  // Most tools surface this via withErrorHandling → { isError: true }.
  // Workflow tools that compose multiple sub-calls (e.g. catalog_summarize_asset
  // with its Promise.allSettled fan-out) surface sub-call failures as
  // structured non-error fields like `{ error: ..., detail: ... }`. Both
  // shapes are valid; the regression we're catching is "handler reaches
  // production with no error containment at all" — i.e. a rejected promise.
  const throwingClient = makeMockClient(() => {
    throw new Error("simulated transport failure");
  });
  const definitions = buildAllToolDefinitions(throwingClient);

  // Bypass elicitation for destructive tools — the elicitation path is
  // covered separately in confirmation.test.ts. Here we only exercise the
  // post-confirm error path.
  let savedSkip: string | undefined;
  beforeAll(() => {
    savedSkip = process.env[SKIP_CONFIRMATIONS_ENV_VAR];
    process.env[SKIP_CONFIRMATIONS_ENV_VAR] = "true";
  });
  afterAll(() => {
    if (savedSkip === undefined) delete process.env[SKIP_CONFIRMATIONS_ENV_VAR];
    else process.env[SKIP_CONFIRMATIONS_ENV_VAR] = savedSkip;
  });

  it.each(definitions.map((d) => [d.name, d]))(
    "%s contains the underlying call failure (no unhandled rejection)",
    async (_name, def) => {
      // Empty args — the throw originates at the first c.execute /
      // c.executeRaw (or sooner, on arg-coercion). The handler must catch it
      // and return a valid ToolResult. If the wrapper is missing, the await
      // rejects and this test fails.
      const res = await def.handler({}, undefined);
      expect(res).toBeDefined();
      expect(Array.isArray(res.content)).toBe(true);
      expect(res.content[0]?.type).toBe("text");
    }
  );
});
