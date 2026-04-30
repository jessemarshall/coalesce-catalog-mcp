import { describe, it, expect } from "vitest";
import { registerCatalogPrompts } from "../../src/prompts/index.js";
import { createClient } from "../../src/client.js";
import { buildAllToolDefinitions } from "../../src/server.js";

interface CapturedPrompt {
  name: string;
  metadata: { title?: string; description?: string };
  handler: () => Promise<{
    messages: Array<{ role: string; content: { type: string; text: string } }>;
  }>;
}

/**
 * Capture every prompt registered against a fake McpServer. Lets us inspect
 * the title/description metadata and the rendered message body without
 * spinning up the full server pipeline. Mirrors the registration-coverage
 * test convention of asserting against the same registration call the
 * production server makes.
 */
function captureRegisteredPrompts(): CapturedPrompt[] {
  const captured: CapturedPrompt[] = [];
  const fakeServer = {
    registerPrompt: (
      name: string,
      metadata: { title?: string; description?: string },
      handler: CapturedPrompt["handler"]
    ): void => {
      captured.push({ name, metadata, handler });
    },
    // Other server methods are not invoked from registerCatalogPrompts.
  };
  // Cast through unknown — we only stub the surface registerCatalogPrompts
  // actually uses, not the full McpServer.
  registerCatalogPrompts(fakeServer as unknown as Parameters<typeof registerCatalogPrompts>[0]);
  return captured;
}

const EXPECTED_PROMPT_NAMES = [
  "catalog-start-here",
  "catalog-asset-summary",
  "catalog-find-consumers",
  "catalog-investigate-lineage-gaps",
  "catalog-governance-rollout",
  "catalog-daily-guide",
  "catalog-audit-documentation",
];

const KNOWN_TOOL_NAMES: Set<string> = (() => {
  const client = createClient({
    apiKey: "dummy",
    region: "eu",
    endpoint: "https://example.invalid/public/graphql",
  });
  return new Set(buildAllToolDefinitions(client).map((d) => d.name));
})();

describe("registerCatalogPrompts — registration shape", () => {
  it("registers exactly the 7 expected prompts", () => {
    const prompts = captureRegisteredPrompts();
    expect(prompts).toHaveLength(EXPECTED_PROMPT_NAMES.length);
    const names = prompts.map((p) => p.name).sort();
    expect(names).toEqual([...EXPECTED_PROMPT_NAMES].sort());
  });

  it("every prompt declares a non-empty title", () => {
    for (const p of captureRegisteredPrompts()) {
      expect(p.metadata.title, `prompt ${p.name} missing title`).toBeTruthy();
      expect(typeof p.metadata.title).toBe("string");
      expect((p.metadata.title as string).length).toBeGreaterThan(0);
    }
  });

  it("every prompt declares a non-empty description", () => {
    for (const p of captureRegisteredPrompts()) {
      expect(
        p.metadata.description,
        `prompt ${p.name} missing description`
      ).toBeTruthy();
      expect((p.metadata.description as string).length).toBeGreaterThan(20);
    }
  });

  it("prompt names are unique (no double-registration)", () => {
    const names = captureRegisteredPrompts().map((p) => p.name);
    const unique = new Set(names);
    expect(unique.size).toBe(names.length);
  });
});

describe("registerCatalogPrompts — message body", () => {
  it("every prompt yields exactly one user-role text message when invoked", async () => {
    for (const p of captureRegisteredPrompts()) {
      const result = await p.handler();
      expect(result.messages).toHaveLength(1);
      const msg = result.messages[0];
      expect(msg.role).toBe("user");
      expect(msg.content.type).toBe("text");
      expect(msg.content.text.length).toBeGreaterThan(20);
    }
  });

  it("every catalog_* tool name referenced in a prompt body is a real registered tool", async () => {
    // Catches the regression where a prompt references a tool name that
    // doesn't exist (typo, rename without prompt update). This is exactly
    // the failure mode that wouldn't surface until a user runs the prompt
    // and gets a "tool not found" error from their client.
    //
    // The terminating `[a-z]` (not `[a-z_]`) excludes wildcards like
    // `catalog_get_*` from matching as `catalog_get_` — those are
    // intentional documentation, not tool references.
    const toolMentionRegex = /\bcatalog_(?:[a-z_]*[a-z])\b/g;
    const violations: Array<{ prompt: string; mention: string }> = [];
    for (const p of captureRegisteredPrompts()) {
      const result = await p.handler();
      const body = result.messages[0].content.text;
      const mentions = body.match(toolMentionRegex) ?? [];
      for (const mention of mentions) {
        if (!KNOWN_TOOL_NAMES.has(mention)) {
          violations.push({ prompt: p.name, mention });
        }
      }
    }
    expect(
      violations,
      `prompts referenced unknown tool names: ${JSON.stringify(violations)}`
    ).toEqual([]);
  });

  it("catalog-start-here points the agent at the routing context resources", async () => {
    const prompts = captureRegisteredPrompts();
    const startHere = prompts.find((p) => p.name === "catalog-start-here");
    expect(startHere).toBeDefined();
    const body = (await startHere!.handler()).messages[0].content.text;
    expect(body).toContain("catalog://context/overview");
    expect(body).toContain("catalog://context/tool-routing");
  });

  it("catalog-asset-summary references find_asset_by_path then summarize_asset", async () => {
    const prompts = captureRegisteredPrompts();
    const summary = prompts.find((p) => p.name === "catalog-asset-summary");
    expect(summary).toBeDefined();
    const body = (await summary!.handler()).messages[0].content.text;
    expect(body).toContain("catalog_find_asset_by_path");
    expect(body).toContain("catalog_summarize_asset");
    // Order matters: resolve the path first, then summarise. If a refactor
    // ever reverses the prose, this catches it.
    expect(body.indexOf("catalog_find_asset_by_path")).toBeLessThan(
      body.indexOf("catalog_summarize_asset")
    );
  });

  it("catalog-find-consumers references the lineage tools and the queries tool", async () => {
    const prompts = captureRegisteredPrompts();
    const consumers = prompts.find((p) => p.name === "catalog-find-consumers");
    expect(consumers).toBeDefined();
    const body = (await consumers!.handler()).messages[0].content.text;
    expect(body).toContain("catalog_get_lineages");
    expect(body).toContain("catalog_get_table_queries");
  });

  it("catalog-investigate-lineage-gaps points at trace_missing_lineage", async () => {
    const prompts = captureRegisteredPrompts();
    const investigate = prompts.find(
      (p) => p.name === "catalog-investigate-lineage-gaps"
    );
    expect(investigate).toBeDefined();
    const body = (await investigate!.handler()).messages[0].content.text;
    expect(body).toContain("catalog_trace_missing_lineage");
    // Mutation guidance must require explicit approval before catalog_upsert_lineages.
    expect(body).toMatch(/approval/i);
    expect(body).toContain("catalog_upsert_lineages");
  });

  it("catalog-governance-rollout reads the playbook resource and lists opening tool calls", async () => {
    const prompts = captureRegisteredPrompts();
    const rollout = prompts.find(
      (p) => p.name === "catalog-governance-rollout"
    );
    expect(rollout).toBeDefined();
    const body = (await rollout!.handler()).messages[0].content.text;
    expect(body).toContain("catalog://context/governance-rollout");
    expect(body).toContain("catalog_search_sources");
    expect(body).toContain("catalog_search_databases");
    expect(body).toContain("catalog_search_tables");
    expect(body).toContain("catalog_summarize_asset");
  });

  it("catalog-daily-guide references owner_scorecard and surfaces all the finding-bucket keys", async () => {
    const prompts = captureRegisteredPrompts();
    const daily = prompts.find((p) => p.name === "catalog-daily-guide");
    expect(daily).toBeDefined();
    const body = (await daily!.handler()).messages[0].content.text;
    expect(body).toContain("catalog_owner_scorecard");
    // Each finding bucket the prompt promises to render must appear by name.
    // Drift in either prompts/index.ts or owner-scorecard.ts's finding keys
    // would otherwise produce an instruction the agent can't honour.
    for (const key of [
      "lineage_isolated_ids",
      "missing_owner_ids",
      "uncertified_ids",
      "new_asset_ids",
      "thin_description_ids",
      "no_domain_tag_ids",
      "pii_tagged_ids",
      "unclassified_owned_ids",
    ]) {
      expect(body, `daily-guide missing finding key ${key}`).toContain(key);
    }
  });

  it("catalog-audit-documentation steers to search_columns with the isDocumented filter", async () => {
    const prompts = captureRegisteredPrompts();
    const audit = prompts.find(
      (p) => p.name === "catalog-audit-documentation"
    );
    expect(audit).toBeDefined();
    const body = (await audit!.handler()).messages[0].content.text;
    expect(body).toContain("catalog_search_columns");
    expect(body).toContain("isDocumented");
    expect(body).toContain("catalog_search_tables");
  });
});
