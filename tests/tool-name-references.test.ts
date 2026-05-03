import { describe, it, expect } from "vitest";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { createClient } from "../src/client.js";
import { buildAllToolDefinitions } from "../src/server.js";

// Catches the regression where a tool description, a workflow handler's
// emitted `gaps` text, or any other string literal in `src/` references a
// `catalog_*` tool name that does not exist (typo, rename without follow-up).
//
// This sits alongside the prompts/index.test.ts tool-mention check, which
// only validates prompt bodies. A typo in a workflow's gap text — e.g.
// "use catalog_update_columns_metadata to describe them" — would slip past
// that test entirely; the agent would only discover the broken reference at
// runtime when its tool call fails.

const __filename = fileURLToPath(import.meta.url);
const SRC_ROOT = join(dirname(__filename), "..", "src");

function walkTsFiles(dir: string): string[] {
  const out: string[] = [];
  for (const name of readdirSync(dir)) {
    // Skip generated GraphQL types — they pull operation names like
    // "catalog_get_table_queries" inadvertently into doc comments and aren't
    // a meaningful surface for tool-mention checks.
    if (name === "generated") continue;
    const abs = join(dir, name);
    const stat = statSync(abs);
    if (stat.isDirectory()) {
      out.push(...walkTsFiles(abs));
      continue;
    }
    if (stat.isFile() && abs.endsWith(".ts")) out.push(abs);
  }
  return out;
}

const dummyClient = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

const KNOWN_TOOL_NAMES: Set<string> = new Set(
  buildAllToolDefinitions(dummyClient).map((d) => d.name)
);

// `catalog_get_` (no trailing letter) matches wildcard usages like
// `catalog_get_*` in prose; we only flag tokens ending in [a-z], which forces
// a complete identifier and excludes the wildcard placeholder.
const TOOL_MENTION = /\bcatalog_(?:[a-z_]*[a-z])\b/g;

describe("tool-name references in src/", () => {
  const files = walkTsFiles(SRC_ROOT);

  it("every catalog_* token in a .ts source file resolves to a real registered tool", () => {
    const violations: Array<{ file: string; mention: string }> = [];
    for (const file of files) {
      const body = readFileSync(file, "utf8");
      const mentions = body.match(TOOL_MENTION) ?? [];
      for (const mention of mentions) {
        if (!KNOWN_TOOL_NAMES.has(mention)) {
          violations.push({
            file: file.slice(SRC_ROOT.length + 1),
            mention,
          });
        }
      }
    }
    expect(
      violations,
      `unknown catalog_* tool name(s) referenced in src/: ${JSON.stringify(violations)}`
    ).toEqual([]);
  });
});

describe("tool-name references in static context resources", () => {
  // Catches drift in catalog://context/* markdown — same regression class as
  // the source-file check above, but the markdown is loaded at runtime by
  // ReadResource so a typo there only surfaces when an agent actually fetches
  // the resource and tries to call the suggested tool.
  const RESOURCE_DIR = join(SRC_ROOT, "resources", "context");
  const files = readdirSync(RESOURCE_DIR)
    .filter((n) => n.endsWith(".md"))
    .map((n) => join(RESOURCE_DIR, n));

  it("every catalog_* token in a context .md resolves to a real registered tool", () => {
    const violations: Array<{ file: string; mention: string }> = [];
    for (const file of files) {
      const body = readFileSync(file, "utf8");
      const mentions = body.match(TOOL_MENTION) ?? [];
      for (const mention of mentions) {
        if (!KNOWN_TOOL_NAMES.has(mention)) {
          violations.push({
            file: file.slice(SRC_ROOT.length + 1),
            mention,
          });
        }
      }
    }
    expect(
      violations,
      `unknown catalog_* tool name(s) referenced in context resources: ${JSON.stringify(violations)}`
    ).toEqual([]);
  });
});

describe("tool-name references in top-level docs", () => {
  // Same regression class as the in-tree scans, but for human-facing docs
  // (README.md, docs/prerelease.md, CLAUDE.md). README has 60+ catalog_*
  // mentions and is the first thing a new contributor reads — a stale rename
  // here misleads readers without a CI signal until the next typo PR.
  const REPO_ROOT = join(SRC_ROOT, "..");
  // CLAUDE.md is intentionally gitignored — it's a per-clone symlink into the
  // sibling mcp-smithy repo (see commit 840f0d0). The file is absent in CI
  // checkouts, so include it only when it actually exists. Filtering keeps the
  // scan opportunistic: it locks tool-name accuracy locally where the symlink
  // resolves, and a missing CLAUDE.md never fails CI.
  const files = [
    join(REPO_ROOT, "README.md"),
    join(REPO_ROOT, "CLAUDE.md"),
    join(REPO_ROOT, "docs", "prerelease.md"),
  ].filter((f) => existsSync(f));

  it("every catalog_* token in a top-level doc resolves to a real registered tool", () => {
    const violations: Array<{ file: string; mention: string }> = [];
    for (const file of files) {
      const body = readFileSync(file, "utf8");
      const mentions = body.match(TOOL_MENTION) ?? [];
      for (const mention of mentions) {
        if (!KNOWN_TOOL_NAMES.has(mention)) {
          violations.push({
            file: file.slice(REPO_ROOT.length + 1),
            mention,
          });
        }
      }
    }
    expect(
      violations,
      `unknown catalog_* tool name(s) referenced in top-level docs: ${JSON.stringify(violations)}`
    ).toEqual([]);
  });
});
