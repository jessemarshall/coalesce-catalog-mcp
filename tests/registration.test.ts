import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { createClient } from "../src/client.js";
import { defineTableTools } from "../src/mcp/tables.js";
import { defineLineageTools } from "../src/mcp/lineage.js";
import { defineColumnTools } from "../src/mcp/columns.js";
import { defineDashboardTools } from "../src/mcp/dashboards.js";
import { defineDiscoveryTools } from "../src/mcp/discovery.js";
import { defineAnnotationTools } from "../src/mcp/annotations.js";
import { defineGovernanceTools } from "../src/mcp/governance.js";
import { defineAiTools } from "../src/mcp/ai.js";
import { defineFindAssetByPath } from "../src/workflows/find-asset-by-path.js";
import { defineSummarizeAsset } from "../src/workflows/summarize-asset.js";
import { defineTraceMissingLineage } from "../src/workflows/trace-missing-lineage.js";

const client = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

function allDefinitions() {
  return [
    ...defineTableTools(client),
    ...defineLineageTools(client),
    ...defineColumnTools(client),
    ...defineDashboardTools(client),
    ...defineDiscoveryTools(client),
    ...defineAnnotationTools(client),
    ...defineGovernanceTools(client),
    ...defineAiTools(client),
    defineFindAssetByPath(client),
    defineSummarizeAsset(client),
    defineTraceMissingLineage(client),
  ];
}

describe("tool registration", () => {
  it("registers 52 tools across all domains", () => {
    expect(allDefinitions()).toHaveLength(52);
  });

  it("every tool name starts with 'catalog_'", () => {
    for (const def of allDefinitions()) {
      expect(def.name).toMatch(/^catalog_/);
    }
  });

  it("tool names are unique", () => {
    const names = allDefinitions().map((d) => d.name);
    const unique = new Set(names);
    expect(unique.size).toBe(names.length);
  });

  it("every tool has a title", () => {
    for (const def of allDefinitions()) {
      expect(def.config.title).toBeTruthy();
      expect(typeof def.config.title).toBe("string");
    }
  });

  it("every tool has a non-empty description", () => {
    for (const def of allDefinitions()) {
      expect(def.config.description.length).toBeGreaterThan(20);
    }
  });

  it("every tool declares annotations", () => {
    for (const def of allDefinitions()) {
      expect(def.config.annotations).toBeDefined();
      expect(
        typeof def.config.annotations?.readOnlyHint === "boolean"
      ).toBe(true);
    }
  });

  it("splits roughly 29 read / 23 write", () => {
    const reads = allDefinitions().filter(
      (d) => d.config.annotations?.readOnlyHint === true
    );
    const writes = allDefinitions().filter(
      (d) => d.config.annotations?.readOnlyHint !== true
    );
    expect(reads).toHaveLength(29);
    expect(writes).toHaveLength(23);
  });

  it("destructive writes have destructiveHint=true", () => {
    const destructive = allDefinitions().filter(
      (d) => d.config.annotations?.destructiveHint === true
    );
    // detach_tags, delete_lineages, delete_external_links,
    // delete_term, remove_data_qualities, remove_user_owners,
    // remove_team_owners, remove_team_users, remove_pinned_assets
    expect(destructive.length).toBeGreaterThanOrEqual(9);
    for (const def of destructive) {
      expect(def.config.annotations?.readOnlyHint).not.toBe(true);
    }
  });
});
