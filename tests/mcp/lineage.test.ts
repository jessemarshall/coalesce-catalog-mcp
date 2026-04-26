import { describe, it, expect } from "vitest";
import { createClient } from "../../src/client.js";
import { defineLineageTools } from "../../src/mcp/lineage.js";

const client = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

const upsert = defineLineageTools(client).find(
  (t) => t.name === "catalog_upsert_lineages"
)!;

// The upsert_lineages input shape: { data: [{parent{Table,Dashboard}Id,
// child{Table,Dashboard}Id}] }. Zod refines each row so exactly one parent
// and exactly one child must be present.
import { z } from "zod";

const rowSchema = (upsert.config.inputSchema as {
  data: z.ZodArray<z.ZodTypeAny>;
}).data.element;

describe("upsert_lineages row refine (exactly-one-parent × exactly-one-child)", () => {
  it("accepts table→table edge", () => {
    expect(() =>
      rowSchema.parse({ parentTableId: "p", childTableId: "c" })
    ).not.toThrow();
  });

  it("accepts table→dashboard edge", () => {
    expect(() =>
      rowSchema.parse({ parentTableId: "p", childDashboardId: "c" })
    ).not.toThrow();
  });

  it("accepts dashboard→table edge", () => {
    expect(() =>
      rowSchema.parse({ parentDashboardId: "p", childTableId: "c" })
    ).not.toThrow();
  });

  it("accepts dashboard→dashboard edge", () => {
    expect(() =>
      rowSchema.parse({ parentDashboardId: "p", childDashboardId: "c" })
    ).not.toThrow();
  });

  it("rejects an edge with both parents set", () => {
    expect(() =>
      rowSchema.parse({
        parentTableId: "p1",
        parentDashboardId: "p2",
        childTableId: "c",
      })
    ).toThrow();
  });

  it("rejects an edge with both children set", () => {
    expect(() =>
      rowSchema.parse({
        parentTableId: "p",
        childTableId: "c1",
        childDashboardId: "c2",
      })
    ).toThrow();
  });

  it("rejects an edge missing a parent", () => {
    expect(() => rowSchema.parse({ childTableId: "c" })).toThrow();
  });

  it("rejects an edge missing a child", () => {
    expect(() => rowSchema.parse({ parentTableId: "p" })).toThrow();
  });

  it("rejects an empty object", () => {
    expect(() => rowSchema.parse({})).toThrow();
  });
});

describe("catalog_get_lineages withChildAssetType narrowing", () => {
  const getLineages = defineLineageTools(client).find(
    (t) => t.name === "catalog_get_lineages"
  )!;
  const inputSchema = z.object(
    getLineages.config.inputSchema as Record<string, z.ZodTypeAny>
  );

  it("accepts TABLE", () => {
    expect(() =>
      inputSchema.parse({ withChildAssetType: "TABLE" })
    ).not.toThrow();
  });

  it("accepts DASHBOARD", () => {
    expect(() =>
      inputSchema.parse({ withChildAssetType: "DASHBOARD" })
    ).not.toThrow();
  });

  it("rejects COLUMN (use catalog_get_field_lineages)", () => {
    expect(() =>
      inputSchema.parse({ withChildAssetType: "COLUMN" })
    ).toThrow();
  });

  it("rejects DASHBOARD_FIELD (use catalog_get_field_lineages)", () => {
    expect(() =>
      inputSchema.parse({ withChildAssetType: "DASHBOARD_FIELD" })
    ).toThrow();
  });
});
