import { describe, it, expect } from "vitest";
import { z } from "zod";
import { createClient } from "../src/client.js";
import { defineTableTools } from "../src/mcp/tables.js";
import { defineColumnTools } from "../src/mcp/columns.js";
import { defineAnnotationTools } from "../src/mcp/annotations.js";
import { defineAiTools } from "../src/mcp/ai.js";

const client = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

function find<T extends { name: string; config: { inputSchema: z.ZodRawShape } }>(
  defs: T[],
  name: string
): T {
  const match = defs.find((d) => d.name === name);
  if (!match) throw new Error(`tool ${name} not registered`);
  return match;
}

describe("catalog_get_table input schema", () => {
  const def = find(defineTableTools(client), "catalog_get_table");
  const schema = z.object(def.config.inputSchema);

  it("requires id", () => {
    expect(() => schema.parse({})).toThrow();
  });

  it("rejects empty id", () => {
    expect(() => schema.parse({ id: "" })).toThrow();
  });

  it("accepts a non-empty id", () => {
    expect(() => schema.parse({ id: "abc-123" })).not.toThrow();
  });
});

describe("catalog_get_table_queries input schema", () => {
  const def = find(defineTableTools(client), "catalog_get_table_queries");
  const schema = z.object(def.config.inputSchema);

  it("requires tableIds", () => {
    expect(() => schema.parse({})).toThrow();
  });

  it("rejects empty tableIds array", () => {
    expect(() => schema.parse({ tableIds: [] })).toThrow();
  });

  it("rejects more than 50 tableIds", () => {
    expect(() =>
      schema.parse({ tableIds: Array.from({ length: 51 }, (_, i) => String(i)) })
    ).toThrow();
  });

  it("accepts 1-50 tableIds", () => {
    expect(() => schema.parse({ tableIds: ["a"] })).not.toThrow();
    expect(() =>
      schema.parse({ tableIds: Array.from({ length: 50 }, (_, i) => String(i)) })
    ).not.toThrow();
  });
});

describe("catalog_update_column_metadata input schema", () => {
  const def = find(
    defineColumnTools(client),
    "catalog_update_column_metadata"
  );
  const schema = z.object(def.config.inputSchema);

  it("rejects empty data batch", () => {
    expect(() => schema.parse({ data: [] })).toThrow();
  });

  it("rejects >500 items", () => {
    expect(() =>
      schema.parse({
        data: Array.from({ length: 501 }, (_, i) => ({ id: String(i) })),
      })
    ).toThrow();
  });

  it("requires id on every row", () => {
    expect(() =>
      schema.parse({ data: [{ descriptionRaw: "x" }] })
    ).toThrow();
  });

  it("accepts a valid single-row batch", () => {
    expect(() =>
      schema.parse({
        data: [{ id: "col-uuid", descriptionRaw: "hello", isPii: true }],
      })
    ).not.toThrow();
  });
});

describe("catalog_attach_tags input schema", () => {
  const def = find(defineAnnotationTools(client), "catalog_attach_tags");
  const schema = z.object(def.config.inputSchema);

  it("requires entityType + entityId + label", () => {
    expect(() =>
      schema.parse({ data: [{ entityType: "TABLE" }] })
    ).toThrow();
    expect(() =>
      schema.parse({ data: [{ entityType: "TABLE", entityId: "x" }] })
    ).toThrow();
  });

  it("only accepts known entityType values", () => {
    expect(() =>
      schema.parse({
        data: [{ entityType: "USER", entityId: "x", label: "y" }],
      })
    ).toThrow();
  });

  it("accepts a valid row", () => {
    expect(() =>
      schema.parse({
        data: [{ entityType: "TABLE", entityId: "x", label: "y" }],
      })
    ).not.toThrow();
  });
});

describe("catalog_ask_assistant input schema", () => {
  const def = find(defineAiTools(client), "catalog_ask_assistant");
  const schema = z.object(def.config.inputSchema);

  it("requires email to be a valid email", () => {
    expect(() =>
      schema.parse({
        email: "not-an-email",
        externalConversationId: "c",
        question: "q",
      })
    ).toThrow();
  });

  it("enforces question length cap of 10000 chars", () => {
    expect(() =>
      schema.parse({
        email: "a@b.co",
        externalConversationId: "c",
        question: "x".repeat(10001),
      })
    ).toThrow();
  });

  it("accepts a valid payload", () => {
    expect(() =>
      schema.parse({
        email: "a@b.co",
        externalConversationId: "c",
        question: "what feeds orders?",
      })
    ).not.toThrow();
  });
});
