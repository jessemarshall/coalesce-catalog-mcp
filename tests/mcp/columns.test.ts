import { describe, it, expect } from "vitest";
import { createClient } from "../../src/client.js";
import { defineColumnTools } from "../../src/mcp/columns.js";

const client = createClient({
  apiKey: "dummy",
  region: "eu",
  endpoint: "https://example.invalid/public/graphql",
});

const searchColumns = defineColumnTools(client).find(
  (t) => t.name === "catalog_search_columns"
)!;

// The predicate-only guard throws before any network call, so we can invoke
// the handler directly with a fake client — the thrown error is converted to
// an isError payload by the withErrorHandling wrapper.

async function call(args: Record<string, unknown>) {
  return searchColumns.handler(args);
}

describe("catalog_search_columns predicate-only guard", () => {
  it("rejects isDocumented alone", async () => {
    const res = await call({ isDocumented: false, nbPerPage: 10 });
    expect(res.isError).toBe(true);
    expect(res.content[0].text).toMatch(/Unscoped predicate-only/);
    expect(res.content[0].text).toMatch(/tableId/);
  });

  it("rejects isPii alone", async () => {
    const res = await call({ isPii: true });
    expect(res.isError).toBe(true);
  });

  it("rejects isPrimaryKey alone", async () => {
    const res = await call({ isPrimaryKey: true });
    expect(res.isError).toBe(true);
  });

  it("rejects hasColumnJoins alone", async () => {
    const res = await call({ hasColumnJoins: true });
    expect(res.isError).toBe(true);
  });

  it("rejects a combination of predicates with no scope", async () => {
    const res = await call({ isPii: true, isDocumented: false });
    expect(res.isError).toBe(true);
  });

  it("accepts isDocumented + tableId (attempts the call, fails at transport layer with the fake endpoint)", async () => {
    const res = await call({
      isDocumented: false,
      tableId: "abc-123",
      nbPerPage: 2,
    });
    // Fake endpoint → fetch fails. Key assertion: the error reason is NOT
    // the predicate-guard message — it got through the guard to the client.
    expect(res.isError).toBe(true);
    expect(res.content[0].text).not.toMatch(/Unscoped predicate-only/);
  });

  it("accepts isDocumented + schemaId", async () => {
    const res = await call({
      isDocumented: false,
      schemaId: "abc-123",
    });
    expect(res.content[0].text).not.toMatch(/Unscoped predicate-only/);
  });

  it("accepts isDocumented + nameContains", async () => {
    const res = await call({
      isDocumented: false,
      nameContains: "order",
    });
    expect(res.content[0].text).not.toMatch(/Unscoped predicate-only/);
  });

  it("accepts no predicates with no scope (unscoped but predicate-free)", async () => {
    // This is fine — the API is indexed on the baseline list; predicate-
    // only filters are the ones that tank without scope.
    const res = await call({ nbPerPage: 2 });
    expect(res.content[0].text).not.toMatch(/Unscoped predicate-only/);
  });
});
