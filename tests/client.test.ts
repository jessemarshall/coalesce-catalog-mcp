import { describe, it, expect } from "vitest";
import {
  CatalogApiError,
  CatalogGraphQLError,
  createClient,
} from "../src/client.js";

const dummyConfig = {
  apiKey: "dummy",
  region: "eu" as const,
  endpoint: "https://example.invalid/public/graphql",
};

describe("CatalogApiError", () => {
  it("carries status + detail", () => {
    const err = new CatalogApiError("boom", 404, { extra: 1 });
    expect(err.name).toBe("CatalogApiError");
    expect(err.status).toBe(404);
    expect(err.detail).toEqual({ extra: 1 });
  });
});

describe("CatalogGraphQLError", () => {
  it("builds a message from the first error + count", () => {
    const err = new CatalogGraphQLError([
      { message: "first" },
      { message: "second" },
      { message: "third" },
    ]);
    expect(err.name).toBe("CatalogGraphQLError");
    expect(err.message).toMatch(/first/);
    expect(err.message).toMatch(/\+2 more/);
    expect(err.errors).toHaveLength(3);
  });

  it("handles single-error payloads", () => {
    const err = new CatalogGraphQLError([{ message: "only" }]);
    expect(err.message).toContain("only");
    expect(err.message).not.toMatch(/more/);
  });

  it("exposes partialData when present", () => {
    const err = new CatalogGraphQLError(
      [{ message: "x" }],
      { someData: true }
    );
    expect(err.partialData).toEqual({ someData: true });
  });
});

describe("createClient", () => {
  it("returns the endpoint + region without hitting the network", () => {
    const client = createClient(dummyConfig);
    expect(client.endpoint).toBe(dummyConfig.endpoint);
    expect(client.region).toBe("eu");
    expect(typeof client.query).toBe("function");
  });
});
