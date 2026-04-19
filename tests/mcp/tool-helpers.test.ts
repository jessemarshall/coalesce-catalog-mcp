import { describe, it, expect } from "vitest";
import { listEnvelope } from "../../src/mcp/tool-helpers.js";

describe("listEnvelope", () => {
  it("computes hasMore=false when everything fits", () => {
    const env = listEnvelope(0, 10, 2, [{ id: "a" }, { id: "b" }]);
    expect(env.pagination).toEqual({
      page: 0,
      nbPerPage: 10,
      totalCount: 2,
      hasMore: false,
    });
    expect(env.data).toHaveLength(2);
  });

  it("computes hasMore=true when more pages remain", () => {
    // page 0 of 10 per page; 100 total; returned 10 so far
    const env = listEnvelope(
      0,
      10,
      100,
      Array.from({ length: 10 }, (_, i) => ({ id: String(i) }))
    );
    expect(env.pagination.hasMore).toBe(true);
  });

  it("computes hasMore=false on the last page", () => {
    // page 9 of 10 per page; 100 total; returned 10 so far
    const env = listEnvelope(
      9,
      10,
      100,
      Array.from({ length: 10 }, (_, i) => ({ id: String(i) }))
    );
    expect(env.pagination.hasMore).toBe(false);
  });

  it("computes hasMore=false for an empty result", () => {
    const env = listEnvelope(0, 10, 0, []);
    expect(env.pagination.hasMore).toBe(false);
    expect(env.data).toHaveLength(0);
  });

  it("infers hasMore=true when totalCount is null and page is full", () => {
    const env = listEnvelope(
      0,
      10,
      null,
      Array.from({ length: 10 }, (_, i) => ({ id: String(i) }))
    );
    expect(env.pagination.hasMore).toBe(true);
    expect(env.pagination.totalCount).toBeUndefined();
  });

  it("infers hasMore=false when totalCount is null and page is partial", () => {
    const env = listEnvelope(0, 10, null, [{ id: "a" }, { id: "b" }]);
    expect(env.pagination.hasMore).toBe(false);
    expect(env.pagination.totalCount).toBeUndefined();
  });

  it("infers hasMore=false when totalCount is null and page is empty", () => {
    const env = listEnvelope(0, 10, null, []);
    expect(env.pagination.hasMore).toBe(false);
    expect(env.pagination.totalCount).toBeUndefined();
  });
});
