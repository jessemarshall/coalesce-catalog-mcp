import { describe, it, expect } from "vitest";
import {
  PaginationInputSchema,
  toGraphQLPagination,
} from "../src/schemas/pagination.js";
import {
  SortDirectionSchema,
  NullsPrioritySchema,
} from "../src/schemas/sorting.js";

describe("PaginationInputSchema", () => {
  it("accepts empty input", () => {
    expect(PaginationInputSchema.parse({})).toEqual({});
  });

  it("accepts bounded nbPerPage", () => {
    expect(PaginationInputSchema.parse({ nbPerPage: 50 })).toEqual({ nbPerPage: 50 });
    expect(PaginationInputSchema.parse({ nbPerPage: 500 })).toEqual({ nbPerPage: 500 });
  });

  it("rejects nbPerPage above 500", () => {
    expect(() => PaginationInputSchema.parse({ nbPerPage: 501 })).toThrow();
  });

  it("rejects nbPerPage below 1", () => {
    expect(() => PaginationInputSchema.parse({ nbPerPage: 0 })).toThrow();
  });

  it("rejects negative page", () => {
    expect(() => PaginationInputSchema.parse({ page: -1 })).toThrow();
  });

  it("accepts page 0", () => {
    expect(PaginationInputSchema.parse({ page: 0 })).toEqual({ page: 0 });
  });
});

describe("toGraphQLPagination", () => {
  it("applies defaults when input empty", () => {
    expect(toGraphQLPagination({})).toEqual({ nbPerPage: 100, page: 0 });
  });

  it("preserves caller values", () => {
    expect(toGraphQLPagination({ nbPerPage: 25, page: 2 })).toEqual({
      nbPerPage: 25,
      page: 2,
    });
  });

  it("fills the missing field from defaults", () => {
    expect(toGraphQLPagination({ nbPerPage: 5 })).toEqual({ nbPerPage: 5, page: 0 });
    expect(toGraphQLPagination({ page: 3 })).toEqual({ nbPerPage: 100, page: 3 });
  });
});

describe("SortDirectionSchema", () => {
  it("accepts ASC and DESC", () => {
    expect(SortDirectionSchema.parse("ASC")).toBe("ASC");
    expect(SortDirectionSchema.parse("DESC")).toBe("DESC");
  });

  it("rejects lowercase or other strings", () => {
    expect(() => SortDirectionSchema.parse("asc")).toThrow();
    expect(() => SortDirectionSchema.parse("UP")).toThrow();
  });
});

describe("NullsPrioritySchema", () => {
  it("accepts FIRST and LAST", () => {
    expect(NullsPrioritySchema.parse("FIRST")).toBe("FIRST");
    expect(NullsPrioritySchema.parse("LAST")).toBe("LAST");
  });

  it("rejects other strings", () => {
    expect(() => NullsPrioritySchema.parse("first")).toThrow();
  });
});
