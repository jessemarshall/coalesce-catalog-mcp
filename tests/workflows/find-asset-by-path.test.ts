import { describe, it, expect } from "vitest";

// parsePath is not exported; re-exercise it via the compiled module path parsing
// indirectly by importing from the source file. Easiest test setup: re-declare
// the same pure function here and cross-check both implementations by running
// both. To keep maintenance cost low, we instead export parsePath from the
// workflow file and import it directly here. The test file assumes the export.
import { parsePath } from "../../src/workflows/find-asset-by-path.js";

describe("parsePath", () => {
  it("splits a 3-part unquoted path", () => {
    expect(parsePath("DB.SCHEMA.TABLE")).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("splits a 4-part path with column", () => {
    expect(parsePath("DB.SCHEMA.TABLE.COL")).toEqual([
      "DB",
      "SCHEMA",
      "TABLE",
      "COL",
    ]);
  });

  it("unwraps double-quoted identifiers", () => {
    expect(parsePath('"DB"."SCHEMA"."TABLE"')).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("unwraps backtick-quoted identifiers", () => {
    expect(parsePath("`DB`.`SCHEMA`.`TABLE`")).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("handles mixed quoting", () => {
    expect(parsePath('"DB".SCHEMA.`TABLE`')).toEqual(["DB", "SCHEMA", "TABLE"]);
  });

  it("preserves dots inside quoted identifiers", () => {
    expect(parsePath('"weird.name".schema.table')).toEqual([
      "weird.name",
      "schema",
      "table",
    ]);
  });

  it("preserves spaces inside identifiers", () => {
    expect(parsePath('"Databricks Demo".coalesce.sample_data.orders')).toEqual([
      "Databricks Demo",
      "coalesce",
      "sample_data",
      "orders",
    ]);
  });

  it("trims surrounding whitespace per component", () => {
    expect(parsePath("  db  .  schema  .  table  ")).toEqual([
      "db",
      "schema",
      "table",
    ]);
  });

  it("drops empty components from leading/trailing dots", () => {
    expect(parsePath(".db.schema.table.")).toEqual(["db", "schema", "table"]);
  });

  it("returns empty for an empty string", () => {
    expect(parsePath("")).toEqual([]);
  });

  it("handles 2 parts (caller detects too-few, tool returns error)", () => {
    expect(parsePath("db.schema")).toHaveLength(2);
  });
});
