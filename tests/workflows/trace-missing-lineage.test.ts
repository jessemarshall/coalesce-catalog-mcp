import { describe, it, expect } from "vitest";
import { dominantLineageType } from "../../src/workflows/trace-missing-lineage.js";

describe("dominantLineageType", () => {
  it("returns null for an empty list", () => {
    expect(dominantLineageType([])).toBeNull();
  });

  it("returns null when every row has no lineageType", () => {
    expect(
      dominantLineageType([{ lineageType: null }, { lineageType: undefined }])
    ).toBeNull();
  });

  it("returns the single type when uniform", () => {
    expect(
      dominantLineageType([
        { lineageType: "AUTOMATIC" },
        { lineageType: "AUTOMATIC" },
        { lineageType: "AUTOMATIC" },
      ])
    ).toBe("AUTOMATIC");
  });

  it("returns the plurality winner when mixed", () => {
    expect(
      dominantLineageType([
        { lineageType: "AUTOMATIC" },
        { lineageType: "MANUAL_CUSTOMER" },
        { lineageType: "MANUAL_CUSTOMER" },
        { lineageType: "OTHER_TECHNOS" },
      ])
    ).toBe("MANUAL_CUSTOMER");
  });

  it("ignores null entries when counting", () => {
    expect(
      dominantLineageType([
        { lineageType: null },
        { lineageType: "MANUAL_OPS" },
        { lineageType: "MANUAL_OPS" },
        { lineageType: "AUTOMATIC" },
      ])
    ).toBe("MANUAL_OPS");
  });
});
