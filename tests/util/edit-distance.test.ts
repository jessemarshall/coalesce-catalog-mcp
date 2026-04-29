import { describe, expect, it } from "vitest";
import { editDistance } from "../../src/util/edit-distance.js";

describe("editDistance", () => {
  it("returns 0 for identical strings", () => {
    expect(editDistance("hello", "hello")).toBe(0);
  });

  it("returns 0 for two empty strings", () => {
    expect(editDistance("", "")).toBe(0);
  });

  it("returns the length of b when a is empty", () => {
    expect(editDistance("", "abc")).toBe(3);
  });

  it("returns the length of a when b is empty", () => {
    expect(editDistance("abc", "")).toBe(3);
  });

  it("counts a single substitution", () => {
    expect(editDistance("cat", "bat")).toBe(1);
  });

  it("counts a single insertion", () => {
    expect(editDistance("cat", "cats")).toBe(1);
  });

  it("counts a single deletion", () => {
    expect(editDistance("cats", "cat")).toBe(1);
  });

  it("computes the canonical kitten/sitting distance", () => {
    // sitten (substitute) → sittin (substitute) → sitting (insert)
    expect(editDistance("kitten", "sitting")).toBe(3);
  });

  it("is symmetric — order does not change the result", () => {
    expect(editDistance("kitten", "sitting")).toBe(
      editDistance("sitting", "kitten")
    );
    expect(editDistance("a", "abcdefghij")).toBe(
      editDistance("abcdefghij", "a")
    );
  });

  it("is case-sensitive — callers fold beforehand if needed", () => {
    expect(editDistance("Tag", "tag")).toBe(1);
    expect(editDistance("PII", "pii")).toBe(3);
  });

  it("handles strings of very different lengths in either order", () => {
    // Anvil's swap-asymmetry concern: row buffers should size to min(la, lb).
    // Both directions should produce the same answer.
    const longer = "abcdefghijklmnopqrstuvwxyz";
    const shorter = "axz";
    expect(editDistance(longer, shorter)).toBe(editDistance(shorter, longer));
    expect(editDistance(longer, shorter)).toBe(23);
  });
});
