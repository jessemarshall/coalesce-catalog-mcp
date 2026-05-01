import { describe, it, expect } from "vitest";
import {
  textResult,
  errorResult,
  declineResult,
  READ_ONLY_ANNOTATIONS,
  WRITE_ANNOTATIONS,
  DESTRUCTIVE_ANNOTATIONS,
} from "../src/catalog/types.js";

// Direct unit tests for the response helpers and annotation presets in
// `src/catalog/types.ts`. These are imported by every tool — tiny in line
// count, but with load-bearing contracts that other code paths depend on:
//
//   - `errorResult` MUST set `isError: true` (clients filtering on isError
//     route the call into their retry/alert path).
//   - `declineResult` MUST NOT set `isError: true` (a user clicking "no" on
//     a destructive elicitation is expected behavior, not a tool failure).
//   - The two helpers MUST share the same `{ error, detail? }` payload so a
//     caller parsing `JSON.parse(content[0].text)` sees the same shape.
//   - Annotation presets feed `server.ts:isReadOnlyTool`, which checks
//     `readOnlyHint === true` to decide which tools register in read-only
//     mode. Drift in any preset's `readOnlyHint` flips the read-only mode
//     surface area silently.

function parsePayload(result: { content: { text: string }[] }): unknown {
  return JSON.parse(result.content[0].text);
}

describe("textResult", () => {
  it("wraps the payload as a single text content block with pretty JSON", () => {
    const result = textResult({ a: 1, b: "two" });
    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe("text");
    expect(parsePayload(result)).toEqual({ a: 1, b: "two" });
  });

  it("uses 2-space indentation so output is human-readable", () => {
    const result = textResult({ a: 1 });
    // \n + 2-space indent on `"a"` proves pretty-printing rather than
    // single-line JSON.
    expect(result.content[0].text).toBe('{\n  "a": 1\n}');
  });

  it("does not set isError on success results", () => {
    const result = textResult({ ok: true });
    expect(result.isError).toBeUndefined();
  });

  it("preserves null and zero values in the payload", () => {
    const result = textResult({ value: null, count: 0 });
    expect(parsePayload(result)).toEqual({ value: null, count: 0 });
  });

  it("handles array payloads", () => {
    const result = textResult([1, 2, 3]);
    expect(parsePayload(result)).toEqual([1, 2, 3]);
  });

  it("handles primitive payloads (string)", () => {
    const result = textResult("hello");
    expect(parsePayload(result)).toBe("hello");
  });
});

describe("errorResult", () => {
  it("sets isError: true so clients route the response into their retry path", () => {
    const result = errorResult("something broke");
    expect(result.isError).toBe(true);
  });

  it("emits the message-only payload when no detail is provided", () => {
    const result = errorResult("boom");
    expect(parsePayload(result)).toEqual({ error: "boom" });
  });

  it("includes detail under the `detail` key when provided", () => {
    const result = errorResult("graphql failed", {
      kind: "graphql_error",
      errors: [{ message: "bad input" }],
    });
    expect(parsePayload(result)).toEqual({
      error: "graphql failed",
      detail: {
        kind: "graphql_error",
        errors: [{ message: "bad input" }],
      },
    });
  });

  it("treats explicit null detail as a present value (kept under `detail`)", () => {
    // The implementation guards on `=== undefined`, so `null` is included
    // verbatim. This locks that behavior so a future refactor that switched
    // to a truthiness check would be flagged.
    const result = errorResult("x", null);
    expect(parsePayload(result)).toEqual({ error: "x", detail: null });
  });

  it("returns a single-text-block content array", () => {
    const result = errorResult("x");
    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe("text");
  });
});

describe("declineResult", () => {
  it("does NOT set isError so user declines aren't treated as tool failures", () => {
    // Load-bearing: clients filtering on isError must NOT retry/alert when
    // a user clicks "no" on a destructive elicitation. Changing this would
    // silently break that contract.
    const result = declineResult("user declined");
    expect(result.isError).toBeUndefined();
  });

  it("emits the message-only payload when no detail is provided", () => {
    const result = declineResult("user declined");
    expect(parsePayload(result)).toEqual({ error: "user declined" });
  });

  it("includes detail under the `detail` key when provided", () => {
    const result = declineResult("user declined", {
      kind: "user_declined",
      action: "decline",
    });
    expect(parsePayload(result)).toEqual({
      error: "user declined",
      detail: { kind: "user_declined", action: "decline" },
    });
  });

  it("uses the same payload shape as errorResult so callers can parse identically", () => {
    // The shapes intentionally match — clients can JSON.parse the text
    // block and read `.error` / `.detail` without first checking isError.
    const errored = parsePayload(errorResult("oops", { kind: "x" }));
    const declined = parsePayload(declineResult("oops", { kind: "x" }));
    expect(errored).toEqual(declined);
  });

  it("returns a single-text-block content array", () => {
    const result = declineResult("user declined");
    expect(result.content).toHaveLength(1);
    expect(result.content[0].type).toBe("text");
  });
});

describe("annotation presets", () => {
  // server.ts:isReadOnlyTool checks `def.config.annotations?.readOnlyHint
  // === true` to decide which tools register when the server is in
  // COALESCE_CATALOG_READ_ONLY=true mode. These assertions lock the preset
  // shape so a drift would surface here, not as a silent expansion of the
  // read-only surface area.

  it("READ_ONLY_ANNOTATIONS marks the tool as read-only and idempotent", () => {
    expect(READ_ONLY_ANNOTATIONS).toEqual({
      readOnlyHint: true,
      idempotentHint: true,
      destructiveHint: false,
      openWorldHint: true,
    });
  });

  it("WRITE_ANNOTATIONS does NOT have readOnlyHint=true (so it's filtered out in read-only mode)", () => {
    expect(WRITE_ANNOTATIONS.readOnlyHint).toBe(false);
    expect(WRITE_ANNOTATIONS.destructiveHint).toBe(false);
    expect(WRITE_ANNOTATIONS.idempotentHint).toBe(false);
    expect(WRITE_ANNOTATIONS.openWorldHint).toBe(true);
  });

  it("DESTRUCTIVE_ANNOTATIONS sets destructiveHint=true (so the SDK can warn the user)", () => {
    expect(DESTRUCTIVE_ANNOTATIONS.destructiveHint).toBe(true);
    expect(DESTRUCTIVE_ANNOTATIONS.readOnlyHint).toBe(false);
    expect(DESTRUCTIVE_ANNOTATIONS.idempotentHint).toBe(false);
    expect(DESTRUCTIVE_ANNOTATIONS.openWorldHint).toBe(true);
  });

  it("only READ_ONLY_ANNOTATIONS qualifies a tool for read-only-mode registration", () => {
    // Mirrors server.ts:isReadOnlyTool's exact predicate; if any other
    // preset gains readOnlyHint=true, this test fires.
    expect(READ_ONLY_ANNOTATIONS.readOnlyHint).toBe(true);
    expect(WRITE_ANNOTATIONS.readOnlyHint).toBe(false);
    expect(DESTRUCTIVE_ANNOTATIONS.readOnlyHint).toBe(false);
  });

  it("READ_ONLY_ANNOTATIONS and DESTRUCTIVE_ANNOTATIONS are mutually exclusive on the destructive axis", () => {
    expect(READ_ONLY_ANNOTATIONS.destructiveHint).toBe(false);
    expect(DESTRUCTIVE_ANNOTATIONS.destructiveHint).toBe(true);
  });
});
