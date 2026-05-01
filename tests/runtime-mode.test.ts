import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { isReadOnlyMode } from "../src/runtime-mode.js";
import { READ_ONLY_ENV_VAR } from "../src/constants.js";

// Direct unit-level coverage for the strict-equality semantics of
// isReadOnlyMode. The strict `=== "true"` comparison is load-bearing: a
// future refactor to a truthiness check (or to .toLowerCase() === "true")
// would silently expand which env-var values enable read-only mode and could
// flip read-only enforcement on/off for users who set
// COALESCE_CATALOG_READ_ONLY=1 expecting it to "just work". The end-to-end
// tests/read-only-mode.test.ts spawns the built dist server and is too
// coarse to surface that regression.

describe("isReadOnlyMode", () => {
  let originalValue: string | undefined;

  beforeEach(() => {
    originalValue = process.env[READ_ONLY_ENV_VAR];
  });

  afterEach(() => {
    if (originalValue === undefined) delete process.env[READ_ONLY_ENV_VAR];
    else process.env[READ_ONLY_ENV_VAR] = originalValue;
  });

  it("returns true when the env var is exactly the string 'true'", () => {
    process.env[READ_ONLY_ENV_VAR] = "true";
    expect(isReadOnlyMode()).toBe(true);
  });

  it("returns false when the env var is unset", () => {
    delete process.env[READ_ONLY_ENV_VAR];
    expect(isReadOnlyMode()).toBe(false);
  });

  it("returns false when the env var is the literal string 'false'", () => {
    process.env[READ_ONLY_ENV_VAR] = "false";
    expect(isReadOnlyMode()).toBe(false);
  });

  it("returns false for case variants like 'True', 'TRUE'", () => {
    process.env[READ_ONLY_ENV_VAR] = "True";
    expect(isReadOnlyMode()).toBe(false);
    process.env[READ_ONLY_ENV_VAR] = "TRUE";
    expect(isReadOnlyMode()).toBe(false);
  });

  it("returns false for truthy non-'true' values like '1', 'yes', 'on'", () => {
    for (const v of ["1", "yes", "on", "y", "enabled"]) {
      process.env[READ_ONLY_ENV_VAR] = v;
      expect(isReadOnlyMode(), `expected false for value ${JSON.stringify(v)}`).toBe(false);
    }
  });

  it("returns false for the empty string", () => {
    process.env[READ_ONLY_ENV_VAR] = "";
    expect(isReadOnlyMode()).toBe(false);
  });

  it("returns false for surrounding whitespace ('true ', ' true')", () => {
    process.env[READ_ONLY_ENV_VAR] = "true ";
    expect(isReadOnlyMode()).toBe(false);
    process.env[READ_ONLY_ENV_VAR] = " true";
    expect(isReadOnlyMode()).toBe(false);
  });
});
