import { describe, it, expect, beforeEach, afterEach } from "vitest";
import {
  resolveCatalogAuth,
  CatalogConfigError,
} from "../src/services/config/credentials.js";

// Snapshot and restore relevant env vars around each test so test isolation
// is maintained even if the host machine has real credentials set.
const ENV_KEYS = [
  "COALESCE_CATALOG_API_KEY",
  "COALESCE_CATALOG_REGION",
  "COALESCE_CATALOG_API_URL",
] as const;

let saved: Record<string, string | undefined>;

beforeEach(() => {
  saved = {};
  for (const k of ENV_KEYS) {
    saved[k] = process.env[k];
    delete process.env[k];
  }
});

afterEach(() => {
  for (const k of ENV_KEYS) {
    if (saved[k] === undefined) delete process.env[k];
    else process.env[k] = saved[k];
  }
});

describe("resolveCatalogAuth", () => {
  // ── Missing key ──────────────────────────────────────────────────────────

  it("throws CatalogConfigError when COALESCE_CATALOG_API_KEY is unset", () => {
    expect(() => resolveCatalogAuth()).toThrow(CatalogConfigError);
  });

  it("throws CatalogConfigError when COALESCE_CATALOG_API_KEY is empty", () => {
    process.env.COALESCE_CATALOG_API_KEY = "   ";
    expect(() => resolveCatalogAuth()).toThrow(CatalogConfigError);
  });

  it("error message includes the setup hint", () => {
    try {
      resolveCatalogAuth();
      expect.unreachable("should have thrown");
    } catch (e) {
      expect((e as Error).message).toMatch(/COALESCE_CATALOG_API_KEY/);
      expect((e as Error).message).toMatch(/castordoc\.com/);
    }
  });

  // ── Default region (eu) ──────────────────────────────────────────────────

  it("defaults to eu region with the EU endpoint", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    const auth = resolveCatalogAuth();
    expect(auth.region).toBe("eu");
    expect(auth.endpoint).toBe("https://api.castordoc.com/public/graphql");
    expect(auth.apiKey).toBe("tok-123");
  });

  // ── Explicit region ──────────────────────────────────────────────────────

  it("accepts COALESCE_CATALOG_REGION=us", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_REGION = "us";
    const auth = resolveCatalogAuth();
    expect(auth.region).toBe("us");
    expect(auth.endpoint).toBe(
      "https://api.us.castordoc.com/public/graphql"
    );
  });

  it("accepts COALESCE_CATALOG_REGION=eu (explicit)", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_REGION = "eu";
    const auth = resolveCatalogAuth();
    expect(auth.region).toBe("eu");
  });

  it("is case-insensitive for region", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_REGION = "US";
    const auth = resolveCatalogAuth();
    expect(auth.region).toBe("us");
  });

  it("trims whitespace from region", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_REGION = "  eu  ";
    const auth = resolveCatalogAuth();
    expect(auth.region).toBe("eu");
  });

  it("throws for invalid region", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_REGION = "ap";
    expect(() => resolveCatalogAuth()).toThrow(CatalogConfigError);
    expect(() => resolveCatalogAuth()).toThrow(/ap/);
  });

  // ── Custom API URL override ──────────────────────────────────────────────

  it("uses COALESCE_CATALOG_API_URL when set", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_API_URL = "https://custom.example.com";
    const auth = resolveCatalogAuth();
    expect(auth.endpoint).toBe(
      "https://custom.example.com/public/graphql"
    );
  });

  it("strips trailing slashes from custom URL", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_API_URL = "https://custom.example.com///";
    const auth = resolveCatalogAuth();
    expect(auth.endpoint).toBe(
      "https://custom.example.com/public/graphql"
    );
  });

  it("custom URL takes precedence over region", () => {
    process.env.COALESCE_CATALOG_API_KEY = "tok-123";
    process.env.COALESCE_CATALOG_REGION = "us";
    process.env.COALESCE_CATALOG_API_URL = "https://local.test";
    const auth = resolveCatalogAuth();
    // Region is still resolved, but endpoint uses the override
    expect(auth.region).toBe("us");
    expect(auth.endpoint).toBe("https://local.test/public/graphql");
  });

  // ── API key trimming ─────────────────────────────────────────────────────

  it("trims whitespace from the API key", () => {
    process.env.COALESCE_CATALOG_API_KEY = "  tok-abc  ";
    const auth = resolveCatalogAuth();
    expect(auth.apiKey).toBe("tok-abc");
  });
});
