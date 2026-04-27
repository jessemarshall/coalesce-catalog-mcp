import {
  DEFAULT_REGION,
  GRAPHQL_PATH,
  REGION_BASE_URLS,
  type CatalogRegion,
} from "./constants.js";

export interface CatalogAuth {
  apiKey: string;
  region: CatalogRegion;
  /** Fully-qualified GraphQL endpoint (base URL + /public/graphql). */
  endpoint: string;
}

const SETUP_HINT =
  "Set COALESCE_CATALOG_API_KEY in your environment. " +
  "Generate a token at https://app.castordoc.com (Settings → API tokens). " +
  "Optionally set COALESCE_CATALOG_REGION=us for the US region (default: eu).";

export class CatalogConfigError extends Error {
  constructor(message: string) {
    super(`${message}\n\n${SETUP_HINT}`);
    this.name = "CatalogConfigError";
  }
}

function resolveRegion(): CatalogRegion {
  const raw = process.env.COALESCE_CATALOG_REGION?.trim().toLowerCase();
  if (!raw) return DEFAULT_REGION;
  if (raw === "eu" || raw === "us") return raw;
  throw new CatalogConfigError(
    `Invalid COALESCE_CATALOG_REGION "${raw}" — must be "eu" or "us"`
  );
}

function resolveBaseUrl(region: CatalogRegion): string {
  const override = process.env.COALESCE_CATALOG_API_URL?.trim();
  if (override) return override.replace(/\/+$/, "");
  return REGION_BASE_URLS[region];
}

export function resolveCatalogAuth(): CatalogAuth {
  const apiKey = process.env.COALESCE_CATALOG_API_KEY?.trim();
  if (!apiKey) {
    throw new CatalogConfigError("Missing COALESCE_CATALOG_API_KEY");
  }
  const region = resolveRegion();
  const baseUrl = resolveBaseUrl(region);
  return { apiKey, region, endpoint: `${baseUrl}${GRAPHQL_PATH}` };
}
