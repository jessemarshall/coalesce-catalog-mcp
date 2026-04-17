export const SERVER_NAME = "coalesce-catalog";
export const SERVER_VERSION = "0.1.0";

export type CatalogRegion = "eu" | "us";

export const DEFAULT_REGION: CatalogRegion = "eu";

export const REGION_BASE_URLS: Record<CatalogRegion, string> = {
  eu: "https://api.castordoc.com",
  us: "https://api.us.castordoc.com",
};

export const GRAPHQL_PATH = "/public/graphql";

export const DEFAULT_REQUEST_TIMEOUT_MS = 30_000;

export const DEFAULT_PAGE_SIZE = 100;
export const MAX_PAGE_SIZE = 500;

export const READ_ONLY_ENV_VAR = "COALESCE_CATALOG_READ_ONLY";
