import { DEFAULT_REQUEST_TIMEOUT_MS } from "./constants.js";
import {
  resolveCatalogAuth,
  type CatalogAuth,
} from "./services/config/credentials.js";

export interface ClientConfig extends CatalogAuth {
  requestTimeoutMs?: number;
}

export interface RequestOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export interface GraphQLError {
  message: string;
  path?: (string | number)[];
  extensions?: Record<string, unknown>;
}

export class CatalogApiError extends Error {
  constructor(
    message: string,
    public status: number,
    public detail?: unknown
  ) {
    super(message);
    this.name = "CatalogApiError";
  }
}

export class CatalogGraphQLError extends Error {
  constructor(
    public errors: GraphQLError[],
    public partialData?: unknown
  ) {
    const first = errors[0]?.message ?? "Unknown GraphQL error";
    const extra = errors.length > 1 ? ` (+${errors.length - 1} more)` : "";
    super(`Catalog GraphQL error: ${first}${extra}`);
    this.name = "CatalogGraphQLError";
  }
}

export function validateConfig(): ClientConfig {
  return resolveCatalogAuth();
}

export interface CatalogClient {
  readonly endpoint: string;
  readonly region: CatalogAuth["region"];
  execute<TData, TVars extends Record<string, unknown> = Record<string, unknown>>(
    document: string,
    variables?: TVars,
    options?: RequestOptions
  ): Promise<TData>;
}

function buildAbortController(
  timeoutMs: number,
  external?: AbortSignal
): { controller: AbortController; clear: () => void } {
  const controller = new AbortController();
  const onAbort = () => controller.abort(external?.reason);
  if (external) {
    if (external.aborted) controller.abort(external.reason);
    else external.addEventListener("abort", onAbort, { once: true });
  }
  const timer = setTimeout(() => {
    controller.abort(new Error(`Request timed out after ${timeoutMs}ms`));
  }, timeoutMs);
  return {
    controller,
    clear: () => {
      clearTimeout(timer);
      external?.removeEventListener("abort", onAbort);
    },
  };
}

export function createClient(config: ClientConfig): CatalogClient {
  const requestTimeoutMs = config.requestTimeoutMs ?? DEFAULT_REQUEST_TIMEOUT_MS;

  async function execute<
    TData,
    TVars extends Record<string, unknown> = Record<string, unknown>,
  >(
    document: string,
    variables?: TVars,
    options?: RequestOptions
  ): Promise<TData> {
    const timeoutMs = options?.timeoutMs ?? requestTimeoutMs;
    const { controller, clear } = buildAbortController(
      timeoutMs,
      options?.signal
    );

    let response: Response;
    try {
      response = await fetch(config.endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Token ${config.apiKey}`,
          Accept: "application/json",
        },
        body: JSON.stringify({ query: document, variables: variables ?? {} }),
        signal: controller.signal,
      });
    } finally {
      clear();
    }

    if (!response.ok) {
      let detail: unknown;
      try {
        detail = await response.json();
      } catch {
        try {
          detail = await response.text();
        } catch {
          detail = "[response body could not be read]";
        }
      }
      throw new CatalogApiError(
        mapHttpStatusMessage(response.status),
        response.status,
        detail
      );
    }

    const body = (await response.json()) as {
      data?: TData;
      errors?: GraphQLError[];
    };

    if (body.errors?.length) {
      throw new CatalogGraphQLError(body.errors, body.data);
    }
    if (body.data === undefined) {
      throw new CatalogApiError(
        "Catalog API returned no data and no errors",
        response.status,
        body
      );
    }
    return body.data;
  }

  return { endpoint: config.endpoint, region: config.region, execute };
}

function mapHttpStatusMessage(status: number): string {
  switch (status) {
    case 400:
      return "Bad request sent to Catalog API";
    case 401:
      return "Invalid or expired Catalog API token (set COALESCE_CATALOG_API_KEY)";
    case 403:
      return "Catalog API token lacks permission for this operation";
    case 404:
      return "Catalog API endpoint not found";
    case 429:
      return "Catalog API rate limit exceeded";
    default:
      return `Catalog API unavailable (HTTP ${status})`;
  }
}
