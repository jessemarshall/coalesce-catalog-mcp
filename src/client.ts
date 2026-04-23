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

export interface RawGraphQLResponse<TData = Record<string, unknown>> {
  data?: TData | null;
  errors?: GraphQLError[];
  extensions?: Record<string, unknown>;
}

export interface RawExecuteOptions extends RequestOptions {
  operationName?: string;
}

export interface CatalogClient {
  readonly endpoint: string;
  readonly region: CatalogAuth["region"];
  execute<TData, TVars extends Record<string, unknown> = Record<string, unknown>>(
    document: string,
    variables?: TVars,
    options?: RequestOptions
  ): Promise<TData>;
  /**
   * Lower-level passthrough that returns the raw GraphQL envelope
   * ({ data, errors, extensions }) without throwing on `errors[]`. Intended
   * for debug/escape-hatch tools (catalog_run_graphql) where the caller needs
   * to see validation/execution errors verbatim rather than have them mapped
   * to tool errors. Still raises CatalogApiError for HTTP-level failures.
   */
  executeRaw<TData = Record<string, unknown>, TVars extends Record<string, unknown> = Record<string, unknown>>(
    document: string,
    variables?: TVars,
    options?: RawExecuteOptions
  ): Promise<RawGraphQLResponse<TData>>;
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

  async function postGraphQL<TData>(
    document: string,
    variables: Record<string, unknown> | undefined,
    options: RawExecuteOptions | undefined
  ): Promise<{ status: number; body: RawGraphQLResponse<TData> }> {
    const timeoutMs = options?.timeoutMs ?? requestTimeoutMs;
    const { controller, clear } = buildAbortController(
      timeoutMs,
      options?.signal
    );

    const payload: Record<string, unknown> = { query: document };
    // Only include `variables` when the caller supplied them. Strict GraphQL
    // servers can reject `variables: {}` on a document that declares no
    // variables, and run_graphql promises to pass the caller's input through
    // verbatim.
    if (variables !== undefined) payload.variables = variables;
    if (options?.operationName) payload.operationName = options.operationName;

    let response: Response;
    try {
      response = await fetch(config.endpoint, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Token ${config.apiKey}`,
          Accept: "application/json",
        },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
    } finally {
      clear();
    }

    if (!response.ok) {
      // GraphQL servers sometimes return 4xx alongside a body that still
      // matches the `{ data?, errors? }` envelope — Castor returns HTTP 400
      // with a populated `errors[]` for validation failures. When the body
      // parses as a GraphQL envelope, treat it as a normal response so
      // execute()'s GraphQL-error branch can surface it and executeRaw()
      // can pass it through verbatim. Only fall back to CatalogApiError
      // when the body isn't JSON or doesn't look GraphQL-shaped (auth
      // failures, 5xx with HTML pages, etc.).
      const detail = await readErrorBody(response);
      if (isGraphQLEnvelope(detail)) {
        return {
          status: response.status,
          body: detail as RawGraphQLResponse<TData>,
        };
      }
      throw new CatalogApiError(
        mapHttpStatusMessage(response.status),
        response.status,
        detail
      );
    }

    const body = (await response.json()) as RawGraphQLResponse<TData>;
    return { status: response.status, body };
  }

  async function execute<
    TData,
    TVars extends Record<string, unknown> = Record<string, unknown>,
  >(
    document: string,
    variables?: TVars,
    options?: RequestOptions
  ): Promise<TData> {
    const { status, body } = await postGraphQL<TData>(document, variables, options);
    if (body.errors?.length) {
      throw new CatalogGraphQLError(body.errors, body.data);
    }
    if (body.data === undefined || body.data === null) {
      throw new CatalogApiError(
        "Catalog API returned no data and no errors",
        status,
        body
      );
    }
    return body.data;
  }

  async function executeRaw<
    TData = Record<string, unknown>,
    TVars extends Record<string, unknown> = Record<string, unknown>,
  >(
    document: string,
    variables?: TVars,
    options?: RawExecuteOptions
  ): Promise<RawGraphQLResponse<TData>> {
    const { body } = await postGraphQL<TData>(document, variables, options);
    return body;
  }

  return {
    endpoint: config.endpoint,
    region: config.region,
    execute,
    executeRaw,
  };
}

async function readErrorBody(response: Response): Promise<unknown> {
  // Read the body once as text, then attempt JSON parsing. Reading via
  // .json() first would consume the body stream — if parsing failed,
  // the subsequent .text() call would also fail because the stream is
  // already consumed.
  try {
    const text = await response.text();
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  } catch {
    return "[response body could not be read]";
  }
}

function isGraphQLEnvelope(body: unknown): boolean {
  return (
    typeof body === "object" &&
    body !== null &&
    ("errors" in body || "data" in body)
  );
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
