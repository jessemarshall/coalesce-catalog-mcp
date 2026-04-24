/**
 * Tests for the CatalogClient's HTTP-level behavior: execute(), executeRaw(),
 * error classification, timeout handling, 4xx GraphQL-envelope passthrough,
 * and readErrorBody fallback paths.
 *
 * These tests mock global `fetch` to exercise the real createClient code
 * without hitting the network.
 */

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  createClient,
  CatalogApiError,
  CatalogGraphQLError,
  type ClientConfig,
} from "../src/client.js";

const BASE_CONFIG: ClientConfig = {
  apiKey: "test-key",
  region: "eu",
  endpoint: "https://api.test.invalid/public/graphql",
  requestTimeoutMs: 5000,
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function textResponse(body: string, status: number): Response {
  return new Response(body, {
    status,
    headers: { "Content-Type": "text/plain" },
  });
}

let fetchSpy: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchSpy = vi.fn<typeof globalThis.fetch>();
  vi.stubGlobal("fetch", fetchSpy);
});

afterEach(() => {
  vi.restoreAllMocks();
});

// ---------------------------------------------------------------------------
// execute() — happy path
// ---------------------------------------------------------------------------

describe("execute() happy path", () => {
  it("sends correct headers and body, returns parsed data", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({ data: { getTables: { data: [{ id: "t1" }] } } })
    );
    const client = createClient(BASE_CONFIG);
    const result = await client.execute<{ getTables: { data: unknown[] } }>(
      "query { getTables { data { id } } }",
      { scope: { nameContains: "foo" } }
    );

    expect(result.getTables.data).toEqual([{ id: "t1" }]);

    // Verify fetch was called with the right shape
    const [url, init] = fetchSpy.mock.calls[0];
    expect(url).toBe(BASE_CONFIG.endpoint);
    expect(init.method).toBe("POST");
    expect(init.headers).toEqual(
      expect.objectContaining({
        "Content-Type": "application/json",
        Authorization: "Token test-key",
        Accept: "application/json",
      })
    );
    const body = JSON.parse(init.body as string);
    expect(body.query).toBe("query { getTables { data { id } } }");
    expect(body.variables).toEqual({ scope: { nameContains: "foo" } });
  });

  it("omits variables key when variables are undefined", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({ data: { getUsers: [] } })
    );
    const client = createClient(BASE_CONFIG);
    await client.execute("query { getUsers { id } }");

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body as string);
    expect(body).not.toHaveProperty("variables");
  });
});

// ---------------------------------------------------------------------------
// execute() — GraphQL errors
// ---------------------------------------------------------------------------

describe("execute() GraphQL error handling", () => {
  it("throws CatalogGraphQLError when response has errors[]", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({
        data: null,
        errors: [
          { message: "Variable $scope: type mismatch" },
          { message: "second error" },
        ],
      })
    );
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { getTables { data { id } } }");
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(CatalogGraphQLError);
      const gqlErr = err as CatalogGraphQLError;
      expect(gqlErr.errors).toHaveLength(2);
      expect(gqlErr.message).toContain("Variable $scope");
      expect(gqlErr.message).toContain("+1 more");
    }
  });

  it("includes partialData when data is non-null alongside errors", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({
        data: { getTables: { data: [{ id: "partial" }] } },
        errors: [{ message: "partial failure" }],
      })
    );
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { getTables { data { id } } }");
    } catch (err) {
      const gqlErr = err as CatalogGraphQLError;
      expect(gqlErr.partialData).toEqual({
        getTables: { data: [{ id: "partial" }] },
      });
    }
  });

  it("throws CatalogApiError when data is null and no errors", async () => {
    fetchSpy.mockResolvedValueOnce(jsonResponse({ data: null }));
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(CatalogApiError);
      expect((err as CatalogApiError).message).toContain("no data and no errors");
    }
  });
});

// ---------------------------------------------------------------------------
// execute() — HTTP error handling
// ---------------------------------------------------------------------------

describe("execute() HTTP error handling", () => {
  it("throws CatalogApiError with mapped message for 401", async () => {
    fetchSpy.mockResolvedValueOnce(
      textResponse("Unauthorized", 401)
    );
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(CatalogApiError);
      const apiErr = err as CatalogApiError;
      expect(apiErr.status).toBe(401);
      expect(apiErr.message).toContain("Invalid or expired");
    }
  });

  it("throws CatalogApiError with mapped message for 403", async () => {
    fetchSpy.mockResolvedValueOnce(textResponse("Forbidden", 403));
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(CatalogApiError);
      expect((err as CatalogApiError).status).toBe(403);
      expect((err as CatalogApiError).message).toContain("lacks permission");
    }
  });

  it("throws CatalogApiError with mapped message for 429", async () => {
    fetchSpy.mockResolvedValueOnce(textResponse("Too Many Requests", 429));
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(CatalogApiError);
      expect((err as CatalogApiError).message).toContain("rate limit");
    }
  });

  it("throws CatalogApiError with generic message for 500", async () => {
    fetchSpy.mockResolvedValueOnce(
      textResponse("<html>Internal Server Error</html>", 500)
    );
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(CatalogApiError);
      expect((err as CatalogApiError).status).toBe(500);
      expect((err as CatalogApiError).message).toContain("HTTP 500");
    }
  });

  it("surfaces JSON error body as detail", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({ code: "RATE_LIMITED", retryAfter: 30 }, 429)
    );
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      const apiErr = err as CatalogApiError;
      // Body has no `data` or `errors` key, so it's not a GraphQL envelope
      // and should surface as detail on the CatalogApiError
      expect(apiErr.detail).toEqual({ code: "RATE_LIMITED", retryAfter: 30 });
    }
  });
});

// ---------------------------------------------------------------------------
// 4xx with GraphQL envelope passthrough
// ---------------------------------------------------------------------------

describe("4xx GraphQL envelope passthrough", () => {
  it("treats HTTP 400 with errors[] as a GraphQL error, not CatalogApiError", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse(
        {
          errors: [
            {
              message: "Variable \"$data\" got invalid value",
              extensions: { code: "BAD_USER_INPUT" },
            },
          ],
        },
        400
      )
    );
    const client = createClient(BASE_CONFIG);

    await expect(client.execute("mutation { test }")).rejects.toThrow(
      CatalogGraphQLError
    );
  });

  it("treats HTTP 400 with data key as a GraphQL response for executeRaw", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse(
        {
          data: { test: "value" },
          errors: [{ message: "partial" }],
        },
        400
      )
    );
    const client = createClient(BASE_CONFIG);
    const raw = await client.executeRaw("query { test }");

    expect(raw.data).toEqual({ test: "value" });
    expect(raw.errors).toHaveLength(1);
  });
});

// ---------------------------------------------------------------------------
// executeRaw()
// ---------------------------------------------------------------------------

describe("executeRaw()", () => {
  it("returns the full envelope without throwing on errors[]", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({
        data: null,
        errors: [{ message: "validation fail" }],
        extensions: { tracing: true },
      })
    );
    const client = createClient(BASE_CONFIG);
    const raw = await client.executeRaw("query { test }");

    expect(raw.data).toBeNull();
    expect(raw.errors).toHaveLength(1);
    expect(raw.errors![0].message).toBe("validation fail");
    expect(raw.extensions).toEqual({ tracing: true });
  });

  it("passes operationName through when provided", async () => {
    fetchSpy.mockResolvedValueOnce(
      jsonResponse({ data: { test: 1 } })
    );
    const client = createClient(BASE_CONFIG);
    await client.executeRaw("query Foo { test } query Bar { test }", undefined, {
      operationName: "Bar",
    });

    const body = JSON.parse(fetchSpy.mock.calls[0][1].body as string);
    expect(body.operationName).toBe("Bar");
  });

  it("still throws CatalogApiError for non-GraphQL HTTP errors", async () => {
    fetchSpy.mockResolvedValueOnce(textResponse("Gateway Timeout", 504));
    const client = createClient(BASE_CONFIG);

    await expect(client.executeRaw("query { test }")).rejects.toThrow(
      CatalogApiError
    );
  });
});

// ---------------------------------------------------------------------------
// Timeout handling
// ---------------------------------------------------------------------------

describe("timeout handling", () => {
  it("aborts the request after the configured timeout", async () => {
    const client = createClient({ ...BASE_CONFIG, requestTimeoutMs: 50 });

    fetchSpy.mockImplementationOnce(
      (_url: string, init: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          // Wait for the abort signal to fire
          init.signal!.addEventListener("abort", () => {
            reject(new DOMException("The operation was aborted", "AbortError"));
          });
        })
    );

    await expect(client.execute("query { test }")).rejects.toThrow();
  });

  it("respects per-request timeout override", async () => {
    const client = createClient({ ...BASE_CONFIG, requestTimeoutMs: 10000 });

    fetchSpy.mockImplementationOnce(
      (_url: string, init: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init.signal!.addEventListener("abort", () => {
            reject(new DOMException("The operation was aborted", "AbortError"));
          });
        })
    );

    await expect(
      client.execute("query { test }", undefined, { timeoutMs: 50 })
    ).rejects.toThrow();
  });

  it("respects external abort signal", async () => {
    const client = createClient(BASE_CONFIG);
    const externalController = new AbortController();

    fetchSpy.mockImplementationOnce(
      (_url: string, init: RequestInit) =>
        new Promise<Response>((_resolve, reject) => {
          init.signal!.addEventListener("abort", () => {
            reject(new DOMException("The operation was aborted", "AbortError"));
          });
        })
    );

    const promise = client.execute("query { test }", undefined, {
      signal: externalController.signal,
    });

    // Abort externally
    externalController.abort(new Error("user cancelled"));

    await expect(promise).rejects.toThrow();
  });
});

// ---------------------------------------------------------------------------
// readErrorBody fallback paths
// ---------------------------------------------------------------------------

describe("error body reading fallbacks", () => {
  it("falls back to text() when response is not JSON", async () => {
    fetchSpy.mockResolvedValueOnce(
      textResponse("Bad Gateway: upstream connection refused", 502)
    );
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      const apiErr = err as CatalogApiError;
      expect(apiErr.status).toBe(502);
      expect(apiErr.detail).toBe("Bad Gateway: upstream connection refused");
    }
  });

  it("handles empty error response body gracefully", async () => {
    fetchSpy.mockResolvedValueOnce(
      new Response("", { status: 503 })
    );
    const client = createClient(BASE_CONFIG);

    try {
      await client.execute("query { test }");
      expect.fail("should have thrown");
    } catch (err) {
      expect(err).toBeInstanceOf(CatalogApiError);
      expect((err as CatalogApiError).status).toBe(503);
    }
  });
});
