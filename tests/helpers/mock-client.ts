import type { CatalogClient, RawGraphQLResponse } from "../../src/client.js";

export interface MockCall {
  document: string;
  variables: unknown;
}

export interface MockClient extends CatalogClient {
  calls: MockCall[];
}

type Responder = (
  document: string,
  variables: unknown,
  callIndex: number
) => unknown | Promise<unknown>;

export function makeMockClient(responder: Responder): MockClient {
  const calls: MockCall[] = [];
  async function runResponder(document: string, variables: unknown): Promise<unknown> {
    const callIndex = calls.length;
    calls.push({ document, variables });
    return await responder(document, variables, callIndex);
  }
  return {
    endpoint: "https://example.invalid/public/graphql",
    region: "eu",
    calls,
    async execute<TData>(document: string, variables?: unknown): Promise<TData> {
      const result = await runResponder(document, variables);
      return result as TData;
    },
    // For tests that want to exercise executeRaw, the responder can return
    // a full { data?, errors?, extensions? } envelope. Otherwise the result
    // is wrapped as { data: result } so the common execute-path shape still
    // passes through.
    async executeRaw<TData>(
      document: string,
      variables?: unknown
    ): Promise<RawGraphQLResponse<TData>> {
      const result = await runResponder(document, variables);
      if (result && typeof result === "object") {
        const obj = result as Record<string, unknown>;
        if ("data" in obj || "errors" in obj || "extensions" in obj) {
          return obj as RawGraphQLResponse<TData>;
        }
      }
      return { data: result as TData };
    },
  };
}

export function makeQueueClient(
  ...responses: ReadonlyArray<unknown | Error>
): MockClient {
  return makeMockClient((_doc, _vars, i) => {
    if (i >= responses.length) {
      throw new Error(
        `mock queue exhausted: ${responses.length} responses, asked for index ${i}`
      );
    }
    const r = responses[i];
    if (r instanceof Error) throw r;
    return r;
  });
}
