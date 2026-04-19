import type { CatalogClient } from "../../src/client.js";

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
  return {
    endpoint: "https://example.invalid/public/graphql",
    region: "eu",
    calls,
    async query<TData>(document: string, variables?: unknown): Promise<TData> {
      const callIndex = calls.length;
      calls.push({ document, variables });
      const result = await responder(document, variables, callIndex);
      return result as TData;
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
