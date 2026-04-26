import { describe, it, expect } from "vitest";
import { defineAiTools } from "../../src/mcp/ai.js";
import {
  ADD_AI_ASSISTANT_JOB,
  GET_AI_ASSISTANT_JOB_RESULT,
  SEARCH_QUERIES,
} from "../../src/catalog/operations.js";
import { CatalogGraphQLError } from "../../src/client.js";
import { makeMockClient, makeQueueClient } from "../helpers/mock-client.js";

function findTool(tools: ReturnType<typeof defineAiTools>, name: string) {
  const t = tools.find((x) => x.name === name);
  if (!t) throw new Error(`tool ${name} not registered`);
  return t;
}

function parseResult(r: { content: { text: string }[] }): unknown {
  return JSON.parse(r.content[0].text);
}

describe("catalog_search_queries handler", () => {
  it("returns the search results unwrapped from data.searchQueries.data", async () => {
    const client = makeMockClient(() => ({
      searchQueries: {
        data: [
          { id: "q1", queryText: "select 1" },
          { id: "q2", queryText: "select 2" },
        ],
      },
    }));
    const tool = findTool(defineAiTools(client), "catalog_search_queries");

    const res = await tool.handler({ question: "weekly active users" });

    expect(res.isError).toBeUndefined();
    expect(parseResult(res)).toEqual({
      data: [
        { id: "q1", queryText: "select 1" },
        { id: "q2", queryText: "select 2" },
      ],
    });
    expect(client.calls).toHaveLength(1);
    expect(client.calls[0].document).toBe(SEARCH_QUERIES);
    expect(client.calls[0].variables).toEqual({
      data: { question: "weekly active users" },
      scope: undefined,
    });
  });

  it("forwards translateQuestionToEnglish when set", async () => {
    const client = makeMockClient(() => ({ searchQueries: { data: [] } }));
    const tool = findTool(defineAiTools(client), "catalog_search_queries");

    await tool.handler({
      question: "utilisateurs actifs",
      translateQuestionToEnglish: true,
    });

    expect(client.calls[0].variables).toMatchObject({
      data: { question: "utilisateurs actifs", translateQuestionToEnglish: true },
    });
  });

  it("builds scope when tableIds + filterMode are provided", async () => {
    const client = makeMockClient(() => ({ searchQueries: { data: [] } }));
    const tool = findTool(defineAiTools(client), "catalog_search_queries");

    await tool.handler({
      question: "joins to orders",
      tableIds: ["t-1", "t-2"],
      filterMode: "ANY",
    });

    expect(client.calls[0].variables).toMatchObject({
      scope: { tableIds: ["t-1", "t-2"], filterMode: "ANY" },
    });
  });

  it("omits scope when tableIds is an empty array", async () => {
    const client = makeMockClient(() => ({ searchQueries: { data: [] } }));
    const tool = findTool(defineAiTools(client), "catalog_search_queries");

    await tool.handler({ question: "anything", tableIds: [] });

    const vars = client.calls[0].variables as { scope?: unknown };
    expect(vars.scope).toBeUndefined();
  });

  it("rejects filterMode without tableIds up-front", async () => {
    const client = makeMockClient(() => ({ searchQueries: { data: [] } }));
    const tool = findTool(defineAiTools(client), "catalog_search_queries");

    const res = await tool.handler({ question: "x", filterMode: "ANY" });

    expect(res.isError).toBe(true);
    expect(parseResult(res)).toMatchObject({
      error: expect.stringMatching(/filterMode requires tableIds/),
    });
    expect(client.calls).toHaveLength(0);
  });

  it("rejects filterMode with an empty tableIds array", async () => {
    const client = makeMockClient(() => ({ searchQueries: { data: [] } }));
    const tool = findTool(defineAiTools(client), "catalog_search_queries");

    const res = await tool.handler({
      question: "x",
      tableIds: [],
      filterMode: "ALL",
    });

    expect(res.isError).toBe(true);
    expect(parseResult(res)).toMatchObject({
      error: expect.stringMatching(/filterMode requires tableIds/),
    });
    expect(client.calls).toHaveLength(0);
  });

  it("surfaces GraphQL errors as isError tool results", async () => {
    const client = makeMockClient(() => {
      throw new CatalogGraphQLError([{ message: "boom" }]);
    });
    const tool = findTool(defineAiTools(client), "catalog_search_queries");

    const res = await tool.handler({ question: "x" });

    expect(res.isError).toBe(true);
    expect(parseResult(res)).toMatchObject({
      error: expect.stringMatching(/boom/),
      detail: { kind: "graphql_error" },
    });
  });
});

describe("catalog_ask_assistant handler", () => {
  it("starts a job and returns the data payload (with jobId)", async () => {
    const client = makeMockClient(() => ({
      addAiAssistantJob: { data: { jobId: "job-abc" } },
    }));
    const tool = findTool(defineAiTools(client), "catalog_ask_assistant");

    const res = await tool.handler({
      email: "user@example.com",
      question: "what is X?",
      externalConversationId: "conv-1",
    });

    expect(res.isError).toBeUndefined();
    expect(parseResult(res)).toEqual({ jobId: "job-abc" });
    expect(client.calls[0].document).toBe(ADD_AI_ASSISTANT_JOB);
    expect(client.calls[0].variables).toEqual({
      data: {
        email: "user@example.com",
        externalConversationId: "conv-1",
        question: "what is X?",
      },
    });
  });

  it("forwards origin when provided", async () => {
    const client = makeMockClient(() => ({
      addAiAssistantJob: { data: { jobId: "job-1" } },
    }));
    const tool = findTool(defineAiTools(client), "catalog_ask_assistant");

    await tool.handler({
      email: "user@example.com",
      question: "q",
      externalConversationId: "c",
      origin: "SLACK_BOT",
    });

    expect(client.calls[0].variables).toMatchObject({
      data: { origin: "SLACK_BOT" },
    });
  });
});

describe("catalog_get_assistant_result handler — start-then-poll lifecycle", () => {
  function jobResult(status: string, extras: Record<string, unknown> = {}) {
    return {
      getAiAssistantJobResult: {
        data: { jobId: "job-1", status, ...extras },
      },
    };
  }

  it("returns ADDED status on the first poll", async () => {
    const client = makeQueueClient(jobResult("ADDED"));
    const tool = findTool(defineAiTools(client), "catalog_get_assistant_result");

    const res = await tool.handler({ jobId: "job-1" });

    expect(parseResult(res)).toMatchObject({ status: "ADDED" });
    expect(client.calls[0].document).toBe(GET_AI_ASSISTANT_JOB_RESULT);
    expect(client.calls[0].variables).toEqual({ data: { id: "job-1" } });
  });

  it("includes delaySeconds in the variables only when provided", async () => {
    const client = makeQueueClient(jobResult("ACTIVE"), jobResult("ACTIVE"));
    const tool = findTool(defineAiTools(client), "catalog_get_assistant_result");

    await tool.handler({ jobId: "job-1" });
    await tool.handler({ jobId: "job-1", delaySeconds: 7 });

    expect(client.calls[0].variables).toEqual({ data: { id: "job-1" } });
    expect(client.calls[1].variables).toEqual({
      data: { id: "job-1", delaySeconds: 7 },
    });
  });

  it("accepts delaySeconds=0 as a valid (and forwarded) value", async () => {
    const client = makeQueueClient(jobResult("ACTIVE"));
    const tool = findTool(defineAiTools(client), "catalog_get_assistant_result");

    await tool.handler({ jobId: "job-1", delaySeconds: 0 });

    expect(client.calls[0].variables).toEqual({
      data: { id: "job-1", delaySeconds: 0 },
    });
  });

  it("walks ADDED → ACTIVE → COMPLETED across successive polls", async () => {
    const client = makeQueueClient(
      jobResult("ADDED"),
      jobResult("ACTIVE"),
      jobResult("COMPLETED", {
        answer: "the answer is 42",
        assets: [{ id: "t-1", kind: "TABLE" }],
      })
    );
    const tool = findTool(defineAiTools(client), "catalog_get_assistant_result");

    const r1 = await tool.handler({ jobId: "job-1" });
    const r2 = await tool.handler({ jobId: "job-1" });
    const r3 = await tool.handler({ jobId: "job-1" });

    expect(parseResult(r1)).toMatchObject({ status: "ADDED" });
    expect(parseResult(r2)).toMatchObject({ status: "ACTIVE" });
    expect(parseResult(r3)).toMatchObject({
      status: "COMPLETED",
      answer: "the answer is 42",
      assets: [{ id: "t-1", kind: "TABLE" }],
    });
    expect(client.calls).toHaveLength(3);
  });

  it("returns FAILED status without throwing", async () => {
    const client = makeQueueClient(jobResult("FAILED", { answer: null }));
    const tool = findTool(defineAiTools(client), "catalog_get_assistant_result");

    const res = await tool.handler({ jobId: "job-1" });

    expect(res.isError).toBeUndefined();
    expect(parseResult(res)).toMatchObject({ status: "FAILED" });
  });

  it("returns RETRIES_EXHAUSTED status without throwing", async () => {
    const client = makeQueueClient(jobResult("RETRIES_EXHAUSTED"));
    const tool = findTool(defineAiTools(client), "catalog_get_assistant_result");

    const res = await tool.handler({ jobId: "job-1" });

    expect(res.isError).toBeUndefined();
    expect(parseResult(res)).toMatchObject({ status: "RETRIES_EXHAUSTED" });
  });

  it("surfaces transport errors mid-poll as isError without losing the jobId echo", async () => {
    const client = makeQueueClient(
      jobResult("ACTIVE"),
      new CatalogGraphQLError([{ message: "transient backend failure" }])
    );
    const tool = findTool(defineAiTools(client), "catalog_get_assistant_result");

    const r1 = await tool.handler({ jobId: "job-1" });
    const r2 = await tool.handler({ jobId: "job-1" });

    expect(r1.isError).toBeUndefined();
    expect(r2.isError).toBe(true);
    expect(parseResult(r2)).toMatchObject({
      error: expect.stringMatching(/transient backend failure/),
    });
  });
});
