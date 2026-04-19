import { describe, it, expect } from "vitest";
import { defineIntrospectionTools } from "../../src/mcp/introspection.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

function describeTypeTool(client: ReturnType<typeof makeMockClient>) {
  return defineIntrospectionTools(client).find(
    (t) => t.name === "catalog_describe_type"
  )!;
}

function runGraphqlTool(client: ReturnType<typeof makeMockClient>) {
  return defineIntrospectionTools(client).find(
    (t) => t.name === "catalog_run_graphql"
  )!;
}

describe("catalog_describe_type", () => {
  it("flattens nested type references into GraphQL-native strings and flags required fields", async () => {
    const client = makeMockClient(() => ({
      __type: {
        name: "Lineage",
        kind: "OBJECT",
        description: "A table-level lineage edge.",
        fields: [
          {
            name: "id",
            description: "Unique identifier",
            type: {
              kind: "NON_NULL",
              name: null,
              ofType: { kind: "SCALAR", name: "ID", ofType: null },
            },
          },
          {
            name: "parentTableId",
            description: null,
            type: { kind: "SCALAR", name: "String", ofType: null },
          },
          {
            name: "tags",
            description: null,
            type: {
              kind: "LIST",
              name: null,
              ofType: {
                kind: "NON_NULL",
                name: null,
                ofType: { kind: "SCALAR", name: "String", ofType: null },
              },
            },
          },
        ],
        inputFields: null,
        enumValues: null,
      },
    }));
    const tool = describeTypeTool(client);
    const out = parseResult(await tool.handler({ typeName: "Lineage" }));
    const type = out.type as Record<string, unknown>;
    expect(type.name).toBe("Lineage");
    expect(type.kind).toBe("OBJECT");
    const fields = type.fields as Array<Record<string, unknown>>;
    expect(fields).toHaveLength(3);
    expect(fields[0]).toMatchObject({ name: "id", type: "ID!", isRequired: true });
    expect(fields[1]).toMatchObject({
      name: "parentTableId",
      type: "String",
      isRequired: false,
    });
    expect(fields[2]).toMatchObject({
      name: "tags",
      type: "[String!]",
      isRequired: false,
    });
  });

  it("returns INPUT_OBJECT shape as inputFields, not fields", async () => {
    const client = makeMockClient(() => ({
      __type: {
        name: "GetLineagesScope",
        kind: "INPUT_OBJECT",
        description: null,
        fields: null,
        inputFields: [
          {
            name: "withDeleted",
            description: null,
            type: { kind: "SCALAR", name: "Boolean", ofType: null },
          },
        ],
        enumValues: null,
      },
    }));
    const tool = describeTypeTool(client);
    const out = parseResult(await tool.handler({ typeName: "GetLineagesScope" }));
    const type = out.type as Record<string, unknown>;
    expect(type.inputFields).toBeDefined();
    expect(type.fields).toBeUndefined();
    expect((type.inputFields as unknown[])).toHaveLength(1);
  });

  it("returns ENUM values", async () => {
    const client = makeMockClient(() => ({
      __type: {
        name: "LineageType",
        kind: "ENUM",
        description: null,
        fields: null,
        inputFields: null,
        enumValues: [
          { name: "AUTOMATIC", description: null },
          { name: "MANUAL_CUSTOMER", description: null },
        ],
      },
    }));
    const tool = describeTypeTool(client);
    const out = parseResult(await tool.handler({ typeName: "LineageType" }));
    const type = out.type as Record<string, unknown>;
    expect(type.kind).toBe("ENUM");
    expect((type.enumValues as unknown[])).toHaveLength(2);
  });

  it("returns notFound + near-match suggestions when __type is null", async () => {
    let call = 0;
    const client = makeMockClient(() => {
      call++;
      if (call === 1) return { __type: null };
      return {
        __schema: {
          types: [
            { name: "Lineage" },
            { name: "FieldLineage" },
            { name: "LineageType" },
            { name: "Dashboard" },
            { name: "__Type" },
          ],
        },
      };
    });
    const tool = describeTypeTool(client);
    const out = parseResult(await tool.handler({ typeName: "linage" }));
    expect(out.notFound).toBe(true);
    expect(out.typeName).toBe("linage");
    const suggestions = out.suggestions as string[];
    expect(suggestions).toContain("Lineage");
    // Private __-prefixed types are filtered out of suggestions
    expect(suggestions).not.toContain("__Type");
  });
});

describe("catalog_run_graphql", () => {
  it("blocks mutations by default with a structured error", async () => {
    const client = makeMockClient(() => {
      throw new Error("handler should short-circuit before calling the server");
    });
    const tool = runGraphqlTool(client);
    const out = parseResult(
      await tool.handler({
        query: "mutation DeleteIt { deleteLineages(data: []) }",
      })
    );
    expect(out.blocked).toBe("mutation");
    expect(client.calls).toHaveLength(0);
  });

  it("allows mutations when allowMutations is true", async () => {
    const client = makeMockClient(() => ({
      data: { deleteLineages: true },
    }));
    const tool = runGraphqlTool(client);
    const out = parseResult(
      await tool.handler({
        query: "mutation { deleteLineages(data: []) }",
        allowMutations: true,
      })
    );
    expect(out.data).toMatchObject({ deleteLineages: true });
    expect(client.calls).toHaveLength(1);
  });

  it("strips # comments before mutation detection so commented-out mutation text isn't flagged", async () => {
    const client = makeMockClient(() => ({
      data: { getLineages: { totalCount: 0, data: [] } },
    }));
    const tool = runGraphqlTool(client);
    const out = parseResult(
      await tool.handler({
        query: "# mutation is in a comment\nquery { getLineages { totalCount data { id } } }",
      })
    );
    expect(out.blocked).toBeUndefined();
    expect(out.data).toBeDefined();
  });

  it("surfaces GraphQL validation errors verbatim rather than re-mapping them", async () => {
    const client = makeMockClient(() => ({
      data: null,
      errors: [
        {
          message: "Cannot query field \"brokenField\" on type \"Lineage\".",
          path: ["getLineages"],
        },
      ],
    }));
    const tool = runGraphqlTool(client);
    const out = parseResult(
      await tool.handler({
        query: "query { getLineages { brokenField } }",
      })
    );
    expect(out.errors).toBeDefined();
    const errors = out.errors as Array<Record<string, unknown>>;
    expect(errors[0].message).toMatch(/brokenField/);
  });

  it("passes operationName through for multi-op documents", async () => {
    const client = makeMockClient((_doc, vars) => {
      void vars;
      return { data: { ok: true } };
    });
    const tool = runGraphqlTool(client);
    await tool.handler({
      query: "query A { getLineages { totalCount } } query B { getTables { totalCount } }",
      operationName: "B",
    });
    expect(client.calls).toHaveLength(1);
  });

  it("scopes mutation detection to operationName when provided — mixed doc + selected query is allowed", async () => {
    const client = makeMockClient(() => ({ data: { ok: true } }));
    const tool = runGraphqlTool(client);
    const out = parseResult(
      await tool.handler({
        query: "query GetIt { getLineages { totalCount } } mutation ZapIt { deleteLineages(data: []) }",
        operationName: "GetIt",
      })
    );
    expect(out.blocked).toBeUndefined();
    expect(out.data).toBeDefined();
  });

  it("scopes mutation detection to operationName when provided — selected mutation is still blocked", async () => {
    const client = makeMockClient(() => {
      throw new Error("should short-circuit before calling the server");
    });
    const tool = runGraphqlTool(client);
    const out = parseResult(
      await tool.handler({
        query: "query GetIt { getLineages { totalCount } } mutation ZapIt { deleteLineages(data: []) }",
        operationName: "ZapIt",
      })
    );
    expect(out.blocked).toBe("mutation");
  });

  it("refuses allowMutations when the server is in read-only mode", async () => {
    const originalEnv = process.env.COALESCE_CATALOG_READ_ONLY;
    process.env.COALESCE_CATALOG_READ_ONLY = "true";
    try {
      const client = makeMockClient(() => {
        throw new Error("should short-circuit before calling the server");
      });
      const tool = runGraphqlTool(client);
      const out = parseResult(
        await tool.handler({
          query: "mutation { deleteLineages(data: []) }",
          allowMutations: true,
        })
      );
      expect(out.blocked).toBe("read_only_mode");
      expect(client.calls).toHaveLength(0);
    } finally {
      if (originalEnv === undefined) delete process.env.COALESCE_CATALOG_READ_ONLY;
      else process.env.COALESCE_CATALOG_READ_ONLY = originalEnv;
    }
  });
});
