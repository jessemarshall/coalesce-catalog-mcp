import type { CodegenConfig } from "@graphql-codegen/cli";

/**
 * graphql-codegen config. Consumes the SDL emitted by scripts/codegen.mjs
 * and produces plain TypeScript types only — no resolvers, no hooks, no
 * framework-specific plugins. Zod runtime validators are hand-written and
 * cross-checked against these types with `satisfies`.
 */
const config: CodegenConfig = {
  schema: "src/generated/schema.graphql",
  generates: {
    "src/generated/types.ts": {
      plugins: ["typescript"],
      config: {
        enumsAsTypes: true,
        skipTypename: true,
        useTypeImports: true,
        avoidOptionals: false,
        scalars: {
          UUID: "string",
          DateTime: "string",
          Date: "string",
          JSON: "unknown",
          JSONObject: "Record<string, unknown>",
        },
      },
    },
  },
};

export default config;
