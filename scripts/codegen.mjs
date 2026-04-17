#!/usr/bin/env node
/**
 * Fetches the Catalog Public GraphQL schema via introspection, writes it to
 * src/generated/schema.graphql as SDL, then invokes `graphql-codegen` (via the
 * config in codegen.ts) to produce src/generated/types.ts.
 *
 * Env:
 *   COALESCE_CATALOG_API_KEY  (required) — auth token
 *   COALESCE_CATALOG_REGION   (optional) — "eu" (default) | "us"
 *   COALESCE_CATALOG_API_URL  (optional) — override full base URL
 */
import { spawnSync } from "node:child_process";
import { mkdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  buildClientSchema,
  getIntrospectionQuery,
  printSchema,
} from "graphql";

const REGION_BASE_URLS = {
  eu: "https://api.castordoc.com",
  us: "https://api.us.castordoc.com",
};

function resolveEndpoint() {
  const override = process.env.COALESCE_CATALOG_API_URL?.trim();
  if (override) return `${override.replace(/\/+$/, "")}/public/graphql`;
  const region = (process.env.COALESCE_CATALOG_REGION ?? "eu").toLowerCase();
  if (region !== "eu" && region !== "us") {
    throw new Error(`Invalid region "${region}"; expected "eu" or "us"`);
  }
  return `${REGION_BASE_URLS[region]}/public/graphql`;
}

async function fetchIntrospection(endpoint, token) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Token ${token}`,
      Accept: "application/json",
    },
    body: JSON.stringify({ query: getIntrospectionQuery() }),
  });
  if (!response.ok) {
    const body = await response.text().catch(() => "[unreadable]");
    throw new Error(
      `Introspection failed: HTTP ${response.status}\n${body.slice(0, 500)}`
    );
  }
  const payload = await response.json();
  if (payload.errors?.length) {
    throw new Error(
      `Introspection returned GraphQL errors:\n${JSON.stringify(payload.errors, null, 2)}`
    );
  }
  if (!payload.data) {
    throw new Error("Introspection returned no data");
  }
  return payload.data;
}

async function main() {
  const token = process.env.COALESCE_CATALOG_API_KEY?.trim();
  if (!token) {
    console.error("COALESCE_CATALOG_API_KEY is required");
    process.exit(1);
  }
  const endpoint = resolveEndpoint();

  const here = dirname(fileURLToPath(import.meta.url));
  const repoRoot = join(here, "..");
  const generatedDir = join(repoRoot, "src", "generated");
  const sdlPath = join(generatedDir, "schema.graphql");

  console.error(`[codegen] Introspecting ${endpoint}`);
  const introspection = await fetchIntrospection(endpoint, token);
  const schema = buildClientSchema(introspection);
  const sdl = printSchema(schema);

  mkdirSync(generatedDir, { recursive: true });
  const header =
    "# AUTO-GENERATED via `npm run codegen`. Do not edit manually.\n" +
    `# Source: ${endpoint}\n\n`;
  writeFileSync(sdlPath, header + sdl + "\n", "utf8");
  console.error(`[codegen] Wrote ${sdlPath}`);

  console.error("[codegen] Running graphql-codegen");
  const result = spawnSync(
    process.platform === "win32" ? "npx.cmd" : "npx",
    ["graphql-codegen", "--config", "codegen.ts"],
    { cwd: repoRoot, stdio: "inherit", shell: process.platform === "win32" }
  );
  if (result.status !== 0) process.exit(result.status ?? 1);

  console.error("[codegen] Done");
}

main().catch((err) => {
  console.error(err instanceof Error ? err.message : err);
  process.exit(1);
});
