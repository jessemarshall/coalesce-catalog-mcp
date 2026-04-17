import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

interface ResourceSpec {
  uri: string;
  name: string;
  description: string;
  file: string;
}

/**
 * Static markdown context resources. Each is loaded from its bundled file at
 * request time (no eager read) so the server starts fast and stays low-memory.
 *
 * URIs use the `catalog://context/<slug>` scheme — distinct from transform's
 * `coalesce://context/...` so both servers can coexist in a single client.
 */
const RESOURCES: ResourceSpec[] = [
  {
    uri: "catalog://context/overview",
    name: "Catalog Overview",
    description:
      "High-level tour of the Coalesce Catalog: entity graph (source → database → schema → table/column), cross-cutting annotations (tags, terms, data products, quality, ownership), and lineage provenance. Read this first if unfamiliar with the Catalog data model.",
    file: "context/overview.md",
  },
  {
    uri: "catalog://context/tool-routing",
    name: "Catalog Tool Routing Guide",
    description:
      "Decision tree mapping common user questions to the right Catalog MCP tool. Optimises for direct-tool answers over chained searches.",
    file: "context/tool-routing.md",
  },
  {
    uri: "catalog://context/ecosystem-boundaries",
    name: "Catalog vs Transform MCP Boundaries",
    description:
      "Which Coalesce MCP to reach for when. Covers the workflow seam where Transform-authored nodes materialize Catalog-indexed tables.",
    file: "context/ecosystem-boundaries.md",
  },
  {
    uri: "catalog://context/investigation-playbook",
    name: "Catalog Investigation Playbook",
    description:
      "Step-by-step flow for triaging Catalog-related customer issues (missing lineage, missing descriptions, PII gaps). Usable for Pylon/Slack/Salesforce tickets.",
    file: "context/investigation-playbook.md",
  },
];

function readResource(relativePath: string): string {
  // After `npm run build`, this file lives at dist/resources/index.js and the
  // markdown files at dist/resources/context/*.md. __dirname points to
  // dist/resources, so we resolve relatively to it.
  const absolute = join(__dirname, relativePath);
  return readFileSync(absolute, "utf-8");
}

export function registerCatalogResources(server: McpServer): void {
  for (const spec of RESOURCES) {
    server.resource(
      spec.name,
      spec.uri,
      { description: spec.description, mimeType: "text/markdown" },
      async (uri) => ({
        contents: [
          {
            uri: uri.toString(),
            mimeType: "text/markdown",
            text: readResource(spec.file),
          },
        ],
      })
    );
  }
}
