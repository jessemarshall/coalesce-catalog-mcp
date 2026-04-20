import {
  McpServer,
  ResourceTemplate,
} from "@modelcontextprotocol/sdk/server/mcp.js";
import { statSync } from "node:fs";
import { buildCacheUri, resolveCacheUri } from "../cache/paths.js";
import { listSessionArtifacts, readArtifact } from "../cache/store.js";

/**
 * Register the dynamic `catalog://cache/{key}` resource. A single
 * ResourceTemplate covers every artifact in the session cache dir — `list()`
 * enumerates disk on demand and the reader resolves the base64url key back to
 * an absolute path, rejecting any URI that tries to escape the session dir.
 */
export function registerCacheResource(server: McpServer): void {
  server.resource(
    "Catalog Cache Artifact",
    new ResourceTemplate("catalog://cache/{key}", {
      list: async () => ({
        resources: listSessionArtifacts().map((a) => ({
          uri: buildCacheUri(a.relPath),
          name: a.relPath,
          mimeType: "application/json",
        })),
      }),
    }),
    {
      description:
        "Dynamic per-session cache of large tool responses. Tools return a URI instead of inlining payloads over ~16 KB so the agent can fetch them on demand.",
    },
    async (uri) => {
      const resolved = resolveCacheUri(uri.toString());
      if (!resolved) {
        throw new Error(`Unknown cache resource: ${uri.toString()}`);
      }
      try {
        statSync(resolved.absPath);
      } catch {
        throw new Error(
          `Cache artifact not found (likely evicted or from a previous session): ${uri.toString()}`
        );
      }
      return {
        contents: [
          {
            uri: uri.toString(),
            mimeType: "application/json",
            text: readArtifact(resolved.absPath),
          },
        ],
      };
    }
  );
}
