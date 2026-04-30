import { READ_ONLY_ENV_VAR } from "./constants.js";

/**
 * Whether the server is running with mutation tools dropped at registration
 * time. Lives in its own module so callers (e.g. catalog_run_graphql, which
 * must refuse `allowMutations: true` in read-only mode) can import it without
 * pulling in `server.ts` — that would create a runtime cycle, since
 * `server.ts` already imports the introspection module that needs this check.
 */
export function isReadOnlyMode(): boolean {
  return process.env[READ_ONLY_ENV_VAR] === "true";
}
