import { READ_ONLY_ENV_VAR } from "./constants.js";

// Lives in its own module so mcp/introspection.ts can read this without forming a cycle through server.ts.
export function isReadOnlyMode(): boolean {
  return process.env[READ_ONLY_ENV_VAR] === "true";
}
