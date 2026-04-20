import { randomUUID } from "node:crypto";
import { buildCacheUri, buildRelPath } from "./paths.js";
import { writeJsonArtifact } from "./store.js";

export const EXTERNALIZE_RESPONSE_THRESHOLD = 16 * 1024;
export const EXTERNALIZE_SECTION_THRESHOLD = 2 * 1024;

export interface ExternalizedPointer {
  externalized: true;
  resourceUri: string;
  byteSize: number;
  note: string;
}

export interface ExternalizeOptions {
  toolName: string;
  section?: string;
  threshold: number;
}

/**
 * If `payload` serializes to more than `threshold` bytes, write it to the
 * session cache and return a small pointer the model can follow via
 * ReadResource. Otherwise return the payload unchanged.
 *
 * The byte-size check uses the same JSON.stringify the MCP transport will
 * use, so the threshold is an accurate proxy for context footprint.
 */
export function externalizeIfLarge<T>(
  payload: T,
  opts: ExternalizeOptions
): T | ExternalizedPointer {
  const serialized = JSON.stringify(payload);
  const byteSize = Buffer.byteLength(serialized, "utf8");
  if (byteSize <= opts.threshold) return payload;

  const relPath = buildRelPath(opts.toolName, opts.section, randomUUID());
  const artifact = writeJsonArtifact(relPath, payload);
  return {
    externalized: true,
    resourceUri: buildCacheUri(artifact.relPath),
    byteSize: artifact.byteSize,
    note: `Payload exceeded ${opts.threshold} bytes; fetch via ReadResource on resourceUri to read the full contents.`,
  };
}

export function isExternalizedPointer(value: unknown): value is ExternalizedPointer {
  return (
    typeof value === "object" &&
    value !== null &&
    (value as ExternalizedPointer).externalized === true &&
    typeof (value as ExternalizedPointer).resourceUri === "string"
  );
}
