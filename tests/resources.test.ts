import { describe, it, expect } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const RESOURCE_FILES = [
  "overview.md",
  "tool-routing.md",
  "ecosystem-boundaries.md",
  "investigation-playbook.md",
];

describe("resource markdown files", () => {
  const srcDir = join(__dirname, "..", "src", "resources", "context");

  it("every registered resource has a backing markdown file in src/", () => {
    for (const file of RESOURCE_FILES) {
      const path = join(srcDir, file);
      expect(existsSync(path), `missing ${path}`).toBe(true);
    }
  });

  it("every resource file is non-trivial (> 200 chars)", () => {
    for (const file of RESOURCE_FILES) {
      const contents = readFileSync(join(srcDir, file), "utf-8");
      expect(contents.length).toBeGreaterThan(200);
    }
  });

  it("overview references the entity graph", () => {
    const contents = readFileSync(join(srcDir, "overview.md"), "utf-8");
    expect(contents).toMatch(/Entity graph|source.*database.*schema/i);
  });

  it("tool-routing lists the key tool prefixes", () => {
    const contents = readFileSync(join(srcDir, "tool-routing.md"), "utf-8");
    expect(contents).toContain("catalog_find_asset_by_path");
    expect(contents).toContain("catalog_summarize_asset");
    expect(contents).toContain("catalog_get_lineages");
  });

  it("ecosystem-boundaries mentions transform and catalog", () => {
    const contents = readFileSync(
      join(srcDir, "ecosystem-boundaries.md"),
      "utf-8"
    );
    expect(contents).toMatch(/transform/i);
    expect(contents).toMatch(/catalog/i);
  });
});
