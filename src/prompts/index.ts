import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";

/**
 * Reusable prompt templates exposed via the MCP prompts/list & prompts/get
 * protocol. Each template is an intentionally short, high-intent instruction
 * that nudges the model toward the correct Catalog tool sequence — not a
 * verbose playbook (the investigation playbook lives as a resource).
 */
export function registerCatalogPrompts(server: McpServer): void {
  server.registerPrompt(
    "catalog-start-here",
    {
      title: "Catalog: Start Here",
      description:
        "Orient to the Catalog MCP: the entity graph, how to resolve warehouse paths to UUIDs, and which tool answers which question.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Before calling any Catalog tool, read catalog://context/overview for the entity graph and catalog://context/tool-routing for the decision tree mapping user questions → tools. If the user gave a warehouse path like DB.SCHEMA.TABLE, call catalog_find_asset_by_path first to resolve it. If the user wants a single-asset overview, prefer catalog_summarize_asset over chaining individual get_* calls.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "catalog-asset-summary",
    {
      title: "Catalog: Summarize One Asset",
      description:
        "Given a warehouse path or Catalog UUID, produce a full cross-domain summary of a table or dashboard.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "If the user gave a warehouse path, first call catalog_find_asset_by_path to resolve it to a UUID. Then call catalog_summarize_asset with the right kind (TABLE or DASHBOARD). Present the core identity, ownership (users + teams), tags, upstream/downstream lineage counts, and — for tables — the column list and any quality-check results. Follow up with catalog_get_* tools only if the summary flags gaps the user cares about.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "catalog-find-consumers",
    {
      title: "Catalog: Find Consumers of an Asset",
      description:
        "Enumerate everything downstream of a table: dashboards, child tables, and SQL queries that read it.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Resolve the asset (catalog_find_asset_by_path if the user gave a path; catalog_search_tables if just a name). Then: (1) call catalog_get_lineages with parentTableId AND hydrate: true — the hydrated `parent`/`child` + `direction` fields remove the N+1 lookup pattern and give you readable asset names in one call. (2) Call catalog_get_table_queries to show SELECT-type queries that touched the table (filter queryType: SELECT). (3) Present the consumer map as a compact ASCII tree (see catalog://context/tool-routing for the tree format — one line per edge, `↑`/`↓` arrows, `lineageType` + ISO timestamp suffix). Dump raw JSON only if the user explicitly asks for it.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "catalog-investigate-lineage-gaps",
    {
      title: "Catalog: Investigate Lineage Gaps",
      description:
        "Diagnose why lineage looks incomplete for a given table, with structured findings and remediation suggestions.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Resolve the table (catalog_find_asset_by_path). Call catalog_trace_missing_lineage with the tableId. Summarize each finding the tool returns (severity + recommendation). For findings that warrant deeper inspection, follow up with catalog_get_lineages (hydrate: true) for table-level edges and catalog_get_field_lineages (hydrate: true) for column-level edges — render the results as an ASCII tree (see catalog://context/tool-routing) so the user can eyeball the shape, not the UUIDs. If the user has READ_WRITE access and wants to patch a gap, propose a specific catalog_upsert_lineages call and wait for explicit approval before executing.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "catalog-governance-rollout",
    {
      title: "Catalog: Governance Rollout Walkthrough",
      description:
        "Guide a new Catalog user through a phased 'best-in-class' data governance rollout — tiering, ownership, metadata, glossary, tags, lineage, data products, quality, review cadence.",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Read catalog://context/governance-rollout for the full 8-phase playbook, then have a grounded conversation with the user about where they are today and where to start.\n\n" +
              "Opening moves:\n" +
              "  1. Run catalog_search_sources + catalog_search_databases to establish what's actually in the catalog.\n" +
              "  2. Run catalog_search_tables sortBy:'popularity' sortDirection:'DESC' nbPerPage:25 to surface the top-25 candidates for Tier-1.\n" +
              "  3. For each of those 25, call catalog_summarize_asset and note: has owner? has description? has lineage? has quality checks?\n" +
              "  4. Present a short table of gaps. Recommend Phase 0 (ingestion audit) + Phase 1 (ownership assignment) as the first 2 weeks of work.\n\n" +
              "Do not dump the whole playbook at them. Pull specific phases on demand based on their questions. If they ask 'where do I start with tagging?', jump to Phase 4 and adapt it to what you saw in the catalog. Always ground advice in what actually exists in their account — not abstract principles.\n\n" +
              "If the user has READ_WRITE access and wants to execute a recommendation (tag, assign owner, push a description), propose the specific catalog_* mutation call and wait for explicit approval before executing.",
          },
        },
      ],
    })
  );

  server.registerPrompt(
    "catalog-audit-documentation",
    {
      title: "Catalog: Audit Documentation Coverage",
      description:
        "Produce a ranked report of undocumented assets in a scope (database, schema, or table).",
    },
    async () => ({
      messages: [
        {
          role: "user",
          content: {
            type: "text",
            text:
              "Resolve the scope the user is asking about — could be a databaseId (use catalog_search_schemas to drill down), schemaId, or single tableId. To find undocumented columns: catalog_search_columns with isDocumented: false and the appropriate scope filter. To find undocumented tables: catalog_search_tables in the scope, then filter the returned rows client-side by description absence or description length < 50. Report counts, the top offenders (by popularity DESC), and — if the user has READ_WRITE and asks — propose a catalog_update_column_metadata batch with suggested descriptions for the model to co-author.",
          },
        },
      ],
    })
  );
}
