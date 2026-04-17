# Catalog vs Transform — Choosing the Right MCP

Two separate Coalesce MCP servers. They're complementary but have distinct data models — don't call one when the other is the right answer.

## coalesce-transform-mcp

**Use when** the user is building, running, or debugging pipeline definitions **inside** the Coalesce Transform product:
- Node creation, column mapping, join/aggregation configuration
- Pipeline runs (start, retry, status, diagnose)
- Environment / workspace / project management
- Git integration, deployments
- SQL generation inside a Coalesce node

Data model: workspaces, environments, nodes, runs, jobs, projects.

## coalesce-catalog-mcp (this server)

**Use when** the user is asking about **already-materialized** warehouse + BI assets:
- Discovery — "what tables are there in schema X?"
- Lineage — "what feeds into / reads from this table?"
- Governance — owners, tags, glossary terms, data products, pinned assets
- Usage signals — who queries this table, which dashboards depend on it
- Documentation + PII/quality flags

Data model: sources, databases, schemas, tables, columns, dashboards, lineage edges, tags, terms, users, teams, quality checks.

## The workflow seam

The two are connected: **Coalesce nodes materialize warehouse tables; those tables appear in the Catalog.**

For questions that span both worlds, call both and stitch results:
- "Where is the SQL for the table X that this dashboard reads?" → Catalog lineage (`catalog_get_lineages parentDashboardId: X`) → resolve to a table → find the Coalesce node that produces that table (via transform `list_workspace_nodes` filtered by output name).
- "Is this Coalesce node's output actually being used?" → transform `get_workspace_node` gives the output table name → resolve via `catalog_find_asset_by_path` → `catalog_get_lineages` downstream to see dashboards/queries.

## Don't

- Don't use catalog to edit Coalesce nodes (Transform owns that).
- Don't use transform to look up who owns a warehouse table or which dashboard depends on it (Catalog owns those).
- Don't rebuild lineage in Coalesce if it already exists in Catalog — inherit it.

## Installed alongside

Both servers can be configured in the same MCP client. Tools are namespaced by server name (`mcp__coalesce-transform-mcp__...` vs `mcp__coalesce-catalog-mcp__...`), and both servers prefix their own tool names (`coa_*` / `catalog_*`) to stay disambiguated even when exposed side-by-side.
