# Coalesce Catalog — Overview

The Coalesce Catalog (formerly Castor) is a data catalog: a searchable index of the warehouse + BI assets your organization uses, with descriptions, lineage, ownership, tags, and usage signals. This MCP server exposes the Catalog's Public GraphQL API.

## Entity graph

Assets form a hierarchy rooted in the **Source** (an ingested warehouse or BI tool):

```
Source (Snowflake, Tableau, dbt, ...)
  └─ Database (Snowflake DB, BigQuery project, Databricks catalog)
       └─ Schema
            └─ Table  ─┐       
                 └─ Column       
       └─ Dashboard    │
            └─ Dashboard Field
```

UUIDs from `catalog_search_sources` flow down as `sourceId` on `catalog_search_databases`, whose `id` becomes `databaseId` on `catalog_search_schemas`, and so on. When in doubt about how to scope a query, walk this graph from the top.

## Cross-cutting annotations

- **Tags** — reusable labels attached to any asset. `catalog_attach_tags` auto-creates missing tag labels.
- **Terms** — glossary definitions that form a hierarchy (`parentTermId`). A term can be linked 1:1 with a tag so tagged entities inherit the term's context.
- **Data products** — assets promoted to a curated surface; governance-approved and discoverable.
- **Owners** — users and teams with stewardship over specific assets.
- **Quality checks** — test results (dbt, Monte Carlo, Soda, etc.) registered against a table/column.
- **External links** — URLs on tables (runbooks, GitHub, Airflow runs).
- **Pinned assets** — hand-curated "see also" relationships between any two catalog entities.

## Lineage

Two independent graphs:

- **Asset lineage** (`catalog_get_lineages`) — edges between tables and dashboards. One parent × one child per row.
- **Field lineage** (`catalog_get_field_lineages`) — edges between columns and dashboard-fields. Much denser than asset lineage; requires at least one scope filter or the API will reject the query as too broad.

Provenance is surfaced via `lineageType`: **AUTOMATIC** (inferred by Catalog), **MANUAL_CUSTOMER** (via public API), **MANUAL_OPS** (Catalog ops team), **OTHER_TECHNOS** (imported from dbt/etc.). All-manual usually indicates automatic detection failed for that asset.

## When to use what

- Don't have a UUID yet? Start with `catalog_find_asset_by_path` (warehouse path → id).
- Need everything about one asset? `catalog_summarize_asset` bundles 5 queries into one call.
- Diagnosing lineage holes? `catalog_trace_missing_lineage`.
- Free-text/semantic search over historical SQL? `catalog_search_queries`.

## Authentication + region

Configure via env:
- `COALESCE_CATALOG_API_KEY` (required)
- `COALESCE_CATALOG_REGION` (`eu` default | `us`)
- `COALESCE_CATALOG_READ_ONLY=true` drops all mutation tools at startup.

READ tokens work on all query tools; mutations require READ_WRITE.
