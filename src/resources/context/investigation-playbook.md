# Customer Catalog Issue — Investigation Playbook

Structured flow for responding to Catalog-related tickets (Pylon, Slack, Salesforce). Adapt the steps to the specific question; the order below works for most missing-lineage / missing-description / missing-metadata reports.

## 1. Reproduce: resolve the user's asset reference

Users name assets by **warehouse path** (`PROD.ANALYTICS.ORDERS`) or **display name** ("the orders table"). The first step is always a UUID.

- Preferred: `catalog_find_asset_by_path` on the fully-qualified path. Handles quoted identifiers (`"prod"."analytics"."orders"`) and case folding.
- Fallback when the user gave a partial name: `catalog_search_tables` with `nameContains`, narrow by `schemaId` or `databaseId` if known.

If the asset can't be found:
- It may not be ingested yet — check `catalog_search_sources` + `catalog_search_databases` to confirm the source and database exist.
- Or the user may be referring to a deleted/hidden asset — retry with `withDeleted: true` and/or `withHidden: true`.

## 2. Confirm the symptom

Get the full asset picture with `catalog_summarize_asset` (1 call, ~5 sub-queries in parallel). This gives you:
- Description provenance (is it empty? external-only? generated?)
- Ownership (are owners assigned?)
- Tags (governance classifications)
- Lineage counts (0 upstream on a derived table is a red flag)
- Column count + quality-check count (gates for Tier-1 stewardship)

## 3. Diagnose (lineage-specific)

For "lineage looks wrong" tickets, run `catalog_trace_missing_lineage` — it emits structured findings with severity (info/warning/alert) and per-finding recommendations. Follow up on each:

- `no_upstream_table_lineage` → check the SQL that populates the table. Is it ingested from a source the Catalog supports for lineage? Recently added?
- `upstream_lineage_all_manual` → automatic detection failed. Could be an unsupported SQL construct (SELECT *, stored procs, dynamic SQL). Validate the source warehouse connection.
- `no_field_lineage` / `partial_field_lineage` → column-level lineage absent. Check if the source tool's extractor supports column-level parsing for this technology.

## 4. Diagnose (SQL usage / who queries this?)

- `catalog_get_table_queries` for the tableId — shows actual SQL. Filter by `queryType: SELECT` to exclude writes.
- `catalog_search_queries` (semantic) — useful when the user asks "what queries compute metric X?" rather than "what queries touch table Y?"

## 5. Diagnose (documentation / PII gaps)

- Undocumented columns: `catalog_search_columns` with `tableId: <id>, isDocumented: false`.
- PII sweep across a schema: `catalog_search_columns` with `schemaId`, `isPii: null` — report the counts by `isPii` status.
- Data quality coverage: `catalog_search_quality_checks` with `tableId: <id>` and interpret `status` distribution.

## 6. Remediate (if the user has a READ_WRITE token)

Highest-leverage fixes, in order:

1. **Description** — `catalog_update_column_metadata` (descriptionRaw for markdown) or `catalog_update_table_metadata` (externalDescription).
2. **Flags** — `isPii` / `isPrimaryKey` via `catalog_update_column_metadata`.
3. **Lineage** — `catalog_upsert_lineages` to patch missing table-level edges. Goes in as MANUAL_CUSTOMER lineageType.
4. **Tags** — `catalog_attach_tags` auto-creates the tag if the label is new.
5. **Ownership** — `catalog_upsert_user_owners` / `catalog_upsert_team_owners`.
6. **External links** — `catalog_create_external_links` to attach a runbook URL.

## 7. Confirm the fix

Re-run `catalog_summarize_asset` (or `catalog_trace_missing_lineage` for lineage fixes) and compare the findings.

## 8. Cross-check with Transform MCP

If the asset is produced by a Coalesce node, the same investigation may need Transform-side follow-up (e.g., the node's SQL is the actual root cause). See [ecosystem-boundaries](ecosystem-boundaries.md) for the handoff pattern.
