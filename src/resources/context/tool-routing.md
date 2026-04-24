# Catalog Tool Routing — Decision Tree

Pick the tool that directly answers the user's question. Don't chain general-purpose searches when a targeted tool exists.

## "Where do I start? / How do I roll this out?"

New Catalog users asking "what do I do first?" should be handed
the phased playbook at [catalog://context/governance-rollout](catalog://context/governance-rollout) — it covers tiering, ownership, metadata, glossary, tags, lineage, data products, and review cadence across an 8-12 week rollout. The `catalog-governance-rollout` prompt kicks off the walkthrough with live data from their account (top-25 tables, current owner/description coverage, etc.).

## "I have a warehouse path (DB.SCHEMA.TABLE or DB.SCHEMA.TABLE.COL)"

→ `catalog_find_asset_by_path` — always the first call. Returns the UUID you need for everything else.

## "Tell me about this table/dashboard" (one-shot, ONE asset)

→ `catalog_summarize_asset` — single call returns identity + owners + tags + lineage counts + columns + quality checks for **one** asset.

Bulk sections (columns, qualityChecks, lineage samples) over ~2 KB come back as `sampleUri: "catalog://cache/..."` resource URIs with `totalCount` / `returned` / `hasMore` still inline. Only dereference a `sampleUri` (via ReadResource) if that section is actually needed — pagination decisions can be made from the inline counts.

## "Tell me about THESE N tables/dashboards" (governance checks, bulk audits)

**Do NOT fan out to `catalog_summarize_asset` across many ids** — that produces N round trips and N × ~3 KB of context per call. Instead:

→ `catalog_search_tables({ ...scope, projection: "detailed" })` — one paginated call returns ownership, tags, descriptions, and schema context for every match. `projection: "detailed"` is the "many assets, full metadata" shape. Defaults to `"summary"` (identity + freshness + popularity) when omitted.

→ `catalog_search_dashboards({ ...scope, projection: "detailed" })` — same pattern for dashboards.

→ `catalog_search_terms({ ...scope, projection: "detailed" })` — same pattern for glossary terms. `detailed` adds `ownerEntities`, `teamOwnerEntities`, and `tagEntities` — use it when grading term health (missing owner, orphaned, untagged) in one paginated call.

Responses over 16 KB auto-externalize to a `catalog://cache/` resource URI, so a 500-row detailed page stays context-safe. Fetch the URI only when you actually need the rows.

**When to reach for `catalog_summarize_asset` instead:** you need lineage edges, column-level detail, or recent quality-check rows for a single asset. For flat list-shaped questions ("who owns every table in SCHEMA X?", "which dashboards in folder Y are unverified?", "show me the description coverage for database Z"), the search tools with `projection: "detailed"` are the right call.

## "Find assets by..."

| Need | Tool | Key filters |
|---|---|---|
| Tables by name | `catalog_search_tables` | `nameContains`, `pathContains`, `schemaId` |
| Columns by flag | `catalog_search_columns` | `isPii`, `isPrimaryKey`, `isDocumented`, `hasColumnJoins` |
| Dashboards by BI tool | `catalog_search_dashboards` | `sourceId`, `folderPath` |
| Tables/dashboards in a data product | `catalog_search_data_products` | `entityType`, `withTagId` |
| Tables/cols/dashboards matching a tag | `catalog_search_tags` then filter by `linkedTermId` | `labelContains` |

## "What feeds into / reads from this asset?" (lineage)

- Upstream of a table: `catalog_get_lineages` with `childTableId`
- Downstream of a table: `catalog_get_lineages` with `parentTableId`
- Upstream of a column: `catalog_get_field_lineages` with `childColumnId`
- Which dashboards read from table X: `catalog_get_lineages` with `parentTableId` + `withChildAssetType: DASHBOARD`
- Suspected lineage gap: `catalog_trace_missing_lineage` (heuristic diagnostic, not authoritative)

### Presenting lineage to a user — render a tree

The lineage tools return structured edge records (`{ id, direction, parent, child, lineageType, refreshedAtIso, ... }`) optimised for machine parsing. **When showing lineage to a human**, render a compact ASCII tree instead of dumping the JSON. The expected shape:

```
FCT_ORDERS  (coalesce.sample_data.FCT_ORDERS)
├─↑ WRK_ORDERS                         refreshed 2026-03-15
├─↓ DIM_CUSTOMER_LOYALTY               refreshed 2026-03-15
└─↓ V_SALES                            refreshed 2026-03-15
```

Conventions:

- `↑` = upstream (parent → this asset), `↓` = downstream (this asset → child)
- One line per edge. Columns: asset name, age or ISO timestamp
- For multi-hop trees from `catalog_explore_lineage` (if you've called it with depth > 1), indent each hop with `│  ` for the continuation and `├─` / `└─` for the last sibling
- Always call lineage tools with `hydrate: true` when a user is in the loop — skip it for agent-internal multi-step reasoning where IDs are enough
- If an endpoint returns `hydrationUnavailable: true` (dashboard fields), render as `DASHBOARD_FIELD <uuid>` and note the limitation once at the end of the tree
- The `lineageType` field (AUTOMATIC / MANUAL_CUSTOMER / MANUAL_OPS / OTHER_TECHNOS) is still present in the raw edge record — omit it from the tree unless the user explicitly asks about provenance, or unless the edge is non-AUTOMATIC (a manual edge on an otherwise auto-detected graph is usually a tell worth surfacing).

For field lineage the same shape applies, with column names in place of tables:

```
ORDER_TOTAL  (on FCT_ORDERS)
├─↑ o_totalprice    (on STG_ORDERS)       refreshed 2025-11-12
└─↓ total_sales     (on FCT_DAILY_SALES)  refreshed 2026-03-19
```

## "How is this table used?" (SQL-level)

- Every SQL query that touched these tables: `catalog_get_table_queries` (up to 50 tableIds, ALL/ANY filter).
- Semantic SQL search ("queries computing active users"): `catalog_search_queries` — natural language → 10 best matches.
- Observed JOIN relationships: `catalog_get_column_joins` (scope by `columnIds` for speed; `tableIds` can be slow on large accounts).

## "Who owns this?" / "Where's the runbook?"

- Owners: part of `catalog_summarize_asset` output, or `catalog_get_table` / `catalog_get_dashboard` detail.
- Runbooks / external URLs: surfaced as `externalLinks` on the detail endpoints.

## "What does user X own?" / "Who's on team Y?"

The public API exposes no user-by-email or team-by-name endpoint — every lookup begins with a client-side page-scan through `getUsers` / `getTeams`. Shape the scan to match what you already have:

- **You only have an email (or team name)** → `catalog_search_users({ projection: "detailed" })` (or `catalog_search_teams({ projection: "detailed" })`). Page-scans once, inlines `ownedAssetIds` (+ `memberIds` for teams) on every row, match client-side. One pass, no follow-up call.
- **You already have a `userId` / `teamId`** (e.g. surfaced by another tool) → `catalog_get_user_owned_assets` / `catalog_get_team_members` / `catalog_get_team_owned_assets`. These anchor on the UUID and page the asset/member list — useful when the list is too large to return inline.

Default `projection: "summary"` keeps rows compact (counts only, no UUID arrays); reach for `detailed` only when you need the IDs. Detailed responses over 16 KB auto-externalize to a `catalog://cache/` resource URI.

## "Grade this asset" (quality / documentation)

- Quality test results: `catalog_search_quality_checks` (scope by `tableId`).
- Documentation coverage: `catalog_search_columns` with `isDocumented: false` + `tableId` or `schemaId`.

## "Is this asset ready to promote?" (per-asset readiness)

→ `catalog_audit_data_product_readiness({ assetKind, assetId })` — one-shot eight-axis readiness report for a TABLE or DASHBOARD. Grades description / ownership / tags / column-doc coverage / upstream + downstream lineage edges / quality checks / verification with hardcoded thresholds, returns per-axis `status: "pass"|"warn"|"fail"|"na"`, `signals`, and actionable `gaps`. Overall `readyToPromote: true` iff no axis fails (warns are allowed).

DASHBOARD assets always report `na` for columnDocs and qualityChecks. Tables whose column count exceeds `columnSampleCap` (default 200) flag `sampled: true` on the columnDocs axis.

Reach for this instead of chaining `catalog_summarize_asset` + `catalog_search_columns` + two `catalog_get_lineages` calls + `catalog_search_quality_checks` when the question is "should we promote this one asset?"

## "Find every unowned table in this scope and tell me who should own them"

→ `catalog_resolve_ownership_gaps({ schemaId | databaseId | tableIds })` — scope an audit, find the unowned subset, gather per-table evidence (top query authors from recent queries + 1-hop upstream/downstream lineage-neighbor owners). Raw signals only, no confidence scores. Refuses loudly above 200 unowned tables — narrow via schemaId or a tighter tableIds batch.

Pair with `catalog_governance_scorecard` (size the ownership gap across a scope) then this tool (close it by picking owners from the evidence). Act on the evidence via `catalog_upsert_user_owners` / `catalog_upsert_team_owners`.

## "Fill in descriptions / tags / owners downstream from this table"

→ `catalog_propagate_metadata({ sourceTableId, axes, maxDepth, overwritePolicy, dryRun })` — compute a typed diff plan for propagating description / tags / owners from a source table along downstream lineage. Default is dry-run + `axes: ['description']` + `overwritePolicy: 'ifEmpty'` — the safest possible invocation.

Tags and owners are opt-in per-call (owner propagation is high-trust; don't default it). Non-dry-run requires MCP elicitation confirmation. Width caps + pagination ceilings match `catalog_assess_impact` — refuses rather than silently truncating on wide graphs. Partial-failure tracking is per-axis in the execution response.

## "Give me my daily Catalog to-do list" (owner cleanup)

→ `catalog_owner_scorecard({ email })` — per-owner hygiene scorecard. Enumerates every table/dashboard/term the user owns and categorises each by issue: thin description, PII/domain-tag coverage, new-asset window, certification, lineage gaps (isolated / upstream-only / downstream-only for tables), and term-specific health (missing owner, orphaned, uncertified).

Returns structured ID lists per category, sorted newest-first by `createdAt DESC`. Complete picture or explicit refusal — no silent truncation. For a rendered walkthrough with remediation prompts, invoke the `catalog-daily-guide` prompt instead of calling the tool directly.

## "So-and-so is leaving — plan how to hand off their stuff"

→ `catalog_reconcile_ownership_handoff({ email })` — departing-owner workflow. Enumerates every asset the user owns, scores each by `blastRadiusScore` (popularity × downstream consumer count × query volume), and gathers per-asset candidate-owner evidence (top query authors, 1-hop upstream/downstream neighbor owners). One paginated `getTeams` call tags each candidate with their team memberships. Aggregates across the whole portfolio into `candidateSummary[]` sorted by assets-covered DESC.

Capacity gate: 200 owned assets (larger portfolios are bulk reassignments, not per-asset handoffs — split by domain/schema). Distinct from `catalog_owner_scorecard` (grades hygiene of what's owned) and `catalog_resolve_ownership_gaps` (finds currently-unowned tables). Use when the trigger is "this person is leaving / changing roles," not "this asset has no owner."

Act on the plan via `catalog_upsert_user_owners` / `catalog_upsert_team_owners`, typically starting with the top-asset-count candidate.

## AI assistant (async)

1. `catalog_ask_assistant` — submits a question, returns a jobId.
2. `catalog_get_assistant_result` — poll until `status: COMPLETED`.
Use the assistant when the question needs RAG over the full catalog corpus (all descriptions, tags, lineage) — for structured queries, prefer the explicit tools above.

## Mutation routing (require READ_WRITE token)

| Change | Tool |
|---|---|
| Fix a description | `catalog_update_column_metadata` (descriptionRaw) or `catalog_update_table_metadata` (externalDescription) |
| Flag PII / primary key | `catalog_update_column_metadata` |
| Tag an asset | `catalog_attach_tags` (auto-creates the tag label if new) |
| Patch a missing lineage edge | `catalog_upsert_lineages` |
| Push a dbt/Monte Carlo result | `catalog_upsert_data_qualities` |
| Assign ownership | `catalog_upsert_user_owners` or `catalog_upsert_team_owners` |
| Create a new glossary term | `catalog_create_term` |

Mutations are filtered out when `COALESCE_CATALOG_READ_ONLY=true`.
