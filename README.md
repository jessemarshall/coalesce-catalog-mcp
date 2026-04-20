# coalesce-catalog-mcp

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

MCP server for the [Coalesce Catalog](https://coalesce.io/catalog) Public GraphQL API. Exposes tables, columns, dashboards, lineage, tags, glossary terms, data products, governance (owners/teams/quality/pinned assets), semantic SQL search, and the Catalog AI Assistant across your warehouse + BI stack. Ships composite workflow tools for resolving warehouse paths, summarising assets in one call, and diagnosing lineage coverage gaps.

Built as the catalog-side companion to [`coalesce-transform-mcp`](https://www.npmjs.com/package/coalesce-transform-mcp) — install both, keep the surfaces namespaced, let agents route between them.

---

## I want to…

|     | Task | Jump to |
| :-: | ---- | ------- |
| 🚀 | Get running in 2 minutes | [Quick Start](#quick-start) |
| 🧭 | Roll out Catalog governance from zero | [Governance rollout](#governance-rollout) |
| 📚 | Understand what context the server ships | [Context Resources](#context-resources) |
| 🔍 | Find a specific tool | [Tools](#tools) |
| 💬 | Trigger canned investigation prompts | [Prompts](#prompts) |
| 📦 | Walk through the full setup | [Full Installation](#full-installation) |
| 🔑 | Get an API token + pick a region | [Credentials](#credentials) |
| 🔒 | Lock down to read-only | [Safety model](#safety-model) |

---

## Quick Start

> [!TIP]
> **❄️ Snowflake Cortex Code + coalesce-catalog-mcp.** CoCo is Snowflake's AI coding CLI — it already authenticates to your warehouse and runs under your Snowflake role. Drop this MCP in and an agent can answer "who owns `PROD.SALES.ORDERS`, what feeds it, which dashboards read it?" inline with your SQL workflow — lineage, ownership, tags, and descriptions without leaving the terminal.

<details>
<summary><b>❄️ Install in Snowflake Cortex Code (CoCo)</b></summary>

**Why this pairing?** Cortex Code is Snowflake's AI coding CLI — it already authenticates to your warehouse, runs under your Snowflake role, and has native tools for querying live data. Add `coalesce-catalog-mcp` and a single agent session can resolve warehouse paths to Catalog UUIDs, walk lineage (upstream tables + downstream dashboards), check documentation/PII coverage, and push fixes (descriptions, ownership, tags, lineage patches) back to the Catalog — all alongside the SQL you're already writing.

One-liner (after [installing the Cortex Code CLI](https://docs.snowflake.com/en/user-guide/cortex-code/cortex-code-cli)):

```bash
cortex mcp add coalesce-catalog node /absolute/path/to/coalesce-catalog-mcp/dist/index.js
```

Or edit `~/.snowflake/cortex/mcp.json` directly:

```json
{
  "mcpServers": {
    "coalesce-catalog": {
      "type": "stdio",
      "command": "node",
      "args": ["/absolute/path/to/coalesce-catalog-mcp/dist/index.js"],
      "env": {
        "COALESCE_CATALOG_API_KEY": "<YOUR_TOKEN>",
        "COALESCE_CATALOG_REGION": "eu"
      }
    }
  }
}
```

Pair with [`coalesce-transform-mcp`](https://www.npmjs.com/package/coalesce-transform-mcp) in the same CoCo session for end-to-end reach: Transform builds the pipeline, Catalog tells you who consumes it.

</details>

<details>
<summary><b>Install in Claude Code (CLI)</b></summary>

Add an entry to `~/.claude.json`:

```json
{
  "mcpServers": {
    "coalesce-catalog": {
      "command": "node",
      "args": ["/absolute/path/to/coalesce-catalog-mcp/dist/index.js"],
      "env": {
        "COALESCE_CATALOG_API_KEY": "<YOUR_TOKEN>",
        "COALESCE_CATALOG_REGION": "eu"
      }
    }
  }
}
```

Restart Claude Code. Tools appear as `mcp__coalesce-catalog__*`.

</details>

<details>
<summary><b>Install in Claude Desktop</b></summary>

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) or the Windows equivalent and add the same `mcpServers` block as above. Full-quit and re-open Claude Desktop.

</details>

<details>
<summary><b>Install in Cursor</b></summary>

Paste into `.cursor/mcp.json` (project) or `~/.cursor/mcp.json` (global):

```json
{
  "mcpServers": {
    "coalesce-catalog": {
      "command": "node",
      "args": ["/absolute/path/to/coalesce-catalog-mcp/dist/index.js"],
      "env": {
        "COALESCE_CATALOG_API_KEY": "<YOUR_TOKEN>",
        "COALESCE_CATALOG_REGION": "eu"
      }
    }
  }
}
```

Restart Cursor or hit "Refresh MCP servers" in the chat UI.

</details>

<details>
<summary><b>Install in VS Code</b></summary>

VS Code's built-in MCP support reads `.vscode/mcp.json` (per-workspace) or the user-level settings JSON. Paste:

```json
{
  "mcpServers": {
    "coalesce-catalog": {
      "command": "node",
      "args": ["/absolute/path/to/coalesce-catalog-mcp/dist/index.js"],
      "env": {
        "COALESCE_CATALOG_API_KEY": "<YOUR_TOKEN>",
        "COALESCE_CATALOG_REGION": "eu"
      }
    }
  }
}
```

Reload the window (`Developer: Reload Window`) or restart VS Code.

</details>

<details>
<summary><b>Install in VS Code Insiders</b></summary>

Same config shape as VS Code stable — `.vscode/mcp.json` per-workspace, or user settings. Paste:

```json
{
  "mcpServers": {
    "coalesce-catalog": {
      "command": "node",
      "args": ["/absolute/path/to/coalesce-catalog-mcp/dist/index.js"],
      "env": {
        "COALESCE_CATALOG_API_KEY": "<YOUR_TOKEN>",
        "COALESCE_CATALOG_REGION": "eu"
      }
    }
  }
}
```

Reload the window or restart VS Code Insiders.

</details>

<details>
<summary><b>Install in Windsurf</b></summary>

Windsurf forks VS Code and keeps MCP config under `~/.codeium/windsurf/mcp_config.json`. Paste:

```json
{
  "mcpServers": {
    "coalesce-catalog": {
      "command": "node",
      "args": ["/absolute/path/to/coalesce-catalog-mcp/dist/index.js"],
      "env": {
        "COALESCE_CATALOG_API_KEY": "<YOUR_TOKEN>",
        "COALESCE_CATALOG_REGION": "eu"
      }
    }
  }
}
```

Restart Windsurf. Tools appear under the Cascade chat's MCP panel.

</details>

---

## Governance rollout

New to Catalog, or rolling it out across an org for the first time? The server ships an opinionated **best-in-class governance playbook** that takes a team from zero to a trusted, governed catalog in 8–12 weeks.

[**`catalog://context/governance-rollout`**](src/resources/context/governance-rollout.md) — 547-line playbook with:

- **Pre-flight checklist** — 6 organizational prerequisites (exec sponsor, named Steward with allocated time, success metric, stakeholder map, budget, incentive model). Hard-stop if any are missing.
- **9 sequenced phases** — ingestion audit → ownership → metadata → glossary → tagging → lineage → data products → quality checks → review cadence + incident response → adoption. Each phase has a goal, success criterion, effort estimate, and the specific `catalog_*` tool calls that execute it.
- **Tiering model** (T1/T2/T3) with capacity-grounded guidance (T1 ≤ 5 % of your table count) and how to pick the T1 list from popularity + lineage signals instead of gut instinct.
- **Roles** — 3 max (Catalog Steward, Domain Owner, Data Steward). Explicit refusal of the RACI-matrix antipattern.
- **Compliance coverage** — GDPR Data-Subject Request workflow via lineage walks, right-to-delete, retention/residency tags, regulation-scope tags (`regulation:gdpr|ccpa|hipaa|sox|pci`), audit-trail attachment.
- **Coalesce Transform integration** — description flow, ownership flow, lineage consistency between the two products.
- **RBAC** — Catalog role model, read-only mode, sensible token-scope defaults.
- **Incident response runbook** — 5-step flow using Catalog as impact-assessment source of truth.
- **Change management** — 7 tactics for driving adoption, ordered by effectiveness. Anti-patterns included.
- **KPIs** — 10 measurable targets with per-metric ownership, not vanity metrics.
- **Quick-start recipe** — 5 tool calls you can run on Monday morning to show progress by end of week.

Use the [`catalog-governance-rollout`](#prompts) MCP prompt to kick off a guided walkthrough that grounds advice in *your* account's live state (top-25 tables, current coverage gaps) rather than generic principles.

---

## Context Resources

The server ships **5 static markdown resources** under the `catalog://context/*` URI scheme. Your client fetches them via `resources/list` + `resources/read`; agents cite them to orient quickly or when routing between tools.

- **`catalog://context/overview`** — Entity graph (source → database → schema → table/column), cross-cutting annotations (tags, terms, data products, quality, ownership), lineage provenance, auth, and region setup.
- **`catalog://context/tool-routing`** — Decision tree mapping common user questions to the right tool. Use this if you're unsure whether to reach for `catalog_search_*`, `catalog_get_*`, or a composite workflow.
- **`catalog://context/ecosystem-boundaries`** — When to pick this server vs `coalesce-transform-mcp`. Explains the workflow seam where Transform-authored nodes materialise Catalog-indexed tables.
- **`catalog://context/investigation-playbook`** — Step-by-step flow for triaging Catalog-related customer tickets (missing lineage, missing descriptions, PII sweeps).
- **`catalog://context/governance-rollout`** — Best-in-class rollout playbook. See the [Governance rollout](#governance-rollout) section above for the summary.

Content lives in [`src/resources/context/`](src/resources/context) — edit the markdown, rebuild, and the server picks it up on restart. There's no per-user override mechanism yet; fork the repo or overlay your own server alongside for org-specific context.

---

## Tools

> [!NOTE]
>
> ### Legend
>
> - ✍️ **Write** — mutates Catalog state. Reversible (upsert/attach/create/update). Requires a READ_WRITE API token.
> - ⚠️ **Destructive** — deletes state. Not recoverable via this API.
> - **Read-only by default is OFF.** Set `COALESCE_CATALOG_READ_ONLY=true` to drop every ✍️ and ⚠️ tool at server startup.

<!-- start of tool reference -->

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/project-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/project-light.png"><img src="docs/icons/project-light.png" width="28" height="28" alt="project"></picture> <b>Discovery</b> &mdash; resolve warehouse paths, browse the source → schema hierarchy</summary>

**Path resolution**

- **`catalog_find_asset_by_path`** - Resolve a dotted warehouse path (`DATABASE.SCHEMA.TABLE` or `...TABLE.COLUMN`) to a Catalog UUID. Walks the hierarchy; returns structured diagnostics on ambiguity or not-found.

**Source → database → schema hierarchy**

- **`catalog_search_sources`** - List connected sources (warehouses, BI tools, transform tools, quality tools). Filter by technology, type, origin.
- **`catalog_search_databases`** - List warehouse databases. Scope by `sourceIds` or name.
- **`catalog_search_schemas`** - List warehouse schemas. Scope by `databaseIds`, `sourceIds`, or name.

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/file-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/file-light.png"><img src="docs/icons/file-light.png" width="28" height="28" alt="file"></picture> <b>Tables</b> &mdash; search, detail, SQL usage, metadata writes</summary>

**Read**

- **`catalog_search_tables`** - Find tables by substring/path/scope. Sort by name, popularity, `levelOfCompletion`, etc.
- **`catalog_get_table`** - Full detail by UUID — description provenance, owners (users + teams), tags, external links, schema + database context.
- **`catalog_get_table_queries`** - SQL queries that touched given tableIds (max 50). Filter by SELECT/WRITE, ALL/ANY filter mode.

**Write**

- ✍️ **`catalog_update_table_metadata`** - Batch update `name` / `externalDescription` / `tableType` / `url` / `externalId` (max 500 per call).

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/repo-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/repo-light.png"><img src="docs/icons/repo-light.png" width="28" height="28" alt="repo"></picture> <b>Columns</b> &mdash; search, detail, observed joins, metadata writes</summary>

**Read**

- **`catalog_search_columns`** - Find columns by name/table/schema/database/source. Boolean filters for `isPii`, `isPrimaryKey`, `isDocumented`, `hasColumnJoins`.
- **`catalog_get_column`** - Full detail by UUID, including description provenance and tags. Use `tableId` from the response with `catalog_get_table` for parent context (the public API forbids the nested `table` relation on column queries).
- **`catalog_get_column_joins`** - Warehouse-observed JOIN relationships between columns, ranked by count. Useful for discovering de-facto foreign keys.

**Write**

- ✍️ **`catalog_update_column_metadata`** - Batch update `descriptionRaw` (Catalog-native markdown), `externalDescription`, `isPii`, `isPrimaryKey` (max 500 per call).

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/workflow-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/workflow-light.png"><img src="docs/icons/workflow-light.png" width="28" height="28" alt="workflow"></picture> <b>Dashboards</b> &mdash; search, detail</summary>

- **`catalog_search_dashboards`** - Find BI dashboards (Tableau, Looker, Sigma, Mode, Power BI, etc.). Scope by `sourceId`, `folderPath`, or name.
- **`catalog_get_dashboard`** - Full detail by UUID, including ownership, tags, folder location, external slug/URL, verification state.

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/git-branch-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/git-branch-light.png"><img src="docs/icons/git-branch-light.png" width="28" height="28" alt="git-branch"></picture> <b>Lineage</b> &mdash; asset and column-level edges, diagnostics, writes</summary>

**Read**

- **`catalog_get_lineages`** - Table / dashboard lineage edges. Scope by `parentTableId` (downstream), `childTableId` (upstream), `withChildAssetType`, `lineageType`.
- **`catalog_get_field_lineages`** - Column / dashboard-field edges. Scope is required — unscoped calls time out. Pair with `catalog_search_columns` to fan out.

**Composite workflow**

- **`catalog_get_column_lineage`** - Complete column-level lineage graph for a starting column. Accepts an FQN (`DATABASE.SCHEMA.TABLE.COLUMN`) or UUID, resolves it, walks the BFS **exhaustively with no depth cap**, and batch-resolves every reached column id to `{ name, fqn, tableName, schemaName, databaseName }`. Returns nodes + edges as a DAG (handles cycles + shared children). Configurable `maxNodes` safety ceiling (default 10000) guards against pathological graphs.
- **`catalog_trace_missing_lineage`** - Heuristic diagnostic: probes a table's upstream/downstream counts, lineage provenance, and column-level coverage %; returns `findings[]` with severity + remediation suggestions.

**Write**

- ✍️ **`catalog_upsert_lineages`** - Create/refresh table↔table, table↔dashboard, dashboard↔dashboard edges. Each edge must declare exactly one parent + one child (Zod-enforced). Batch up to 500.
- ⚠️ **`catalog_delete_lineages`** - Delete edges by endpoints. Batch up to 500.

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/book-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/book-light.png"><img src="docs/icons/book-light.png" width="28" height="28" alt="book"></picture> <b>Tags, terms, data products</b> &mdash; the cross-cutting annotation layer</summary>

**Read**

- **`catalog_search_tags`** - Tag labels, colors, linked terms.
- **`catalog_search_terms`** - Glossary terms (hierarchy via `parentTermId` + `depthLevel`, linked tag). Set `projection: "detailed"` to inline `ownerEntities`, `teamOwnerEntities`, and `tagEntities` for term-health audits.
- **`catalog_search_data_products`** - Assets promoted to curated data products. Filter by `entityType` (TABLE/DASHBOARD/TERM), `withTagId`.

**Write**

- ✍️ **`catalog_attach_tags`** - Bind tags to entities by label. Auto-creates the tag label if new. Batch up to 500.
- ⚠️ **`catalog_detach_tags`** - Remove tag bindings (does not delete the tag itself). Batch up to 500.
- ✍️ **`catalog_create_term`** - Create a glossary term, optionally under a parent and/or linked to a tag.
- ✍️ **`catalog_update_term`** - Update fields in place. Pass `parentTermId: null` to move to root, `linkedTagId: null` to unlink.
- ⚠️ **`catalog_delete_term`** - Hard delete. Children become orphans.

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/shield-lock-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/shield-lock-light.png"><img src="docs/icons/shield-lock-light.png" width="28" height="28" alt="shield-lock"></picture> <b>Governance</b> &mdash; users, teams, quality checks, pinned assets, owner/team writes</summary>

**Read**

- **`catalog_search_users`** - List Catalog users (id, email, role, `ownedAssetIds`).
- **`catalog_search_teams`** - List teams (members, Slack routing, `ownedAssetIds`).
- **`catalog_search_quality_checks`** - Data-quality test results (dbt, Monte Carlo, Soda, Great Expectations, etc.). Scope by `tableId`.
- **`catalog_search_pinned_assets`** - Curated "see also" links between catalog entities.

**Ownership writes**

- ✍️ **`catalog_upsert_user_owners`** - Mark a user as owner of N assets.
- ⚠️ **`catalog_remove_user_owners`** - Strip ownership (specific targets or all).
- ✍️ **`catalog_upsert_team_owners`** - Team equivalent of upsert.
- ⚠️ **`catalog_remove_team_owners`** - Team equivalent of remove.

**Team management**

- ✍️ **`catalog_upsert_team`** - Create-or-update by unique team name. Enforces Slack `#channel` / `@group` prefixes.
- ✍️ **`catalog_add_team_users`** - Add members by email (must be existing Catalog users).
- ⚠️ **`catalog_remove_team_users`** - Remove members by email.

**Quality checks**

- ✍️ **`catalog_upsert_data_qualities`** - Push check results for one table. Nested `qualityChecks[]` under a single `tableId`.
- ⚠️ **`catalog_remove_data_qualities`** - Remove checks by `(tableId, externalId)` composite keys.

**External links**

- ✍️ **`catalog_create_external_links`** - Attach URLs to tables (GITHUB / GITLAB / AIRFLOW / OTHER).
- ✍️ **`catalog_update_external_links`** - Update the URL of an existing link.
- ⚠️ **`catalog_delete_external_links`** - Remove links by id.

**Pinned assets**

- ✍️ **`catalog_upsert_pinned_assets`** - Curated `{from, to}` cross-asset pointers. Types: COLUMN / DASHBOARD / DASHBOARD_FIELD / TABLE / TERM.
- ⚠️ **`catalog_remove_pinned_assets`** - Remove by endpoints.

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/beaker-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/beaker-light.png"><img src="docs/icons/beaker-light.png" width="28" height="28" alt="beaker"></picture> <b>AI</b> &mdash; semantic SQL search and the Catalog Assistant</summary>

- **`catalog_search_queries`** - Semantic (natural-language) search over ingested SQL queries. Returns up to 10 matches with query text + author + referenced tableIds. Optional `tableIds` (max 10) + `filterMode` (ALL/ANY) for scoping.
- **`catalog_ask_assistant`** - Kick off an async AI Assistant job against the Catalog's RAG index. Returns a jobId.
- **`catalog_get_assistant_result`** - Poll a job; returns `status` (ADDED/ACTIVE/COMPLETED/FAILED/RETRIES_EXHAUSTED) plus `answer` + referenced `assets[]` when completed.

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/tools-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/tools-light.png"><img src="docs/icons/tools-light.png" width="28" height="28" alt="tools"></picture> <b>Introspection / escape hatch</b> &mdash; GraphQL schema describe + raw query passthrough</summary>

- **`catalog_describe_type`** - Introspect a GraphQL type on the Catalog Public API. Returns kind, description, fields (OBJECT / INTERFACE), inputFields (INPUT_OBJECT), or enumValues (ENUM). Each field is rendered as native GraphQL SDL (`[String!]!`) with an `isRequired` flag. On miss, returns near-match suggestions via Levenshtein + substring matching. Use for API-shape questions ("does getLineages accept a column scope?", "what's in GetFieldLineagesScope?").
- **`catalog_run_graphql`** - Execute an arbitrary GraphQL query or mutation against the Catalog Public API. Returns the raw response envelope (`data`, `errors`, `extensions`) unchanged — validation errors come through verbatim so you can debug them. Mutations are blocked by default; pass `allowMutations: true` to opt in. **Escape hatch, not the default path** — reach for the structured tools (`catalog_summarize_asset`, `catalog_get_column_lineage`, etc.) whenever they fit.

</details>

<details>

<summary><picture><source media="(prefers-color-scheme: dark)" srcset="docs/icons/tools-dark.png"><source media="(prefers-color-scheme: light)" srcset="docs/icons/tools-light.png"><img src="docs/icons/tools-light.png" width="28" height="28" alt="tools"></picture> <b>Composite workflows</b> &mdash; one-call multi-tool orchestrations</summary>

- **`catalog_find_asset_by_path`** - Resolve a dotted warehouse path to a UUID. (See [Discovery](#discovery) above.)
- **`catalog_summarize_asset`** - Full cross-domain overview of a TABLE or DASHBOARD in a single call: identity + ownership + tags + lineage counts + (for tables) columns + quality checks. Sub-queries run in parallel via `Promise.allSettled`; caller-controllable limits per section.
- **`catalog_trace_missing_lineage`** - Lineage coverage diagnostic. See [Lineage](#lineage--asset-and-column-level-edges-diagnostics-writes) above.
- **`catalog_assess_impact`** - Deprecation blast-radius report for a TABLE or DASHBOARD. Walks downstream lineage (depth 1-3, paginated exhaustively per node), batch-enriches every reached asset with ownership + popularity, and returns a 0-100 severity score with per-component rationale. **Completeness contract:** refuses with an explicit error when the depth-2 graph exceeds 2000 distinct nodes (or 500 at depth 3) — never returns a silently truncated report. Surfaces `distinctOwnerTeamCount` (teams to coordinate with) and `unownedCount` (orphaned downstream).
- **`catalog_governance_scorecard`** - Coverage matrix per database, schema, or explicit `tableIds` list. Per-table flags for ownership / description / column-doc % / tag count, plus an optional 5th axis (`includeQualityCoverage: true` adds quality-check coverage). Aggregate `governanceScore` is popularity-weighted by default (matching Health-dashboard semantics); pass `weighting: 'equal'` for one-table-one-vote audits. Refuses scopes >500 tables.

</details>

<!-- end of tool reference -->

---

## Prompts

Six reusable prompt templates kick off common workflows without re-reciting the tool chain. Invoke via `prompts/get` in your MCP client (in Claude Code, type `/` and look for the catalog- prefixed entries).

- **`catalog-start-here`** - Orientation: reads `overview` + `tool-routing` context and gives the model the routing defaults it should follow.
- **`catalog-governance-rollout`** - Kicks off the [governance playbook](#governance-rollout) walkthrough grounded in your account's live state (runs source / database / top-25 table sweeps first, then phase-by-phase recommendations).
- **`catalog-asset-summary`** - Given a path or UUID, run `find_asset_by_path` + `summarize_asset` and present the result.
- **`catalog-find-consumers`** - Enumerate everything downstream of a table: child tables, dashboards, and SQL queries that read it.
- **`catalog-investigate-lineage-gaps`** - Run `trace_missing_lineage` and walk each finding with a proposed remediation (upsert_lineages call) — asks for approval before executing.
- **`catalog-audit-documentation`** - Undocumented-column / undocumented-table report across a scope (database, schema, or table).

---

## Full Installation

**Requirements:**

- [Node.js](https://nodejs.org/) 22+ (works on 20 with an engine-mismatch warning)
- A Coalesce Catalog account and a Public-API token (Catalog UI → Settings → API tokens)
- An MCP-compatible client (see [Quick Start](#quick-start))

**1. Install the package.** The [Quick Start](#quick-start) snippets all use `npx -y coalesce-catalog-mcp@preview`, which npm fetches on first invocation — no explicit install needed. If you prefer a pinned global install:

```bash
npm install -g coalesce-catalog-mcp@preview
```

**2. Register with your MCP client** via one of the [Quick Start](#quick-start) paths.

**3. Restart the client** and try the `/catalog-start-here` prompt (or whatever the slash-command UX is in your client). The agent should list the 4 context resources and 57 tools. If you get an auth error, double-check `COALESCE_CATALOG_API_KEY` has the right scope — READ tokens work on every query tool but mutations require READ_WRITE.

### Credentials

<!-- ENV_METADATA_CORE_TABLE_START -->
| Variable | Description | Default |
| -------- | ----------- | ------- |
| `COALESCE_CATALOG_API_KEY` | Public-API token from the Catalog UI (Settings → API tokens). **Required.** READ tokens work on every query tool; mutations require a READ_WRITE token. | — |
| `COALESCE_CATALOG_REGION` | Catalog region: `eu` or `us`. Selects the default base URL. | `eu` |
| `COALESCE_CATALOG_API_URL` | Full base URL override. The path `/public/graphql` is appended automatically. | region-derived |
| `COALESCE_CATALOG_READ_ONLY` | When `true`, every mutation tool is filtered out at server registration time (57 tools → 34). | `false` |
<!-- ENV_METADATA_CORE_TABLE_END -->

**Region base URLs:**

- EU (default): `https://api.castordoc.com`
- US: `https://api.us.castordoc.com`

> Profile-file support (e.g. `~/.coalesce/catalog-profiles.yml`, matching the `~/.coa/config` pattern in [`coalesce-transform-mcp`](https://www.npmjs.com/package/coalesce-transform-mcp)) is on the roadmap but not shipped. Today the server reads env vars only.

### Safety model

Two layers keep destructive operations from happening by accident.

- **Tool annotations.** Every tool carries MCP `readOnlyHint` / `destructiveHint` / `idempotentHint`. The ✍️ and ⚠️ markers in [Tools](#tools) track `readOnlyHint: false` and `destructiveHint: true` respectively.
- **`COALESCE_CATALOG_READ_ONLY=true`** hides all 23 mutation tools at server registration time. Use it for audits, agent sandboxes, or pairing with a prod token. When set, the server registers 34 tools instead of 57.

Mutation tools additionally require a READ_WRITE API token on the server side — a READ token returns `AuthorizationError` at call time regardless of client config.

---

## Links

| | Resource | |
| :-: | :-- | :-- |
| 📘 | [Coalesce Catalog Docs](https://docs.coalesce.io/docs/catalog) | Product documentation |
| 🔌 | [Catalog Public API](https://docs.coalesce.io/docs/catalog/public-api) | GraphQL API reference |
| 🔁 | [`coalesce-transform-mcp`](https://www.npmjs.com/package/coalesce-transform-mcp) | Transform-side companion server |
| 🔗 | [Model Context Protocol](https://modelcontextprotocol.io/) | MCP spec & ecosystem |

---

## Contributing

Issues and PRs welcome once we open the repo publicly.

- 🐛 **Bug reports** — via GitHub issues
- 💡 **Feature requests** — via GitHub discussions

## License

[MIT](LICENSE) — built on top of the open [Model Context Protocol](https://modelcontextprotocol.io/).
