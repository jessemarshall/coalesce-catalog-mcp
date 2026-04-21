# Data Governance Rollout — Best-in-Class Catalog Playbook

A sequenced, opinionated method for a team starting from zero to reach a governed, trusted Catalog state in **8–12 weeks**. Every phase has an owner, a success criterion, effort estimates, and the specific `catalog_*` tools that execute it.

Skip the theory if your program is already mature; jump to **Phase X** of your choice. If this is your first rollout, read the **Pre-flight** section below before touching the Catalog — most governance programs die from organizational misalignment, not execution.

> **Why phased, not parallel.** Assigning owners before descriptions exist gives each owner an empty TODO list. Writing descriptions before tiers exist burns steward hours on low-value assets. Rolling out tags before a glossary exists produces a mess of inconsistent labels you'll rename later. Order matters. The [phase dependency diagram](#phase-dependency-map) below shows which phases can overlap safely and which must be sequential.

---

## Pre-flight — organizational prerequisites

Catalog launches fail more often from missing prerequisites than missing tooling. Tick these before Phase 0.

| Prerequisite | Who signs off | What you need to see |
|---|---|---|
| **Named executive sponsor** | VP / C-level in the data org | Email or doc with the sponsor's commitment to fund and defend the program for at least 2 quarters. |
| **Named Catalog Steward with allocated time** | Sponsor | 20 % of one person's role minimum; 50 % during Phase 0–2. Not "a rotating responsibility." |
| **Agreed success metric tied to the sponsor's OKRs** | Sponsor + Steward | One measurable thing the sponsor will care about in 90 days. Examples: "P1 incident MTTR drops 30 % because lineage now reaches dashboards" or "New-hire ramp on any T1 asset is <1 day." |
| **Stakeholder map of Domain Owners** | Steward | Spreadsheet naming the owner per business domain (Sales, Marketing, Finance, Product, Ops, Data Platform). Must be actual humans with budget, not just org-chart seats. |
| **Budget for tooling + training** | Sponsor | Licences (if applicable) + 1 h/quarter of every Data Steward's time for refreshers + a small budget for the launch announcement. |
| **Incentive model for Data Stewards** | Steward + HR/ops | How stewardship shows up on a performance review. Without this, stewards deprioritize Catalog work within 60 days. |

**Hard stop**: if you can't fill all six rows, a Catalog rollout will look busy for 90 days and then quietly die. Fix the gap first. Come back to Phase 0.

---

## The maturity you're aiming for

A "best-in-class" Catalog state isn't 100 % coverage of every asset. It's:

1. **Every Tier-1 asset has a human owner** who's accountable and reachable.
2. **Every Tier-1 asset has a plain-English description** that a new hire could use.
3. **Sensitive data (PII, financial, regulated) is tagged and classified** with retention + residency metadata.
4. **Lineage — asset and column level — is complete for Tier-1** and audited for Tier-2.
5. **Business terms exist for the concepts your org actually argues about** (Revenue, Active User, Customer, Order, etc.) and are linked to the assets that implement them. Adoption rate ≥ 50 %.
6. **Quality-check results flow in from dbt / Monte Carlo / Soda / GX** and are visible on the asset.
7. **Curated "data products" are promoted** so downstream consumers know what's blessed vs in-flight.
8. **Incident-response workflows use Catalog** as the source of truth for downstream impact.
9. **A review cadence exists** — the catalog is maintained, not just launched.
10. **Adoption is measured** — search traffic, link-shares in Slack, new-hire usage in week-1.

Every phase below moves one of those dials.

---

## Phase dependency map

```
Phase 0 (audit)
    │
    ▼
Phase 1 (ownership) ──┐
    │                 │
    ▼                 ▼
Phase 2 (metadata)   Phase 3 (glossary)  ← can run in parallel with P2
    │                 │
    └────────┬────────┘
             ▼
        Phase 4 (tagging + compliance)
             │
             ▼
        Phase 5 (lineage)
             │
             ▼
   ┌─────────┼─────────┐
   ▼         ▼         ▼
Phase 6   Phase 7   Phase 8
(data     (quality) (review
products)           cadence)
             │
             ▼
        Phase 9 (adoption — runs ongoing from Phase 6 onward)
```

**Rules of the graph**:

- Phase 0 is strictly first. Everything depends on knowing what's in the catalog.
- Phase 1 (ownership) must precede Phase 2 (metadata) — you need someone to approve descriptions. Skip this and your descriptions have no accountable source.
- Phases 2 and 3 can run in parallel — different stewards, non-overlapping work.
- Phase 4 (tagging) requires both a glossary (Phase 3, for term-linked tags) and ownership (Phase 1, to know who approves tag taxonomies).
- Phase 5 (lineage) can start in parallel with Phase 4 for Tier-1 assets — lineage doesn't depend on tags.
- Phases 6/7/8 are parallel-safe once 1-5 are done.
- Phase 9 (adoption) is a continuous band — start it the moment Phase 6 produces the first data product people can actually search for.

---

## Tiering model — do this in week 1

Before doing anything else, classify your assets into tiers. This is a governance framework, not a Catalog feature — you apply it via **tags** in the Catalog.

**Definitions** (what qualifies):

| Tier | Definition |
|---|---|
| **T1 — Core** | Assets that feed executive dashboards, regulatory reports, billing, or customer-facing products. Breaking one is a P1 incident. |
| **T2 — Analytical** | Team/domain-level assets used for day-to-day analysis. Breaking one is a team problem, not a P1. |
| **T3 — Sandbox** | Experimental, raw ingestion, personal workspaces, deprecated. Zero production claim. |

**Governance scope** (what you commit to do about each tier):

| Tier | Owner | Description | PII flags | Lineage | Quality | Glossary link |
|---|---|---|---|---|---|---|
| **T1** | Required | 100 % | Required | Required | Required (daily) | Required |
| **T2** | Required | ≥ 80 % | Required where applicable | Audited for gaps | Where time permits | Encouraged |
| **T3** | Not required | Not required | Recommended if PII | Not required | Not required | Not required |

**Apply tiers with `catalog_attach_tags`** — labels like `tier-1`, `tier-2`, `tier-3`. Every other phase in this playbook filters its scope by tier, so do this first.

### How to pick your T1 list

Start with usage signals, not gut instinct:

- **Downstream popularity**: `catalog_search_tables sortBy:"popularity" sortDirection:"DESC" nbPerPage:100` — the top 100 are strong T1 candidates.
- **Dashboard consumption**: `catalog_search_dashboards sortBy:"popularity" sortDirection:"DESC" nbPerPage:50` — for each one, walk `catalog_get_lineages childDashboardId: <id>` upstream; those tables are T1 candidates.
- **Active query traffic**: `catalog_get_table_queries tableIds: [<suspected T1>]` on suspected T1 tables. If nobody's queried it in 90 days, it's not T1.

**Aim for T1 ≤ 5 % of your total table count.** The rule of thumb: one dedicated Catalog Steward can review ~20–50 assets per week to the T1 bar. 5 % of a 10k-table warehouse = 500 T1 assets = ~10 weeks of initial review capacity, which is why the rollout is sized 8–12 weeks. If your T1 list is 500 tables on a small warehouse, your standards will slip and nothing will be T1 in practice.

---

## Roles (keep it small — 3 roles max)

| Role | Who | What they do | Time allocation |
|---|---|---|---|
| **Catalog Steward** | One named person (not a team) | Owns the overall catalog health. Runs the weekly sweep. Escalates unowned T1 assets. | 20–50 % FTE |
| **Domain Owner** | Team lead per business domain (Sales, Marketing, Product, Finance, …) | Owns the T1/T2 assets in their domain. Assigns individual owners. Approves tier changes. | 2 h/week |
| **Data Steward** | Individual named on each asset | Writes descriptions, flags PII, responds to downstream questions. | 30 min–2 h per assigned asset, upfront |

Catalog maps to these roles via `catalog_upsert_user_owners` (individual) and `catalog_upsert_team_owners` (domain). If you try to run this with 7 roles and a RACI matrix, it dies in week 2.

---

# Phase 0 — Ingestion audit (week 1)

**Goal**: know what's in the catalog before you try to govern it.

**Success criterion**: you can recite, without looking, how many sources are connected, how many databases/schemas each source exposes, and which source technology each uses.

**Effort**: one person, 4–8 hours.

**Steps**

1. `catalog_search_sources nbPerPage:100` — list every connected source. Flag anything unexpected (a dev/test warehouse someone connected and forgot).
2. `catalog_search_databases nbPerPage:100` per source — spot the orphans (a database nobody claims).
3. `catalog_search_schemas` per database — look for naming patterns that suggest tiering is already emerging in convention (e.g. `_staging`, `_sandbox`, `_prod`).
4. Document the source tree somewhere outside the Catalog (Notion / Confluence / internal wiki) — this is your operational map.

**Red flags**

- Sources in `EXTRACTION` status for > 24 h that haven't refreshed.
- Databases with 0 schemas (extraction failed silently).
- More than 3 warehouses with overlapping data (consolidation work coming).

**Common mistakes**

- Trying to "clean up" mystery sources in Phase 0. Document them; remediate in Phase 8 once owners are assigned.
- Skipping the write-up step. The external document is the first artifact of the program; without it there's no visible deliverable for the sponsor.

---

# Phase 1 — Ownership assignment (weeks 2–3)

**Goal**: every T1 asset has a human on record.

**Success criterion**: ≥ 95 % of T1 assets have at least one user or team owner. Orphans have a date on the Catalog Steward's calendar to resolve.

**Effort**: Catalog Steward ~8–16 h; Domain Owners ~1–2 h each.

**Top-down first** — assign team owners for whole schemas or databases. Fast coverage, coarse granularity.

```
catalog_upsert_team_owners {
  teamId: "<Data Platform team UUID>",
  targetEntities: [
    { entityType: "TABLE", entityId: "<uuid-1>" },
    { entityType: "TABLE", entityId: "<uuid-2>" }
  ]
}
```

One call per team, with up to N target entities per call. Repeat per domain team.

**Bottom-up second** — assign individual owners for the top 20 Tier-1 tables. These are the people who get paged when the table breaks.

```
catalog_upsert_user_owners {
  userId: "<steward UUID>",
  targetEntities: [
    { entityType: "TABLE", entityId: "<T1 table UUID>" }
  ]
}
```

**RBAC & access control — do this alongside ownership**

Catalog's own role model is separate from the stewardship roles above:

- **Catalog VIEWER** tokens can see metadata but not edit. Use for broad organizational access.
- **Catalog EDITOR / READ_WRITE** tokens are required for every mutation in this playbook. Issue them to Domain Owners and the Catalog Steward — not to the whole org.
- **The `COALESCE_CATALOG_READ_ONLY=true` env var** on an MCP client filters out all mutation tools at server startup. Pair a production-scope token with this flag for agents that should only read.

A sensible default: one `EDITOR` token per Domain Owner, scoped access enforced at the API-token layer; VIEWER for everyone else; the Catalog Steward holds the `ADMIN`/super token for incident fixes.

**Common mistakes**

- Assigning a single user as owner of 400 tables. That person becomes a bottleneck; they'll never actually describe or maintain them.
- Assigning team owners to Tier-3 sandbox schemas — it's noise.
- Forgetting terms and dashboards. `catalog_upsert_user_owners` and `catalog_upsert_team_owners` both take `entityType: "DASHBOARD" | "TERM"` too.
- Using a single shared EDITOR token across the organization. Rotate, scope, and audit.

**How to find unowned T1 assets** — `catalog_resolve_ownership_gaps schemaId:<T1 schema>` (or `tableIds:<list>` for an explicit T1 set). Returns per-unowned-table evidence bundles: top query authors from recent SQL + 1-hop lineage-neighbor owners. Use the evidence to pick owners, then execute `catalog_upsert_user_owners` / `catalog_upsert_team_owners`. Refuses loudly above 200 unowned tables — narrow the scope if that trips.

**How to grade a single T1 candidate for promotion** — `catalog_audit_data_product_readiness assetKind:"TABLE" assetId:<id>` returns per-axis `pass|warn|fail|na` scores for description / ownership / tags / columnDocs / lineage / quality checks / verification, plus `readyToPromote: true|false`. Pair with `catalog_governance_scorecard` for cross-scope aggregate coverage.

---

# Phase 2 — Core metadata (weeks 3–6)

**Goal**: every T1 asset has a description a new hire could use, PII is flagged, and audit-ready compliance metadata is in place.

**Success criterion**: 100 % of T1 columns have `description` or `descriptionRaw` set. 100 % of T1 tables have `externalDescription` set. 100 % of regulated-data columns have `isPii: true`.

**Effort**: ~30 min per table + ~5–10 min per column review, across Data Stewards. For a 500-table T1, ~250 h spread across 10–20 stewards over 3–4 weeks.

**Strategy — drafts, then review**

Description-writing is a labor problem, not a Catalog problem. The Catalog gives you batching; use it.

- Generate first drafts in bulk using the asset's name + schema context + sample queries. LLM-friendly — the `catalog-audit-documentation` prompt walks the flow.
- Route drafts to the assigned owner for approval (email / Slack / whatever works). **Do not auto-approve AI drafts.** Trusted-looking descriptions that are actually wrong are worse than no description.
- Push approved drafts with `catalog_update_column_metadata` (batched, up to 500 rows).
- **Propagate a good source-table description to its downstream tables** — once a T1 source (e.g. a `WRK_*` / `DIM_*` / `FCT_*` table) has an approved description, `catalog_propagate_metadata sourceTableId:<id> axes:["description"] maxDepth:2 dryRun:true` computes a diff plan showing which downstream tables would get the description (only blanks, under `overwritePolicy: "ifEmpty"`). Review the plan, then re-issue with `dryRun: false` to execute. Never default-on tags or owners propagation — those require a separate review.

### PII & compliance metadata

**`isPii: true`** in the same `catalog_update_column_metadata` call on any column whose name or sample values suggest it. For regulated industries this is non-negotiable for T1.

**GDPR / CCPA / HIPAA workflow support** — Catalog's metadata alone doesn't discharge legal obligations, but it's the inventory those obligations reference:

- **Data-Subject Access Request (DSR / right-to-access)** — locate every column holding a given subject identifier:
  1. `catalog_search_columns nameContains:"email" isPii:true schemaId:<prod>` (and variations for phone, SSN, IP).
  2. For each match, `catalog_get_field_lineages childColumnId:<col> hydrate:true` to find every downstream column storing the same data.
  3. Export the list to the legal team for redaction / export.
- **Right to delete / erasure** — same lineage walk, plus `catalog_get_lineages parentTableId:<pii table>` to identify all downstream derivations. Erase at the source; propagate.
- **Audit trail** — attach the regulatory justification to the asset via `catalog_create_external_links`:
  ```
  catalog_create_external_links {
    data: [{
      tableId: "<t1 pii table>",
      technology: "OTHER",
      url: "https://legal.internal/dpa-2026-05-customer-data.pdf"
    }]
  }
  ```
- **Retention tagging** — apply `retention:7-years`, `retention:regulatory`, `retention:gdpr-delete-on-request` via `catalog_attach_tags` in Phase 4.
- **Data residency** — apply `residency:eu-only`, `residency:us-only`, `residency:global` where the warehouse is cross-regional.

**Primary-key flagging** — `isPrimaryKey: true` on join columns. Downstream quality checks depend on this.

**Don't describe sandbox assets.** Rule: `isDocumented: false` AND `tier:3` tag = skip. Save your stewards' hours for the assets that matter.

---

# Phase 3 — Business glossary (weeks 4–8, overlaps Phase 2)

**Goal**: define the 20–50 business terms your org actually argues about.

**Success criterion**: 90 % of T1 assets have at least one linked business term. Every term has a named Data Steward. No two terms mean the same thing. Adoption rate (measured in Phase 9) ≥ 50 %.

**Effort**: 2–4 h per term × 20–50 terms, spread across Domain Owners. Typically 80–120 h total, over 4 weeks.

**Why 20–50 terms specifically**: below 20, you don't cover the core vocabulary; above 50, adoption collapses. Published data-mesh research shows glossaries with 100+ terms have <20 % usage — too much to remember, too much to maintain.

**Pick your terms from conflict, not from a whiteboard**

Terms worth defining are the ones that cause fights. Ask every Domain Owner: "Name three terms where your team and another team disagree on the definition." You'll get:

- **Active user** (last 7 days? 30? including deleted accounts?)
- **Revenue** (bookings? recognized? net? MRR vs ARR?)
- **Customer** (account-level? contact-level? including churned?)

Those are your first terms. Each one gets a `catalog_create_term` with a clear, unambiguous description and an assigned Data Steward.

**Hierarchy** — use `parentTermId` sparingly. A deep term tree looks organized but nobody navigates it. Three levels max: **Domain → Concept → Variant**.

**Link to tags** — `catalog_create_term { linkedTagId: "<existing tag UUID>" }`. This is the seam that makes governance visible — tagging an asset with a glossary-linked tag implicitly associates the asset with the business concept.

**Common mistakes**

- Defining "data" / "user" / "product" — too generic to be useful.
- A 12-person committee writing 400 terms nobody uses. See anti-patterns.
- Skipping the steward assignment — an orphan term definition rots faster than an orphan table.

---

# Phase 4 — Tagging & classification (weeks 6–10)

**Goal**: every T1 asset is tagged along the core axes + required compliance axes.

**Effort**: ~2 h of taxonomy design + batched application via `catalog_attach_tags` (one hour per domain to roll out).

**The core axes — don't overthink this**:

1. **Domain** — `domain:sales`, `domain:marketing`, `domain:finance`, `domain:product`. One tag per asset.
2. **Sensitivity** — `sensitivity:public`, `sensitivity:internal`, `sensitivity:confidential`, `sensitivity:regulated`. One tag per asset.
3. **Lifecycle** — `lifecycle:active`, `lifecycle:deprecated`, `lifecycle:sandbox`. One tag per asset.

**Compliance / regulatory axes** (add if applicable):

4. **Retention** — `retention:1-year`, `retention:7-years`, `retention:regulatory`, `retention:gdpr-delete-on-request`.
5. **Residency** — `residency:eu-only`, `residency:us-only`, `residency:global`.
6. **Regulatory scope** — `regulation:gdpr`, `regulation:ccpa`, `regulation:hipaa`, `regulation:sox`, `regulation:pci`.

With `catalog_attach_tags`, auto-create the label on first use. Keep the namespace tight — no `domain:sales-emea-2024-q3`. Propose all tag labels to the sponsor / compliance team before widespread rollout.

**Apply in batches**. A typical batch: "apply `domain:sales` to every table in `PROD.SALES` schema." This is one `catalog_attach_tags` call with up to 500 rows.

**Audit drift quarterly** — `catalog_search_tags labelContains:"domain:"` to see every domain tag and their usage distribution. If `domain:sales` tags 2,000 assets and `domain:marketing` tags 4, someone's not tagging their stuff.

---

# Phase 5 — Lineage validation (weeks 8–12)

**Goal**: no T1 table has zero lineage; no T1 lineage edge is stale.

**Success criterion**: every T1 table passes `catalog_trace_missing_lineage` with zero `alert`-severity findings.

**Effort**: ~30–60 min per T1 table with gaps, spread across Data Stewards.

**Method** — loop over T1:

```
for tableId in t1_tables:
  trace = catalog_trace_missing_lineage(tableId, columnSampleSize: 10)
  for finding in trace.findings:
    if finding.severity == "alert":
      investigate(finding)
      if gap_confirmed:
        catalog_upsert_lineages(...)   # files a MANUAL_CUSTOMER edge
```

**What causes gaps**

- Dynamic SQL (parameterised queries, string concat) — Catalog's parser can't trace these. Patch manually.
- `SELECT *` — parser knows the query ran but not which columns it depends on. Fix is to rewrite the source SQL if possible; manual edges if not.
- Stored procs — similar.
- Unsupported tools — if you're reading from an obscure BI tool, lineage may just not exist. Document the gap in the asset description.

**Don't patch lineage you're about to delete.** If a gap is on a T3 sandbox table, close the gap by dropping the table, not by filing a manual edge.

---

# Phase 6 — Data products (weeks 10+, ongoing)

**Goal**: 5–10 canonical "data products" are promoted and discoverable. The rule: a handful of curated assets people trust beats a thousand assets they don't.

**Effort**: ~8–16 h per data product launch (write the docs, vet the quality checks, tag, pin supporting assets, announce).

A **data product** in Catalog is a T1 asset that's been governance-approved and explicitly curated — the opposite of "some random table people found via search." Use `catalog_search_data_products` to list; use the underlying catalog tagging + ownership flow to promote.

**Selection criteria** — a data product must have:

- A named Domain Owner.
- Full description (markdown-rich, uses `descriptionRaw`).
- 100 % column description coverage.
- At least one quality check reporting SUCCESS within the last 24 h.
- A linked business term for its primary concept.
- Pinned supporting assets (`catalog_upsert_pinned_assets`) — the dimension tables or reference data required to make sense of it.

**Launch announcement** — when a data product is promoted, write a short note (external to Catalog) with: what it answers, who owns it, how to get access, SLA. Link to its Catalog URL. This is what drives Phase 9 adoption.

---

# Phase 7 — Quality checks (ongoing)

**Goal**: every T1 table has at least one quality-check dimension covered, refreshed daily.

**Effort**: ~2 h to wire up each integration initially (dbt, Monte Carlo, etc.); near-zero ongoing.

**Source tools** — Catalog doesn't run checks; it surfaces results from your test tools:

- **dbt tests** — push via Catalog's dbt integration (automated) or `catalog_upsert_data_qualities` (explicit).
- **Monte Carlo / Soda / Great Expectations / Anomalo** — same pattern.
- **Custom checks** — wire them to `catalog_upsert_data_qualities` with a stable `externalId` per check so updates are idempotent.

**Dimensions to prioritize** — freshness (`lastRefreshedAt`), row-count delta, primary-key uniqueness, null-rate on critical columns. In that order.

---

# Phase 8 — Review cadence + incident response (ongoing, forever)

A launched catalog without a review cadence rots in 6 months. Three rhythms, calendared, plus an incident runbook that uses the Catalog as the source of truth.

### Review rhythms

**Weekly — Catalog Steward**

- New-asset sweep: find tables created in the last 7 days that aren't tagged or described. `catalog_search_tables sortBy:"popularity" sortDirection:"DESC" nbPerPage:100` — scan `createdAt` > 7 days ago client-side.
- Assign or deprecate.

**Monthly — Domain Owners**

- Review your domain's tier-1 and tier-2 assets. Anything still unowned after 30 days? Any quality checks failing?
- `catalog_search_users` + filter by `ownedAssetCount > 0`: audit whether any owner has left the company (leaver scrub).
- Run `catalog_search_columns isDocumented:false schemaId:<your domain>` to find undocumented columns in your scope. (Note: `catalog_search_columns` requires a scope when filtering by `isDocumented` — pass `schemaId` or `tableId` or the call will be rejected up-front.)

**Quarterly — cross-org**

- Glossary review: are term definitions still accurate? Are any terms being ignored (link coverage < 10 % → candidate for deletion)?
- Tier re-classification: has anything moved T2 → T1 or T1 → T3?
- Coverage KPIs — see [KPIs](#kpis-to-track) below.

### Incident response — when a T1 asset breaks

Catalog is your impact-assessment source of truth. The flow:

1. **Triage impact immediately** — `catalog_get_lineages parentTableId:<broken> hydrate:true` for downstream tables; `catalog_get_lineages parentTableId:<broken> withChildAssetType:"DASHBOARD" hydrate:true` for dashboards. This list is who to notify.
2. **Alert the owners** — pull owners from `catalog_get_table`; cross-reference against `catalog_search_users` for contact info. Use the team's Slack channel from `catalog_search_teams.slackChannel`.
3. **Record the incident** — push the ALERT state via `catalog_upsert_data_qualities` so anyone searching the asset during the incident sees the warning.
4. **Tag** — `catalog_attach_tags label:"incident:<ISO date>"` on the affected asset + all downstream T1s from step 1.
5. **Link the post-mortem** — `catalog_create_external_links technology:"OTHER" url:<post-mortem doc>` once written.

This is what turns the Catalog from a passive inventory into an active incident tool — the reason Phase 8 exists.

---

# Phase 9 — Adoption & change management (ongoing, start early)

A catalog with perfect metadata and no users is a museum. This phase runs as a continuous band starting the moment Phase 6 produces the first data product.

**Goal**: the Catalog is where people go first when they have a data question. Measured by search traffic, link-shares in Slack/Teams, and new-hire time-to-first-query.

**Effort**: ongoing, ~4 h/month shared across Catalog Steward + one communications partner.

**Tactics in order of effectiveness**

1. **Embed the Catalog in the tools people already use.** Slack unfurl (paste a Catalog asset URL → see description + owner inline). Salesforce / Notion / Confluence embeds. A browser plugin for the BI tool. Adoption = convenience; never make users context-switch.
2. **Onboarding integration.** Every new hire in a data role has "explore the Catalog" as a day-1 task. Their onboarding buddy walks them through the top 5 data products.
3. **Incentive alignment.** Include Catalog coverage in team scorecards. Domain Owners whose T1 coverage drops below 95 % hear about it from their VP. Data Stewards whose descriptions are cited in incidents get recognition.
4. **Launch announcements per data product.** Every Phase 6 promotion gets a Slack post in `#data-announcements` (or equivalent). Include: what it answers, the Catalog URL, owner, a 2-min video tour.
5. **Catalog health dashboard.** Publish the [KPIs](#kpis-to-track) internally — monthly email to the sponsor, quarterly to the whole data org. Make the numbers visible; people work toward what they're measured on.
6. **Quarterly refresher training.** 30-min session: new terms, new data products, tag taxonomy changes. Record for async consumption.
7. **Feedback loop.** A `#catalog-feedback` channel or an email alias monitored by the Catalog Steward. Every ticket closes or gets published as a known issue.

**Anti-patterns in this phase**

- Announcing adoption as a top-down mandate without tool embedding. People will open the Catalog once, then never again.
- Training sessions without follow-up. One-off training is theatre.
- Measuring adoption by login count. Measure search depth, link-shares, and return visits — login alone is compliance, not use.

---

# Coalesce Transform integration — if you use the sister product

If your organization uses **Coalesce Transform** (the pipeline-authoring product), the Catalog + Transform pair is meant to work together. Ignore this section if you don't.

**The workflow seam**: Coalesce Transform nodes materialise warehouse tables; those tables appear in the Catalog via warehouse extraction. That intersection is where metadata duplication and drift happen if you don't explicitly manage it.

**Description flow**

- Transform nodes have a `description` field set by the pipeline author.
- After the node materialises, the resulting warehouse table has no automatic link — the Catalog extraction gives it a default/empty description.
- **Recommended**: have Data Stewards write the *business* description in the Catalog (aimed at consumers) and have pipeline authors write the *technical* description on the Transform node (aimed at maintainers). Different audiences, different texts.
- Alternative: one-way sync Transform → Catalog via a scheduled job that calls `catalog_update_table_metadata externalDescription:<from-transform-node>`. Works if you have fewer than 5 stewards who'd otherwise write the Catalog description.

**Ownership flow**

- Transform has `project role` assignments. Catalog has owner entities.
- The Transform project owner is *not* automatically the Catalog asset owner. Assign explicitly — `catalog_upsert_user_owners` pointing at the same person.
- Exception: for fully-generated T3 staging tables, the Transform project team can own them collectively via `catalog_upsert_team_owners`.

**Lineage consistency**

- Transform already knows node → node dependency. Catalog infers lineage from SQL post-materialisation.
- These should agree. If `catalog_trace_missing_lineage` shows gaps on a Transform-authored table, usually one of:
  - The Transform node uses `overrideSQL` or dynamic patterns the Catalog parser can't follow — patch via `catalog_upsert_lineages`.
  - The Transform pipeline hasn't run recently; extraction missed it — check Transform run status.

**Tooling cross-reference**: see [catalog://context/ecosystem-boundaries](catalog://context/ecosystem-boundaries) for the full routing between this MCP and `coalesce-transform-mcp`.

---

## KPIs to track

| Metric | Target | How to measure | Owner |
|---|---|---|---|
| T1 owner coverage | ≥ 95 % | summarize each T1 asset, count `ownership.users + ownership.teams > 0` | Catalog Steward |
| T1 description coverage | ≥ 98 % | `catalog_search_columns tableId:<T1> isDocumented:false` per T1 — aggregate; should trend to 0 | Domain Owner |
| PII columns flagged in regulated data | 100 % | manual audit on known PII schemas; `catalog_search_columns isPii:true schemaId:<scope>` to verify | Catalog Steward + compliance |
| T1 lineage coverage | ≥ 90 % of T1 tables pass `trace_missing_lineage` with 0 alerts | run monthly, chart the trend | Catalog Steward |
| Glossary term-link coverage on T1 | ≥ 90 % of T1 assets have ≥ 1 linked term | sample-audit T1; `catalog_get_table` exposes `tagEntities.tag.linkedTermId` | Domain Owner |
| Glossary term adoption rate | ≥ 50 % of terms have > 5 linked assets | `catalog_search_terms` + per-term asset count via tag lookup | Catalog Steward |
| Data products with daily-fresh quality checks | 100 % | `catalog_search_quality_checks tableId:<DP>` on each data product | Domain Owner |
| Median time to assign owner on a new T1 asset | ≤ 7 days | weekly sweep cycle + tracking | Catalog Steward |
| Adoption: monthly search depth | Trending up | External (your MCP client analytics / Catalog UI analytics) | Catalog Steward |
| Adoption: new-hire time-to-first-catalog-search | < 1 week | onboarding tracking | People / onboarding team |

Track monthly. A catalog is healthy when these numbers creep toward the targets; unhealthy when they plateau or slip. None of them ever hit 100 % forever — new assets land, owners churn. The metric is the direction.

---

## Common anti-patterns (don't)

- **Auto-generated descriptions that nobody reviewed.** Worse than no description — people trust them.
- **"All hands on deck" tagging sprints.** Tagging without a taxonomy creates inconsistent labels you'll have to rename.
- **Tiering by table size / row count.** Use popularity and consumption, not volume.
- **Glossary by committee.** A 12-person committee writes 400 terms nobody uses. A single Data Steward writes 30 terms everyone uses.
- **Governance without a steward.** If nobody's paid or bonused to maintain the catalog, it rots.
- **Manual lineage everywhere.** Lineage you have to update by hand won't be updated. If automatic detection is failing across the board, fix the detection (SQL hygiene, tool support) — don't patch it edge by edge.
- **Launching without adoption work.** Perfect metadata + zero consumers = museum. Phase 9 is not optional.
- **One shared EDITOR token across the team.** Non-auditable, un-revocable on a leaver event, invites accidents.
- **Using the Catalog as the system-of-record for sensitive data.** It's an index. Authoritative PII mapping still lives in legal/compliance systems; Catalog metadata points at it.

---

## Quick-start: the first Monday morning

If you only have one day before the meeting where you have to "show governance is working":

1. `catalog_search_sources nbPerPage:100` — confirm every source is ingesting.
2. `catalog_search_tables sortBy:"popularity" sortDirection:"DESC" nbPerPage:25` — list your top 25 tables.
3. `catalog_summarize_asset kind:"TABLE" id:<each>` on each of those 25 — note which have no owner, no description, no lineage.
4. Tag those 25 with `tier-1` via `catalog_attach_tags` (`data: [{ entityType: "TABLE", entityId: <id>, label: "tier-1" }, ...]`).
5. `catalog_upsert_user_owners` or `catalog_upsert_team_owners` on the top 10 to get coverage moving.

That's a concrete, measurable first week. Everything else is Phase 2+.

---

## Iterating this playbook as you mature

The 8-phase rollout assumes you're at maturity level 0. As you mature, relax specific rules:

- **Past 12 months in**: move "quarterly glossary review" to "biannual" once the term list has stabilised.
- **Past 50 data products**: consider splitting the Catalog Steward role from an individual into a small platform team with on-call rotation.
- **When T1 reaches 98 % coverage on everything**: you can introduce a "T0 — Platinum" tier for the handful of assets with SLAs measured in minutes (executive dashboards, billing tables). Tighter quality checks, named on-call per asset.
- **When three or more teams are running their own Catalog stewardship**: federate. Catalog Steward becomes coordinator, not sole maintainer. Each Domain Owner takes a larger cut of tagging and term work.

The playbook should shrink, not grow, as the program matures. If after a year your governance program requires *more* process than this doc, something has gone wrong.

---

## Further reading

- **DAMA-DMBOK 2nd edition** — the canonical data-management framework; chapters 3 (governance) and 4 (metadata management) cover the theory behind what this playbook operationalises.
- **Data Mesh (Zhamak Dehghani)** — the intellectual basis for the data-product framing in Phase 6.
- **DGI (Data Governance Institute) Framework** — ten components model; useful if you need to map this playbook's rollout into a formal framework for a board-level audit.

This playbook is opinionated in ways those frameworks are not — notably the 3-role limit, the phased-not-parallel stance, and the 5 % T1 cap. Those are practitioner calls, not references from the literature; treat them as the defaults to adjust rather than the One True Way.
