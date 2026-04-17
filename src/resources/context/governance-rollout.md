# Data Governance Rollout — Best-in-Class Catalog Playbook

A sequenced, opinionated method for a team starting from zero to reach a governed, trusted Catalog state in **8–12 weeks**. Every phase has an owner, a success criterion, and the specific `catalog_*` tools that execute it. Skip the theory if your program is already mature; jump to **Phase X** of your choice.

> **Why phased, not parallel.** Assigning owners before descriptions exist gives each owner an empty TODO list. Writing descriptions before tiers exist burns steward hours on low-value assets. Rolling out tags before a glossary exists produces a mess of inconsistent labels you'll rename later. Order matters.

---

## The maturity you're aiming for

A "best-in-class" Catalog state isn't 100 % coverage of every asset. It's:

1. **Every Tier-1 asset has a human owner** who's accountable and reachable.
2. **Every Tier-1 asset has a plain-English description** that a new hire could use.
3. **Sensitive data (PII, financial, regulated) is tagged** consistently.
4. **Lineage — asset and column level — is complete for Tier-1** and audited for Tier-2.
5. **Business terms exist for the concepts your org actually argues about** (Revenue, Active User, Customer, Order, etc.) and are linked to the assets that implement them.
6. **Quality-check results flow in from dbt / Monte Carlo / Soda / GX** and are visible on the asset.
7. **Curated "data products" are promoted** so downstream consumers know what's blessed vs in-flight.
8. **A review cadence exists** — the catalog is maintained, not just launched.

Every phase below moves one of those dials.

---

## Tiering model — do this in week 1

Before doing anything else, classify your assets into tiers. This is a governance framework, not a Catalog feature — you apply it via **tags** in the Catalog.

| Tier | Definition | What you govern |
|---|---|---|
| **T1 — Core** | Assets that feed executive dashboards, regulatory reports, billing, or customer-facing products. Breaking one is a P1 incident. | Full: owner, description, PII flags, lineage, quality checks, glossary links. |
| **T2 — Analytical** | Team/domain-level assets used for day-to-day analysis. Breaking one is a team problem, not a P1. | Medium: owner, description, PII flags where relevant. Lineage + quality where time permits. |
| **T3 — Sandbox** | Experimental, raw ingestion, personal workspaces, deprecated. Zero production claim. | Minimal. Auto-tag as T3 so nobody mistakes them for trusted. |

**Apply tiers with `catalog_attach_tags`** — labels like `tier-1`, `tier-2`, `tier-3`. Every other phase in this playbook filters its scope by tier, so do this first.

### How to pick your T1 list

Start with usage signals, not gut instinct:

- **Downstream popularity**: `catalog_search_tables sortBy:"popularity" sortDirection:"DESC" nbPerPage:100` — the top 100 are strong T1 candidates.
- **Dashboard consumption**: `catalog_search_dashboards sortBy:"popularity" sortDirection:"DESC" nbPerPage:50` — for each one, walk `catalog_get_lineages parentDashboardId` upstream; those tables are T1.
- **Active query traffic**: `catalog_get_table_queries` on suspected T1 tables. If nobody's queried it in 90 days, it's not T1.

Aim for **T1 ≤ 5 % of your total table count**. If your T1 list is 500 tables, your standards will slip and nothing will be T1 in practice.

---

## Roles (keep it small — 3 roles max)

| Role | Who | What they do |
|---|---|---|
| **Catalog Steward** | One named person (not a team) | Owns the overall catalog health. Runs the weekly sweep. Escalates unowned T1 assets. |
| **Domain Owner** | Team lead per business domain (Sales, Marketing, Product, Finance, …) | Owns the T1/T2 assets in their domain. Assigns individual owners. Approves tier changes. |
| **Data Steward** | Individual named on each asset | Writes descriptions, flags PII, responds to downstream questions. |

Catalog maps to these roles via `catalog_upsert_user_owners` (individual) and `catalog_upsert_team_owners` (domain). If you try to run this with 7 roles and a RACI matrix, it dies in week 2.

---

# Phase 0 — Ingestion audit (week 1)

**Goal**: know what's in the catalog before you try to govern it.

**Success criterion**: you can recite, without looking, how many sources are connected, how many databases/schemas each source exposes, and which source technology each uses.

**Steps**

1. `catalog_search_sources nbPerPage:100` — list every connected source. Flag anything unexpected (a dev/test warehouse someone connected and forgot).
2. `catalog_search_databases nbPerPage:100` per source — spot the orphans (a database nobody claims).
3. `catalog_search_schemas` per database — look for naming patterns that suggest tiering is already emerging in convention (e.g. `_staging`, `_sandbox`, `_prod`).
4. Document the source tree somewhere outside the Catalog (Notion / Confluence / internal wiki) — this is your operational map.

**Red flags**

- Sources in `EXTRACTION` status for > 24 h that haven't refreshed.
- Databases with 0 schemas (extraction failed silently).
- More than 3 warehouses with overlapping data (consolidation work coming).

---

# Phase 1 — Ownership assignment (weeks 2–3)

**Goal**: every T1 asset has a human on record.

**Success criterion**: ≥ 95 % of T1 assets have at least one user or team owner. Orphans have a date on the Catalog Steward's calendar to resolve.

**Top-down first** — assign team owners for whole schemas or databases. Fast coverage, coarse granularity.

```
catalog_upsert_team_owners {
  teamId: <Data Platform team UUID>,
  targetEntities: [
    { entityType: "TABLE", entityId: "<every_table_in_PROD_CORE>" }
  ]
}
```

**Bottom-up second** — assign individual owners for the top 20 Tier-1 tables. These are the people who get paged when the table breaks.

**Common mistakes**

- Assigning a single user as owner of 400 tables. That person becomes a bottleneck; they'll never actually describe or maintain them.
- Assigning team owners to Tier-3 sandbox schemas — it's noise.
- Forgetting terms and dashboards. `catalog_upsert_user_owners` and `catalog_upsert_team_owners` both take `entityType: "DASHBOARD" | "TERM"` too.

**How to find unowned T1 assets** — `catalog_summarize_asset` on each T1 candidate; filter where `ownership.users.length + ownership.teams.length == 0`. Or hand it off to the `catalog-audit-documentation` prompt with an ownership-focused twist.

---

# Phase 2 — Core metadata (weeks 3–6)

**Goal**: every T1 asset has a description a new hire could use.

**Success criterion**: 100 % of T1 columns have `description` or `descriptionRaw` set. 100 % of T1 tables have `externalDescription` set.

**Strategy — drafts, then review**

Description-writing is a labor problem, not a Catalog problem. The Catalog gives you batching; use it.

- Generate first drafts in bulk using the asset's name + schema context + sample queries. LLM-friendly — the `catalog-audit-documentation` prompt walks the flow.
- Route drafts to the assigned owner for approval (email / Slack / whatever works). Do not auto-approve AI drafts.
- Push approved drafts with `catalog_update_column_metadata` (batched, up to 500 rows).

**PII flagging alongside** — set `isPii: true` in the same `catalog_update_column_metadata` call on any column whose name or sample values suggest it. If you're in a regulated industry, this is non-negotiable for T1.

**Primary-key flagging** — `isPrimaryKey: true` on join columns. Downstream quality checks depend on this.

**Don't describe sandbox assets.** `isDocumented: false` + `tier:3` tag = skip.

---

# Phase 3 — Business glossary (weeks 4–8, overlaps Phase 2)

**Goal**: define the 20–50 business terms your org actually argues about.

**Success criterion**: 90 % of T1 assets have at least one linked business term. Every term has a named Data Steward. No two terms mean the same thing.

**Pick your terms from conflict, not from a whiteboard**

Terms worth defining are the ones that cause fights. Ask every Domain Owner: "Name three terms where your team and another team disagree on the definition." You'll get:

- **Active user** (last 7 days? 30? including deleted accounts?)
- **Revenue** (bookings? recognized? net? MRR vs ARR?)
- **Customer** (account-level? contact-level? including churned?)

Those are your first terms. Each one gets a `catalog_create_term` with a clear, unambiguous description and an assigned Data Steward.

**Hierarchy** — use `parentTermId` sparingly. A deep term tree looks organized but nobody navigates it. Three levels max: **Domain → Concept → Variant**.

**Link to tags** — `catalog_create_term { linkedTagId: <existing tag UUID> }`. This is the seam that makes governance visible — tagging an asset with a glossary-linked tag implicitly associates the asset with the business concept.

---

# Phase 4 — Tagging & classification (weeks 6–10)

**Goal**: every T1 asset is tagged along three axes.

**The three axes** — don't overthink this:

1. **Domain** — `domain:sales`, `domain:marketing`, `domain:finance`, `domain:product`. One tag per asset.
2. **Sensitivity** — `sensitivity:public`, `sensitivity:internal`, `sensitivity:confidential`, `sensitivity:regulated`. One tag per asset.
3. **Lifecycle** — `lifecycle:active`, `lifecycle:deprecated`, `lifecycle:sandbox`. One tag per asset.

With `catalog_attach_tags`, auto-create the label on first use. Keep the namespace tight — no `domain:sales-emea-2024-q3`.

**Apply in batches**. A typical batch: "apply `domain:sales` to every table in `PROD.SALES` schema." This is one `catalog_attach_tags` call with up to 500 rows.

**Audit drift quarterly** — `catalog_search_tags labelContains:"domain:"` to see every domain tag and their usage distribution. If `domain:sales` tags 2,000 assets and `domain:marketing` tags 4, someone's not tagging their stuff.

---

# Phase 5 — Lineage validation (weeks 8–12)

**Goal**: no T1 table has zero lineage; no T1 lineage edge is stale.

**Success criterion**: every T1 table passes `catalog_trace_missing_lineage` with zero `alert`-severity findings.

**Method** — loop over T1:

```
for tableId in t1_tables:
  trace = catalog_trace_missing_lineage(tableId, columnSampleSize: 10)
  for finding in trace.findings:
    if finding.severity == "alert":
      investigate(finding)
      if gap_confirmed:
        catalog_upsert_lineages(...)   # file a MANUAL_CUSTOMER edge
```

**What causes gaps**

- Dynamic SQL (parameterised queries, string concat) — Catalog's parser can't trace these. Patch manually.
- `SELECT *` — parser knows the query ran but not which columns it depends on. Fix is to rewrite the source SQL if possible; manual edges if not.
- Stored procs — similar.
- Unsupported tools — if you're reading from an obscure BI tool, lineage may just not exist. Document the gap in the asset description.

**Don't patch lineage you're about to delete.** If a gap is on a T3 sandbox table, close the gap by dropping the table, not by filing a manual edge.

---

# Phase 6 — Data products (weeks 10+, ongoing)

**Goal**: 5–10 canonical "data products" are promoted and discoverable.

A **data product** in Catalog is a T1 asset that's been governance-approved and explicitly curated — the opposite of "some random table people found via search." Use `catalog_search_data_products` to list; use the underlying catalog tagging + ownership flow to promote.

**Selection criteria** — a data product must have:

- A named Domain Owner.
- Full description (markdown-rich, uses `descriptionRaw`).
- 100 % column description coverage.
- At least one quality check reporting SUCCESS within the last 24 h.
- A linked business term for its primary concept.
- Pinned supporting assets (`catalog_upsert_pinned_assets`) — the dimension tables or reference data required to make sense of it.

**Launch announcement** — when a data product is promoted, write a short note (external to Catalog) with: what it answers, who owns it, how to get access, SLA. Link to its Catalog URL.

---

# Phase 7 — Quality checks (ongoing)

**Goal**: every T1 table has at least one quality-check dimension covered, refreshed daily.

**Source tools** — Catalog doesn't run checks; it surfaces results from your test tools:

- **dbt tests** — push via Catalog's dbt integration (automated) or `catalog_upsert_data_qualities` (explicit).
- **Monte Carlo / Soda / Great Expectations / Anomalo** — same pattern.
- **Custom checks** — wire them to `catalog_upsert_data_qualities` with a stable `externalId` per check so updates are idempotent.

**Dimensions to prioritize** — freshness (`lastRefreshedAt`), row-count delta, primary-key uniqueness, null-rate on critical columns. In that order.

---

# Phase 8 — Review cadence (ongoing, forever)

A launched catalog without a review cadence rots in 6 months. Three rhythms, calendared:

**Weekly — Catalog Steward**

- New-asset sweep: find tables created in the last 7 days that aren't tagged or described. `catalog_search_tables sortBy:"tablePopularity" sortDirection:"ASC"` then check `createdAt` > 7 days ago.
- Assign or deprecate.

**Monthly — Domain Owners**

- Review your domain's tier-1 and tier-2 assets. Anything still unowned after 30 days? Any quality checks failing?
- `catalog_search_users` + filter by `ownedAssetCount > 0`: audit whether any owner has left the company (leaver scrub).

**Quarterly — cross-org**

- Glossary review: are term definitions still accurate? Are any terms being ignored?
- Tier re-classification: has anything moved T2 → T1 or T1 → T3?
- Coverage KPIs — see below.

---

## KPIs to track

| Metric | Target | How to measure |
|---|---|---|
| T1 owner coverage | ≥ 95 % | summarize each T1 asset, count `ownership.users + ownership.teams > 0` |
| T1 description coverage | ≥ 98 % | `catalog_search_columns tableId:<T1> isDocumented:false` — should return 0 or near-0 |
| PII columns flagged in regulated data | 100 % | manual audit on known PII schemas; `catalog_search_columns isPii:true` + a schema scope to verify |
| T1 lineage coverage | ≥ 90 % of T1 tables pass `trace_missing_lineage` with 0 alerts | run monthly, chart the trend |
| Data products with daily-fresh quality checks | 100 % | `catalog_search_quality_checks tableId:<DP>` on each data product |
| Median time to assign owner on a new T1 asset | ≤ 7 days | weekly sweep cycle + tracking |

Track these monthly. A catalog is healthy when these numbers creep toward the targets; unhealthy when they plateau or slip. None of them ever hit 100 % forever — new assets land, owners churn. The metric is the direction.

---

## Common anti-patterns (don't)

- **Auto-generated descriptions that nobody reviewed.** Worse than no description — people trust them.
- **"All hands on deck" tagging sprints.** Tagging without a taxonomy creates inconsistent labels you'll have to rename.
- **Tiering by table size / row count.** Use popularity and consumption, not volume.
- **Glossary by committee.** A 12-person committee writes 400 terms nobody uses. A single Data Steward writes 30 terms everyone uses.
- **Governance without a steward.** If nobody's paid or bonused to maintain the catalog, it rots.
- **Manual lineage everywhere.** Lineage you have to update by hand won't be updated. If automatic detection is failing across the board, fix the detection (SQL hygiene, tool support) — don't patch it edge by edge.

---

## Quick-start: the first Monday morning

If you only have one day before the meeting where you have to "show governance is working":

1. `catalog_search_sources nbPerPage:100` — confirm every source is ingesting.
2. `catalog_search_tables sortBy:"popularity" sortDirection:"DESC" nbPerPage:25` — list your top 25 tables.
3. `catalog_summarize_asset` on each of those 25 — note which have no owner, no description, no lineage.
4. Tag those 25 with `tier:1` via `catalog_attach_tags`.
5. `catalog_upsert_user_owners` or `catalog_upsert_team_owners` on the top 10 to get coverage moving.

That's a concrete, measurable first week. Everything else is Phase 2+.
