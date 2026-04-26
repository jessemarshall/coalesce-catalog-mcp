import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { CatalogClient } from "./client.js";
import { SERVER_NAME, SERVER_VERSION, READ_ONLY_ENV_VAR } from "./constants.js";
import type { CatalogToolDefinition } from "./catalog/types.js";
import { defineTableTools } from "./mcp/tables.js";
import { defineLineageTools } from "./mcp/lineage.js";
import { defineColumnTools } from "./mcp/columns.js";
import { defineDashboardTools } from "./mcp/dashboards.js";
import { defineDiscoveryTools } from "./mcp/discovery.js";
import { defineAnnotationTools } from "./mcp/annotations.js";
import { defineGovernanceTools } from "./mcp/governance.js";
import { defineAiTools } from "./mcp/ai.js";
import { defineIntrospectionTools } from "./mcp/introspection.js";
import { defineFindAssetByPath } from "./workflows/find-asset-by-path.js";
import { defineSummarizeAsset } from "./workflows/summarize-asset.js";
import { defineTraceMissingLineage } from "./workflows/trace-missing-lineage.js";
import { defineAssessImpact } from "./workflows/assess-impact.js";
import { defineGovernanceScorecard } from "./workflows/governance-scorecard.js";
import { defineAuditGovernanceFreshness } from "./workflows/audit-governance-freshness.js";
import { defineOwnerScorecard } from "./workflows/owner-scorecard.js";
import { defineColumnLineage } from "./workflows/column-lineage.js";
import { defineAuditDataProductReadiness } from "./workflows/audit-data-product-readiness.js";
import { defineResolveOwnershipGaps } from "./workflows/resolve-ownership-gaps.js";
import { defineReconcileOwnershipHandoff } from "./workflows/reconcile-ownership-handoff.js";
import { definePropagateMetadata } from "./workflows/propagate-metadata.js";
import { definePropagateTagsUpstream } from "./workflows/propagate-tags-upstream.js";
import { defineTriageQualityFailures } from "./workflows/triage-quality-failures.js";
import { defineAssessQualityFailureDashboardImpact } from "./workflows/assess-quality-failure-dashboard-impact.js";
import { defineAuditTagHygiene } from "./workflows/audit-tag-hygiene.js";
import { registerCatalogResources } from "./resources/index.js";
import { registerCatalogPrompts } from "./prompts/index.js";
import { cleanupStaleSessions } from "./cache/store.js";
import { withResponseExternalization } from "./mcp/tool-helpers.js";

export function isReadOnlyMode(): boolean {
  return process.env[READ_ONLY_ENV_VAR] === "true";
}

const SERVER_INSTRUCTIONS = `
coalesce-catalog-mcp — Coalesce Catalog (Castor) Public GraphQL API, wrapped as
MCP. Use this server for data catalog discovery, lineage, governance metadata,
and asset annotations across your warehouse + BI tools.

ECOSYSTEM BOUNDARIES
- coalesce-transform-mcp — pipeline/node authoring inside the Coalesce Transform
  product. Reach for Transform when the user is building, running, or debugging
  nodes, pipelines, jobs, or environments.
- coalesce-catalog-mcp (this server) — discovery, lineage, governance across the
  already-materialized warehouse and BI layer. Reach for Catalog when the user
  is asking "what exists / who owns it / what feeds into it / what depends on
  it / how is it described" about tables, columns, dashboards, terms, or tags.

WORKFLOW SEAM
- A Coalesce node materialises a warehouse table → that table appears in the
  Catalog. When a user needs end-to-end context (node definition + downstream
  dashboards), call both servers and stitch results.

COMPOSED WORKFLOW TOOLS — prefer these over chaining 4-6 primitives:
- catalog_find_asset_by_path — resolve "DB.SCHEMA.TABLE[.COLUMN]" to a UUID
  before any catalog_get_* call.
- catalog_summarize_asset — full cross-domain context (identity + ownership +
  tags + lineage + columns + quality) for one asset in one call.
- catalog_assess_impact — deprecation blast-radius report. Walks downstream
  lineage (depth 1-3, refuses rather than truncating on wide graphs), enriches
  every reached asset with ownership, and returns a 0-100 severity score with
  per-component rationale. Use before any deprecate/archive/restructure decision.
- catalog_governance_scorecard — coverage matrix per database/schema/table list:
  per-table flags for ownership / description / column-doc % / tag count, with a
  popularity-weighted aggregate roll-up. Use to drive Health dashboards or
  governance-rollout playbooks.
- catalog_audit_governance_freshness — extends the scorecard with verifiedAt +
  sensitivity-driven cadence policy: per-table staleness (days-since-last-review
  minus required-cadence-days), bucketed (neverReviewed / overdue / dueSoon /
  ok), sorted by stalenessDays * popularity. Answers "is metadata still current,
  not just present?".
- catalog_owner_scorecard — per-owner cleanup scorecard: given an email,
  enumerates every owned table/dashboard/term and groups them by hygiene issue
  (thin description, PII, uncertified, lineage gaps, term orphans, etc.). Pair
  with the catalog-daily-guide prompt for a rendered walkthrough.
- catalog_trace_missing_lineage — diagnose where a table's lineage coverage is
  thin or absent; returns severity-tagged findings with recommendations.
- catalog_audit_data_product_readiness — per-asset promotion-readiness report.
  Grades 8 axes (description, ownership, tags, column-doc, upstream/downstream
  lineage, quality checks, verification) with hardcoded thresholds and returns
  per-axis pass/warn/fail/na status + actionable gaps + overall readyToPromote
  flag. Use when a user asks "is this table/dashboard ready to promote?".
- catalog_resolve_ownership_gaps — for a database/schema/tableIds scope, finds
  unowned tables and returns per-table evidence bundles (top query authors + 1-hop
  lineage neighbor owners). Raw signals only, no confidence scores; refuses loudly
  above 200 unowned tables. Pair with governance_scorecard (size the gap) then
  this tool (close it).
- catalog_reconcile_ownership_handoff — for a departing owner (by email), builds
  a blast-radius-ranked handoff plan: enumerates every owned table/dashboard/term,
  scores each asset (popularity x downstream consumer count x query volume), gathers
  candidate-owner evidence (query authors, 1-hop neighbor owners, team membership),
  and aggregates candidates into a ranked summary. Refuses above 200 owned assets.
- catalog_propagate_metadata — downstream metadata propagation from a source
  table. Computes a typed diff plan for description / tags / owners axes,
  returns in dry-run mode by default; non-dry-run requires MCP elicitation
  confirmation and reports per-axis partial-failure tracking.
- catalog_propagate_tags_upstream — upstream-direction tag propagation from a
  presentation source (dashboard or gold-layer table) to the warehouse tables
  that feed it. Dry-run by default; execute requires acknowledgeProvenance-
  Semantics=true (provenance trail in plan) AND elicitation confirmation.
  Refuses above 200 reached upstream tables.
- catalog_triage_quality_failures — triage all failing quality checks into a
  prioritised action queue ranked by popularity * failure count, grouped by
  owner, with optional 1-hop upstream lineage pointers for root-cause analysis.
- catalog_assess_quality_failure_dashboard_impact — extends triage_quality_failures
  forward through lineage: enumerates which BI dashboards are downstream of
  failing checks, ranks them by blast-radius (dashboard popularity x failure
  count x criticality-tag boost), and shows which failing tables reach each.
- catalog_audit_tag_hygiene — audit structural health of the tag layer: detects
  orphaned, unlinked, skewed, and near-duplicate tags across tables and dashboards.

TOOLING NOTES
- All list tools paginate server-side; responses include \`pagination.hasMore\`.
  Start with nbPerPage=25-100 and page=0; only fetch deeper pages on demand.
- Read-only mode: set COALESCE_CATALOG_READ_ONLY=true to drop all mutation
  tools at registration time. Default is read-write.
- Destructive tools (delete/remove/detach) request interactive confirmation
  via MCP elicitation. Set COALESCE_CATALOG_SKIP_CONFIRMATIONS=true to
  bypass — only safe for vetted, non-interactive callers (CI, batch jobs).
`.trim();

/**
 * Whether a tool should be available in read-only mode. Tools default to
 * "write" (excluded) unless they declare readOnlyHint: true, matching the
 * transform MCP's convention.
 */
function isReadOnlyTool(def: CatalogToolDefinition): boolean {
  return def.config.annotations?.readOnlyHint === true;
}

/**
 * Single source of truth for the full tool registration list. Exported so the
 * registration test can assert on the same array the server actually
 * registers — without this, the test's hand-maintained list silently drifted
 * away from server.ts every time a new tool was added.
 */
export function buildAllToolDefinitions(
  client: CatalogClient
): CatalogToolDefinition[] {
  return [
    ...defineTableTools(client),
    ...defineLineageTools(client),
    ...defineColumnTools(client),
    ...defineDashboardTools(client),
    ...defineDiscoveryTools(client),
    ...defineAnnotationTools(client),
    ...defineGovernanceTools(client),
    ...defineAiTools(client),
    ...defineIntrospectionTools(client),
    defineFindAssetByPath(client),
    defineSummarizeAsset(client),
    defineTraceMissingLineage(client),
    defineAssessImpact(client),
    defineGovernanceScorecard(client),
    defineAuditGovernanceFreshness(client),
    defineOwnerScorecard(client),
    defineColumnLineage(client),
    defineAuditDataProductReadiness(client),
    defineResolveOwnershipGaps(client),
    defineReconcileOwnershipHandoff(client),
    definePropagateMetadata(client),
    definePropagateTagsUpstream(client),
    defineTriageQualityFailures(client),
    defineAssessQualityFailureDashboardImpact(client),
    defineAuditTagHygiene(client),
  ];
}

export function createCoalesceCatalogMcpServer(
  client: CatalogClient
): McpServer {
  const server = new McpServer(
    { name: SERVER_NAME, version: SERVER_VERSION },
    { instructions: SERVER_INSTRUCTIONS }
  );

  const readOnly = isReadOnlyMode();
  const definitions = buildAllToolDefinitions(client);

  type RegisterToolHandler = Parameters<McpServer["registerTool"]>[2];
  for (const def of definitions) {
    if (readOnly && !isReadOnlyTool(def)) continue;
    // SDK's handler type demands an index signature on the return; our
    // narrower ToolResult is structurally compatible. Casting through the
    // SDK's exact handler param type (not `any`) keeps future SDK changes
    // surfacing at compile time.
    const wrapped = withResponseExternalization(def.handler, {
      toolName: def.name,
      neverExternalize: def.neverExternalize,
    });
    server.registerTool(
      def.name,
      def.config,
      wrapped as unknown as RegisterToolHandler
    );
  }

  registerCatalogResources(server);
  registerCatalogPrompts(server);
  cleanupStaleSessions();

  return server;
}
