import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLE_DETAIL,
  GET_DASHBOARD_DETAIL,
  GET_COLUMNS_SUMMARY,
  GET_DATA_QUALITIES,
  GET_LINEAGES,
} from "../catalog/operations.js";
import type {
  GetColumnsOutput,
  GetLineagesOutput,
  GetQualityChecksOutput,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

type AssetKind = "TABLE" | "DASHBOARD";

type AxisName =
  | "description"
  | "ownership"
  | "tags"
  | "columnDocs"
  | "upstreamLineage"
  | "downstreamLineage"
  | "qualityChecks"
  | "verification";

type AxisStatus = "pass" | "warn" | "fail" | "na";

const ALL_AXES: AxisName[] = [
  "description",
  "ownership",
  "tags",
  "columnDocs",
  "upstreamLineage",
  "downstreamLineage",
  "qualityChecks",
  "verification",
];

// Column-doc axis thresholds. Promotion-grade is 80%+ documented; 50-80% is a
// warn (documented enough to ship, but noticeable gaps a consumer will hit);
// below 50% is a fail. Mirrors the governance-scorecard's "columnDocPct" field
// so a table that passes the scorecard's health check also passes this axis.
const COLUMN_DOC_PASS_PCT = 80;
const COLUMN_DOC_WARN_PCT = 50;

// Description axis thresholds. An empty or whitespace-only description fails;
// under 20 chars fails too (common "ok" / "pending" placeholders fall here);
// 20-80 warns (probably a one-liner; informative but thin); 80+ passes. The
// bar is deliberately lower than a human review would require — the axis
// checks for presence + intent, not quality.
const DESCRIPTION_PASS_CHARS = 80;
const DESCRIPTION_WARN_CHARS = 20;

const DEFAULT_COLUMN_SAMPLE_CAP = 200;
const COLUMN_PAGE_SIZE = 500;
// One lineage edge is enough to pass the edge-count axes. Fetch a single page
// and rely on totalCount — the tool doesn't need the edge list, just the count.
const LINEAGE_PROBE_PAGE_SIZE = 1;
const QUALITY_PROBE_PAGE_SIZE = 1;

const AuditInputShape = {
  assetKind: z
    .enum(["TABLE", "DASHBOARD"])
    .describe("Asset type to audit."),
  assetId: z.string().min(1).describe("Catalog UUID of the asset."),
  axes: z
    .array(
      z.enum([
        "description",
        "ownership",
        "tags",
        "columnDocs",
        "upstreamLineage",
        "downstreamLineage",
        "qualityChecks",
        "verification",
      ])
    )
    .optional()
    .describe(
      "Which axes to evaluate. Default: all axes. DASHBOARD assets always report `status: \"na\"` for columnDocs and qualityChecks (those axes have no dashboard-level signal). Narrow the axes list when the caller only cares about a subset (e.g. `['ownership', 'description']` for a governance-hygiene check)."
    ),
  columnSampleCap: z
    .number()
    .int()
    .min(10)
    .max(1000)
    .optional()
    .describe(
      `Max columns to inspect when computing columnDocs coverage. Default ${DEFAULT_COLUMN_SAMPLE_CAP}. Wide-table outliers (1000+ col staging tables) are sampled to this cap; the columnDocs axis reports \`sampled: true\` when that happens so the caller knows the pct is approximate.`
    ),
};

interface AxisResult {
  name: AxisName;
  status: AxisStatus;
  signals: Record<string, unknown>;
  gaps: string[];
}

interface ColumnCoverage {
  described: number;
  total: number;
  pct: number;
  sampled: boolean;
}

function isNonEmptyString(v: unknown): v is string {
  return typeof v === "string" && v.trim().length > 0;
}

function extractOwners(row: Record<string, unknown>): {
  userOwners: Array<{ userId: string | null; email: string | null; fullName: string | null }>;
  teamOwners: Array<{ teamId: string | null; name: string | null }>;
} {
  const userOwners = Array.isArray(row.ownerEntities)
    ? (row.ownerEntities as Array<Record<string, unknown>>)
        .filter((o) => o.userId != null)
        .map((o) => {
          const u = (o.user as Record<string, unknown> | undefined) ?? {};
          return {
            userId: (o.userId as string | null) ?? null,
            email: (u.email as string | null) ?? null,
            fullName: (u.fullName as string | null) ?? null,
          };
        })
    : [];
  const teamOwners = Array.isArray(row.teamOwnerEntities)
    ? (row.teamOwnerEntities as Array<Record<string, unknown>>)
        .filter((t) => t.teamId != null)
        .map((t) => {
          const team = (t.team as Record<string, unknown> | undefined) ?? {};
          return {
            teamId: (t.teamId as string | null) ?? null,
            name: (team.name as string | null) ?? null,
          };
        })
    : [];
  return { userOwners, teamOwners };
}

function extractTags(row: Record<string, unknown>): Array<{ label: string | null }> {
  if (!Array.isArray(row.tagEntities)) return [];
  return (row.tagEntities as Array<Record<string, unknown>>).map((t) => {
    const tag = (t.tag as Record<string, unknown> | undefined) ?? {};
    return { label: (tag.label as string | null) ?? null };
  });
}

async function probeColumnDocCoverage(
  client: CatalogClient,
  tableId: string,
  cap: number
): Promise<ColumnCoverage> {
  // Paginate until we've either seen every column or hit the cap. Per-table
  // column counts have no hard ceiling in the API, so wide staging tables
  // (1000+ cols) would otherwise wedge the audit on a single asset. When the
  // cap trips we mark sampled: true so the caller knows the pct is
  // approximate — not silently truncated.
  let described = 0;
  let total = 0;
  let sampled = false;
  // Hard ceiling on pages so a misbehaving server (totalCount stuck ahead of
  // returned rows) can't wedge the probe. Theoretical max = cap / pageSize,
  // plus a couple of pages of slack for response-size variance.
  const maxPages = Math.ceil(cap / COLUMN_PAGE_SIZE) + 3;
  for (let page = 0; page < maxPages; page++) {
    const resp = await client.execute<{ getColumns: GetColumnsOutput }>(
      GET_COLUMNS_SUMMARY,
      {
        scope: { tableIds: [tableId] },
        pagination: { nbPerPage: COLUMN_PAGE_SIZE, page },
      }
    );
    const rows = resp.getColumns.data as Array<{
      tableId: string;
      description?: string | null;
    }>;
    for (const col of rows) {
      if (total >= cap) {
        sampled = true;
        break;
      }
      total += 1;
      if (isNonEmptyString(col.description)) described += 1;
    }
    if (total >= cap) {
      sampled = true;
      break;
    }
    if (rows.length < COLUMN_PAGE_SIZE) break;
    const columnsTotal = resp.getColumns.totalCount;
    if (typeof columnsTotal !== "number" || !Number.isFinite(columnsTotal)) {
      throw new Error(
        `getColumns returned non-numeric totalCount (${String(columnsTotal)}) ` +
          `for tableId=${tableId}; cannot verify column-doc coverage completeness.`
      );
    }
    if ((page + 1) * COLUMN_PAGE_SIZE >= columnsTotal) break;
    if (page === maxPages - 1) {
      throw new Error(
        `Column pagination exceeded ${maxPages} pages for tableId=${tableId} ` +
          `(cap=${cap}). Likely server returning duplicate pages or non-numeric totalCount.`
      );
    }
  }
  return {
    described,
    total,
    pct: total === 0 ? 0 : Math.round((described / total) * 100),
    sampled,
  };
}

async function probeLineageEdgeCount(
  client: CatalogClient,
  opts: {
    direction: "upstream" | "downstream";
    assetKind: AssetKind;
    assetId: string;
  }
): Promise<number> {
  // We only need the count — fetch 1 row and read totalCount. For dashboards
  // we still scope to dashboard endpoints; the API rejects parent/child id
  // mismatches at the schema level so we don't have to worry about cross-type
  // probes returning zero silently.
  const scope: Record<string, string> = {};
  if (opts.direction === "downstream") {
    scope[opts.assetKind === "TABLE" ? "parentTableId" : "parentDashboardId"] =
      opts.assetId;
  } else {
    scope[opts.assetKind === "TABLE" ? "childTableId" : "childDashboardId"] =
      opts.assetId;
  }
  const resp = await client.execute<{ getLineages: GetLineagesOutput }>(
    GET_LINEAGES,
    {
      scope,
      pagination: { nbPerPage: LINEAGE_PROBE_PAGE_SIZE, page: 0 },
    }
  );
  const total = resp.getLineages.totalCount;
  if (typeof total !== "number" || !Number.isFinite(total)) {
    throw new Error(
      `getLineages returned non-numeric totalCount (${String(total)}) for ` +
        `${opts.direction} probe of ${opts.assetKind.toLowerCase()} ${opts.assetId}; ` +
        `cannot grade the ${opts.direction}Lineage axis.`
    );
  }
  return total;
}

async function probeQualityCheckCount(
  client: CatalogClient,
  tableId: string
): Promise<number> {
  const resp = await client.execute<{ getDataQualities: GetQualityChecksOutput }>(
    GET_DATA_QUALITIES,
    {
      scope: { tableId },
      pagination: { nbPerPage: QUALITY_PROBE_PAGE_SIZE, page: 0 },
    }
  );
  const total = resp.getDataQualities.totalCount;
  if (typeof total !== "number" || !Number.isFinite(total)) {
    throw new Error(
      `getDataQualities returned non-numeric totalCount (${String(total)}) for ` +
        `tableId=${tableId}; cannot grade the qualityChecks axis.`
    );
  }
  return total;
}

function gradeDescription(row: Record<string, unknown>): AxisResult {
  const direct = isNonEmptyString(row.description) ? (row.description as string) : "";
  const raw = isNonEmptyString(row.descriptionRaw)
    ? (row.descriptionRaw as string)
    : "";
  const external = isNonEmptyString(row.externalDescription)
    ? (row.externalDescription as string)
    : "";
  // Prefer whichever surface has the longest content — `description` is the
  // merged display value, but on fresh imports it can be empty while
  // externalDescription carries the source-extracted copy. Auditing for
  // "is there *some* description surfacing to a consumer" means checking all
  // three, not just the merged view.
  const candidate = [direct, raw, external].reduce(
    (a, b) => (b.length > a.length ? b : a),
    ""
  );
  const trimmedLength = candidate.trim().length;
  const gaps: string[] = [];
  let status: AxisStatus;
  if (trimmedLength === 0) {
    status = "fail";
    gaps.push(
      "No description surfaces on this asset. Add one via catalog_update_table_metadata (externalDescription)."
    );
  } else if (trimmedLength < DESCRIPTION_WARN_CHARS) {
    status = "fail";
    gaps.push(
      `Description is ${trimmedLength} chars — below the ${DESCRIPTION_WARN_CHARS}-char minimum for a usable description.`
    );
  } else if (trimmedLength < DESCRIPTION_PASS_CHARS) {
    status = "warn";
    gaps.push(
      `Description is ${trimmedLength} chars — passes minimum but below the ${DESCRIPTION_PASS_CHARS}-char promotion bar. Expand to cover purpose, grain, and notable caveats.`
    );
  } else {
    status = "pass";
  }
  return {
    name: "description",
    status,
    signals: {
      length: trimmedLength,
      hasDirectDescription: direct.length > 0,
      hasDescriptionRaw: raw.length > 0,
      hasExternalDescription: external.length > 0,
      isDescriptionGenerated: row.isDescriptionGenerated === true,
    },
    gaps,
  };
}

function gradeOwnership(row: Record<string, unknown>): AxisResult {
  const { userOwners, teamOwners } = extractOwners(row);
  const gaps: string[] = [];
  let status: AxisStatus;
  if (userOwners.length === 0 && teamOwners.length === 0) {
    status = "fail";
    gaps.push(
      "No user or team owner is attached. Assign via catalog_upsert_user_owners or catalog_upsert_team_owners."
    );
  } else {
    status = "pass";
  }
  return {
    name: "ownership",
    status,
    signals: {
      userOwnerCount: userOwners.length,
      teamOwnerCount: teamOwners.length,
      userOwners,
      teamOwners,
    },
    gaps,
  };
}

function gradeTags(row: Record<string, unknown>): AxisResult {
  const tags = extractTags(row);
  const gaps: string[] = [];
  let status: AxisStatus;
  if (tags.length === 0) {
    status = "warn";
    gaps.push(
      "No tags attached. Tag with a domain / data-product / sensitivity label via catalog_attach_tags. (Warn, not fail — some data products intentionally omit tags.)"
    );
  } else {
    status = "pass";
  }
  return {
    name: "tags",
    status,
    signals: {
      tagCount: tags.length,
      labels: tags.map((t) => t.label),
    },
    gaps,
  };
}

function gradeColumnDocs(
  assetKind: AssetKind,
  coverage: ColumnCoverage | null
): AxisResult {
  if (assetKind === "DASHBOARD" || !coverage) {
    return {
      name: "columnDocs",
      status: "na",
      signals: { reason: "dashboards have no column-doc signal" },
      gaps: [],
    };
  }
  if (coverage.total === 0) {
    return {
      name: "columnDocs",
      status: "na",
      signals: {
        described: 0,
        total: 0,
        pct: 0,
        sampled: false,
        reason: "table has no columns to grade",
      },
      gaps: [],
    };
  }
  const gaps: string[] = [];
  let status: AxisStatus;
  if (coverage.pct >= COLUMN_DOC_PASS_PCT) {
    status = "pass";
  } else if (coverage.pct >= COLUMN_DOC_WARN_PCT) {
    status = "warn";
    gaps.push(
      `${coverage.pct}% of columns have descriptions (${coverage.described}/${coverage.total}). Lift to ${COLUMN_DOC_PASS_PCT}%+ before promoting. Find undocumented columns via catalog_search_columns({ tableId, isDocumented: false }).`
    );
  } else {
    status = "fail";
    gaps.push(
      `${coverage.pct}% of columns have descriptions (${coverage.described}/${coverage.total}) — below the ${COLUMN_DOC_WARN_PCT}% minimum. Use catalog_search_columns({ tableId, isDocumented: false }) to find undocumented columns and catalog_update_columns_metadata to describe them.`
    );
  }
  return {
    name: "columnDocs",
    status,
    signals: { ...coverage },
    gaps,
  };
}

function gradeLineage(
  direction: "upstream" | "downstream",
  count: number
): AxisResult {
  const axisName: AxisName =
    direction === "upstream" ? "upstreamLineage" : "downstreamLineage";
  if (count === 0) {
    // Warn, not fail. A "ready data product" at the source of a pipeline
    // legitimately has no upstream (raw source table); a product at the
    // leaf of a pipeline legitimately has no downstream (terminal report).
    // The warn flags it for manual verification rather than blocking
    // promotion on a structurally-valid zero.
    return {
      name: axisName,
      status: "warn",
      signals: { edgeCount: 0 },
      gaps: [
        `No ${direction} lineage edges detected. Verify this is intentional (e.g. a source-of-truth table has no upstream, a terminal report has no downstream). If not, investigate via catalog_trace_missing_lineage or patch with catalog_upsert_lineages.`,
      ],
    };
  }
  return {
    name: axisName,
    status: "pass",
    signals: { edgeCount: count },
    gaps: [],
  };
}

function gradeQualityChecks(
  assetKind: AssetKind,
  count: number | null
): AxisResult {
  if (assetKind === "DASHBOARD" || count === null) {
    return {
      name: "qualityChecks",
      status: "na",
      signals: { reason: "dashboards have no quality-check signal" },
      gaps: [],
    };
  }
  if (count === 0) {
    return {
      name: "qualityChecks",
      status: "fail",
      signals: { checkCount: 0 },
      gaps: [
        "No data-quality checks attached. Register dbt / Monte Carlo / Soda / Great Expectations results via catalog_upsert_data_qualities.",
      ],
    };
  }
  return {
    name: "qualityChecks",
    status: "pass",
    signals: { checkCount: count },
    gaps: [],
  };
}

function gradeVerification(row: Record<string, unknown>): AxisResult {
  const isDeprecated = row.isDeprecated === true;
  const isVerified = row.isVerified === true;
  if (isDeprecated) {
    return {
      name: "verification",
      status: "fail",
      signals: { isDeprecated: true, isVerified },
      gaps: [
        "Asset is marked deprecated — it cannot be promoted as a data product in its current state.",
      ],
    };
  }
  if (!isVerified) {
    return {
      name: "verification",
      status: "warn",
      signals: { isDeprecated: false, isVerified: false },
      gaps: [
        "Asset is not marked verified. Verify in the Catalog UI (or leave as a data-steward follow-up) before promoting.",
      ],
    };
  }
  return {
    name: "verification",
    status: "pass",
    signals: { isDeprecated: false, isVerified: true },
    gaps: [],
  };
}

export function defineAuditDataProductReadiness(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_audit_data_product_readiness",
    config: {
      title: "Audit Data Product Readiness",
      description:
        "Per-asset promotion-readiness report for a TABLE or DASHBOARD: grades eight governance axes (description, ownership, tags, column-doc coverage, upstream/downstream lineage, quality checks, verification) and returns per-axis `status: \"pass\"|\"warn\"|\"fail\"|\"na\"`, raw `signals`, and actionable `gaps` text. The aggregate tells the caller whether the asset is `readyToPromote` (no axes failing).\n\n" +
        "Thresholds are deterministic and hardcoded so two callers grading the same asset get the same answer:\n" +
        "  - description: fail <20 chars / warn <80 chars / pass 80+ chars. Checks all three surfaces (description, descriptionRaw, externalDescription) and uses the longest.\n" +
        "  - ownership: pass if any user OR team owner is attached; fail otherwise.\n" +
        "  - tags: pass if ≥1 tag attached; warn if 0 (tags are soft-required — some data products legitimately omit them).\n" +
        "  - columnDocs (TABLE only): pass ≥80%, warn ≥50%, fail <50%. Samples up to `columnSampleCap` columns per table (default 200); wide tables report `sampled: true`.\n" +
        "  - upstream/downstream lineage: warn on zero (terminal / source assets legitimately have none); pass on ≥1 edge.\n" +
        "  - qualityChecks (TABLE only): fail if 0 checks attached; pass if ≥1.\n" +
        "  - verification: fail if isDeprecated; warn if !isVerified; pass if verified.\n\n" +
        "`readyToPromote` is true iff no axes fail (warns are allowed). DASHBOARD assets always report `status: \"na\"` for columnDocs and qualityChecks — those axes have no dashboard-level signal.\n\n" +
        "Replaces 5+ chained calls (detail + columns + lineage up/down + quality) with one audit. Pair with catalog_governance_scorecard for cross-asset rollups, catalog_assess_impact for deprecation-grade blast-radius, and catalog_owner_scorecard for per-owner hygiene.",
      inputSchema: AuditInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const assetKind = args.assetKind as AssetKind;
      const assetId = args.assetId as string;
      const requestedAxes = (args.axes as AxisName[] | undefined) ?? ALL_AXES;
      const axesSet = new Set<AxisName>(requestedAxes);
      const columnSampleCap =
        (args.columnSampleCap as number | undefined) ?? DEFAULT_COLUMN_SAMPLE_CAP;

      // Fetch asset detail first. Everything else (lineage counts, column
      // coverage, quality counts) fans out in parallel once we know the
      // asset exists.
      const detailResp =
        assetKind === "TABLE"
          ? await c.execute<{ getTables: { data: Record<string, unknown>[] } }>(
              GET_TABLE_DETAIL,
              { ids: [assetId] }
            )
          : await c.execute<{
              getDashboards: { data: Record<string, unknown>[] };
            }>(GET_DASHBOARD_DETAIL, { ids: [assetId] });
      const detailRow =
        assetKind === "TABLE"
          ? (detailResp as { getTables: { data: Record<string, unknown>[] } })
              .getTables.data[0]
          : (detailResp as {
              getDashboards: { data: Record<string, unknown>[] };
            }).getDashboards.data[0];

      if (!detailRow) {
        return { notFound: true, assetKind, assetId };
      }

      const needsColumnDocs =
        axesSet.has("columnDocs") && assetKind === "TABLE";
      const needsUpstream = axesSet.has("upstreamLineage");
      const needsDownstream = axesSet.has("downstreamLineage");
      const needsQuality =
        axesSet.has("qualityChecks") && assetKind === "TABLE";

      const [coverage, upstreamCount, downstreamCount, qualityCount] =
        await Promise.all([
          needsColumnDocs
            ? probeColumnDocCoverage(c, assetId, columnSampleCap)
            : Promise.resolve(null),
          needsUpstream
            ? probeLineageEdgeCount(c, {
                direction: "upstream",
                assetKind,
                assetId,
              })
            : Promise.resolve(null),
          needsDownstream
            ? probeLineageEdgeCount(c, {
                direction: "downstream",
                assetKind,
                assetId,
              })
            : Promise.resolve(null),
          needsQuality
            ? probeQualityCheckCount(c, assetId)
            : Promise.resolve(null),
        ]);

      const axisResults: AxisResult[] = [];
      for (const axis of requestedAxes) {
        switch (axis) {
          case "description":
            axisResults.push(gradeDescription(detailRow));
            break;
          case "ownership":
            axisResults.push(gradeOwnership(detailRow));
            break;
          case "tags":
            axisResults.push(gradeTags(detailRow));
            break;
          case "columnDocs":
            axisResults.push(gradeColumnDocs(assetKind, coverage));
            break;
          case "upstreamLineage":
            axisResults.push(
              gradeLineage("upstream", upstreamCount ?? 0)
            );
            break;
          case "downstreamLineage":
            axisResults.push(
              gradeLineage("downstream", downstreamCount ?? 0)
            );
            break;
          case "qualityChecks":
            axisResults.push(gradeQualityChecks(assetKind, qualityCount));
            break;
          case "verification":
            axisResults.push(gradeVerification(detailRow));
            break;
        }
      }

      const counts = { pass: 0, warn: 0, fail: 0, na: 0 };
      for (const a of axisResults) counts[a.status] += 1;
      const failingAxes = axisResults
        .filter((a) => a.status === "fail")
        .map((a) => a.name);
      const warningAxes = axisResults
        .filter((a) => a.status === "warn")
        .map((a) => a.name);

      return {
        asset: {
          id: detailRow.id,
          kind: assetKind,
          name: detailRow.name ?? null,
          popularity: detailRow.popularity ?? null,
          isVerified: detailRow.isVerified ?? null,
          isDeprecated: detailRow.isDeprecated ?? null,
          ...(assetKind === "TABLE"
            ? {
                tableType: detailRow.tableType ?? null,
                schemaId: detailRow.schemaId ?? null,
              }
            : {
                type: detailRow.type ?? null,
                folderPath: detailRow.folderPath ?? null,
              }),
        },
        axes: axisResults,
        overall: {
          axesEvaluated: requestedAxes,
          passCount: counts.pass,
          warnCount: counts.warn,
          failCount: counts.fail,
          naCount: counts.na,
          readyToPromote: counts.fail === 0,
          failingAxes,
          warningAxes,
        },
      };
    }, client),
  };
}
