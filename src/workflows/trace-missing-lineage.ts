import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_TABLE_DETAIL,
  GET_LINEAGES,
  GET_FIELD_LINEAGES,
  GET_COLUMNS_SUMMARY,
} from "../catalog/operations.js";
import type {
  GetColumnsOutput,
  GetFieldLineagesOutput,
  GetLineagesOutput,
  LineageType,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";

const TraceMissingLineageInputShape = {
  tableId: z
    .string()
    .min(1)
    .describe("Catalog UUID of the table to inspect."),
  columnSampleSize: z
    .number()
    .int()
    .min(1)
    .max(50)
    .optional()
    .describe(
      "Number of columns to probe for field-lineage coverage. Default 10. Higher values give a more accurate coverage percentage at the cost of more GraphQL calls."
    ),
};

interface Finding {
  severity: "info" | "warning" | "alert";
  code: string;
  message: string;
  recommendation?: string;
}

export function dominantLineageType(
  rows: Array<{ lineageType?: LineageType | null | undefined }>
): LineageType | null {
  const counts = new Map<LineageType, number>();
  for (const r of rows) {
    if (!r.lineageType) continue;
    counts.set(r.lineageType, (counts.get(r.lineageType) ?? 0) + 1);
  }
  let top: LineageType | null = null;
  let best = 0;
  for (const [k, v] of counts) {
    if (v > best) {
      top = k;
      best = v;
    }
  }
  return top;
}

export function defineTraceMissingLineage(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_trace_missing_lineage",
    config: {
      title: "Diagnose Lineage Coverage Gaps",
      description:
        "Inspect a table and heuristically identify where its lineage coverage is thin or missing. Checks:\n" +
        "  1. Table-level upstream edges — 0 means either a true source, or missing lineage.\n" +
        "  2. Table-level downstream edges — 0 means either unused, or missing downstream.\n" +
        "  3. Column-level field-lineage coverage — probes N sampled columns for any parent field edge; reports the coverage percentage.\n" +
        "  4. Lineage provenance — surfaces whether edges are AUTOMATIC (detected by Catalog) or MANUAL (curator-added). All-manual is a signal that automatic detection may be broken.\n\n" +
        "Returns a structured `findings[]` list with severity (info/warning/alert) + recommendation text. This is diagnostic, not authoritative — treat the recommendations as a starting point for investigation. Use catalog_get_lineages / catalog_get_field_lineages to follow up on specific gaps.",
      inputSchema: TraceMissingLineageInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const tableId = args.tableId as string;
      const sampleSize = (args.columnSampleSize as number | undefined) ?? 10;

      // Fetch in parallel: detail, upstream edges, downstream edges, columns
      const [detailRes, upstreamRes, downstreamRes, columnsRes] =
        await Promise.allSettled([
          c.execute<{ getTables: { data: Record<string, unknown>[] } }>(
            GET_TABLE_DETAIL,
            { ids: [tableId] }
          ),
          c.execute<{ getLineages: GetLineagesOutput }>(GET_LINEAGES, {
            scope: { childTableId: tableId },
            pagination: { nbPerPage: 500, page: 0 },
          }),
          c.execute<{ getLineages: GetLineagesOutput }>(GET_LINEAGES, {
            scope: { parentTableId: tableId },
            pagination: { nbPerPage: 500, page: 0 },
          }),
          c.execute<{ getColumns: GetColumnsOutput }>(GET_COLUMNS_SUMMARY, {
            scope: { tableId },
            sorting: [{ sortingKey: "sourceOrder", direction: "ASC" }],
            pagination: { nbPerPage: sampleSize, page: 0 },
          }),
        ]);

      if (detailRes.status !== "fulfilled") {
        return {
          error: "Failed to fetch table detail",
          detail:
            detailRes.reason instanceof Error
              ? detailRes.reason.message
              : String(detailRes.reason),
        };
      }
      const row = detailRes.value.getTables.data[0];
      if (!row) return { notFound: true, tableId };

      const findings: Finding[] = [];
      const upstreamOk = upstreamRes.status === "fulfilled";
      const downstreamOk = downstreamRes.status === "fulfilled";
      const columnsOk = columnsRes.status === "fulfilled";

      const upstream = upstreamOk ? upstreamRes.value.getLineages : null;
      const downstream = downstreamOk ? downstreamRes.value.getLineages : null;
      const columns = columnsOk ? columnsRes.value.getColumns : null;

      // (1) Upstream lineage absence
      if (upstream && upstream.totalCount === 0) {
        findings.push({
          severity: "warning",
          code: "no_upstream_table_lineage",
          message: "Table has zero upstream lineage edges.",
          recommendation:
            "If this table is expected to be derived from other tables, automatic lineage may have failed. Review the SQL that populates it, or add manual edges via upsertLineages.",
        });
      }

      // (2) Downstream lineage absence
      if (downstream && downstream.totalCount === 0) {
        findings.push({
          severity: "info",
          code: "no_downstream_table_lineage",
          message: "Table has zero downstream lineage edges.",
          recommendation:
            "Either the table is genuinely unused (candidate for deprecation) or downstream consumers haven't been ingested. Cross-check against catalog_get_table_queries to see whether the table is still being read.",
        });
      }

      // (3) Provenance — all-manual is a tell
      if (upstream && upstream.data.length > 0) {
        const dominant = dominantLineageType(upstream.data);
        if (dominant && dominant !== "AUTOMATIC") {
          findings.push({
            severity: "warning",
            code: "upstream_lineage_all_manual",
            message: `Dominant upstream lineage type is ${dominant} (not AUTOMATIC).`,
            recommendation:
              "Automatic lineage detection may have missed this table's dependencies. Validate that the source warehouse is ingested correctly, or keep relying on manual edges.",
          });
        }
      }

      // (4) Field-level coverage
      let fieldCoverage: {
        sampledColumnCount: number;
        columnsWithUpstream: number;
        coveragePct: number;
        details: Array<{
          columnId: string;
          columnName: string;
          upstreamCount: number;
        }>;
      } | null = null;

      if (columns && columns.data.length > 0) {
        const sampledColumns = columns.data.slice(0, sampleSize);
        const fieldResults = await Promise.allSettled(
          sampledColumns.map((col) =>
            c.execute<{ getFieldLineages: GetFieldLineagesOutput }>(
              GET_FIELD_LINEAGES,
              {
                scope: { childColumnId: col.id as string },
                pagination: { nbPerPage: 1, page: 0 },
              }
            )
          )
        );
        const details = fieldResults.map((r, i) => {
          const col = sampledColumns[i] as Record<string, unknown>;
          if (r.status !== "fulfilled") {
            return {
              columnId: col.id as string,
              columnName: col.name as string,
              upstreamCount: -1,
            };
          }
          return {
            columnId: col.id as string,
            columnName: col.name as string,
            upstreamCount: r.value.getFieldLineages.totalCount,
          };
        });
        const valid = details.filter((d) => d.upstreamCount >= 0);
        const withUpstream = valid.filter((d) => d.upstreamCount > 0).length;
        const coveragePct =
          valid.length === 0
            ? 0
            : Math.round((withUpstream / valid.length) * 100);
        fieldCoverage = {
          sampledColumnCount: valid.length,
          columnsWithUpstream: withUpstream,
          coveragePct,
          details,
        };

        if (valid.length > 0 && coveragePct === 0) {
          findings.push({
            severity: "alert",
            code: "no_field_lineage",
            message: `None of the ${valid.length} sampled columns have upstream field lineage.`,
            recommendation:
              "Column-level lineage is completely absent — this blocks impact analysis. Verify the source system is supported for field-lineage extraction, or supply manual field-level edges.",
          });
        } else if (valid.length > 0 && coveragePct < 50) {
          findings.push({
            severity: "warning",
            code: "partial_field_lineage",
            message: `Only ${withUpstream}/${valid.length} sampled columns have upstream field lineage (${coveragePct}%).`,
            recommendation:
              "Partial coverage often means the SQL uses constructs (e.g. SELECT *, dynamic SQL, stored procs) that automatic detection can't parse. Consider narrowing and rerunning lineage, or supplying manual edges for the uncovered columns.",
          });
        }
      }

      return {
        table: {
          id: row.id,
          name: row.name,
          tableType: row.tableType,
          transformationSource: row.transformationSource,
          schemaId: row.schemaId,
        },
        tableLineage: {
          upstream: upstream
            ? { totalCount: upstream.totalCount }
            : {
                error:
                  upstreamRes.status === "rejected"
                    ? String(upstreamRes.reason)
                    : "unknown",
              },
          downstream: downstream
            ? { totalCount: downstream.totalCount }
            : {
                error:
                  downstreamRes.status === "rejected"
                    ? String(downstreamRes.reason)
                    : "unknown",
              },
        },
        fieldLineageCoverage: fieldCoverage,
        findings,
        summary: {
          findingCount: findings.length,
          severityCounts: {
            alert: findings.filter((f) => f.severity === "alert").length,
            warning: findings.filter((f) => f.severity === "warning").length,
            info: findings.filter((f) => f.severity === "info").length,
          },
        },
      };
    }, client),
  };
}
