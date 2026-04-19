import { describe, it, expect } from "vitest";
import { defineTraceMissingLineage, dominantLineageType } from "../../src/workflows/trace-missing-lineage.js";
import {
  GET_TABLE_DETAIL,
  GET_LINEAGES,
  GET_FIELD_LINEAGES,
  GET_COLUMNS_SUMMARY,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: { content: { text: string }[] }): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

const TABLE_ROW = {
  id: "t-1",
  name: "ORDERS",
  tableType: "TABLE",
  transformationSource: "DBT",
  schemaId: "s-1",
};

interface RouterSpec {
  detail?: unknown | Error;
  upstream?: unknown | Error;
  downstream?: unknown | Error;
  columns?: unknown | Error;
  fieldLineages?: Array<unknown | Error>;
}

// Routes responses by document + lineage direction + a per-column queue for
// field-lineage calls so the assertions read straightforwardly regardless of
// Promise.allSettled scheduling.
function makeRouter(spec: RouterSpec) {
  const fieldQueue = [...(spec.fieldLineages ?? [])];
  return makeMockClient((document, variables) => {
    if (document === GET_TABLE_DETAIL) {
      if (spec.detail instanceof Error) throw spec.detail;
      return spec.detail ?? { getTables: { data: [TABLE_ROW] } };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as { scope?: Record<string, unknown> };
      const isUpstream = vars.scope?.childTableId !== undefined;
      const r = isUpstream ? spec.upstream : spec.downstream;
      if (r instanceof Error) throw r;
      return r ?? { getLineages: { totalCount: 0, data: [] } };
    }
    if (document === GET_COLUMNS_SUMMARY) {
      if (spec.columns instanceof Error) throw spec.columns;
      return spec.columns ?? { getColumns: { totalCount: 0, data: [] } };
    }
    if (document === GET_FIELD_LINEAGES) {
      const next = fieldQueue.shift();
      if (next === undefined) {
        throw new Error("field-lineage queue exhausted");
      }
      if (next instanceof Error) throw next;
      return next;
    }
    throw new Error(`unexpected document: ${document.slice(0, 40)}…`);
  });
}

describe("catalog_trace_missing_lineage — dominant type helper", () => {
  it("returns null for an empty list", () => {
    expect(dominantLineageType([])).toBeNull();
  });

  it("returns null when every row has no lineageType", () => {
    expect(
      dominantLineageType([{ lineageType: null }, { lineageType: undefined }])
    ).toBeNull();
  });

  it("returns the single type when uniform", () => {
    expect(
      dominantLineageType([
        { lineageType: "AUTOMATIC" },
        { lineageType: "AUTOMATIC" },
      ])
    ).toBe("AUTOMATIC");
  });

  it("returns the plurality winner when mixed", () => {
    expect(
      dominantLineageType([
        { lineageType: "AUTOMATIC" },
        { lineageType: "MANUAL_CUSTOMER" },
        { lineageType: "MANUAL_CUSTOMER" },
      ])
    ).toBe("MANUAL_CUSTOMER");
  });
});

describe("catalog_trace_missing_lineage — handler", () => {
  it("returns notFound when detail has no rows", async () => {
    const client = makeRouter({ detail: { getTables: { data: [] } } });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "missing" });
    expect(parseResult(res)).toEqual({ notFound: true, tableId: "missing" });
  });

  it("surfaces a top-level error when detail itself rejects", async () => {
    const client = makeRouter({ detail: new Error("detail blew up") });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "t-1" });
    const out = parseResult(res);
    expect(out.error).toBe("Failed to fetch table detail");
    expect(out.detail).toMatch(/detail blew up/);
  });

  it("flags zero-upstream and zero-downstream with the right severities", async () => {
    const client = makeRouter({
      upstream: { getLineages: { totalCount: 0, data: [] } },
      downstream: { getLineages: { totalCount: 0, data: [] } },
    });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "t-1" });
    const out = parseResult(res);
    const findings = out.findings as Array<Record<string, unknown>>;

    const codes = findings.map((f) => f.code);
    expect(codes).toContain("no_upstream_table_lineage");
    expect(codes).toContain("no_downstream_table_lineage");
    const upstreamFinding = findings.find(
      (f) => f.code === "no_upstream_table_lineage"
    );
    const downstreamFinding = findings.find(
      (f) => f.code === "no_downstream_table_lineage"
    );
    expect(upstreamFinding?.severity).toBe("warning");
    expect(downstreamFinding?.severity).toBe("info");
    expect(out.summary).toMatchObject({
      severityCounts: { warning: 1, info: 1, alert: 0 },
    });
  });

  it("flags upstream_lineage_all_manual when dominant type is MANUAL_*", async () => {
    const client = makeRouter({
      upstream: {
        getLineages: {
          totalCount: 3,
          data: [
            { lineageType: "MANUAL_CUSTOMER" },
            { lineageType: "MANUAL_CUSTOMER" },
            { lineageType: "AUTOMATIC" },
          ],
        },
      },
      downstream: { getLineages: { totalCount: 2, data: [{ id: "l-1" }] } },
    });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "t-1" });
    const findings = (parseResult(res).findings as Array<Record<string, unknown>>);
    const manual = findings.find(
      (f) => f.code === "upstream_lineage_all_manual"
    );
    expect(manual).toBeDefined();
    expect((manual?.message as string)).toMatch(/MANUAL_CUSTOMER/);
  });

  it("raises no_field_lineage (alert) when every sampled column has zero upstream edges", async () => {
    const client = makeRouter({
      upstream: { getLineages: { totalCount: 1, data: [{ lineageType: "AUTOMATIC" }] } },
      downstream: { getLineages: { totalCount: 1, data: [{ id: "l-1" }] } },
      columns: {
        getColumns: {
          totalCount: 2,
          data: [
            { id: "c-1", name: "ID" },
            { id: "c-2", name: "TOTAL" },
          ],
        },
      },
      fieldLineages: [
        { getFieldLineages: { totalCount: 0, data: [] } },
        { getFieldLineages: { totalCount: 0, data: [] } },
      ],
    });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "t-1" });
    const out = parseResult(res);
    const findings = out.findings as Array<Record<string, unknown>>;
    const alert = findings.find((f) => f.code === "no_field_lineage");
    expect(alert).toBeDefined();
    expect(alert?.severity).toBe("alert");
    expect(out.fieldLineageCoverage).toMatchObject({
      sampledColumnCount: 2,
      columnsWithUpstream: 0,
      coveragePct: 0,
    });
  });

  it("raises partial_field_lineage (warning) when coverage is below 50%", async () => {
    const client = makeRouter({
      upstream: { getLineages: { totalCount: 1, data: [{ lineageType: "AUTOMATIC" }] } },
      downstream: { getLineages: { totalCount: 1, data: [{ id: "l-1" }] } },
      columns: {
        getColumns: {
          totalCount: 3,
          data: [
            { id: "c-1", name: "ID" },
            { id: "c-2", name: "TOTAL" },
            { id: "c-3", name: "AMOUNT" },
          ],
        },
      },
      fieldLineages: [
        { getFieldLineages: { totalCount: 1, data: [] } },
        { getFieldLineages: { totalCount: 0, data: [] } },
        { getFieldLineages: { totalCount: 0, data: [] } },
      ],
    });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "t-1" });
    const out = parseResult(res);
    const findings = out.findings as Array<Record<string, unknown>>;
    const partial = findings.find((f) => f.code === "partial_field_lineage");
    expect(partial).toBeDefined();
    expect(partial?.severity).toBe("warning");
    expect(out.fieldLineageCoverage).toMatchObject({
      sampledColumnCount: 3,
      columnsWithUpstream: 1,
      coveragePct: 33,
    });
  });

  it("marks a rejected field-lineage call with upstreamCount=-1 and excludes it from coverage math", async () => {
    const client = makeRouter({
      upstream: { getLineages: { totalCount: 1, data: [{ lineageType: "AUTOMATIC" }] } },
      downstream: { getLineages: { totalCount: 1, data: [{ id: "l-1" }] } },
      columns: {
        getColumns: {
          totalCount: 2,
          data: [
            { id: "c-1", name: "ID" },
            { id: "c-2", name: "TOTAL" },
          ],
        },
      },
      // First column throws; second succeeds with 1 upstream → coverage should
      // be 1/1 = 100%, not 1/2 = 50%.
      fieldLineages: [
        new Error("field lineage endpoint down"),
        { getFieldLineages: { totalCount: 1, data: [] } },
      ],
    });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "t-1" });
    const out = parseResult(res);
    const coverage = out.fieldLineageCoverage as Record<string, unknown>;
    expect(coverage.sampledColumnCount).toBe(1);
    expect(coverage.columnsWithUpstream).toBe(1);
    expect(coverage.coveragePct).toBe(100);
    const details = coverage.details as Array<Record<string, unknown>>;
    expect(details.find((d) => d.columnId === "c-1")?.upstreamCount).toBe(-1);
  });

  it("surfaces per-section errors inline when upstream/downstream reject", async () => {
    const client = makeRouter({
      upstream: new Error("upstream down"),
      downstream: new Error("downstream down"),
    });
    const tool = defineTraceMissingLineage(client);
    const res = await tool.handler({ tableId: "t-1" });
    const out = parseResult(res);
    const lineage = out.tableLineage as Record<string, Record<string, unknown>>;
    expect(lineage.upstream.error).toMatch(/upstream down/);
    expect(lineage.downstream.error).toMatch(/downstream down/);
    // No findings about lineage counts when we couldn't read them.
    const codes = (out.findings as Array<Record<string, unknown>>).map(
      (f) => f.code
    );
    expect(codes).not.toContain("no_upstream_table_lineage");
    expect(codes).not.toContain("no_downstream_table_lineage");
  });
});
