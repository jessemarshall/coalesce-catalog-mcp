import { describe, it, expect } from "vitest";
import { defineAuditDataProductReadiness } from "../../src/workflows/audit-data-product-readiness.js";
import {
  GET_TABLE_DETAIL,
  GET_DASHBOARD_DETAIL,
  GET_COLUMNS_SUMMARY,
  GET_DATA_QUALITIES,
  GET_LINEAGES,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
  isError?: boolean;
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockColumn {
  id: string;
  tableId: string;
  name: string;
  description?: string | null;
}

interface RouterOpts {
  tableDetail?: Record<string, unknown> | null;
  dashboardDetail?: Record<string, unknown> | null;
  columns?: MockColumn[];
  upstreamTotal?: number;
  downstreamTotal?: number;
  qualityTotal?: number;
  columnsTotalOverride?: number;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLE_DETAIL) {
      return {
        getTables: {
          data: opts.tableDetail ? [opts.tableDetail] : [],
        },
      };
    }
    if (document === GET_DASHBOARD_DETAIL) {
      return {
        getDashboards: {
          data: opts.dashboardDetail ? [opts.dashboardDetail] : [],
        },
      };
    }
    if (document === GET_COLUMNS_SUMMARY) {
      const vars = variables as {
        pagination: { nbPerPage: number; page: number };
      };
      const all = opts.columns ?? [];
      const start = vars.pagination.page * vars.pagination.nbPerPage;
      const slice = all.slice(start, start + vars.pagination.nbPerPage);
      return {
        getColumns: {
          totalCount: opts.columnsTotalOverride ?? all.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data: slice,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: Record<string, string>;
      };
      const isDownstream =
        !!vars.scope?.parentTableId || !!vars.scope?.parentDashboardId;
      const total = isDownstream
        ? (opts.downstreamTotal ?? 0)
        : (opts.upstreamTotal ?? 0);
      return {
        getLineages: { totalCount: total, nbPerPage: 1, page: 0, data: [] },
      };
    }
    if (document === GET_DATA_QUALITIES) {
      return {
        getDataQualities: {
          totalCount: opts.qualityTotal ?? 0,
          nbPerPage: 1,
          page: 0,
          data: [],
        },
      };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

function makeColumns(
  tableId: string,
  total: number,
  describedCount: number
): MockColumn[] {
  return Array.from({ length: total }, (_, i) => ({
    id: `${tableId}-c${i}`,
    tableId,
    name: `col_${i}`,
    description: i < describedCount ? "documented" : null,
  }));
}

describe("catalog_audit_data_product_readiness — asset not found", () => {
  it("returns notFound rather than throwing when the asset id doesn't resolve", async () => {
    const client = makeRouter({ tableDetail: null });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({ assetKind: "TABLE", assetId: "missing" })
    );
    expect(out).toEqual({
      notFound: true,
      assetKind: "TABLE",
      assetId: "missing",
    });
  });
});

describe("catalog_audit_data_product_readiness — all axes happy path", () => {
  it("grades a fully-promoted table as readyToPromote with no gaps", async () => {
    const tableDetail = {
      id: "t-1",
      name: "ORDERS",
      popularity: 0.9,
      tableType: "TABLE",
      schemaId: "sch-1",
      isVerified: true,
      isDeprecated: false,
      description:
        "Business-grade orders fact table, one row per order. Grain is the order_id; partitioned daily.",
      ownerEntities: [
        {
          id: "o1",
          userId: "u1",
          user: { id: "u1", email: "alice@example.com", fullName: "Alice" },
        },
      ],
      teamOwnerEntities: [
        { id: "to1", teamId: "team-1", team: { name: "Revenue Analytics" } },
      ],
      tagEntities: [{ id: "te1", tag: { label: "pii-reviewed" } }],
    };
    const client = makeRouter({
      tableDetail,
      columns: makeColumns("t-1", 10, 9), // 90% documented
      upstreamTotal: 3,
      downstreamTotal: 8,
      qualityTotal: 5,
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({ assetKind: "TABLE", assetId: "t-1" })
    );

    expect(out.asset).toMatchObject({ id: "t-1", kind: "TABLE", name: "ORDERS" });
    const overall = out.overall as Record<string, unknown>;
    expect(overall.readyToPromote).toBe(true);
    expect(overall.failCount).toBe(0);
    expect(overall.warnCount).toBe(0);
    const axes = out.axes as Array<{ name: string; status: string }>;
    expect(axes.map((a) => a.name)).toEqual([
      "description",
      "ownership",
      "tags",
      "columnDocs",
      "upstreamLineage",
      "downstreamLineage",
      "qualityChecks",
      "verification",
    ]);
    for (const axis of axes) {
      expect(axis.status).toBe("pass");
    }
  });
});

describe("catalog_audit_data_product_readiness — failure modes per axis", () => {
  it("fails description when absent and warns when thin", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t-2",
        name: "RAW_T",
        ownerEntities: [],
        teamOwnerEntities: [],
        tagEntities: [],
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-2",
        axes: ["description"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("fail");
    expect(axis.gaps).toEqual([
      expect.stringMatching(/No description surfaces/i),
    ]);
  });

  it("warns description when present but under the 80-char promotion bar", async () => {
    // 36 chars — past the 20-char fail threshold, below the 80-char pass bar.
    const thin = "orders fact table — daily refreshed.";
    const client = makeRouter({
      tableDetail: {
        id: "t-3",
        name: "T",
        description: thin,
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-3",
        axes: ["description"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("warn");
    expect((axis.signals as { length: number }).length).toBe(thin.length);
  });

  it("fails ownership when no user/team owner attached", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t-4",
        name: "T",
        ownerEntities: [],
        teamOwnerEntities: [],
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-4",
        axes: ["ownership"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("fail");
  });

  it("treats orphaned owner rows (userId:null) as unowned", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t-5",
        name: "T",
        // Binding exists but userId is null — functionally unowned. Same rule
        // governance_scorecard applies; test pins the parity so the two
        // tools agree on the same asset.
        ownerEntities: [{ id: "o1", userId: null, user: null }],
        teamOwnerEntities: [],
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-5",
        axes: ["ownership"],
      })
    );
    expect((out.axes as Array<Record<string, unknown>>)[0].status).toBe("fail");
  });

  it("fails columnDocs below 50%, warns at 50-80%, passes at 80%+", async () => {
    async function run(pct: number): Promise<string> {
      const described = Math.floor((pct / 100) * 10);
      const client = makeRouter({
        tableDetail: { id: "t", name: "T" },
        columns: makeColumns("t", 10, described),
      });
      const tool = defineAuditDataProductReadiness(client);
      const out = parseResult(
        await tool.handler({
          assetKind: "TABLE",
          assetId: "t",
          axes: ["columnDocs"],
        })
      );
      return (out.axes as Array<Record<string, unknown>>)[0].status as string;
    }
    expect(await run(0)).toBe("fail");
    expect(await run(40)).toBe("fail");
    expect(await run(60)).toBe("warn");
    expect(await run(80)).toBe("pass");
    expect(await run(100)).toBe("pass");
  });

  it("flags sampled: true when a table exceeds columnSampleCap", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-wide", name: "WIDE" },
      columns: makeColumns("t-wide", 300, 300),
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-wide",
        axes: ["columnDocs"],
        columnSampleCap: 100,
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("pass");
    expect(axis.signals).toMatchObject({
      described: 100,
      total: 100,
      pct: 100,
      sampled: true,
    });
  });

  it("warns both lineage axes on an isolated asset (zero edges either side)", async () => {
    const client = makeRouter({
      tableDetail: { id: "t-iso", name: "T" },
      upstreamTotal: 0,
      downstreamTotal: 0,
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-iso",
        axes: ["upstreamLineage", "downstreamLineage"],
      })
    );
    const axes = out.axes as Array<Record<string, unknown>>;
    expect(axes[0].status).toBe("warn");
    expect(axes[1].status).toBe("warn");
  });

  it("fails qualityChecks when zero, passes when any", async () => {
    const clientFail = makeRouter({
      tableDetail: { id: "t", name: "T" },
      qualityTotal: 0,
    });
    const toolFail = defineAuditDataProductReadiness(clientFail);
    const outFail = parseResult(
      await toolFail.handler({
        assetKind: "TABLE",
        assetId: "t",
        axes: ["qualityChecks"],
      })
    );
    expect(
      (outFail.axes as Array<Record<string, unknown>>)[0].status
    ).toBe("fail");

    const clientPass = makeRouter({
      tableDetail: { id: "t", name: "T" },
      qualityTotal: 3,
    });
    const toolPass = defineAuditDataProductReadiness(clientPass);
    const outPass = parseResult(
      await toolPass.handler({
        assetKind: "TABLE",
        assetId: "t",
        axes: ["qualityChecks"],
      })
    );
    expect(
      (outPass.axes as Array<Record<string, unknown>>)[0].status
    ).toBe("pass");
  });

  it("fails verification on a deprecated asset, warns when unverified, passes when verified", async () => {
    async function run(flags: {
      isVerified?: boolean;
      isDeprecated?: boolean;
    }): Promise<string> {
      const client = makeRouter({
        tableDetail: { id: "t", name: "T", ...flags },
      });
      const tool = defineAuditDataProductReadiness(client);
      const out = parseResult(
        await tool.handler({
          assetKind: "TABLE",
          assetId: "t",
          axes: ["verification"],
        })
      );
      return (out.axes as Array<Record<string, unknown>>)[0].status as string;
    }
    expect(await run({ isDeprecated: true })).toBe("fail");
    expect(await run({ isVerified: false })).toBe("warn");
    expect(await run({ isVerified: true })).toBe("pass");
  });
});

describe("catalog_audit_data_product_readiness — tags axis null-label handling", () => {
  // The Tag.label GraphQL scalar is non-nullable per src/generated/types.ts,
  // but the previous local extractTags helper preserved nulls defensively.
  // Pin the post-extractTagLabels semantic: tag entities that violate the
  // schema (label is null/empty/non-string) are filtered, so a row with N
  // such entities grades as warn (no usable labels) — NOT pass with N null
  // labels in the signals payload as the old helper produced.
  it("warns when every tag entity has a null label (schema-violating shape)", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t-null-tags",
        name: "T",
        // Two tag entities, both carrying tag.label = null. Old behaviour:
        // tagCount=2, status=pass, labels=[null, null]. New behaviour:
        // tagCount=0, status=warn, labels=[].
        tagEntities: [
          { id: "te1", tag: { label: null } },
          { id: "te2", tag: { label: null } },
        ],
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-null-tags",
        axes: ["tags"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("warn");
    expect(axis.signals).toEqual({ tagCount: 0, labels: [] });
    expect(axis.gaps).toEqual([
      expect.stringMatching(/No tags attached/i),
    ]);
  });

  it("warns when every tag entity has an empty-string label", async () => {
    // Same contract for empty strings — extractTagLabels filters
    // length===0 strings out.
    const client = makeRouter({
      tableDetail: {
        id: "t-empty-tags",
        name: "T",
        tagEntities: [{ id: "te1", tag: { label: "" } }],
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-empty-tags",
        axes: ["tags"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("warn");
    expect(axis.signals).toEqual({ tagCount: 0, labels: [] });
  });

  it("counts only non-empty string labels when entities mix valid + invalid tags", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t-mixed-tags",
        name: "T",
        tagEntities: [
          { id: "te1", tag: { label: "Critical" } },
          { id: "te2", tag: { label: null } },
          { id: "te3", tag: { label: "" } },
          { id: "te4", tag: { label: "PII" } },
        ],
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t-mixed-tags",
        axes: ["tags"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("pass");
    expect(axis.signals).toEqual({
      tagCount: 2,
      labels: ["Critical", "PII"],
    });
  });
});

describe("catalog_audit_data_product_readiness — dashboard asset quirks", () => {
  it("returns status: 'na' for columnDocs and qualityChecks on a DASHBOARD", async () => {
    const client = makeRouter({
      dashboardDetail: {
        id: "d-1",
        name: "Revenue Dashboard",
        type: "LOOKER",
        folderPath: "/revenue",
        description:
          "High-level revenue dashboard, refreshed hourly. Filters by region and tier. Used for the daily exec sync.",
        ownerEntities: [
          {
            id: "o1",
            userId: "u1",
            user: { email: "a@b.com", fullName: "A" },
          },
        ],
        teamOwnerEntities: [],
        tagEntities: [{ id: "te", tag: { label: "exec" } }],
        isVerified: true,
      },
      upstreamTotal: 2,
      downstreamTotal: 0, // terminal dashboard — warn expected
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({ assetKind: "DASHBOARD", assetId: "d-1" })
    );
    const axes = out.axes as Array<Record<string, unknown>>;
    const byName = Object.fromEntries(
      axes.map((a) => [a.name as string, a.status])
    );
    expect(byName.columnDocs).toBe("na");
    expect(byName.qualityChecks).toBe("na");
    expect(byName.downstreamLineage).toBe("warn");
    // N/A axes do NOT count toward the fail count — a dashboard can pass.
    expect((out.overall as { readyToPromote: boolean }).readyToPromote).toBe(
      true
    );
  });
});

describe("catalog_audit_data_product_readiness — readyToPromote semantics", () => {
  it("readyToPromote stays true when only warnings are present", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t",
        name: "T",
        description:
          "Business-grade orders fact table, one row per order. Grain is the order_id; partitioned daily.",
        ownerEntities: [
          { id: "o", userId: "u", user: { email: "a@b.com", fullName: "A" } },
        ],
        teamOwnerEntities: [],
        tagEntities: [], // warn only
        isVerified: false, // warn only
      },
      columns: makeColumns("t", 10, 10),
      upstreamTotal: 1,
      downstreamTotal: 1,
      qualityTotal: 1,
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({ assetKind: "TABLE", assetId: "t" })
    );
    const overall = out.overall as Record<string, unknown>;
    expect(overall.readyToPromote).toBe(true);
    expect(overall.warnCount).toBe(2);
    expect(overall.failCount).toBe(0);
  });

  it("readyToPromote flips to false when any axis fails", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t",
        name: "T",
        description: "ok", // fail (too short)
        ownerEntities: [
          { id: "o", userId: "u", user: { email: "a@b.com", fullName: "A" } },
        ],
        teamOwnerEntities: [],
        tagEntities: [{ id: "te", tag: { label: "t" } }],
        isVerified: true,
      },
      columns: makeColumns("t", 10, 10),
      upstreamTotal: 1,
      downstreamTotal: 1,
      qualityTotal: 1,
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({ assetKind: "TABLE", assetId: "t" })
    );
    const overall = out.overall as Record<string, unknown>;
    expect(overall.readyToPromote).toBe(false);
    expect(overall.failCount).toBe(1);
    expect(overall.failingAxes).toEqual(["description"]);
  });
});

describe("catalog_audit_data_product_readiness — description fallback surfaces", () => {
  it("reads the longest of description / descriptionRaw / externalDescription", async () => {
    // description is empty; externalDescription carries the real text.
    // The grader should find it via the fallback and grade against its length.
    const externalDescription =
      "Imported description from the source system; explains grain, freshness, and the relationship between this table and its upstream staging.";
    const client = makeRouter({
      tableDetail: {
        id: "t",
        name: "T",
        description: null,
        descriptionRaw: null,
        externalDescription,
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t",
        axes: ["description"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("pass");
    const signals = axis.signals as {
      length: number;
      hasDirectDescription: boolean;
      hasExternalDescription: boolean;
    };
    expect(signals.hasDirectDescription).toBe(false);
    expect(signals.hasExternalDescription).toBe(true);
    expect(signals.length).toBe(externalDescription.length);
  });

  it("treats whitespace-only description as empty (fail)", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t",
        name: "T",
        description: "   \n\t  ",
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t",
        axes: ["description"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("fail");
  });
});

describe("catalog_audit_data_product_readiness — columnDocs edge cases", () => {
  it("returns status: 'na' when a table has zero columns", async () => {
    const client = makeRouter({
      tableDetail: { id: "t", name: "T" },
      columns: [],
    });
    const tool = defineAuditDataProductReadiness(client);
    const out = parseResult(
      await tool.handler({
        assetKind: "TABLE",
        assetId: "t",
        axes: ["columnDocs"],
      })
    );
    const axis = (out.axes as Array<Record<string, unknown>>)[0];
    expect(axis.status).toBe("na");
    expect(axis.signals).toMatchObject({
      total: 0,
      reason: expect.stringMatching(/no columns/),
    });
  });
});

describe("catalog_audit_data_product_readiness — defensive guards", () => {
  it("throws (isError: true) when getColumns returns a non-numeric totalCount", async () => {
    // The column probe paginates and checks totalCount between pages when
    // the first page is full. A non-numeric totalCount should throw rather
    // than silently stop with an incomplete coverage picture.
    const client = makeMockClient((document, variables) => {
      if (document === GET_TABLE_DETAIL) {
        return { getTables: { data: [{ id: "t", name: "T" }] } };
      }
      if (document === GET_COLUMNS_SUMMARY) {
        const vars = variables as {
          pagination: { nbPerPage: number; page: number };
        };
        // Return a full page so the loop can't short-circuit on short-page.
        const data = Array.from(
          { length: vars.pagination.nbPerPage },
          (_, i) => ({
            id: `c-${i}`,
            tableId: "t",
            name: `col_${i}`,
            description: "x",
          })
        );
        return {
          getColumns: {
            totalCount: null,
            nbPerPage: vars.pagination.nbPerPage,
            page: vars.pagination.page,
            data,
          },
        };
      }
      return {};
    });
    const tool = defineAuditDataProductReadiness(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t",
      axes: ["columnDocs"],
      columnSampleCap: 1000,
    });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/non-numeric totalCount/);
  });

  it("throws (isError: true) when getLineages returns a non-numeric totalCount", async () => {
    const client = makeMockClient((document) => {
      if (document === GET_TABLE_DETAIL) {
        return { getTables: { data: [{ id: "t", name: "T" }] } };
      }
      if (document === GET_LINEAGES) {
        return {
          getLineages: { totalCount: null, nbPerPage: 1, page: 0, data: [] },
        };
      }
      return {};
    });
    const tool = defineAuditDataProductReadiness(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t",
      axes: ["upstreamLineage"],
    });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/non-numeric totalCount/);
  });

  it("throws (isError: true) when getDataQualities returns a non-numeric totalCount", async () => {
    const client = makeMockClient((document) => {
      if (document === GET_TABLE_DETAIL) {
        return { getTables: { data: [{ id: "t", name: "T" }] } };
      }
      if (document === GET_DATA_QUALITIES) {
        return {
          getDataQualities: {
            totalCount: null,
            nbPerPage: 1,
            page: 0,
            data: [],
          },
        };
      }
      return {};
    });
    const tool = defineAuditDataProductReadiness(client);
    const res = await tool.handler({
      assetKind: "TABLE",
      assetId: "t",
      axes: ["qualityChecks"],
    });
    expect(res.isError).toBe(true);
    expect(parseResult(res).error).toMatch(/non-numeric totalCount/);
  });
});

describe("catalog_audit_data_product_readiness — axes subsetting", () => {
  it("only runs probes for requested axes (no wasted API calls)", async () => {
    const client = makeRouter({
      tableDetail: {
        id: "t",
        name: "T",
        description:
          "A useful business-grade fact table that describes its purpose well enough to pass the promotion bar threshold.",
        ownerEntities: [
          { id: "o", userId: "u", user: { email: "a@b.com", fullName: "A" } },
        ],
      },
    });
    const tool = defineAuditDataProductReadiness(client);
    await tool.handler({
      assetKind: "TABLE",
      assetId: "t",
      axes: ["description", "ownership"],
    });
    // Only GET_TABLE_DETAIL should have fired — no column, lineage, or
    // quality calls.
    const documents = client.calls.map((c) => c.document);
    expect(documents).toEqual([GET_TABLE_DETAIL]);
  });
});
