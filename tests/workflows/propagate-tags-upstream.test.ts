import { describe, it, expect } from "vitest";
import { definePropagateTagsUpstream } from "../../src/workflows/propagate-tags-upstream.js";
import {
  GET_TABLE_DETAIL,
  GET_DASHBOARD_DETAIL,
  GET_TABLES_DETAIL_BATCH,
  GET_LINEAGES,
  ATTACH_TAGS,
} from "../../src/catalog/operations.js";
import { makeMockClient } from "../helpers/mock-client.js";

function parseResult(r: {
  content: { text: string }[];
  isError?: boolean;
}): Record<string, unknown> {
  return JSON.parse(r.content[0].text) as Record<string, unknown>;
}

interface MockTagEntity {
  tag: { id: string; label: string };
}

interface MockTable {
  id: string;
  name?: string | null;
  tagEntities?: MockTagEntity[];
}

interface MockDashboard {
  id: string;
  name?: string | null;
  tagEntities?: MockTagEntity[];
}

interface RouterOpts {
  sourceTable?: MockTable | null;
  sourceDashboard?: MockDashboard | null;
  // child id -> parent table ids (upstream parents)
  upstreamByChildTable?: Map<string, string[]>;
  upstreamByChildDashboard?: Map<string, string[]>;
  tablesByIds?: Map<string, MockTable>;
  attachTagsResponse?: (data: unknown) => boolean;
}

function makeRouter(opts: RouterOpts) {
  return makeMockClient((document, variables) => {
    if (document === GET_TABLE_DETAIL) {
      return {
        getTables: { data: opts.sourceTable ? [opts.sourceTable] : [] },
      };
    }
    if (document === GET_DASHBOARD_DETAIL) {
      return {
        getDashboards: {
          data: opts.sourceDashboard ? [opts.sourceDashboard] : [],
        },
      };
    }
    if (document === GET_TABLES_DETAIL_BATCH) {
      const vars = variables as { scope?: { ids?: string[] } };
      const ids = vars.scope?.ids ?? [];
      const rows = ids
        .map((id) => opts.tablesByIds?.get(id))
        .filter((r): r is MockTable => !!r);
      return {
        getTables: {
          totalCount: rows.length,
          nbPerPage: ids.length || 1,
          page: 0,
          data: rows,
        },
      };
    }
    if (document === GET_LINEAGES) {
      const vars = variables as {
        scope?: { childTableId?: string; childDashboardId?: string };
        pagination: { nbPerPage: number; page: number };
      };
      const childTable = vars.scope?.childTableId;
      const childDash = vars.scope?.childDashboardId;
      let parents: string[] = [];
      if (childTable) {
        parents = opts.upstreamByChildTable?.get(childTable) ?? [];
      } else if (childDash) {
        parents = opts.upstreamByChildDashboard?.get(childDash) ?? [];
      }
      const data =
        vars.pagination.page === 0
          ? parents.map((parentTableId) => ({
              parentTableId,
              ...(childTable
                ? { childTableId: childTable }
                : { childDashboardId: childDash }),
            }))
          : [];
      return {
        getLineages: {
          totalCount: parents.length,
          nbPerPage: vars.pagination.nbPerPage,
          page: vars.pagination.page,
          data,
        },
      };
    }
    if (document === ATTACH_TAGS) {
      const success = opts.attachTagsResponse
        ? opts.attachTagsResponse(variables)
        : true;
      return { attachTags: success };
    }
    throw new Error(`unexpected document: ${document.slice(0, 60)}`);
  });
}

describe("catalog_propagate_tags_upstream — source not found", () => {
  it("returns notFound for a missing TABLE source", async () => {
    const client = makeRouter({ sourceTable: null });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "missing",
        sourceAssetType: "TABLE",
      })
    );
    expect(out).toEqual({
      notFound: true,
      sourceAssetId: "missing",
      sourceAssetType: "TABLE",
    });
  });

  it("returns notFound for a missing DASHBOARD source", async () => {
    const client = makeRouter({ sourceDashboard: null });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "missing",
        sourceAssetType: "DASHBOARD",
      })
    );
    expect(out.notFound).toBe(true);
  });
});

describe("catalog_propagate_tags_upstream — dry-run plan from a dashboard source", () => {
  it("emits an add plan against immediate upstream tables and never mutates", async () => {
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "Sales Critical",
        tagEntities: [
          { tag: { id: "tg-1", label: "Critical" } },
          { tag: { id: "tg-2", label: "Production" } },
        ],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-up1", "t-up2"]]]),
      tablesByIds: new Map([
        ["t-up1", { id: "t-up1", name: "FACT_ORDERS", tagEntities: [] }],
        ["t-up2", { id: "t-up2", name: "DIM_CUSTOMER", tagEntities: [] }],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "dash-1",
        sourceAssetType: "DASHBOARD",
      })
    );
    const plan = out.plan as Array<Record<string, unknown>>;
    expect(plan).toHaveLength(2);
    for (const row of plan) {
      const tags = row.changes as { tags: Record<string, unknown> };
      expect(tags.tags.action).toBe("add");
      expect(tags.tags.added).toEqual(["Critical", "Production"]);
      const provenance = row.provenance as Record<string, unknown>;
      expect(provenance.sourceAssetType).toBe("DASHBOARD");
      expect(provenance.sourceAssetName).toBe("Sales Critical");
    }
    // No ATTACH_TAGS call in dry-run.
    expect(client.calls.map((c) => c.document)).not.toContain(ATTACH_TAGS);
  });

  it("filters source tags via tagLabels (case-insensitive)", async () => {
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "D",
        tagEntities: [
          { tag: { id: "tg-1", label: "Critical" } },
          { tag: { id: "tg-2", label: "tableau-internal" } },
        ],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-up1"]]]),
      tablesByIds: new Map([
        ["t-up1", { id: "t-up1", name: "T1", tagEntities: [] }],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "dash-1",
        sourceAssetType: "DASHBOARD",
        tagLabels: ["critical"], // case-insensitive
      })
    );
    const source = out.source as Record<string, unknown>;
    expect(source.effectiveTags).toEqual(["Critical"]);
    const plan = out.plan as Array<Record<string, unknown>>;
    const tags = plan[0].changes as { tags: Record<string, unknown> };
    expect(tags.tags.added).toEqual(["Critical"]);
  });
});

describe("catalog_propagate_tags_upstream — overwritePolicy", () => {
  it("ifEmpty (default) skips upstream tables that already have any tags", async () => {
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "D",
        tagEntities: [{ tag: { id: "tg-1", label: "Critical" } }],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-up1"]]]),
      tablesByIds: new Map([
        [
          "t-up1",
          {
            id: "t-up1",
            name: "T1",
            tagEntities: [{ tag: { id: "tg-x", label: "ad-hoc" } }],
          },
        ],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "dash-1",
        sourceAssetType: "DASHBOARD",
      })
    );
    const plan = out.plan as Array<Record<string, unknown>>;
    const tags = plan[0].changes as { tags: Record<string, unknown> };
    expect(tags.tags.action).toBe("skip");
    expect(tags.tags.reason).toMatch(/overwritePolicy=ifEmpty/);
  });

  it("overwrite additively merges — adds only the missing source tags", async () => {
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "D",
        tagEntities: [
          { tag: { id: "tg-1", label: "Critical" } },
          { tag: { id: "tg-2", label: "ad-hoc" } },
        ],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-up1"]]]),
      tablesByIds: new Map([
        [
          "t-up1",
          {
            id: "t-up1",
            name: "T1",
            tagEntities: [{ tag: { id: "tg-x", label: "ad-hoc" } }],
          },
        ],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "dash-1",
        sourceAssetType: "DASHBOARD",
        overwritePolicy: "overwrite",
      })
    );
    const plan = out.plan as Array<Record<string, unknown>>;
    const tags = plan[0].changes as { tags: Record<string, unknown> };
    expect(tags.tags.action).toBe("add");
    expect(tags.tags.added).toEqual(["Critical"]);
    expect(tags.tags.alreadyPresent).toEqual(["ad-hoc"]);
  });
});

describe("catalog_propagate_tags_upstream — multi-hop traversal", () => {
  it("traverses to depth=2 and records pathFromSource", async () => {
    // dash-1 -> t-mid -> t-root
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "D",
        tagEntities: [{ tag: { id: "tg-1", label: "Critical" } }],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-mid"]]]),
      upstreamByChildTable: new Map([["t-mid", ["t-root"]]]),
      tablesByIds: new Map([
        ["t-mid", { id: "t-mid", name: "MID", tagEntities: [] }],
        ["t-root", { id: "t-root", name: "ROOT", tagEntities: [] }],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "dash-1",
        sourceAssetType: "DASHBOARD",
        maxDepth: 2,
      })
    );
    const plan = out.plan as Array<Record<string, unknown>>;
    expect(plan).toHaveLength(2);
    // Sorted by depth ASC: t-mid (depth 1) first, t-root (depth 2) second.
    expect(plan[0].tableId).toBe("t-mid");
    expect(plan[0].depth).toBe(1);
    expect(plan[1].tableId).toBe("t-root");
    expect(plan[1].depth).toBe(2);
    const prov = plan[1].provenance as Record<string, unknown>;
    const path = prov.pathFromSource as Array<Record<string, unknown>>;
    expect(path.map((p) => p.tableId)).toEqual(["t-mid", "t-root"]);
  });

  it("respects maxDepth and stops short of deeper parents", async () => {
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "D",
        tagEntities: [{ tag: { id: "tg-1", label: "Critical" } }],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-mid"]]]),
      upstreamByChildTable: new Map([["t-mid", ["t-root"]]]),
      tablesByIds: new Map([
        ["t-mid", { id: "t-mid", name: "MID", tagEntities: [] }],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "dash-1",
        sourceAssetType: "DASHBOARD",
        maxDepth: 1,
      })
    );
    const plan = out.plan as Array<Record<string, unknown>>;
    expect(plan).toHaveLength(1);
    expect(plan[0].tableId).toBe("t-mid");
  });
});

describe("catalog_propagate_tags_upstream — provenance gate", () => {
  it("refuses to execute when dryRun=false but acknowledgeProvenanceSemantics is missing", async () => {
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "D",
        tagEntities: [{ tag: { id: "tg-1", label: "Critical" } }],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-up1"]]]),
      tablesByIds: new Map([
        ["t-up1", { id: "t-up1", name: "T1", tagEntities: [] }],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const res = await tool.handler({
      sourceAssetId: "dash-1",
      sourceAssetType: "DASHBOARD",
      dryRun: false,
    });
    expect(res.isError).toBe(true);
    const msg = parseResult(res).error as string;
    expect(msg).toMatch(/acknowledgeProvenanceSemantics must be true/);
    // No mutations attempted.
    expect(client.calls.map((c) => c.document)).not.toContain(ATTACH_TAGS);
  });

  it("executes ATTACH_TAGS when dryRun=false AND acknowledgeProvenanceSemantics=true (skipping confirmation via env)", async () => {
    const prevSkip = process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = "true";
    try {
      const client = makeRouter({
        sourceDashboard: {
          id: "dash-1",
          name: "D",
          tagEntities: [{ tag: { id: "tg-1", label: "Critical" } }],
        },
        upstreamByChildDashboard: new Map([["dash-1", ["t-up1"]]]),
        tablesByIds: new Map([
          ["t-up1", { id: "t-up1", name: "T1", tagEntities: [] }],
        ]),
      });
      const tool = definePropagateTagsUpstream(client);
      const out = parseResult(
        await tool.handler({
          sourceAssetId: "dash-1",
          sourceAssetType: "DASHBOARD",
          dryRun: false,
          acknowledgeProvenanceSemantics: true,
        })
      );
      const exec = out.execution as Record<string, unknown>;
      expect(exec.planned).toBe(1);
      expect(exec.batchesAccepted).toBe(1);
      expect(exec.batchesRejected).toBe(0);
      // Confirm the ATTACH_TAGS call carries the right rows.
      const tagCall = client.calls.find((c) => c.document === ATTACH_TAGS);
      expect(tagCall).toBeTruthy();
      const data = (tagCall!.variables as { data: unknown[] }).data;
      expect(data).toEqual([
        { entityType: "TABLE", entityId: "t-up1", label: "Critical" },
      ]);
    } finally {
      if (prevSkip === undefined)
        delete process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
      else process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = prevSkip;
    }
  });

  it("reports partialFailure with failedAttachments when ATTACH_TAGS returns false", async () => {
    const prevSkip = process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
    process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = "true";
    try {
      const client = makeRouter({
        sourceDashboard: {
          id: "dash-1",
          name: "D",
          tagEntities: [{ tag: { id: "tg-1", label: "Critical" } }],
        },
        upstreamByChildDashboard: new Map([["dash-1", ["t-up1"]]]),
        tablesByIds: new Map([
          ["t-up1", { id: "t-up1", name: "T1", tagEntities: [] }],
        ]),
        attachTagsResponse: () => false,
      });
      const tool = definePropagateTagsUpstream(client);
      const out = parseResult(
        await tool.handler({
          sourceAssetId: "dash-1",
          sourceAssetType: "DASHBOARD",
          dryRun: false,
          acknowledgeProvenanceSemantics: true,
        })
      );
      const exec = out.execution as Record<string, unknown>;
      expect(exec.partialFailure).toBe(true);
      expect(exec.batchesRejected).toBe(1);
      expect((exec.failedAttachments as unknown[]).length).toBe(1);
    } finally {
      if (prevSkip === undefined)
        delete process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS;
      else process.env.COALESCE_CATALOG_SKIP_CONFIRMATIONS = prevSkip;
    }
  });
});

describe("catalog_propagate_tags_upstream — empty paths", () => {
  it("emits an empty plan when source has no tags", async () => {
    const client = makeRouter({
      sourceDashboard: {
        id: "dash-1",
        name: "D",
        tagEntities: [],
      },
      upstreamByChildDashboard: new Map([["dash-1", ["t-up1"]]]),
      tablesByIds: new Map([
        ["t-up1", { id: "t-up1", name: "T1", tagEntities: [] }],
      ]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "dash-1",
        sourceAssetType: "DASHBOARD",
      })
    );
    const plan = out.plan as Array<Record<string, unknown>>;
    const tags = plan[0].changes as { tags: Record<string, unknown> };
    expect(tags.tags.action).toBe("skip");
    expect(tags.tags.reason).toMatch(/no tags to propagate/);
  });

  it("emits an empty plan when source has no upstream parents", async () => {
    const client = makeRouter({
      sourceTable: {
        id: "t-orphan",
        name: "ORPHAN",
        tagEntities: [{ tag: { id: "tg-1", label: "Critical" } }],
      },
      upstreamByChildTable: new Map([["t-orphan", []]]),
    });
    const tool = definePropagateTagsUpstream(client);
    const out = parseResult(
      await tool.handler({
        sourceAssetId: "t-orphan",
        sourceAssetType: "TABLE",
      })
    );
    const traversal = out.traversal as Record<string, unknown>;
    expect(traversal.upstreamTablesReached).toBe(0);
    expect(out.plan).toEqual([]);
  });
});
