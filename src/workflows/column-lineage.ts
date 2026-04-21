import { z } from "zod";
import type { CatalogClient } from "../client.js";
import {
  READ_ONLY_ANNOTATIONS,
  type CatalogToolDefinition,
} from "../catalog/types.js";
import {
  GET_COLUMNS_SUMMARY,
  GET_DASHBOARDS_SUMMARY,
  GET_FIELD_LINEAGES,
  GET_TABLES_WITH_SCHEMA_CHAIN,
} from "../catalog/operations.js";
import type {
  FieldLineage,
  GetColumnsOutput,
  GetDashboardsOutput,
  GetFieldLineagesOutput,
  LineageType,
} from "../generated/types.js";
import { withErrorHandling } from "../mcp/tool-helpers.js";
import { resolveAssetByPath } from "./find-asset-by-path.js";
import { ENRICHMENT_BATCH_SIZE } from "./shared.js";

// ── Inputs ──────────────────────────────────────────────────────────────────

const LineageTypeSchema = z.enum([
  "AUTOMATIC",
  "MANUAL_CUSTOMER",
  "MANUAL_OPS",
  "OTHER_TECHNOS",
]) satisfies z.ZodType<LineageType>;

const DirectionSchema = z.enum(["upstream", "downstream", "both"]);

const ColumnLineageInputShape = {
  columnFQN: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Fully-qualified column path (DATABASE.SCHEMA.TABLE.COLUMN). Pass either columnFQN or columnId — the FQN is resolved internally via the same walk as catalog_find_asset_by_path."
    ),
  columnId: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Catalog UUID of the root column. Skip FQN resolution when you already have an id (e.g. from catalog_search_columns)."
    ),
  caseSensitive: z
    .boolean()
    .optional()
    .describe(
      "Case-sensitive FQN matching. Default: false (matches typical Snowflake/Postgres behavior)."
    ),
  direction: DirectionSchema.optional().describe(
    "Which side of the graph to walk. Default: both."
  ),
  includeDashboardFields: z
    .boolean()
    .optional()
    .describe(
      "Include column → dashboard-field edges. Dashboard-field names are unavailable via the public API — the node surfaces as { assetType: DASHBOARD_FIELD, hydrationUnavailable: true } plus its parent dashboard name when resolvable. Default: true."
    ),
  lineageTypes: z
    .array(LineageTypeSchema)
    .optional()
    .describe(
      "Filter edges by provenance (AUTOMATIC / MANUAL_CUSTOMER / MANUAL_OPS / OTHER_TECHNOS). Applied post-fetch, since the scope filter only supports a single value. Default: all types."
    ),
  maxNodes: z
    .number()
    .int()
    .min(100)
    .max(50000)
    .optional()
    .describe(
      "Safety ceiling for total reached nodes. The walk has no depth cap — it runs until exhausted. This limit exists only to protect the agent's context window against pathological graphs; hitting it throws an actionable error. Default: 10000."
    ),
};

// ── Types ───────────────────────────────────────────────────────────────────

type Direction = "upstream" | "downstream" | "both";
type AssetType = "COLUMN" | "DASHBOARD_FIELD";

interface ReachedNode {
  assetType: AssetType;
  id: string;
  depth: number;
  dashboardId?: string;
}

interface LineageEdge {
  id: string;
  lineageType: LineageType | null;
  parentId: string;
  parentType: AssetType;
  childId: string;
  childType: AssetType;
}

interface LineageSide {
  nodeIds: Map<string, ReachedNode>;
  edges: LineageEdge[];
}

// ── BFS constants ───────────────────────────────────────────────────────────

const FIELD_LINEAGE_PAGE_SIZE = 500;
// Hard ceiling per frontier-node page walk — mirrors assess-impact's guard
// against a server that never decrements totalCount. 40 pages * 500 rows =
// 20,000 edges off a single column. Throws rather than silently truncating.
const FIELD_LINEAGE_PAGES_PER_NODE_MAX = 40;
const FANOUT_PARALLELISM = 20;
const DEFAULT_MAX_NODES = 10000;

// ── BFS ─────────────────────────────────────────────────────────────────────

async function fetchAllColumnEdges(
  client: CatalogClient,
  columnId: string,
  direction: "upstream" | "downstream"
): Promise<FieldLineage[]> {
  // Only COLUMN nodes enter the BFS frontier — DASHBOARD_FIELD endpoints are
  // always BI leaves, so we only ever scope by parent/childColumnId.
  const scope =
    direction === "downstream"
      ? { parentColumnId: columnId }
      : { childColumnId: columnId };

  const all: FieldLineage[] = [];
  for (let page = 0; page < FIELD_LINEAGE_PAGES_PER_NODE_MAX; page++) {
    const resp = await client.execute<{ getFieldLineages: GetFieldLineagesOutput }>(
      GET_FIELD_LINEAGES,
      {
        scope,
        pagination: { nbPerPage: FIELD_LINEAGE_PAGE_SIZE, page },
      }
    );
    const rows = resp.getFieldLineages.data;
    for (const r of rows) all.push(r);
    if (rows.length < FIELD_LINEAGE_PAGE_SIZE) return all;
  }
  throw new Error(
    `Field-lineage pagination exceeded ${FIELD_LINEAGE_PAGES_PER_NODE_MAX} pages for ` +
      `column ${columnId} (${direction}, >${FIELD_LINEAGE_PAGES_PER_NODE_MAX * FIELD_LINEAGE_PAGE_SIZE} edges). ` +
      `Refusing to produce a partial lineage report.`
  );
}

function otherEndpoint(
  edge: FieldLineage,
  selfId: string
): { id: string; type: AssetType; dashboardId?: string } | null {
  const parentId = edge.parentColumnId ?? edge.parentDashboardFieldId ?? null;
  const parentType: AssetType | null = edge.parentColumnId
    ? "COLUMN"
    : edge.parentDashboardFieldId
      ? "DASHBOARD_FIELD"
      : null;
  const childId = edge.childColumnId ?? edge.childDashboardFieldId ?? null;
  const childType: AssetType | null = edge.childColumnId
    ? "COLUMN"
    : edge.childDashboardFieldId
      ? "DASHBOARD_FIELD"
      : null;

  if (parentId === selfId && childId && childType) {
    return {
      id: childId,
      type: childType,
      dashboardId:
        childType === "DASHBOARD_FIELD" ? edge.childDashboardId ?? undefined : undefined,
    };
  }
  if (childId === selfId && parentId && parentType) {
    return { id: parentId, type: parentType };
  }
  return null;
}

function edgeMatchesFilter(
  edge: FieldLineage,
  lineageTypes: LineageType[] | undefined,
  includeDashboardFields: boolean
): boolean {
  if (
    !includeDashboardFields &&
    (edge.parentDashboardFieldId || edge.childDashboardFieldId)
  ) {
    return false;
  }
  if (lineageTypes && lineageTypes.length > 0) {
    if (!edge.lineageType || !lineageTypes.includes(edge.lineageType)) {
      return false;
    }
  }
  return true;
}

class NodeBudget {
  private reached = 0;
  constructor(private readonly max: number) {}
  consume(n: number): void {
    this.reached += n;
    if (this.reached > this.max) {
      throw new Error(
        `Column lineage graph exceeded maxNodes=${this.max} (reached ${this.reached} before the cap fired). ` +
          `The walk has no depth cap, so the traversal explores every reachable node; raise maxNodes ` +
          `(up to 50000) if this graph is intentionally large, or scope the request with lineageTypes / ` +
          `direction to narrow it.`
      );
    }
  }
}

async function walkDirection(
  client: CatalogClient,
  rootColumnId: string,
  direction: "upstream" | "downstream",
  lineageTypes: LineageType[] | undefined,
  includeDashboardFields: boolean,
  budget: NodeBudget
): Promise<LineageSide> {
  const nodeIds = new Map<string, ReachedNode>();
  nodeIds.set(rootColumnId, {
    assetType: "COLUMN",
    id: rootColumnId,
    depth: 0,
  });
  const edges: LineageEdge[] = [];
  const seenEdgeIds = new Set<string>();

  let frontier: string[] = [rootColumnId];
  let depth = 0;
  while (frontier.length > 0) {
    depth++;

    // Fetch all edges for every node in this frontier, bounded by fan-out
    // parallelism so we don't open hundreds of sockets at once.
    const edgesPerNode: FieldLineage[][] = new Array(frontier.length);
    for (let i = 0; i < frontier.length; i += FANOUT_PARALLELISM) {
      const slice = frontier.slice(i, i + FANOUT_PARALLELISM);
      const sliceEdges = await Promise.all(
        slice.map((id) => fetchAllColumnEdges(client, id, direction))
      );
      for (let j = 0; j < sliceEdges.length; j++) {
        edgesPerNode[i + j] = sliceEdges[j];
      }
    }

    const nextFrontier: string[] = [];
    for (let i = 0; i < frontier.length; i++) {
      const nodeId = frontier[i];
      for (const edge of edgesPerNode[i]) {
        if (!edgeMatchesFilter(edge, lineageTypes, includeDashboardFields)) continue;
        const other = otherEndpoint(edge, nodeId);
        if (!other) continue;

        // Dedupe edges — the same edge can surface twice when a cycle pulls
        // BFS back through it from the opposite direction.
        if (!seenEdgeIds.has(edge.id)) {
          seenEdgeIds.add(edge.id);
          const parentId = edge.parentColumnId ?? edge.parentDashboardFieldId;
          const childId = edge.childColumnId ?? edge.childDashboardFieldId;
          // `otherEndpoint` already guaranteed both endpoints exist and match
          // their types, so these are non-null in practice. Assert to keep
          // the output shape tight rather than propagating empty-string
          // fallbacks.
          if (!parentId || !childId) continue;
          edges.push({
            id: edge.id,
            lineageType: edge.lineageType ?? null,
            parentId,
            parentType: edge.parentColumnId ? "COLUMN" : "DASHBOARD_FIELD",
            childId,
            childType: edge.childColumnId ? "COLUMN" : "DASHBOARD_FIELD",
          });
        }

        if (!nodeIds.has(other.id)) {
          nodeIds.set(other.id, {
            assetType: other.type,
            id: other.id,
            depth,
            dashboardId: other.dashboardId,
          });
          budget.consume(1);
          // Only expand COLUMN nodes further. DASHBOARD_FIELD endpoints are
          // BI leaves — chaining through them produces nothing useful for a
          // data-lineage trace, and the public API has no nice handle on them.
          if (other.type === "COLUMN") {
            nextFrontier.push(other.id);
          }
        } else if (other.type === "DASHBOARD_FIELD" && other.dashboardId) {
          // Upgrade a previously-recorded dashboard-field entry with its
          // dashboardId if we only now learned it via this edge.
          const prev = nodeIds.get(other.id)!;
          if (!prev.dashboardId) {
            nodeIds.set(other.id, { ...prev, dashboardId: other.dashboardId });
          }
        }
      }
    }

    if (nextFrontier.length === 0) break;
    frontier = nextFrontier;
  }

  return { nodeIds, edges };
}

// ── Name / FQN resolution ───────────────────────────────────────────────────

// Narrow projections of the generated types — every field below is
// non-nullable in the GraphQL schema (see src/generated/types.ts), so we
// don't shadow them with a looser nullable shape.
interface ColumnRow {
  id: string;
  name: string;
  tableId: string;
}

// getTables with the nested schema { database } relation chain returns this
// shape per row. Only `getTables` accepts the chain — getColumns rejects it,
// and GetSchemasScope / GetDatabasesScope have no `ids` filter, so we can't
// batch-look-up those two entities by UUID separately.
interface TableWithAncestors {
  id: string;
  name: string;
  schemaName: string;
  databaseName: string;
}

// Shape of a single row in GET_TABLES_WITH_SCHEMA_CHAIN. Declared locally
// (rather than casting through the generated `Table` type) so the
// non-nullable schema → database chain is expressed in the type system and
// a future contract-drift in the server response fails loudly instead of
// silently coercing to an empty string.
interface TableWithChainRow {
  id: string;
  name: string;
  schemaId: string;
  schema: {
    id: string;
    name: string;
    databaseId: string;
    database: { id: string; name: string };
  };
}

interface GetTablesWithChainOutput {
  totalCount: number;
  nbPerPage: number;
  page: number | null;
  data: TableWithChainRow[];
}

interface DashboardRow {
  id: string;
  name: string;
}

interface ResolvedColumn {
  name: string;
  /** null when any upstream ancestor (table / schema / database) lookup missed. */
  fqn: string | null;
  tableName: string | null;
  schemaName: string | null;
  databaseName: string | null;
}

async function batchFetch<TRow>(
  ids: string[],
  batchSize: number,
  fetchBatch: (batch: string[]) => Promise<TRow[]>
): Promise<TRow[]> {
  const out: TRow[] = [];
  for (let i = 0; i < ids.length; i += batchSize) {
    const slice = ids.slice(i, i + batchSize);
    const rows = await fetchBatch(slice);
    out.push(...rows);
  }
  return out;
}

async function resolveColumnMetadata(
  client: CatalogClient,
  columnIds: string[]
): Promise<Map<string, ResolvedColumn>> {
  const result = new Map<string, ResolvedColumn>();
  if (columnIds.length === 0) return result;

  const columnRows = await batchFetch<ColumnRow>(columnIds, ENRICHMENT_BATCH_SIZE, async (batch) => {
    const r = await client.execute<{ getColumns: GetColumnsOutput }>(
      GET_COLUMNS_SUMMARY,
      {
        scope: { ids: batch },
        pagination: { nbPerPage: batch.length, page: 0 },
      }
    );
    return r.getColumns.data.map((c) => ({
      id: c.id,
      name: c.name,
      tableId: c.tableId,
    }));
  });

  // Batch the table lookup with the nested schema { database } chain — one
  // query returns everything we need to build the FQN. Earlier draft tried
  // getSchemas + getDatabases with an `ids` filter, but neither scope
  // accepts `ids`; only getTables does, and only getTables allows the
  // relation chain. See GET_TABLES_WITH_SCHEMA_CHAIN's comment.
  const tableIds = [...new Set(columnRows.map((c) => c.tableId))];
  const tableRows = await batchFetch<TableWithAncestors>(
    tableIds,
    ENRICHMENT_BATCH_SIZE,
    async (batch) => {
      const r = await client.execute<{ getTables: GetTablesWithChainOutput }>(
        GET_TABLES_WITH_SCHEMA_CHAIN,
        {
          scope: { ids: batch },
          pagination: { nbPerPage: batch.length, page: 0 },
        }
      );
      return r.getTables.data.map((t) => ({
        id: t.id,
        name: t.name,
        schemaName: t.schema.name,
        databaseName: t.schema.database.name,
      }));
    }
  );
  const tableById = new Map(tableRows.map((t) => [t.id, t]));

  for (const col of columnRows) {
    const table = tableById.get(col.tableId);
    const fqn =
      table && table.schemaName && table.databaseName
        ? `${table.databaseName}.${table.schemaName}.${table.name}.${col.name}`
        : null;
    result.set(col.id, {
      name: col.name,
      fqn,
      tableName: table?.name ?? null,
      schemaName: table?.schemaName || null,
      databaseName: table?.databaseName || null,
    });
  }
  return result;
}

async function resolveDashboardNames(
  client: CatalogClient,
  dashboardIds: string[]
): Promise<Map<string, DashboardRow>> {
  const result = new Map<string, DashboardRow>();
  if (dashboardIds.length === 0) return result;
  const rows = await batchFetch<DashboardRow>(dashboardIds, ENRICHMENT_BATCH_SIZE, async (batch) => {
    const r = await client.execute<{ getDashboards: GetDashboardsOutput }>(
      GET_DASHBOARDS_SUMMARY,
      {
        scope: { ids: batch },
        pagination: { nbPerPage: batch.length, page: 0 },
      }
    );
    return r.getDashboards.data.map((d) => ({
      id: d.id,
      name: d.name,
    }));
  });
  for (const row of rows) result.set(row.id, row);
  return result;
}

// ── Output shaping ──────────────────────────────────────────────────────────

interface EnrichedLineageNode {
  assetType: AssetType;
  id: string;
  depth: number;
  name: string | null;
  fqn?: string | null;
  tableName?: string | null;
  schemaName?: string | null;
  databaseName?: string | null;
  dashboardId?: string | null;
  dashboardName?: string | null;
  hydrationUnavailable?: boolean;
}

function enrichNodes(
  side: LineageSide,
  columnMeta: Map<string, ResolvedColumn>,
  dashboardById: Map<string, DashboardRow>
): EnrichedLineageNode[] {
  const out: EnrichedLineageNode[] = [];
  for (const node of side.nodeIds.values()) {
    if (node.depth === 0) continue;
    if (node.assetType === "COLUMN") {
      const meta = columnMeta.get(node.id);
      out.push({
        assetType: "COLUMN",
        id: node.id,
        depth: node.depth,
        name: meta?.name ?? null,
        fqn: meta?.fqn ?? null,
        tableName: meta?.tableName ?? null,
        schemaName: meta?.schemaName ?? null,
        databaseName: meta?.databaseName ?? null,
      });
    } else {
      const dash = node.dashboardId ? dashboardById.get(node.dashboardId) : undefined;
      out.push({
        assetType: "DASHBOARD_FIELD",
        id: node.id,
        depth: node.depth,
        name: null,
        dashboardId: node.dashboardId ?? null,
        dashboardName: dash?.name ?? null,
        hydrationUnavailable: true,
      });
    }
  }
  out.sort((a, b) => a.depth - b.depth || (a.fqn ?? a.id).localeCompare(b.fqn ?? b.id));
  return out;
}

// ── Tool definition ─────────────────────────────────────────────────────────

export function defineColumnLineage(
  client: CatalogClient
): CatalogToolDefinition {
  return {
    name: "catalog_get_column_lineage",
    config: {
      title: "Get Column Lineage (Complete Graph)",
      description:
        "Return the complete column-level lineage graph for a starting column — every upstream column that feeds it and every downstream column it feeds (plus dashboard-field endpoints by default). Accepts either a fully-qualified column path (DATABASE.SCHEMA.TABLE.COLUMN) or a column UUID.\n\n" +
        "Why use this over catalog_get_field_lineages directly: that tool is a single-hop, UUID-scoped primitive. This workflow resolves the FQN to a UUID, walks the BFS exhaustively (no depth cap — it runs until the graph is fully covered), and batch-resolves every reached column id to `{ name, fqn, tableName, schemaName, databaseName }` so the output doesn't require a round trip to interpret.\n\n" +
        "Graph shape: returns the reached nodes as a flat array per direction plus the edge list between them (DAG-safe; handles cycles and shared children naturally). Node map includes: assetType (COLUMN | DASHBOARD_FIELD), depth (shortest distance from root), resolved name + FQN (for columns) or parent dashboard name (for dashboard-fields). Dashboard-field names are not exposed by the Catalog Public API, so those nodes surface with `hydrationUnavailable: true`.\n\n" +
        "Dashboard-field caveat: only **downstream** edges carry a `childDashboardId`, so `dashboardName` can be resolved there. Upstream dashboard-field endpoints (a dashboard-field as a column's *parent*) have no `parentDashboardId` in the underlying schema — they come back with `hydrationUnavailable: true` and `dashboardId: null`.\n\n" +
        "Safety: the walk has no depth cap. It has a configurable total-node ceiling (default 10000) to protect the agent's context from pathological graphs — if a graph exceeds it, the tool throws rather than silently truncating.",
      inputSchema: ColumnLineageInputShape,
      annotations: READ_ONLY_ANNOTATIONS,
    },
    handler: withErrorHandling(async (args, c) => {
      const columnFQN = args.columnFQN as string | undefined;
      const columnIdArg = args.columnId as string | undefined;
      const caseSensitive = args.caseSensitive === true;
      const direction = (args.direction as Direction | undefined) ?? "both";
      const includeDashboardFields =
        (args.includeDashboardFields as boolean | undefined) ?? true;
      const lineageTypes = args.lineageTypes as LineageType[] | undefined;
      const maxNodes = (args.maxNodes as number | undefined) ?? DEFAULT_MAX_NODES;

      if (!columnFQN && !columnIdArg) {
        return {
          error:
            "Pass either `columnFQN` (e.g. DATABASE.SCHEMA.TABLE.COLUMN) or `columnId` (Catalog UUID).",
        };
      }

      let rootColumnId = columnIdArg;
      let rootFqnFromResolver: string | null = null;
      if (columnFQN) {
        const resolved = await resolveAssetByPath(c, columnFQN, caseSensitive);
        if ("notFound" in resolved) {
          return { error: "column_not_found", detail: resolved };
        }
        if ("ambiguous" in resolved) {
          return { error: "column_ambiguous", detail: resolved };
        }
        if (resolved.resolved.kind !== "COLUMN") {
          return {
            error: "not_a_column",
            detail:
              "The supplied FQN resolved to a TABLE. Pass a 4-part path (DATABASE.SCHEMA.TABLE.COLUMN) to target a column.",
            resolved: resolved.resolved,
          };
        }
        rootColumnId = resolved.resolved.id;
        rootFqnFromResolver = resolved.resolved.fullPath;
      }

      if (!rootColumnId) {
        return { error: "Column id could not be resolved." };
      }

      const budget = new NodeBudget(maxNodes);
      const wantUpstream = direction === "upstream" || direction === "both";
      const wantDownstream = direction === "downstream" || direction === "both";

      const [upstream, downstream] = await Promise.all([
        wantUpstream
          ? walkDirection(c, rootColumnId, "upstream", lineageTypes, includeDashboardFields, budget)
          : Promise.resolve(null),
        wantDownstream
          ? walkDirection(c, rootColumnId, "downstream", lineageTypes, includeDashboardFields, budget)
          : Promise.resolve(null),
      ]);

      // Gather all reached column ids + dashboard ids for a single-pass batch
      // resolution. Root goes in alongside downstream results so we only do
      // one round of column/table/schema/database lookups.
      const columnIdsToResolve = new Set<string>([rootColumnId]);
      const dashboardIds = new Set<string>();
      let dashboardFieldCount = 0;
      for (const side of [upstream, downstream]) {
        if (!side) continue;
        for (const n of side.nodeIds.values()) {
          if (n.depth === 0) continue;
          if (n.assetType === "COLUMN") {
            columnIdsToResolve.add(n.id);
          } else {
            dashboardFieldCount++;
            if (n.dashboardId) dashboardIds.add(n.dashboardId);
          }
        }
      }

      const [columnMeta, dashboardMeta] = await Promise.all([
        resolveColumnMetadata(c, [...columnIdsToResolve]),
        resolveDashboardNames(c, [...dashboardIds]),
      ]);

      const rootMeta = columnMeta.get(rootColumnId) ?? null;

      const output: Record<string, unknown> = {
        root: {
          columnId: rootColumnId,
          fqn: rootFqnFromResolver ?? rootMeta?.fqn ?? null,
          name: rootMeta?.name ?? null,
          tableName: rootMeta?.tableName ?? null,
          schemaName: rootMeta?.schemaName ?? null,
          databaseName: rootMeta?.databaseName ?? null,
        },
        direction,
        filters: {
          includeDashboardFields,
          lineageTypes: lineageTypes ?? null,
        },
      };

      if (upstream) {
        const enrichedNodes = enrichNodes(upstream, columnMeta, dashboardMeta);
        output.upstream = {
          nodeCount: enrichedNodes.length,
          edgeCount: upstream.edges.length,
          nodes: enrichedNodes,
          edges: upstream.edges,
        };
      }
      if (downstream) {
        const enrichedNodes = enrichNodes(downstream, columnMeta, dashboardMeta);
        output.downstream = {
          nodeCount: enrichedNodes.length,
          edgeCount: downstream.edges.length,
          nodes: enrichedNodes,
          edges: downstream.edges,
        };
      }

      output.stats = {
        totalColumnsReached: columnIdsToResolve.size - 1,
        totalDashboardFieldsReached: dashboardFieldCount,
        maxNodes,
      };

      return output;
    }, client),
  };
}
