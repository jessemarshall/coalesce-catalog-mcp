#!/usr/bin/env bash
# Pre-ship smoke tests for the coalesce-catalog-mcp mutation surface.
#
# Runs 10 mutations across 4 self-cleaning loops against the live Catalog API:
#   1. Terms CRUD     (create → update → delete)
#   2. Tag attach/detach on the orders test table
#   3. External links CRUD on the orders test table
#   4. Lineage upsert/delete between two existing tables
#
# Fixtures use the `zz_mcp_smoke_test_DELETEME` naming convention so any
# residue from a failed run is easy to find and clean up.
#
# REQUIRES: COALESCE_CATALOG_API_KEY with READ_WRITE scope.
# Set SMOKE_CONFIRM=1 to run — the guard prevents accidental execution.
#
# Coverage:
#   - create_term, update_term, delete_term
#   - attach_tags, detach_tags
#   - create_external_links, update_external_links, delete_external_links
#   - upsert_lineages, delete_lineages
#
# NOT covered here (require a nominated test asset, not self-cleaning):
#   - update_table_metadata, update_column_metadata
#   - upsert_data_qualities, remove_data_qualities
#   - upsert_user_owners, remove_user_owners
#   - upsert_team_owners, remove_team_owners
#   - upsert_team, add_team_users, remove_team_users
#   - upsert_pinned_assets, remove_pinned_assets

set -euo pipefail

if [ "${SMOKE_CONFIRM:-}" != "1" ]; then
  echo "Refusing to run — this script modifies live Catalog data." >&2
  echo "Re-run with SMOKE_CONFIRM=1 $0" >&2
  exit 2
fi
if [ -z "${COALESCE_CATALOG_API_KEY:-}" ]; then
  echo "COALESCE_CATALOG_API_KEY is required." >&2
  exit 2
fi

cd "$(dirname "$0")/.."

# Known EU test fixtures — the orders table is the canonical smoke target.
ORDERS_ID="844b1c67-cbbf-4815-a65f-41166b1f0bea"
STG_ORDERS_ID="0007da0b-3609-4442-af92-58e1a3aa01cf"
TMP="$(mktemp -t mcp-smoke.XXXXXX)"
trap 'rm -f "$TMP"' EXIT

# One-shot MCP tool call via stdio. Writes last JSON-RPC line to $TMP.
call() {
  local name="$1"; local args="$2"
  {
    echo '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0.0.0"}}}'
    echo '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'
    printf '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"%s","arguments":%s}}\n' "$name" "$args"
  } | node dist/index.js 2>/dev/null | tail -1 > "$TMP"
  if [ "$(jq -r '.result.isError // false' "$TMP")" = "true" ]; then
    echo "  ❌ ERROR:"
    jq -r '.result.content[0].text' "$TMP" | head -10 | sed 's/^/    /'
    return 1
  fi
  jq -r '.result.content[0].text' "$TMP"
}

PASS=0; FAIL=0
record() {
  if [ "$1" = "ok" ]; then PASS=$((PASS+1)); echo "  ✅ $2"
  else FAIL=$((FAIL+1)); echo "  ❌ $2"
  fi
}

echo "=== 1/4: Terms CRUD ==="
if RES=$(call catalog_create_term '{"name":"zz_mcp_smoke_test_DELETEME","description":"Ephemeral smoke test."}'); then
  TID=$(echo "$RES" | jq -r '.term.id')
  record ok "create_term id=$TID"
  if call catalog_update_term "{\"id\":\"$TID\",\"description\":\"Updated.\"}" > /dev/null; then
    record ok "update_term"
  else record fail "update_term"; fi
  if call catalog_delete_term "{\"id\":\"$TID\"}" > /dev/null; then
    record ok "delete_term"
  else record fail "delete_term (leaked id=$TID)"; fi
else record fail "create_term"; fi

echo ""
echo "=== 2/4: Tag attach/detach on orders ==="
if call catalog_attach_tags "{\"data\":[{\"entityType\":\"TABLE\",\"entityId\":\"$ORDERS_ID\",\"label\":\"zz_mcp_smoke_test\"}]}" > /dev/null; then
  record ok "attach_tags (auto-creates label)"
  if call catalog_detach_tags "{\"data\":[{\"entityType\":\"TABLE\",\"entityId\":\"$ORDERS_ID\",\"label\":\"zz_mcp_smoke_test\"}]}" > /dev/null; then
    record ok "detach_tags"
  else record fail "detach_tags"; fi
else record fail "attach_tags"; fi

echo ""
echo "=== 3/4: External links CRUD on orders ==="
if RES=$(call catalog_create_external_links "{\"data\":[{\"tableId\":\"$ORDERS_ID\",\"technology\":\"GITHUB\",\"url\":\"https://example.invalid/zz-mcp-smoke\"}]}"); then
  LID=$(echo "$RES" | jq -r '.data[0].id')
  record ok "create_external_links id=$LID"
  if call catalog_update_external_links "{\"data\":[{\"id\":\"$LID\",\"url\":\"https://example.invalid/zz-mcp-smoke-UPDATED\"}]}" > /dev/null; then
    record ok "update_external_links"
  else record fail "update_external_links"; fi
  if call catalog_delete_external_links "{\"data\":[{\"id\":\"$LID\"}]}" > /dev/null; then
    record ok "delete_external_links"
  else record fail "delete_external_links (leaked id=$LID)"; fi
else record fail "create_external_links"; fi

echo ""
echo "=== 4/4: Lineage upsert/delete ==="
# Upsert an edge between two existing tables, then delete it. The two tables
# are not otherwise linked on the EU test account.
if call catalog_upsert_lineages "{\"data\":[{\"parentTableId\":\"$ORDERS_ID\",\"childTableId\":\"$STG_ORDERS_ID\"}]}" > /dev/null; then
  record ok "upsert_lineages"
  if call catalog_delete_lineages "{\"data\":[{\"parentTableId\":\"$ORDERS_ID\",\"childTableId\":\"$STG_ORDERS_ID\"}]}" > /dev/null; then
    record ok "delete_lineages"
  else record fail "delete_lineages (leaked edge)"; fi
else record fail "upsert_lineages"; fi

echo ""
echo "======================================================="
echo "Smoke summary: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
