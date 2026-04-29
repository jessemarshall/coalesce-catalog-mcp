/**
 * Iterative Levenshtein edit distance with ping-pong row buffers
 * (memory O(min(a.length, b.length)), time O(a.length * b.length)).
 *
 * Shared between catalog_describe_type's near-match suggestions
 * (src/mcp/introspection.ts) and catalog_audit_tag_hygiene's
 * near-duplicate tag detection (src/workflows/audit-tag-hygiene.ts).
 * Comparison is case-sensitive — callers fold beforehand if needed.
 */
export function editDistance(a: string, b: string): number {
  if (a === b) return 0;
  if (a.length === 0) return b.length;
  if (b.length === 0) return a.length;

  // Ensure b is the shorter string so the row buffers are sized to min(la, lb).
  // Edit distance is symmetric, so swapping the operands does not change the result.
  if (a.length < b.length) [a, b] = [b, a];
  const la = a.length;
  const lb = b.length;

  let prev = Array.from({ length: lb + 1 }, (_, i) => i);
  let curr = new Array<number>(lb + 1);

  for (let i = 1; i <= la; i++) {
    curr[0] = i;
    for (let j = 1; j <= lb; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      curr[j] = Math.min(
        prev[j] + 1,
        curr[j - 1] + 1,
        prev[j - 1] + cost
      );
    }
    [prev, curr] = [curr, prev];
  }
  return prev[lb];
}
