---
name: Bug report
about: Something's broken — help us reproduce it
title: "[Bug] "
labels: bug
---

## What happened?

<!-- A clear and concise description of the bug. Include exact error messages if any. -->

## Steps to reproduce

1.
2.
3.

## Expected vs actual

**Expected:**

**Actual:**

## Environment

- `coalesce-catalog-mcp` version:
- Node.js version (`node --version`):
- MCP client (Claude Code / Cursor / Cortex Code / VS Code / Windsurf / …):
- Catalog region (`COALESCE_CATALOG_REGION`): `eu` | `us`
- Read-only mode on (`COALESCE_CATALOG_READ_ONLY=true`)? yes/no
- OS:

## Relevant tool call (if applicable)

<!-- Which tool? What arguments? Redact any UUIDs or descriptions you consider sensitive. -->

```
catalog_xxx with arguments { ... }
```

## Error output

<!-- Paste the `isError: true` payload (it's usually structured) or the stderr output from the server. -->

```
```

## Additional context

<!-- Anything else that might help: API response dumps (redacted), related tools that worked, etc. -->
