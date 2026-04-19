# Prerelease channel

Prereleases are cut from the `preview` branch and published to the `@preview` npm dist-tag while `@latest` stays on stable. Point `npx` at the preview channel:

```json
{
  "coalesce-catalog": {
    "command": "npx",
    "args": ["coalesce-catalog-mcp@preview"]
  }
}
```

Restart your MCP client after changing the config so `npx` re-resolves.

## Pinning an exact version

To pin an exact prerelease rather than whatever `@preview` resolves to today, replace `@preview` with the full version, e.g. `coalesce-catalog-mcp@0.3.0-preview.1`.

If `npx` serves a stale cached copy when `@preview` advances, force a fresh fetch with `npx -y coalesce-catalog-mcp@preview`.

## Running preview and stable side-by-side

Register both under different server names:

```json
{
  "mcpServers": {
    "coalesce-catalog": {
      "command": "npx",
      "args": ["coalesce-catalog-mcp"]
    },
    "coalesce-catalog-preview": {
      "command": "npx",
      "args": ["coalesce-catalog-mcp@preview"]
    }
  }
}
```

Agents will see `coalesce-catalog__*` and `coalesce-catalog-preview__*` tools as separate namespaces.
