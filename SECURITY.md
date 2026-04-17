# Security Policy

## Reporting a Vulnerability

If you've discovered a security issue in `coalesce-catalog-mcp`, **please do not open a public GitHub issue**. Instead, report it privately so we can coordinate a fix before public disclosure.

Report via one of:

- **GitHub Security Advisory** — https://github.com/jessemarshall/coalesce-catalog-mcp/security/advisories/new (preferred)
- **Email** — jesse.marshall@coalesce.io with subject `[SECURITY] coalesce-catalog-mcp`

Include as much detail as you can:

- A description of the issue and its impact
- Reproduction steps or a proof-of-concept
- The version(s) of `coalesce-catalog-mcp` affected
- Any mitigations or workarounds you've identified

We aim to acknowledge reports within 2 business days and provide a status update within 10 business days.

## Scope

In scope:

- The MCP server code in this repository (`src/`, published as `coalesce-catalog-mcp` on npm)
- Bundled GraphQL operation documents (`src/catalog/operations.ts`) and generated schema types (`src/generated/`)
- Credential handling (`src/services/config/credentials.ts`)
- Pre-commit hook behaviour (`.husky/`)

Out of scope — report to the relevant upstream project:

- Vulnerabilities in the Coalesce Catalog (Castor) API itself — report to Coalesce/Castor directly
- Vulnerabilities in the `@modelcontextprotocol/sdk`, `zod`, `graphql`, or `@graphql-codegen/*` packages — report to their respective maintainers
- Misconfiguration in a user's MCP client setup (e.g. an over-privileged API token committed to their own repo)

## Handling Credentials

`coalesce-catalog-mcp` reads a Castor API token from `COALESCE_CATALOG_API_KEY`. The token is used as a `Authorization: Token <value>` header on requests to the Catalog GraphQL endpoint. It is never logged, cached, or written to disk by this server.

If your token has been leaked publicly (committed to a repo, pasted into a chat, etc.), rotate it immediately at Catalog → Settings → API tokens.

## Pre-Commit Secret Scanning

A `.husky/pre-commit` hook scans the staged diff for common credential patterns (JWT, `sk-`, `ghp_`, `npm_`, `AKIA*`, `castor::*`) before allowing a commit. The scanner is best-effort, not authoritative — review your own diffs before pushing, and consider tooling like [gitleaks](https://github.com/gitleaks/gitleaks) or [trufflehog](https://github.com/trufflesecurity/trufflehog) for defence in depth.
