#!/usr/bin/env node
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { validateConfig, createClient } from "./client.js";
import { createCoalesceCatalogMcpServer } from "./server.js";

const config = validateConfig();
const client = createClient(config);
const server = createCoalesceCatalogMcpServer(client);

const transport = new StdioServerTransport();
await server.connect(transport);
