# AI instructions

This file provides guidance to coding agents when working in this documentation package.

## Commands

```bash
# Validate docs configuration and content
mint validate
```

**Important**: Run `mint validate` after every change to ensure docs.json and page references are valid.

## Structure

This is a Mintlify documentation site for Chroma (the open-source AI application database).

- `docs.json` - Main configuration file defining navigation, theme, and site settings
- `docs/` - Core documentation (getting started, collections, querying, embeddings, CLI)
- `cloud/` - Chroma Cloud specific documentation (schema, search API, sync)
- `guides/` - Build and deploy guides
- `integrations/` - Embedding model and framework integrations
- `openapi.json` - OpenAPI spec for auto-generated API reference pages
- `snippets/` - Reusable MDX components

## Mintlify docs

https://www.mintlify.com/docs/llms.txt
