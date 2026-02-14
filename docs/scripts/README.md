# Documentation Generator Scripts

## Python Reference

Generate all split reference files into `docs/mintlify/reference/python/`:

```bash
uv run docs/scripts/generate_python_reference.py --output reference/python/
```

This produces `client.mdx`, `collection.mdx`, `embedding-functions.mdx`, `search.mdx`, and `schema.mdx`. There is no index page; `/reference/python` and `/reference/python/index` redirect to `/reference/python/client`. The file `reference/python/where-filter.mdx` is maintained by hand (Python DSL only) and is not overwritten by the script.

## TypeScript Reference

Generate all split reference files into `docs/mintlify/reference/typescript/`:

```bash
bun run docs/scripts/generate_ts_reference.ts --output reference/typescript/
```

This produces `client.mdx`, `collection.mdx`, `embedding-functions.mdx`, `search.mdx`, and `schema.mdx`. There is no index page; `/reference/typescript` and `/reference/typescript/index` redirect to `/reference/typescript/client`. The file `reference/typescript/where-filter.mdx` is maintained by hand (TypeScript DSL only) and is not overwritten by the script.
