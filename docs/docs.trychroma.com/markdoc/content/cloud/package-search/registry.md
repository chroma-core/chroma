---
id: package-search-registry
name: Package Search Registry
---

# Package Search Registry

[Chroma Package Search](https://github.com/chroma-core/package-search) is the index of public code packages that powers the [Package Search MCP server](/cloud/package-search/mcp). It is the source of truth for which packages and versions Chroma indexes for code search and retrieval.

Chroma currently indexes about 13k versions of 3k packages across multiple registries.

## How it works

The registry is maintained in the [Package Search repository](https://github.com/chroma-core/package-search). It defines what should be indexed and how to locate each package’s source at a specific version.

- [`index.json`](https://github.com/chroma-core/package-search/blob/main/index.json) declares which packages should be indexed.
- [`versions.json`](https://github.com/chroma-core/package-search/blob/main/versions.json) is a generated output that lists all packages and versions currently indexed. It is automatically updated by the indexing service.

Chroma’s indexer reads these files, resolves each version to a git tag according to the package’s `tag_formats`, fetches the source, and indexes only files matching the package’s `include` globs.

## Supported registries

Chroma supports these registries and identifiers:

- [`npm`](https://www.npmjs.com/) — JavaScript + TypeScript packages
- [`py_pi`](https://pypi.org/) — Python packages
- [`crates_io`](https://crates.io/) — Rust crates
- [`golang_proxy`](https://proxy.golang.org/) — Go modules
- [`github_releases`](https://docs.github.com/en/repositories/releasing-projects-on-github/about-releases) — Packages distributed via GitHub Releases

## Indexed versions

Version discovery is driven by the package’s tag formats and the underlying registry. The indexer resolves published versions to git tags (annotated or lightweight) using the configured formats. Historical indexing is bounded by the sentinel timestamp, so versions published before that time are ignored.

## How to add a package

Anyone can request additional packages by opening a Pull Request against the Package Search repository.

[Follow the directions in the README](https://github.com/chroma-core/package-search/blob/main/README.md#adding-new-packages) to add a new package.
