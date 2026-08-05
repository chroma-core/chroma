# AST-Aware Code Ingestion with Chroma

This example demonstrates how to maintain an incrementally-updated,
semantic knowledge base of a source code repository using ChromaDB.

## Features

- **Semantic chunking** — Python files are split at function/class/method
  boundaries using the `ast` module.  Markdown, YAML, and JSON get
  structure-aware splits.  Everything else falls back to sliding windows.
- **Credential redaction** — Secret patterns (API keys, database URLs,
  private keys) are replaced inline so embeddings never leak credentials.
- **Deterministic IDs** — Chunk IDs are derived from content hashes,
  making `upsert` idempotent.  Re-running ingestion on an unchanged
  file costs one hash comparison, not an embedding call.
- **Incremental updates** — Only changed files are re-embedded.
  Deleted/renamed files are pruned automatically.
- **Live watch mode** — Uses `watchdog` to debounce filesystem events
  and trigger incremental reindexes.

## Quick Start

```bash
# 1. Install dependencies
pip install chromadb watchdog

# 2. Run a one-shot index
python ingest.py /path/to/your/repo --collection my_project

# 3. Or watch and auto-update
python ingest.py /path/to/your/repo --collection my_project --watch
```

## How It Works

### Chunking

The `chunker.py` module dispatches by file extension:

| Extension | Strategy |
|-----------|----------|
| `.py` | AST nodes (`FunctionDef`, `AsyncFunctionDef`, `ClassDef`) + module-level leftovers |
| `.md` | Split on `#` / `##` / `###` headers |
| `.yaml`, `.yml` | Split on top-level keys |
| `.json` | Split on top-level keys (dict) |
| everything else | Sliding window with overlap |

### Deduplication

Each chunk receives an `id` computed as:

```python
sha256(f"{file_path}:{start_line}:{end_line}:{content}")[:32]
```

This guarantees that unchanged chunks get the same ID across runs.
Chroma's `upsert` skips re-embedding for identical IDs, so the
incremental cost is ~O(changed files), not ~O(total lines).

### Pruning

After each run we compare every `file_path` in the collection metadata
against the current filesystem.  Missing paths are deleted with:

```python
collection.delete(where={"file_path": stale_path})
```

### Watch Mode

`watchdog` monitors `on_created`, `on_modified`, `on_deleted`, and
`on_moved`.  Events are debounced (default 2 s) so rapid saves don't
trigger multiple reindexes.

## Query Examples

```python
import chromadb

client = chromadb.PersistentClient(path="./chroma_code_db")
coll = client.get_collection("my_project")

# Find functions related to user authentication
results = coll.query(
    query_texts=["how is user authentication handled"],
    n_results=5,
    where={"chunk_type": "FUNCTION"},
)

# Narrow to a specific file
results = coll.query(
    query_texts=["database connection pool"],
    n_results=3,
    where={"file_path": {"$contains": "models.py"}},
)
```

## Extending

- **Add a language** — Implement `_chunk_rust` (or Go, etc.) in
  `chunker.py` and register it in `chunk_file`.
- **Custom redaction** — Extend `_CREDENTIAL_PATTERNS` with your
  org's secret formats.
- **Batch embedding** — If you have >10k files, replace the per-file
  `upsert` with batched calls (Chroma batches are ~5k documents).
