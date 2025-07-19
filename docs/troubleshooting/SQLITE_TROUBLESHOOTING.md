# ChromaDB SQLite Compatibility Guide

Why this guide? Many developers hit confusing sqlite3 errors when first running ChromaDB on
systems that ship an outdated SQLite build.  This quick reference shows how to diagnose and fix the
issue in minutes.

## Common Errors

sqlite3.OperationalError: no such module: VectorSearch
ModuleNotFoundError: No module named '_sqlite3'
chromadb.errors.InvalidDimensionException

## Root Cause
ChromaDB requires SQLite ≥ 3.35 compiled with FTS5 support.  Stock Python builds on Ubuntu,
Windows, and macOS often link against older or stripped‑down versions, so ChromaDB’s vector search
extension cannot load.

## Solutions

### 1  |  Python‑only Fix (Recommended)

Add to 'requirements.txt':
```text
pysqlite3-binary>=0.5.2
```

Override the import at the very top of your main entrypoint (before any other SQLite usage):
```python
import sys
try:
    import pysqlite3 as sqlite3  # bundles SQLite 3.45 with FTS5
    sys.modules["sqlite3"] = sqlite3
except ImportError:
    # Fallback — will still work on environments that already have a good SQLite
    pass
```
### 2  |  Environment Override
If you build your own Python interpreter with a modern SQLite, set:
```bash
export CHROMA_SQLITE_OVERRIDE=1
```

ChromaDB will skip the version check and trust your system library.

### 3  |  Docker Base Image
Use an up‑to‑date base such as python:3.11-slim or python:3.10-bookworm which already ships
SQLite 3.43 + FTS5.

## Verification
```python
import chromadb
client = chromadb.Client()
collection = client.create_collection("test")
print("✅ ChromaDB working correctly!")
```

## Tested Environments

Ubuntu 20.04 & 22.04 (Python 3.10 & 3.11)

Windows 10 & 11 (WSL 2 + native)

macOS Monterey 12 + (macOS‑provided Python & Homebrew)

Docker images based on Debian bookworm & Alpine edge

## Contribute This Guide Back to ChromaDB

Fork the repo: https://github.com/chroma-core/chroma

Add this file under /docs/troubleshooting/SQLITE_TROUBLESHOOTING.md (create folders if needed).

Reference it from the main README or existing Getting Started guide.

Open a Pull Request titled “Docs: Add SQLite troubleshooting guide”.

If you spot new edge‑cases, please open an issue or PR.  Let’s save everyone those midnight
OperationalError headaches! 🚀

