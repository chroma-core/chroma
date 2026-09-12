# HTTPXodus migration: chroma-core/chroma → httpx2

Target: https://github.com/chroma-core/chroma
Issue: https://github.com/chroma-core/chroma/issues/7671
Branch: `httpxodus/httpx2-migration`
Fork: https://github.com/ProgrammerPlus1998/chroma

## Approach

Dual-import (Option A from the issue). 3.9 users keep `httpx`; 3.10+ users
get `httpx2` automatically. Library code is untouched, no public API change.

```python
try:
    import httpx2 as httpx
except ImportError:
    import httpx
```

## Changes

```
 chromadb/api/async_client.py     | 5 ++++-
 chromadb/api/async_fastapi.py    | 5 ++++-
 chromadb/api/base_http_client.py | 5 ++++-
 chromadb/api/client.py           | 5 ++++-
 chromadb/api/fastapi.py          | 5 ++++-
 pyproject.toml                   | 2 +-
 6 files changed, 21 insertions(+), 6 deletions(-)
```

- `pyproject.toml`: added `httpx2>=2.12.0; python_version >= "3.10"` next to
  the existing `httpx>=0.27.0`. `requires-python = ">=3.9"` is unchanged.
- `chromadb/api/{base_http_client,fastapi,async_fastapi,client,async_client}.py`:
  each top-level `import httpx` replaced with the dual-import block above.
  No call-site changes — `httpx.Client`, `httpx.AsyncClient`, `httpx.Limits`,
  `httpx.Response`, `httpx.HTTPStatusError`, `httpx.ConnectError` all keep
  working under either package.

Rust core (`rust/`), `Cargo.toml`, `chromadb-async/`, `chromadb/hnswlib/`,
and every file under `chromadb/test/` were intentionally left alone.

## Verification

- httpx2 2.12.0 installed; all five `chromadb/api/*.py` modules import and
  resolve `httpx` to `httpx2`. `httpx.Limits(max_connections=10, ...)` works
  under both packages (smoke test confirmed).
- `chromadb/test/api/test_base_http_client.py` (5 unit tests) passes with
  httpx2 *uninstalled* (fallback to httpx). With httpx2 installed the tests
  fail because the test file imports `httpx` directly and creates
  `httpx.Response` fixtures; those fixtures are then passed to
  `BaseHTTPClient._raise_chroma_error`, which now catches
  `httpx2.HTTPStatusError` — a different class. The test file was excluded
  from this PR per the task scope (`tests/` is explicitly out of bounds);
  the fix is a follow-up that mirrors the same dual-import in the test
  layer.
- The full `pip install -e ".[dev]"` step was not run: chromadb's build
  backend is maturin + a Rust workspace (`rust/python_bindings`), and the
  Rust toolchain on this host is not set up. The Python-only install that
  was used (httpx, httpx2, orjson, overrides, numpy, pyyaml, bcrypt, grpcio,
  pydantic, pydantic-settings, tenacity, mmh3, tqdm, kubernetes, typer,
  pybase64, rich, jsonschema, opentelemetry-*, fastapi, onnxruntime,
  tokenizers, opentelemetry-instrumentation-fastapi, hypothesis, uvicorn,
  pypika) is enough to import the five touched modules and to confirm the
  dual-import resolves to httpx2 in 3.10+ environments.

## Commit

```
82f7f1c refactor: migrate httpx to httpx2 with dual import
```

Pushed to `ProgrammerPlus1998/chroma:httpxodus/httpx2-migration`. No PR
opened — left for review before any upstream pull request.
