# Chroma Codebase Guidelines for AI Agents

See [CLAUDE.md](./CLAUDE.md) for codebase conventions (commit message format, etc.).

## Cursor Cloud specific instructions

Scope that is set up in this environment: the **single-node (embedded) Chroma
server** — the Rust core + Python `chromadb` package. The distributed stack
(Tilt / Kubernetes / Docker) is NOT set up here; those tests
(`test_k8s_integration`, `chromadb/test/distributed/*`) require `tilt up` and
will fail without it. See `DEVELOP.md` for the standard commands.

Non-obvious environment notes:

- **Python venv lives at `./venv`.** Activate with `source venv/bin/activate`
  before running `maturin`, `pytest`, `chroma`, etc. Deps are installed there,
  not system-wide (system pip is externally-managed on this Ubuntu 24.04 box).
- **Rust must be stable ≥ 1.85** (a dependency, `rmcp-macros`, needs
  `edition2024`). The pinned-in-CI-as-"stable" toolchain is `rustup default
  stable`; the shipped 1.83 is too old. `cargo` is on PATH at
  `/usr/local/cargo/bin`.
- **`protoc` is required** to compile the Rust workspace (`rust/types/build.rs`
  compiles protobufs) and therefore for `maturin develop`. Installed at
  `/usr/local/bin/protoc`.
- **Build the Python bindings with `maturin develop`** (from inside the venv).
  This compiles the whole Rust workspace the first time. The compiled
  extension does NOT hot-reload — after changing Rust code you must re-run
  `maturin develop` for Python to pick it up.
- **Python gRPC proto stubs are generated, not committed.** Regenerate with
  `make -C idl proto_python` (uses `grpc_tools` from the venv). Only needed for
  distributed-mode modules/tests; the embedded path does not import them.
- **Run the single-node server:** `chroma run --path ./chroma_data` (listens on
  `:8000`; Swagger UI at `/docs/`, health at `/api/v2/heartbeat`). The first
  embedding query downloads the default ONNX model (~79 MB) to
  `~/.cache/chroma` and needs network.
- **Tests:** embedded Python tests run with `CHROMA_RUST_BINDINGS_TEST_ONLY=1
  python -m pytest chromadb/test/...`. Full single-node integration tests:
  `bin/rust-integration-test.sh <pytest-args>`. Rust tests use `cargo nextest`
  in CI (not installed here) but plain `cargo test -p <crate>` works.
- **Lint** (matches CI): `pre-commit run --all-files black` and `... flake8`
  for Python; `cargo fmt --all -- --check` and `cargo clippy` for Rust. Note
  `flake8` only exists inside the pre-commit hook env, not as a direct dep.
