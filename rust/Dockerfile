# syntax=docker/dockerfile:1

# ============================================================================
# chef: shared toolchain base (rust + protoc + cargo-chef).
#
# This is the slow-changing setup layer. It is shared by the `planner` and
# `builder` stages below so the toolchain/protoc install is built and cached
# once, regardless of source changes.
# ============================================================================
FROM rust:1.92.0 AS chef

ARG PROTOC_VERSION=31.1

# ADDRESS_SANITIZER is an optional build argument to enable Address Sanitizer.
ARG ADDRESS_SANITIZER
RUN if [ "$ADDRESS_SANITIZER" = "1" ]; then \
  apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  build-essential gcc g++ libssl-dev ca-certificates && \
  rustup default nightly && \
  rustup target add x86_64-unknown-linux-gnu ; \
  fi

RUN ARCH=$(uname -m) && \
  if [ "$ARCH" = "x86_64" ]; then \
  PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-x86_64.zip; \
  elif [ "$ARCH" = "aarch64" ]; then \
  PROTOC_ZIP=protoc-${PROTOC_VERSION}-linux-aarch_64.zip; \
  else \
  echo "Unsupported architecture: $ARCH" && exit 1; \
  fi && \
  curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/$PROTOC_ZIP && \
  unzip -o $PROTOC_ZIP -d /usr/local bin/protoc && \
  unzip -o $PROTOC_ZIP -d /usr/local 'include/*' && \
  rm -f $PROTOC_ZIP && \
  chmod +x /usr/local/bin/protoc && \
  protoc --version  # Verify installed version

# cargo-chef lets us cache the dependency compile as a content-addressed image
# LAYER (keyed on the dependency graph) instead of leaving it in a volatile
# `--mount=type=cache` directory that is wiped on a cold/contended builder.
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
  cargo install cargo-chef --locked

WORKDIR /chroma

# ============================================================================
# planner: emit recipe.json (the dependency graph only).
#
# This stage is cheap. Its only purpose is to produce a `recipe.json` that
# changes ONLY when dependencies change (Cargo.toml / Cargo.lock), giving the
# `builder` stage a stable cache key for the dependency compile.
# ============================================================================
FROM chef AS planner

COPY idl/ idl/
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY rust/ rust/

RUN cargo chef prepare --recipe-path recipe.json

# ============================================================================
# builder: cook dependencies into a durable layer, then build the workspace.
#
# `cargo chef cook` compiles ONLY third-party dependencies, writing them to
# ./target. Because this step's only input is recipe.json (and we do NOT mount
# a cache over ./target), its result is captured as a regular image layer:
#   * content-addressed on recipe.json -> reused on every build whose deps are
#     unchanged (the common case: app-only source change),
#   * never thrashed cross-arch (each arch builds its own layer),
#   * exportable to a registry (a `--mount=type=cache` dir can never be).
#
# The subsequent `cargo build` then compiles only the first-party workspace
# crates, reusing the cooked dependencies already present in ./target.
# ============================================================================
FROM chef AS builder

ARG RELEASE_MODE=
ARG ENABLE_AVX512=
ARG LOG_SERVICE_CARGO_FEATURES=
ARG ADDRESS_SANITIZER

ENV RUSTFLAGS=${ADDRESS_SANITIZER:+'-Z sanitizer=address'}
ENV CC_x86_64_unknown_linux_gnu=${ADDRESS_SANITIZER:+gcc}
ENV CXX_x86_64_unknown_linux_gnu=${ADDRESS_SANITIZER:+g++}
ENV AR_x86_64_unknown_linux_gnu=${ADDRESS_SANITIZER:+ar}
ENV CARGO_INCREMENTAL=0

# Skip building these as they're not needed by images (and if Python bindings
# are built, the final binaries are unnecessarily linked against Python).
ENV EXCLUDED_PACKAGES="chromadb_rust_bindings chromadb-js-bindings chroma-benchmark "

# --- Dependency compile (durable layer, keyed on recipe.json) ----------------
# Note: cache mounts are kept ONLY for the crate download dirs (registry/git);
# the compiled output in ./target is intentionally a layer, not a mount, which
# is what makes cargo-chef effective.
COPY --from=planner /chroma/recipe.json recipe.json
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/git/ \
  if [ "$ENABLE_AVX512" = "1" ]; then \
  export CXXFLAGS="-mavx512f -mavx512dq -mavx512bw -mavx512vl" && \
  export CFLAGS="-mavx512f -mavx512dq -mavx512bw -mavx512vl" && \
  export RUSTFLAGS="${RUSTFLAGS} -C target-feature=+avx,+fma" ; \
  fi && \
  build_target=$( [ "${ADDRESS_SANITIZER}" = "1" ] && echo '--target x86_64-unknown-linux-gnu' || echo '' ) && \
  release_flag=$( [ "$RELEASE_MODE" = "1" ] && echo '--release' || echo '' ) && \
  cargo chef cook ${build_target} --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES) ${release_flag} --recipe-path recipe.json

# --- Workspace compile (first-party crates) ----------------------------------
COPY idl/ idl/
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY rust/ rust/

# Note: Using flag ENABLE_AVX512 to build AVX512 optimizations for hnswlib, and
# AVX for Rust. Once Rust supports AVX512, the target-features will be updated
# to use AVX512.
# No `--mount=type=cache` on ./target here: the cooked dependencies live in the
# layer produced above, and mounting a cache over ./target would shadow them.
RUN --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/git/ \
  if [ "$ENABLE_AVX512" = "1" ]; then \
  export CXXFLAGS="-mavx512f -mavx512dq -mavx512bw -mavx512vl" && \
  export CFLAGS="-mavx512f -mavx512dq -mavx512bw -mavx512vl" && \
  export RUSTFLAGS="${RUSTFLAGS} -C target-feature=+avx,+fma" ; \
  fi && \
  build_target=$( [ "${ADDRESS_SANITIZER}" = "1" ] && echo '--target x86_64-unknown-linux-gnu' || echo '' ) && \
  release_flag=$( [ "$RELEASE_MODE" = "1" ] && echo '--release' || echo '' ) && \
  cargo build ${build_target} --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES) ${release_flag} && \
  if [ -n "$LOG_SERVICE_CARGO_FEATURES" ]; then \
  cargo build ${build_target} -p chroma-log-service --bin log_service --features "$LOG_SERVICE_CARGO_FEATURES" ${release_flag}; \
  fi && \
  build_dir=$( [ "$RELEASE_MODE" = "1" ] && echo release || echo debug ) && \
  build_dir=$( [ "${ADDRESS_SANITIZER}" = "1" ] && echo "x86_64-unknown-linux-gnu/${build_dir}" || echo "${build_dir}" ) && \
  for bin in chroma garbage_collector_service chroma-load log_service heap_tender_service query_service compaction_service work_queue_service fn_consumer sysdb_service spanner_migration; do \
  cp "target/${build_dir}/${bin}" "./${bin}"; \
  done

FROM debian:stable-slim AS runner
ARG ADDRESS_SANITIZER

ENV ASAN_OPTIONS=${ADDRESS_SANITIZER:+'symbolize=1'}
ENV ASAN_SYMBOLIZER_PATH=${ADDRESS_SANITIZER:+'/usr/bin/llvm-symbolizer'}
# If ADDRESS_SANITIZER is set, set RUST_BACKTRACE to full. Otherwise, set it to 0.
ENV RUST_BACKTRACE=${ADDRESS_SANITIZER:+'full'}${ADDRESS_SANITIZER:-'0'}

RUN if [ "$ADDRESS_SANITIZER" = "1" ]; then apt-get update \
  && apt-get install -y build-essential llvm; \
  fi
RUN apt-get update && apt-get install -y dumb-init libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

FROM runner AS cli

COPY --from=builder /chroma/rust/frontend/sample_configs/docker_single_node.yaml /config.yaml
COPY --from=builder /chroma/chroma /usr/local/bin/chroma

EXPOSE 8000

ENTRYPOINT [ "dumb-init", "--", "chroma" ]
CMD [ "run", "/config.yaml" ]

FROM runner AS garbage_collector
COPY --from=builder /chroma/garbage_collector_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./garbage_collector_service" ]

FROM runner AS load_service
COPY --from=builder /chroma/chroma-load .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./chroma-load" ]

FROM runner AS log_service
COPY --from=builder /chroma/log_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./log_service" ]

FROM runner AS heap_tender_service
COPY --from=builder /chroma/heap_tender_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./heap_tender_service" ]

FROM runner AS query_service
COPY --from=builder /chroma/query_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./query_service" ]

FROM runner AS compaction_service
COPY --from=builder /chroma/compaction_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./compaction_service" ]

FROM runner AS work_queue_service
COPY --from=builder /chroma/work_queue_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./work_queue_service" ]

FROM runner AS fn_consumer
COPY --from=builder /chroma/fn_consumer .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./fn_consumer" ]

FROM runner AS sysdb_service
COPY --from=builder /chroma/sysdb_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./sysdb_service" ]

FROM runner AS rust-sysdb-migration
COPY --from=builder /chroma/spanner_migration .
ENTRYPOINT ["sh", "-c", "ulimit -c 0 && exec ./spanner_migration" ]
