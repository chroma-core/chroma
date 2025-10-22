FROM rust:1.85.0 AS builder

ARG RELEASE_MODE=
ARG PROTOC_VERSION=31.1
ARG ENABLE_AVX512=

WORKDIR /chroma

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

COPY idl/ idl/
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY rust/ rust/

# Skip building these as they're not needed by images (and if Python bindings are built, the final binaries are unnecessarily linked against Python).
ENV EXCLUDED_PACKAGES="chromadb_rust_bindings chromadb-js-bindings chroma-benchmark "

# Note: Using flag ENABLE_AVX512 to build AVX512 optimizations for hnswlib, and AVX for Rust.
# Once Rust supports AVX512, the target-features will be updated to use AVX512.
RUN --mount=type=cache,sharing=locked,target=/chroma/target/ \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
  if [ "$ENABLE_AVX512" = "1" ]; then \
    export CXXFLAGS="-mavx512f -mavx512dq -mavx512bw -mavx512vl" && \
    export CFLAGS="-mavx512f -mavx512dq -mavx512bw -mavx512vl" && \
    export RUSTFLAGS="-C target-feature=+avx,+fma" && \
    echo "Building with AVX512 optimizations for hnswlib and AVX for Rust"; \
  else \
    echo "Building without AVX512 optimizations"; \
  fi && \
  if [ "$RELEASE_MODE" = "1" ]; then cargo build --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES) --release; else cargo build --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES); fi && \
  # Move CLI binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/chroma ./chroma; else mv target/debug/chroma ./chroma; fi && \
  # Move garbage collector binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/garbage_collector_service ./garbage_collector_service; else mv target/debug/garbage_collector_service ./garbage_collector_service; fi && \
  # Move load service binaries
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/chroma-load ./chroma-load; else mv target/debug/chroma-load ./chroma-load; fi && \
  # Move log service binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/log_service ./log_service; else mv target/debug/log_service ./log_service; fi && \
  # Move heap tender binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/heap_tender_service ./heap_tender_service; else mv target/debug/heap_tender_service ./heap_tender_service; fi && \
  # Move query service binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/query_service ./query_service; else mv target/debug/query_service ./query_service; fi && \
  # Move compaction service binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/compaction_service ./compaction_service; else mv target/debug/compaction_service ./compaction_service; fi


FROM debian:stable-slim AS runner

RUN apt-get update && apt-get install -y dumb-init libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

FROM runner AS cli

COPY --from=builder /chroma/rust/frontend/sample_configs/docker_single_node.yaml /config.yaml
COPY --from=builder /chroma/rust/frontend/sample_configs/distributed.yaml /chroma_config.yaml
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
COPY --from=builder /chroma/rust/worker/chroma_config.yaml .
COPY --from=builder /chroma/query_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./query_service" ]

FROM runner AS compaction_service
COPY --from=builder /chroma/rust/worker/chroma_config.yaml .
COPY --from=builder /chroma/compaction_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./compaction_service" ]
