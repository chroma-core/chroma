FROM rust:1.85.0 AS builder

ARG RELEASE_MODE=
ARG PROTOC_VERSION=31.1
ARG ENABLE_AVX512=

# ADDRESS_SANITIZER is an optional build argument to enable Address Sanitizer.
ARG ADDRESS_SANITIZER
RUN if [ "$ADDRESS_SANITIZER" = "1" ]; then \
  apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  build-essential gcc g++ libssl-dev ca-certificates && \
  rustup default nightly && \
  rustup target add x86_64-unknown-linux-gnu ; \
  fi
ENV RUSTFLAGS=${ADDRESS_SANITIZER:+'-Z sanitizer=address'}
ENV CC_x86_64_unknown_linux_gnu=${ADDRESS_SANITIZER:+gcc}
ENV CXX_x86_64_unknown_linux_gnu=${ADDRESS_SANITIZER:+g++}
ENV AR_x86_64_unknown_linux_gnu=${ADDRESS_SANITIZER:+ar}

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
    export RUSTFLAGS="${RUSTFLAGS} -C target-feature=+avx,+fma" && \
    echo "Building with AVX512 optimizations for hnswlib and AVX for Rust"; \
  else \
    echo "Building without AVX512 optimizations"; \
  fi && \
  build_target=$( [ "${ADDRESS_SANITIZER}" = "1" ] && echo '--target x86_64-unknown-linux-gnu' || echo '' ) && \
  if [ "$RELEASE_MODE" = "1" ]; then cargo build ${build_target} --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES) --release; else cargo build ${build_target} --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES); fi && \
  build_dir=$( [ "$RELEASE_MODE" = "1" ] && echo release || echo debug ) && \
  build_dir=$( [ "${ADDRESS_SANITIZER}" = "1" ] && echo "x86_64-unknown-linux-gnu/${build_dir}" || echo "${build_dir}" ) && \
  for bin in chroma garbage_collector_service chroma-load log_service heap_tender_service query_service compaction_service sysdb_service; do \
  mv "target/${build_dir}/${bin}" "./${bin}"; \
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

FROM runner AS sysdb_service
COPY --from=builder /chroma/sysdb_service .
ENTRYPOINT [ "sh", "-c", "ulimit -c 0 && exec ./sysdb_service" ]