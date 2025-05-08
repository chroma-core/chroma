FROM rust:1.81.0 AS builder

ARG RELEASE_MODE=

WORKDIR /chroma

ENV PROTOC_ZIP=protoc-25.1-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/$PROTOC_ZIP \
  && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
  && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
  && rm -f $PROTOC_ZIP

COPY idl/ idl/
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY rust/ rust/

# Skip building these as they're not needed by images (and if Python bindings are built, the final binaries are unnecessarily linked against Python).
ENV EXCLUDED_PACKAGES="chromadb_rust_bindings chromadb-js-bindings chroma-benchmark "

RUN --mount=type=cache,sharing=locked,target=/chroma/target/ \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
  if [ "$RELEASE_MODE" = "1" ]; then cargo build --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES) --release; else cargo build --workspace $(printf -- '--exclude %s ' $EXCLUDED_PACKAGES); fi && \
  # Move CLI binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/chroma ./chroma; else mv target/debug/chroma ./chroma; fi && \
  # Move garbage collector binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/garbage_collector_service ./garbage_collector_service; else mv target/debug/garbage_collector_service ./garbage_collector_service; fi && \
  # Move load service binaries
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/chroma-load-start ./chroma-load-start; else mv target/debug/chroma-load-start ./chroma-load-start; fi && \
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/chroma-load ./chroma-load; else mv target/debug/chroma-load ./chroma-load; fi && \
  # Move log service binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/log_service ./log_service; else mv target/debug/log_service ./log_service; fi && \
  # Move query service binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/query_service ./query_service; else mv target/debug/query_service ./query_service; fi && \
  # Move compaction service binary
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/compaction_service ./compaction_service; else mv target/debug/compaction_service ./compaction_service; fi


FROM debian:stable-slim AS runner

RUN apt-get update && apt-get install -y dumb-init libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

FROM runner AS cli

COPY --from=builder /chroma/rust/frontend/sample_configs/docker_single_node.yaml /config.yaml
COPY --from=builder /chroma/rust/frontend/sample_configs/tilt_config.yaml /tilt_config.yaml
COPY --from=builder /chroma/chroma /usr/local/bin/chroma

EXPOSE 8000

ENTRYPOINT [ "dumb-init", "--", "chroma" ]
CMD [ "run", "/config.yaml" ]

FROM runner AS garbage_collector
COPY --from=builder /chroma/garbage_collector_service .
ENTRYPOINT [ "./garbage_collector_service" ]

FROM runner AS load_service
COPY --from=builder /chroma/chroma-load .
COPY --from=builder /chroma/chroma-load-start .
ENTRYPOINT [ "./chroma-load" ]

FROM runner AS log_service
COPY --from=builder /chroma/log_service .
ENTRYPOINT [ "./log_service" ]

FROM runner AS query_service
COPY --from=builder /chroma/rust/worker/chroma_config.yaml .
# NOTE(rescrv): We need the tilt config in the docker file.  This is temporary.
COPY --from=builder /chroma/rust/worker/tilt_config.yaml .

COPY --from=builder /chroma/query_service .
ENTRYPOINT [ "./query_service" ]

FROM runner AS compaction_service
COPY --from=builder /chroma/rust/worker/chroma_config.yaml .
# NOTE(rescrv): We need the tilt config in the docker file.  This is temporary.
COPY --from=builder /chroma/rust/worker/tilt_config.yaml .

COPY --from=builder /chroma/compaction_service .
ENTRYPOINT [ "./compaction_service" ]
