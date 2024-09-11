FROM rust:1.79.0 AS builder
ARG CHROMA_KUBERNETES_INTEGRATION=0
ENV CHROMA_KUBERNETES_INTEGRATION=$CHROMA_KUBERNETES_INTEGRATION

ARG RELEASE_MODE=

WORKDIR /
RUN git clone https://github.com/chroma-core/hnswlib.git

WORKDIR /chroma/

ENV PROTOC_ZIP=protoc-25.1-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY idl/ idl/
COPY rust/ rust/

FROM builder AS query_service_builder
RUN --mount=type=cache,target=/chroma/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin query_service --release; else cargo build --bin query_service; fi && \
    if [ "$RELEASE_MODE" = "1" ]; then mv target/release/query_service ./query_service; else mv target/debug/query_service ./query_service; fi

FROM builder AS compaction_service_builder
RUN --mount=type=cache,target=/chroma/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin compaction_service --release; else cargo build --bin compaction_service; fi && \
    if [ "$RELEASE_MODE" = "1" ]; then mv target/release/compaction_service ./compaction_service; else mv target/debug/compaction_service ./compaction_service; fi


FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /chroma/rust/worker/chroma_config.yaml .

FROM runner AS query_service
COPY --from=query_service_builder /chroma/query_service .
ENTRYPOINT [ "./query_service" ]

FROM runner AS compaction_service
COPY --from=compaction_service_builder /chroma/compaction_service .
ENTRYPOINT [ "./compaction_service" ]
