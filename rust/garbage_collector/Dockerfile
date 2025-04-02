FROM rust:1.81.0 AS builder

ARG RELEASE_MODE=

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

FROM builder AS garbage_collector_builder
RUN --mount=type=cache,sharing=locked,target=/chroma/target/ \
    --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
    cd rust/garbage_collector && \
    if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin garbage_collector_service --release; else cargo build --bin garbage_collector_service; fi && \
    cd ../.. && \
    if [ "$RELEASE_MODE" = "1" ]; then mv target/release/garbage_collector_service ./garbage_collector_service; else mv target/debug/garbage_collector_service ./garbage_collector_service; fi

FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

FROM runner AS garbage_collector
COPY --from=garbage_collector_builder /chroma/garbage_collector_service .
ENTRYPOINT [ "./garbage_collector_service" ]
