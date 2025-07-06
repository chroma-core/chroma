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

FROM builder AS log_service_builder
# sharing=locked is necessary to prevent cargo build from running concurrently on the same mounted directory
RUN --mount=type=cache,sharing=locked,target=/chroma/target/ \
    --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
    cd rust/log-service && \
    if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin log_service --release; else cargo build --bin log_service; fi && \
    cd ../.. && \
    if [ "$RELEASE_MODE" = "1" ]; then mv target/release/log_service ./log_service; else mv target/debug/log_service ./log_service; fi


FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*

FROM runner AS log_service
COPY --from=log_service_builder /chroma/log_service .
ENTRYPOINT [ "./log_service" ]
