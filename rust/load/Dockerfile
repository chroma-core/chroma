FROM rust:1.81.0 AS builder

ARG RELEASE_MODE=

WORKDIR /chroma/

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY idl/ idl/
COPY rust/ rust/

FROM builder AS load_service_builder
# we want to avoid caching the target directory because Buildkit has its own cache that conflicts with the cargo cache
RUN --mount=type=cache,target=/usr/local/cargo/git/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin chroma-load --release; else cargo build --bin chroma-load; fi && \
    if [ "$RELEASE_MODE" = "1" ]; then mv target/release/chroma-load ./chroma-load; else mv target/debug/chroma-load ./chroma-load; fi

FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /chroma/rust/load/chroma_load_config.yaml .

FROM runner AS load_service
COPY --from=load_service_builder /chroma/chroma-load .
ENTRYPOINT [ "./chroma-load" ]
