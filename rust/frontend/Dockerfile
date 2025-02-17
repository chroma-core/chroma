FROM rust:1.81.0 AS builder

ARG RELEASE_MODE=

WORKDIR /

# ADD with an external URL is always run, so this will trigger a cache invalidation (and thus run the below git clone) if the latest commit changes
ADD "https://api.github.com/repos/chroma-core/hnswlib/commits?per_page=1" hnswlib_commits.json
RUN git clone https://github.com/chroma-core/hnswlib.git

WORKDIR /chroma/

ENV PROTOC_ZIP=protoc-25.1-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP

# Install python for pyo3
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY idl/ idl/
COPY rust/ rust/

FROM builder AS frontend_service_builder
# sharing=locked is necessary to prevent cargo build from running concurrently on the same mounted directory
RUN --mount=type=cache,sharing=locked,target=/chroma/target/ \
    --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
    if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin frontend_service --release --features python; else cargo build --bin frontend_service --features python; fi && \
    if [ "$RELEASE_MODE" = "1" ]; then mv target/release/frontend_service ./frontend_service; else mv target/debug/frontend_service ./frontend_service; fi


FROM debian:bookworm-slim AS runner
RUN apt-get update && apt-get install -y libssl-dev ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /chroma/rust/frontend/frontend_config.yaml .

# Install python for pyo3
RUN apt-get update && apt-get install -y python3 python3-pip && rm -rf /var/lib/apt/lists/*

FROM runner AS frontend_service
COPY --from=frontend_service_builder /chroma/frontend_service .
ENTRYPOINT [ "./frontend_service" ]
