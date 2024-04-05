FROM rust:1.74.1 as builder
ARG CHROMA_KUBERNETES_INTEGRATION=0
ENV CHROMA_KUBERNETES_INTEGRATION $CHROMA_KUBERNETES_INTEGRATION

ARG RELEASE_MODE=

WORKDIR /
RUN git clone https://github.com/chroma-core/hnswlib.git

# Cache dependencies by building them without our code first.
# https://dev.to/rogertorres/first-steps-with-docker-rust-30oi
# https://www.reddit.com/r/rust/comments/126xeyx/exploring_the_problem_of_faster_cargo_docker/
WORKDIR /chroma/
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY idl/ idl/
COPY /rust/worker/Cargo.toml rust/worker/Cargo.toml
ENV PROTOC_ZIP=protoc-25.1-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP

FROM builder as query_service_builder
# We need to replace the query node's real main() with a dummy at the expected location.
RUN mkdir -p rust/worker/src/bin/ && echo "fn main() {}" > rust/worker/src/bin/query_service.rs
RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin query_service --release; else cargo build --bin query_service; fi
RUN rm -rf rust/

COPY rust/ rust/
RUN touch rust/worker/src/bin/query_service.rs
RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin query_service --release; else cargo build --bin query_service; fi
RUN if [ "$RELEASE_MODE" = "1" ]; then mv target/release/query_service .; else mv target/debug/query_service .; fi

FROM debian:bookworm-slim as query_service

COPY --from=query_service_builder /chroma/query_service .
COPY --from=query_service_builder /chroma/rust/worker/chroma_config.yaml .
RUN apt-get update && apt-get install -y libssl-dev

ENTRYPOINT [ "./query_service" ]

FROM builder as compaction_service_builder

# We need to replace the compaction node's real main() with a dummy at the expected location.
RUN mkdir -p rust/worker/src/bin/ && echo "fn main() {}" > rust/worker/src/bin/compaction_service.rs
RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin compaction_service --release; else cargo build --bin compaction_service; fi
RUN rm -rf rust/

COPY rust/ rust/
RUN touch rust/worker/src/bin/compaction_service.rs
RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin compaction_service --release; else cargo build --bin compaction_service; fi
RUN if [ "$RELEASE_MODE" = "1" ]; then mv target/release/compaction_service .; else mv target/debug/compaction_service .; fi

FROM debian:bookworm-slim as compaction_service

COPY --from=compaction_service_builder /chroma/compaction_service .
COPY --from=compaction_service_builder /chroma/rust/worker/chroma_config.yaml .
RUN apt-get update && apt-get install -y libssl-dev

ENTRYPOINT [ "./compaction_service" ]
