FROM rust:1.79.0 as builder
ARG CHROMA_KUBERNETES_INTEGRATION=0
ENV CHROMA_KUBERNETES_INTEGRATION $CHROMA_KUBERNETES_INTEGRATION

ARG RELEASE_MODE=1

WORKDIR /
RUN git clone https://github.com/chroma-core/hnswlib.git

# Cache dependencies by building them without our code first.
# https://dev.to/rogertorres/first-steps-with-docker-rust-30oi
# https://www.reddit.com/r/rust/comments/126xeyx/exploring_the_problem_of_faster_cargo_docker/
WORKDIR /chroma/
COPY Cargo.toml Cargo.toml
COPY Cargo.lock Cargo.lock
COPY idl/ idl/
COPY /rust/blockstore/Cargo.toml rust/blockstore/Cargo.toml
COPY /rust/cache/Cargo.toml rust/cache/Cargo.toml
COPY /rust/config/Cargo.toml rust/config/Cargo.toml
COPY /rust/error/Cargo.toml rust/error/Cargo.toml
COPY /rust/storage/Cargo.toml rust/storage/Cargo.toml
COPY /rust/types/Cargo.toml rust/types/Cargo.toml
COPY /rust/distance/Cargo.toml rust/distance/Cargo.toml
COPY /rust/index/Cargo.toml rust/index/Cargo.toml
COPY /rust/worker/Cargo.toml rust/worker/Cargo.toml
ENV PROTOC_ZIP=protoc-25.1-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP

FROM builder as query_service_builder
# We need to replace the query node's real main() and all packages lib.rs with a dummy at the expected location. This forces a build of the dependencies only.
RUN for dir in blockstore cache config error storage types distance index; do \
    mkdir -p rust/$dir/src && echo "pub fn main() {}" > rust/$dir/src/lib.rs; \
    done
RUN mkdir -p rust/worker/src/bin/ && echo "fn main() {}" > rust/worker/src/bin/query_service.rs
RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin query_service --release; else cargo build --bin query_service; fi
RUN rm -rf rust/

COPY rust/ rust/
# We touch the files to hint to docker that these files are changed
RUN for dir in blockstore cache config error storage types distance index; do \
    touch rust/$dir/src/lib.rs; \
    done
RUN touch rust/worker/src/bin/query_service.rs

RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin query_service --release; else cargo build --bin query_service; fi
RUN if [ "$RELEASE_MODE" = "1" ]; then mv target/release/query_service .; else mv target/debug/query_service .; fi

FROM debian:bookworm-slim as query_service

RUN apt-get update && apt-get install -y libssl-dev ca-certificates
COPY --from=query_service_builder /chroma/query_service .
COPY --from=query_service_builder /chroma/rust/worker/chroma_config.yaml .

ENTRYPOINT [ "./query_service" ]

FROM builder as compaction_service_builder
# We need to replace the compaction node's real main() with a dummy at the expected location. This forces a build of the dependencies only.
RUN for dir in blockstore cache config error storage types distance index; do \
    mkdir -p rust/$dir/src && echo "pub fn main() {}" > rust/$dir/src/lib.rs; \
    done
RUN mkdir -p rust/worker/src/bin/ && echo "fn main() {}" > rust/worker/src/bin/compaction_service.rs
RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin compaction_service --release; else cargo build --bin compaction_service; fi
RUN rm -rf rust/

COPY rust/ rust/
RUN for dir in blockstore cache config error storage types distance index; do \
    touch rust/$dir/src/lib.rs; \
    done
RUN touch rust/worker/src/bin/compaction_service.rs
RUN if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin compaction_service --release; else cargo build --bin compaction_service; fi
RUN if [ "$RELEASE_MODE" = "1" ]; then mv target/release/compaction_service .; else mv target/debug/compaction_service .; fi

FROM debian:bookworm-slim as compaction_service

RUN apt-get update && apt-get install -y libssl-dev ca-certificates
COPY --from=compaction_service_builder /chroma/compaction_service .
COPY --from=compaction_service_builder /chroma/rust/worker/chroma_config.yaml .

# ENV export _RJEM_MALLOC_CONF=prof:true,lg_prof_interval:30,lg_prof_sample:21,prof_prefix:/tmp/jeprof
ENV _RJEM_MALLOC_CONF "prof:true,lg_prof_interval:30,lg_prof_sample:21,prof_prefix:/tmp/jeprof"

ENTRYPOINT [ "./compaction_service" ]
