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

# sharing=locked is necessary to prevent cargo build from running concurrently on the same mounted directory
RUN --mount=type=cache,sharing=locked,target=/chroma/target/ \
  --mount=type=cache,sharing=locked,target=/usr/local/cargo/registry/ \
  cd rust/cli && \
  if [ "$RELEASE_MODE" = "1" ]; then cargo build --bin chroma --release; else cargo build --bin chroma; fi && \
  cd ../.. && \
  if [ "$RELEASE_MODE" = "1" ]; then mv target/release/chroma ./chroma; else mv target/debug/chroma ./chroma; fi


FROM debian:stable-slim AS runner

RUN apt-get update && apt-get install -y dumb-init && rm -rf /var/lib/apt/lists/*

COPY --from=builder /chroma/rust/frontend/sample_configs/docker_single_node.yaml /config.yaml
COPY --from=builder /chroma/rust/frontend/sample_configs/tilt_config.yaml /tilt_config.yaml
COPY --from=builder /chroma/chroma /usr/local/bin/chroma

EXPOSE 8000

ENTRYPOINT [ "dumb-init", "--", "chroma" ]
CMD [ "run", "/config.yaml" ]
