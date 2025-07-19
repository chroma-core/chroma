FROM rust:1.82.0 AS builder

ARG RELEASE_MODE=
ARG PROTOC_VERSION=31.1

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
