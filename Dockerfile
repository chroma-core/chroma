FROM python:3.11-slim-bookworm AS builder
ARG REBUILD_HNSWLIB
ARG PROTOC_VERSION=31.1
RUN apt-get update --fix-missing && apt-get install -y --fix-missing \
    build-essential \
    gcc \
    g++ \
    cmake \
    autoconf \
    python3-dev \
    unzip \
    curl \
    make && \
    curl https://sh.rustup.rs -sSf | sh -s -- -y --default-toolchain stable && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir /install
ENV PATH="/root/.cargo/bin:$PATH"
ENV CARGO_INCREMENTAL=0

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

WORKDIR /install

COPY ./requirements.txt requirements.txt

RUN --mount=type=cache,target=/root/.cache/pip pip install maturin cffi patchelf
RUN --mount=type=cache,target=/root/.cache/pip pip install --upgrade --prefix="/install" -r requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip pip install --upgrade --prefix="/install" "fastapi>=0.115.9" "opentelemetry-instrumentation-fastapi>=0.41b0"
RUN --mount=type=cache,target=/root/.cache/pip if [ "$REBUILD_HNSWLIB" = "true" ]; then pip install --no-binary :all: --force-reinstall --prefix="/install" chroma-hnswlib; fi

# Install gRPC tools for Python with fixed version
RUN --mount=type=cache,target=/root/.cache/pip pip install grpcio==1.58.0 grpcio-tools==1.58.0

WORKDIR /chroma

# Copy only package build inputs so unrelated repository files do not
# invalidate the wheel build layer.
COPY ./Cargo.toml ./Cargo.toml
COPY ./Cargo.lock ./Cargo.lock
COPY ./LICENSE ./LICENSE
COPY ./README.md ./README.md
COPY ./pyproject.toml ./pyproject.toml
COPY ./rust-toolchain.toml ./rust-toolchain.toml
COPY ./chromadb ./chromadb
COPY ./idl ./idl
COPY ./rust ./rust
COPY ./schemas ./schemas

# Generate Protobufs
RUN make -C idl proto_python
RUN --mount=type=cache,sharing=locked,target=/root/.cargo/registry/ \
    --mount=type=cache,sharing=locked,target=/root/.cargo/git/ \
    --mount=type=cache,sharing=locked,target=/chroma/target/ \
    python3 -m maturin build --out /wheels
RUN pip install --prefix="/install" --no-deps /wheels/*.whl

FROM python:3.11-slim-bookworm AS final

# Create working directory
RUN mkdir -p /chroma/chromadb
WORKDIR /chroma

# Copy entrypoint
COPY ./bin/docker_entrypoint.sh /docker_entrypoint.sh

RUN apt-get update --fix-missing && apt-get install -y curl && \
    chmod +x /docker_entrypoint.sh && \
    rm -rf /var/lib/apt/lists/*

# Copy built dependencies and runtime files
COPY --from=builder /install /usr/local
COPY --from=builder /chroma/chromadb/log_config.yml /chroma/chromadb/log_config.yml

ENV CHROMA_HOST_ADDR="0.0.0.0"
ENV CHROMA_HOST_PORT=8000
ENV CHROMA_WORKERS=1
ENV CHROMA_LOG_CONFIG="chromadb/log_config.yml"
ENV CHROMA_TIMEOUT_KEEP_ALIVE=30

EXPOSE 8000

ENTRYPOINT ["/docker_entrypoint.sh"]
CMD [ "--workers ${CHROMA_WORKERS} --host ${CHROMA_HOST_ADDR} --port ${CHROMA_HOST_PORT} --proxy-headers --reload --log-config ${CHROMA_LOG_CONFIG} --timeout-keep-alive ${CHROMA_TIMEOUT_KEEP_ALIVE}"]
