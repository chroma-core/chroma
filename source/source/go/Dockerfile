FROM golang:bookworm AS builder
ARG PROTOC_VERSION=31.1
WORKDIR /build-dir
RUN apt-get update && apt-get install -y make git bash protobuf-compiler unzip curl

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

# Set the PATH to include Go binaries
ENV PATH=$PATH:/root/go/bin

# Install protoc Go plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

ENV GOCACHE=/root/.cache/go-build

ADD ./go /build-dir/go
ADD ./idl /build-dir/idl

WORKDIR /build-dir/go

# Generate protobuf files
RUN make proto

# Build the Go binaries
RUN --mount=type=cache,target="/root/.cache/go-build" --mount=type=cache,target="/go/pkg/mod" make build

FROM debian:bookworm-slim AS runner
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

FROM runner AS sysdb
COPY --from=builder /build-dir/go/bin/coordinator .
ENV PATH=$PATH:./
CMD /bin/bash
