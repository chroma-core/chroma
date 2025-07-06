FROM golang:bookworm AS builder
WORKDIR /build-dir
RUN apt-get update && apt-get install -y make git bash protobuf-compiler unzip curl

# Install a specific version of protoc
RUN curl -LO https://github.com/protocolbuffers/protobuf/releases/download/v28.2/protoc-28.2-linux-x86_64.zip && \
    unzip protoc-28.2-linux-x86_64.zip -d /usr/local/ && \
    rm protoc-28.2-linux-x86_64.zip && \
    chmod +x /usr/local/bin/protoc

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

FROM runner AS logservice
COPY --from=builder /build-dir/go/bin/logservice .
ENV PATH=$PATH:./
CMD ["./logservice"]

FROM runner AS sysdb
COPY --from=builder /build-dir/go/bin/coordinator .
ENV PATH=$PATH:./
CMD /bin/bash
