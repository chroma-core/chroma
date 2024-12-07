FROM golang:bookworm as builder
WORKDIR /build-dir
RUN apt-get update && apt-get install -y make git bash protobuf-compiler

# Set the PATH to include Go binaries
ENV PATH=$PATH:/root/go/bin

# Copy the Go source code and protobuf files
ADD ./go/ ./go
ADD ./idl/ ./idl
ADD ./go/go.mod ./go/go.mod
ADD ./go/go.sum ./go/go.sum
WORKDIR /build-dir/go
RUN go mod download

# Install protoc Go plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.3.0

ENV GOCACHE=/root/.cache/go-build

WORKDIR /build-dir/go
# Generate protobuf files
# RUN --mount=type=cache,target="/root/.cache/go-build" go generate ./...
# Run 'make proto' to generate protobuf files
RUN --mount=type=cache,target="/root/.cache/go-build" make proto

# Build the Go binaries
RUN --mount=type=cache,target="/root/.cache/go-build" make build

FROM debian:bookworm-slim as sysdb
COPY --from=builder /build-dir/go/bin/coordinator .
ENV PATH=$PATH:./
CMD /bin/bash


FROM debian:bookworm-slim as logservice
WORKDIR /app
COPY --from=builder /build-dir/go/bin/logservice .
CMD ["./logservice"]
