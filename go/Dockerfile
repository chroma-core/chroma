FROM golang:bookworm AS builder
WORKDIR /build-dir
RUN apt-get update && apt-get install -y make git bash

ADD ./go/go.mod ./go.mod
ADD ./go/go.sum ./go.sum
RUN go mod download

ADD ./go/ ./
ENV GOCACHE=/root/.cache/go-build
RUN --mount=type=cache,target="/root/.cache/go-build" make

FROM debian:bookworm-slim AS log-service
COPY --from=builder /build-dir/bin/logservice .
ENV PATH=$PATH:./
CMD ["./logservice"]

FROM debian:bookworm-slim AS sysdb
COPY --from=builder /build-dir/bin/coordinator .
ENV PATH=$PATH:./
CMD /bin/bash