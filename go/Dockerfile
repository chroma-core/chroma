FROM golang:bookworm as builder
WORKDIR /build-dir
RUN apt-get update && apt-get install -y make git bash

ADD ./go/go.mod ./go.mod
ADD ./go/go.sum ./go.sum
RUN go mod download

ADD ./go/ ./
ENV GOCACHE=/root/.cache/go-build
RUN --mount=type=cache,target="/root/.cache/go-build" make

FROM debian:bookworm-slim

COPY --from=builder /build-dir/bin/coordinator .
COPY --from=builder /build-dir/bin/logservice .
ENV PATH=$PATH:./

CMD /bin/bash
