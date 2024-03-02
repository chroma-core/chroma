FROM golang:1.20-alpine3.18 as build
WORKDIR /src/chroma-coordinator
RUN apk add --no-cache make git build-base bash

ADD ./go/go.mod ./go.mod
ADD ./go/go.sum ./go.sum
ENV PATH=$PATH:/go/bin
RUN go mod download

ADD ./go/ ./
ENV GOCACHE=/root/.cache/go-build
RUN --mount=type=cache,target="/root/.cache/go-build" make

FROM alpine:3.17.3

RUN apk add --no-cache bash bash-completion jq findutils

# As of 6 Dec 2023, the atlas package isn't in Alpine's main package manager, only
# testing. So we have to add the testing repository to get it.
RUN apk add \
    --no-cache \
    --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing \
    --repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
    atlas

RUN mkdir /chroma-coordinator
WORKDIR /chroma-coordinator

COPY --from=build /src/chroma-coordinator/bin/coordinator /chroma-coordinator/bin/coordinator
COPY --from=build /src/chroma-coordinator/bin/logservice /chroma-coordinator/bin/logservice
ENV PATH=$PATH:/chroma-coordinator/bin

CMD /bin/bash
