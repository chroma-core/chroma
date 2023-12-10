FROM golang:1.20-alpine3.18 as build

RUN apk add --no-cache make git build-base bash

ENV PATH=$PATH:/go/bin
ADD ./go/coordinator /src/chroma-coordinator

RUN cd /src/chroma-coordinator \
    && make

FROM alpine:3.17.3

RUN apk add --no-cache bash bash-completion

# As of 6 Dec 2023, the atlas package isn't in Alpine's main package manager, only
# testing. So we have to add the testing repository to get it.
RUN apk add \
    --no-cache \
    --repository http://dl-cdn.alpinelinux.org/alpine/edge/testing \
    --repository http://dl-cdn.alpinelinux.org/alpine/edge/main \
    atlas

RUN mkdir /chroma-coordinator
WORKDIR /chroma-coordinator

COPY --from=build /src/chroma-coordinator/bin/chroma /chroma-coordinator/bin/chroma
ENV PATH=$PATH:/chroma-coordinator/bin

COPY --from=build /src/chroma-coordinator/migrations /chroma-coordinator/migrations

CMD /bin/bash
