FROM golang:1.20-alpine as build

RUN apk add --no-cache make git build-base bash

ENV PATH=$PATH:/go/bin
ADD ./go/coordinator /src/chroma-coordinator

RUN cd /src/chroma-coordinator \
    && make

FROM alpine:3.17.3

RUN apk add --no-cache bash bash-completion

RUN mkdir /chroma-coordinator
WORKDIR /chroma-coordinator

COPY --from=build /src/chroma-coordinator/bin/chroma /chroma-coordinator/bin/chroma
ENV PATH=$PATH:/chroma-coordinator/bin

RUN chroma completion bash > ~/.bashrc

CMD /bin/bash
