FROM rust:1.74.1 as builder
ARG CHROMA_KUBERNETES_INTEGRATION=0
ARG RELEASE_MODE=0
ENV CHROMA_KUBERNETES_INTEGRATION $CHROMA_KUBERNETES_INTEGRATION

WORKDIR /
RUN git clone https://github.com/chroma-core/hnswlib.git

WORKDIR /chroma/

ENV PROTOC_ZIP=protoc-25.1-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP

COPY . .

ENV CARGO_TARGET_DIR=/root/.cache/cargo
RUN --mount=type=cache,target=/root/.cache/cargo if [ "$RELEASE_MODE" = "1" ]; then cargo build --release; else cargo build; fi

WORKDIR /chroma/rust/worker

CMD ["cargo", "run"]
