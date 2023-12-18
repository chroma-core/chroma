FROM rust:1.74.1 as builder

WORKDIR /
RUN git clone https://github.com/chroma-core/hnswlib.git

WORKDIR /chroma/
COPY . .

ENV PROTOC_ZIP=protoc-25.1-linux-x86_64.zip
RUN curl -OL https://github.com/protocolbuffers/protobuf/releases/download/v25.1/$PROTOC_ZIP \
    && unzip -o $PROTOC_ZIP -d /usr/local bin/protoc \
    && unzip -o $PROTOC_ZIP -d /usr/local 'include/*' \
    && rm -f $PROTOC_ZIP

RUN cargo build

WORKDIR /chroma/rust/worker

CMD ["cargo", "run"]
