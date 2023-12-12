FROM rust:1.74.1 as builder

WORKDIR /chroma
COPY . .

RUN cargo build

# For now this runs cargo test since we have no main binary
CMD ["cargo", "test"]
