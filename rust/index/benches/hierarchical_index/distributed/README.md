# Sharded hierarchical SPANN

This module splits one hierarchical SPANN index across several processes, called owners, so a corpus too large for one machine can be built and served by many. Every owner holds the whole routing tree, which is small, and the leaves of its own part; a query machine walks the tree, scans its own part, sends one scan request to each other owner whose leaves the walk reached, and merges the results. Experiments and results are in `../sharded_scaling`.

## The pieces

- **The writer** (`construction.rs`, `engine.rs`, the `two_machine_spann` bench): builds and balances the tree across owners, checkpoints it, and saves generations to local disk or S3.
- **The saved reader** (`saved_reader.rs`, `owner_scan.rs`, the `two_machine_reader` bench): opens a saved generation, optionally splits its leaves into parts at open time (`--parts M`), serves scan requests (`--mode serve`) and benchmarks queries (`--mode bench`).
- **The vector store** (`vector_store.rs`): each owner's full vectors in blockfiles, read for exact rerank.
- **The transport** (`transport.rs`): authenticated request/reply over TCP between owners.

## Building and testing

The module compiles only where it is asked for, so the other hierarchical test crates do not build it:

- The `two_machine_spann`, `two_machine_reader` and `two_machine_correctness` benches need `--cfg distributed_owner`, for example `cargo rustc -p chroma-index --release --bench two_machine_reader -- --cfg distributed_owner`.
- Its tests run in the `hierarchical_distributed` test crate behind the `sharded-bench` feature: `cargo test -p chroma-index --features sharded-bench --test hierarchical_distributed`.
- The writer's profile counts work in the shared two-means split kernel only with the `clustering-profile` feature; without it those counts are zero.

## Large replies between owners

Owners stream large replies as ordered chunks over the connection that carries the request. Each chunk carries at most 16 MiB of JSON payload by default. The construction executable accepts `--max-frame-bytes` to configure this ceiling in bytes; configure both owners equally. The same setting bounds each request, which remains a single frame. The limit excludes the four-byte length prefix. Chunking bounds the serialized transport buffer, while the decoded application value still requires memory proportional to the reply.

Small replies retain the ordinary length-prefixed encoding. Large replies start with the reserved `u32::MAX` prefix, carry nonempty length-prefixed chunks, and end with a zero-length marker. The receiver requires complete JSON and the end marker before returning the connection to its pool. Both peers must support this framing. A partial write, missing marker, malformed value, or expired absolute deadline fails the call and discards the connection. The transport does not replay the handler. An explicit codec size rejection remains available only before any reply bytes have been written.

Transport counters count logical messages and all wire bytes, including chunk headers. For streamed replies, serialization and deserialization wall-time scopes include socket waits during streaming; those inclusive scopes do not isolate encoding CPU. The reply tests cover both timing modes, connection reuse, UTF-8 boundaries, truncation, deadlines, serialization failures, and a reply exceeding the default 16 MiB ceiling.
