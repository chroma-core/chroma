# Partial registration operator validation

This focused target includes the unchanged production registration and attached-function operator source under their actual module paths. It uses the actual hierarchy type bridge, collection flush type, registration input, test metadata authority and in-memory log. It checks the operator call path and metadata-before-log error ordering. It does not compile the worker orchestrator wrapper or compaction context, activate cross-host transport, or establish production atomicity, authoritative attempt terminality or monotonic log repair. The full worker build remains blocked by the recorded usearch-enabled dependency mismatch.

The proposed command is `cargo test --locked --offline -j 2 -p chroma-index --test hierarchical_register_operator hierarchical_registration_tests -- --nocapture`, with the pinned Rust 1.92 toolchain, debug symbols disabled and the existing target directory. The two methods remain unexecuted until source review.
