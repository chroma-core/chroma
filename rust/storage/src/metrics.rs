//! Metrics for storage operations.

use opentelemetry::metrics::{Counter, Histogram};

/// Metrics for tracking S3 and object storage operations.
///
/// All metrics are registered under the `chroma.storage` meter.
#[derive(Clone)]
pub(crate) struct StorageMetrics {
    /// Number of S3 get operations.
    pub(crate) s3_get_count: Counter<u64>,
    /// Number of S3 put operations.
    pub(crate) s3_put_count: Counter<u64>,
    /// Number of S3 delete operations.
    pub(crate) s3_delete_count: Counter<u64>,
    /// Number of keys deleted via batch delete operations.
    pub(crate) s3_delete_many_count: Counter<u64>,
    /// Latency of S3 get operations in milliseconds.
    pub(crate) s3_get_latency_ms: Histogram<u64>,
    /// Latency of S3 put operations in milliseconds.
    pub(crate) s3_put_latency_ms: Histogram<u64>,
    /// Bytes written per S3 put operation.
    pub(crate) s3_put_bytes: Histogram<u64>,
    /// Bytes written per S3 put operation that took more than 1 second.
    pub(crate) s3_put_bytes_slow: Histogram<u64>,
    /// Number of parts in multipart uploads.
    pub(crate) s3_multipart_upload_parts: Histogram<u64>,
    /// Bytes per upload part in multipart uploads.
    pub(crate) s3_upload_part_bytes: Histogram<u64>,
    /// Number of failed S3 put operations.
    pub(crate) s3_put_error_count: Counter<u64>,
    /// Number of S3 copy operations.
    pub(crate) s3_copy_count: Counter<u64>,
    /// Latency of S3 copy operations in milliseconds.
    pub(crate) s3_copy_latency_ms: Histogram<u64>,
    /// Number of S3 rename operations.
    pub(crate) s3_rename_count: Counter<u64>,
    /// Latency of S3 rename operations in milliseconds.
    pub(crate) s3_rename_latency_ms: Histogram<u64>,
    /// Number of S3 list operations.
    pub(crate) s3_list_count: Counter<u64>,
    /// Latency of S3 list operations in milliseconds.
    pub(crate) s3_list_latency_ms: Histogram<u64>,
}

impl Default for StorageMetrics {
    fn default() -> Self {
        Self {
            s3_get_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_get_count")
                .with_description("Number of S3 get operations")
                .build(),
            s3_put_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_put_count")
                .with_description("Number of S3 put operations")
                .build(),
            s3_delete_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_delete_count")
                .with_description("Number of S3 delete operations")
                .build(),
            s3_delete_many_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_delete_many_count")
                .with_description("Number of S3 delete many operations")
                .build(),
            s3_get_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_get_latency_ms")
                .with_description("Latency of S3 get operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_put_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_put_latency_ms")
                .with_description("Latency of S3 put operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_put_bytes: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_put_bytes")
                .with_description("Bytes written per S3 put operation")
                .with_unit("bytes")
                .build(),
            s3_put_bytes_slow: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_put_bytes_slow")
                .with_description("Bytes written per S3 put operation that took more than 1 second")
                .with_unit("bytes")
                .build(),
            s3_multipart_upload_parts: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_multipart_upload_parts")
                .with_description("Number of parts in multipart uploads")
                .build(),
            s3_upload_part_bytes: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_upload_part_bytes")
                .with_description("Bytes per upload part in multipart uploads")
                .with_unit("bytes")
                .build(),
            s3_put_error_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_put_error_count")
                .with_description("Number of failed S3 put operations")
                .build(),
            s3_copy_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_copy_count")
                .with_description("Number of S3 copy operations")
                .build(),
            s3_copy_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_copy_latency_ms")
                .with_description("Latency of S3 copy operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_rename_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_rename_count")
                .with_description("Number of S3 rename operations")
                .build(),
            s3_rename_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_rename_latency_ms")
                .with_description("Latency of S3 rename operations in milliseconds")
                .with_unit("ms")
                .build(),
            s3_list_count: opentelemetry::global::meter("chroma.storage")
                .u64_counter("s3_list_count")
                .with_description("Number of S3 list operations")
                .build(),
            s3_list_latency_ms: opentelemetry::global::meter("chroma.storage")
                .u64_histogram("s3_list_latency_ms")
                .with_description("Latency of S3 list operations in milliseconds")
                .with_unit("ms")
                .build(),
        }
    }
}
