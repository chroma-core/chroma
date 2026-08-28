//! Logical compaction workload facts used by attached-function planners.

use crate::chroma_proto;

/// Version emitted for function workload descriptors produced by this build.
pub const FUNCTION_WORKLOAD_FORMAT_VERSION: u32 = 1;

/// Logical workload facts for one compacted collection window.
///
/// These values describe data shape rather than a memory prediction. Function
/// consumers can combine consecutive descriptors and apply an estimator that
/// matches the function they are about to execute.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FunctionWorkload {
    /// Schema version for interpreting the remaining fields.
    pub format_version: u32,
    /// Number of source WAL records read for the window.
    pub source_log_records: u64,
    /// Logical bytes in the source WAL records.
    pub source_log_bytes: u64,
    /// Number of records remaining after log materialization.
    pub materialized_records: u64,
    /// Number of materialized records that are not deletes.
    pub non_delete_records: u64,
    /// Bytes in non-delete record identifiers.
    pub id_bytes: u64,
    /// Bytes in non-delete record documents.
    pub document_bytes: u64,
    /// Logical bytes in non-delete record metadata.
    pub metadata_bytes: u64,
    /// Bytes in non-delete record embeddings.
    pub embedding_bytes: u64,
    /// Number of metadata entries in non-delete records.
    pub metadata_entries: u64,
    /// Largest identifier, document, and metadata total for one non-delete record.
    pub max_non_embedding_record_bytes: u64,
}

impl FunctionWorkload {
    /// Returns an empty descriptor using the current format.
    pub fn current() -> Self {
        Self {
            format_version: FUNCTION_WORKLOAD_FORMAT_VERSION,
            ..Default::default()
        }
    }

    /// Returns whether this descriptor is understood by this build.
    pub fn is_supported(&self) -> bool {
        self.format_version == FUNCTION_WORKLOAD_FORMAT_VERSION
    }

    /// Merges another consecutive compaction window into this descriptor.
    pub fn merge(&mut self, other: &Self) {
        if self.format_version != other.format_version {
            self.format_version = 0;
        }
        self.source_log_records = self
            .source_log_records
            .saturating_add(other.source_log_records);
        self.source_log_bytes = self.source_log_bytes.saturating_add(other.source_log_bytes);
        self.materialized_records = self
            .materialized_records
            .saturating_add(other.materialized_records);
        self.non_delete_records = self
            .non_delete_records
            .saturating_add(other.non_delete_records);
        self.id_bytes = self.id_bytes.saturating_add(other.id_bytes);
        self.document_bytes = self.document_bytes.saturating_add(other.document_bytes);
        self.metadata_bytes = self.metadata_bytes.saturating_add(other.metadata_bytes);
        self.embedding_bytes = self.embedding_bytes.saturating_add(other.embedding_bytes);
        self.metadata_entries = self.metadata_entries.saturating_add(other.metadata_entries);
        self.max_non_embedding_record_bytes = self
            .max_non_embedding_record_bytes
            .max(other.max_non_embedding_record_bytes);
    }
}

impl From<FunctionWorkload> for chroma_proto::FunctionWorkload {
    fn from(value: FunctionWorkload) -> Self {
        Self {
            format_version: value.format_version,
            source_log_records: value.source_log_records,
            source_log_bytes: value.source_log_bytes,
            materialized_records: value.materialized_records,
            non_delete_records: value.non_delete_records,
            id_bytes: value.id_bytes,
            document_bytes: value.document_bytes,
            metadata_bytes: value.metadata_bytes,
            embedding_bytes: value.embedding_bytes,
            metadata_entries: value.metadata_entries,
            max_non_embedding_record_bytes: value.max_non_embedding_record_bytes,
        }
    }
}

impl From<chroma_proto::FunctionWorkload> for FunctionWorkload {
    fn from(value: chroma_proto::FunctionWorkload) -> Self {
        Self {
            format_version: value.format_version,
            source_log_records: value.source_log_records,
            source_log_bytes: value.source_log_bytes,
            materialized_records: value.materialized_records,
            non_delete_records: value.non_delete_records,
            id_bytes: value.id_bytes,
            document_bytes: value.document_bytes,
            metadata_bytes: value.metadata_bytes,
            embedding_bytes: value.embedding_bytes,
            metadata_entries: value.metadata_entries,
            max_non_embedding_record_bytes: value.max_non_embedding_record_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_adds_totals_and_keeps_largest_record() {
        let mut workload = FunctionWorkload {
            format_version: 1,
            source_log_records: 2,
            source_log_bytes: 20,
            materialized_records: 2,
            non_delete_records: 1,
            id_bytes: 3,
            document_bytes: 4,
            metadata_bytes: 5,
            embedding_bytes: 6,
            metadata_entries: 1,
            max_non_embedding_record_bytes: 12,
        };
        workload.merge(&FunctionWorkload {
            format_version: 1,
            source_log_records: 3,
            source_log_bytes: 30,
            materialized_records: 2,
            non_delete_records: 2,
            id_bytes: 7,
            document_bytes: 8,
            metadata_bytes: 9,
            embedding_bytes: 10,
            metadata_entries: 2,
            max_non_embedding_record_bytes: 11,
        });

        assert_eq!(
            workload,
            FunctionWorkload {
                format_version: 1,
                source_log_records: 5,
                source_log_bytes: 50,
                materialized_records: 4,
                non_delete_records: 3,
                id_bytes: 10,
                document_bytes: 12,
                metadata_bytes: 14,
                embedding_bytes: 16,
                metadata_entries: 3,
                max_non_embedding_record_bytes: 12,
            }
        );
    }

    #[test]
    fn merge_marks_mixed_formats_unsupported() {
        let mut workload = FunctionWorkload::current();
        workload.merge(&FunctionWorkload {
            format_version: 2,
            ..Default::default()
        });

        assert_eq!(workload.format_version, 0);
        assert!(!workload.is_supported());
    }
}
