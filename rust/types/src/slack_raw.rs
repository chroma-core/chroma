//! Shared spec for the `slack_raw` append-log collection.
//!
//! Real-time Slack messages are written as raw, single records to
//! `slack_raw` (an append log). Record **metadata** (channel/team/thread/op)
//! is inverted-indexed so records are filterable at read time, while the
//! text/vector indexes stay disabled: batching, rendering, and embedding are
//! deferred to a downstream attached function rather than happening at
//! ingest. Foundation `/init` creates this collection and wires it as the
//! attached function's base input.
//!
//! This lives in `chroma-types` (the low-level crate both Foundation and
//! hosted-chroma's sync service already depend on) so the collection name and
//! schema are a single source of truth shared across repos — the sync ingest
//! endpoint can create `slack_raw` identically to `/init` without depending on
//! the heavier `foundation-api` crate. Per-record metadata is the producer's
//! responsibility (written at ingest time by sync), not part of this spec.

use crate::{
    BoolInvertedIndexConfig, FloatInvertedIndexConfig, IntInvertedIndexConfig, Schema,
    SchemaBuilderError, StringInvertedIndexConfig,
};

/// Name of the raw Slack append-log collection.
pub const SLACK_RAW_COLLECTION_NAME: &str = "slack_raw";

/// Schema for the `slack_raw` collection: a metadata-indexed hybrid.
///
/// The four scalar metadata inverted indexes (string, int, float, bool) are
/// enabled so records can be filtered by their metadata (channel, team,
/// thread, op, …). Everything text/vector stays disabled — no FTS on
/// `#document`, no dense `#embedding` index (and no embedding function or
/// pinned dimension), no sparse vector index. Documents are stored verbatim;
/// rendering and embedding happen downstream in the attached function.
///
/// # Errors
///
/// Propagates [`SchemaBuilderError`] if the schema builder rejects an index
/// config. The configs here are static, so this is not expected in practice,
/// but callers sit on request paths and should surface the error rather than
/// crash.
pub fn slack_raw_schema() -> Result<Schema, SchemaBuilderError> {
    Schema::new_record_only()
        .create_index(None, StringInvertedIndexConfig {}.into())?
        .create_index(None, IntInvertedIndexConfig {}.into())?
        .create_index(None, FloatInvertedIndexConfig {}.into())?
        .create_index(None, BoolInvertedIndexConfig {}.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DOCUMENT_KEY;

    #[test]
    fn slack_raw_collection_name_is_slack_raw() {
        assert_eq!(SLACK_RAW_COLLECTION_NAME, "slack_raw");
    }

    #[test]
    fn slack_raw_schema_indexes_metadata_not_text() {
        let schema = slack_raw_schema().expect("static schema construction must succeed");

        // All four scalar metadata inverted indexes are enabled so records
        // are filterable by channel/team/thread/op metadata.
        let string = schema.defaults.string.as_ref().unwrap();
        assert!(
            string.string_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must index string metadata"
        );
        let int = schema.defaults.int.as_ref().unwrap();
        assert!(
            int.int_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must index int metadata"
        );
        let float = schema.defaults.float.as_ref().unwrap();
        assert!(
            float.float_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must index float metadata"
        );
        let boolean = schema.defaults.boolean.as_ref().unwrap();
        assert!(
            boolean.bool_inverted_index.as_ref().unwrap().enabled,
            "slack_raw must index bool metadata"
        );

        // Text/vector indexing stays deferred to the attached function.
        assert!(
            !schema.is_fts_enabled(),
            "slack_raw must not enable full-text search"
        );
        assert!(
            !schema.is_sparse_index_enabled(),
            "slack_raw must not enable a sparse vector index"
        );
        let float_list = schema.defaults.float_list.as_ref().unwrap();
        assert!(
            !float_list.vector_index.as_ref().unwrap().enabled,
            "slack_raw must not enable a dense vector index"
        );

        // The #document override keeps the raw message text out of both the
        // FTS and string inverted indexes even though metadata strings are
        // indexed via the defaults.
        let document = schema.keys.get(DOCUMENT_KEY).unwrap();
        let doc_string = document.string.as_ref().unwrap();
        assert!(!doc_string.fts_index.as_ref().unwrap().enabled);
        assert!(!doc_string.string_inverted_index.as_ref().unwrap().enabled);
    }
}
