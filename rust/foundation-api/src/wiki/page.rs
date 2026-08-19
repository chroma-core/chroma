//! Helpers for reading and writing wiki page chunk metadata.

use crate::wiki::chunking::Chunk;
use crate::wiki::embed::SPARSE_KEY;
use chroma_types::{Metadata, MetadataValue, SparseVector};

/// Slugs that are seeded system pages rather than content/category pages.
const SYSTEM_SLUGS: [&str; 3] = ["", "meta", "categories"];

/// Conservative target for one `source_ids` metadata value, leaving headroom
/// below Chroma Cloud's 4 KiB limit.
pub(crate) const MAX_SOURCE_IDS_VALUE_BYTES: usize = 3 * 1024;

/// Failures while assigning page-level metadata to record chunks.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PageMetadataError {
    /// One source ID cannot fit in a metadata value by itself.
    #[error("source ID is {bytes} bytes; maximum per chunk is {limit} bytes")]
    SourceIdTooLarge { bytes: usize, limit: usize },
    /// The page does not have enough chunks for all bounded source-ID arrays.
    #[error("source IDs require {required} chunks, but the page has {available}")]
    InsufficientChunks { required: usize, available: usize },
}

/// The page `kind` stamped on every chunk.
pub(crate) fn kind_for(slug: &str) -> &'static str {
    if SYSTEM_SLUGS.contains(&slug) {
        "system"
    } else if slug.starts_with("category:") {
        "category"
    } else {
        "page"
    }
}

/// Builds the per-chunk metadata: the always-on fields plus the sparse vector.
/// Categories are stamped on every chunk, while source IDs are packed into
/// consecutive chunks so each `source_ids` value stays within Chroma's limit.
///
/// # Errors
///
/// Returns [`PageMetadataError`] if one source ID exceeds the value target or
/// the page has too few chunks to store all source IDs.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_metadatas(
    slug: &str,
    chunks: &[Chunk],
    sparse: Vec<SparseVector>,
    kind: &str,
    title: &str,
    created_at: i64,
    updated_at: i64,
    version: i64,
    categories: &[String],
    source_ids: &[String],
    author: Option<&str>,
    last_written_by: &str,
) -> Result<Vec<Metadata>, PageMetadataError> {
    let source_ids_by_chunk = distribute_source_ids(source_ids, chunks.len())?;
    Ok(chunks
        .iter()
        .zip(sparse)
        .zip(source_ids_by_chunk)
        .map(|((chunk, sparse_vec), chunk_source_ids)| {
            let mut meta = Metadata::new();
            meta.insert("slug".to_string(), MetadataValue::Str(slug.to_string()));
            meta.insert(
                "chunk_id".to_string(),
                MetadataValue::Int(chunk.chunk_id as i64),
            );
            meta.insert(
                "line_no".to_string(),
                MetadataValue::Int(chunk.line_no as i64),
            );
            meta.insert("kind".to_string(), MetadataValue::Str(kind.to_string()));
            meta.insert("title".to_string(), MetadataValue::Str(title.to_string()));
            meta.insert("created_at".to_string(), MetadataValue::Int(created_at));
            meta.insert("updated_at".to_string(), MetadataValue::Int(updated_at));
            meta.insert("version".to_string(), MetadataValue::Int(version));
            meta.insert(
                "last_written_by".to_string(),
                MetadataValue::Str(last_written_by.to_string()),
            );
            meta.insert(
                SPARSE_KEY.to_string(),
                MetadataValue::SparseVector(sparse_vec),
            );
            if !categories.is_empty() {
                meta.insert(
                    "categories".to_string(),
                    MetadataValue::StringArray(categories.to_vec()),
                );
            }
            if !chunk_source_ids.is_empty() {
                meta.insert(
                    "source_ids".to_string(),
                    MetadataValue::StringArray(chunk_source_ids),
                );
            }
            if let Some(author) = author {
                meta.insert("author".to_string(), MetadataValue::Str(author.to_string()));
            }
            meta
        })
        .collect())
}

/// Greedily packs source IDs in input order. Chroma measures a string-array
/// metadata value as the sum of its strings' UTF-8 byte lengths.
fn distribute_source_ids(
    source_ids: &[String],
    num_chunks: usize,
) -> Result<Vec<Vec<String>>, PageMetadataError> {
    let mut distributed = Vec::new();
    let mut current_chunk = Vec::new();
    let mut chunk_bytes = 0;

    for source_id in source_ids {
        let source_id_bytes = source_id.len();
        if source_id_bytes > MAX_SOURCE_IDS_VALUE_BYTES {
            return Err(PageMetadataError::SourceIdTooLarge {
                bytes: source_id_bytes,
                limit: MAX_SOURCE_IDS_VALUE_BYTES,
            });
        }
        if !current_chunk.is_empty() && chunk_bytes + source_id_bytes > MAX_SOURCE_IDS_VALUE_BYTES {
            distributed.push(current_chunk);
            current_chunk = Vec::new();
            chunk_bytes = 0;
        }
        current_chunk.push(source_id.clone());
        chunk_bytes += source_id_bytes;
    }
    if !current_chunk.is_empty() {
        distributed.push(current_chunk);
    }

    if distributed.len() > num_chunks {
        return Err(PageMetadataError::InsufficientChunks {
            required: distributed.len(),
            available: num_chunks,
        });
    }
    distributed.resize_with(num_chunks, Vec::new);

    Ok(distributed)
}

/// Reads a string-valued metadata field, or `None` if it is absent or a
/// different type.
pub(crate) fn meta_str(meta: &Metadata, key: &str) -> Option<String> {
    match meta.get(key) {
        Some(MetadataValue::Str(value)) => Some(value.clone()),
        _ => None,
    }
}

/// Reads an integer-valued metadata field, or `None` if it is absent or a
/// different type.
pub(crate) fn meta_int(meta: &Metadata, key: &str) -> Option<i64> {
    match meta.get(key) {
        Some(MetadataValue::Int(value)) => Some(*value),
        _ => None,
    }
}

/// Reads a string-array metadata field, or an empty `Vec` if it is absent or a
/// different type.
pub(crate) fn meta_str_array(meta: &Metadata, key: &str) -> Vec<String> {
    match meta.get(key) {
        Some(MetadataValue::StringArray(values)) => values.clone(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wiki::chunking::ChunkRecordId;

    fn chunk(chunk_id: usize, line_no: usize, text: &str) -> Chunk {
        Chunk {
            id: ChunkRecordId::new("foo", chunk_id).to_string(),
            slug: "foo".to_string(),
            chunk_id,
            line_no,
            text: text.to_string(),
        }
    }

    fn sparse(index: u32) -> SparseVector {
        SparseVector::new(vec![index], vec![1.0]).unwrap()
    }

    #[test]
    fn kind_for_classifies_system_category_and_page() {
        assert_eq!(kind_for(""), "system");
        assert_eq!(kind_for("meta"), "system");
        assert_eq!(kind_for("categories"), "system");
        assert_eq!(kind_for("category:archive"), "category");
        assert_eq!(kind_for("getting-started"), "page");
    }

    #[test]
    fn build_metadatas_stamps_all_fields_and_sparse_vector() {
        let chunks = vec![chunk(0, 0, "Title"), chunk(1, 2, "Body")];
        let metas = build_metadatas(
            "foo",
            &chunks,
            vec![sparse(1), sparse(2)],
            "page",
            "Title",
            10,
            20,
            3,
            &["a".to_string()],
            &["slack_master:abc".to_string()],
            Some("Claude Sonnet 4.5"),
            "00000000-0000-0000-0000-000000000001",
        )
        .expect("metadata should fit");

        assert_eq!(metas.len(), 2);
        let first = &metas[0];
        assert_eq!(first.get("slug"), Some(&MetadataValue::Str("foo".into())));
        assert_eq!(first.get("chunk_id"), Some(&MetadataValue::Int(0)));
        assert_eq!(first.get("line_no"), Some(&MetadataValue::Int(0)));
        assert_eq!(first.get("kind"), Some(&MetadataValue::Str("page".into())));
        assert_eq!(
            first.get("title"),
            Some(&MetadataValue::Str("Title".into()))
        );
        assert_eq!(first.get("created_at"), Some(&MetadataValue::Int(10)));
        assert_eq!(first.get("updated_at"), Some(&MetadataValue::Int(20)));
        assert_eq!(first.get("version"), Some(&MetadataValue::Int(3)));
        assert_eq!(
            first.get("last_written_by"),
            Some(&MetadataValue::Str(
                "00000000-0000-0000-0000-000000000001".to_string()
            ))
        );
        assert_eq!(
            first.get("author"),
            Some(&MetadataValue::Str("Claude Sonnet 4.5".to_string()))
        );
        assert_eq!(
            first.get("categories"),
            Some(&MetadataValue::StringArray(vec!["a".to_string()]))
        );
        assert_eq!(
            first.get("source_ids"),
            Some(&MetadataValue::StringArray(vec![
                "slack_master:abc".to_string()
            ]))
        );
        assert!(!metas[1].contains_key("source_ids"));
        assert!(matches!(
            metas[1].get(SPARSE_KEY),
            Some(MetadataValue::SparseVector(_))
        ));
    }

    #[test]
    fn build_metadatas_distributes_source_ids_within_value_limit() {
        let chunks = vec![
            chunk(0, 0, "Title"),
            chunk(1, 2, "Body"),
            chunk(2, 3, "More body"),
        ];
        let source_ids = vec!["a".repeat(1536), "b".repeat(1536), "é".repeat(1536)];
        let metas = build_metadatas(
            "foo",
            &chunks,
            vec![sparse(1), sparse(2), sparse(3)],
            "page",
            "Title",
            10,
            20,
            3,
            &[],
            &source_ids,
            None,
            "00000000-0000-0000-0000-000000000001",
        )
        .expect("metadata should fit");

        let distributed: Vec<Vec<String>> = metas
            .iter()
            .filter_map(|meta| match meta.get("source_ids") {
                Some(MetadataValue::StringArray(values)) => Some(values.clone()),
                _ => None,
            })
            .collect();

        assert_eq!(
            distributed,
            vec![source_ids[..2].to_vec(), source_ids[2..].to_vec()]
        );
        assert!(distributed.iter().all(|values| {
            values.iter().map(String::len).sum::<usize>() <= MAX_SOURCE_IDS_VALUE_BYTES
        }));
        assert!(!metas[2].contains_key("source_ids"));
    }

    #[test]
    fn distribute_source_ids_rejects_an_individually_oversized_id() {
        let oversized = "a".repeat(MAX_SOURCE_IDS_VALUE_BYTES + 1);

        let err = distribute_source_ids(&[oversized], 2).expect_err("source ID should not fit");

        assert_eq!(
            err,
            PageMetadataError::SourceIdTooLarge {
                bytes: MAX_SOURCE_IDS_VALUE_BYTES + 1,
                limit: MAX_SOURCE_IDS_VALUE_BYTES,
            }
        );
    }

    #[test]
    fn distribute_source_ids_rejects_too_few_chunks() {
        let source_ids = vec!["a".repeat(MAX_SOURCE_IDS_VALUE_BYTES), "b".to_string()];

        let err = distribute_source_ids(&source_ids, 1).expect_err("two chunks should be required");

        assert_eq!(
            err,
            PageMetadataError::InsufficientChunks {
                required: 2,
                available: 1,
            }
        );
    }

    #[test]
    fn build_metadatas_omits_empty_categories_and_source_ids() {
        let chunks = vec![chunk(0, 0, "Title")];
        let metas = build_metadatas(
            "foo",
            &chunks,
            vec![sparse(1)],
            "page",
            "Title",
            10,
            20,
            1,
            &[],
            &[],
            None,
            "00000000-0000-0000-0000-000000000001",
        )
        .expect("metadata should fit");

        assert!(!metas[0].contains_key("categories"));
        assert!(!metas[0].contains_key("source_ids"));
        assert!(!metas[0].contains_key("author"));
        assert!(metas[0].contains_key(SPARSE_KEY));
    }
}
