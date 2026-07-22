//! Helpers for reading and writing wiki page chunk metadata.

use crate::wiki::chunking::Chunk;
use crate::wiki::embed::SPARSE_KEY;
use chroma_types::{Metadata, MetadataValue, SparseVector};

/// Slugs that are seeded system pages rather than content/category pages.
const SYSTEM_SLUGS: [&str; 3] = ["", "meta", "categories"];

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

/// Builds the per-chunk metadata: the always-on fields plus the sparse vector,
/// with `categories` / `source_ids` stamped on every chunk only when non-empty.
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
    last_written_by: &str,
) -> Vec<Metadata> {
    chunks
        .iter()
        .zip(sparse)
        .map(|(chunk, sparse_vec)| {
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
            if !source_ids.is_empty() {
                meta.insert(
                    "source_ids".to_string(),
                    MetadataValue::StringArray(source_ids.to_vec()),
                );
            }
            meta
        })
        .collect()
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
            "00000000-0000-0000-0000-000000000001",
        );

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
            first.get("categories"),
            Some(&MetadataValue::StringArray(vec!["a".to_string()]))
        );
        assert_eq!(
            first.get("source_ids"),
            Some(&MetadataValue::StringArray(vec![
                "slack_master:abc".to_string()
            ]))
        );
        assert!(matches!(
            metas[1].get(SPARSE_KEY),
            Some(MetadataValue::SparseVector(_))
        ));
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
            "00000000-0000-0000-0000-000000000001",
        );

        assert!(!metas[0].contains_key("categories"));
        assert!(!metas[0].contains_key("source_ids"));
        assert!(metas[0].contains_key(SPARSE_KEY));
    }
}
