//! Schema and embedding-function builders for Foundation collections.
//!
//! Extracted from `init` so the bootstrap handler stays focused on
//! orchestration. Everything here is pure (no I/O): it describes the dense
//! (Qwen) + sparse (SPLADE) indices every Foundation collection is created
//! with, and the known embedding functions that back them.

use chroma_types::{
    EmbeddingFunctionConfiguration, EmbeddingFunctionNewConfiguration, IndexConfig, KnnIndex,
    Schema, SparseIndexAlgorithm, SparseVectorIndexConfig, DOCUMENT_KEY,
};

/// The Chroma Cloud Qwen3-Embedding-0.6B known embedding function,
/// serialized exactly as the `chroma-cloud-qwen` embedding function expects
/// (see `schemas/embedding_functions/chroma-cloud-qwen.json` and the
/// Python/Rust implementations). This is the dense model Foundation uses by
/// default; the wiki collection is 1024-dimensional to match it.
pub(super) fn qwen_embedding_function() -> EmbeddingFunctionConfiguration {
    EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
        name: "chroma-cloud-qwen".to_string(),
        config: serde_json::json!({
            "api_key_env_var": "CHROMA_API_KEY",
            "model": "Qwen/Qwen3-Embedding-0.6B",
            // `generic_retrieval` is the general-knowledge task the
            // chroma sync pipeline uses by default. It must be set (not null)
            // for the instruction below to actually be applied at query time.
            "task": "generic_retrieval",
            "instructions": {
                "generic_retrieval": {
                    "documents": "",
                    "query": "Retrieve semantically similar text",
                }
            },
        }),
    })
}

/// The Chroma Cloud SPLADE sparse embedding function
pub(super) fn splade_embedding_function() -> EmbeddingFunctionConfiguration {
    EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
        name: "chroma-cloud-splade".to_string(),
        config: serde_json::json!({
            "model": "prithivida/Splade_PP_en_v1",
            "api_key_env_var": "CHROMA_API_KEY",
        }),
    })
}

/// Dense + sparse embedding functions to register on a Foundation
/// collection. A supplied function makes Chroma auto-embed that modality
/// from the document server-side; `None` leaves the corresponding index
/// EF-less (the writer supplies vectors).
#[derive(Default)]
pub(super) struct CollectionEmbeddingFunctions {
    pub(super) dense: Option<EmbeddingFunctionConfiguration>,
    pub(super) sparse: Option<EmbeddingFunctionConfiguration>,
}

/// Build the [`Schema`] used for Foundation collections. Adds a
/// `sparse_embedding` sparse vector index for SPLADE.
///
/// The dense function is set on the dense vector index (defaults +
/// `#embedding`); the sparse function is set on the sparse index. Mirrors
/// the hosted-chroma file-upload `build_collection_schema`.
pub(super) fn foundation_collection_schema(
    embedding_functions: CollectionEmbeddingFunctions,
) -> Schema {
    let CollectionEmbeddingFunctions { dense, sparse } = embedding_functions;
    // Both branches default the dense vector index to SPANN — what the
    // distributed frontend uses by default — and the planner is also given
    // `KnnIndex::Spann` in `ensure_collection`. When an embedding function
    // is supplied, `default_with_embedding_function` is the schema-native
    // way to set it on both the schema defaults and the `#embedding` key.
    let base = match dense {
        Some(dense) => Schema::default_with_embedding_function(dense),
        None => Schema::new_default(KnnIndex::Spann),
    };
    // Auto-embed sparse vectors from the document only when a sparse EF is
    // supplied; otherwise leave `source_key` unset alongside the EF.
    let sparse_source_key = sparse.as_ref().map(|_| DOCUMENT_KEY.to_string());
    base.create_index(
        Some("sparse_embedding"),
        IndexConfig::SparseVector(SparseVectorIndexConfig {
            embedding_function: sparse,
            source_key: sparse_source_key,
            bm25: Some(false),
            // TODO: Change this to MaxScore
            algorithm: SparseIndexAlgorithm::Wand,
        }),
    )
    .expect("static schema construction should never fail")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_types::SegmentType;
    use frontend_core::collection_ops::{
        plan_create_collection, supported_segment_types, ExecutorKind, TenantFeatureFlags,
    };

    #[test]
    fn foundation_schema_has_sparse_vector_index() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        assert!(
            schema.is_sparse_index_enabled(),
            "schema must have a sparse vector index for SPLADE embeddings"
        );
    }

    #[test]
    fn foundation_schema_sparse_key_is_sparse_embedding() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        let sparse_vt = schema
            .keys
            .get("sparse_embedding")
            .expect("schema must have a 'sparse_embedding' key override");
        let idx = sparse_vt
            .sparse_vector
            .as_ref()
            .and_then(|sv| sv.sparse_vector_index.as_ref())
            .expect("'sparse_embedding' key must have a sparse_vector_index");
        assert!(idx.enabled, "sparse_vector_index must be enabled");
        assert_eq!(idx.config.bm25, Some(false));
    }

    #[test]
    fn foundation_plan_produces_schema_and_segments() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        let plan = plan_create_collection(
            None,
            Some(schema),
            ExecutorKind::Distributed,
            &supported_segment_types(ExecutorKind::Distributed),
            true,
            KnnIndex::Spann,
            TenantFeatureFlags::default(),
        )
        .expect("planning with foundation schema must succeed");

        assert!(
            plan.schema.is_some(),
            "plan must carry a reconciled schema when enable_schema=true"
        );
        assert!(
            !plan.segments.is_empty(),
            "plan must produce at least one segment"
        );
        let reconciled = plan.schema.as_ref().unwrap();
        assert!(
            reconciled.is_sparse_index_enabled(),
            "reconciled schema must preserve the sparse vector index"
        );
        assert!(
            plan.segments
                .iter()
                .any(|s| s.r#type == SegmentType::Spann || s.r#type == SegmentType::QuantizedSpann),
            "plan must include a SPANN vector segment, got: {:?}",
            plan.segments.iter().map(|s| &s.r#type).collect::<Vec<_>>()
        );
    }

    #[test]
    fn qwen_embedding_function_matches_known_serialization() {
        let EmbeddingFunctionConfiguration::Known(known) = qwen_embedding_function() else {
            panic!("Qwen embedding function must be a known embedding function");
        };
        assert_eq!(known.name, "chroma-cloud-qwen");
        assert_eq!(
            known.config,
            serde_json::json!({
                "api_key_env_var": "CHROMA_API_KEY",
                "model": "Qwen/Qwen3-Embedding-0.6B",
                "task": "generic_retrieval",
                "instructions": {
                    "generic_retrieval": {
                        "documents": "",
                        "query": "Retrieve semantically similar text",
                    }
                },
            })
        );
    }

    #[test]
    fn splade_embedding_function_matches_file_upload_serialization() {
        let EmbeddingFunctionConfiguration::Known(known) = splade_embedding_function() else {
            panic!("SPLADE embedding function must be a known embedding function");
        };
        assert_eq!(known.name, "chroma-cloud-splade");
        assert_eq!(
            known.config,
            serde_json::json!({
                "model": "prithivida/Splade_PP_en_v1",
                "api_key_env_var": "CHROMA_API_KEY",
            })
        );
    }

    #[test]
    fn auto_embed_schema_sets_splade_sparse_function() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions {
            dense: Some(qwen_embedding_function()),
            sparse: Some(splade_embedding_function()),
        });
        let sparse_idx = schema
            .keys
            .get("sparse_embedding")
            .and_then(|vt| vt.sparse_vector.as_ref())
            .and_then(|sv| sv.sparse_vector_index.as_ref())
            .expect("auto-embed schema must have a sparse_embedding index");
        assert_eq!(
            sparse_idx.config.embedding_function,
            Some(splade_embedding_function())
        );
        assert_eq!(sparse_idx.config.source_key, Some("#document".to_string()));
    }

    #[test]
    fn no_dense_ef_leaves_sparse_function_unset() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        let sparse_idx = schema
            .keys
            .get("sparse_embedding")
            .and_then(|vt| vt.sparse_vector.as_ref())
            .and_then(|sv| sv.sparse_vector_index.as_ref())
            .expect("schema must have a sparse_embedding index");
        assert_eq!(sparse_idx.config.embedding_function, None);
        assert_eq!(sparse_idx.config.source_key, None);
    }

    #[test]
    fn foundation_schema_sets_dense_embedding_function() {
        let ef = qwen_embedding_function();
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions {
            dense: Some(ef.clone()),
            sparse: Some(splade_embedding_function()),
        });

        let defaults_ef = schema
            .defaults
            .float_list
            .as_ref()
            .and_then(|fl| fl.vector_index.as_ref())
            .expect("schema defaults must carry a dense vector index")
            .config
            .embedding_function
            .clone();
        assert_eq!(defaults_ef, Some(ef.clone()));

        let embedding_ef = schema
            .keys
            .get("#embedding")
            .and_then(|vt| vt.float_list.as_ref())
            .and_then(|fl| fl.vector_index.as_ref())
            .expect("#embedding key must carry a dense vector index")
            .config
            .embedding_function
            .clone();
        assert_eq!(embedding_ef, Some(ef));
    }

    #[test]
    fn foundation_schema_without_embedding_function_leaves_it_unset() {
        let schema = foundation_collection_schema(CollectionEmbeddingFunctions::default());
        let defaults_ef = schema
            .defaults
            .float_list
            .as_ref()
            .and_then(|fl| fl.vector_index.as_ref())
            .expect("schema defaults must carry a dense vector index")
            .config
            .embedding_function
            .clone();
        assert_eq!(defaults_ef, None);
    }
}
