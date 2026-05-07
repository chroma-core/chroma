//! Shared library logic for collection lifecycle operations (currently:
//! create-collection planning). Both `chroma-frontend`'s HTTP CRUD handlers
//! and `foundation-api`'s init handler call into this module so the rules
//! for how a collection is laid out (segment dispatch, schema reconciliation,
//! tenant feature flags) live in exactly one place.
//!
//! The planner is a pure function — no async, no I/O, no caller-specific
//! state. Callers compute their own inputs (executor kind, supported segment
//! types, tenant flags) and hand the returned [`CreateCollectionPlan`] to
//! `SysDb::create_collection`.

use std::collections::HashSet;

use chroma_types::{
    CollectionUuid, CreateCollectionError, FtsAlgorithm, InternalCollectionConfiguration, KnnIndex,
    Quantization, Schema, SchemaError, Segment, SegmentScope, SegmentType, SegmentUuid,
    SparseIndexAlgorithm, VectorIndexConfiguration,
};

/// Discriminant for which executor will serve the collection. Carries no
/// executor state — callers pass this to indicate deployment mode without
/// dragging the full `Executor` type into this crate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutorKind {
    Distributed,
    Local,
}

/// Per-tenant feature flags that influence the planned schema. Callers
/// compute these from their own tenant config and pass them in so the
/// planner stays stateless.
#[derive(Clone, Copy, Debug, Default)]
pub struct TenantFeatureFlags {
    pub enable_maxscore: bool,
    pub enable_quantization: bool,
    pub enable_token_bitmap_fts: bool,
}

/// The output of planning a create-collection: everything the caller needs
/// to hand to `SysDb::create_collection`.
#[derive(Clone, Debug)]
pub struct CreateCollectionPlan {
    pub collection_id: CollectionUuid,
    pub segments: Vec<Segment>,
    pub configuration: Option<InternalCollectionConfiguration>,
    pub schema: Option<Schema>,
}

/// The canonical set of segment types an executor of the given kind can
/// host. Callers that don't already track this (e.g. `foundation-api`,
/// which has no `Executor`) can pass `&supported_segment_types(kind)`
/// directly to [`plan_create_collection`].
pub fn supported_segment_types(kind: ExecutorKind) -> Vec<SegmentType> {
    match kind {
        ExecutorKind::Distributed => vec![
            SegmentType::HnswDistributed,
            SegmentType::Spann,
            SegmentType::QuantizedSpann,
            SegmentType::BlockfileRecord,
            SegmentType::BlockfileMetadata,
        ],
        ExecutorKind::Local => vec![
            SegmentType::HnswLocalMemory,
            SegmentType::HnswLocalPersisted,
            SegmentType::Sqlite,
        ],
    }
}

/// Plan the creation of a collection. Validates the user-supplied
/// configuration against the executor's supported segment types,
/// reconciles schema + config (if `enable_schema`), applies tenant feature
/// flags, and emits the segment set appropriate for the executor / schema
/// combination.
///
/// Returns a [`CreateCollectionPlan`] containing the fresh `collection_id`,
/// the segment list, and the (possibly-reconciled) configuration and schema
/// to pass to `SysDb::create_collection`. The caller is responsible for the
/// sysdb call, cache invalidation, and read-side schema reconciliation.
pub fn plan_create_collection(
    mut configuration: Option<InternalCollectionConfiguration>,
    schema: Option<Schema>,
    executor_kind: ExecutorKind,
    supported_segment_types: &[SegmentType],
    enable_schema: bool,
    default_knn_index: KnnIndex,
    tenant_flags: TenantFeatureFlags,
) -> Result<CreateCollectionPlan, CreateCollectionError> {
    let collection_id = CollectionUuid::new();
    let supported: HashSet<SegmentType> = supported_segment_types.iter().copied().collect();

    if let Some(config) = configuration.as_ref() {
        match &config.vector_index {
            VectorIndexConfiguration::Spann { .. } => {
                if !supported.contains(&SegmentType::Spann)
                    && !supported.contains(&SegmentType::QuantizedSpann)
                {
                    return Err(CreateCollectionError::SpannNotImplemented);
                }
            }
            VectorIndexConfiguration::Hnsw { .. } => {
                if !supported.contains(&SegmentType::HnswDistributed)
                    && !supported.contains(&SegmentType::HnswLocalMemory)
                    && !supported.contains(&SegmentType::HnswLocalPersisted)
                {
                    return Err(CreateCollectionError::HnswNotSupported);
                }
            }
        }
    }

    match default_knn_index {
        KnnIndex::Spann => {
            if !supported.contains(&SegmentType::Spann)
                && !supported.contains(&SegmentType::QuantizedSpann)
            {
                return Err(CreateCollectionError::SpannNotImplemented);
            }
        }
        KnnIndex::Hnsw => {
            if !supported.contains(&SegmentType::HnswDistributed)
                && !supported.contains(&SegmentType::HnswLocalMemory)
                && !supported.contains(&SegmentType::HnswLocalPersisted)
            {
                return Err(CreateCollectionError::HnswNotSupported);
            }
        }
    }

    let mut reconciled_schema = if enable_schema {
        // It's safe to take here, bc we're moving all config info to schema
        // when configuration is None, we then populate in sysdb with empty
        // config {} — this allows for easier migration paths in the future.
        let config_for_reconcile = configuration.take();
        match Schema::reconcile_schema_and_config(
            schema.as_ref(),
            config_for_reconcile.as_ref(),
            default_knn_index,
        ) {
            Ok(schema) => Some(schema),
            Err(e) => return Err(CreateCollectionError::InvalidSchema(e)),
        }
    } else {
        None
    };

    if let Some(ref mut schema) = reconciled_schema {
        if tenant_flags.enable_quantization {
            schema.quantize(Quantization::FourBitRabitQWithUSearch);
        }
        if tenant_flags.enable_maxscore {
            schema.set_sparse_algorithm(SparseIndexAlgorithm::MaxScore);
        }
        if tenant_flags.enable_token_bitmap_fts {
            schema.set_fts_algorithm(FtsAlgorithm::TokenBitmap);
        }
    }

    let segments = match executor_kind {
        ExecutorKind::Distributed => {
            let mut vector_segment_type = SegmentType::HnswDistributed;
            if enable_schema {
                if let Some(schema) = reconciled_schema.as_ref() {
                    if schema.get_internal_spann_config().is_some() {
                        // Use QuantizedSpann if quantization is enabled,
                        // otherwise plain Spann.
                        if schema.is_quantization_enabled() {
                            vector_segment_type = SegmentType::QuantizedSpann;
                        } else {
                            vector_segment_type = SegmentType::Spann;
                        }
                    }
                }
            }
            if let Some(config) = configuration.as_ref() {
                if matches!(config.vector_index, VectorIndexConfiguration::Spann(_)) {
                    vector_segment_type = SegmentType::Spann;
                }
            }

            vec![
                Segment {
                    id: SegmentUuid::new(),
                    r#type: vector_segment_type,
                    scope: SegmentScope::VECTOR,
                    collection: collection_id,
                    metadata: None,
                    file_path: Default::default(),
                },
                Segment {
                    id: SegmentUuid::new(),
                    r#type: SegmentType::BlockfileMetadata,
                    scope: SegmentScope::METADATA,
                    collection: collection_id,
                    metadata: None,
                    file_path: Default::default(),
                },
                Segment {
                    id: SegmentUuid::new(),
                    r#type: SegmentType::BlockfileRecord,
                    scope: SegmentScope::RECORD,
                    collection: collection_id,
                    metadata: None,
                    file_path: Default::default(),
                },
            ]
        }
        ExecutorKind::Local => {
            if enable_schema {
                if let Some(schema) = reconciled_schema.as_ref() {
                    if schema.is_sparse_index_enabled() {
                        return Err(CreateCollectionError::InvalidSchema(
                            SchemaError::InvalidUserInput {
                                reason: "Sparse vector indexing is not enabled in local"
                                    .to_string(),
                            },
                        ));
                    }
                }
            }

            vec![
                Segment {
                    id: SegmentUuid::new(),
                    r#type: SegmentType::HnswLocalPersisted,
                    scope: SegmentScope::VECTOR,
                    collection: collection_id,
                    metadata: None,
                    file_path: Default::default(),
                },
                Segment {
                    id: SegmentUuid::new(),
                    r#type: SegmentType::Sqlite,
                    scope: SegmentScope::METADATA,
                    collection: collection_id,
                    metadata: None,
                    file_path: Default::default(),
                },
            ]
        }
    };

    Ok(CreateCollectionPlan {
        collection_id,
        segments,
        configuration,
        schema: reconciled_schema,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn distributed_supported() -> Vec<SegmentType> {
        supported_segment_types(ExecutorKind::Distributed)
    }

    fn local_supported() -> Vec<SegmentType> {
        supported_segment_types(ExecutorKind::Local)
    }

    #[test]
    fn distributed_default_is_hnsw_trio() {
        let plan = plan_create_collection(
            None,
            None,
            ExecutorKind::Distributed,
            &distributed_supported(),
            false,
            KnnIndex::Hnsw,
            TenantFeatureFlags::default(),
        )
        .expect("plan");

        assert_eq!(plan.segments.len(), 3);
        assert!(plan
            .segments
            .iter()
            .any(|s| s.r#type == SegmentType::HnswDistributed && s.scope == SegmentScope::VECTOR));
        assert!(plan.segments.iter().any(
            |s| s.r#type == SegmentType::BlockfileMetadata && s.scope == SegmentScope::METADATA
        ));
        assert!(plan
            .segments
            .iter()
            .any(|s| s.r#type == SegmentType::BlockfileRecord && s.scope == SegmentScope::RECORD));
    }

    #[test]
    fn local_default_is_two_segments() {
        let plan = plan_create_collection(
            None,
            None,
            ExecutorKind::Local,
            &local_supported(),
            false,
            KnnIndex::Hnsw,
            TenantFeatureFlags::default(),
        )
        .expect("plan");

        assert_eq!(plan.segments.len(), 2);
        assert!(plan
            .segments
            .iter()
            .any(|s| s.r#type == SegmentType::HnswLocalPersisted));
        assert!(plan
            .segments
            .iter()
            .any(|s| s.r#type == SegmentType::Sqlite));
    }

    #[test]
    fn rejects_spann_when_unsupported() {
        // Local supports no Spann variants; if the default knn is Spann the
        // planner must refuse.
        let err = plan_create_collection(
            None,
            None,
            ExecutorKind::Local,
            &local_supported(),
            false,
            KnnIndex::Spann,
            TenantFeatureFlags::default(),
        )
        .expect_err("should reject Spann default on local executor");
        assert!(matches!(err, CreateCollectionError::SpannNotImplemented));
    }

    #[test]
    fn distinct_collection_ids_per_plan() {
        let plan_a = plan_create_collection(
            None,
            None,
            ExecutorKind::Distributed,
            &distributed_supported(),
            false,
            KnnIndex::Hnsw,
            TenantFeatureFlags::default(),
        )
        .unwrap();
        let plan_b = plan_create_collection(
            None,
            None,
            ExecutorKind::Distributed,
            &distributed_supported(),
            false,
            KnnIndex::Hnsw,
            TenantFeatureFlags::default(),
        )
        .unwrap();
        assert_ne!(plan_a.collection_id, plan_b.collection_id);
    }
}
