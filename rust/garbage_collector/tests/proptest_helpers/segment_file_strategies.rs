use chroma_blockstore::arrow::provider::BlockManager;
use chroma_blockstore::test_utils::sparse_index_test_utils::create_test_sparse_index;
use chroma_blockstore::RootManager;
use chroma_index::hnsw_provider::{HnswIndexProvider, FILES};
use chroma_segment::types::ChromaSegmentFlusher;
use chroma_storage::Storage;
use chroma_types::{CollectionUuid, DatabaseUuid, SegmentFlushInfo, SegmentUuid};
use futures::StreamExt;
use proptest::prelude::{any, any_with, Arbitrary, BoxedStrategy};
use proptest::strategy::Strategy;
use proptest::{prelude::Just, prop_oneof};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

use super::proptest_types::SegmentIds;

#[derive(Clone, Debug, Eq, Hash, PartialEq, Default)]
enum SegmentFileReferenceType {
    #[default]
    HNSWIndex,
    HNSWPath,
    SparseIndex {
        name: String,
    },
}

impl Arbitrary for SegmentFileReferenceType {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        prop_oneof![
            Just(SegmentFileReferenceType::HNSWIndex),
            Just(SegmentFileReferenceType::HNSWPath),
            (0..=10).prop_map(|name| SegmentFileReferenceType::SparseIndex {
                name: format!("sparse_index_{}", name)
            }),
        ]
        .boxed()
    }
}

impl SegmentFileReferenceType {
    fn name(&self) -> &str {
        match self {
            SegmentFileReferenceType::HNSWIndex => "hnsw_index",
            SegmentFileReferenceType::HNSWPath => "hnsw_path",
            SegmentFileReferenceType::SparseIndex { name } => name,
        }
    }
}

#[derive(Clone, Debug)]
enum FileReference {
    SparseIndex {
        path: String,
        block_paths: Vec<String>,
    },
    Hnsw {
        file_paths: Vec<String>,
    },
}

impl FileReference {
    fn paths(&self) -> Vec<String> {
        match self {
            FileReference::SparseIndex { path, block_paths } => {
                let mut paths = vec![path.clone()];
                paths.extend(block_paths.iter().cloned());
                paths
            }
            FileReference::Hnsw { file_paths, .. } => file_paths.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct SegmentFileReference {
    reference_id: Uuid,
    reference: FileReference,
}

fn new_hnsw_index_strategy(prefix_path: String) -> BoxedStrategy<SegmentFileReference> {
    let hnsw_index_id = Uuid::new_v4();
    let hnsw_index = FileReference::Hnsw {
        file_paths: FILES
            .iter()
            .map(|file_name| {
                HnswIndexProvider::format_key(
                    &prefix_path,
                    &chroma_index::IndexUuid(hnsw_index_id),
                    file_name,
                )
            })
            .collect::<Vec<String>>(),
    };
    Just(SegmentFileReference {
        reference_id: hnsw_index_id,
        reference: hnsw_index,
    })
    .boxed()
}

fn new_or_forked_sparse_index_strategy(
    existing_sparse_index: Option<SegmentFileReference>,
    prefix_path: String,
) -> BoxedStrategy<SegmentFileReference> {
    let prefix_path_clone = prefix_path.clone();
    let new_block_paths_strategy = (1..10).prop_map(move |num| {
        let mut block_paths = vec![];
        for _ in 0..num {
            block_paths.push(BlockManager::format_key(
                &prefix_path_clone,
                &Uuid::new_v4(),
            ));
        }
        block_paths
    });

    let existing_block_paths = if let Some(existing_sparse_index) = existing_sparse_index {
        match existing_sparse_index {
            SegmentFileReference {
                reference: FileReference::SparseIndex { block_paths, .. },
                ..
            } => block_paths,
            _ => unreachable!(),
        }
    } else {
        vec![]
    };

    let num_existing_block_paths = existing_block_paths.len();

    let block_paths_strategy = (
        new_block_paths_strategy,
        proptest::sample::subsequence(
            existing_block_paths,
            (num_existing_block_paths.min(2))..=num_existing_block_paths,
        ),
    )
        .prop_map(|(new_block_paths, existing_block_paths)| {
            let mut block_paths = HashSet::new();
            block_paths.extend(new_block_paths);
            block_paths.extend(existing_block_paths);
            block_paths
        });

    block_paths_strategy
        .prop_map(move |block_paths| {
            let sparse_index_id = Uuid::new_v4();
            let sparse_index = FileReference::SparseIndex {
                path: RootManager::get_storage_key(&prefix_path, &sparse_index_id),
                block_paths: block_paths.into_iter().collect(),
            };
            SegmentFileReference {
                reference_id: sparse_index_id,
                reference: sparse_index,
            }
        })
        .boxed()
}

/// A collection of file references for a segment.
#[derive(Clone, Debug)]
pub struct SegmentFilePaths {
    paths: HashMap<SegmentFileReferenceType, Vec<SegmentFileReference>>,
    pub root_segment_id: SegmentUuid,
    pub prefix_path: String,
}

impl Arbitrary for SegmentFilePaths {
    type Parameters = (String, DatabaseUuid, CollectionUuid);
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        let segment_id = SegmentUuid::new();
        let prefix_path = format!(
            "tenant/{}/database/{}/collection/{}/segment/{}",
            params.0, params.1, params.2, segment_id
        );
        let prefix_path_clone = prefix_path.clone();
        proptest::collection::vec(
            any::<SegmentFileReferenceType>().prop_flat_map(move |segment_file_reference_type| {
                let refs = match segment_file_reference_type.clone() {
                    SegmentFileReferenceType::HNSWIndex => {
                        new_hnsw_index_strategy(prefix_path.clone())
                    }
                    SegmentFileReferenceType::HNSWPath => {
                        new_hnsw_index_strategy(prefix_path.clone())
                    }
                    SegmentFileReferenceType::SparseIndex { .. } => {
                        new_or_forked_sparse_index_strategy(None, prefix_path.clone())
                    }
                };
                (Just(segment_file_reference_type), refs)
            }),
            1..10,
        )
        .prop_map(move |elements| SegmentFilePaths {
            paths: elements
                .into_iter()
                .map(|(k, v)| (k, vec![v]))
                .collect::<HashMap<_, _>>(),
            root_segment_id: segment_id,
            prefix_path: prefix_path_clone.clone(),
        })
        .boxed()
    }
}

impl From<SegmentFilePaths> for HashMap<String, Vec<String>> {
    fn from(segment_file_paths: SegmentFilePaths) -> Self {
        assert!(!segment_file_paths.paths.is_empty());
        let mut file_paths = HashMap::new();
        for (key, value) in segment_file_paths.paths {
            file_paths.insert(
                key.name().to_string(),
                value
                    .iter()
                    .map(|f| {
                        ChromaSegmentFlusher::flush_key(
                            &segment_file_paths.prefix_path,
                            &f.reference_id,
                        )
                    })
                    .collect(),
            );
        }
        file_paths
    }
}

impl SegmentFilePaths {
    fn paths(&self) -> Vec<String> {
        let mut paths = vec![];
        for file_reference in self.paths.values() {
            for file_ref in file_reference {
                paths.extend(file_ref.reference.paths());
            }
        }
        assert!(!paths.is_empty());
        paths
    }

    fn into_segment_flush_info(self, segment_id: SegmentUuid) -> SegmentFlushInfo {
        SegmentFlushInfo {
            segment_id,
            file_paths: self.into(),
        }
    }

    pub fn next_version_strategy(&self) -> BoxedStrategy<Self> {
        let prefix_path = self.prefix_path.clone();
        let hnsw_references_strategy = (
            any::<Option<bool>>(),
            prop_oneof![
                Just(SegmentFileReferenceType::HNSWIndex),
                Just(SegmentFileReferenceType::HNSWPath),
            ],
        )
            .prop_map({
                let current_refs = self.paths.clone();
                move |(hnsw_index, ref_type)| {
                    let mut refs = HashMap::new();
                    match hnsw_index {
                        Some(true) => {
                            // Inherit from parent
                            if let Some(parent_hnsw_index) = current_refs.get(&ref_type) {
                                refs.insert(ref_type, parent_hnsw_index.clone());
                            }
                        }
                        Some(false) => {
                            // Don't inherit, create new
                            let hnsw_index_id = Uuid::new_v4();
                            let hnsw_index = FileReference::Hnsw {
                                file_paths: FILES
                                    .iter()
                                    .map(|file_name| {
                                        HnswIndexProvider::format_key(
                                            &prefix_path,
                                            &chroma_index::IndexUuid(hnsw_index_id),
                                            file_name,
                                        )
                                    })
                                    .collect::<Vec<String>>(),
                            };

                            refs.insert(
                                ref_type,
                                vec![SegmentFileReference {
                                    reference_id: hnsw_index_id,
                                    reference: hnsw_index,
                                }],
                            );
                        }
                        None => {}
                    }

                    refs
                }
            });

        let sparse_indices = self
            .paths
            .iter()
            .filter(|(k, _)| matches!(k, SegmentFileReferenceType::SparseIndex { .. }))
            .flat_map(|(k, v)| v.iter().map(|v| (k.clone(), v.clone())))
            .collect::<Vec<_>>();

        let prefix_path = self.prefix_path.clone();
        let current_sparse_indices_strategy = if sparse_indices.is_empty() {
            Just(HashMap::new()).boxed()
        } else {
            let num_sparse_indices = sparse_indices.len();
            // proptest does not yet support `Vec<BoxedStrategy<T>> -> BoxedStrategy<Vec<T>>`, so instead we first sample a subset of Vec<T> and apply the desired flat map while sampling. We then reject the generated Vec<T> if it contains duplicates.
            proptest::collection::vec(
                proptest::sample::select(sparse_indices).prop_flat_map(
                    move |(sparse_index_name, sparse_index)| {
                        let sparse_index = new_or_forked_sparse_index_strategy(
                            Some(sparse_index),
                            prefix_path.clone(),
                        );
                        (Just(sparse_index_name), sparse_index)
                    },
                ),
                (1.min(num_sparse_indices))..=num_sparse_indices,
            )
            .prop_filter("duplicate sparse index sampled", |sparse_indices| {
                let mut seen = HashSet::new();
                for (sparse_index_name, _) in sparse_indices {
                    if seen.contains(sparse_index_name) {
                        return false;
                    }
                    seen.insert(sparse_index_name);
                }
                true
            })
            .prop_map(|sparse_indices| {
                let mut refs: HashMap<SegmentFileReferenceType, Vec<SegmentFileReference>> =
                    HashMap::new();
                for (sparse_index_name, sparse_index) in sparse_indices {
                    let entry = refs.entry(sparse_index_name).or_default();
                    if !entry
                        .iter()
                        .any(|r| r.reference_id == sparse_index.reference_id)
                    {
                        entry.push(sparse_index);
                    }
                }
                refs
            })
            .boxed()
        };

        let new_sparse_indices_strategy = proptest::collection::hash_map(
            (0..10).prop_map(|i| format!("sparse_index_{}", i)),
            new_or_forked_sparse_index_strategy(None, self.prefix_path.clone()),
            1..3,
        )
        .prop_map(|sparse_indices| {
            let mut refs = HashMap::new();
            for (sparse_index_name, sparse_index) in sparse_indices {
                refs.insert(
                    SegmentFileReferenceType::SparseIndex {
                        name: sparse_index_name,
                    },
                    vec![sparse_index],
                );
            }
            refs
        });

        let sparse_indices_strategy =
            (current_sparse_indices_strategy, new_sparse_indices_strategy).prop_map(
                |(current_sparse_indices, new_sparse_indices)| {
                    let mut refs = HashMap::new();
                    refs.extend(new_sparse_indices);
                    refs.extend(current_sparse_indices);
                    refs
                },
            );

        let segment_id = self.root_segment_id;
        let prefix_path = self.prefix_path.clone();
        (hnsw_references_strategy, sparse_indices_strategy)
            .prop_map(move |(hnsw_references, sparse_indices)| {
                let mut references = hnsw_references;
                references.extend(sparse_indices);

                Self {
                    paths: references,
                    root_segment_id: segment_id,
                    prefix_path: prefix_path.clone(),
                }
            })
            .boxed()
    }
}

/// A group of the three segment types. Note that the files generated for each segment type may not corelate with what the real system would create (e.g. the metadata segment may have HNSW files).
///
/// This grouping is used instead of generating a variable number of segments as the latter is quite difficult to construct with proptest (there's no transform for `Vec<BoxedStrategy<T>> -> BoxedStrategy<Vec<T>>`).
#[derive(Clone, Debug)]
pub struct SegmentGroup {
    pub vector: SegmentFilePaths,
    pub metadata: SegmentFilePaths,
    pub record: SegmentFilePaths,
}

impl SegmentGroup {
    pub fn get_all_file_paths(&self) -> Vec<String> {
        let mut all_file_paths = vec![];
        all_file_paths.extend(self.vector.paths());
        all_file_paths.extend(self.metadata.paths());
        all_file_paths.extend(self.record.paths());
        all_file_paths
    }

    pub fn into_segment_flushes(self, ids: &SegmentIds) -> Arc<[SegmentFlushInfo]> {
        let vector_flush_info = self.vector.into_segment_flush_info(ids.vector);
        let metadata_flush_info = self.metadata.into_segment_flush_info(ids.metadata);
        let record_flush_info = self.record.into_segment_flush_info(ids.record);

        Arc::from([vector_flush_info, metadata_flush_info, record_flush_info])
    }

    pub async fn write_files(&self, storage: &Storage) {
        futures::future::join_all([
            write_files_for_segment(storage, &self.vector),
            write_files_for_segment(storage, &self.metadata),
            write_files_for_segment(storage, &self.record),
        ])
        .await;
    }
}

async fn write_files_for_segment(storage: &Storage, file_paths: &SegmentFilePaths) {
    let prefix_path = file_paths.prefix_path.clone();
    for refs in file_paths.paths.values() {
        for file_ref in refs {
            match &file_ref.reference {
                FileReference::SparseIndex { block_paths, .. } => {
                    let block_ids = block_paths
                        .iter()
                        .map(|block_path| {
                            Uuid::parse_str(block_path.split('/').last().unwrap()).unwrap()
                        })
                        .collect::<Vec<_>>();
                    create_test_sparse_index(
                        storage,
                        file_ref.reference_id,
                        block_ids,
                        None,
                        prefix_path.clone(),
                    )
                    .await
                    .unwrap();

                    // Write blocks
                    let contents = vec![0; 8];
                    futures::stream::iter(block_paths.iter())
                        .map(|file| {
                            let storage = storage.clone();
                            let contents = contents.clone();
                            async move {
                                storage
                                    .put_bytes(file, contents, Default::default())
                                    .await
                                    .unwrap();
                            }
                        })
                        .buffer_unordered(32)
                        .collect()
                        .await
                }
                FileReference::Hnsw { file_paths, .. } => {
                    let contents = vec![0; 8];
                    futures::stream::iter(file_paths.iter())
                        .map(|file| {
                            let storage = storage.clone();
                            let contents = contents.clone();
                            async move {
                                storage
                                    .put_bytes(file, contents, Default::default())
                                    .await
                                    .unwrap();
                            }
                        })
                        .buffer_unordered(32)
                        .collect()
                        .await
                }
            }
        }
    }
}

impl Arbitrary for SegmentGroup {
    type Parameters = (String, DatabaseUuid, CollectionUuid);
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(params: Self::Parameters) -> Self::Strategy {
        (
            any_with::<SegmentFilePaths>(params.clone()),
            any_with::<SegmentFilePaths>(params.clone()),
            any_with::<SegmentFilePaths>(params.clone()),
        )
            .prop_map(
                |(vector_segment_paths, metadata_segment_paths, record_segment_paths)| {
                    assert!(!vector_segment_paths.paths().is_empty());
                    assert!(!metadata_segment_paths.paths().is_empty());
                    assert!(!record_segment_paths.paths().is_empty());
                    SegmentGroup {
                        vector: (vector_segment_paths),
                        metadata: (metadata_segment_paths),
                        record: (record_segment_paths),
                    }
                },
            )
            .boxed()
    }
}
