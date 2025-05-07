use super::proptest_types::SegmentIds;
use chroma_blockstore::test_utils::sparse_index_test_utils::create_test_sparse_index;
use chroma_storage::Storage;
use chroma_types::{SegmentFlushInfo, SegmentUuid};
use futures::StreamExt;
use proptest::prelude::{any, Arbitrary, BoxedStrategy};
use proptest::strategy::Strategy;
use proptest::{prelude::Just, prop_oneof};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use uuid::Uuid;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum SegmentFileReferenceType {
    HNSWIndex,
    HNSWPath,
    SparseIndex { name: String },
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
    HNSW {
        prefix: String,
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
            FileReference::HNSW { file_paths, .. } => file_paths.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct SegmentFileReference {
    reference_id: Uuid,
    reference: FileReference,
}

fn new_hnsw_index_strategy() -> BoxedStrategy<SegmentFileReference> {
    let hnsw_index_id = Uuid::new_v4();
    let hnsw_index = FileReference::HNSW {
        prefix: format!("hnsw/{}", hnsw_index_id),
        file_paths: vec![
            format!("hnsw/{}/header.bin", hnsw_index_id),
            format!("hnsw/{}/data_level0.bin", hnsw_index_id),
            format!("hnsw/{}/length.bin", hnsw_index_id),
            format!("hnsw/{}/link_lists.bin", hnsw_index_id),
        ],
    };
    Just(SegmentFileReference {
        reference_id: hnsw_index_id,
        reference: hnsw_index,
    })
    .boxed()
}

fn new_or_forked_sparse_index_strategy(
    existing_sparse_index: Option<SegmentFileReference>,
) -> BoxedStrategy<SegmentFileReference> {
    let new_block_paths_strategy = (1..10).prop_map(|num| {
        let mut block_paths = vec![];
        for _ in 0..num {
            block_paths.push(format!("block/{}", Uuid::new_v4()));
        }
        block_paths
    });

    let existing_block_paths = if let Some(existing_sparse_index) = existing_sparse_index {
        let block_paths = match existing_sparse_index {
            SegmentFileReference {
                reference: FileReference::SparseIndex { block_paths, .. },
                ..
            } => block_paths,
            _ => unreachable!(),
        };

        block_paths
    } else {
        vec![]
    };

    let num_existing_block_paths = existing_block_paths.len();

    let block_paths_strategy = (
        new_block_paths_strategy,
        proptest::sample::subsequence(existing_block_paths, num_existing_block_paths),
    )
        .prop_map(|(new_block_paths, existing_block_paths)| {
            let mut block_paths = HashSet::new();
            block_paths.extend(new_block_paths);
            block_paths.extend(existing_block_paths);
            block_paths
        });

    block_paths_strategy
        .prop_map(|(block_paths)| {
            let sparse_index_id = Uuid::new_v4();
            let sparse_index = FileReference::SparseIndex {
                path: format!("sparse_index/{}", sparse_index_id),
                block_paths: block_paths.into_iter().collect(),
            };
            SegmentFileReference {
                reference_id: sparse_index_id.into(),
                reference: sparse_index,
            }
        })
        .boxed()
}

#[derive(Clone, Debug)]
pub struct SegmentFilePaths(HashMap<SegmentFileReferenceType, Vec<SegmentFileReference>>);

impl Arbitrary for SegmentFilePaths {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        proptest::collection::vec(
            any::<SegmentFileReferenceType>().prop_flat_map(|segment_file_reference_type| {
                let refs = match segment_file_reference_type.clone() {
                    SegmentFileReferenceType::HNSWIndex => new_hnsw_index_strategy(),
                    SegmentFileReferenceType::HNSWPath => new_hnsw_index_strategy(),
                    SegmentFileReferenceType::SparseIndex { .. } => {
                        new_or_forked_sparse_index_strategy(None)
                    }
                };
                (Just(segment_file_reference_type), refs)
            }),
            0..10,
        )
        .prop_map(|elements| {
            SegmentFilePaths(
                elements
                    .into_iter()
                    .map(|(k, v)| (k, vec![v]))
                    .collect::<HashMap<_, _>>(),
            )
        })
        .boxed()
    }
}

impl From<SegmentFilePaths> for HashMap<String, Vec<String>> {
    fn from(segment_file_paths: SegmentFilePaths) -> Self {
        let mut file_paths = HashMap::new();
        for (key, value) in segment_file_paths.0 {
            file_paths.insert(
                key.name().to_string(),
                value.iter().map(|f| f.reference_id.to_string()).collect(),
            );
        }
        file_paths
    }
}

impl SegmentFilePaths {
    fn paths(&self) -> Vec<String> {
        let mut paths = vec![];
        for file_reference in self.0.values() {
            for file_ref in file_reference {
                paths.extend(file_ref.reference.paths());
            }
        }
        paths
    }

    fn into_segment_flush_info(self, segment_id: SegmentUuid) -> SegmentFlushInfo {
        SegmentFlushInfo {
            segment_id,
            file_paths: self.into(),
        }
    }

    pub fn next_version_strategy(&self) -> BoxedStrategy<Self> {
        let hnsw_references_strategy = (any::<Option<bool>>(), any::<Option<bool>>()).prop_map({
            let current_refs = self.0.clone();
            move |(hnsw_index, hnsw_path)| {
                let mut refs = HashMap::new();
                match hnsw_index {
                    Some(true) => {
                        // Inherit from parent
                        if let Some(parent_hnsw_index) =
                            current_refs.get(&SegmentFileReferenceType::HNSWIndex)
                        {
                            refs.insert(
                                SegmentFileReferenceType::HNSWIndex,
                                parent_hnsw_index.clone(),
                            );
                        }
                    }
                    Some(false) => {
                        // Don't inherit, create new
                        let hnsw_index_id = Uuid::new_v4();
                        let hnsw_index = FileReference::HNSW {
                            prefix: format!("hnsw/{}", hnsw_index_id),
                            file_paths: vec![
                                format!("hnsw/{}/header.bin", hnsw_index_id),
                                format!("hnsw/{}/data_level0.bin", hnsw_index_id),
                                format!("hnsw/{}/length.bin", hnsw_index_id),
                                format!("hnsw/{}/link_lists.bin", hnsw_index_id),
                            ],
                        };

                        refs.insert(
                            SegmentFileReferenceType::HNSWIndex,
                            vec![SegmentFileReference {
                                reference_id: hnsw_index_id,
                                reference: hnsw_index,
                            }],
                        );
                    }
                    None => {}
                }

                // todo: handle hnsw_path

                refs
            }
        });

        // todo
        // let sparse_indices = self
        //     .0
        //     .iter()
        //     .filter(|(k, _)| matches!(k, SegmentFileReferenceType::SparseIndex { .. }))
        //     .map(|(k, v)| (k.clone(), v.clone()))
        //     .collect::<Vec<_>>();

        let new_sparse_indices_strategy = proptest::collection::hash_map(
            (0..10).prop_map(|i| format!("sparse_index_{}", i)),
            new_or_forked_sparse_index_strategy(None),
            0..3,
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

        (hnsw_references_strategy, new_sparse_indices_strategy)
            .prop_map(|(hnsw_references, sparse_indices)| {
                let mut references = hnsw_references;
                references.extend(sparse_indices);

                Self(references)
            })
            .boxed()
    }
}

/// A group of the three segment types. Note that the files generated for each segment type may not corelate with what the real system would create (e.g. the metadata segment may have HNSW files).
///
/// This grouping is used instead of generating a variable number of segments as the latter is quite difficult to construct with proptest (there's no transform for `Vec<BoxedStrategy<T>> -> BoxedStrategy<Vec<T>>`).
///
/// We do not track segment IDs here as they are not known until after creation.
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
        Self::write_files_for_segment(storage, &self.vector).await;
        Self::write_files_for_segment(storage, &self.metadata).await;
        Self::write_files_for_segment(storage, &self.record).await;
    }

    async fn write_files_for_segment(storage: &Storage, file_paths: &SegmentFilePaths) {
        for (ref_type, refs) in &file_paths.0 {
            for file_ref in refs {
                // file_ref.reference.
                match &file_ref.reference {
                    FileReference::SparseIndex { block_paths, .. } => {
                        let block_ids = block_paths
                            .iter()
                            .map(|block_path| {
                                Uuid::parse_str(block_path.split('/').last().unwrap()).unwrap()
                            })
                            .collect::<Vec<_>>();
                        create_test_sparse_index(storage, file_ref.reference_id, block_ids, None)
                            .await
                            .unwrap();

                        // Write blocks
                        let contents = vec![0; 8];
                        futures::stream::iter(block_paths.into_iter())
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
                    FileReference::HNSW { file_paths, .. } => {
                        let contents = vec![0; 8];
                        futures::stream::iter(file_paths.into_iter())
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
}

impl Arbitrary for SegmentGroup {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        (
            any::<SegmentFilePaths>(),
            any::<SegmentFilePaths>(),
            any::<SegmentFilePaths>(),
        )
            .prop_map(
                |(vector_segment_paths, metadata_segment_paths, record_segment_paths)| {
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
