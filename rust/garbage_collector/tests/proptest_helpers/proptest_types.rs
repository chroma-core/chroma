use super::segment_file_strategies::SegmentGroup;
use chroma_types::CollectionUuid;

#[derive(Clone, Debug)]
pub enum Transition {
    CreateCollection {
        collection_id: CollectionUuid,
        segments: SegmentGroup,
    },
    IncrementCollectionVersion {
        collection_id: CollectionUuid,
        next_segments: SegmentGroup,
    },
    ForkCollection {
        source_collection_id: CollectionUuid,
        new_collection_id: CollectionUuid,
    },
    DeleteCollection(CollectionUuid),
    GarbageCollect {
        collection_id: CollectionUuid,
        min_versions_to_keep: usize,
    },
    NoOp,
}
