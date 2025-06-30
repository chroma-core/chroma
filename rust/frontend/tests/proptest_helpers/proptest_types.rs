use chroma_types::{
    AddCollectionRecordsRequest, DeleteCollectionRecordsRequest, GetRequest, QueryRequest,
    UpdateCollectionRecordsRequest, UpsertCollectionRecordsRequest,
};

#[derive(Debug, Clone)]
pub(crate) enum CollectionRequest {
    Init {
        dimension: usize,
    },
    Add(AddCollectionRecordsRequest),
    Update(UpdateCollectionRecordsRequest),
    Upsert(UpsertCollectionRecordsRequest),
    Delete(DeleteCollectionRecordsRequest),
    #[allow(dead_code)]
    Compact,
    // These do not mutate state. They're transitions rather than being tested during `invariants()` because `invariants()` cannot generate dynamic requests.
    Get(GetRequest),
    Query(QueryRequest),
}
