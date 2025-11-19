use chroma_types::{AttachedFunctionUuid, CollectionUuid};

#[derive(Default, Debug)]
pub struct GetCollectionsOptions {
    pub collection_id: Option<CollectionUuid>,
    pub collection_ids: Option<Vec<CollectionUuid>>,
    pub include_soft_deleted: bool,
    pub name: Option<String>,
    pub tenant: Option<String>,
    pub database: Option<String>,
    pub limit: Option<u32>,
    pub offset: u32,
}

#[derive(Default, Debug)]
pub struct GetAttachedFunctionsOptions {
    pub id: Option<AttachedFunctionUuid>,
    pub name: Option<String>,
    pub input_collection_id: Option<CollectionUuid>,
    pub only_ready: bool,
}
