use bon::Builder;
use chroma_types::{CollectionConfiguration, Metadata};

#[derive(Builder)]
pub struct CreateCollectionRequest {
    pub name: String,
    pub configuration: Option<CollectionConfiguration>,
    pub metadata: Option<Metadata>,
    pub database_name: Option<String>,
}

#[derive(Default, Builder)]
pub struct ListCollectionsRequest {
    #[builder(default = 100)]
    pub limit: usize,
    pub offset: Option<usize>,
    pub database_name: Option<String>,
}
