use chroma_types::{CollectionUuid, DatabaseName, TopologyName};

#[derive(Debug, Clone)]
pub enum DatabaseOrTopology {
    Database(DatabaseName),
    Topology(TopologyName),
}

#[derive(Default, Debug)]
pub struct GetCollectionsOptions {
    pub collection_id: Option<CollectionUuid>,
    pub collection_ids: Option<Vec<CollectionUuid>>,
    pub include_soft_deleted: bool,
    pub name: Option<String>,
    pub tenant: Option<String>,
    pub database_or_topology: Option<DatabaseOrTopology>,
    pub limit: Option<u32>,
    pub offset: u32,
}
