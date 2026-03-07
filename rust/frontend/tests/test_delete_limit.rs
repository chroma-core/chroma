use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{config::FrontendServerConfig, Frontend};
use chroma_sqlite::config::SqliteDBConfig;
use chroma_system::System;
use chroma_types::{
    plan::ReadLevel, AddCollectionRecordsRequest, CountRequest, CreateCollectionRequest,
    DatabaseName, DeleteCollectionRecordsRequest, Metadata, MetadataComparison, MetadataExpression,
    MetadataValue, PrimitiveOperator, Where,
};

const TENANT: &str = "default_tenant";
const DATABASE: &str = "default_database";

async fn setup() -> (Frontend, chroma_types::Collection) {
    let system = System::new();
    let registry = Registry::new();
    let mut config = FrontendServerConfig::single_node_default();
    config.frontend.sqlitedb = Some(SqliteDBConfig {
        url: None,
        ..Default::default()
    });

    let mut frontend = Frontend::try_from_config(&(config.frontend, system), &registry)
        .await
        .unwrap();

    let database_name = DatabaseName::new(DATABASE).expect("database name should be valid");
    let collection = frontend
        .create_collection(
            CreateCollectionRequest::try_new(
                TENANT.to_string(),
                database_name,
                "test_collection".to_string(),
                None,
                None,
                None,
                false,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    (frontend, collection)
}

async fn add_records(
    frontend: &mut Frontend,
    collection: &chroma_types::Collection,
    ids: Vec<&str>,
    metadatas: Option<Vec<Option<Metadata>>>,
) {
    let num = ids.len();
    let embeddings: Vec<Vec<f32>> = (0..num).map(|i| vec![i as f32; 3]).collect();

    frontend
        .add(
            AddCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                ids.into_iter().map(String::from).collect(),
                embeddings,
                None,
                None,
                metadatas,
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

async fn count(frontend: &mut Frontend, collection: &chroma_types::Collection) -> u32 {
    frontend
        .count(
            CountRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                ReadLevel::default(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn test_delete_by_ids_with_limit() {
    let (mut frontend, collection) = setup().await;
    add_records(
        &mut frontend,
        &collection,
        vec!["a", "b", "c", "d", "e"],
        None,
    )
    .await;
    assert_eq!(count(&mut frontend, &collection).await, 5);

    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
                Some(2),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 2);
    assert_eq!(count(&mut frontend, &collection).await, 3);
}

#[tokio::test]
async fn test_delete_by_where_with_limit() {
    let (mut frontend, collection) = setup().await;

    let mut meta_a = Metadata::new();
    meta_a.insert("category".to_string(), MetadataValue::Str("a".to_string()));
    let mut meta_b = Metadata::new();
    meta_b.insert("category".to_string(), MetadataValue::Str("b".to_string()));

    add_records(
        &mut frontend,
        &collection,
        vec!["id1", "id2", "id3", "id4"],
        Some(vec![
            Some(meta_a.clone()),
            Some(meta_a.clone()),
            Some(meta_a.clone()),
            Some(meta_b),
        ]),
    )
    .await;
    assert_eq!(count(&mut frontend, &collection).await, 4);

    // Where matches 3 records (category == "a"), but limit is 1.
    let where_clause = Where::Metadata(MetadataExpression {
        key: "category".to_string(),
        comparison: MetadataComparison::Primitive(
            PrimitiveOperator::Equal,
            MetadataValue::Str("a".to_string()),
        ),
    });

    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                None,
                Some(where_clause),
                Some(1),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 1);
    assert_eq!(count(&mut frontend, &collection).await, 3);
}

#[tokio::test]
async fn test_delete_without_limit() {
    let (mut frontend, collection) = setup().await;
    add_records(&mut frontend, &collection, vec!["a", "b", "c"], None).await;
    assert_eq!(count(&mut frontend, &collection).await, 3);

    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                Some(vec!["a".to_string(), "b".to_string(), "c".to_string()]),
                None,
                None,
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 3);
    assert_eq!(count(&mut frontend, &collection).await, 0);
}

#[tokio::test]
async fn test_delete_with_limit_zero() {
    let (mut frontend, collection) = setup().await;
    add_records(&mut frontend, &collection, vec!["a", "b", "c"], None).await;
    assert_eq!(count(&mut frontend, &collection).await, 3);

    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                Some(vec!["a".to_string(), "b".to_string()]),
                None,
                Some(0),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 0);
    assert_eq!(count(&mut frontend, &collection).await, 3);
}

#[tokio::test]
async fn test_delete_with_limit_greater_than_matches() {
    let (mut frontend, collection) = setup().await;
    add_records(&mut frontend, &collection, vec!["a", "b"], None).await;
    assert_eq!(count(&mut frontend, &collection).await, 2);

    // Limit is 100, but only 2 records match.
    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                Some(vec!["a".to_string(), "b".to_string()]),
                None,
                Some(100),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 2);
    assert_eq!(count(&mut frontend, &collection).await, 0);
}
