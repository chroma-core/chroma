use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{config::FrontendServerConfig, Frontend};
use chroma_sqlite::config::SqliteDBConfig;
use chroma_system::System;
use chroma_types::{
    plan::ReadLevel, AddCollectionRecordsRequest, CountRequest, CreateCollectionRequest,
    DatabaseName, DeleteCollectionRecordsRequest, GetRequest, IncludeList, Metadata,
    MetadataComparison, MetadataExpression, MetadataValue, PrimitiveOperator, Where,
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

fn where_eq(key: &str, value: &str) -> Where {
    Where::Metadata(MetadataExpression {
        key: key.to_string(),
        comparison: MetadataComparison::Primitive(
            PrimitiveOperator::Equal,
            MetadataValue::Str(value.to_string()),
        ),
    })
}

// --- Validation tests ---

#[test]
fn test_delete_limit_without_where_rejected() {
    let result = DeleteCollectionRecordsRequest::try_new(
        TENANT.to_string(),
        DATABASE.to_string(),
        chroma_types::CollectionUuid::new(),
        Some(vec!["a".to_string(), "b".to_string()]),
        None,
        Some(1),
    );
    assert!(
        result.is_err(),
        "limit without where clause should be rejected"
    );
}

#[test]
fn test_delete_limit_zero_without_where_rejected() {
    let result = DeleteCollectionRecordsRequest::try_new(
        TENANT.to_string(),
        DATABASE.to_string(),
        chroma_types::CollectionUuid::new(),
        Some(vec!["a".to_string()]),
        None,
        Some(0),
    );
    assert!(
        result.is_err(),
        "limit=0 without where clause should be rejected"
    );
}

// --- Functional tests ---

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
    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                None,
                Some(where_eq("category", "a")),
                Some(1),
            )
            .unwrap(),
            String::new(),
            String::new(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 1);
    assert_eq!(count(&mut frontend, &collection).await, 3);
}

#[tokio::test]
async fn test_delete_by_where_with_limit_zero() {
    let (mut frontend, collection) = setup().await;

    let mut meta_a = Metadata::new();
    meta_a.insert("category".to_string(), MetadataValue::Str("a".to_string()));

    add_records(
        &mut frontend,
        &collection,
        vec!["id1", "id2", "id3"],
        Some(vec![
            Some(meta_a.clone()),
            Some(meta_a.clone()),
            Some(meta_a),
        ]),
    )
    .await;
    assert_eq!(count(&mut frontend, &collection).await, 3);

    // limit=0: no records should be deleted even though where matches all.
    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                None,
                Some(where_eq("category", "a")),
                Some(0),
            )
            .unwrap(),
            String::new(),
            String::new(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 0);
    assert_eq!(count(&mut frontend, &collection).await, 3);
}

#[tokio::test]
async fn test_delete_by_where_with_limit_greater_than_matches() {
    let (mut frontend, collection) = setup().await;

    let mut meta_a = Metadata::new();
    meta_a.insert("category".to_string(), MetadataValue::Str("a".to_string()));

    add_records(
        &mut frontend,
        &collection,
        vec!["id1", "id2"],
        Some(vec![Some(meta_a.clone()), Some(meta_a)]),
    )
    .await;
    assert_eq!(count(&mut frontend, &collection).await, 2);

    // Limit is 100, but only 2 records match. Should delete all 2.
    let response = frontend
        .delete(
            DeleteCollectionRecordsRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                None,
                Some(where_eq("category", "a")),
                Some(100),
            )
            .unwrap(),
            String::new(),
            String::new(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 2);
    assert_eq!(count(&mut frontend, &collection).await, 0);
}

#[tokio::test]
async fn test_delete_by_ids_without_limit() {
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
            String::new(),
            String::new(),
        )
        .await
        .unwrap();

    assert_eq!(response.deleted, 3);
    assert_eq!(count(&mut frontend, &collection).await, 0);
}

#[tokio::test]
async fn test_delete_by_where_with_limit_loop() {
    let (mut frontend, collection) = setup().await;

    // Add 10 records: 7 with category="a", 3 with category="b".
    let mut meta_a = Metadata::new();
    meta_a.insert("category".to_string(), MetadataValue::Str("a".to_string()));
    let mut meta_b = Metadata::new();
    meta_b.insert("category".to_string(), MetadataValue::Str("b".to_string()));

    let ids: Vec<&str> = vec!["a1", "a2", "a3", "a4", "a5", "a6", "a7", "b1", "b2", "b3"];
    let metadatas = vec![
        Some(meta_a.clone()),
        Some(meta_a.clone()),
        Some(meta_a.clone()),
        Some(meta_a.clone()),
        Some(meta_a.clone()),
        Some(meta_a.clone()),
        Some(meta_a),
        Some(meta_b.clone()),
        Some(meta_b.clone()),
        Some(meta_b),
    ];

    add_records(&mut frontend, &collection, ids, Some(metadatas)).await;
    assert_eq!(count(&mut frontend, &collection).await, 10);

    // Loop delete with where={category: "a"}, limit=2.
    let mut total_deleted: u32 = 0;
    let mut remaining = 10u32;

    loop {
        let response = frontend
            .delete(
                DeleteCollectionRecordsRequest::try_new(
                    collection.tenant.clone(),
                    collection.database.clone(),
                    collection.collection_id,
                    None,
                    Some(where_eq("category", "a")),
                    Some(2),
                )
                .unwrap(),
                String::new(),
                String::new(),
            )
            .await
            .unwrap();

        assert!(
            response.deleted <= 2,
            "deleted {} exceeds limit of 2",
            response.deleted
        );

        if response.deleted == 0 {
            break;
        }

        total_deleted += response.deleted;
        remaining -= response.deleted;

        let current_count = count(&mut frontend, &collection).await;
        assert_eq!(
            current_count, remaining,
            "count mismatch after deleting {} total",
            total_deleted
        );
    }

    // Should have deleted exactly 7 category="a" records.
    assert_eq!(total_deleted, 7);

    // Only 3 category="b" records should remain.
    let current_count = count(&mut frontend, &collection).await;
    assert_eq!(current_count, 3);

    // Verify remaining records are all category="b".
    let get_response = frontend
        .get(
            GetRequest::try_new(
                collection.tenant.clone(),
                collection.database.clone(),
                collection.collection_id,
                None,
                None,
                None,
                0,
                IncludeList::default_get(),
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.ids.len(), 3);
    for id in &get_response.ids {
        assert!(
            id.starts_with('b'),
            "expected only category='b' records, found id={}",
            id
        );
    }
}
