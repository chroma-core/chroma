use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{
    compaction_client::compaction_client::CompactionClientConfig, frontend::Frontend,
    FrontendConfig,
};
use chroma_log::config::LogConfig;
use chroma_sqlite::config::SqliteDBConfig;
use chroma_system::System;
use chroma_types::{
    are_metadatas_close_to_equal,
    strategies::{
        arbitrary_metadata, arbitrary_update_metadata, TestWhereFilter, TestWhereFilterParams,
        DOCUMENT_TEXT_STRATEGY,
    },
    AddCollectionRecordsRequest, Collection, CountRequest, CreateCollectionRequest,
    DeleteCollectionRecordsRequest, GetRequest, GetResponse, Include, IncludeList,
    ManualCompactionRequest, QueryRequest, QueryResponse, UpdateCollectionRecordsRequest,
    UpsertCollectionRecordsRequest,
};
use proptest::prelude::*;
use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
use std::{cell::RefCell, sync::Arc};

struct GetRequestSelectivity {
    where_clause_only: Vec<f64>,
    where_clause_and_ids: Vec<f64>,
    ids_only: Vec<f64>,
}

struct QueryRequestSelectivity {
    where_clause_only: Vec<f64>,
    where_clause_and_ids: Vec<f64>,
    ids_only: Vec<f64>,
    embeddings_only: Vec<f64>,
}

struct Stats {
    get_request_selectivity: GetRequestSelectivity,
    query_request_selectivity: QueryRequestSelectivity,
    num_log_operations: usize,
}

impl Drop for Stats {
    fn drop(&mut self) {
        fn print_selectivity(selectivity: &Vec<f64>) {
            let partial_results = selectivity
                .iter()
                .filter(|x| **x != 0.0 && **x != 1.0)
                .count();
            let no_results = selectivity.iter().filter(|x| **x == 0.0).count();
            let all_results = selectivity.iter().filter(|x| **x == 1.0).count();

            println!(
                "      {:05.2}% of queries returned no results",
                no_results as f64 / selectivity.len() as f64 * 100.0
            );
            println!(
                "      {:05.2}% of queries returned some results",
                partial_results as f64 / selectivity.len() as f64 * 100.0
            );
            println!(
                "      {:05.2}% of queries returned all results",
                all_results as f64 / selectivity.len() as f64 * 100.0
            );
        }

        println!("Statistics:");
        println!(
            "  A total of {} log operations were created.",
            self.num_log_operations
        );

        // Get request selectivity
        let total_get_requests = self.get_request_selectivity.where_clause_only.len()
            + self.get_request_selectivity.where_clause_and_ids.len()
            + self.get_request_selectivity.ids_only.len();
        println!(
            "  .get() selectivity ({} total requests):",
            total_get_requests
        );
        println!(
            "    .get() with a where clause only ({:2.2}%):",
            self.get_request_selectivity.where_clause_only.len() as f64 / total_get_requests as f64
                * 100.0
        );
        print_selectivity(&self.get_request_selectivity.where_clause_only);

        println!(
            "    .get() with a where clause & IDs ({:2.2}%):",
            self.get_request_selectivity.where_clause_and_ids.len() as f64
                / total_get_requests as f64
                * 100.0
        );
        print_selectivity(&self.get_request_selectivity.where_clause_and_ids);

        println!(
            "    .get() with IDs only ({:2.2}%):",
            self.get_request_selectivity.ids_only.len() as f64 / total_get_requests as f64 * 100.0
        );
        print_selectivity(&self.get_request_selectivity.ids_only);

        // Query request selectivity
        let total_query_requests = self.query_request_selectivity.where_clause_only.len()
            + self.query_request_selectivity.where_clause_and_ids.len()
            + self.query_request_selectivity.ids_only.len()
            + self.query_request_selectivity.embeddings_only.len();
        println!(
            "  .query() selectivity ({} total requests):",
            total_query_requests
        );
        println!(
            "    .query() with a where clause & embeddings ({:2.2}%):",
            self.query_request_selectivity.where_clause_only.len() as f64
                / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.where_clause_only);

        println!(
            "    .query() with a where clause & IDs & embeddings ({:2.2}%):",
            self.query_request_selectivity.where_clause_and_ids.len() as f64
                / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.where_clause_and_ids);

        println!(
            "    .query() with IDs & embeddings ({:2.2}%):",
            self.query_request_selectivity.ids_only.len() as f64 / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.ids_only);

        println!(
            "    .query() with embeddings only ({:2.2}%):",
            self.query_request_selectivity.embeddings_only.len() as f64
                / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.embeddings_only);
    }
}

thread_local! {
    static STATS: RefCell<Stats> = const {
        RefCell::new(
        Stats {
            num_log_operations: 0,
            get_request_selectivity: GetRequestSelectivity {
                where_clause_only: vec![],
                where_clause_and_ids: vec![],
                ids_only: vec![],
            },
            query_request_selectivity: QueryRequestSelectivity {
                where_clause_only: vec![],
                where_clause_and_ids: vec![],
                ids_only: vec![],
                embeddings_only: vec![],
            },
        }
    )
    };
}

#[derive(Debug, Clone)]
enum CollectionRequest {
    Init { dimension: usize },
    Add(AddCollectionRecordsRequest),
    Update(UpdateCollectionRecordsRequest),
    Upsert(UpsertCollectionRecordsRequest),
    Delete(DeleteCollectionRecordsRequest),
    Compact,
    // These do not mutate state. They're transitions rather than tested during `invariants()` because `invariants()` cannot generate dynamic requests.
    Get(GetRequest),
    Query(QueryRequest),
}

fn arbitrary_collection_request(
    state: &FrontendReferenceState,
) -> impl Strategy<Value = CollectionRequest> {
    let collection = state.collection.clone().unwrap();
    let embedding_strategy = state.get_embedding_strategy();
    let known_ids = state.get_known_ids();

    let id_strategy = if known_ids.is_empty() {
        "\\PC{1,}".boxed()
    } else {
        prop_oneof![
            "\\PC{1,}",
            (Just(known_ids.clone()), any::<proptest::sample::Index>())
                .prop_map(|(known_ids, index)| { index.get(&known_ids).clone() }),
        ]
        .boxed()
    };

    let add_strategy = (1..=10usize)
        .prop_flat_map({
            let id_strategy = id_strategy.clone();
            let embedding_strategy = embedding_strategy.clone();

            move |num_records| {
                let ids = proptest::collection::vec(id_strategy.clone(), num_records);
                let embeddings = proptest::collection::vec(embedding_strategy.clone(), num_records);
                let documents = proptest::option::of(proptest::collection::vec(
                    proptest::option::of(DOCUMENT_TEXT_STRATEGY),
                    num_records,
                ));
                let metadatas = proptest::option::of(proptest::collection::vec(
                    proptest::option::of(arbitrary_metadata(0..=10usize)),
                    num_records,
                ));

                (ids, embeddings, documents, metadatas)
            }
        })
        .prop_map({
            let tenant = collection.tenant.clone();
            let database = collection.database.clone();
            let collection_id = collection.collection_id.clone();

            move |(ids, embeddings, documents, metadatas)| {
                CollectionRequest::Add(
                    AddCollectionRecordsRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id.clone(),
                        ids,
                        Some(embeddings),
                        documents,
                        None,
                        metadatas,
                    )
                    .unwrap(),
                )
            }
        });

    let update_strategy = (1..=10usize)
        .prop_flat_map({
            let id_strategy = id_strategy.clone();
            let embedding_strategy = embedding_strategy.clone();

            move |num_records| {
                let ids = proptest::collection::vec(id_strategy.clone(), num_records);
                let embeddings = proptest::option::of(proptest::collection::vec(
                    proptest::option::of(embedding_strategy.clone()),
                    num_records,
                ));
                let documents = proptest::option::of(proptest::collection::vec(
                    proptest::option::of(DOCUMENT_TEXT_STRATEGY),
                    num_records,
                ));
                let metadatas = proptest::option::of(proptest::collection::vec(
                    proptest::option::of(arbitrary_update_metadata(0..=10usize)),
                    num_records,
                ));

                (ids, embeddings, documents, metadatas)
            }
        })
        .prop_map({
            let tenant = collection.tenant.clone();
            let database = collection.database.clone();
            let collection_id = collection.collection_id.clone();

            move |(ids, embeddings, documents, metadatas)| {
                CollectionRequest::Update(
                    UpdateCollectionRecordsRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id.clone(),
                        ids,
                        embeddings,
                        documents,
                        None,
                        metadatas,
                    )
                    .unwrap(),
                )
            }
        });

    let upsert_strategy = (1..=10usize)
        .prop_flat_map({
            let id_strategy = id_strategy.clone();
            let embedding_strategy = embedding_strategy.clone();

            move |num_records| {
                let ids = proptest::collection::vec(id_strategy.clone(), num_records);
                let embeddings = proptest::collection::vec(embedding_strategy.clone(), num_records);
                let documents = proptest::option::of(proptest::collection::vec(
                    proptest::option::of(DOCUMENT_TEXT_STRATEGY),
                    num_records,
                ));
                let metadatas = proptest::option::of(proptest::collection::vec(
                    proptest::option::of(arbitrary_update_metadata(0..=10usize)),
                    num_records,
                ));

                (ids, embeddings, documents, metadatas)
            }
        })
        .prop_map({
            let tenant = collection.tenant.clone();
            let database = collection.database.clone();
            let collection_id = collection.collection_id.clone();

            move |(ids, embeddings, documents, metadatas)| {
                CollectionRequest::Upsert(
                    UpsertCollectionRecordsRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id.clone(),
                        ids,
                        Some(embeddings),
                        documents,
                        None,
                        metadatas,
                    )
                    .unwrap(),
                )
            }
        });

    let delete_strategy = prop_oneof![
        (
            Just::<Option<TestWhereFilter>>(None),
            proptest::collection::vec(id_strategy.clone(), 1..=10).prop_map(Some)
        ),
        (any::<TestWhereFilter>().prop_map(Some), Just(None)),
        (
            any::<TestWhereFilter>().prop_map(Some),
            proptest::collection::vec(id_strategy, 1..=10).prop_map(Some)
        ),
    ]
    .prop_map({
        let tenant = collection.tenant.clone();
        let database = collection.database.clone();
        let collection_id = collection.collection_id.clone();

        move |(filter, ids)| {
            CollectionRequest::Delete(
                DeleteCollectionRecordsRequest::try_new(
                    tenant.clone(),
                    database.clone(),
                    collection_id.clone(),
                    ids,
                    filter.map(|filter| filter.clause),
                )
                .unwrap(),
            )
        }
    });

    let compact_strategy = Just(CollectionRequest::Compact).boxed();

    prop_oneof![
        add_strategy,
        update_strategy,
        upsert_strategy,
        delete_strategy,
        arbitrary_get_request(state),
        arbitrary_query_request(state),
        compact_strategy
    ]
}

fn arbitrary_get_request(
    state: &FrontendReferenceState,
) -> impl Strategy<Value = CollectionRequest> {
    let collection = state.collection.clone().unwrap();

    // todo: should be lazy
    let mut frontend = state.frontend.clone().unwrap();
    let records = state.runtime.block_on(async {
        frontend
            .get(
                GetRequest::try_new(
                    collection.tenant.clone(),
                    collection.database.clone(),
                    collection.collection_id,
                    None,
                    None,
                    None,
                    0,
                    IncludeList(vec![Include::Metadata, Include::Document]),
                )
                .unwrap(),
            )
            .await
            .unwrap()
    });
    let documents = records
        .documents
        .unwrap()
        .into_iter()
        .filter_map(|doc| doc)
        .collect::<Vec<_>>();
    let metadatas = records
        .metadatas
        .unwrap()
        .into_iter()
        .filter_map(|meta| meta)
        .collect::<Vec<_>>();

    let where_strategy = any_with::<TestWhereFilter>(TestWhereFilterParams {
        seed_documents: Some(documents),
        seed_metadata: Some(metadatas),
        ..Default::default()
    });

    let known_ids = state.get_known_ids();

    let ids_strategy = if known_ids.len() > 0 {
        let known_ids_len = known_ids.len();
        prop_oneof![
            1 => proptest::collection::vec("\\PC{1,}", 0..10),
            2 => proptest::sample::subsequence(known_ids, 0..known_ids_len)
        ]
        .boxed()
    } else {
        proptest::collection::vec("\\PC{1,}", 0..10).boxed()
    };

    let include_list_strategy = any::<IncludeList>();

    prop_oneof![
        1 => (
            ids_strategy.clone().prop_map(Some),
            Just::<Option<TestWhereFilter>>(None),
            include_list_strategy.clone()
        ),
        5 => (
            Just::<Option<Vec<String>>>(None),
            where_strategy.clone().prop_map(Some),
            include_list_strategy.clone()
        ),
        2 => (ids_strategy.prop_map(Some), where_strategy.prop_map(Some), include_list_strategy),
    ]
    .prop_map({
        let tenant = collection.tenant.clone();
        let database = collection.database.clone();
        let collection_id = collection.collection_id.clone();

        move |(ids, filter, include_list)| {
            CollectionRequest::Get(
                GetRequest::try_new(
                    tenant.clone(),
                    database.clone(),
                    collection_id,
                    ids,
                    filter.map(|filter| filter.clause),
                    Some(10000), // todo
                    0,           // todo
                    include_list,
                )
                .unwrap(),
            )
        }
    })
}

fn arbitrary_query_request(
    state: &FrontendReferenceState,
) -> impl Strategy<Value = CollectionRequest> {
    let collection = state.collection.clone().unwrap();

    // todo: should be lazy
    let mut frontend = state.frontend.clone().unwrap();
    let records = state.runtime.block_on(async {
        frontend
            .get(
                GetRequest::try_new(
                    collection.tenant.clone(),
                    collection.database.clone(),
                    collection.collection_id,
                    None,
                    None,
                    None,
                    0,
                    IncludeList(vec![Include::Metadata, Include::Document]),
                )
                .unwrap(),
            )
            .await
            .unwrap()
    });
    let documents = records
        .documents
        .unwrap()
        .into_iter()
        .filter_map(|doc| doc)
        .collect::<Vec<_>>();
    let metadatas = records
        .metadatas
        .unwrap()
        .into_iter()
        .filter_map(|meta| meta)
        .collect::<Vec<_>>();

    let where_strategy = any_with::<TestWhereFilter>(TestWhereFilterParams {
        seed_documents: Some(documents),
        seed_metadata: Some(metadatas),
        ..Default::default()
    });

    let known_ids = state.get_known_ids();

    let ids_strategy = if known_ids.len() > 0 {
        let known_ids_len = known_ids.len();
        proptest::sample::subsequence(known_ids, 0..known_ids_len)
            .prop_map(Some)
            .boxed()
    } else {
        Just(None).boxed()
    };

    let embeddings_strategy = proptest::collection::vec(state.get_embedding_strategy(), 0..10);

    let n_results_strategy = (1..=100u32).boxed();
    let include_list_strategy = any::<IncludeList>();

    (
        prop_oneof![
            (
                ids_strategy.clone().prop_map(Some),
                Just::<Option<TestWhereFilter>>(None),
            ),
            (
                Just::<Option<Option<Vec<String>>>>(None),
                where_strategy.clone().prop_map(Some),
            ),
            (ids_strategy.prop_map(Some), where_strategy.prop_map(Some),),
            (Just(None), Just(None),),
        ],
        embeddings_strategy,
        n_results_strategy,
        include_list_strategy,
    )
        .prop_map({
            let tenant = collection.tenant.clone();
            let database = collection.database.clone();
            let collection_id = collection.collection_id.clone();

            move |((ids, filter), embeddings, n_results, include_list)| {
                CollectionRequest::Query(
                    QueryRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id,
                        ids.flatten(),
                        filter.map(|filter| filter.clause),
                        embeddings,
                        n_results,
                        include_list,
                    )
                    .unwrap(),
                )
            }
        })
}

fn check_get_responses_are_close_to_equal(reference: &GetResponse, received: &GetResponse) {
    assert_eq!(
        reference.ids, received.ids,
        "Expected {:?} to be equal to {:?}",
        reference.ids, received.ids
    );
    assert_eq!(
        reference.embeddings, received.embeddings,
        "Expected {:?} to be equal to {:?}",
        reference.embeddings, received.embeddings
    );
    assert_eq!(
        reference.documents, received.documents,
        "Expected {:?} to be equal to {:?}",
        reference.documents, received.documents
    );
    assert_eq!(
        reference.metadatas.is_none(),
        received.metadatas.is_none(),
        "Expected {:?} to be equal to {:?}",
        reference.metadatas,
        received.metadatas
    );

    if let Some(reference_metadatas) = reference.metadatas.as_ref() {
        if let Some(received_metadatas) = received.metadatas.as_ref() {
            assert_eq!(
                reference_metadatas.len(),
                received_metadatas.len(),
                "Expected {:?} to be equal to {:?}",
                reference,
                received
            );
            for i in 0..reference_metadatas.len() {
                let reference = &reference_metadatas[i];
                let received = &received_metadatas[i];

                assert_eq!(
                    reference.is_none(),
                    received.is_none(),
                    "Expected {:?} to be equal to {:?}",
                    reference,
                    received
                );

                if let Some(reference) = reference {
                    if let Some(received) = received {
                        assert!(
                            are_metadatas_close_to_equal(reference, received),
                            "Expected {:?} to be equal to {:?}",
                            reference,
                            received
                        );
                    }
                }
            }
        }
    }
}

// todo: check distances
fn check_query_responses_are_close_to_equal(reference: &QueryResponse, received: &QueryResponse) {
    assert_eq!(
        reference.ids, received.ids,
        "Expected {:?} to be equal to {:?}",
        reference.ids, received.ids
    );
    assert_eq!(
        reference.embeddings, received.embeddings,
        "Expected {:?} to be equal to {:?}",
        reference.embeddings, received.embeddings
    );
    assert_eq!(
        reference.documents, received.documents,
        "Expected {:?} to be equal to {:?}",
        reference.documents, received.documents
    );
    assert_eq!(
        reference.metadatas.is_none(),
        received.metadatas.is_none(),
        "Expected {:?} to be equal to {:?}",
        reference.metadatas,
        received.metadatas
    );

    if let Some(reference_metadatas_list) = reference.metadatas.as_ref() {
        if let Some(received_metadatas_list) = received.metadatas.as_ref() {
            assert_eq!(
                reference_metadatas_list.len(),
                received_metadatas_list.len(),
                "Expected {:?} to be equal to {:?}",
                reference_metadatas_list.len(),
                received_metadatas_list.len()
            );
            for i in 0..reference_metadatas_list.len() {
                let reference_metadatas = &reference_metadatas_list[i];
                let received_metadatas = &received_metadatas_list[i];

                assert_eq!(
                    reference_metadatas.len(),
                    received_metadatas.len(),
                    "Expected {:?} to be equal to {:?}",
                    reference_metadatas,
                    received_metadatas
                );

                for i in 0..reference_metadatas.len() {
                    let reference = &reference_metadatas[i];
                    let received = &received_metadatas[i];

                    assert_eq!(
                        reference.is_none(),
                        received.is_none(),
                        "Expected {:?} to be equal to {:?}",
                        reference,
                        received
                    );

                    if let Some(reference) = reference {
                        if let Some(received) = received {
                            assert!(
                                are_metadatas_close_to_equal(reference, received),
                                "Expected {:?} to be equal to {:?}",
                                reference,
                                received
                            );
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct FrontendReferenceState {
    collection: Option<Collection>,
    frontend: Option<Frontend>,
    runtime: Arc<tokio::runtime::Runtime>,
}

impl std::fmt::Debug for FrontendReferenceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrontendReferenceState")
            .field("collection", &self.collection)
            .finish()
    }
}

impl FrontendReferenceState {
    fn get_known_ids(&self) -> Vec<String> {
        let mut frontend = self.frontend.clone().unwrap();
        let collection = self.collection.clone().unwrap();

        self.runtime.block_on(async {
            let records = frontend
                .get(
                    GetRequest::try_new(
                        collection.tenant,
                        collection.database,
                        collection.collection_id,
                        None,
                        None,
                        None,
                        0,
                        IncludeList(vec![]),
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            let mut ids = records.ids;
            ids.sort_unstable();
            ids
        })
    }

    fn get_dimension(&self) -> usize {
        self.collection.clone().unwrap().dimension.unwrap() as usize
    }

    fn get_embedding_strategy(&self) -> impl Strategy<Value = Vec<f32>> + Clone {
        // todo: should shrink be enabled?
        proptest::collection::vec((0.0..=1.0f32).no_shrink(), self.get_dimension())
    }
}

struct FrontendStateMachine {}
impl ReferenceStateMachine for FrontendStateMachine {
    type State = FrontendReferenceState;
    type Transition = CollectionRequest;

    fn init_state() -> BoxedStrategy<Self::State> {
        let runtime = Arc::new(tokio::runtime::Runtime::new().unwrap());

        Just(FrontendReferenceState {
            collection: None,
            frontend: None,
            runtime,
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        if state.collection.is_some() {
            return arbitrary_collection_request(state).boxed();
        }

        (0..=100usize)
            .prop_map(|dimension| CollectionRequest::Init { dimension })
            .boxed()
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        if state.collection.is_none() && !matches!(transition, CollectionRequest::Init { .. }) {
            // First transition must always be CreateCollection
            return false;
        }

        if matches!(transition, CollectionRequest::Compact) {
            return false; // todo
        }

        return true;
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        let frontend = state.frontend.clone();

        state.runtime.block_on(async {
            match transition {
                CollectionRequest::Init { dimension } => {
                    let system = System::new();
                    let registry = Registry::new();
                    let mut config = FrontendConfig::single_node_default();
                    config.sqlitedb = Some(SqliteDBConfig {
                        url: None,
                        ..Default::default()
                    });
                    config.allow_reset = true;

                    let mut frontend = Frontend::try_from_config(&(config, system), &registry)
                        .await
                        .unwrap();

                    let mut collection = frontend
                        .create_collection(
                            CreateCollectionRequest::try_new(
                                "default_tenant".to_string(),
                                "default_database".to_string(),
                                "test".to_string(),
                                None,
                                None,
                                false,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    collection.dimension = Some(*dimension as i32);
                    state.collection = Some(collection);
                    state.frontend = Some(frontend);
                }
                CollectionRequest::Add(request) => {
                    // println!("applying transition: add");
                    let mut request = request.clone();
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    STATS.with_borrow_mut(|stats| stats.num_log_operations += request.ids.len());

                    frontend.unwrap().add(request).await.unwrap();
                }
                CollectionRequest::Update(request) => {
                    // println!("applying transition: update");
                    let mut request = request.clone();
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    STATS.with_borrow_mut(|stats| stats.num_log_operations += request.ids.len());

                    frontend.unwrap().update(request).await.unwrap();
                }
                CollectionRequest::Upsert(request) => {
                    // println!("applying transition: upsert");
                    let mut request = request.clone();
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    STATS.with_borrow_mut(|stats| stats.num_log_operations += request.ids.len());

                    frontend.unwrap().upsert(request).await.unwrap();
                }
                CollectionRequest::Delete(request) => {
                    // println!("applying transition: delete");
                    let mut request = request.clone();
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant.clone();
                    request.database_name = collection.database.clone();

                    let mut frontend = frontend.unwrap();

                    // Update stats
                    {
                        if request.r#where.is_some() {
                            let filtered_records = frontend
                                .clone()
                                .get(
                                    GetRequest::try_new(
                                        collection.tenant,
                                        collection.database,
                                        collection.collection_id,
                                        request.ids.clone(),
                                        request.r#where.clone(),
                                        None,
                                        0,
                                        IncludeList(vec![]),
                                    )
                                    .unwrap(),
                                )
                                .await
                                .unwrap();

                            STATS.with_borrow_mut(|stats| {
                                stats.num_log_operations += filtered_records.ids.len()
                            });
                        }

                        if let Some(ids) = &request.ids {
                            STATS.with_borrow_mut(|stats| stats.num_log_operations += ids.len());
                        }
                    }

                    frontend.delete(request).await.unwrap();
                }
                CollectionRequest::Get(_) => {
                    // (handled by the frontend under test)
                }
                CollectionRequest::Query(_) => {
                    // (handled by the frontend under test)
                }
                CollectionRequest::Compact => {
                    // state
                    //     .frontend
                    //     .manually_compact(
                    //         ManualCompactionRequest::try_new(state.collection.collection_id)
                    //             .unwrap(),
                    //     )
                    //     .await
                    //     .unwrap();
                }
            }
        });

        state
    }
}

// todo: rename?
struct FrontendUnderTest {
    collection: Option<Collection>,
    frontend: Frontend,
    runtime: tokio::runtime::Runtime,
}

impl StateMachineTest for FrontendUnderTest {
    type SystemUnderTest = Self;
    type Reference = FrontendStateMachine;

    fn init_test(
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) -> Self::SystemUnderTest {
        println!("starting test --------------------------------------");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let frontend = runtime.block_on(async {
            // let system = System::new();
            // let registry = Registry::new();

            // let mut frontend_config = FrontendConfig::load();
            // frontend_config.allow_reset = true;
            // if let chroma_sysdb::SysDbConfig::Grpc(ref mut grpc) = frontend_config.sysdb {
            //     grpc.host = "localhost".to_string();
            //     grpc.port = 50051;
            // }

            // if let LogConfig::Grpc(ref mut log) = frontend_config.log {
            //     log.host = "localhost".to_string();
            //     log.port = 50052;
            // }

            // if let CompactionClientConfig::Grpc(ref mut grpc) = frontend_config.compaction_client {
            //     grpc.url = "http://localhost:50054".to_string();
            // }

            // Frontend::try_from_config(&(frontend_config, system), &registry)
            //     .await
            //     .expect("Error creating Frontend Config")

            let system = System::new();
            let registry = Registry::new();
            let mut config = FrontendConfig::single_node_default();
            config.sqlitedb = Some(SqliteDBConfig {
                url: None,
                ..Default::default()
            });
            config.allow_reset = true;

            Frontend::try_from_config(&(config, system), &registry)
                .await
                .unwrap()
        });

        FrontendUnderTest {
            collection: None,
            frontend,
            runtime,
        }
    }

    fn apply(
        mut state: Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        transition: <Self::Reference as ReferenceStateMachine>::Transition,
    ) -> Self::SystemUnderTest {
        // println!("Applying transition: {:?}", transition);
        state.runtime.block_on(async {
            match transition {
                CollectionRequest::Init { .. } => {
                    state.frontend.reset().await.unwrap();

                    let collection = state
                        .frontend
                        .create_collection(
                            CreateCollectionRequest::try_new(
                                "default_tenant".to_string(),
                                "default_database".to_string(),
                                "test".to_string(),
                                None,
                                None,
                                false,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                    state.collection = Some(collection);
                }
                CollectionRequest::Add(mut request) => {
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    state.frontend.add(request.clone()).await.unwrap();
                }
                CollectionRequest::Update(mut request) => {
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    state.frontend.update(request.clone()).await.unwrap();
                }
                CollectionRequest::Upsert(mut request) => {
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    state.frontend.upsert(request.clone()).await.unwrap();
                }
                CollectionRequest::Delete(mut request) => {
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    state.frontend.delete(request.clone()).await.unwrap();
                }
                CollectionRequest::Get(mut request) => {
                    let expected_result = {
                        let collection = ref_state.collection.clone().unwrap();
                        request.collection_id = collection.collection_id;
                        request.tenant_id = collection.tenant;
                        request.database_name = collection.database;

                        ref_state
                            .frontend
                            .clone()
                            .unwrap()
                            .get(request.clone())
                            .await
                            .unwrap()
                    };

                    let count = {
                        let collection = ref_state.collection.clone().unwrap();
                        ref_state
                            .frontend
                            .clone()
                            .unwrap()
                            .count(
                                CountRequest::try_new(
                                    collection.tenant,
                                    collection.database,
                                    collection.collection_id,
                                )
                                .unwrap(),
                            )
                            .await
                            .unwrap()
                    };

                    if count > 0 {
                        let selectivity = expected_result.ids.len() as f64 / count as f64;

                        STATS.with_borrow_mut(|stats| {
                            if request.r#where.is_some() && request.ids.is_none() {
                                stats
                                    .get_request_selectivity
                                    .where_clause_only
                                    .push(selectivity);
                            } else if request.r#where.is_none() && request.ids.is_some() {
                                stats.get_request_selectivity.ids_only.push(selectivity);
                            } else if request.r#where.is_some() && request.ids.is_some() {
                                stats
                                    .get_request_selectivity
                                    .where_clause_and_ids
                                    .push(selectivity);
                            }
                        });
                    }

                    let received_result = {
                        let collection = state.collection.clone().unwrap();
                        request.collection_id = collection.collection_id;
                        request.tenant_id = collection.tenant;
                        request.database_name = collection.database;

                        state.frontend.get(request.clone()).await.unwrap()
                    };

                    check_get_responses_are_close_to_equal(&expected_result, &received_result);
                }
                CollectionRequest::Query(mut request) => {
                    let expected_result = {
                        let collection = ref_state.collection.clone().unwrap();
                        request.collection_id = collection.collection_id;
                        request.tenant_id = collection.tenant;
                        request.database_name = collection.database;

                        ref_state
                            .frontend
                            .clone()
                            .unwrap()
                            .query(request.clone())
                            .await
                            .unwrap()
                    };

                    let count = {
                        let collection = ref_state.collection.clone().unwrap();
                        ref_state
                            .frontend
                            .clone()
                            .unwrap()
                            .count(
                                CountRequest::try_new(
                                    collection.tenant,
                                    collection.database,
                                    collection.collection_id,
                                )
                                .unwrap(),
                            )
                            .await
                            .unwrap()
                    };

                    if count > 0 {
                        let selectivity = expected_result.ids.len() as f64 / count as f64;

                        STATS.with_borrow_mut(|stats| {
                            if request.r#where.is_some() && request.ids.is_none() {
                                stats
                                    .query_request_selectivity
                                    .where_clause_only
                                    .push(selectivity);
                            } else if request.r#where.is_none() && request.ids.is_some() {
                                stats.query_request_selectivity.ids_only.push(selectivity);
                            } else if request.r#where.is_some() && request.ids.is_some() {
                                stats
                                    .query_request_selectivity
                                    .where_clause_and_ids
                                    .push(selectivity);
                            } else {
                                stats
                                    .query_request_selectivity
                                    .embeddings_only
                                    .push(selectivity);
                            }
                        });
                    }

                    let received_result = {
                        let collection = state.collection.clone().unwrap();
                        request.collection_id = collection.collection_id;
                        request.tenant_id = collection.tenant;
                        request.database_name = collection.database;

                        state.frontend.query(request.clone()).await.unwrap()
                    };

                    check_query_responses_are_close_to_equal(&expected_result, &received_result);
                }
                CollectionRequest::Compact => {
                    state
                        .frontend
                        .manually_compact(
                            ManualCompactionRequest::try_new(
                                state.collection.as_ref().unwrap().collection_id,
                            )
                            .unwrap(),
                        )
                        .await
                        .unwrap();
                }
            }
        });

        state
    }

    fn check_invariants(
        state: &Self::SystemUnderTest,
        ref_state: &<Self::Reference as ReferenceStateMachine>::State,
    ) {
        let mut reference_frontend = match ref_state.frontend.as_ref() {
            Some(frontend) => frontend.clone(),
            None => return,
        };

        let reference_collection = match ref_state.collection.as_ref() {
            Some(collection) => collection.clone(),
            None => return,
        };

        let mut frontend_under_test = state.frontend.clone();
        let collection_under_test = match state.collection.clone() {
            Some(collection) => collection,
            None => return,
        };

        state.runtime.block_on(async move {
            let expected_count = reference_frontend
                .count(
                    CountRequest::try_new(
                        reference_collection.tenant.clone(),
                        reference_collection.database.clone(),
                        reference_collection.collection_id,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            let received_count = frontend_under_test
                .count(
                    CountRequest::try_new(
                        collection_under_test.tenant.clone(),
                        collection_under_test.database.clone(),
                        collection_under_test.collection_id,
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(
                expected_count, received_count,
                "Expected {:?} to be equal to {:?}",
                expected_count, received_count
            );

            // todo: test query

            let expected_results = reference_frontend
                .get(
                    GetRequest::try_new(
                        reference_collection.tenant.clone(),
                        reference_collection.database.clone(),
                        reference_collection.collection_id,
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

            let received_results = frontend_under_test
                .get(
                    GetRequest::try_new(
                        collection_under_test.tenant,
                        collection_under_test.database,
                        collection_under_test.collection_id,
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

            check_get_responses_are_close_to_equal(&expected_results, &received_results);
        });
    }
}

prop_state_machine! {
     #![proptest_config(proptest::test_runner::Config {
            cases: 50,
        // cases: 10,
            // verbose: 2,
            // fork: true,
            ..proptest::test_runner::Config::default()
        })]
    #[test]
    // todo
    fn test_collection(sequential 1..100usize => FrontendUnderTest);
}
