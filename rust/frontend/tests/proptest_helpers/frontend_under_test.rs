use crate::{define_thread_local_stats, CollectionRequest, FrontendReferenceStateMachine};
use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{frontend::Frontend, FrontendConfig};
use chroma_sqlite::config::SqliteDBConfig;
use chroma_system::System;
use chroma_types::{
    Collection, CountRequest, CreateCollectionRequest, GetRequest, IncludeList,
    ManualCompactionRequest,
};
use proptest_state_machine::{ReferenceStateMachine, StateMachineTest};

use super::assertions::{
    check_get_responses_are_close_to_equal, check_query_responses_are_close_to_equal,
};

define_thread_local_stats!(STATS);

// todo: rename?
pub(crate) struct FrontendUnderTest {
    collection: Option<Collection>,
    frontend: Frontend,
    runtime: tokio::runtime::Runtime,
}

impl StateMachineTest for FrontendUnderTest {
    type SystemUnderTest = Self;
    type Reference = FrontendReferenceStateMachine;

    fn init_test(
        _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
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

                    STATS.with_borrow_mut(|stats| stats.num_log_operations += request.ids.len());

                    state.frontend.add(request.clone()).await.unwrap();
                }
                CollectionRequest::Update(mut request) => {
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    STATS.with_borrow_mut(|stats| stats.num_log_operations += request.ids.len());

                    state.frontend.update(request.clone()).await.unwrap();
                }
                CollectionRequest::Upsert(mut request) => {
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    STATS.with_borrow_mut(|stats| stats.num_log_operations += request.ids.len());

                    state.frontend.upsert(request.clone()).await.unwrap();
                }
                CollectionRequest::Delete(mut request) => {
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant.clone();
                    request.database_name = collection.database.clone();

                    // Update stats
                    {
                        if request.r#where.is_some() {
                            // todo: count instead?
                            let filtered_records = state
                                .frontend
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
