use super::arbitrary::arbitrary_collection_request;
use crate::CollectionRequest;
use chroma_config::{registry::Registry, Configurable};
use chroma_frontend::{frontend::Frontend, FrontendConfig};
use chroma_sqlite::config::SqliteDBConfig;
use chroma_system::System;
use chroma_types::{
    Collection, CreateCollectionRequest, GetRequest, IncludeList, ManualCompactionRequest,
};
use proptest::prelude::*;
use proptest_state_machine::ReferenceStateMachine;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct FrontendReferenceState {
    pub collection: Option<Collection>,
    pub frontend: Option<Frontend>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

impl std::fmt::Debug for FrontendReferenceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrontendReferenceState")
            .field("collection", &self.collection)
            .finish()
    }
}

impl FrontendReferenceState {
    pub fn get_known_ids(&self) -> Vec<String> {
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

    pub fn get_dimension(&self) -> usize {
        self.collection.clone().unwrap().dimension.unwrap() as usize
    }

    pub fn get_embedding_strategy(&self) -> impl Strategy<Value = Vec<f32>> + Clone {
        // todo: should shrink be enabled?
        proptest::collection::vec((0.0..=1.0f32).no_shrink(), self.get_dimension())
    }
}

pub(crate) struct FrontendReferenceStateMachine {}
impl ReferenceStateMachine for FrontendReferenceStateMachine {
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

        (3..=100usize)
            .prop_map(|dimension| CollectionRequest::Init { dimension })
            .boxed()
    }

    fn preconditions(state: &Self::State, transition: &Self::Transition) -> bool {
        if state.collection.is_none() && !matches!(transition, CollectionRequest::Init { .. }) {
            // First transition must always be CreateCollection
            return false;
        }

        return true;
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        let frontend = state.frontend.clone();
        let collection = state.collection.clone();

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

                    frontend.unwrap().add(request).await.unwrap();
                }
                CollectionRequest::Update(request) => {
                    // println!("applying transition: update");
                    let mut request = request.clone();
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

                    frontend.unwrap().update(request).await.unwrap();
                }
                CollectionRequest::Upsert(request) => {
                    // println!("applying transition: upsert");
                    let mut request = request.clone();
                    let collection = state.collection.clone().unwrap();
                    request.collection_id = collection.collection_id;
                    request.tenant_id = collection.tenant;
                    request.database_name = collection.database;

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

                    frontend.delete(request).await.unwrap();
                }
                CollectionRequest::Get(_) => {
                    // (handled by the frontend under test)
                }
                CollectionRequest::Query(_) => {
                    // (handled by the frontend under test)
                }
                CollectionRequest::Compact => {
                    frontend
                        .unwrap()
                        .manually_compact(
                            ManualCompactionRequest::try_new(collection.unwrap().collection_id)
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                }
            }
        });

        state
    }
}
