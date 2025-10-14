use crate::CollectionRequest;
use chroma_frontend::impls::in_memory_frontend::InMemoryFrontend;
use chroma_types::{Collection, CreateCollectionRequest, GetRequest, IncludeList};
use proptest::prelude::*;
use proptest_state_machine::ReferenceStateMachine;
use std::sync::Arc;

use super::arbitrary::CollectionRequestArbitraryParams;

#[derive(Clone)]
pub(crate) struct FrontendReferenceState {
    pub collection: Option<Collection>,
    pub frontend: Option<InMemoryFrontend>,
    pub runtime: Arc<tokio::runtime::Runtime>,
}

impl Default for FrontendReferenceState {
    fn default() -> Self {
        Self {
            collection: None,
            frontend: None,
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
        }
    }
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
        let frontend = self.frontend.as_ref().unwrap();
        let collection = self.collection.clone().unwrap();

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
            .unwrap();

        let mut ids = records.ids;
        ids.sort_unstable();
        ids
    }

    pub fn get_dimension(&self) -> usize {
        self.collection.clone().unwrap().dimension.unwrap() as usize
    }

    pub fn get_embedding_strategy(&self) -> impl Strategy<Value = Vec<f32>> + Clone {
        // todo: should shrink be enabled?
        // todo: try storing embedding strategy on self
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
            return proptest::arbitrary::arbitrary_with::<CollectionRequest, _, _>(
                CollectionRequestArbitraryParams {
                    current_state: state.clone(),
                    ..Default::default()
                },
            )
            .boxed();
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

        // ID filtering on query requests can only include existing IDs
        if let CollectionRequest::Query(request) = transition {
            if let Some(ids) = &request.ids {
                let known_ids = state.get_known_ids();
                if !ids.iter().all(|id| known_ids.contains(id)) {
                    return false;
                }
            }
        }

        true
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        if let CollectionRequest::Init { dimension } = transition {
            let mut frontend = InMemoryFrontend::new();

            let mut collection = frontend
                .create_collection(
                    CreateCollectionRequest::try_new(
                        "default_tenant".to_string(),
                        "default_database".to_string(),
                        "test".to_string(),
                        None,
                        None,
                        None,
                        false,
                    )
                    .unwrap(),
                )
                .unwrap();
            collection.dimension = Some(*dimension as i32);
            state.collection = Some(collection);
            state.frontend = Some(frontend);
            return state;
        }

        let frontend = state.frontend.as_mut();

        match transition {
            CollectionRequest::Init { .. } => {
                unreachable!()
            }
            CollectionRequest::Add(request) => {
                let mut request = request.clone();
                let collection = state.collection.clone().unwrap();
                request.collection_id = collection.collection_id;
                request.tenant_id = collection.tenant;
                request.database_name = collection.database;

                frontend.unwrap().add(request).unwrap();
            }
            CollectionRequest::Update(request) => {
                let mut request = request.clone();
                let collection = state.collection.clone().unwrap();
                request.collection_id = collection.collection_id;
                request.tenant_id = collection.tenant;
                request.database_name = collection.database;

                frontend.unwrap().update(request).unwrap();
            }
            CollectionRequest::Upsert(request) => {
                let mut request = request.clone();
                let collection = state.collection.clone().unwrap();
                request.collection_id = collection.collection_id;
                request.tenant_id = collection.tenant;
                request.database_name = collection.database;

                frontend.unwrap().upsert(request).unwrap();
            }
            CollectionRequest::Delete(request) => {
                let mut request = request.clone();
                let collection = state.collection.clone().unwrap();
                request.collection_id = collection.collection_id;
                request.tenant_id = collection.tenant.clone();
                request.database_name = collection.database.clone();

                frontend.unwrap().delete(request).unwrap();
            }
            CollectionRequest::Get(_) => {
                // (handled by the frontend under test)
            }
            CollectionRequest::Query(_) => {
                // (handled by the frontend under test)
            }
            CollectionRequest::Compact => {}
        }

        state
    }
}
