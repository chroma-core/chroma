use crate::CollectionRequest;
use chroma_distance::DistanceFunction;
use chroma_frontend::impls::in_memory_frontend::InMemoryFrontend;
use chroma_types::{
    Collection, CreateCollectionRequest, GetRequest, Include, IncludeList, QueryRequest,
    QueryResponse,
};
use proptest::prelude::*;
use proptest_state_machine::ReferenceStateMachine;
use std::collections::HashSet;
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

    pub fn ann_accuracy(&self, request: QueryRequest, response_to_validate: &QueryResponse) {
        let min_recall = 0.5; // todo
        let distance_function = DistanceFunction::Euclidean;
        let accuracy_threshold = 10_f64.powi(self.get_dimension().ilog10() as i32) * 1e-6;
        // todo: handle other distance functions (need to update collection creation)

        let collection = self.collection.clone().unwrap();
        let frontend = self.frontend.as_ref().unwrap();
        let filtered_records = frontend
            .get(
                GetRequest::try_new(
                    collection.tenant,
                    collection.database,
                    collection.collection_id,
                    request.ids,
                    request.r#where,
                    None,
                    0,
                    IncludeList(vec![Include::Embedding]),
                )
                .unwrap(),
            )
            .unwrap();

        for query_i in 0..request.embeddings.len() {
            let mut missing = 0;

            let query_embedding = &request.embeddings[query_i];

            let mut record_index_and_distance_from_query = vec![];
            for (i, record_embedding) in filtered_records
                .embeddings
                .as_ref()
                .unwrap()
                .iter()
                .enumerate()
            {
                let distance = distance_function.distance(&query_embedding, record_embedding);
                record_index_and_distance_from_query.push((i, distance));
            }

            let mut sorted_record_index_and_distance_from_query =
                record_index_and_distance_from_query.clone();
            sorted_record_index_and_distance_from_query
                .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

            let received_ids = response_to_validate.ids[query_i]
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            let expected_ids = sorted_record_index_and_distance_from_query
                [..(request.n_results as usize).min(record_index_and_distance_from_query.len())]
                .iter()
                .map(|(i, _)| filtered_records.ids[*i].clone())
                .collect::<HashSet<_>>();

            missing += expected_ids.difference(&received_ids).count();

            for (i, received_id) in response_to_validate.ids[query_i].iter().enumerate() {
                let was_unexpected = !expected_ids.contains(received_id);
                let reference_i = filtered_records
                    .ids
                    .iter()
                    .position(|id| id == received_id)
                    .unwrap();

                let received_distance =
                    response_to_validate.distances.as_ref().unwrap()[query_i][i].unwrap();
                let expected_distance = distance_function.distance(
                    query_embedding,
                    &filtered_records.embeddings.as_ref().unwrap()[reference_i],
                );

                let correct_distance = (received_distance as f64 - expected_distance as f64).abs()
                    < accuracy_threshold;
                if was_unexpected {
                    if correct_distance {
                        missing -= 1;
                    } else {
                        // continue
                    }
                } else {
                    assert!(
                        correct_distance,
                        "Expected distance to be within {:.6} of {:.6}, was {:.6} for ID {}.",
                        accuracy_threshold, expected_distance, received_distance, received_id,
                    );
                }
            }

            let size = expected_ids.len();
            if size > 0 {
                let recall = (size - missing) as f32 / size as f32; // todo?
                assert!(
                    recall >= min_recall,
                    "Expected recall to be >= {:.2}, was {:.2}. Missing {} out of {}, accuracy threshold {}.",
                    min_recall,
                    recall,
                    missing,
                    size,
                    accuracy_threshold
                );
            }
        }

        // todo: results are sorted by distance
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
