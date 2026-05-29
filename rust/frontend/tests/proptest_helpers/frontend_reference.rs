use crate::CollectionRequest;
use chroma_frontend::impls::in_memory_frontend::InMemoryFrontend;
use chroma_types::{
    AddCollectionRecordsRequest, Collection, CreateCollectionRequest, DatabaseName,
    DeleteCollectionRecordsRequest, GetRequest, IncludeList, Metadata, MetadataValue,
    UpdateCollectionRecordsRequest, UpdateMetadata, UpsertCollectionRecordsRequest, CHROMA_KEY,
};
use proptest::prelude::*;
use proptest_state_machine::ReferenceStateMachine;
use std::collections::BTreeMap;
use std::sync::Arc;

use super::arbitrary::CollectionRequestArbitraryParams;

#[derive(Clone, Debug, Default)]
pub(crate) struct FrontendRecordState {
    document: Option<String>,
    metadata: Option<Metadata>,
}

#[derive(Clone, Debug)]
pub(crate) struct FrontendGenerationState {
    pub collection: Collection,
    pub dimension: usize,
    pub known_ids: Vec<String>,
    pub seed_documents: Vec<String>,
    pub seed_metadata: Vec<Metadata>,
}

impl FrontendGenerationState {
    pub fn get_embedding_strategy(&self) -> impl Strategy<Value = Vec<f32>> + Clone {
        proptest::collection::vec((0.0..=1.0f32).no_shrink(), self.dimension)
    }
}

#[derive(Clone)]
pub(crate) struct FrontendReferenceState {
    pub collection: Option<Collection>,
    pub frontend: Option<InMemoryFrontend>,
    pub runtime: Arc<tokio::runtime::Runtime>,
    records: BTreeMap<String, FrontendRecordState>,
}

impl Default for FrontendReferenceState {
    fn default() -> Self {
        Self {
            collection: None,
            frontend: None,
            runtime: Arc::new(tokio::runtime::Runtime::new().unwrap()),
            records: BTreeMap::new(),
        }
    }
}

impl std::fmt::Debug for FrontendReferenceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrontendReferenceState")
            .field("collection", &self.collection)
            .field("record_count", &self.records.len())
            .finish()
    }
}

impl FrontendReferenceState {
    pub fn get_known_ids(&self) -> Vec<String> {
        self.records.keys().cloned().collect()
    }

    pub fn contains_known_id(&self, id: &str) -> bool {
        self.records.contains_key(id)
    }

    pub fn current_count(&self) -> u32 {
        self.records.len() as u32
    }

    pub fn get_dimension(&self) -> usize {
        self.collection.clone().unwrap().dimension.unwrap() as usize
    }

    pub fn generation_state(&self) -> FrontendGenerationState {
        FrontendGenerationState {
            collection: self.collection.clone().unwrap(),
            dimension: self.get_dimension(),
            known_ids: self.get_known_ids(),
            seed_documents: self
                .records
                .values()
                .filter_map(|record| record.document.clone())
                .collect(),
            seed_metadata: self
                .records
                .values()
                .filter_map(|record| record.metadata.clone())
                .collect(),
        }
    }

    fn metadata_from_add_metadata(metadata: &Metadata) -> Option<Metadata> {
        let metadata = metadata
            .iter()
            .filter(|(key, _)| !key.starts_with(CHROMA_KEY))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Metadata>();

        if metadata.is_empty() {
            None
        } else {
            Some(metadata)
        }
    }

    fn merge_update_metadata(metadata: &mut Option<Metadata>, update: Option<&UpdateMetadata>) {
        let Some(update) = update else {
            return;
        };

        let mut merged = metadata.clone().unwrap_or_default();
        let mut changed = false;

        for (key, value) in update {
            if key.starts_with(CHROMA_KEY) {
                continue;
            }

            changed = true;
            match MetadataValue::try_from(value) {
                Ok(value) => {
                    merged.insert(key.clone(), value);
                }
                Err(_) => {
                    merged.remove(key);
                }
            }
        }

        if changed {
            *metadata = Some(merged);
        }
    }

    fn optional_value<T: Clone>(values: &Option<Vec<Option<T>>>, index: usize) -> Option<T> {
        values
            .as_ref()
            .and_then(|values| values.get(index))
            .cloned()
            .flatten()
    }

    fn optional_ref<T>(values: &Option<Vec<Option<T>>>, index: usize) -> Option<&T> {
        values
            .as_ref()
            .and_then(|values| values.get(index))
            .and_then(|value| value.as_ref())
    }

    fn apply_add_to_records(&mut self, request: &AddCollectionRecordsRequest) {
        for (index, id) in request.ids.iter().enumerate() {
            if self.records.contains_key(id) {
                continue;
            }

            let metadata = Self::optional_ref(&request.metadatas, index)
                .and_then(Self::metadata_from_add_metadata);
            self.records.insert(
                id.clone(),
                FrontendRecordState {
                    document: Self::optional_value(&request.documents, index),
                    metadata,
                },
            );
        }
    }

    fn apply_update_to_records(&mut self, request: &UpdateCollectionRecordsRequest) {
        for (index, id) in request.ids.iter().enumerate() {
            let Some(record) = self.records.get_mut(id) else {
                continue;
            };

            if let Some(document) = Self::optional_value(&request.documents, index) {
                record.document = Some(document);
            }
            Self::merge_update_metadata(
                &mut record.metadata,
                Self::optional_ref(&request.metadatas, index),
            );
        }
    }

    fn apply_upsert_to_records(&mut self, request: &UpsertCollectionRecordsRequest) {
        for (index, id) in request.ids.iter().enumerate() {
            let update_metadata = Self::optional_ref(&request.metadatas, index);

            if let Some(record) = self.records.get_mut(id) {
                if let Some(document) = Self::optional_value(&request.documents, index) {
                    record.document = Some(document);
                }
                Self::merge_update_metadata(&mut record.metadata, update_metadata);
            } else {
                let mut metadata = None;
                Self::merge_update_metadata(&mut metadata, update_metadata);
                self.records.insert(
                    id.clone(),
                    FrontendRecordState {
                        document: Self::optional_value(&request.documents, index),
                        metadata,
                    },
                );
            }
        }
    }

    fn ids_matching_delete(&self, request: &DeleteCollectionRecordsRequest) -> Vec<String> {
        if request.r#where.is_none() {
            return request.ids.clone().unwrap_or_default();
        }

        self.frontend
            .as_ref()
            .unwrap()
            .get(
                GetRequest::try_new(
                    request.tenant_id.clone(),
                    request.database_name.clone(),
                    request.collection_id,
                    request.ids.clone(),
                    request.r#where.clone(),
                    request.limit,
                    0,
                    IncludeList(vec![]),
                )
                .unwrap(),
            )
            .unwrap()
            .ids
    }

    fn apply_delete_to_records(&mut self, deleted_ids: &[String]) {
        for id in deleted_ids {
            self.records.remove(id);
        }
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
            records: BTreeMap::new(),
        })
        .boxed()
    }

    fn transitions(state: &Self::State) -> BoxedStrategy<Self::Transition> {
        if state.collection.is_some() {
            return proptest::arbitrary::arbitrary_with::<CollectionRequest, _, _>(
                CollectionRequestArbitraryParams::new(state.generation_state()),
            )
            .boxed();
        }

        let dimension_range = if crate::frontend_deep_profile_enabled() {
            3..=100usize
        } else {
            3..=32usize
        };

        dimension_range
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
                if !ids.iter().all(|id| state.contains_known_id(id)) {
                    return false;
                }
            }
        }

        true
    }

    fn apply(mut state: Self::State, transition: &Self::Transition) -> Self::State {
        if let CollectionRequest::Init { dimension } = transition {
            let mut frontend = InMemoryFrontend::new();
            let database_name =
                DatabaseName::new("default_database").expect("database name should be valid");

            let mut collection = frontend
                .create_collection(
                    CreateCollectionRequest::try_new(
                        "default_tenant".to_string(),
                        database_name,
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
            state.records.clear();
            return state;
        }

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

                state
                    .frontend
                    .as_mut()
                    .unwrap()
                    .add(request.clone())
                    .unwrap();
                state.apply_add_to_records(&request);
            }
            CollectionRequest::Update(request) => {
                let mut request = request.clone();
                let collection = state.collection.clone().unwrap();
                request.collection_id = collection.collection_id;
                request.tenant_id = collection.tenant;
                request.database_name = collection.database;

                state
                    .frontend
                    .as_mut()
                    .unwrap()
                    .update(request.clone())
                    .unwrap();
                state.apply_update_to_records(&request);
            }
            CollectionRequest::Upsert(request) => {
                let mut request = request.clone();
                let collection = state.collection.clone().unwrap();
                request.collection_id = collection.collection_id;
                request.tenant_id = collection.tenant;
                request.database_name = collection.database;

                state
                    .frontend
                    .as_mut()
                    .unwrap()
                    .upsert(request.clone())
                    .unwrap();
                state.apply_upsert_to_records(&request);
            }
            CollectionRequest::Delete(request) => {
                let mut request = request.clone();
                let collection = state.collection.clone().unwrap();
                request.collection_id = collection.collection_id;
                request.tenant_id = collection.tenant.clone();
                request.database_name = collection.database.clone();

                let deleted_ids = state.ids_matching_delete(&request);
                state
                    .frontend
                    .as_mut()
                    .unwrap()
                    .delete(request.clone())
                    .unwrap();
                state.apply_delete_to_records(&deleted_ids);
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
