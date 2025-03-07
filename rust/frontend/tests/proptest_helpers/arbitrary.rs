use crate::{CollectionRequest, FrontendReferenceState};
use chroma_types::{
    strategies::{
        arbitrary_metadata, arbitrary_update_metadata, TestWhereFilter, TestWhereFilterParams,
        DOCUMENT_TEXT_STRATEGY,
    },
    AddCollectionRecordsRequest, DeleteCollectionRecordsRequest, GetRequest, Include, IncludeList,
    QueryRequest, UpdateCollectionRecordsRequest, UpsertCollectionRecordsRequest,
};
use proptest::prelude::*;

/// Generates an arbitrary collection request given the current reference frontend state.
/// When the reference frontend has at least one ID, there's a 50/50 chance of generated transitions using an existing ID.
/// Add, update, and upsert transitions will have at most 10 records.
/// Generated get requests with document/metadata filtering are heavily biased towards filtering on current documents/metadata values rather than a completely randomized filter.
pub(crate) fn arbitrary_collection_request(
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
            let collection_id = collection.collection_id;

            move |(ids, embeddings, documents, metadatas)| {
                CollectionRequest::Add(
                    AddCollectionRecordsRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id,
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
            let collection_id = collection.collection_id;

            move |(ids, embeddings, documents, metadatas)| {
                CollectionRequest::Update(
                    UpdateCollectionRecordsRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id,
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
            let collection_id = collection.collection_id;

            move |(ids, embeddings, documents, metadatas)| {
                CollectionRequest::Upsert(
                    UpsertCollectionRecordsRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id,
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
        let collection_id = collection.collection_id;

        move |(filter, ids)| {
            CollectionRequest::Delete(
                DeleteCollectionRecordsRequest::try_new(
                    tenant.clone(),
                    database.clone(),
                    collection_id,
                    ids,
                    filter.map(|filter| filter.clause),
                )
                .unwrap(),
            )
        }
    });

    prop_oneof![
        add_strategy,
        update_strategy,
        upsert_strategy,
        delete_strategy,
        arbitrary_get_request(state),
        // todo: enable KNN requests
        // arbitrary_query_request(state),
    ]
}

fn arbitrary_get_request(
    state: &FrontendReferenceState,
) -> impl Strategy<Value = CollectionRequest> {
    let collection = state.collection.clone().unwrap();

    let frontend = state.frontend.clone().unwrap();
    let records = frontend
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
        .unwrap();
    let documents = records
        .documents
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let metadatas = records
        .metadatas
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let where_strategy = any_with::<TestWhereFilter>(TestWhereFilterParams {
        seed_documents: Some(documents),
        seed_metadata: Some(metadatas),
        ..Default::default()
    });

    let known_ids = state.get_known_ids();

    let ids_strategy = if !known_ids.is_empty() {
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

    (
        prop_oneof![
            1 => (
                ids_strategy.clone().prop_map(Some),
                Just::<Option<TestWhereFilter>>(None),
            ),
            5 => (
                Just::<Option<Vec<String>>>(None),
                where_strategy.clone().prop_map(Some),
            ),
            2 => (ids_strategy.prop_map(Some), where_strategy.prop_map(Some)),
        ],
        include_list_strategy,
        proptest::option::weighted(0.1, 0..100u32),
        proptest::option::weighted(0.1, 0..100u32).prop_map(|offset| offset.unwrap_or(0)),
    )
        .prop_map({
            let tenant = collection.tenant.clone();
            let database = collection.database.clone();
            let collection_id = collection.collection_id;

            move |((ids, filter), include_list, limit, offset)| {
                CollectionRequest::Get(
                    GetRequest::try_new(
                        tenant.clone(),
                        database.clone(),
                        collection_id,
                        ids,
                        filter.map(|filter| filter.clause),
                        limit,
                        offset,
                        include_list,
                    )
                    .unwrap(),
                )
            }
        })
}

#[allow(dead_code)]
fn arbitrary_query_request(
    state: &FrontendReferenceState,
) -> impl Strategy<Value = CollectionRequest> {
    let collection = state.collection.clone().unwrap();

    let frontend = state.frontend.clone().unwrap();
    let records = frontend
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
        .unwrap();
    let documents = records
        .documents
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    let metadatas = records
        .metadatas
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let where_strategy = any_with::<TestWhereFilter>(TestWhereFilterParams {
        seed_documents: Some(documents),
        seed_metadata: Some(metadatas),
        ..Default::default()
    });

    let known_ids = state.get_known_ids();

    let ids_strategy = if !known_ids.is_empty() {
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
            let collection_id = collection.collection_id;

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
