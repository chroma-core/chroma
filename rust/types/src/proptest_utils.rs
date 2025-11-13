#[cfg(feature = "testing")]
#[allow(dead_code)] // Functions are used in test modules that are conditionally compiled
pub mod strategies {
    use crate::hnsw_configuration::Space;
    use crate::{
        strategies::TEST_NAME_PATTERN, EmbeddingFunctionConfiguration,
        EmbeddingFunctionNewConfiguration, InternalCollectionConfiguration,
        InternalHnswConfiguration, InternalSpannConfiguration, KnnIndex, VectorIndexConfiguration,
    };
    use proptest::prelude::*;
    use proptest::string::string_regex;
    use serde_json::json;

    pub fn embedding_function_strategy(
    ) -> impl Strategy<Value = Option<EmbeddingFunctionConfiguration>> {
        let known_strategy = string_regex(TEST_NAME_PATTERN).unwrap().prop_map(|name| {
            EmbeddingFunctionConfiguration::Known(EmbeddingFunctionNewConfiguration {
                name,
                config: json!({ "alpha": 1 }),
            })
        });

        proptest::option::of(prop_oneof![
            Just(EmbeddingFunctionConfiguration::Legacy),
            known_strategy,
        ])
    }

    pub fn space_strategy() -> impl Strategy<Value = Space> {
        prop_oneof![Just(Space::L2), Just(Space::Cosine), Just(Space::Ip),]
    }

    pub fn internal_hnsw_configuration_strategy() -> impl Strategy<Value = InternalHnswConfiguration>
    {
        (
            space_strategy(),
            1usize..=256,
            1usize..=256,
            1usize..=64,
            1usize..=32,
            prop_oneof![Just(0.5f64), Just(1.0f64), Just(1.5f64), Just(2.0f64)],
            2usize..=4096,
            2usize..=4096,
        )
            .prop_map(
                |(
                    space,
                    ef_construction,
                    ef_search,
                    max_neighbors,
                    num_threads,
                    resize_factor,
                    sync_threshold,
                    batch_size,
                )| InternalHnswConfiguration {
                    space,
                    ef_construction,
                    ef_search,
                    max_neighbors,
                    num_threads,
                    resize_factor,
                    sync_threshold,
                    batch_size,
                },
            )
    }

    pub fn spann_epsilon_strategy() -> impl Strategy<Value = f32> {
        prop_oneof![Just(5.0f32), Just(7.5f32), Just(10.0f32)]
    }

    pub fn internal_spann_configuration_strategy(
    ) -> impl Strategy<Value = InternalSpannConfiguration> {
        (
            (
                1u32..=128,               // search_nprobe
                Just(1.0f32),             // search_rng_factor (validated == 1.0)
                spann_epsilon_strategy(), // search_rng_epsilon ∈ [5, 10]
                1u32..=64,                // write_nprobe (max 64)
                1u32..=8,                 // nreplica_count (max 8)
                Just(1.0f32),             // write_rng_factor (validated == 1.0)
                spann_epsilon_strategy(), // write_rng_epsilon ∈ [5, 10]
                50u32..=200,              // split_threshold (min 50, max 200)
                1usize..=1000,            // num_samples_kmeans (max 1000)
            ),
            (
                Just(100.0f32),   // initial_lambda (validated == 100)
                1u32..=64,        // reassign_neighbor_count (max 64)
                25u32..=100,      // merge_threshold (min 25, max 100)
                1u32..=8,         // num_centers_to_merge_to (max 8)
                space_strategy(), // space
                1usize..=200,     // ef_construction (max 200)
                1usize..=200,     // ef_search (max 200)
                1usize..=64,      // max_neighbors (max 64)
            ),
        )
            .prop_map(
                |(
                    (
                        search_nprobe,
                        search_rng_factor,
                        search_rng_epsilon,
                        write_nprobe,
                        nreplica_count,
                        write_rng_factor,
                        write_rng_epsilon,
                        split_threshold,
                        num_samples_kmeans,
                    ),
                    (
                        initial_lambda,
                        reassign_neighbor_count,
                        merge_threshold,
                        num_centers_to_merge_to,
                        space,
                        ef_construction,
                        ef_search,
                        max_neighbors,
                    ),
                )| InternalSpannConfiguration {
                    search_nprobe,
                    search_rng_factor,
                    search_rng_epsilon,
                    write_nprobe,
                    nreplica_count,
                    write_rng_factor,
                    write_rng_epsilon,
                    split_threshold,
                    num_samples_kmeans,
                    initial_lambda,
                    reassign_neighbor_count,
                    merge_threshold,
                    num_centers_to_merge_to,
                    space,
                    ef_construction,
                    ef_search,
                    max_neighbors,
                },
            )
    }

    pub fn knn_index_strategy() -> impl Strategy<Value = KnnIndex> {
        prop_oneof![Just(KnnIndex::Hnsw), Just(KnnIndex::Spann),]
    }

    pub fn internal_collection_configuration_strategy(
    ) -> impl Strategy<Value = InternalCollectionConfiguration> {
        prop_oneof![
            (
                internal_hnsw_configuration_strategy(),
                embedding_function_strategy()
            )
                .prop_map(|(hnsw, embedding_function)| {
                    InternalCollectionConfiguration {
                        vector_index: VectorIndexConfiguration::Hnsw(hnsw),
                        embedding_function,
                    }
                }),
            (
                internal_spann_configuration_strategy(),
                embedding_function_strategy()
            )
                .prop_map(|(spann, embedding_function)| {
                    InternalCollectionConfiguration {
                        vector_index: VectorIndexConfiguration::Spann(spann),
                        embedding_function,
                    }
                }),
        ]
    }
}
