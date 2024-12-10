use std::collections::HashMap;

use crate::{
    Distribution, DocumentQuery, GetQuery, KeySelector, MetadataQuery, QueryQuery, Skew, Workload,
};

/// Return a map of all pre-configured workloads.
pub fn all_workloads() -> HashMap<String, Workload> {
    HashMap::from_iter([
        (
            "get-no-filter".to_string(),
            Workload::Get(GetQuery {
                skew: Skew::Zipf { theta: 0.999 },
                limit: Distribution::Constant(10),
                metadata: None,
                document: None,
            }),
        ),
        (
            "get-document".to_string(),
            Workload::Get(GetQuery {
                skew: Skew::Zipf { theta: 0.999 },
                limit: Distribution::Constant(10),
                metadata: None,
                document: Some(DocumentQuery::Raw(serde_json::json!({"$contains": "the"}))),
            }),
        ),
        (
            "get-metadata".to_string(),
            Workload::Get(GetQuery {
                skew: Skew::Zipf { theta: 0.999 },
                limit: Distribution::Constant(10),
                metadata: Some(MetadataQuery::Raw(serde_json::json!({"i1": 1000}))),
                document: None,
            }),
        ),
        (
            "query-no-filter".to_string(),
            Workload::Query(QueryQuery {
                skew: Skew::Zipf { theta: 0.999 },
                limit: Distribution::Constant(10),
                metadata: None,
                document: None,
            }),
        ),
        (
            "hybrid-fts-vector".to_string(),
            Workload::Hybrid(vec![
                (
                    0.3,
                    Workload::Get(GetQuery {
                        skew: Skew::Zipf { theta: 0.999 },
                        limit: Distribution::Constant(10),
                        metadata: None,
                        document: Some(DocumentQuery::Raw(serde_json::json!({"$contains": "the"}))),
                    }),
                ),
                (
                    0.7,
                    Workload::Query(QueryQuery {
                        skew: Skew::Zipf { theta: 0.999 },
                        limit: Distribution::Constant(10),
                        metadata: Some(MetadataQuery::Raw(serde_json::json!({"i1": 1000}))),
                        document: None,
                    }),
                ),
            ]),
        ),
        (
            "hybrid-fts-md-vector".to_string(),
            Workload::Hybrid(vec![
                (
                    0.5,
                    Workload::Get(GetQuery {
                        skew: Skew::Zipf { theta: 0.999 },
                        limit: Distribution::Constant(10),
                        metadata: None,
                        document: Some(DocumentQuery::Raw(serde_json::json!({"$contains": "the"}))),
                    }),
                ),
                (
                    0.25,
                    Workload::Get(GetQuery {
                        skew: Skew::Zipf { theta: 0.999 },
                        limit: Distribution::Constant(10),
                        metadata: Some(MetadataQuery::Raw(serde_json::json!({"i1": 1000}))),
                        document: None,
                    }),
                ),
                (
                    0.25,
                    Workload::Query(QueryQuery {
                        skew: Skew::Zipf { theta: 0.999 },
                        limit: Distribution::Constant(10),
                        metadata: None,
                        document: None,
                    }),
                ),
            ]),
        ),
        ("load".to_string(), Workload::Load),
        (
            "random-upsert".to_string(),
            Workload::RandomUpsert(KeySelector::Random(Skew::Zipf { theta: 0.999 })),
        ),
    ])
}
