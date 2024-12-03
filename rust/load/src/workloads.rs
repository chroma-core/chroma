use std::collections::HashMap;

use crate::{Distribution, DocumentQuery, GetQuery, MetadataQuery, QueryQuery, Workload};

pub fn all_workloads() -> HashMap<String, Workload> {
    HashMap::from_iter([
        (
            "get-no-filter".to_string(),
            Workload::Get(GetQuery {
                limit: Distribution::Constant(10),
                metadata: None,
                document: None,
            }),
        ),
        (
            "get-document".to_string(),
            Workload::Get(GetQuery {
                limit: Distribution::Constant(10),
                metadata: None,
                document: Some(DocumentQuery::Raw(serde_json::json!({"$contains": "the"}))),
            }),
        ),
        (
            "get-metadata".to_string(),
            Workload::Get(GetQuery {
                limit: Distribution::Constant(10),
                metadata: Some(MetadataQuery::Raw(serde_json::json!({"i1": 1000}))),
                document: None,
            }),
        ),
        (
            "query-no-filter".to_string(),
            Workload::Query(QueryQuery {
                limit: Distribution::Constant(10),
                metadata: None,
                document: None,
            }),
        ),
    ])
}
