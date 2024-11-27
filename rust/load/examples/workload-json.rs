use chroma_load::{Distribution, GetQuery, QueryQuery, Workload};

fn main() {
    let w = Workload::Hybrid(vec![
        (1.0, Workload::Nop),
        (1.0, Workload::ByName("foo".to_string())),
        (
            1.0,
            Workload::Get(GetQuery {
                limit: Distribution::Constant(10),
                document: None,
                metadata: None,
            }),
        ),
        (
            1.0,
            Workload::Query(QueryQuery {
                limit: Distribution::Constant(10),
                document: None,
                metadata: None,
            }),
        ),
        (
            1.0,
            Workload::Delay(
                chrono::DateTime::parse_from_rfc3339("2021-01-01T00:00:00+00:00").unwrap(),
                Box::new(Workload::Nop),
            ),
        ),
    ]);
    println!("{}", serde_json::to_string_pretty(&w).unwrap());
}
