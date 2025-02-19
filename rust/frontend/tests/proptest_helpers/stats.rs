pub(crate) struct GetRequestSelectivity {
    pub where_clause_only: Vec<f64>,
    pub where_clause_and_ids: Vec<f64>,
    pub ids_only: Vec<f64>,
}

pub(crate) struct QueryRequestSelectivity {
    pub where_clause_only: Vec<f64>,
    pub where_clause_and_ids: Vec<f64>,
    pub ids_only: Vec<f64>,
    pub embeddings_only: Vec<f64>,
}

/// Collects statistics about the test run and logs them when the test ends.
pub(crate) struct Stats {
    pub get_request_selectivity: GetRequestSelectivity,
    pub query_request_selectivity: QueryRequestSelectivity,
    pub num_log_operations: usize,
}

impl Drop for Stats {
    fn drop(&mut self) {
        fn print_selectivity(selectivity: &[f64]) {
            let partial_results = selectivity
                .iter()
                .filter(|x| **x != 0.0 && **x != 1.0)
                .count();
            let no_results = selectivity.iter().filter(|x| **x == 0.0).count();
            let all_results = selectivity.iter().filter(|x| **x == 1.0).count();

            println!(
                "      {:05.2}% of queries returned no results",
                no_results as f64 / selectivity.len() as f64 * 100.0
            );
            println!(
                "      {:05.2}% of queries returned some results",
                partial_results as f64 / selectivity.len() as f64 * 100.0
            );
            println!(
                "      {:05.2}% of queries returned all results",
                all_results as f64 / selectivity.len() as f64 * 100.0
            );
        }

        println!("Statistics:");
        println!(
            "  A total of {} log operations were created.",
            self.num_log_operations
        );

        // Get request selectivity
        let total_get_requests = self.get_request_selectivity.where_clause_only.len()
            + self.get_request_selectivity.where_clause_and_ids.len()
            + self.get_request_selectivity.ids_only.len();
        println!(
            "  .get() selectivity ({} total requests):",
            total_get_requests
        );
        println!(
            "    .get() with a where clause only ({:2.2}%):",
            self.get_request_selectivity.where_clause_only.len() as f64 / total_get_requests as f64
                * 100.0
        );
        print_selectivity(&self.get_request_selectivity.where_clause_only);

        println!(
            "    .get() with a where clause & IDs ({:2.2}%):",
            self.get_request_selectivity.where_clause_and_ids.len() as f64
                / total_get_requests as f64
                * 100.0
        );
        print_selectivity(&self.get_request_selectivity.where_clause_and_ids);

        println!(
            "    .get() with IDs only ({:2.2}%):",
            self.get_request_selectivity.ids_only.len() as f64 / total_get_requests as f64 * 100.0
        );
        print_selectivity(&self.get_request_selectivity.ids_only);

        // Query request selectivity
        let total_query_requests = self.query_request_selectivity.where_clause_only.len()
            + self.query_request_selectivity.where_clause_and_ids.len()
            + self.query_request_selectivity.ids_only.len()
            + self.query_request_selectivity.embeddings_only.len();
        println!(
            "  .query() selectivity ({} total requests):",
            total_query_requests
        );
        println!(
            "    .query() with a where clause & embeddings ({:2.2}%):",
            self.query_request_selectivity.where_clause_only.len() as f64
                / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.where_clause_only);

        println!(
            "    .query() with a where clause & IDs & embeddings ({:2.2}%):",
            self.query_request_selectivity.where_clause_and_ids.len() as f64
                / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.where_clause_and_ids);

        println!(
            "    .query() with IDs & embeddings ({:2.2}%):",
            self.query_request_selectivity.ids_only.len() as f64 / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.ids_only);

        println!(
            "    .query() with embeddings only ({:2.2}%):",
            self.query_request_selectivity.embeddings_only.len() as f64
                / total_query_requests as f64
                * 100.0
        );
        print_selectivity(&self.query_request_selectivity.embeddings_only);
    }
}

#[macro_export]
macro_rules! define_thread_local_stats {
    ($name:ident) => {
        thread_local! {
            static $name: ::std::cell::RefCell<$crate::proptest_helpers::stats::Stats> = const {
                ::std::cell::RefCell::new(
                    $crate::proptest_helpers::stats::Stats {
                        num_log_operations: 0,
                        get_request_selectivity: $crate::proptest_helpers::stats::GetRequestSelectivity {
                            where_clause_only: vec![],
                            where_clause_and_ids: vec![],
                            ids_only: vec![],
                        },
                        query_request_selectivity: $crate::proptest_helpers::stats::QueryRequestSelectivity {
                            where_clause_only: vec![],
                            where_clause_and_ids: vec![],
                            ids_only: vec![],
                            embeddings_only: vec![],
                        },
                    }
                )
            };
        }
    };
}
