use std::time::{Duration, Instant};

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
    pub run_started_at: Option<Instant>,
    pub case_durations: Vec<Duration>,
    pub transition_counts: Vec<usize>,
}

impl Stats {
    pub fn start_run(&mut self) {
        self.run_started_at.get_or_insert_with(Instant::now);
    }

    pub fn record_case(&mut self, duration: Duration, transitions: usize) {
        self.case_durations.push(duration);
        self.transition_counts.push(transitions);
    }
}

impl Drop for Stats {
    fn drop(&mut self) {
        fn average(values: &[f64]) -> f64 {
            if values.is_empty() {
                0.0
            } else {
                values.iter().sum::<f64>() / values.len() as f64
            }
        }

        fn p50(values: &mut [f64]) -> f64 {
            if values.is_empty() {
                return 0.0;
            }

            values.sort_by(f64::total_cmp);
            values[values.len() / 2]
        }

        fn percent(part: usize, total: usize) -> f64 {
            if total == 0 {
                0.0
            } else {
                part as f64 / total as f64 * 100.0
            }
        }

        fn print_selectivity(selectivity: &[f64]) {
            if selectivity.is_empty() {
                println!("      no requests generated");
                return;
            }

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
        let total_cases = self.case_durations.len();
        let total_transitions = self.transition_counts.iter().sum::<usize>();
        let total_wall_clock = self
            .run_started_at
            .map(|started_at| started_at.elapsed().as_secs_f64())
            .unwrap_or_default();
        let mut case_durations = self
            .case_durations
            .iter()
            .map(|duration| duration.as_secs_f64())
            .collect::<Vec<_>>();
        let transition_times = self
            .case_durations
            .iter()
            .zip(self.transition_counts.iter())
            .filter(|(_, transitions)| **transitions > 0)
            .map(|(duration, transitions)| duration.as_secs_f64() / *transitions as f64)
            .collect::<Vec<_>>();
        let mut transition_times_for_p50 = transition_times.clone();
        let mut transition_counts = self
            .transition_counts
            .iter()
            .map(|count| *count as f64)
            .collect::<Vec<_>>();

        println!(
            "  Total frontend state-machine wall-clock time: {:.2}s",
            total_wall_clock
        );
        println!("  Test cases generated: {}", total_cases);
        println!(
            "  Transitions generated: {} total, {:.2} average/case, {:.0} p50/case",
            total_transitions,
            average(&transition_counts),
            p50(&mut transition_counts)
        );
        println!(
            "  Case duration: {:.4}s average, {:.4}s p50",
            average(&case_durations),
            p50(&mut case_durations)
        );
        println!(
            "  Transition duration: {:.4}s average, {:.4}s p50",
            average(&transition_times),
            p50(&mut transition_times_for_p50)
        );
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
            percent(
                self.get_request_selectivity.where_clause_only.len(),
                total_get_requests
            )
        );
        print_selectivity(&self.get_request_selectivity.where_clause_only);

        println!(
            "    .get() with a where clause & IDs ({:2.2}%):",
            percent(
                self.get_request_selectivity.where_clause_and_ids.len(),
                total_get_requests
            )
        );
        print_selectivity(&self.get_request_selectivity.where_clause_and_ids);

        println!(
            "    .get() with IDs only ({:2.2}%):",
            percent(
                self.get_request_selectivity.ids_only.len(),
                total_get_requests
            )
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
            percent(
                self.query_request_selectivity.where_clause_only.len(),
                total_query_requests
            )
        );
        print_selectivity(&self.query_request_selectivity.where_clause_only);

        println!(
            "    .query() with a where clause & IDs & embeddings ({:2.2}%):",
            percent(
                self.query_request_selectivity.where_clause_and_ids.len(),
                total_query_requests
            )
        );
        print_selectivity(&self.query_request_selectivity.where_clause_and_ids);

        println!(
            "    .query() with IDs & embeddings ({:2.2}%):",
            percent(
                self.query_request_selectivity.ids_only.len(),
                total_query_requests
            )
        );
        print_selectivity(&self.query_request_selectivity.ids_only);

        println!(
            "    .query() with embeddings only ({:2.2}%):",
            percent(
                self.query_request_selectivity.embeddings_only.len(),
                total_query_requests
            )
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
                        run_started_at: None,
                        case_durations: vec![],
                        transition_counts: vec![],
                    }
                )
            };
        }
    };
}
