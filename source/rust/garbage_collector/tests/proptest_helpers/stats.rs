use super::{garbage_collector_reference::ReferenceState, proptest_types::Transition};
use std::collections::HashMap;

#[derive(Default)]
pub(crate) struct Stats {
    /// Fork tree: file path -> # of times used in tree
    pub file_usage_counts: Vec<HashMap<String, usize>>,
    pub current_fork_tree: Option<HashMap<String, usize>>, // gets moved into file_usage_counts at end of each test case

    pub fork_tree_depths: Vec<usize>,
    pub last_fork_tree_depth: Option<usize>, // gets moved into fork_tree_depths at end of each test case

    pub num_forks_per_tree: Vec<usize>,
    pub current_num_forks: usize, // gets moved into num_forks_per_tree at end of each test case

    pub num_versions_per_tree: Vec<usize>,
    pub current_num_versions: usize, // gets moved into num_versions_per_tree at end of each test case
}

impl Stats {
    pub fn record_transition(&mut self, transition: &Transition, ref_state: &ReferenceState) {
        match transition {
            Transition::CreateCollection { segments, .. } => {
                let current_fork_tree = self.current_fork_tree.get_or_insert_with(HashMap::new);

                for file_path in segments.get_all_file_paths() {
                    let count = current_fork_tree.entry(file_path.clone()).or_insert(0);
                    *count += 1;
                }

                self.last_fork_tree_depth = Some(ref_state.get_graph_depth());
                self.current_num_versions += 1;
            }
            Transition::IncrementCollectionVersion { next_segments, .. } => {
                let current_fork_tree = self.current_fork_tree.get_or_insert_with(HashMap::new);

                for file_path in next_segments.get_all_file_paths() {
                    let count = current_fork_tree.entry(file_path.clone()).or_insert(0);
                    *count += 1;
                }

                self.last_fork_tree_depth = Some(ref_state.get_graph_depth());
                self.current_num_versions += 1;
            }
            Transition::ForkCollection { .. } => {
                self.current_num_forks += 1;
            }
            Transition::DeleteCollection(_) => {}
            Transition::GarbageCollect { .. } => {}
            Transition::NoOp => {}
        }
    }

    pub fn record_test_case_end(&mut self) {
        // Move the current fork tree into the file usage counts
        let current_fork_tree = self.current_fork_tree.take().unwrap_or_default();
        self.file_usage_counts.push(current_fork_tree);

        // Record the depth of the fork tree
        if let Some(fork_depth) = self.last_fork_tree_depth.take() {
            self.fork_tree_depths.push(fork_depth);
        }

        // Record the number of forks and versions in the current tree
        self.num_forks_per_tree.push(self.current_num_forks);
        self.current_num_forks = 0;
        self.num_versions_per_tree.push(self.current_num_versions);
        self.current_num_versions = 0;
    }
}

impl Drop for Stats {
    fn drop(&mut self) {
        println!("Statistics:");
        let total_num_files: usize = self
            .file_usage_counts
            .iter()
            .map(|file_usage| file_usage.len())
            .sum();
        println!("  A total of {} files were created.", total_num_files);

        let average_file_reuse: f64 = self
            .file_usage_counts
            .iter()
            .map(|file_usage| {
                let num_reused_files = file_usage.iter().filter(|(_, count)| **count > 1).count();
                num_reused_files as f64 / file_usage.len() as f64
            })
            .sum::<f64>()
            / self.file_usage_counts.len() as f64;
        println!(
            "  Average file reuse: {:.2}% of files were reused at least once (per tree)",
            average_file_reuse * 100.0
        );

        // For each file path that was reused, how many times was it reused?
        let reused_files_counts = self
            .file_usage_counts
            .iter()
            .flat_map(|file_usage| {
                file_usage
                    .iter()
                    .filter(|(_, count)| **count > 1)
                    .map(|(_, count)| *count)
            })
            .collect::<Vec<usize>>();
        let average_reuse_count: f64 =
            reused_files_counts.iter().sum::<usize>() as f64 / reused_files_counts.len() as f64;
        println!(
            "  Files that were reused were reused an average of: {:.2} times (per tree)",
            average_reuse_count
        );

        let average_fork_tree_depth: f64 = self
            .fork_tree_depths
            .iter()
            .map(|depth| *depth as f64)
            .sum::<f64>()
            / self.fork_tree_depths.len() as f64;
        println!("  Average tree depth: {:.2}", average_fork_tree_depth);

        let average_num_forks: f64 = self
            .num_forks_per_tree
            .iter()
            .map(|num_forks| *num_forks as f64)
            .sum::<f64>()
            / self.num_forks_per_tree.len() as f64;
        println!(
            "  Average number of forks: {:.2} (per tree)",
            average_num_forks
        );

        let average_num_versions: f64 = self
            .num_versions_per_tree
            .iter()
            .map(|num_versions| *num_versions as f64)
            .sum::<f64>()
            / self.num_versions_per_tree.len() as f64;
        println!(
            "  Average number of versions: {:.2} (per tree)",
            average_num_versions
        );
    }
}

#[macro_export]
macro_rules! define_thread_local_stats {
    ($name:ident) => {
        thread_local! {
            static $name: ::std::cell::RefCell<$crate::proptest_helpers::stats::Stats> = const {
                ::std::cell::RefCell::new(
                    $crate::proptest_helpers::stats::Stats {
                        file_usage_counts: vec![],
                        current_fork_tree: None,
                        fork_tree_depths: vec![],
                        last_fork_tree_depth: None,
                        num_forks_per_tree: vec![],
                        current_num_forks: 0,
                        num_versions_per_tree: vec![],
                        current_num_versions: 0,
                    }
                )
            };
        }
    };
}
