use super::*;

fn query(ids: &[u64], distances: &[f64]) -> BenchQuery {
    BenchQuery {
        id: "q".into(),
        vector: vec![0.0; 2],
        truth_ids: ids.to_vec(),
        truth_distances: distances.to_vec(),
    }
}

#[test]
fn recall_counts_listed_ties_at_the_kth_position() {
    // IDs 3 and 4 are duplicates that tie across position 2.
    let q = query(&[1, 2, 3, 4, 5], &[0.1, 0.2, 0.3, 0.3, 0.5]);
    let mut never = |_| -> Option<f64> { panic!("listed IDs need no exact distance") };
    let r = tie_aware_recall(&[1, 2, 4], &q, 3, &mut never).unwrap();
    assert_eq!((r.hits, r.unresolved), (3, 0));
    let r = tie_aware_recall(&[1, 5, 9], &q, 3, &mut never).unwrap();
    assert_eq!(r.hits, 1, "5 is listed beyond the tie; 9 cannot tie");
}

#[test]
fn recall_resolves_unlisted_ids_only_when_ties_reach_the_list_end() {
    let q = query(&[1, 2, 3], &[0.1, 0.2, 0.2]);
    let mut exact = |id| match id {
        7 => Some(0.2 * (1.0 + 1e-12)),
        8 => Some(0.25),
        10 => Some(0.15),
        _ => None,
    };
    let r = tie_aware_recall(&[1, 7, 8, 9], &q, 3, &mut exact).unwrap();
    assert_eq!((r.hits, r.unresolved), (2, 1));
    // Closer than the k-th distance is a hit even if the references missed it.
    let r = tie_aware_recall(&[10], &q, 3, &mut exact).unwrap();
    assert_eq!(r.hits, 1);
}

#[test]
fn recall_ignores_repeated_ids_and_caps_at_k() {
    let q = query(&[1, 2, 3, 4], &[0.1, 0.1, 0.1, 0.1]);
    let mut never = |_| -> Option<f64> { None };
    let r = tie_aware_recall(&[1, 1, 2, 3, 4], &q, 2, &mut never).unwrap();
    assert_eq!(r.hits, 2);
    assert!(tie_aware_recall(&[1], &q, 5, &mut never).is_err());
}

#[test]
fn stats_use_nearest_rank() {
    let values: Vec<f64> = (1..=1000).map(f64::from).collect();
    let s = stats(&values).unwrap();
    assert_eq!((s.p50, s.p90, s.p99, s.max), (500.0, 900.0, 990.0, 1000.0));
    assert_eq!(s.mean, 500.5);
    assert!(stats(&[]).is_none());
}

#[test]
fn pages_count_rows_that_straddle_boundaries_once() {
    // Rows of 3,072 bytes: row 0 fits page 0, row 1 spans pages 0-1.
    assert_eq!(distinct_pages(&[0], 0, 0, 3072, 3072), 1);
    assert_eq!(distinct_pages(&[1], 0, 0, 3072, 3072), 2);
    assert_eq!(distinct_pages(&[0, 1, 1], 0, 0, 3072, 3072), 2);
    assert_eq!(distinct_pages(&[10], 10, 4096, 3072, 3072), 1);
}

#[test]
fn exact_distance_accumulates_in_float64() {
    assert_eq!(exact_squared_l2(&[1.0, 2.0], &[0.0, 4.0]), 5.0);
}

#[test]
fn recording_vectors_capture_reads_per_thread() {
    let source = |id: u64| -> std::result::Result<Arc<[f32]>, String> { Ok(vec![id as f32].into()) };
    let recording = RecordingVectors(&source);
    take_touched_vectors();
    recording.get(3).unwrap();
    std::thread::scope(|s| {
        s.spawn(|| recording.get(9).unwrap());
    });
    recording.get(4).unwrap();
    assert_eq!(take_touched_vectors(), vec![3, 4]);
}

#[test]
fn query_loading_joins_by_id_and_checks_references() {
    let dir = std::env::temp_dir().join(format!("reader-bench-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let queries = dir.join("q.jsonl");
    let truth = dir.join("t.jsonl");
    std::fs::write(
        &queries,
        "{\"query_id\":\"a\",\"vector\":[1.0,2.0]}\n{\"query_id\":\"b\",\"vector\":[3.0,4.0]}\n",
    )
    .unwrap();
    std::fs::write(
        &truth,
        "{\"query_id\":\"b\",\"ids\":[5],\"distances\":[0.5]}\n{\"query_id\":\"a\",\"ids\":[1,2],\"distances\":[0.1,0.2]}\n",
    )
    .unwrap();
    let loaded = load_queries(&queries, &truth, 2, None).unwrap();
    assert_eq!(loaded.iter().map(|q| q.id.as_str()).collect::<Vec<_>>(), ["a", "b"]);
    assert_eq!(loaded[1].truth_ids, [5]);
    assert_eq!(load_queries(&queries, &truth, 2, Some(1)).unwrap().len(), 1);
    assert!(load_queries(&queries, &truth, 3, None).is_err());
    std::fs::write(&truth, "{\"query_id\":\"a\",\"ids\":[1,2],\"distances\":[0.2,0.1]}\n").unwrap();
    assert!(load_queries(&queries, &truth, 2, None).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}

struct Shape(HashMap<u128, (Option<u128>, usize)>);
impl TreeShape for Shape {
    fn parent(&self, id: u128) -> Option<u128> {
        self.0[&id].0
    }
    fn child_count(&self, id: u128) -> usize {
        self.0[&id].1
    }
}

#[test]
fn walk_diagnostics_track_reach_per_level_and_the_optimal_leaf_budget() {
    // root 1 -> internal 10, 20 -> leaves 11, 12 under 10 and 21, 22 under 20.
    let shape = Shape(
        [
            (1, (None, 2)),
            (10, (Some(1), 2)),
            (20, (Some(1), 2)),
            (11, (Some(10), 0)),
            (12, (Some(10), 0)),
            (21, (Some(20), 0)),
            (22, (Some(20), 0)),
        ]
        .into(),
    );
    // Four true neighbors: two in leaf 21, one in 11, one in 12.
    let truth = [(1, 21), (2, 21), (3, 11), (4, 12)];
    // The beam kept internal node 10 only, then leaf 11.
    let d = walk_diagnostics(&shape, &[vec![1], vec![10]], &[11], &truth);
    assert_eq!(d.levels.len(), 2);
    assert_eq!((d.levels[0].scored, d.levels[0].kept, d.levels[0].reach), (2, 1, 0.5));
    assert_eq!((d.levels[1].scored, d.levels[1].kept, d.levels[1].reach), (2, 1, 0.25));
    assert_eq!(d.leaf_reach, 0.25);
    // One leaf of budget at best holds leaf 21's two neighbors.
    assert_eq!(d.optimal_recall, 0.5);
    let all = walk_diagnostics(&shape, &[vec![1], vec![10, 20]], &[11, 12, 21, 22], &truth);
    assert_eq!(all.levels[1].reach, 1.0);
    assert_eq!(all.optimal_recall, 1.0);
}

#[test]
fn exact_leaf_ranks_order_leaves_by_centroid_distance() {
    let (a, b, c) = ([0.0f32, 0.0], [1.0f32, 0.0], [5.0f32, 0.0]);
    let centroids = vec![(3u128, &c[..]), (1u128, &a[..]), (2u128, &b[..])];
    let truth = vec![(10u64, 3u128), (11, 2), (12, 9)];
    assert_eq!(exact_leaf_ranks(&centroids, &[0.9, 0.0], &truth), vec![2, 0, usize::MAX]);
}
