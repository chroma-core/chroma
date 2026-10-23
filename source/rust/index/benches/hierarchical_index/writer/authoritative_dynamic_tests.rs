//! Bounded construction parity, not a two-million-vector quality/performance run.
use super::super::super::super::reader::{quality::QualityLimits, HierarchicalSpannReader};
use super::super::super::{
    live_owner::{LiveOwnerHook, Mode},
    live_owner_tests::record_construction,
    HierarchicalSpannWriter,
};
use super::super::split::tests::{
    authority_topology, compare_saved, ids_json, normalized, topology, walk,
};
use super::*;
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc};
fn provider(path: &std::path::Path) -> BlockfileProvider {
    use chroma_blockstore::arrow::{config::BlockManagerConfig, provider::ArrowBlockfileProvider};
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
        Storage::Local(LocalStorage::new(path.to_str().unwrap())),
        BlockManagerConfig::default().max_block_size_bytes,
        new_cache_for_test(),
        new_cache_for_test(),
        2,
    ))
}
fn scaled() -> HierarchicalSpannConfig {
    HierarchicalSpannConfig {
        branching_factor: 3,
        split_threshold: 8,
        merge_threshold: 2,
        write_beam_min: 128,
        write_beam_max: 128,
        reassign_neighbor_count: 128,
        ..Default::default()
    }
}
fn fixture() -> Vec<Vec<f32>> {
    let b = std::fs::read(std::env::var("SPANN_AUTHORITY_CANONICAL").unwrap()).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(&b)),
        std::env::var("SPANN_AUTHORITY_CANONICAL_SHA256").unwrap()
    );
    let near = |x: f32| {
        let mut v = vec![0.; 32];
        v[0] = x;
        v[1] = 1.;
        v[2] = -0.;
        v
    };
    let mut rows = vec![near(0.), near(1.), near(30.), near(31.), near(32.)];
    for (index, row) in b.chunks_exact(128).cycle().take(187).enumerate() {
        let mut v: Vec<_> = row
            .chunks_exact(4)
            .map(|a| f32::from_le_bytes(a.try_into().unwrap()))
            .collect();
        v[0] += 128.;
        v[31] = 1024.;
        if index >= 91 {
            v[30] = 2048.;
        }
        rows.push(v);
    }
    for x in [16., 14., 14., 14., 14.] {
        rows.push(near(x));
    }
    rows.extend((0..28).map(|id| near(40. + id as f32)));
    assert_eq!(rows.len(), 225);
    rows
}
async fn census(
    p: &BlockfileProvider,
    ids: HierarchicalSpannIds,
    n: u32,
    c: HierarchicalSpannConfig,
) -> Value {
    let mut r = HierarchicalSpannReader::open(p, ids, DistanceFunction::Euclidean, c)
        .await
        .unwrap();
    let targets = (0..n).map(|id| (id, u64::from(id))).collect();
    let report = r
        .ground_truth_memberships("complete", n as u64, &targets, QualityLimits::default())
        .await
        .unwrap();
    assert_eq!(report.live_rows, n as u64);
    assert_eq!(report.expected_gt, n as usize);
    assert_eq!(r.memory_usage().embedding_count, 0);
    serde_json::to_value(report).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_default_construction_internal_growth_merge_contraction_and_saved225_parity() {
    let out = PathBuf::from(std::env::var("SPANN_DYNAMIC_GLOBAL_OUTPUT").unwrap());
    let store = out.join("default-store");
    std::fs::create_dir(&store).unwrap();
    let p = provider(&store);
    let rows = fixture();
    let initial = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, scaled());
    initial.add(0, &rows[0]);
    let old = initial
        .commit(&p, None)
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    drop(initial);
    let mut reference =
        HierarchicalSpannWriter::open(&p, old.clone(), DistanceFunction::Euclidean, scaled())
            .await
            .unwrap();
    reference.load_all_postings().await.unwrap();
    reference.load_embeddings_sync(&[0]);
    let limits = ConstructionLimits {
        max_vectors: 4096,
        max_nodes: 4096,
        max_owners: 4,
        max_leaf_rows: 4096,
        max_owner_physical_rows: 8192,
        max_batch_rows: 128,
        max_events: 100_000,
        trace: true,
        move_above: Some(80),
        ..Default::default()
    };
    let mut tree = AuthoritativeTree::open_dynamic(
        p.clone(),
        old.clone(),
        DistanceFunction::Euclidean,
        scaled(),
        1,
        4,
        limits,
    )
    .await
    .unwrap();
    let mut pairs = vec![];
    let mut reference_parent = old.clone();
    // Explicit identical batches: add rows, then use the captured reference outer
    // round order, including recursive immediate balance from reassignment.
    for end in (9..=185).step_by(8).chain(std::iter::once(192)) {
        let start = tree.locations.len();
        let hook = Arc::new(
            LiveOwnerHook::new(
                Mode::ReferenceTrace,
                tree.branches.clone(),
                4,
                32,
                DistanceFunction::Euclidean,
            )
            .unwrap(),
        );
        *reference.live_owner_hook.lock() = Some(hook.clone());
        let (splits, rounds) = record_construction(|| {
            for id in start..end {
                reference.add(id as u32, &rows[id]);
            }
            reference.balance_index();
        });
        tree.split_decisions = splits.into();
        tree.balance_rounds = rounds.into();
        let before = tree.events.len();
        let batch: Vec<_> = (start..end)
            .map(|id| (id as u32, rows[id].clone()))
            .collect();
        if start == 1 {
            tree.register_dynamic_batch(&batch[..4]).await.unwrap();
            tree.register_dynamic_batch(&batch[4..]).await.unwrap();
            assert!(!tree.split_decisions.is_empty());
            assert!(!tree.balance_rounds.is_empty());
            assert!(!tree.event_counts.contains_key("split"));
            tree.balance_dynamic_current().unwrap();
        } else {
            tree.add_dynamic_batch(&batch, false).await.unwrap();
        }
        assert!(tree.split_decisions.is_empty());
        assert!(tree.balance_rounds.is_empty());
        assert_eq!(
            topology(&reference),
            authority_topology(&tree),
            "topology through row {end}"
        );
        let r = normalized(&hook.events(), true);
        let a = normalized(&tree.events[before..], false);
        if r != a {
            std::fs::write(
                out.join("default-trace-mismatch.json"),
                serde_json::to_vec_pretty(&json!({"end":end,"reference":r,"owner":a})).unwrap(),
            )
            .unwrap();
        }
        assert_eq!(r, a, "semantic trace through {end}");
        pairs.push(json!({"end":end,"reference":r,"owner":a}));
    }
    assert_eq!(tree.locations.len(), 192);
    assert!(
        tree.event_counts
            .get("internal-split")
            .copied()
            .unwrap_or(0)
            > 0
    );
    assert!(tree.event_counts.get("root-grown").copied().unwrap_or(0) > 1);
    assert!(
        tree.event_counts.get("move").copied().unwrap_or(0) > 0,
        "actual threshold placement transfer required"
    );
    let saved192 = tree.save().await.unwrap();
    let ref192 = reference
        .commit(&p, Some(&reference_parent))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    reference_parent = ref192.clone();
    compare_saved(&p, &saved192, &ref192, &rows[..192]).await;
    let report192 = census(&p, saved192.clone(), 192, scaled()).await;
    assert_eq!(report192, census(&p, ref192, 192, scaled()).await);
    let mut pinned =
        HierarchicalSpannReader::open(&p, saved192.clone(), DistanceFunction::Euclidean, scaled())
            .await
            .unwrap();
    let targets = (0..192).map(|i| (i, u64::from(i))).collect();
    let pinned_report = serde_json::to_value(
        pinned
            .ground_truth_memberships("pinned", 192, &targets, QualityLimits::default())
            .await
            .unwrap(),
    )
    .unwrap();
    let old_objects: BTreeMap<_, _> = walk(&store)
        .into_iter()
        .map(|f| {
            (
                f.strip_prefix(&store)
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                format!("{:x}", Sha256::digest(std::fs::read(f).unwrap())),
            )
        })
        .collect();
    // Continue the SAME tree with unmodified defaults. At this bounded size the
    // default512 merge threshold must coalesce the undersized multilevel tree.
    tree.configure_dynamic(HierarchicalSpannConfig::default())
        .unwrap();
    reference.config = HierarchicalSpannConfig::default();
    let hook = Arc::new(
        LiveOwnerHook::new(
            Mode::ReferenceTrace,
            tree.branches.clone(),
            4,
            32,
            DistanceFunction::Euclidean,
        )
        .unwrap(),
    );
    *reference.live_owner_hook.lock() = Some(hook.clone());
    let (splits, rounds) = record_construction(|| reference.balance_index());
    tree.split_decisions = splits.into();
    tree.balance_rounds = rounds.into();
    let before = tree.events.len();
    tree.balance_dynamic_current().unwrap();
    assert!(tree.split_decisions.is_empty() && tree.balance_rounds.is_empty());
    assert_eq!(topology(&reference), authority_topology(&tree));
    assert_eq!(
        normalized(&hook.events(), true),
        normalized(&tree.events[before..], false)
    );
    assert!(tree.event_counts.get("merge").copied().unwrap_or(0) > 0);
    assert!(
        tree.event_counts
            .get("root-contracted")
            .copied()
            .unwrap_or(0)
            > 0
    );
    assert_eq!(tree.branches.len(), 1);
    let hook = Arc::new(
        LiveOwnerHook::new(
            Mode::ReferenceTrace,
            tree.branches.clone(),
            4,
            32,
            DistanceFunction::Euclidean,
        )
        .unwrap(),
    );
    *reference.live_owner_hook.lock() = Some(hook.clone());
    let (splits, rounds) = record_construction(|| {
        for id in 192..225 {
            reference.add(id as u32, &rows[id]);
        }
        reference.balance_index();
    });
    tree.split_decisions = splits.into();
    tree.balance_rounds = rounds.into();
    let batch: Vec<_> = (192..225).map(|id| (id as u32, rows[id].clone())).collect();
    let before = tree.events.len();
    tree.add_dynamic_batch(&batch, false).await.unwrap();
    assert!(tree.split_decisions.is_empty() && tree.balance_rounds.is_empty());
    assert_eq!(topology(&reference), authority_topology(&tree));
    assert_eq!(
        normalized(&hook.events(), true),
        normalized(&tree.events[before..], false)
    );
    let saved225 = tree.save().await.unwrap();
    let ref225 = reference
        .commit(&p, Some(&reference_parent))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    compare_saved(&p, &saved225, &ref225, &rows).await;
    let final_report = census(
        &p,
        saved225.clone(),
        225,
        HierarchicalSpannConfig::default(),
    )
    .await;
    assert_eq!(
        final_report,
        census(&p, ref225.clone(), 225, HierarchicalSpannConfig::default()).await
    );
    assert_eq!(
        pinned_report,
        serde_json::to_value(
            pinned
                .ground_truth_memberships("pinned", 192, &targets, QualityLimits::default())
                .await
                .unwrap()
        )
        .unwrap()
    );
    for (file, hash) in &old_objects {
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(std::fs::read(store.join(file)).unwrap())
            ),
            *hash
        );
    }
    let counters = tree.counters().unwrap();
    assert_eq!(
        counters.values().map(|c| c.current_rows).sum::<usize>(),
        225
    );
    assert_eq!(
        counters.values().map(|c| c.physical_rows).sum::<usize>(),
        final_report["physical_rows"].as_u64().unwrap() as usize
    );
    for stats in tree.counters().unwrap().values() {
        assert_eq!(stats.prepared_posting_payload_bytes, 0);
        assert_eq!(stats.prepared_current_records, 0);
    }
    assert!(
        tree.vector_batch_forks < 32,
        "one fork per bounded batch, not per row"
    );
    assert_eq!(tree.limits.max_vectors, 4096);
    std::fs::write(out.join("default-construction-result.json"),serde_json::to_vec_pretty(&json!({"status":"passed","rows":225,"saved192":ids_json(&saved192),"saved225":ids_json(&saved225),"reference225":ids_json(&ref225),"census192":report192,"census225":final_report,"pinned192":pinned_report,"events":tree.events,"event_counts":tree.event_counts,"pairs":pairs,"accounting":tree.construction_accounting(),"owner_counters":tree.counters().unwrap(),"old_objects":old_objects,"scope":"scaled structural coverage and actual default merge/add semantics; not a 2M readiness/performance result"})).unwrap()).unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_default_limits_and_invalid_batch_reject_before_owner_mutation() {
    let dir = tempfile::tempdir().unwrap();
    let p = provider(dir.path());
    let cfg = HierarchicalSpannConfig::default();
    let w = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, cfg.clone());
    w.add(0, &vec![1.; 32]);
    let roots = w.commit(&p, None).await.unwrap().flush().await.unwrap();
    let limits = ConstructionLimits::default();
    assert_eq!(limits.max_vectors, 2_000_000);
    assert!(limits.max_leaf_rows >= 2048);
    limits.validate().unwrap();
    let mut t =
        AuthoritativeTree::open_dynamic(p, roots, DistanceFunction::Euclidean, cfg, 1, 2, limits)
            .await
            .unwrap();
    let before = t.latest_vectors;
    assert!(t
        .add_dynamic_batch(&[(3, vec![0.; 32])], false)
        .await
        .is_err());
    assert!(t
        .add_dynamic_batch(&[(1, vec![f32::NAN; 32])], false)
        .await
        .is_err());
    assert_eq!(t.latest_vectors, before);
    assert_eq!(t.vector_batch_forks, 0);
    assert!(!t.poisoned);
    assert_eq!(t.locations.len(), 1);
    assert_eq!(t.counters().unwrap()[&0].registered_rows, 0);
    let mut unsupported = HierarchicalSpannConfig::default();
    unsupported.fp_npa = false;
    assert!(t.configure_dynamic(unsupported).is_err());
    assert!(t.routing.config.fp_npa);
    let mut zero = HierarchicalSpannConfig::default();
    zero.split_threshold = 0;
    zero.merge_threshold = 0;
    assert!(t.configure_dynamic(zero).is_err());
    let max = t.limits.max_vectors;
    t.limits.max_vectors = 1;
    assert!(t
        .add_dynamic_batch(&[(1, vec![2.; 32])], false)
        .await
        .is_err());
    assert_eq!(t.vector_batch_forks, 0);
    t.limits.max_vectors = max;
    t.add_dynamic_batch(&[(1, vec![2.; 32])], false)
        .await
        .unwrap();
    assert_eq!(t.vector_batch_forks, 1);
    assert!(t.events.is_empty());
    assert_eq!(t.event_counts["leaf_registered"], 1);
    assert_eq!(t.counters().unwrap()[&0].physical_rows, 2);
}
