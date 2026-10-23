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
    tree.require_split_tape = true;
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
    pairs.push(json!({"phase":"default-merge-balance","reference":normalized(&hook.events(),true),"owner":normalized(&tree.events[before..],false)}));
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
    pairs.push(json!({"phase":"default-continued-add","reference":normalized(&hook.events(),true),"owner":normalized(&tree.events[before..],false)}));
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
    let mut small = HierarchicalSpannConfig::default();
    small.split_threshold = 2;
    small.merge_threshold = 0;
    small.branching_factor = 3;
    t.configure_dynamic(small.clone()).unwrap();
    assert!(!t.require_split_tape);
    assert!(t.split_decisions.is_empty());
    t.add_dynamic_batch(&[(2, vec![3.; 32]), (3, vec![4.; 32])], false)
        .await
        .unwrap();
    assert!(t.event_counts.get("split").copied().unwrap_or(0) > 0);
    assert!(t.events.is_empty());
    let roots = t.save().await.unwrap();
    assert_eq!(census(&t.provider, roots, 4, small).await["live_rows"], 4);
}

/// Synthetic data proves actual point/provider/decode attribution and off/on ID parity.
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn build_speed_observer_actual_reads_and_off_parity() {
    for observed in [false,true] {
        let directory=tempfile::tempdir().unwrap();let p=provider(directory.path());
        let mut c=scaled();c.write_navigation=NavigationMode::Fp;c.fp_npa=true;
        let reference=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,c.clone());
        reference.add(0,&vec![0.;32]);
        let ids=reference.commit(&p,None).await.unwrap().flush().await.unwrap();
        let mut tree=AuthoritativeTree::open_dynamic(p.clone(),ids,DistanceFunction::Euclidean,c,1,1,
            ConstructionLimits{max_vectors:64,max_nodes:100,max_owners:1,max_leaf_rows:128,max_owner_physical_rows:128,max_batch_rows:64,max_dimension:32,max_events:1000,trace:false,move_above:None}).await.unwrap();
        if observed {tree.enable_diagnostic_reads().unwrap();assert!(tree.enable_diagnostic_reads().is_err());}
        let rows:Vec<_>=(1..64).map(|id|(id,vec![id as f32;32])).collect();
        tree.register_dynamic_batch(&rows).await.unwrap();
        assert_eq!(tree.locations().keys().copied().collect::<Vec<_>>(),(0..64).collect::<Vec<_>>());
        assert_eq!(tree.counters().unwrap()[&0].registered_rows,63);
        if observed {
            let warm=tree.diagnostic_read_snapshot().unwrap();
            let warm_obtains:u64=warm["0"]["readers"].as_array().unwrap().iter().flat_map(|r|r["block_access"].as_object().unwrap().values()).map(|a|a["provider_obtains"].as_u64().unwrap()).sum();
            assert_eq!(warm_obtains,63);
            p.clear().await.unwrap();
            for id in 0..64 {assert_eq!(tree.lookup_at(0,id).unwrap().vector_bits,vec![(id as f32).to_bits();32]);}
            let report=tree.diagnostic_read_snapshot().unwrap();let readers=report["0"]["readers"].as_array().unwrap();
            let mut gets=0;let mut decodes=0;
            for reader in readers {assert_eq!(reader["access_overflow"],false);for access in reader["block_access"].as_object().unwrap().values(){gets+=access["reader_requests"].as_u64().unwrap();decodes+=access["decode_successes"].as_u64().unwrap();}}
            assert_eq!(gets,127);assert!(decodes>0);
        } else {assert!(tree.diagnostic_read_snapshot().is_err());}
        let saved=tree.save().await.unwrap();let reader=HierarchicalSpannReader::open(&p,saved,DistanceFunction::Euclidean,scaled()).await.unwrap();
        let query=vec![0.;32];let ids:Vec<_>=(0..64).collect();let actual=reader.rerank_selected(&query,&ids).await.unwrap();
        assert_eq!(actual.len(),64);
        for (id,distance) in actual {assert_eq!(distance,(id as f32).powi(2)*32.);}
    }
}

/// Reuse one fixed construction tape: only the trace policy differs.
#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn disabled_trace_fixed_decision_parity_and_validation() {
    let directory=tempfile::tempdir().unwrap();let p=provider(directory.path());
    let mut c=scaled();c.fp_npa=true;c.write_navigation=NavigationMode::Fp;
    let rows:Vec<Vec<f32>>=(0..64).map(|i|{let mut v=vec![0.;32];v[0]=(i%13) as f32;v[1]=(i/13) as f32;v[2]=-0.;v}).collect();
    let reference=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,c.clone());reference.add(0,&rows[0]);
    let genesis=reference.commit(&p,None).await.unwrap().flush().await.unwrap();
    let (splits,rounds)=record_construction(||{for id in 1..64 {reference.add(id,&rows[id as usize]);}reference.balance_index();});
    assert!(!splits.is_empty());
    let batch:Vec<_>=(1..64).map(|id|(id as u32,rows[id].clone())).collect();
    let mut runs=vec![];
    for trace in [false,true] {for lazy in [false,true] {
        let mut tree=AuthoritativeTree::open_dynamic(p.clone(),genesis.clone(),DistanceFunction::Euclidean,c.clone(),1,1,
            ConstructionLimits{max_vectors:64,max_nodes:256,max_owners:1,max_leaf_rows:128,max_owner_physical_rows:256,max_batch_rows:64,max_dimension:32,max_events:100000,trace,move_above:None}).await.unwrap();
        tree.require_split_tape=true;tree.split_decisions=splits.clone().into();tree.balance_rounds=rounds.clone().into();
        tree.set_lazy_disabled_evaluation_trace(lazy).unwrap();
        let before=tree.counters().unwrap()[&0].actor_commands;
        tree.add_dynamic_batch(&batch,false).await.unwrap();
        assert!(tree.split_decisions.is_empty()&&tree.balance_rounds.is_empty());
        let after=tree.counters().unwrap()[&0].actor_commands;
        let attr=tree.disabled_trace_attribution();let e=attr["evaluation_events"].as_u64().unwrap();assert!(e>0);
        assert!(tree.set_lazy_disabled_evaluation_trace(lazy).is_err());
        let trace_events:Vec<_>=tree.events.iter().filter(|v|v["kind"]=="owner-evaluation").cloned().collect();
        if trace {assert_eq!(trace_events.len(),e as usize);assert_eq!(attr["elided_reference_checks"],0);}
        else if lazy {assert_eq!(attr["elided_reference_checks"],e);assert_eq!(attr["retained_reference_checks"],0);}
        let normalized_full_trace:Vec<_>=tree.events.iter().cloned().map(|mut e| {if e["kind"]=="vector-batch" {e["root"]=json!("per-run immutable UUID");}e}).collect();
        let image=authority_topology(&tree);let events=tree.event_counts.clone();let locations=tree.locations.clone();
        let saved=tree.save().await.unwrap();
        runs.push((trace,lazy,after-before,attr,image,events,locations,saved,trace_events,normalized_full_trace));
    }}
    let baseline=&runs[0];let lazy=&runs[1];
    assert_eq!(baseline.2-lazy.2,baseline.3["evaluation_events"].as_u64().unwrap() as usize);
    for run in &runs[1..] {
        assert_eq!(baseline.4,run.4);assert_eq!(baseline.5,run.5);assert_eq!(baseline.6,run.6);
        compare_saved(&p,&baseline.7,&run.7,&rows).await;
        assert_eq!(census(&p,baseline.7.clone(),64,c.clone()).await,census(&p,run.7.clone(),64,c.clone()).await);
    }
    assert_eq!(runs[2].8,runs[3].8);assert_eq!(runs[2].9,runs[3].9);assert_eq!(runs[2].2,runs[3].2);
    println!("{}",json!({"fixed_rows":64,"evaluation_events":baseline.3["evaluation_events"],"baseline_commands":baseline.2,"lazy_commands":lazy.2,"trace_on_commands":runs[2].2,"exact_saved_parity":true}));

    // A coordinator location pointing to a different, missing current owner
    // must retain the original authoritative lookup failure under both policies.
    for (lazy,missing_current) in [(false,false),(true,false),(false,true),(true,true)] {
        let mut tree=AuthoritativeTree::open_dynamic(p.clone(),genesis.clone(),DistanceFunction::Euclidean,c.clone(),1,2,
            ConstructionLimits{max_vectors:64,max_nodes:256,max_owners:2,max_leaf_rows:128,max_owner_physical_rows:256,max_batch_rows:64,max_dimension:32,max_events:1000,trace:false,move_above:None}).await.unwrap();
        tree.set_lazy_disabled_evaluation_trace(lazy).unwrap();let leaf=tree.locations[&0].1;
        let mut row=tree.rows_owned(leaf,false).unwrap().remove(0);
        if missing_current {row.id=1;tree.locations.insert(1,(0,leaf));} else {tree.locations.insert(0,(1,leaf));}
        let error=tree.trace_validation_probe_for_test(leaf,row,&rows[0]).unwrap_err();
        assert!(error.to_string().contains("reference ID not owned"),"{error:#}");
        assert_eq!(tree.disabled_trace_attribution()["elided_reference_checks"],0);
    }
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
async fn before_scrub_profile_preserves_fixed_work_and_saved_state() {
    let directory=tempfile::tempdir().unwrap();let p=provider(directory.path());
    let mut c=scaled();c.fp_npa=true;c.write_navigation=NavigationMode::Fp;
    let rows:Vec<Vec<f32>>=(0..64).map(|i|{let mut v=vec![0.;32];v[0]=(i%13) as f32;v[1]=(i/13) as f32;v[2]=-0.;v}).collect();
    let reference=HierarchicalSpannWriter::new(32,DistanceFunction::Euclidean,c.clone());reference.add(0,&rows[0]);
    let genesis=reference.commit(&p,None).await.unwrap().flush().await.unwrap();
    let (splits,rounds)=record_construction(||{for id in 1..64 {reference.add(id,&rows[id as usize]);}reference.balance_index();});
    let batch:Vec<_>=(1..64).map(|id|(id as u32,rows[id].clone())).collect();let mut results=vec![];
    for enabled in [false,true] {
        let mut tree=AuthoritativeTree::open_dynamic(p.clone(),genesis.clone(),DistanceFunction::Euclidean,c.clone(),1,1,
            ConstructionLimits{max_vectors:64,max_nodes:256,max_owners:1,max_leaf_rows:128,max_owner_physical_rows:256,max_batch_rows:64,max_dimension:32,max_events:100000,trace:false,move_above:None}).await.unwrap();
        tree.require_split_tape=true;tree.split_decisions=splits.clone().into();tree.balance_rounds=rounds.clone().into();tree.set_lazy_disabled_evaluation_trace(true).unwrap();
        if enabled {tree.enable_before_scrub_attribution().unwrap();assert!(tree.enable_before_scrub_attribution().is_err());}
        tree.add_dynamic_batch(&batch,false).await.unwrap();
        assert!(tree.split_decisions.is_empty()&&tree.balance_rounds.is_empty());
        let profile=tree.before_scrub_profile();
        if enabled {
            let v=&profile["cumulative"];assert!(v["trace_preparations"].as_u64().unwrap()>0);
            assert_eq!(v["physical_rows"],v["reference_queries"]);assert_eq!(v["reference_queries"],v["reference_successes"]);
            assert_eq!(v["physical_requests"],v["semantic_scrub_calls"]);
            assert!(v["semantic_live_rows"].as_u64().unwrap()<=v["physical_rows"].as_u64().unwrap());
            assert!(v["trace_preparation_ns"].as_u64().unwrap()>0&&v["semantic_scrub_ns"].as_u64().unwrap()>0);
            assert!(tree.enable_before_scrub_attribution().is_err());
            println!("{}",profile);
        } else {assert_eq!(profile["enabled"],false);assert!(profile["cumulative"].is_null());}
        let commands=tree.counters().unwrap()[&0].actor_commands;let topology=authority_topology(&tree);let counts=tree.event_counts.clone();let locations=tree.locations.clone();let saved=tree.save().await.unwrap();
        results.push((commands,topology,counts,locations,saved));
    }
    assert_eq!(results[0].0,results[1].0);assert_eq!(results[0].1,results[1].1);assert_eq!(results[0].2,results[1].2);assert_eq!(results[0].3,results[1].3);
    compare_saved(&p,&results[0].4,&results[1].4,&rows).await;
    assert_eq!(census(&p,results[0].4.clone(),64,c.clone()).await,census(&p,results[1].4.clone(),64,c).await);
}
