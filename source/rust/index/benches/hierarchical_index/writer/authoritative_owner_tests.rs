use super::super::{
    common::ReadBeamPolicy,
    config::HierarchicalSpannConfig,
    persistance::PREFIX_EMBEDDING,
    reader::{quality::QualityLimits, HierarchicalSpannReader},
};
use super::{
    authoritative_owner::AuthoritativeTree, lazy_route::Bootstrap, HierarchicalSpannWriter,
};
use chroma_blockstore::{
    arrow::provider::{ArrowBlockfileProvider, BlockfileReaderOptions},
    provider::BlockfileProvider,
};
use chroma_cache::new_cache_for_test;
use chroma_distance::DistanceFunction;
use chroma_storage::{local::LocalStorage, Storage};
use chroma_types::hierarchical_spann::HierarchicalSpannPostingList;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
    sync::Arc,
};
fn provider(path: &Path) -> BlockfileProvider {
    BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
        Storage::Local(LocalStorage::new(path.to_str().unwrap())),
        1024,
        new_cache_for_test(),
        new_cache_for_test(),
        2,
    ))
}
fn fixture() -> Vec<Vec<f32>> {
    let path = std::env::var("SPANN_AUTHORITY_CANONICAL").expect("retained canonical file");
    let data = std::fs::read(path).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(&data)),
        std::env::var("SPANN_AUTHORITY_CANONICAL_SHA256").unwrap()
    );
    assert_eq!(data.len(), 98 * 32 * 4);
    data.chunks_exact(128)
        .map(|row| {
            row.chunks_exact(4)
                .map(|v| f32::from_le_bytes(v.try_into().unwrap()))
                .collect()
        })
        .collect()
}
async fn census(
    p: &BlockfileProvider,
    ids: super::HierarchicalSpannIds,
    n: u32,
    config: HierarchicalSpannConfig,
) -> Value {
    let mut reader = HierarchicalSpannReader::open(p, ids, DistanceFunction::Euclidean, config)
        .await
        .unwrap();
    let targets = (0..n).map(|id| (id, u64::from(id))).collect();
    let c = reader
        .ground_truth_memberships("all", u64::from(n), &targets, QualityLimits::default())
        .await
        .unwrap();
    assert_eq!(c.live_rows, u64::from(n));
    assert_eq!(c.expected_gt, n as usize);
    assert_eq!(
        c.leaves
            .iter()
            .flat_map(|l| l.gt_ids.iter().copied())
            .collect::<BTreeSet<_>>(),
        (0..u64::from(n)).collect()
    );
    assert_eq!(reader.memory_usage().embedding_count, 0);
    serde_json::to_value(c).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authoritative_owner_move_mutate_handoff_and_saved98_match_reference() {
    let output = std::env::var("SPANN_DYNAMIC_GLOBAL_OUTPUT").expect("fresh evidence root");
    let store = Path::new(&output).join("authoritative-store");
    std::fs::create_dir(&store).unwrap();
    let p = provider(&store);
    let rows = fixture();
    let config = HierarchicalSpannConfig {
        branching_factor: 4,
        split_threshold: 8,
        merge_threshold: 1,
        write_beam_min: 128,
        write_beam_max: 128,
        reassign_neighbor_count: 128,
        ..Default::default()
    };
    let w = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config.clone());
    for (id, v) in rows[..96].iter().enumerate() {
        w.add(id as u32, v);
        w.balance_index();
    }
    let parent = w.commit(&p, None).await.unwrap().flush().await.unwrap();
    drop(w);
    let old = census(&provider(&store), parent.clone(), 96, config.clone()).await;
    let mut owned = AuthoritativeTree::open(
        provider(&store),
        parent.clone(),
        DistanceFunction::Euclidean,
        config.clone(),
        96,
        2,
    )
    .await
    .unwrap();
    assert!(owned.locations().values().all(|(o, _)| *o == 0));
    let reference = HierarchicalSpannWriter::open(
        &provider(&store),
        parent.clone(),
        DistanceFunction::Euclidean,
        config.clone(),
    )
    .await
    .unwrap();
    reference.load_all_postings().await.unwrap();
    reference.load_embeddings_sync(&(0..96).collect::<Vec<_>>());
    let topology = Bootstrap::capture(&reference).unwrap();
    let policy = ReadBeamPolicy::uniform(
        Some(config.write_beam_tau),
        config.write_beam_min,
        config.write_beam_max,
    );
    for v in &rows[..96] {
        let a = reference.navigate_with_policy(v, config.write_navigation, &policy);
        let b = owned.candidates(v).unwrap();
        assert_eq!(
            a.iter().map(|(i, d)| (*i, d.to_bits())).collect::<Vec<_>>(),
            b.iter().map(|(i, d)| (*i, d.to_bits())).collect::<Vec<_>>()
        );
    }
    let source_id = 0;
    let source_leaf = owned.locations()[&source_id].1;
    assert!(
        owned.leaf_count(source_leaf).unwrap() <= 6,
        "source has room for two retained-vector insertions"
    );
    let target_id = (1..96u32)
        .find(|id| {
            let leaf = owned.locations()[id].1;
            leaf != source_leaf
                && owned.leaf_count(leaf).unwrap() <= 6
                && owned.candidates(&rows[*id as usize]).unwrap()[0].0 == leaf
        })
        .expect("bounded existing target with headroom");
    let target_leaf = owned.locations()[&target_id].1;
    let before_move = owned.lookup_at(0, source_id).unwrap();
    assert!(owned.lookup_at(99, source_id).is_err());
    let before = owned.locations().clone();
    assert!(owned.move_branch(source_leaf, 99).is_err());
    assert_eq!(owned.locations(), &before);
    assert!(owned.reject_structural_edit().is_err());
    assert_eq!(owned.locations(), &before);
    owned.move_branch(source_leaf, 1).unwrap();
    let after_move = owned.lookup_at(1, source_id).unwrap();
    assert_eq!(before_move.vector_bits, after_move.vector_bits);
    assert_eq!(before_move.version, after_move.version);
    assert_ne!(before_move.thread, after_move.thread);
    assert!(owned.lookup_at(0, source_id).is_err());
    assert_eq!(owned.branch_owners()[&source_leaf], 1);
    assert_eq!(owned.branch_owners()[&target_leaf], 0);
    owned
        .add_new(96, rows[source_id as usize].clone())
        .await
        .unwrap();
    reference.add(96, &rows[source_id as usize]);
    assert_eq!(
        owned.lookup_at(1, 96).unwrap().version,
        reference.current_version_sync(96).unwrap()
    );
    owned
        .replace_and_reroute(source_id, rows[target_id as usize].clone())
        .await
        .unwrap();
    reference.insert_embedding(source_id, Arc::from(rows[target_id as usize].clone()));
    reference.reassign(source_leaf, source_id, 0);
    let current = owned.lookup_at(0, source_id).unwrap();
    assert_eq!(current.leaf, target_leaf);
    assert_eq!(current.version, 2);
    assert_eq!(
        current.version,
        reference.current_version_sync(source_id).unwrap()
    );
    assert_eq!(
        current.vector_bits,
        rows[target_id as usize]
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>()
    );
    assert!(owned.lookup_at(1, source_id).is_err());
    let old_center = vec![10_000.; 32];
    let left = rows[target_id as usize].clone();
    let right = left.clone();
    let source_scan = owned
        .scan_at(
            1,
            source_leaf,
            old_center.clone(),
            left.clone(),
            right.clone(),
            true,
        )
        .unwrap();
    let target_scan = owned
        .scan_at(0, target_leaf, old_center, left, right, true)
        .unwrap();
    assert!(!source_scan.contains(&source_id));
    assert!(
        target_scan.contains(&source_id),
        "destination scan must use owner current v2 and durable replacement vector"
    );
    assert!(owned
        .scan_at(
            1,
            target_leaf,
            vec![0.; 32],
            vec![0.; 32],
            vec![0.; 32],
            true
        )
        .is_err());
    owned
        .add_new(97, rows[source_id as usize].clone())
        .await
        .unwrap();
    reference.add(97, &rows[source_id as usize]);
    assert_eq!(owned.lookup_at(1, 97).unwrap().version, 1);
    assert_eq!(
        Bootstrap::capture(&reference).unwrap().topology_sha256,
        topology.topology_sha256,
        "reference operation slice stays within existing topology"
    );
    let owner_saved = owned.save().await.unwrap();
    let reference_saved = reference
        .commit(&p, Some(&parent))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    let owner_census = census(&provider(&store), owner_saved.clone(), 98, config.clone()).await;
    let reference_census = census(
        &provider(&store),
        reference_saved.clone(),
        98,
        config.clone(),
    )
    .await;
    assert_eq!(owner_census, reference_census);
    assert_eq!(
        old,
        census(&provider(&store), parent.clone(), 96, config.clone()).await
    );
    let pl_owner = p
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            owner_saved.posting_list_id,
            String::new(),
        ))
        .await
        .unwrap();
    let pl_ref = p
        .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            reference_saved.posting_list_id,
            String::new(),
        ))
        .await
        .unwrap();
    let mut saved_postings = BTreeMap::new();
    for &leaf in owned.branch_owners().keys() {
        let a = pl_owner.get("", leaf).await.unwrap().unwrap();
        let b = pl_ref.get("", leaf).await.unwrap().unwrap();
        assert_eq!(a.ids, b.ids);
        assert_eq!(a.versions, b.versions);
        assert_eq!(a.codes, b.codes);
        saved_postings.insert(leaf,json!({"ids":a.ids,"versions":a.versions,"codes_sha256":format!("{:x}",Sha256::digest(a.codes))}));
    }
    let vr = p
        .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
            owner_saved.vector_data_id,
            String::new(),
        ))
        .await
        .unwrap();
    let reference_vectors = p
        .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
            reference_saved.vector_data_id,
            String::new(),
        ))
        .await
        .unwrap();
    for id in 0..98u32 {
        let expected = if id == 0 {
            &rows[target_id as usize]
        } else if id >= 96 {
            &rows[0]
        } else {
            &rows[id as usize]
        };
        let actual = vr.get(PREFIX_EMBEDDING, id).await.unwrap().unwrap();
        let ref_actual = reference_vectors
            .get(PREFIX_EMBEDDING, id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            actual.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
            ref_actual.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
        );
        assert_eq!(
            actual.iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
            expected.iter().map(|x| x.to_bits()).collect::<Vec<_>>()
        );
    }
    let counters = owned.counters().unwrap();
    assert_eq!(counters[&1].exported_current_records, 1);
    assert_eq!(counters[&0].accepted_current_records, 1);
    assert!(counters[&0].moved_branch_payload_bytes > 0);
    assert!(counters.values().map(|c| c.vector_gets).sum::<usize>() > 0);
    std::fs::write(Path::new(&output).join("authoritative-result.json"),serde_json::to_vec_pretty(&json!({"parent_roots":{"posting":parent.posting_list_id,"scalar":parent.scalar_metadata_id,"vectors":parent.vector_data_id,"leaf":parent.leaf_node_id,"internal":parent.internal_node_id},"owner_roots":{"posting":owner_saved.posting_list_id,"scalar":owner_saved.scalar_metadata_id,"vectors":owner_saved.vector_data_id,"leaf":owner_saved.leaf_node_id,"internal":owner_saved.internal_node_id},"events":owned.events,"before_move":before_move,"after_move":after_move,"current_after_handoff":current,"source_leaf":source_leaf,"target_leaf":target_leaf,"target_id":target_id,"source_scan":source_scan,"target_scan":target_scan,"owner_census":owner_census,"reference_census":reference_census,"old_census":old,"saved_postings":saved_postings,"counters":counters,"locations":owned.locations(),"branches":owned.branch_owners(),"scope":"authoritative owner fixed-topology mutation; automatic split/merge unsupported"})).unwrap()).unwrap();
}
