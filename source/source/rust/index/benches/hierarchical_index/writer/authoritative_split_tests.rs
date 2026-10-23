//! One 101-row evolving operation sequence. Random reference split decisions are
//! replayed exactly; this test measures correctness, not clustering quality.
use super::super::super::super::{
    common::TreeNode,
    reader::{quality::QualityLimits, HierarchicalSpannReader},
};
use super::super::super::{
    live_owner::{LiveOwnerHook, Mode},
    live_owner_tests::record_splits,
    HierarchicalSpannWriter,
};
use super::*;
use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
use chroma_cache::new_cache_for_test;
use chroma_storage::{local::LocalStorage, Storage};
use sha2::{Digest, Sha256};
use std::{path::Path, sync::atomic::Ordering};

pub(in super::super) fn provider(path: &Path) -> BlockfileProvider {
    BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
        Storage::Local(LocalStorage::new(path.to_str().unwrap())),
        1024,
        new_cache_for_test(),
        new_cache_for_test(),
        2,
    ))
}
fn config() -> HierarchicalSpannConfig {
    HierarchicalSpannConfig {
        branching_factor: 128,
        split_threshold: 4,
        merge_threshold: 0,
        fp_npa: true,
        write_beam_min: 128,
        write_beam_max: 128,
        reassign_neighbor_count: 128,
        ..Default::default()
    }
}
fn rows() -> Vec<Vec<f32>> {
    let bytes = std::fs::read(std::env::var("SPANN_AUTHORITY_CANONICAL").unwrap()).unwrap();
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        std::env::var("SPANN_AUTHORITY_CANONICAL_SHA256").unwrap()
    );
    assert_eq!(bytes.len(), 98 * 32 * 4);
    let near = |x: f32| {
        let mut v = vec![0.; 32];
        v[0] = x;
        v[1] = 1.;
        v[2] = -0.;
        v
    };
    let mut rows = vec![near(0.), near(1.), near(30.), near(31.), near(32.)];
    // Far branches expand the same live sequence beyond the small witness.
    // These are explicitly constructed local inputs, not a real-data quality run.
    for row in bytes.chunks_exact(128).take(91) {
        let mut v: Vec<_> = row
            .chunks_exact(4)
            .map(|x| f32::from_le_bytes(x.try_into().unwrap()))
            .collect();
        v[0] += 128.;
        v[31] = 1024.;
        rows.push(v);
    }
    rows.extend([16., 14., 14., 14., 14.].into_iter().map(near));
    assert_eq!(rows.len(), 101);
    rows
}
async fn census(p: &BlockfileProvider, ids: HierarchicalSpannIds, n: u32) -> Value {
    let mut r = HierarchicalSpannReader::open(p, ids, DistanceFunction::Euclidean, config())
        .await
        .unwrap();
    let targets = (0..n).map(|i| (i, u64::from(i))).collect();
    let c = r
        .ground_truth_memberships("complete", u64::from(n), &targets, QualityLimits::default())
        .await
        .unwrap();
    assert_eq!(c.live_rows, u64::from(n));
    assert_eq!(c.expected_gt, n as usize);
    assert_eq!(r.memory_usage().embedding_count, 0);
    serde_json::to_value(c).unwrap()
}
pub(in super::super) fn topology(w: &HierarchicalSpannWriter) -> Value {
    let nodes:BTreeMap<_,_>=w.nodes.iter().map(|r| {let id=*r.key();let n=match r.value(){TreeNode::Leaf(n)=>json!({"parent":n.parent_id,"centroid":n.centroid.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),"code":n.centroid_code,"children":Value::Null}),TreeNode::Internal(n)=>json!({"parent":n.parent_id,"centroid":n.centroid.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),"code":n.centroid_code,"children":n.children})};(id,n)}).collect();
    json!({"root":w.root_id(),"next":w.next_node_id.load(Ordering::Relaxed),"nodes":nodes})
}
pub(in super::super) fn authority_topology(t: &AuthoritativeTree) -> Value {
    let nodes:BTreeMap<_,_>=t.routing.nodes.iter().map(|(&id,n)|(id,json!({"parent":n.parent,"centroid":n.centroid.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),"code":n.centroid_code,"children":n.children}))).collect();
    json!({"root":t.routing.root,"next":t.routing.next,"nodes":nodes})
}
pub(in super::super) fn normalized(events: &[Value], reference: bool) -> Vec<Value> {
    let mut result = vec![];
    for original in events {
        let mut e = original.clone();
        let kind = e["kind"].as_str().unwrap().to_string();
        match kind.as_str() {
            "split" => {
                e = json!({"kind":"split","old":e["old"],"left":e["left"],"right":e["right"],"owner":e["owner"]});
            }
            "discovery" | "before_scrub" | "add_destination" | "destination" | "registered"
            | "leaf_registered" => {
                for key in ["scope", "reply"] {
                    e.as_object_mut().unwrap().remove(key);
                }
            }
            "begin" if reference => {
                for key in ["scope", "session", "source"] {
                    e.as_object_mut().unwrap().remove(key);
                }
            }
            "neighbor-begin" if !reference => {
                e["kind"] = json!("begin");
            }
            "version" if reference => {
                e.as_object_mut().unwrap().remove("session");
            }
            "eligible" if reference => {
                e.as_object_mut().unwrap().remove("session");
            }
            "end" if reference => {
                e = json!({"kind":"end"});
            }
            "neighbor-end" if !reference => {
                e = json!({"kind":"end"});
            }
            "owner-evaluation" if !reference && e["neighbor"] == true => {
                result.push(json!({"kind":"version","index":e["index"],"id":e["id"],"stored":e["stored"],"current":e["current"]}));
                if e["valid"] == true && e["seen"] == false {
                    result.push(json!({"kind":"eligible","index":e["index"],"id":e["id"],"eligible":e["eligible"]}));
                }
                continue;
            }
            _ => continue,
        }
        result.push(e);
    }
    result
}
pub(in super::super) async fn compare_saved(
    p: &BlockfileProvider,
    a: &HierarchicalSpannIds,
    b: &HierarchicalSpannIds,
    rows: &[Vec<f32>],
) {
    let ap = storage(
        p.read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            a.posting_list_id,
            String::new(),
        ))
        .await,
    )
    .unwrap();
    let bp = storage(
        p.read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            b.posting_list_id,
            String::new(),
        ))
        .await,
    )
    .unwrap();
    let x: Vec<_> = storage(ap.get_range(""..="", ..).await).unwrap().collect();
    let y: Vec<_> = storage(bp.get_range(""..="", ..).await).unwrap().collect();
    assert_eq!(x.len(), y.len());
    for ((_, xi, x), (_, yi, y)) in x.iter().zip(&y) {
        assert_eq!(xi, yi);
        assert_eq!(x.ids, y.ids);
        assert_eq!(x.versions, y.versions);
        assert_eq!(x.codes, y.codes);
    }
    let av = storage(
        p.read::<u32, &'static [f32]>(BlockfileReaderOptions::new(a.vector_data_id, String::new()))
            .await,
    )
    .unwrap();
    let bv = storage(
        p.read::<u32, &'static [f32]>(BlockfileReaderOptions::new(b.vector_data_id, String::new()))
            .await,
    )
    .unwrap();
    let am = storage(
        p.read::<u32, u32>(BlockfileReaderOptions::new(
            a.scalar_metadata_id,
            String::new(),
        ))
        .await,
    )
    .unwrap();
    let bm = storage(
        p.read::<u32, u32>(BlockfileReaderOptions::new(
            b.scalar_metadata_id,
            String::new(),
        ))
        .await,
    )
    .unwrap();
    for (id, row) in rows.iter().enumerate() {
        let expected: Vec<_> = row.iter().map(|v| v.to_bits()).collect();
        for reader in [&av, &bv] {
            assert_eq!(
                storage(reader.get(PREFIX_EMBEDDING, id as u32).await)
                    .unwrap()
                    .unwrap()
                    .iter()
                    .map(|v| v.to_bits())
                    .collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(
            storage(am.get(PREFIX_VERSION, id as u32).await).unwrap(),
            storage(bm.get(PREFIX_VERSION, id as u32).await).unwrap()
        );
    }
    let ac: Vec<_> = storage(av.get_range(PREFIX_CENTROID..=PREFIX_CENTROID, ..).await)
        .unwrap()
        .collect();
    let bc: Vec<_> = storage(bv.get_range(PREFIX_CENTROID..=PREFIX_CENTROID, ..).await)
        .unwrap()
        .collect();
    assert_eq!(ac.len(), bc.len());
    for ((_, ai, a), (_, bi, b)) in ac.iter().zip(&bc) {
        assert_eq!(ai, bi);
        assert_eq!(
            a.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            b.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evolving_authority_root_split_cross_owner_npa_and_saved101_match_reference() {
    let rows = rows();
    let out = std::path::PathBuf::from(std::env::var("SPANN_DYNAMIC_GLOBAL_OUTPUT").unwrap());
    let store = out.join("evolving-store");
    std::fs::create_dir(&store).unwrap();
    let p = provider(&store);
    let initial = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    initial.add(0, &rows[0]);
    let saved = initial
        .commit(&p, None)
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    drop(initial);
    let old = census(&p, saved.clone(), 1).await;
    let old_files: BTreeMap<_, _> = walk(&store)
        .into_iter()
        .map(|f| {
            (
                f.strip_prefix(&store)
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                format!("{:x}", Sha256::digest(std::fs::read(&f).unwrap())),
            )
        })
        .collect();
    let reference =
        HierarchicalSpannWriter::open(&p, saved.clone(), DistanceFunction::Euclidean, config())
            .await
            .unwrap();
    reference.load_all_postings().await.unwrap();
    reference.load_embeddings_sync(&[0]);
    let mut tree = AuthoritativeTree::open(
        p.clone(),
        saved.clone(),
        DistanceFunction::Euclidean,
        config(),
        1,
        3,
    )
    .await
    .unwrap();
    let mut reference_events = vec![];
    let mut semantic_pairs = vec![];
    let mut snapshots = vec![];
    let mut reference_parent = saved.clone();
    let mut saved96 = None;
    let mut pinned96 = None;
    let mut pinned_report = None;
    let mut old96_files = BTreeMap::new();
    for id in 1..rows.len() {
        let hook = Arc::new(
            LiveOwnerHook::new(
                Mode::ReferenceTrace,
                tree.branches.clone(),
                3,
                32,
                DistanceFunction::Euclidean,
            )
            .unwrap(),
        );
        *reference.live_owner_hook.lock() = Some(hook.clone());
        let decisions = record_splits(|| {
            reference.add(id as u32, &rows[id]);
            reference.balance_index();
        });
        tree.split_decisions = decisions.into();
        let start = tree.events.len();
        tree.add_with_owner_balance(id as u32, rows[id].clone())
            .await
            .unwrap();
        assert!(tree.split_decisions.is_empty());
        let r = normalized(&hook.events(), true);
        let a = normalized(&tree.events[start..], false);
        if r != a {
            std::fs::write(
                out.join("mismatched-semantic-events.json"),
                serde_json::to_vec_pretty(&json!({"id":id,"reference":r,"authority":a})).unwrap(),
            )
            .unwrap();
        }
        assert_eq!(r, a, "full semantic construction trace for add {id}");
        assert_eq!(
            topology(&reference),
            authority_topology(&tree),
            "topology at add {id}"
        );
        reference_events.extend(hook.events());
        semantic_pairs.push(json!({"id":id,"reference":r,"authority":a}));
        if id == 4 {
            let leaf = *tree
                .branches
                .keys()
                .max_by(|a, b| {
                    tree.routing.nodes[a].centroid[0].total_cmp(&tree.routing.nodes[b].centroid[0])
                })
                .unwrap();
            tree.move_branch(leaf, 1).unwrap();
            assert!(tree.lookup_at(0, 2).is_err());
            assert_eq!(tree.lookup_at(1, 2).unwrap().version, 1);
        }
        if id == 95 {
            let s = tree.save().await.unwrap();
            let r = reference
                .commit(&p, Some(&reference_parent))
                .await
                .unwrap()
                .flush()
                .await
                .unwrap();
            reference_parent = r.clone();
            compare_saved(&p, &s, &r, &rows[..96]).await;
            let c = census(&p, s.clone(), 96).await;
            assert_eq!(c, census(&p, r, 96).await);
            snapshots.push(json!({"rows":96,"census":c}));
            let mut pinned =
                HierarchicalSpannReader::open(&p, s.clone(), DistanceFunction::Euclidean, config())
                    .await
                    .unwrap();
            let targets = (0..96).map(|i| (i, u64::from(i))).collect();
            pinned_report = Some(
                serde_json::to_value(
                    pinned
                        .ground_truth_memberships("pinned", 96, &targets, QualityLimits::default())
                        .await
                        .unwrap(),
                )
                .unwrap(),
            );
            pinned96 = Some(pinned);
            old96_files = walk(&store)
                .into_iter()
                .map(|f| {
                    (
                        f.strip_prefix(&store)
                            .unwrap()
                            .to_string_lossy()
                            .to_string(),
                        format!("{:x}", Sha256::digest(std::fs::read(&f).unwrap())),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            saved96 = Some((s, c));
        }
    }
    let authority = tree.save().await.unwrap();
    let expected = reference
        .commit(&p, Some(&reference_parent))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    compare_saved(&p, &authority, &expected, &rows).await;
    let final_census = census(&p, authority.clone(), 101).await;
    assert_eq!(final_census, census(&p, expected.clone(), 101).await);
    assert_eq!(old, census(&p, saved.clone(), 1).await);
    let (mid, mid_census) = saved96.unwrap();
    assert_eq!(mid_census, census(&p, mid.clone(), 96).await);
    let targets = (0..96).map(|i| (i, u64::from(i))).collect();
    let pinned_after = serde_json::to_value(
        pinned96
            .as_mut()
            .unwrap()
            .ground_truth_memberships("pinned", 96, &targets, QualityLimits::default())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(pinned_report.as_ref().unwrap(), &pinned_after);
    for (file, hash) in old_files.iter().chain(&old96_files) {
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(std::fs::read(store.join(file)).unwrap())
            ),
            *hash
        );
    }
    let mut split_owner = 0;
    let mut cross = false;
    let mut registrations = BTreeMap::new();
    let mut later = false;
    for (position, event) in tree.events.iter().enumerate() {
        match event["kind"].as_str().unwrap() {
            "neighbor-begin" => split_owner = event["split_owner"].as_u64().unwrap(),
            "owner-evaluation" if event["neighbor"] == true && event["eligible"] == true => {
                if event["owner"].as_u64().unwrap() != split_owner {
                    cross = true;
                }
            }
            "registered" => {
                registrations.insert(
                    event["id"].as_u64().unwrap(),
                    (position, event["current"].as_u64().unwrap()),
                );
            }
            "before_scrub" => {
                if let Some(rows) = event["versions"].as_array() {
                    for row in rows {
                        if let Some((prior, version)) = registrations.get(&row[0].as_u64().unwrap())
                        {
                            if position > *prior
                                && row[2].as_u64() == Some(*version)
                                && row[1] != row[2]
                            {
                                later = true;
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    assert!(cross, "actual cross-owner neighbor eligibility required");
    assert!(
        later,
        "earlier owner registration must be visible before later scrub"
    );
    let error = tree
        .structural(
            2,
            OwnerCommand::Physical(*tree.branches.iter().find(|(_, o)| **o == 1).unwrap().0),
        )
        .err()
        .unwrap();
    assert!(error
        .to_string()
        .contains("physical rows require preparation"));
    assert!(tree.lookup_at(99, 0).is_err());
    let before_rejection = tree.counters().unwrap();
    let before_locations = tree.locations.clone();
    let before_topology = authority_topology(&tree);
    let old_branching = tree.routing.config.branching_factor;
    tree.routing.config.branching_factor = tree.routing.nodes[&tree.routing.root]
        .children
        .as_ref()
        .unwrap()
        .len();
    let rejected = tree
        .add_with_owner_balance(101, rows[100].clone())
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        rejected,
        "internal parent headroom required before mutation"
    );
    tree.routing.config.branching_factor = old_branching;
    assert_eq!(tree.locations, before_locations);
    assert_eq!(authority_topology(&tree), before_topology);
    let after_rejection = tree.counters().unwrap();
    for owner in before_rejection.keys() {
        assert_eq!(
            before_rejection[owner].prepared_current_records,
            after_rejection[owner].prepared_current_records
        );
        assert_eq!(
            before_rejection[owner].registered_rows,
            after_rejection[owner].registered_rows
        );
    }
    std::fs::write(out.join("evolving-authority-result.json"),serde_json::to_vec_pretty(&json!({"status":"passed","rows":101,"config":{"branching":128,"threshold":4,"merge":0,"fp_npa":true},"constructed_input_bits":rows.iter().map(|v|v.iter().map(|x|x.to_bits()).collect::<Vec<_>>()).collect::<Vec<_>>(),"authority_events":tree.events,"reference_events":reference_events,"semantic_pairs":semantic_pairs,"snapshots":snapshots,"saved_old":ids_json(&saved),"saved96":ids_json(&mid),"saved_authority":ids_json(&authority),"saved_reference":ids_json(&expected),"old_census":old,"census":final_census,"cross_owner":cross,"later_version_visibility":later,"headroom_rejection":rejected,"directory":tree.branches,"counters":tree.counters().unwrap(),"old_object_hashes":old_files,"old96_object_hashes":old96_files,"pinned96_report":pinned_after,"scope":"local owner-authoritative prepared operations; shared provider; no distributed durability or quality claim"})).unwrap()).unwrap();
}
pub(in super::super) fn walk(root: &Path) -> Vec<std::path::PathBuf> {
    let mut files = vec![];
    for e in std::fs::read_dir(root).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            files.extend(walk(&p));
        } else {
            files.push(p);
        }
    }
    files.sort();
    files
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evolving_authority_rejects_unsupported_and_discards_prepared_root_split() {
    let dir = tempfile::tempdir().unwrap();
    let p = provider(dir.path());
    let w = HierarchicalSpannWriter::new(32, DistanceFunction::Euclidean, config());
    let inputs: Vec<_> = (0..5).map(|id| vec![id as f32; 32]).collect();
    for id in 0..4 {
        w.add(id as u32, &inputs[id]);
    }
    let saved = w.commit(&p, None).await.unwrap().flush().await.unwrap();
    let mut t = AuthoritativeTree::open(
        p.clone(),
        saved.clone(),
        DistanceFunction::Euclidean,
        config(),
        4,
        2,
    )
    .await
    .unwrap();
    let wrong_owner = t
        .scan_at(
            1,
            t.routing.root,
            vec![0.; 32],
            vec![1.; 32],
            vec![2.; 32],
            true,
        )
        .unwrap_err();
    assert_eq!(wrong_owner.to_string(), "scan leaf not owned");
    assert_eq!(t.lookup_at(99, 0).unwrap_err().to_string(), "owner missing");
    let before = t.counters().unwrap();
    let original = t.routing.config.clone();
    t.routing.config.merge_threshold = 1;
    let e = t
        .add_with_owner_balance(4, inputs[4].clone())
        .await
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "evolving authority requires FP navigation/FP NPA and merges disabled"
    );
    t.routing.config = original;
    let root = t.routing.root;
    t.routing.nodes.get_mut(&root).unwrap().children = Some(vec![root]);
    let e = t
        .add_with_owner_balance(4, inputs[4].clone())
        .await
        .unwrap_err();
    assert_eq!(
        e.to_string(),
        "internal split and multi-level parent edits unsupported"
    );
    t.routing.nodes.get_mut(&root).unwrap().children = None;
    assert_eq!(t.locations.len(), 4);
    let after = t.counters().unwrap();
    for owner in before.keys() {
        assert_eq!(
            before[owner].prepared_posting_payload_bytes,
            after[owner].prepared_posting_payload_bytes
        );
        assert_eq!(
            before[owner].prepared_current_records,
            after[owner].prepared_current_records
        );
        assert_eq!(before[owner].registered_rows, after[owner].registered_rows);
    }
    let prior_census = census(&p, saved.clone(), 4).await;
    let prior_owners = owner_state(&t);
    let prior_topology = authority_topology(&t);
    let prior_locations = t.locations.clone();
    let prior_branches = t.branches.clone();
    let prior_tombstones = t.tombstones.clone();
    let prior_vector = t.latest_vectors;
    let prior_parent = ids_json(&t.parent);
    let old_files: BTreeMap<_, _> = walk(dir.path())
        .into_iter()
        .map(|f| {
            (
                f.strip_prefix(dir.path())
                    .unwrap()
                    .to_string_lossy()
                    .to_string(),
                format!("{:x}", Sha256::digest(std::fs::read(f).unwrap())),
            )
        })
        .collect();
    let before = t.counters().unwrap();
    // The entry limit passes, then actual private registration and root splitting
    // push the checked report over its cap before authority installation.
    t.events = vec![json!({"kind":"preexisting-test-event"}); 9_999];
    t.split_decisions = record_splits(|| {
        w.add(4, &inputs[4]);
        w.balance_index();
    })
    .into();
    assert!(!t.split_decisions.is_empty());
    let rejected = t
        .add_with_owner_balance(4, inputs[4].clone())
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(rejected, "prepared operation cap");
    assert!(t.split_decisions.is_empty());
    assert!(t.events.iter().any(|e| e["kind"] == "split"));
    let after = t.counters().unwrap();
    assert!(after[&0].prepared_current_records > before[&0].prepared_current_records);
    assert!(after[&0].prepared_posting_payload_bytes > before[&0].prepared_posting_payload_bytes);
    assert_eq!(after[&0].registered_rows, before[&0].registered_rows + 1);
    assert!(after[&1].actor_commands > before[&1].actor_commands);
    assert_eq!(owner_state(&t), prior_owners);
    assert_eq!(authority_topology(&t), prior_topology);
    assert_eq!(t.locations, prior_locations);
    assert_eq!(t.branches, prior_branches);
    assert_eq!(t.tombstones, prior_tombstones);
    assert_eq!(t.latest_vectors, prior_vector);
    assert_eq!(ids_json(&t.parent), prior_parent);
    assert!(!t.poisoned);
    let limit = t
        .owner(0)
        .unwrap()
        .call(Command::CanRegister(root, 4))
        .err()
        .unwrap();
    assert_eq!(
        limit.to_string(),
        "registration conflict or structural split required"
    );
    let restored = t.save().await.unwrap();
    assert_eq!(restored.vector_data_id, prior_vector);
    compare_saved(&p, &restored, &saved, &inputs[..4]).await;
    assert_eq!(census(&p, restored.clone(), 4).await, prior_census);
    let scalar = storage(
        p.read::<u32, u32>(BlockfileReaderOptions::new(
            restored.scalar_metadata_id,
            String::new(),
        ))
        .await,
    )
    .unwrap();
    assert_eq!(storage(scalar.get(PREFIX_VERSION, 4).await).unwrap(), None);
    assert_eq!(
        storage(scalar.get(PREFIX_NEXT_NODE, SINGLETON_KEY).await).unwrap(),
        Some(t.routing.next)
    );
    let vectors = storage(
        p.read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
            restored.vector_data_id,
            String::new(),
        ))
        .await,
    )
    .unwrap();
    assert!(storage(vectors.get(PREFIX_EMBEDDING, 4).await)
        .unwrap()
        .is_none());
    for (file, hash) in &old_files {
        assert_eq!(
            format!(
                "{:x}",
                Sha256::digest(std::fs::read(dir.path().join(file)).unwrap())
            ),
            *hash
        );
    }
    let output = std::path::PathBuf::from(std::env::var("SPANN_DYNAMIC_GLOBAL_OUTPUT").unwrap());
    std::fs::write(output.join("prepared-discard-result.json"),serde_json::to_vec_pretty(&json!({"status":"passed","rejection":rejected,"prepared_split_events":t.events.iter().filter(|e|e["kind"]=="split").collect::<Vec<_>>(),"before_counters":before,"after_counters":after,"prior_owner_state":prior_owners,"restored_owner_state":owner_state(&t),"prior_topology":prior_topology,"restored_topology":authority_topology(&t),"prior_parent":prior_parent,"restored_saved":ids_json(&restored),"prior_census":prior_census,"old_objects":old_files,"rejected_id":4,"private_vector_objects":"may remain unreferenced; no physical rollback claim"})).unwrap()).unwrap();
}
fn owner_state(t: &AuthoritativeTree) -> Value {
    let mut owners = BTreeMap::new();
    for (&owner, actor) in &t.owners {
        let Reply::Ready(rows) = actor.call(Command::Ready).unwrap() else {
            panic!("ready reply");
        };
        let mut states = vec![];
        for (id, leaf) in rows {
            let lookup = t.lookup_at(owner, id).unwrap();
            states.push(json!({"id":id,"leaf":leaf,"version":lookup.version,"vector_bits":lookup.vector_bits}));
        }
        owners.insert(owner, states);
    }
    json!(owners)
}

pub(in super::super) fn ids_json(ids: &HierarchicalSpannIds) -> Value {
    json!({"posting_list_id":ids.posting_list_id,"scalar_metadata_id":ids.scalar_metadata_id,"vector_data_id":ids.vector_data_id,"leaf_node_id":ids.leaf_node_id,"internal_node_id":ids.internal_node_id})
}
