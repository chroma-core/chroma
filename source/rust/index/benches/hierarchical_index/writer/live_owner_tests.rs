//! Local paired correctness with the actual reference split decisions replayed.
//! The production splitter and its randomness are unchanged.
use super::super::{
    config::HierarchicalSpannConfig,
    reader::{quality::QualityLimits, HierarchicalSpannReader},
};
use super::{
    dynamic_global::{CapacityPolicy, DynamicGlobalTree},
    live_owner::{LiveOwnerHook, Mode},
    HierarchicalSpannWriter,
};
use chroma_blockstore::{arrow::provider::ArrowBlockfileProvider, provider::BlockfileProvider};
use chroma_cache::new_cache_for_test;
use chroma_distance::DistanceFunction;
use chroma_index::spann::utils::{self, EmbeddingPoint, SplitResult};
use chroma_storage::{local::LocalStorage, Storage};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{cell::RefCell, collections::BTreeMap, path::Path, sync::Arc};
#[derive(Clone, Serialize, Deserialize)]
struct SplitDecision {
    input: Vec<(u32, u32, Vec<u32>)>,
    left: Vec<u32>,
    left_ids: Vec<u32>,
    right: Vec<u32>,
    right_ids: Vec<u32>,
}
struct Tape {
    replay: bool,
    decisions: Vec<SplitDecision>,
    position: usize,
}
thread_local! {static TAPE:RefCell<Option<Tape>>=const{RefCell::new(None)};}
pub(super) fn reference_split(
    points: Vec<EmbeddingPoint>,
    metric: &DistanceFunction,
) -> SplitResult {
    let input: Vec<_> = points
        .iter()
        .map(|(id, v, e)| (*id, *v, e.iter().map(|x| x.to_bits()).collect()))
        .collect();
    let replay = TAPE.with(|cell| {
        let mut c = cell.borrow_mut();
        if let Some(t) = c.as_mut() {
            if t.replay {
                let d = t
                    .decisions
                    .get(t.position)
                    .expect("missing recorded split")
                    .clone();
                t.position += 1;
                return Some(d);
            }
        }
        None
    });
    if let Some(d) = replay {
        assert_eq!(d.input, input, "reference split input changed");
        let mut by_id: BTreeMap<_, _> = points.into_iter().map(|p| (p.0, p)).collect();
        let left = d
            .left_ids
            .iter()
            .map(|id| by_id.remove(id).expect("left split ID"))
            .collect();
        let right = d
            .right_ids
            .iter()
            .map(|id| by_id.remove(id).expect("right split ID"))
            .collect();
        assert!(by_id.is_empty());
        return (
            Arc::from(d.left.into_iter().map(f32::from_bits).collect::<Vec<_>>()),
            left,
            Arc::from(d.right.into_iter().map(f32::from_bits).collect::<Vec<_>>()),
            right,
        );
    }
    let result = utils::split(points, metric);
    TAPE.with(|cell| {
        if let Some(t) = cell.borrow_mut().as_mut() {
            t.decisions.push(SplitDecision {
                input,
                left: result.0.iter().map(|x| x.to_bits()).collect(),
                left_ids: result.1.iter().map(|p| p.0).collect(),
                right: result.2.iter().map(|x| x.to_bits()).collect(),
                right_ids: result.3.iter().map(|p| p.0).collect(),
            });
        }
    });
    result
}
fn row(id: u32, x: f32) -> (u32, Vec<f32>) {
    (id, vec![x, 1., -0., 0.])
}
fn config(fp: bool) -> HierarchicalSpannConfig {
    HierarchicalSpannConfig {
        fp_npa: fp,
        branching_factor: 16,
        split_threshold: 4,
        merge_threshold: 1,
        write_beam_min: 128,
        write_beam_max: 128,
        reassign_neighbor_count: 128,
        ..Default::default()
    }
}
fn provider(path: &Path) -> BlockfileProvider {
    BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(
        Storage::Local(LocalStorage::new(path.to_str().unwrap())),
        1024,
        new_cache_for_test(),
        new_cache_for_test(),
        2,
    ))
}
fn run(t: &mut DynamicGlobalTree, hook: &Arc<LiveOwnerHook>) {
    t.add_batch(&[row(5, 16.), row(6, 14.), row(7, 14.), row(8, 14.)])
        .unwrap();
    t.add_batch(&[row(9, 14.)]).unwrap();
    assert!(!hook.events().is_empty());
}
async fn census(
    p: &BlockfileProvider,
    ids: super::HierarchicalSpannIds,
    n: u32,
    fp: bool,
) -> Value {
    let mut r = HierarchicalSpannReader::open(p, ids, DistanceFunction::Euclidean, config(fp))
        .await
        .unwrap();
    let targets = (0..n).map(|id| (id, u64::from(id))).collect();
    let c = r
        .ground_truth_memberships("complete", u64::from(n), &targets, QualityLimits::default())
        .await
        .unwrap();
    assert_eq!(c.expected_gt, n as usize);
    assert_eq!(c.live_rows, u64::from(n));
    let actual: BTreeMap<_, _> = c
        .leaves
        .iter()
        .flat_map(|leaf| leaf.gt_ids.iter().map(move |&id| (id, leaf.leaf_id)))
        .collect();
    assert_eq!(actual.len(), n as usize);
    assert_eq!(
        actual.keys().copied().collect::<Vec<_>>(),
        (0..u64::from(n)).collect::<Vec<_>>()
    );
    assert_eq!(r.memory_usage().embedding_count, 0);
    serde_json::to_value(c).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_owner_actual_split_fp_matches_reference_and_saved_membership() {
    run_pair(true).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_owner_actual_split_quantized_matches_reference_and_saved_membership() {
    run_pair(false).await;
}
async fn run_pair(fp: bool) {
    let dir = tempfile::tempdir().unwrap();
    let p = provider(dir.path());
    let capacity = CapacityPolicy {
        move_out_above: 3,
        settle_at: 2,
        max_owners: 4,
    };
    let mut initial =
        DynamicGlobalTree::new(4, DistanceFunction::Euclidean, config(fp), capacity.clone())
            .unwrap();
    assert_eq!(initial.snapshot().nodes.len(), 1);
    initial
        .add_batch(&[
            row(0, 0.),
            row(1, 0.),
            row(2, 30.),
            row(3, 30.),
            row(4, 30.),
        ])
        .unwrap();
    assert_eq!(initial.owner_count(), 2);
    let saved = initial
        .test_reference()
        .commit(&p, None)
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    let old = census(&p, saved.clone(), 5, fp).await;
    let mut copies = vec![];
    for _ in 0..2 {
        let w = HierarchicalSpannWriter::open(
            &p,
            saved.clone(),
            DistanceFunction::Euclidean,
            config(fp),
        )
        .await
        .unwrap();
        w.load_all_postings().await.unwrap();
        w.load_embeddings_sync(&(0..5).collect::<Vec<_>>());
        copies.push(
            DynamicGlobalTree::test_from_reference(
                w,
                initial.branch_owners().clone(),
                capacity.clone(),
            )
            .unwrap(),
        );
    }
    let mut reference = copies.remove(0);
    let mut dispatched = copies.remove(0);
    let reference_hook = reference
        .enable_live_owner_scans(Mode::ReferenceTrace)
        .unwrap();
    TAPE.with(|c| {
        *c.borrow_mut() = Some(Tape {
            replay: false,
            decisions: vec![],
            position: 0,
        })
    });
    run(&mut reference, &reference_hook);
    let decisions = TAPE.with(|c| c.borrow_mut().take().unwrap().decisions);
    assert!(decisions.len() >= 2);
    let dispatched_hook = dispatched
        .enable_live_owner_scans(Mode::OwnerDispatch)
        .unwrap();
    TAPE.with(|c| {
        *c.borrow_mut() = Some(Tape {
            replay: true,
            decisions: decisions.clone(),
            position: 0,
        })
    });
    run(&mut dispatched, &dispatched_hook);
    TAPE.with(|c| {
        let t = c.borrow_mut().take().unwrap();
        assert_eq!(t.position, t.decisions.len());
    });
    assert_eq!(
        reference_hook.events(),
        dispatched_hook.events(),
        "full ordered construction event parity"
    );
    assert_eq!(*reference.snapshot(), *dispatched.snapshot());
    assert_eq!(reference.branch_owners(), dispatched.branch_owners());
    let events = dispatched_hook.events();
    let mut sessions = BTreeMap::new();
    let mut cross = false;
    let mut registration = None;
    let mut later_visibility = false;
    for (position, e) in events.iter().enumerate() {
        match e["kind"].as_str().unwrap() {
            "begin" => {
                sessions.insert(
                    e["session"].as_u64().unwrap(),
                    (
                        e["owner"].as_u64().unwrap(),
                        e["split_owner"].as_u64().unwrap(),
                    ),
                );
            }
            "eligible" => {
                if e["eligible"] == true {
                    let (owner, source) = sessions[&e["session"].as_u64().unwrap()];
                    if owner != source {
                        cross = true;
                    }
                }
            }
            "registered" => {
                if e["id"] == 5 {
                    registration = Some((position, e["current"].as_u64().unwrap()));
                }
            }
            "before_scrub" => {
                if let Some((prior, current)) = registration {
                    if position > prior {
                        if let Some(rows) = e["versions"].as_array() {
                            for row in rows {
                                if row[0] == 5
                                    && row[2].as_u64() == Some(current)
                                    && row[1].as_u64() != Some(current)
                                {
                                    later_visibility = true;
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    assert!(
        cross,
        "actual split must cause crossowner eligible candidate"
    );
    assert!(
        registration.is_some(),
        "global versioned registration required"
    );
    assert!(
        later_visibility,
        "later live scrub must observe earlier immediate registration version"
    );
    assert!(events
        .iter()
        .any(|e| e["kind"] == "destination" && e["id"] == 5));
    assert!(dispatched.branch_owners().len() > initial.branch_owners().len());
    reference.disable_live_owner_scans();
    dispatched.disable_live_owner_scans();
    let ref_ids = reference
        .test_reference()
        .commit(&p, Some(&saved))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    let owner_ids = dispatched
        .test_reference()
        .commit(&p, Some(&saved))
        .await
        .unwrap()
        .flush()
        .await
        .unwrap();
    let ref_census = census(&provider(dir.path()), ref_ids, 10, fp).await;
    let owner_census = census(&provider(dir.path()), owner_ids, 10, fp).await;
    assert_eq!(ref_census, owner_census);
    assert_eq!(old, census(&provider(dir.path()), saved, 5, fp).await);
    let output = std::env::var("SPANN_DYNAMIC_GLOBAL_OUTPUT").expect("fresh output");
    std::fs::write(Path::new(&output).join(if fp {"live-split-fp.json"} else {"live-split-quantized.json"}),serde_json::to_vec_pretty(&json!({"events":events,"reference_events":reference_hook.events(),"transport":dispatched_hook.transport(),"decisions":decisions,"saved_reference":ref_census,"saved_dispatched":owner_census,"saved_old":old,"directory":dispatched.branch_owners(),"tree":dispatched.snapshot(),"crossowner":cross,"later_version_visibility":later_visibility})).unwrap()).unwrap();
}
