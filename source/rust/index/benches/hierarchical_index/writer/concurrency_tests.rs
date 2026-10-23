//! Force mutations between preparation and publication, rather than relying on
//! two operations happening to overlap after a simultaneous start.
use super::*;
use std::sync::mpsc::{self, Receiver, SyncSender};

type Hook = Arc<dyn Fn(NodeId) + Send + Sync>;

fn pause_once(target: NodeId) -> (Hook, Receiver<()>, SyncSender<()>) {
    let (reached_tx, reached_rx) = mpsc::sync_channel(0);
    let (resume_tx, resume_rx) = mpsc::sync_channel(0);
    let resume_rx = Mutex::new(resume_rx);
    let first = AtomicBool::new(true);
    let hook = Arc::new(move |id| {
        if id == target && first.swap(false, Ordering::SeqCst) {
            reached_tx.send(()).unwrap();
            resume_rx
                .lock()
                .recv_timeout(Duration::from_secs(15))
                .expect("test must release prepared operation");
        }
    });
    (hook, reached_rx, resume_tx)
}

fn overfull_root() -> (HierarchicalSpannWriter, NodeId, NodeId) {
    let mut writer = HierarchicalSpannWriter::new(
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig {
            split_threshold: 8,
            merge_threshold: 0,
            branching_factor: 2,
            fp_npa: true,
            ..Default::default()
        },
    );
    let points: Vec<_> = (0..96u32)
        .map(|id| {
            (
                id,
                Arc::from(
                    (0..8)
                        .map(|d| ((id * 31 + d * 17) as f32 * 0.13).sin())
                        .collect::<Vec<_>>(),
                ),
            )
        })
        .collect();
    writer.add_batch_buffered(&points, 4, || {}).unwrap();
    writer.split_leaf(0, MAX_BALANCE_DEPTH);
    let root = writer.root_id();
    let children = match writer.nodes.get(&root).unwrap().value() {
        TreeNode::Internal(n) => n.children().to_vec(),
        _ => panic!("split creates root"),
    };
    let leaf = children
        .into_iter()
        .max_by_key(|id| match writer.nodes.get(id).unwrap().value() {
            TreeNode::Leaf(l) => l.length,
            _ => 0,
        })
        .unwrap();
    let extra = writer.alloc_node_id();
    let centroid = vec![0.0; 8];
    writer.nodes.insert(
        extra,
        TreeNode::Leaf(LeafNode {
            navigation: Arc::new(NavigationNode::new(
                centroid.clone(),
                writer.quantize_origin(&centroid),
                Vec::new(),
                Some(root),
            )),
            ids: Vec::new(),
            codes: Vec::new(),
            length: 0,
        }),
    );
    writer.update_children(root, |ids| ids.push(extra)).unwrap();
    writer.refresh_navigation();
    writer.balance_scheduler.begin(Vec::new(), 1024);
    (writer, root, leaf)
}

fn assert_complete(writer: &HierarchicalSpannWriter) {
    assert_eq!(
        writer.root_reachable_valid_ids().unwrap(),
        (0..96).collect()
    );
    let mut visited = HashSet::new();
    let mut pending = vec![(writer.root_id(), None)];
    while let Some((id, parent)) = pending.pop() {
        assert!(visited.insert(id), "node must be reachable once");
        let node = writer.nodes.get(&id).expect("all children remain present");
        match node.value() {
            TreeNode::Internal(n) => {
                assert_eq!(n.parent_id, parent);
                pending.extend(n.children().iter().map(|&child| (child, Some(id))));
            }
            TreeNode::Leaf(l) => {
                assert_eq!(l.parent_id, parent);
                assert_eq!(l.length, l.ids.len());
                for (slot, vector) in l.ids.iter().enumerate() {
                    let embedding = writer.embeddings.get(vector).unwrap();
                    assert_eq!(
                        code_slice(&l.codes, slot, writer.code_size()),
                        Code::<1>::quantize(&embedding, &l.centroid).as_ref()
                    );
                }
            }
        }
    }
    assert_eq!(
        visited.len(),
        writer.nodes.len(),
        "no unattached replacement records"
    );
}

#[test]
fn prepared_internal_split_retries_after_child_split_publishes() {
    let (writer, root, leaf) = overfull_root();
    let (hook, reached, resume) = pause_once(root);
    *writer.internal_snapshot_hook.lock() = Some(hook);
    std::thread::scope(|scope| {
        let task = scope.spawn(|| writer.split_internal(root));
        reached.recv_timeout(Duration::from_secs(5)).unwrap();
        writer.split_leaf(leaf, MAX_BALANCE_DEPTH);
        assert!(
            !writer.nodes.contains_key(&leaf),
            "child publication completes before parent resumes"
        );
        resume.send(()).unwrap();
        task.join().unwrap();
    });
    *writer.internal_snapshot_hook.lock() = None;
    assert_eq!(
        writer.root_id(),
        root,
        "stale internal preparation must not publish"
    );
    assert_complete(&writer);
    let mut found_retry = false;
    while let Some(job) = writer.balance_scheduler.next() {
        found_retry |= job == Job::Internal(root);
        writer.balance_scheduler.finish(job);
    }
    assert!(found_retry, "invalidated parent must remain scheduled");
    writer.split_internal(root);
    assert_ne!(writer.root_id(), root);
    assert_complete(&writer);
}

#[test]
fn prepared_leaf_split_attaches_to_its_reparented_current_parent() {
    let (writer, root, leaf) = overfull_root();
    let (hook, reached, resume) = pause_once(leaf);
    *writer.leaf_prepared_hook.lock() = Some(hook);
    std::thread::scope(|scope| {
        let task = scope.spawn(|| writer.split_leaf(leaf, MAX_BALANCE_DEPTH));
        reached.recv_timeout(Duration::from_secs(5)).unwrap();
        writer.split_internal(root);
        assert!(
            !writer.nodes.contains_key(&root),
            "ancestor retires while leaf preparation waits"
        );
        let parent = match writer.nodes.get(&leaf).unwrap().value() {
            TreeNode::Leaf(n) => n.parent_id,
            _ => panic!("prepared source must remain a leaf"),
        };
        assert_ne!(
            parent,
            Some(root),
            "leaf has a new current parent before attachment"
        );
        resume.send(()).unwrap();
        task.join().unwrap();
    });
    *writer.leaf_prepared_hook.lock() = None;
    assert!(!writer.nodes.contains_key(&leaf));
    assert_complete(&writer);
}

#[test]
fn navigation_restarts_after_captured_root_retires() {
    let (writer, root, _) = overfull_root();
    let (hook, reached, resume) = pause_once(root);
    *writer.navigation_root_hook.lock() = Some(hook);
    let policy = ReadBeamPolicy::uniform(None, 1024, 1024);
    let candidates = std::thread::scope(|scope| {
        let task = scope.spawn(|| writer.navigate_f32(&[0.0; 8], &policy));
        reached.recv_timeout(Duration::from_secs(5)).unwrap();
        writer.split_internal(root);
        assert!(!writer.nodes.contains_key(&root));
        resume.send(()).unwrap();
        task.join().unwrap()
    });
    *writer.navigation_root_hook.lock() = None;
    assert!(!candidates.is_empty());
    assert!(writer.coordination_counters()["navigation_retries"] > 0);
    let expected = writer.navigate_f32(&[0.0; 8], &policy);
    assert_eq!(
        candidates, expected,
        "retry must recover every current selected branch"
    );
    assert_complete(&writer);
}
