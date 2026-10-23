//! Exact fixed-tree comparison with the current unsharded writer. These tests
//! exercise the real navigation driver; the in-memory backend scores one step
//! and deliberately changes transport reply order or injects malformed replies.
use super::super::super::{
    common as reference, config::HierarchicalSpannConfig, writer::HierarchicalSpannWriter,
};
use super::*;
use chroma_distance::DistanceFunction;
use std::sync::{atomic::Ordering, Arc};

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    Reverse,
    Duplicate,
    Missing,
    PartialChildren,
    ConflictingDuplicate,
    Stale,
    Publishing,
    WrongAttempt,
    WrongStep,
    WrongSlot,
    WrongPlacement,
    WrongOwner,
    WrongRun,
    WrongEpoch,
    WrongSharedSequence,
    DuplicateOrdinal,
    NonFinite,
    OverlappingChunks,
}
struct FixedBackend {
    nodes: BTreeMap<NodeId, RoutingNode>,
    fault: Fault,
}
fn target(id: u128) -> NodeRef {
    NodeRef {
        id,
        authority: if id % 2 == 0 { 2 } else { 1 },
        placement: 4,
    }
}
fn image(
    id: u128,
    coordinate: f32,
    parent: Option<u128>,
    children: &[(u128, f32, bool)],
) -> RoutingNode {
    RoutingNode {
        target: target(id),
        parent: parent.map(target),
        centroid: vec![coordinate; 8],
        centroid_code: vec![],
        children: children
            .iter()
            .map(|(id, c, leaf)| Child {
                target: target(*id),
                centroid: vec![*c; 8],
                leaf: *leaf,
            })
            .collect(),
        leaf: children.is_empty(),
        revision: 1,
    }
}
fn mixed_tree() -> BTreeMap<NodeId, RoutingNode> {
    let specs = [
        (
            1,
            0.,
            None,
            vec![(2, 1., false), (3, 1., true), (4, 2., false), (5, 4., true)],
        ),
        (2, 1., Some(1), vec![(6, 2., true), (7, 3., true)]),
        (3, 1., Some(1), vec![]),
        (4, 2., Some(1), vec![(8, 1., false), (9, 1., true)]),
        (5, 4., Some(1), vec![]),
        (6, 2., Some(2), vec![]),
        (7, 3., Some(2), vec![]),
        (8, 1., Some(4), vec![(10, 0.25, true), (11, 2.25, true)]),
        (9, 1., Some(4), vec![]),
        (10, 0.25, Some(8), vec![]),
        (11, 2.25, Some(8), vec![]),
    ];
    specs
        .into_iter()
        .map(|(id, c, p, children)| (id, image(id, c, p, &children)))
        .collect()
}
fn reference_writer(
    nodes: &BTreeMap<NodeId, RoutingNode>,
    root: NodeId,
) -> HierarchicalSpannWriter {
    let writer = HierarchicalSpannWriter::new(
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
    );
    writer.nodes.remove(&0);
    for (id, node) in nodes {
        let navigation = Arc::new(reference::NavigationNode::new(
            node.centroid.clone(),
            vec![],
            node.children.iter().map(|c| c.target.id as u32).collect(),
            node.parent.map(|p| p.id as u32),
        ));
        let record = if node.leaf {
            reference::TreeNode::Leaf(reference::LeafNode {
                navigation,
                codes: vec![],
                ids: vec![],
                length: 0,
            })
        } else {
            reference::TreeNode::Internal(reference::InternalNode { navigation })
        };
        writer.nodes.insert(*id as u32, record);
    }
    writer.root_id.store(root as u32, Ordering::Relaxed);
    writer
}
impl ExpansionBackend for FixedBackend {
    fn expand(
        &self,
        _authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        let mut replies = vec![];
        for request in requests {
            // One reply per selected internal node intentionally partitions a batched
            // request. Reconstruction must use global frontier order, not reply order.
            for node in &request.nodes {
                let image = self.nodes.get(&node.id).ok_or("missing fixture node")?;
                let children = image
                    .children
                    .iter()
                    .enumerate()
                    .map(|(ordinal, c)| ScoredChild {
                        parent: node.id,
                        ordinal,
                        target: c.target,
                        leaf: c.leaf,
                        distance: DistanceFunction::Euclidean.distance(&request.query, &c.centroid),
                        observed_revision: image.revision,
                    })
                    .collect();
                replies.push(ExpandReply {
                    authority: _authority,
                    run: request.run,
                    epoch: request.epoch,
                    shared_sequence: request.shared_sequence,
                    child_counts: vec![(node.id, image.children.len())],
                    navigation: request.navigation,
                    step: request.step,
                    query_slot: request.query_slot,
                    nodes: vec![*node],
                    status: ExpansionStatus::Complete,
                    children,
                });
            }
        }
        match self.fault {
            Fault::None => {}
            Fault::Reverse => replies.reverse(),
            Fault::Duplicate => {
                let copies = replies.clone();
                replies.extend(copies)
            }
            Fault::Missing => {
                replies.pop();
            }
            Fault::PartialChildren => {
                if let Some(r) = replies.first_mut() {
                    r.children.pop();
                }
            }
            Fault::ConflictingDuplicate => {
                if let Some(r) = replies.first() {
                    let mut copy = r.clone();
                    if let Some(c) = copy.children.first_mut() {
                        c.distance += 1.;
                    }
                    replies.push(copy);
                }
            }
            Fault::Stale => replies[0].status = ExpansionStatus::Stale,
            Fault::Publishing => replies[0].status = ExpansionStatus::Publishing,
            Fault::WrongAttempt => replies[0].navigation += 99,
            Fault::WrongStep => replies[0].step += 1,
            Fault::WrongSlot => replies[0].query_slot = usize::MAX,
            Fault::WrongPlacement => replies[0].nodes[0].placement += 1,
            Fault::WrongOwner => replies[0].authority += 1,
            Fault::WrongRun => replies[0].run += 1,
            Fault::WrongEpoch => replies[0].epoch += 1,
            Fault::WrongSharedSequence => replies[0].shared_sequence += 1,
            Fault::DuplicateOrdinal => {
                let copy = replies[0].children[0].clone();
                replies[0].children.push(copy);
            }
            Fault::NonFinite => replies[0].children[0].distance = f32::NAN,
            Fault::OverlappingChunks => {
                let mut second = replies[0].clone();
                second.children.clear();
                replies.push(second);
            }
        }
        Ok(replies)
    }
}
fn run(
    backend: &FixedBackend,
    queries: &[Vec<f32>],
    policy: &ReadBeamPolicy,
    excluded: Option<NodeId>,
) -> Result<(Vec<Vec<(NodeRef, f32)>>, NavigationWork), String> {
    navigate_batch(
        backend,
        &backend.nodes[&1],
        queries,
        policy,
        17,
        5,
        9,
        100,
        excluded,
    )
}
#[test]
fn mixed_depth_global_beam_matches_actual_reference_with_reordered_and_duplicate_replies() {
    let nodes = mixed_tree();
    let writer = reference_writer(&nodes, 1);
    let queries = vec![vec![0.; 8], vec![1.; 8], vec![-1.; 8]];
    for fault in [Fault::None, Fault::Reverse, Fault::Duplicate] {
        let backend = FixedBackend {
            nodes: nodes.clone(),
            fault,
        };
        for tau in [
            None,
            Some(-1.),
            Some(0.5),
            Some(1.),
            Some(1.7),
            Some(8.),
            Some(f64::NAN),
        ] {
            for width in [0, 1, 2, 3, 16] {
                let policy = ReadBeamPolicy::uniform(tau, width, 16);
                let (actual, work) = run(&backend, &queries, &policy, None).unwrap();
                for (query, actual) in queries.iter().zip(actual) {
                    let expected = writer.navigate_for_distributed_parity(query, &policy);
                    let actual: Vec<_> = actual
                        .into_iter()
                        .map(|(node, score)| (node.id as u32, score))
                        .collect();
                    assert_eq!(
                        actual, expected,
                        "fault={fault:?}, tau={tau:?},width={width}"
                    );
                }
                assert!(work.expansions >= queries.len() as u64);
            }
        }
    }
}
#[test]
fn large_equal_score_frontier_preserves_reference_tie_order() {
    for count in [20, 33, 64, 97] {
        let children = (100..100 + count)
            .map(|id| (id, 1., true))
            .collect::<Vec<_>>();
        let mut nodes = BTreeMap::new();
        nodes.insert(1, image(1, 0., None, &children));
        for (id, c, _) in children {
            nodes.insert(id, image(id, c, Some(1), &[]));
        }
        let writer = reference_writer(&nodes, 1);
        let backend = FixedBackend {
            nodes,
            fault: Fault::Reverse,
        };
        for width in [1, 7, 20, 32] {
            let policy = ReadBeamPolicy::uniform(None, width, width);
            let (actual, _) = run(&backend, &[vec![0.; 8]], &policy, None).unwrap();
            let actual: Vec<_> = actual[0].iter().map(|(n, d)| (n.id as u32, *d)).collect();
            assert_eq!(
                actual,
                writer.navigate_for_distributed_parity(&[0.; 8], &policy),
                "count={count},width={width}"
            );
        }
    }
}
#[test]
fn per_level_tau_and_percentage_floor_match_reference() {
    let nodes = mixed_tree();
    let writer = reference_writer(&nodes, 1);
    let backend = FixedBackend {
        nodes,
        fault: Fault::Reverse,
    };
    let policy = ReadBeamPolicy::with_level_overrides(
        Some(1.1),
        1,
        4,
        vec![Some(8.), Some(0.5), Some(2.)],
        vec![50., 100., 25.],
        vec![4, 4, 2],
    );
    let q = vec![0.3; 8];
    let (actual, _) = run(&backend, &[q.clone()], &policy, None).unwrap();
    assert_eq!(
        actual[0]
            .iter()
            .map(|(n, d)| (n.id as u32, *d))
            .collect::<Vec<_>>(),
        writer.navigate_for_distributed_parity(&q, &policy)
    );
}
#[test]
fn malformed_or_partial_expansion_never_selects_a_partial_beam() {
    for fault in [
        Fault::Missing,
        Fault::PartialChildren,
        Fault::ConflictingDuplicate,
        Fault::Stale,
        Fault::Publishing,
        Fault::WrongAttempt,
        Fault::WrongStep,
        Fault::WrongSlot,
        Fault::WrongPlacement,
        Fault::WrongOwner,
        Fault::WrongRun,
        Fault::WrongEpoch,
        Fault::WrongSharedSequence,
        Fault::DuplicateOrdinal,
        Fault::NonFinite,
        Fault::OverlappingChunks,
    ] {
        let b = FixedBackend {
            nodes: mixed_tree(),
            fault,
        };
        assert!(
            run(
                &b,
                &[vec![0.; 8]],
                &ReadBeamPolicy::uniform(None, 4, 4),
                None
            )
            .is_err(),
            "fault {fault:?} accepted"
        );
    }
}
#[test]
fn excluded_leaf_never_consumes_a_beam_slot() {
    let b = FixedBackend {
        nodes: mixed_tree(),
        fault: Fault::None,
    };
    let (result, _) = run(
        &b,
        &[vec![0.; 8]],
        &ReadBeamPolicy::uniform(None, 2, 2),
        Some(3),
    )
    .unwrap();
    assert!(!result[0].iter().any(|(n, _)| n.id == 3));
    assert!(result[0].iter().any(|(n, _)| n.id == 9));
}
#[test]
fn bounded_depth_and_attempt_exhaustion_fail_explicitly() {
    let mut nodes = BTreeMap::new();
    for id in 1..=130 {
        let child = if id == 130 {
            vec![]
        } else {
            vec![(id + 1, 0., id + 1 == 130)]
        };
        nodes.insert(
            id,
            image(id, 0., if id == 1 { None } else { Some(id - 1) }, &child),
        );
    }
    let b = FixedBackend {
        nodes,
        fault: Fault::None,
    };
    assert!(run(
        &b,
        &[vec![0.; 8]],
        &ReadBeamPolicy::uniform(None, 1, 1),
        None
    )
    .unwrap_err()
    .contains("depth"));
    assert!(navigate_batch(
        &b,
        &b.nodes[&1],
        &[vec![0.; 8], vec![1.; 8]],
        &ReadBeamPolicy::uniform(None, 1, 1),
        1,
        1,
        1,
        u128::MAX,
        None
    )
    .is_err());
}
#[test]
fn invalid_queries_fail_before_owner_dispatch() {
    let b = FixedBackend {
        nodes: mixed_tree(),
        fault: Fault::None,
    };
    for q in [vec![0.; 7], vec![f32::INFINITY; 8], vec![f32::NAN; 8]] {
        assert!(run(&b, &[q], &ReadBeamPolicy::uniform(None, 1, 1), None).is_err());
    }
}
