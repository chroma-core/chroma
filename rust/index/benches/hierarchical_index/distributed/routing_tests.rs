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
        navigation: Default::default(),
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
                versions: vec![],
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

#[test]
fn packed_rows_follow_reordering_replacement_and_centroid_refresh() {
    let mut parent = image(1, 0., None, &[(3, 2., true), (5, 4., true)]);
    parent.prepare_navigation().unwrap();
    let old = parent.clone();
    let old_buffer = Arc::clone(&parent.navigation);
    let rows: Vec<_> = parent.scored_children().unwrap().collect();
    assert_eq!(rows[0].3.len(), 8);
    // Adjacent rows share one allocation rather than separate centroid Vecs.
    assert_eq!(
        rows[1].3.as_ptr() as usize - rows[0].3.as_ptr() as usize,
        8 * std::mem::size_of::<f32>()
    );
    parent.children.swap(0, 1);
    parent.children[0].target = target(7);
    parent.children[0].leaf = false;
    parent.children[0].centroid.fill(9.);
    parent.revision += 1;
    parent.prepare_navigation().unwrap();
    let updated: Vec<_> = parent.scored_children().unwrap().collect();
    assert_eq!(
        (updated[0].0, updated[0].1, updated[0].2),
        (0, target(7), false)
    );
    assert_eq!(updated[0].3, &[9.; 8]);
    assert_eq!(updated[1].1, target(3));
    assert!(!Arc::ptr_eq(&old_buffer, &parent.navigation));
    let retained: Vec<_> = old.scored_children().unwrap().collect();
    assert_eq!(retained[0].1, target(3));
    assert_eq!(retained[0].3, &[2.; 8]);
}

#[test]
fn packed_navigation_roundtrips_existing_wire_fields() {
    #[derive(Serialize)]
    struct Legacy<'a> {
        target: NodeRef,
        parent: Option<NodeRef>,
        centroid: &'a Vec<f32>,
        centroid_code: &'a Vec<u8>,
        children: &'a Vec<Child>,
        leaf: bool,
        revision: u64,
    }
    let mut parent = image(1, 1., None, &[(3, 2., true), (5, 2., false)]);
    parent.prepare_navigation().unwrap();
    let wire = bincode::serialize(&Legacy {
        target: parent.target,
        parent: parent.parent,
        centroid: &parent.centroid,
        centroid_code: &parent.centroid_code,
        children: &parent.children,
        leaf: parent.leaf,
        revision: parent.revision,
    })
    .unwrap();
    assert_eq!(bincode::serialize(&parent).unwrap(), wire);
    let reopened: RoutingNode = bincode::deserialize(&wire).unwrap();
    assert_eq!(parent, reopened);
    assert_eq!(
        reopened
            .scored_children()
            .unwrap()
            .map(|(_, target, leaf, row)| (target, leaf, row.to_vec()))
            .collect::<Vec<_>>(),
        parent
            .scored_children()
            .unwrap()
            .map(|(_, target, leaf, row)| (target, leaf, row.to_vec()))
            .collect::<Vec<_>>()
    );
    let json = serde_json::to_value(&parent).unwrap();
    assert!(json.get("navigation").is_none());
    let decoded: RoutingNode = serde_json::from_value(json).unwrap();
    assert_eq!(decoded.scored_children().unwrap().count(), 2);
}

#[test]
fn packed_navigation_rejects_wrong_child_dimension_before_publication() {
    let mut parent = image(1, 0., None, &[(3, 2., true)]);
    parent.children[0].centroid.pop();
    assert!(parent.prepare_navigation().is_err());
    assert!(serde_json::from_value::<RoutingNode>(serde_json::to_value(parent).unwrap()).is_err());
}

/// Force the retained envelope path for differential testing of local calls.
struct EnvelopeOnly<'a>(&'a super::super::engine::Cluster);
impl ExpansionBackend for EnvelopeOnly<'_> {
    fn observer(&self) -> Option<&WorkProfile> {
        Some(&self.0.local.observability)
    }
    fn expand(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        <super::super::engine::Cluster as ExpansionBackend>::expand(self.0, authority, requests)
    }
}
fn local_navigation_cluster(
    mut nodes: BTreeMap<NodeId, RoutingNode>,
) -> Arc<super::super::engine::Cluster> {
    use super::super::engine::{Cluster, OwnerService, PostingList, StoredNode};
    for node in nodes.values_mut() {
        node.target.authority = 1;
        if let Some(parent) = &mut node.parent {
            parent.authority = 1;
        }
        for child in &mut node.children {
            child.target.authority = 1;
        }
    }
    let root = nodes[&1].clone();
    let owner = Arc::new(OwnerService::new(
        1,
        17,
        8,
        DistanceFunction::Euclidean,
        HierarchicalSpannConfig::default(),
        Arc::new(|_: u64| -> Result<Arc<[f32]>, String> { Ok(vec![0.; 8].into()) }),
    ));
    owner
        .install_initial(
            nodes
                .into_values()
                .map(|routing| StoredNode {
                    postings: routing.leaf.then_some(PostingList {
                        ids: vec![],
                        codes: vec![],
                    }),
                    routing,
                })
                .collect(),
        )
        .unwrap();
    let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
    owner.bind_cluster(&cluster);
    cluster
}

#[test]
fn direct_local_navigation_matches_envelopes_and_reference_without_envelope_copies() {
    let queries = vec![vec![0.; 8], vec![1.; 8], vec![-1.; 8]];
    for nodes in [mixed_tree(), {
        let mut tree = BTreeMap::new();
        let children: Vec<_> = (2..130).map(|id| (id, 1., true)).collect();
        tree.insert(1, image(1, 0., None, &children));
        for (id, center, _) in children {
            tree.insert(id, image(id, center, Some(1), &[]));
        }
        tree
    }] {
        let reference = reference_writer(&nodes, 1);
        let cluster = local_navigation_cluster(nodes);
        let root = cluster
            .local
            .node(cluster.current_view().value.root.target)
            .unwrap();
        for tau in [None, Some(0.5), Some(1.), Some(1.7), Some(f64::NAN)] {
            for width in [0, 1, 3, 16] {
                let policy = ReadBeamPolicy::uniform(tau, width, 16);
                for excluded in [None, Some(3)] {
                    let args = (
                        cluster.local.run,
                        cluster.local.epoch.load(Ordering::Acquire),
                        cluster.current_view().sequence,
                    );
                    let (expected, envelope_work) = navigate_batch(
                        &EnvelopeOnly(&cluster),
                        &root,
                        &queries,
                        &policy,
                        args.0,
                        args.1,
                        args.2,
                        100,
                        excluded,
                    )
                    .unwrap();
                    let before = cluster.local.observability.snapshot();
                    let (actual, direct_work) = navigate_batch(
                        cluster.as_ref(),
                        &root,
                        &queries,
                        &policy,
                        args.0,
                        args.1,
                        args.2,
                        200,
                        excluded,
                    )
                    .unwrap();
                    let after = cluster.local.observability.snapshot();
                    assert_eq!(
                        actual, expected,
                        "tau={tau:?}, width={width}, excluded={excluded:?}"
                    );
                    assert_eq!(
                        (
                            direct_work.expansions,
                            direct_work.distances,
                            direct_work.levels
                        ),
                        (
                            envelope_work.expansions,
                            envelope_work.distances,
                            envelope_work.levels
                        )
                    );
                    if excluded.is_none() {
                        for (query, rows) in queries.iter().zip(&actual) {
                            assert_eq!(
                                rows.iter()
                                    .map(|(node, score)| (node.id as u32, *score))
                                    .collect::<Vec<_>>(),
                                reference.navigate_for_distributed_parity(query, &policy)
                            );
                        }
                    }
                    for counter in [
                        "NavigationQueryBytesCopied",
                        "NavigationReplyBytesCloned",
                        "NavigationRepliesValidated",
                        "NavigationChildRowsValidated",
                        "LocalEngineCalls",
                        "RemoteEngineCalls",
                    ] {
                        assert_eq!(
                            after.counters[counter] - before.counters[counter],
                            0,
                            "local path still incurs {counter}"
                        );
                    }
                }
            }
        }
    }
}

struct PartiallyLocal {
    backend: FixedBackend,
    local_expansions: std::sync::atomic::AtomicUsize,
}
impl ExpansionBackend for PartiallyLocal {
    fn expand_local_nodes(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        _: u128,
        _: u64,
        _: u64,
        children: &mut Vec<ScoredChild>,
        remote: &mut Vec<NodeRef>,
    ) -> Result<(), String> {
        for node in nodes {
            if node.authority != 1 {
                remote.push(*node);
                continue;
            }
            let image = &self.backend.nodes[&node.id];
            for (ordinal, target, leaf, centroid) in image.scored_children()? {
                children.push(ScoredChild {
                    parent: node.id,
                    ordinal,
                    target,
                    leaf,
                    distance: DistanceFunction::Euclidean.distance(query, centroid),
                    observed_revision: image.revision,
                });
            }
            self.local_expansions.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
    fn expand(
        &self,
        owner: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        self.backend.expand(owner, requests)
    }
}

#[test]
fn direct_and_remote_rows_share_one_global_beam_and_keep_remote_validation() {
    let queries = vec![vec![0.; 8], vec![1.; 8], vec![-1.; 8]];
    let policy = ReadBeamPolicy::uniform(Some(1.7), 3, 16);
    let mut nodes = mixed_tree();
    for node in nodes.values_mut() {
        node.prepare_navigation().unwrap();
    }
    let expected = run(
        &FixedBackend {
            nodes: nodes.clone(),
            fault: Fault::None,
        },
        &queries,
        &policy,
        None,
    )
    .unwrap()
    .0;
    for fault in [
        Fault::None,
        Fault::Reverse,
        Fault::Duplicate,
        Fault::Missing,
        Fault::WrongStep,
        Fault::PartialChildren,
    ] {
        let backend = PartiallyLocal {
            backend: FixedBackend {
                nodes: nodes.clone(),
                fault,
            },
            local_expansions: std::sync::atomic::AtomicUsize::new(0),
        };
        let result = navigate_batch(&backend, &nodes[&1], &queries, &policy, 17, 5, 9, 100, None);
        if matches!(fault, Fault::None | Fault::Reverse | Fault::Duplicate) {
            assert_eq!(result.unwrap().0, expected, "fault={fault:?}");
            assert!(backend.local_expansions.load(Ordering::Relaxed) > 0);
        } else {
            assert!(result.is_err(), "invalid remote reply accepted: {fault:?}");
        }
    }
}

#[test]
fn direct_local_stale_image_restarts_whole_attempt_and_recovers_after_publication() {
    let cluster = local_navigation_cluster(mixed_tree());
    let policy = ReadBeamPolicy::uniform(None, 16, 16);
    let queries = vec![vec![0.; 8]];
    let expected = cluster.navigate(&queries, &policy, None).unwrap();
    let retired = cluster.local.routing_images.write().remove(&2).unwrap();
    // The current parent still references this retired internal. A complete
    // traversal must fail/restart rather than selecting the remaining branches.
    assert!(cluster.navigate(&queries, &policy, None).is_err());
    cluster
        .local
        .publish_images(vec![(*retired).clone()], &[])
        .unwrap();
    assert_eq!(cluster.navigate(&queries, &policy, None).unwrap(), expected);
}

// Retain the admitted ordering implementation only as a test oracle. Count
// wide-record comparisons and comparator parent lookups, rather than timing it.
fn legacy_candidate_assembly(
    frontier: &[NodeRef],
    rows: &mut Vec<ScoredChild>,
) -> Result<(usize, usize), String> {
    let order = frontier
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id, i))
        .collect::<BTreeMap<_, _>>();
    let mut lookups = 0;
    rows.sort_unstable_by_key(|child| {
        lookups += 1;
        (order[&child.parent], child.ordinal)
    });
    let mut ordinals = BTreeSet::new();
    if rows.iter().any(|c| !ordinals.insert((c.parent, c.ordinal))) {
        return Err("duplicate child ordinal in expansion".into());
    }
    Ok((lookups / 2, lookups))
}

// Faults are applied after real TCP replies arrive, while local rows still
// come from the actual owner's immutable images.
struct CanonicalReplyFault<'a> {
    cluster: &'a super::super::engine::Cluster,
    fault: Fault,
}
impl ExpansionBackend for CanonicalReplyFault<'_> {
    fn expand_local_nodes(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        run: u128,
        epoch: u64,
        sequence: u64,
        children: &mut Vec<ScoredChild>,
        remote: &mut Vec<NodeRef>,
    ) -> Result<(), String> {
        self.cluster
            .expand_local_nodes(nodes, query, run, epoch, sequence, children, remote)
    }
    fn expand(
        &self,
        authority: AuthorityId,
        requests: Vec<ExpandRequest>,
    ) -> Result<Vec<ExpandReply>, String> {
        let mut replies = self.cluster.expand(authority, requests)?;
        match self.fault {
            Fault::Reverse => {
                replies.reverse();
                for reply in &mut replies {
                    reply.children.reverse();
                }
            }
            Fault::Duplicate => replies.extend(replies.clone()),
            Fault::Missing => {
                replies.pop();
            }
            Fault::PartialChildren => {
                replies[0].children.pop();
            }
            Fault::DuplicateOrdinal => {
                let child = replies[0].children[0].clone();
                replies[0].children.push(child);
            }
            Fault::OverlappingChunks => {
                let mut overlap = replies[0].clone();
                // Multiple remote parents give a different reply key but an
                // overlapping set of parents; exact replay must not hide it.
                assert!(overlap.nodes.len() > 1);
                overlap.nodes.reverse();
                replies.push(overlap);
            }
            Fault::ConflictingDuplicate => {
                let mut changed = replies[0].clone();
                changed.children[0].distance += 1.;
                replies.push(changed);
            }
            Fault::Stale => replies[0].status = ExpansionStatus::Stale,
            Fault::Publishing => replies[0].status = ExpansionStatus::Publishing,
            Fault::WrongAttempt => replies[0].navigation += 1,
            Fault::WrongSlot => replies[0].query_slot = usize::MAX,
            Fault::WrongPlacement => replies[0].nodes[0].placement += 1,
            Fault::WrongOwner => replies[0].authority += 1,
            Fault::WrongRun => replies[0].run += 1,
            Fault::WrongEpoch => replies[0].epoch += 1,
            Fault::WrongSharedSequence => replies[0].shared_sequence += 1,
            Fault::WrongStep => replies[0].step += 1,
            Fault::NonFinite => replies[0].children[0].distance = f32::NAN,
            _ => unreachable!(),
        }
        Ok(replies)
    }
}

#[test]
fn canonical_assembly_64_queries_mixed_tcp_owners_matches_old_and_reference() {
    use super::super::engine::{Cluster, EngineRequest, OwnerService, PostingList, StoredNode};
    use super::super::transport::{RpcClient, RpcServer, TransportConfig};
    let mut nodes = BTreeMap::new();
    let mut upper = vec![];
    for parent in 2..6 {
        let coordinate = if parent < 4 { 1. } else { 2. };
        upper.push((parent, coordinate, false));
        let children = (0..32)
            .map(|ordinal| {
                let id = 100 + parent * 32 + ordinal;
                let center = match ordinal % 4 {
                    0 => 0.,
                    1 => -0.,
                    2 => 1.,
                    _ => -1.,
                };
                (id, center, true)
            })
            .collect::<Vec<_>>();
        nodes.insert(parent, image(parent, coordinate, Some(1), &children));
        for (id, center, _) in children {
            nodes.insert(id, image(id, center, Some(parent), &[]));
        }
    }
    upper.push((6, 1., true));
    nodes.insert(6, image(6, 1., Some(1), &[]));
    nodes.insert(1, image(1, 0., None, &upper));
    let reference = reference_writer(&nodes, 1);
    let owners = [1, 2].map(|owner| {
        let service = Arc::new(OwnerService::new(
            owner,
            17,
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
            Arc::new(|_: u64| -> Result<Arc<[f32]>, String> { Ok(vec![0.; 8].into()) }),
        ));
        service
            .install_initial(
                nodes
                    .values()
                    .filter(|node| node.target.authority == owner)
                    .cloned()
                    .map(|routing| StoredNode {
                        postings: routing.leaf.then_some(PostingList {
                            ids: vec![],
                            codes: vec![],
                        }),
                        routing,
                    })
                    .collect(),
            )
            .unwrap();
        service
    });
    let endpoint = owners[1].clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "canonical-assembly".into(),
        TransportConfig::default(),
        Arc::new(move |request: EngineRequest| endpoint.handle(request)),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "canonical-assembly".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let cluster = Arc::new(Cluster::new(
        owners[0].clone(),
        BTreeMap::from([(2, client)]),
        nodes[&1].clone(),
    ));
    owners[0].bind_cluster(&cluster);
    let root = cluster.local.node(nodes[&1].target).unwrap();
    let queries = (0..64)
        .map(|i| vec![((i % 9) as f32 - 4.) / 2.; 8])
        .collect::<Vec<_>>();
    let epoch = cluster.local.epoch.load(Ordering::Acquire);
    let sequence = cluster.current_view().sequence;
    let mut total_old_comparisons = 0;
    let mut total_old_lookups = 0;
    let mut total_new_lookups = 0;
    let mut total_placed = 0;
    // These are fixed semantic cases, not a performance sweep. They exercise
    // ties, mixed-depth retention, tau/floor selection and the existing NaN tau policy.
    for (tau, width) in [(None, 16), (Some(1.7), 4), (Some(f64::NAN), 4)] {
        let policy = ReadBeamPolicy::uniform(tau, width, 16);
        let (old, old_work) = navigate_batch_with_assembly(
            cluster.as_ref(),
            &root,
            &queries,
            &policy,
            17,
            epoch,
            sequence,
            100,
            None,
            |frontier, rows| {
                let (comparisons, lookups) = legacy_candidate_assembly(frontier, rows)?;
                total_old_comparisons += comparisons;
                total_old_lookups += lookups;
                Ok(())
            },
        )
        .unwrap();
        let (actual, work) = navigate_batch_with_assembly(
            cluster.as_ref(),
            &root,
            &queries,
            &policy,
            17,
            epoch,
            sequence,
            200,
            None,
            |frontier, rows| {
                let mut expected = rows.clone();
                legacy_candidate_assembly(frontier, &mut expected)?;
                total_placed += rows.len();
                total_new_lookups += canonicalize_candidates(frontier, rows)?;
                assert_eq!(
                    *rows, expected,
                    "canonical input order differs before score sort"
                );
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(actual, old);
        let (public, public_work) = navigate_batch(
            cluster.as_ref(),
            &root,
            &queries,
            &policy,
            17,
            epoch,
            sequence,
            300,
            None,
        )
        .unwrap();
        assert_eq!(public, actual);
        assert_eq!(public_work.distances, work.distances);
        assert_eq!(
            (
                work.expansions,
                work.distances,
                work.levels,
                work.owner_requests
            ),
            (
                old_work.expansions,
                old_work.distances,
                old_work.levels,
                old_work.owner_requests
            )
        );
        for (query, rows) in queries.iter().zip(actual) {
            assert_eq!(
                rows.iter()
                    .map(|(node, score)| (node.id as u32, *score))
                    .collect::<Vec<_>>(),
                reference.navigate_for_distributed_parity(query, &policy)
            );
        }
        assert!(work.owner_requests > 0);
    }
    let policy = ReadBeamPolicy::uniform(None, 16, 16);
    for fault in [
        Fault::Reverse,
        Fault::Duplicate,
        Fault::Missing,
        Fault::PartialChildren,
        Fault::DuplicateOrdinal,
        Fault::OverlappingChunks,
        Fault::WrongStep,
        Fault::NonFinite,
    ] {
        let backend = CanonicalReplyFault {
            cluster: cluster.as_ref(),
            fault,
        };
        let old = navigate_batch_with_assembly(
            &backend,
            &root,
            &queries,
            &policy,
            17,
            epoch,
            sequence,
            400,
            None,
            |frontier, rows| legacy_candidate_assembly(frontier, rows).map(|_| ()),
        );
        let actual = navigate_batch(
            &backend, &root, &queries, &policy, 17, epoch, sequence, 500, None,
        );
        if matches!(fault, Fault::Reverse | Fault::Duplicate) {
            let (old, old_work) = old.unwrap();
            let (actual, work) = actual.unwrap();
            assert_eq!(actual, old, "fault={fault:?}");
            assert_eq!(
                (
                    work.expansions,
                    work.distances,
                    work.levels,
                    work.owner_requests
                ),
                (
                    old_work.expansions,
                    old_work.distances,
                    old_work.levels,
                    old_work.owner_requests
                )
            );
        } else {
            assert!(
                old.is_err() && actual.is_err(),
                "malformed reply accepted: {fault:?}"
            );
        }
    }
    assert!(owners[0].observability.snapshot().counters["OwnedRoutingDistances"] > 0);
    assert!(owners[1].observability.snapshot().counters["OwnedRoutingDistances"] > 0);
    assert!(total_new_lookups <= total_placed);
    assert!(total_old_lookups > total_new_lookups);
    assert!(total_old_comparisons > 0);
    eprintln!("canonical assembly: rows={total_placed}, old_wide_comparisons={total_old_comparisons}, old_parent_lookups={total_old_lookups}, new_parent_lookups={total_new_lookups}, new_wide_comparisons=0, new_ordinal_set_insertions=0");
    server.shutdown();
}

#[test]
fn canonical_assembly_rejects_repeated_unknown_and_incomplete_ordinals() {
    let frontier = vec![target(2), target(3)];
    let row = |parent, ordinal| ScoredChild {
        parent,
        ordinal,
        target: target(100 + ordinal as u128),
        leaf: true,
        distance: 0.,
        observed_revision: 1,
    };
    for mut malformed in [
        vec![row(2, 0), row(2, 0)],
        vec![row(2, 0), row(2, 2)],
        vec![row(2, usize::MAX)],
        vec![row(99, 0)],
    ] {
        assert!(canonicalize_candidates(&frontier, &mut malformed).is_err());
    }
    assert!(canonicalize_candidates(&[target(2), target(2)], &mut vec![row(2, 0)]).is_err());
    let mut empty = vec![];
    assert_eq!(canonicalize_candidates(&frontier, &mut empty).unwrap(), 0);
    let mut reversed = vec![row(3, 1), row(2, 0), row(3, 0)];
    let mut expected = reversed.clone();
    legacy_candidate_assembly(&frontier, &mut expected).unwrap();
    assert_eq!(
        canonicalize_candidates(&frontier, &mut reversed).unwrap(),
        3
    );
    assert_eq!(reversed, expected);
}

#[test]
fn reply_validation_64_queries_16_parents_visits_each_row_once_without_body_clone() {
    use super::super::engine::{Cluster, EngineRequest, OwnerService, PostingList, StoredNode};
    use super::super::transport::{RpcClient, RpcServer, TransportConfig};
    const BASE: u128 = 1 << 80;
    let mut nodes = BTreeMap::new();
    let mut root = image(BASE + 1, 0., None, &[]);
    root.target.authority = 1;
    root.leaf = false;
    for parent in 2..18u128 {
        let mut row = image(BASE + parent, 0., Some(BASE + 1), &[]);
        row.target.authority = 2;
        row.parent = Some(root.target);
        row.leaf = false;
        for ordinal in 0..100u128 {
            let id = BASE + 100 + parent * 100 + ordinal;
            let coordinate = if ordinal < 2 { 0. } else { 1. };
            let mut child = image(id, coordinate, Some(BASE + parent), &[]);
            child.target.authority = 2;
            child.parent = Some(row.target);
            if ordinal == 0 {
                child.leaf = false;
                for sub in 0..2u128 {
                    let mut leaf = image(BASE + 10000 + parent * 2 + sub, 0., Some(id), &[]);
                    leaf.target.authority = 2;
                    leaf.parent = Some(child.target);
                    child.children.push(Child {
                        target: leaf.target,
                        centroid: leaf.centroid.clone(),
                        leaf: true,
                    });
                    nodes.insert(leaf.target.id, leaf);
                }
            }
            row.children.push(Child {
                target: child.target,
                centroid: child.centroid.clone(),
                leaf: child.leaf,
            });
            nodes.insert(child.target.id, child);
        }
        root.children.push(Child {
            target: row.target,
            centroid: row.centroid.clone(),
            leaf: false,
        });
        nodes.insert(row.target.id, row);
    }
    nodes.insert(root.target.id, root.clone());
    let reference = reference_writer(&nodes, root.target.id);
    let owners = [1, 2].map(|owner| {
        let service = Arc::new(OwnerService::new(
            owner,
            26,
            8,
            DistanceFunction::Euclidean,
            HierarchicalSpannConfig::default(),
            Arc::new(|_: u64| -> Result<Arc<[f32]>, String> { Ok(vec![0.; 8].into()) }),
        ));
        service
            .install_initial(
                nodes
                    .values()
                    .filter(|node| node.target.authority == owner)
                    .cloned()
                    .map(|routing| StoredNode {
                        postings: routing.leaf.then_some(PostingList {
                            ids: vec![],
                            codes: vec![],
                        }),
                        routing,
                    })
                    .collect(),
            )
            .unwrap();
        service
    });
    let peer = owners[1].clone();
    let mut server = RpcServer::bind(
        "127.0.0.1:0".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
        "reply-validation".into(),
        TransportConfig::default(),
        Arc::new(move |request: EngineRequest| peer.handle(request)),
    )
    .unwrap();
    let client = RpcClient::new(
        server.data_addr(),
        server.control_addr(),
        "reply-validation".into(),
        TransportConfig::default(),
    )
    .unwrap();
    let cluster = Arc::new(Cluster::new(
        owners[0].clone(),
        BTreeMap::from([(2, client)]),
        root,
    ));
    owners[0].bind_cluster(&cluster);
    let root = cluster.local.node(nodes[&(BASE + 1)].target).unwrap();
    let queries = (0..64)
        .map(|i| vec![if i % 2 == 0 { 0. } else { -0. }; 8])
        .collect::<Vec<_>>();
    let policy = ReadBeamPolicy::uniform(None, 16, 16);
    let before = owners[0].observability.snapshot();
    let started = std::time::Instant::now();
    let (actual, work) = navigate_batch(
        cluster.as_ref(),
        &root,
        &queries,
        &policy,
        26,
        cluster.local.epoch.load(Ordering::Acquire),
        cluster.current_view().sequence,
        100,
        None,
    )
    .unwrap();
    let seconds = started.elapsed().as_secs_f64();
    for (query, actual) in queries.iter().zip(&actual) {
        assert_eq!(
            actual
                .iter()
                .map(|(node, score)| (node.id as u32, *score))
                .collect::<Vec<_>>(),
            reference.navigate_for_distributed_parity(query, &policy)
        );
    }
    let after = owners[0].observability.snapshot();
    let rows = after.counters["NavigationChildRowsValidated"]
        - before.counters["NavigationChildRowsValidated"];
    let clones = after.counters["NavigationReplyBytesCloned"]
        - before.counters["NavigationReplyBytesCloned"];
    assert!(rows >= 64 * 16 * 100);
    eprintln!("reply-validation fixed work: queries=64 parents=16 children_per_parent=100 rows={rows} row_visits={} reply_bytes_cloned={clones} native_elapsed_seconds={seconds:.6}", work.reply_validation_row_visits);
    assert_eq!(
        work.reply_validation_row_visits, rows,
        "remote validation revisits child rows per parent"
    );
    assert_eq!(
        clones, 0,
        "remote validation clones the complete reply body"
    );
    // Corrupt replies only after they originate from the actual remote owner.
    // One query retains the same sixteen-parent coverage condition.
    for fault in [
        Fault::Reverse,
        Fault::Duplicate,
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
        let backend = CanonicalReplyFault {
            cluster: cluster.as_ref(),
            fault,
        };
        let result = navigate_batch(
            &backend,
            &root,
            &queries[..1],
            &policy,
            26,
            cluster.local.epoch.load(Ordering::Acquire),
            cluster.current_view().sequence,
            300,
            None,
        );
        if matches!(fault, Fault::Reverse | Fault::Duplicate) {
            assert_eq!(result.unwrap().0, actual[..1], "fault={fault:?}");
        } else {
            assert!(
                result.is_err(),
                "malformed grouped remote reply accepted: {fault:?}"
            );
        }
    }
    server.shutdown();
}

#[test]
fn reply_validation_one_pass_rejects_bad_counts_ordinals_revisions_and_identity() {
    let parent = NodeRef {
        id: (1 << 80) + 2,
        authority: 2,
        placement: 4,
    };
    let expected = HashMap::from([(parent.id, parent)]);
    let valid = ExpandReply {
        authority: 2,
        run: 26,
        epoch: 0,
        shared_sequence: 2,
        navigation: 5,
        step: 2,
        query_slot: 0,
        nodes: vec![parent],
        status: ExpansionStatus::Complete,
        child_counts: vec![(parent.id, 3)],
        children: (0..3)
            .map(|ordinal| ScoredChild {
                parent: parent.id,
                ordinal,
                target: NodeRef {
                    id: (1 << 80) + 100 + ordinal as u128,
                    authority: 2,
                    placement: 4,
                },
                leaf: ordinal != 0,
                distance: 0.,
                observed_revision: 7,
            })
            .collect(),
    };
    let mut unordered = valid.clone();
    unordered.children.reverse();
    let mut visits = 0;
    validate_parent_coverage(&unordered, 2, &expected, &mut BTreeSet::new(), &mut visits).unwrap();
    assert_eq!(visits, 3);
    let mutations: &[(&str, fn(&mut ExpandReply))] = &[
        ("missing row", |r| {
            r.children.pop();
        }),
        ("declared overflow", |r| {
            r.child_counts.push((999, usize::MAX));
        }),
        ("oversized count", |r| {
            r.child_counts[0].1 = usize::MAX;
        }),
        ("repeated declared parent", |r| {
            r.child_counts.push(r.child_counts[0]);
        }),
        ("unknown declared parent", |r| {
            r.child_counts[0].0 += 1;
        }),
        ("duplicate ordinal", |r| {
            r.children[1].ordinal = 0;
        }),
        ("out of bounds ordinal", |r| {
            r.children[1].ordinal = usize::MAX;
        }),
        ("unknown child parent", |r| {
            r.children[1].parent += 1;
        }),
        ("mixed revision", |r| {
            r.children[1].observed_revision += 1;
        }),
        ("nonfinite score", |r| {
            r.children[1].distance = f32::INFINITY;
        }),
        ("wrong placement", |r| {
            r.nodes[0].placement += 1;
        }),
        ("wrong parent owner", |r| {
            r.nodes[0].authority += 1;
        }),
        ("repeated node", |r| {
            r.nodes.push(r.nodes[0]);
        }),
    ];
    for (name, mutate) in mutations {
        let mut reply = valid.clone();
        mutate(&mut reply);
        for cache_parent in [false, true] {
            assert!(
                validate_parent_coverage_with_cache(
                    &reply,
                    2,
                    &expected,
                    &mut BTreeSet::new(),
                    &mut 0,
                    cache_parent
                )
                .is_err(),
                "accepted {name} with cache={cache_parent}"
            );
        }
    }
    let mut received = BTreeSet::from([parent.id]);
    assert!(validate_parent_coverage(&valid, 2, &expected, &mut received, &mut 0).is_err());
    let mut empty = valid;
    empty.child_counts[0].1 = 0;
    empty.children.clear();
    validate_parent_coverage(&empty, 2, &expected, &mut BTreeSet::new(), &mut 0).unwrap();
}

#[test]
fn parent_lookup_cache_preserves_shuffled_rows_and_reduces_grouped_hashing() {
    let frontier = (1..=16).map(target).collect::<Vec<_>>();
    let grouped = frontier
        .iter()
        .flat_map(|parent| {
            (0..100).map(move |ordinal| ScoredChild {
                parent: parent.id,
                ordinal,
                target: target(1000 + parent.id * 100 + ordinal as u128),
                leaf: true,
                distance: (ordinal % 7) as f32,
                observed_revision: 3,
            })
        })
        .collect::<Vec<_>>();
    for shuffled in [false, true] {
        let mut rows = grouped.clone();
        if shuffled {
            rows.sort_by_key(|row| (row.ordinal, row.parent));
        }
        let mut control = rows.clone();
        let control_lookups =
            canonicalize_candidates_with_cache(&frontier, &mut control, false).unwrap();
        let candidate_lookups =
            canonicalize_candidates_with_cache(&frontier, &mut rows, true).unwrap();
        assert_eq!(rows, control);
        assert_eq!(control_lookups, 1600);
        assert_eq!(candidate_lookups, if shuffled { 1600 } else { 16 });
        reference_sort(&mut rows, |row| row.distance, None).unwrap();
        reference_sort(&mut control, |row| row.distance, None).unwrap();
        assert_eq!(rows, control);
    }
}

#[test]
fn parent_lookup_cache_fixed_work_timing() {
    let frontier = (1..=16).map(target).collect::<Vec<_>>();
    let rows = frontier
        .iter()
        .flat_map(|parent| {
            (0..100).map(move |ordinal| ScoredChild {
                parent: parent.id,
                ordinal,
                target: target(1000 + parent.id * 100 + ordinal as u128),
                leaf: true,
                distance: ordinal as f32,
                observed_revision: 3,
            })
        })
        .collect::<Vec<_>>();
    for cache_parent in [false, true, true, false] {
        let start = std::time::Instant::now();
        let mut lookups = 0;
        for _ in 0..256 {
            let mut trial = std::hint::black_box(rows.clone());
            lookups +=
                canonicalize_candidates_with_cache(&frontier, &mut trial, cache_parent).unwrap();
            std::hint::black_box(trial);
        }
        eprintln!(
            "parent_cache={cache_parent} rows={} lookups={lookups} elapsed_seconds={:.6}",
            rows.len() * 256,
            start.elapsed().as_secs_f64()
        );
    }
}
