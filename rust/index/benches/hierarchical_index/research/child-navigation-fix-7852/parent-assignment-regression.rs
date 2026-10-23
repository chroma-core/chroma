    #[test]
    fn review_merge_published_leaf_before_parent_assignment() {
        let writer = HierarchicalSpannWriter::new(8, DistanceFunction::Euclidean, HierarchicalSpannConfig { write_beam_min: 1, write_beam_max: 1, merge_threshold: 0, ..Default::default() });
        writer.add(7, &[0.0; 8]);
        let mut leaf = empty_leaf();
        if let TreeNode::Leaf(leaf) = &mut leaf { leaf.centroid[0] = 2.0; }
        writer.nodes.insert(1, leaf);
        writer.next_node_id.store(2, Ordering::Relaxed);
        writer.create_root_above(&[0, 1]);
        let root = writer.root_id();
        // A replacement leaf is reachable before replace_child assigns its parent.
        writer.nodes.get_mut(&0).unwrap().set_parent_id(None);
        writer.navigation_ready.store(false, Ordering::Release);
        writer.merge_leaf(0, 0);
        eprintln!("source exists={} validation={:?}", writer.nodes.contains_key(&0), writer.root_reachable_valid_ids());
        let result=writer.root_reachable_valid_ids();
        writer.remove_child_locked(root,0);
        eprintln!("after link repair={:?}",writer.root_reachable_valid_ids());
        assert!(result.is_ok());
    }

