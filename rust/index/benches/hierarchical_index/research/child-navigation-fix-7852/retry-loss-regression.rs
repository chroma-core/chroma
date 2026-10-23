    #[test]
    fn review_reassignment_during_root_split_loses_removed_source() {
        let writer = HierarchicalSpannWriter::new(8, DistanceFunction::Euclidean, HierarchicalSpannConfig { merge_threshold: 0, ..Default::default() });
        writer.add(7, &[0.0; 8]);
        writer.nodes.insert(1, empty_leaf());
        writer.nodes.insert(2, empty_leaf());
        writer.next_node_id.store(3, Ordering::Relaxed);
        writer.create_root_above(&[0, 1, 2]);
        let root = writer.root_id();
        writer.navigation_ready.store(false, Ordering::Release);
        // Merge has removed/unlinked its source and now needs to reassign a row.
        writer.nodes.remove(&0);
        writer.remove_child_locked(root, 0);
        // Concurrent split_internal removes the root before publishing replacement.
        let (_, removed_root) = writer.nodes.remove(&root).unwrap();
        writer.reassign(0, 7, 0);
        writer.nodes.insert(root, removed_root);
        let valid = writer.root_reachable_valid_ids().unwrap();
        eprintln!("valid={valid:?} old_version={:?} missing_attempts={}", writer.current_version_sync(7), writer.stats.add_missing_nodes.load(Ordering::Relaxed));
        assert_eq!(valid, HashSet::from([7]));
    }

