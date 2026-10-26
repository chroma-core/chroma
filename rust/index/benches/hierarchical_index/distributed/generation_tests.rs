use super::*;
fn format() -> FormatContract {
    FormatContract {
        version: 1,
        dimensions: 2,
        metric: "l2".into(),
        centroid_bits: 4,
        posting_bits: 1,
        vector_bits: 32,
    }
}
fn fixture(s: &GenerationStore, g: &str) -> GenerationManifest {
    let vectors = s
        .put_object(
            &format!("{g}-vectors"),
            &[0, 0, 128, 63, 0, 0, 0, 64, 0, 0, 64, 64, 0, 0, 128, 64],
        )
        .unwrap();
    let routing = s
        .put_object(
            &format!("{g}-routing"),
            br#"{"root":1,"children":[2,3],"owners":[[2,10],[3,20]]}"#,
        )
        .unwrap();
    let mut owners = vec![];
    for (owner, root, id) in [(10, 2, 1), (20, 3, 2)] {
        let nodes = s
            .put_object(
                &format!("{g}-nodes-{owner}"),
                &serde_json::to_vec(&(root, vec![1.0f32, 2.0], vec![0x91u8])).unwrap(),
            )
            .unwrap();
        let postings = s
            .put_object(
                &format!("{g}-postings-{owner}"),
                &serde_json::to_vec(&(root, vec![(id, vec![0x80u8])])).unwrap(),
            )
            .unwrap();
        owners.push(
            s.save_owner(&OwnerManifest {
                run: "run".into(),
                save_attempt: g.into(),
                generation: g.into(),
                owner,
                roots: vec![root],
                vector_count: 1,
                format: format(),
                nodes: vec![nodes],
                postings: vec![postings],
                full_vectors: vec![vectors.clone()],
                navigation: vec![],
                validation: vec![],
            })
            .unwrap(),
        );
    }
    GenerationManifest {
        run: "run".into(),
        save_attempt: g.into(),
        generation: g.into(),
        format: format(),
        expected_roots: vec![
            RootCoverage { root: 2, owner: 10 },
            RootCoverage { root: 3, owner: 20 },
        ],
        owners,
        shared_routing: routing,
        full_vectors: vec![vectors],
        total_vectors: 2,
    }
}
#[test]
fn immutable_save_coverage_integrity_and_previous_generation() {
    let dir = tempfile::tempdir().unwrap();
    let s = GenerationStore::open(dir.path()).unwrap();
    let one = fixture(&s, "one");
    let d = s.save_generation(&one).unwrap();
    assert_eq!(s.save_generation(&one).unwrap(), d);
    s.publish_latest(&d, None).unwrap();
    s.publish_latest(&d, None).unwrap();
    let mut changed = one.clone();
    changed.total_vectors = 99;
    assert!(s.save_generation(&changed).is_err());
    let mut reused = one.clone();
    reused.generation = "other".into();
    assert!(s.save_generation(&reused).is_err());
    for mut bad in [
        fixture(&s, "missing"),
        fixture(&s, "duplicate"),
        fixture(&s, "precision"),
    ] {
        match bad.generation.as_str() {
            "missing" => {
                bad.owners.pop();
            }
            "duplicate" => bad.expected_roots.push(bad.expected_roots[0].clone()),
            _ => bad.format.centroid_bits = 1,
        }
        assert!(s.save_generation(&bad).is_err());
        assert_eq!(s.latest().unwrap(), Some(d.clone()));
    }
    let two = fixture(&s, "two");
    let d2 = s.save_generation(&two).unwrap();
    assert!(matches!(
        s.publish_latest(&d2, None),
        Err(GenerationError::Conflict)
    ));
    assert_eq!(s.latest().unwrap(), Some(d.clone()));
    s.publish_latest(&d2, Some("one")).unwrap();
    fs::write(dir.path().join(&two.full_vectors[0].path), b"corrupt").unwrap();
    assert!(s.reopen(&d2).is_err());
    assert!(s.reopen(&d).is_ok());
    assert!(s.put_object("../escape", b"x").is_err());
}
#[test]
fn simultaneous_expected_generation_publish_has_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let s = GenerationStore::open(dir.path()).unwrap();
    let a = s.save_generation(&fixture(&s, "a")).unwrap();
    let b = s.save_generation(&fixture(&s, "b")).unwrap();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let jobs: Vec<_> = [a, b]
        .into_iter()
        .map(|d| {
            let s = s.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                s.publish_latest(&d, None)
            })
        })
        .collect();
    let results: Vec<_> = jobs.into_iter().map(|j| j.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(GenerationError::Conflict)))
            .count(),
        1
    );
}
/// Spawn the test executable itself after dropping all writer-side objects. This
/// test needs only this module, serde, uuid and tempfile; no writer cache survives.
#[test]
fn fresh_process_reopens_durable_payloads() {
    const ENV: &str = "SPANN_GENERATION_REOPEN_CHILD";
    if let Some(path) = std::env::var_os(ENV) {
        let s = GenerationStore::open(path).unwrap();
        let d = s.latest().unwrap().unwrap();
        let m = s.reopen(&d).unwrap();
        assert_eq!(m.total_vectors, 2);
        assert_eq!(m.owners.len(), 2);
        let mut ids = vec![];
        for r in &m.owners {
            let owner: OwnerManifest = s.read_json(r).unwrap();
            for p in &owner.postings {
                let (_, postings): (u128, Vec<(u64, Vec<u8>)>) =
                    serde_json::from_slice(&s.read_object(p).unwrap()).unwrap();
                ids.extend(postings.into_iter().map(|p| p.0));
            }
        }
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(s.read_object(&m.full_vectors[0]).unwrap().len(), 16);
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    {
        let s = GenerationStore::open(dir.path()).unwrap();
        let d = s.save_generation(&fixture(&s, "fresh")).unwrap();
        s.publish_latest(&d, None).unwrap();
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("fresh_process_reopens_durable_payloads")
        .arg("--nocapture")
        .env(ENV, dir.path())
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
fn verified_owner_reuse_rejects_missing_corrupt_or_misnamed_objects_before_publication() {
    let dir = tempfile::tempdir().unwrap();
    let store = GenerationStore::open(dir.path()).unwrap();
    let previous = fixture(&store, "previous");
    let descriptor = store.save_generation(&previous).unwrap();
    store.publish_latest(&descriptor, None).unwrap();
    let owner = &previous.owners[0];
    let manifest = store.verify_saved_owner(owner).unwrap();
    let alias = store
        .put_object("owner-alias", &store.read_object(owner).unwrap())
        .unwrap();
    assert!(store.verify_saved_owner(&alias).is_err());
    assert_eq!(manifest, store.verify_saved_owner(owner).unwrap());
    for kind in ["missing", "corrupt"] {
        let next = fixture(&store, kind);
        let next_owner = store.verify_saved_owner(&next.owners[0]).unwrap();
        let posting = &next_owner.postings[0];
        if kind == "missing" {
            fs::remove_file(store.root().join(&posting.path)).unwrap();
        } else {
            let mut bytes = store.read_object(posting).unwrap();
            bytes[0] ^= 1;
            fs::write(store.root().join(&posting.path), bytes).unwrap();
        }
        assert!(store.verify_saved_owner(&next.owners[0]).is_err());
        assert!(store.save_generation(&next).is_err());
        assert!(!store
            .root()
            .join(format!("generations/{kind}.json"))
            .exists());
        assert_eq!(store.latest().unwrap(), Some(descriptor.clone()));
        assert_eq!(store.reopen(&descriptor).unwrap(), previous);
    }
}
