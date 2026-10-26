use super::super::{
    generation::*,
    routing::{Child, NodeRef, RoutingNode},
    saved_reader::*,
};
use super::*;
use chroma_index::quantization::Code;
use std::collections::BTreeSet;

fn fixture(
    store: &GenerationStore,
    generation: &str,
    attempt: &str,
    vector: &ObjectRef,
) -> std::result::Result<GenerationDescriptor, String> {
    let format = FormatContract {
        version: 1,
        dimensions: 2,
        metric: "l2".into(),
        centroid_bits: 4,
        posting_bits: 1,
        vector_bits: 32,
    };
    let root = 1u128 << 80;
    let mut children = vec![];
    let mut owners = vec![];
    for owner in [1, 2] {
        let center = vec![owner as f32; 2];
        let leaf = RoutingNode {
            navigation: Default::default(),
            target: NodeRef {
                id: root + owner as u128,
                authority: owner,
                placement: 0,
            },
            parent: Some(NodeRef {
                id: root,
                authority: 1,
                placement: 0,
            }),
            centroid: center.clone(),
            centroid_code: vec![],
            children: vec![],
            leaf: true,
            revision: 1,
        };
        children.push(Child {
            target: leaf.target,
            centroid: center.clone(),
            leaf: true,
        });
        owners.push(save_owner_snapshot(
            store,
            "run",
            attempt,
            generation,
            owner,
            vec![leaf.target.id],
            format.clone(),
            &[leaf.clone()],
            &[SavedPostingList {
                leaf: leaf.target.id,
                ids: vec![owner],
                codes: Code::<1>::quantize(&center, &center).as_ref().to_vec(),
                centroid: vec![],
            }],
            vec![vector.clone()],
            None,
        )?);
    }
    let shared = RoutingNode {
        navigation: Default::default(),
        target: NodeRef {
            id: root,
            authority: 1,
            placement: 0,
        },
        parent: None,
        centroid: vec![1.5; 2],
        centroid_code: vec![],
        children,
        leaf: false,
        revision: 1,
    };
    let routing = save_shared_routing(store, &format!("shared-{generation}"), &[shared])?;
    store
        .save_generation(&GenerationManifest {
            run: "run".into(),
            save_attempt: attempt.into(),
            generation: generation.into(),
            format,
            expected_roots: vec![
                RootCoverage {
                    root: root + 1,
                    owner: 1,
                },
                RootCoverage {
                    root: root + 2,
                    owner: 2,
                },
            ],
            owners,
            shared_routing: routing,
            full_vectors: vec![vector.clone()],
            total_vectors: 2,
        })
        .map_err(|e| e.to_string())
}
#[test]
fn durable_create_replay_collision_integrity_and_legacy_boundary() {
    let (config, objects) = mock::register();
    let a = GenerationStore::open_durable(&config).unwrap();
    let b = GenerationStore::open_durable(&config).unwrap();
    let r = a.put_object("immutable", b"first").unwrap();
    assert_eq!(r, b.put_object("immutable", b"first").unwrap());
    assert!(matches!(
        b.put_object("immutable", b"changed"),
        Err(GenerationError::Conflict)
    ));
    assert_eq!(b.read_object(&r).unwrap(), b"first");
    let mut legacy = r.clone();
    legacy.sha256 = None;
    assert!(b.read_object(&legacy).is_err());
    objects.objects.lock().get_mut(&r.path).unwrap().0[0] ^= 1;
    assert!(b.read_object(&r).is_err());
    let local = tempfile::tempdir().unwrap();
    let local = GenerationStore::open(local.path()).unwrap();
    let mut old = local.put_object("legacy", b"legacy").unwrap();
    old.sha256 = None;
    assert_eq!(local.read_object(&old).unwrap(), b"legacy");
    for prefix in ["objects/test", "sha256/", "sha256/../bad", "sha256/a/b/"] {
        assert!(DurableStoreConfig {
            bucket: config.bucket.clone(),
            prefix: prefix.into()
        }
        .validate()
        .is_err());
    }
}
#[test]
fn durable_publication_cas_conflicts_leave_previous_reader_and_lazy_objects() {
    let (config, objects) = mock::register();
    let writer = GenerationStore::open_durable(&config).unwrap();
    let vector = writer
        .put_object(
            "independent-vector-view",
            b"immutable vector source descriptor",
        )
        .unwrap();
    let first = fixture(&writer, "gen1", "attempt1", &vector).unwrap();
    writer.publish_latest(&first, None).unwrap();
    writer.publish_latest(&first, None).unwrap();
    let second = fixture(&writer, "gen2", "attempt2", &vector).unwrap();
    let third = fixture(&writer, "gen3", "attempt3", &vector).unwrap();
    *objects.cas_barrier.lock() = Some(Arc::new(std::sync::Barrier::new(2)));
    let a = writer.clone();
    let b = writer.clone();
    let s = second.clone();
    let t = third.clone();
    let one = std::thread::spawn(move || a.publish_latest(&s, Some("gen1")));
    let two = std::thread::spawn(move || b.publish_latest(&t, Some("gen1")));
    let outcomes = [one.join().unwrap(), two.join().unwrap()];
    *objects.cas_barrier.lock() = None;
    assert_eq!(outcomes.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|r| matches!(r, Err(GenerationError::Conflict)))
            .count(),
        1
    );
    drop(writer);
    let fresh = GenerationStore::open_durable(&config).unwrap();
    objects.reads.lock().clear();
    let reader = SavedReader::open(fresh, &first).unwrap();
    assert!(
        objects
            .reads
            .lock()
            .iter()
            .all(|key| !key.contains("postings")),
        "ordinary opening must leave posting payloads lazy: {:?}",
        *objects.reads.lock()
    );
    let before = objects.reads.lock().len();
    assert_eq!(
        reader.validate_membership().unwrap(),
        BTreeSet::from([1, 2])
    );
    assert!(objects.reads.lock().len() > before);
    let current = GenerationStore::open_durable(&config)
        .unwrap()
        .latest()
        .unwrap()
        .unwrap();
    assert!(current == second || current == third);
    assert_eq!(
        reader.validate_membership().unwrap(),
        BTreeSet::from([1, 2])
    );
}
#[test]
fn durable_failed_required_owner_manifest_cannot_publish_partial_generation() {
    let (config, objects) = mock::register();
    let store = GenerationStore::open_durable(&config).unwrap();
    let vector = store
        .put_object("independent-vector-view", b"descriptor")
        .unwrap();
    let first = fixture(&store, "gen1", "attempt1", &vector).unwrap();
    store.publish_latest(&first, None).unwrap();
    *objects.fail_prefix.lock() = Some("owners/3-run-broken-2.json".into());
    // The actual owner-manifest write fails after its immutable payloads. No
    // combined generation or latest write is attempted on that failed save.
    let error = fixture(&store, "gen2", "broken", &vector).unwrap_err();
    assert!(error.contains("injected durable write failure"));
    assert_eq!(store.latest().unwrap(), Some(first.clone()));
    assert!(!objects.objects.lock().contains_key("generations/gen2.json"));
    assert_eq!(
        SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &first)
            .unwrap()
            .validate_membership()
            .unwrap(),
        BTreeSet::from([1, 2])
    );
}

#[test]
fn durable_ambiguous_immutable_put_requires_exact_existing_bytes() {
    let (config, objects) = mock::register();
    let store = GenerationStore::open_durable(&config).unwrap();
    let first = store.put_object("ambiguous", b"original").unwrap();
    // The S3 multipart completion path reports some failed conditions as
    // generic errors. The adapter must still resolve an identical replay.
    *objects.fail_prefix.lock() = Some(first.path.clone());
    assert_eq!(store.put_object("ambiguous", b"original").unwrap(), first);
    let error = store.put_object("ambiguous", b"different").unwrap_err();
    assert!(error.to_string().contains("injected durable write failure"));
    *objects.fail_prefix.lock() = Some("objects/absent".into());
    assert!(store
        .put_object("absent", b"new")
        .unwrap_err()
        .to_string()
        .contains("injected durable write failure"));
    assert_eq!(store.read_object(&first).unwrap(), b"original");
    *objects.fail_prefix.lock() = None;
    *objects.fail_after_put.lock() = true;
    let recovered = store.put_object("lost-response", b"committed").unwrap();
    assert_eq!(store.read_object(&recovered).unwrap(), b"committed");
}

/// Explicit cloud-operator smoke only. Normal tests never connect to AWS.
#[test]
#[ignore = "requires explicit SPANN_DURABLE_SMOKE_CONFIG JSON and authorized cloud execution"]
fn durable_s3_adapter_smoke() {
    let mut config: DurableStoreConfig = serde_json::from_str(
        &std::env::var("SPANN_DURABLE_SMOKE_CONFIG")
            .expect("explicit durable smoke config is required"),
    )
    .unwrap();
    // A unique suffix preserves all earlier attempts and isolates concurrent runs.
    config
        .prefix
        .push_str(&format!("/adapter-smoke-{}", uuid::Uuid::new_v4()));
    config.validate().unwrap();
    eprintln!(
        "durable_s3_smoke_namespace={}",
        serde_json::to_string(&config).unwrap()
    );
    let store = GenerationStore::open_durable(&config).unwrap();
    assert!(store.latest().unwrap().is_none());
    let large_bytes = S3StorageConfig::default()
        .upload_part_size_bytes
        .max(8 * 1024 * 1024)
        + 17;
    for (name, size) in [("small", 17), ("multipart", large_bytes)] {
        let payload = (0..size).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let reference = store.put_object(name, &payload).unwrap();
        assert_eq!(store.put_object(name, &payload).unwrap(), reference);
        let mut changed = payload.clone();
        changed[0] ^= 1;
        assert!(store.put_object(name, &changed).is_err());
        assert_eq!(store.read_object(&reference).unwrap(), payload);
    }
    let vector = store
        .put_object("vector-descriptor", b"independent smoke vector view")
        .unwrap();
    let first = fixture(&store, "gen1", "attempt1", &vector).unwrap();
    let second = fixture(&store, "gen2", "attempt2", &vector).unwrap();
    let third = fixture(&store, "gen3", "attempt3", &vector).unwrap();
    let fourth = fixture(&store, "gen4", "attempt4", &vector).unwrap();
    store.publish_latest(&first, None).unwrap();
    store.publish_latest(&first, None).unwrap();
    store.publish_latest(&second, Some("gen1")).unwrap();
    assert!(matches!(
        store.publish_latest(&third, Some("gen1")),
        Err(GenerationError::Conflict)
    ));
    assert_eq!(store.latest().unwrap(), Some(second));
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let outcomes = std::thread::scope(|scope| {
        let handles = [&third, &fourth].map(|descriptor| {
            let barrier = barrier.clone();
            let store = store.clone();
            scope.spawn(move || {
                barrier.wait();
                store.publish_latest(descriptor, Some("gen2"))
            })
        });
        handles.map(|handle| handle.join().unwrap())
    });
    // Preserve both errors and the durable state before asserting, so a real
    // backend failure distinguishes contention from an ambiguous response.
    eprintln!("durable_s3_cas_outcomes={outcomes:?}");
    eprintln!("durable_s3_cas_latest={:?}", store.latest());
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| matches!(result, Err(GenerationError::Conflict)))
            .count(),
        1
    );
    drop(store);
    let fresh = GenerationStore::open_durable(&config).unwrap();
    let current = fresh.latest().unwrap().unwrap();
    assert!(current == third || current == fourth);
    let reader = SavedReader::open(fresh, &first).unwrap();
    assert_eq!(
        reader.validate_membership().unwrap(),
        BTreeSet::from([1, 2])
    );
    eprintln!("durable_s3_smoke_passed multipart_bytes={large_bytes} immutable_replay=true conflict_preserves_bytes=true conditional_latest=true stale_latest_preserved=true single_cas_winner=true previous_reader_valid=true");
}

/// Repeat only publication contention. Each round has an independent namespace;
/// retain every outcome before failing so intermittent errors stay inspectable.
#[test]
#[ignore = "requires explicit SPANN_DURABLE_SMOKE_CONFIG JSON and authorized cloud execution"]
fn durable_s3_cas_diagnostic() {
    let base: DurableStoreConfig = serde_json::from_str(
        &std::env::var("SPANN_DURABLE_SMOKE_CONFIG")
            .expect("explicit durable smoke config is required"),
    )
    .unwrap();
    base.validate().unwrap();
    let rounds: usize = std::env::var("SPANN_DURABLE_CAS_ROUNDS")
        .unwrap_or_else(|_| "10".into())
        .parse()
        .unwrap();
    assert!((1..=100).contains(&rounds));
    let mut failed = Vec::new();
    for round in 0..rounds {
        let mut config = base.clone();
        config
            .prefix
            .push_str(&format!("/cas-diagnostic-{}", uuid::Uuid::new_v4()));
        eprintln!(
            "durable_s3_cas_round={round} namespace={}",
            serde_json::to_string(&config).unwrap()
        );
        let store = GenerationStore::open_durable(&config).unwrap();
        let vector = store
            .put_object(
                "vector-descriptor",
                b"independent CAS diagnostic vector view",
            )
            .unwrap();
        let first = fixture(&store, "initial", "initial-attempt", &vector).unwrap();
        let left = fixture(&store, "left", "left-attempt", &vector).unwrap();
        let right = fixture(&store, "right", "right-attempt", &vector).unwrap();
        store.publish_latest(&first, None).unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let outcomes = std::thread::scope(|scope| {
            let handles = [&left, &right].map(|descriptor| {
                let store = store.clone();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    barrier.wait();
                    store.publish_latest(descriptor, Some("initial"))
                })
            });
            handles.map(|h| h.join().unwrap())
        });
        let latest = store.latest();
        eprintln!("durable_s3_cas_round={round} outcomes={outcomes:?} latest={latest:?}");
        let winners = outcomes.iter().filter(|r| r.is_ok()).count();
        let conflicts = outcomes
            .iter()
            .filter(|r| matches!(r, Err(GenerationError::Conflict)))
            .count();
        let acknowledged = outcomes
            .iter()
            .enumerate()
            .find(|(_, r)| r.is_ok())
            .map(|(i, _)| if i == 0 { &left } else { &right });
        let consistent = latest.as_ref().ok().and_then(|d| d.as_ref()) == acknowledged;
        if winners != 1 || conflicts != 1 || !consistent {
            failed.push(round);
        }
    }
    assert!(
        failed.is_empty(),
        "CAS diagnostic failed rounds: {failed:?}"
    );
}

#[test]
fn durable_conditional_error_reconciliation_uses_exact_pointer_state() {
    for mode in [
        "own",
        "competitor",
        "unchanged",
        "unavailable",
        "malformed",
        "absent",
        "reformatted",
        "alias",
    ] {
        let (config, objects) = mock::register();
        let store = GenerationStore::open_durable(&config).unwrap();
        let vector = store.put_object("vectors", b"vector view").unwrap();
        let first = fixture(&store, "first", "first-attempt", &vector).unwrap();
        let second = fixture(&store, "second", "second-attempt", &vector).unwrap();
        let competitor = fixture(&store, "competitor", "competitor-attempt", &vector).unwrap();
        if mode != "absent" {
            store.publish_latest(&first, None).unwrap();
        }
        let replacement = match mode {
            "own" | "unavailable" => Some(serde_json::to_vec(&second).unwrap()),
            "competitor" => Some(serde_json::to_vec(&competitor).unwrap()),
            "malformed" => Some(b"not a generation descriptor".to_vec()),
            "reformatted" => Some(serde_json::to_vec_pretty(&first).unwrap()),
            "alias" => {
                let mut alias = second.clone();
                alias.save_attempt = "other-attempt".into();
                Some(serde_json::to_vec(&alias).unwrap())
            }
            _ => None,
        };
        *objects.latest_put_failure.lock() = Some((replacement, mode == "unavailable"));
        let before = objects.writes.lock().len();
        let reads_before = objects
            .reads
            .lock()
            .iter()
            .filter(|key| key.as_str() == "latest.json")
            .count();
        let result = store.publish_latest(
            &second,
            if mode == "absent" {
                None
            } else {
                Some("first")
            },
        );
        assert_eq!(
            objects
                .reads
                .lock()
                .iter()
                .filter(|key| key.as_str() == "latest.json")
                .count(),
            reads_before + 2,
            "one reconciliation read: {mode}"
        );
        assert_eq!(
            objects.writes.lock().len(),
            before + 1,
            "no retries: {mode}"
        );
        match mode {
            "own" => {
                result.unwrap();
                assert_eq!(store.latest().unwrap(), Some(second));
            }
            "alias" => {
                assert!(matches!(result, Err(GenerationError::Conflict)));
            }
            "competitor" => {
                assert!(matches!(result, Err(GenerationError::Conflict)));
                assert_eq!(store.latest().unwrap(), Some(competitor));
            }
            _ => {
                let error = result.unwrap_err().to_string();
                assert!(
                    error.contains("injected ambiguous conditional PUT"),
                    "{mode}: {error}"
                );
                if mode == "unavailable" {
                    assert!(error.contains("injected reconciliation read failure"));
                }
                if mode == "unchanged" {
                    assert_eq!(store.latest().unwrap(), Some(first));
                }
            }
        }
    }
}
