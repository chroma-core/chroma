//! Gate actual sealed local/TCP owner calls to prove overlap and publication
//! ordering without a wall-time speedup threshold.
use super::super::{
    construction::BoundaryView,
    engine::{PostingList, StoredNode},
    protocol::publication::Publication,
    routing::{Child, NodeRef},
    saved_reader::{CanonicalVectorSource, SavedReader, VectorSource},
    transport::{RpcClient, RpcServer, TransportConfig},
};
use super::*;
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use parking_lot::{Condvar, Mutex};
use sha2::{Digest, Sha256};
use std::{
    sync::{mpsc, Arc, LazyLock},
    time::Duration,
};

type Hook = Arc<dyn Fn(u64, bool) -> Result<()> + Send + Sync>;
static HOOKS: LazyLock<Mutex<BTreeMap<u128, Hook>>> = LazyLock::new(Mutex::default);
pub(super) fn owner_hook(run: u128, owner: u64, materialized: bool) -> Result<()> {
    let hook = HOOKS.lock().get(&run).cloned();
    if let Some(hook) = hook {
        hook(owner, materialized)?;
    }
    Ok(())
}
struct HookGuard(u128);
impl Drop for HookGuard {
    fn drop(&mut self) {
        HOOKS.lock().remove(&self.0);
    }
}
fn install_hook(run: u128, hook: Hook) -> HookGuard {
    assert!(HOOKS.lock().insert(run, hook).is_none());
    HookGuard(run)
}

struct Fixture {
    _dir: tempfile::TempDir,
    save: PathBuf,
    cluster: Arc<Cluster>,
    source: Arc<CanonicalVectorSource>,
    peer: Arc<OwnerService>,
    vectors: VectorFileDescriptor,
    server: RpcServer,
    saved_object_reads: Arc<std::sync::atomic::AtomicUsize>,
}
impl Fixture {
    fn new(run: u128) -> Self {
        Self::with_spare_leaves(run, 0)
    }
    fn with_spare_leaves(run: u128, spare_count: usize) -> Self {
        Self::with_deadline(run, spare_count, None)
    }
    fn with_deadline(run: u128, spare_count: usize, deadline: Option<Instant>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let bytes = [0.2_f32, 0.2, 0.8, 0.8]
            .into_iter()
            .flat_map(f32::to_le_bytes)
            .collect::<Vec<_>>();
        let path = dir.path().join("vectors.f32");
        std::fs::write(&path, &bytes).unwrap();
        let vectors = VectorFileDescriptor {
            path: path.to_str().unwrap().into(),
            rows: 2,
            dimensions: 2,
            stride_bytes: 8,
            offset_bytes: 0,
            first_id: 0,
            file_bytes: 16,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            immutable_locators: vec!["fixture:concurrent-save".into()],
        };
        let source = Arc::new(
            CanonicalVectorSource::open(vectors.clone())
                .unwrap()
                .preload()
                .unwrap(),
        );
        let owner = |id| {
            Arc::new(OwnerService::new(
                id,
                run,
                2,
                DistanceFunction::Euclidean,
                super::super::super::config::HierarchicalSpannConfig::default(),
                source.clone(),
            ))
        };
        let one = owner(1);
        let two = owner(2);
        let mut root = one.initial_root().routing;
        root.leaf = false;
        root.centroid.fill(0.5);
        root.centroid_code = Code::<4>::quantize(&root.centroid, &[0.; 2])
            .as_ref()
            .to_vec();
        let leaf = |offset, owner, center, vector| {
            let mut routing = root.clone();
            routing.target = NodeRef {
                id: root.target.id + offset,
                authority: owner,
                placement: 0,
            };
            routing.parent = Some(root.target);
            routing.leaf = true;
            routing.centroid.fill(center);
            routing.centroid_code = Code::<4>::quantize(&routing.centroid, &[0.; 2])
                .as_ref()
                .to_vec();
            let codes = Code::<1>::quantize(&source.get(vector).unwrap(), &routing.centroid)
                .as_ref()
                .to_vec();
            StoredNode {
                routing,
                postings: Some(PostingList {
                    ids: vec![vector],
                    codes,
                }),
            }
        };
        let left = leaf(1, 1, 0.2, 0);
        let right = leaf(2, 2, 0.8, 1);
        let spare = (0..spare_count)
            .map(|i| {
                let mut node = leaf(3 + i as u128, 1, 0.5, 0);
                node.postings = Some(PostingList::default());
                node
            })
            .collect::<Vec<_>>();
        root.children = [&left, &right]
            .into_iter()
            .chain(spare.iter())
            .map(|n| Child {
                target: n.routing.target,
                centroid: n.routing.centroid.clone(),
                leaf: true,
            })
            .collect();
        let units = root
            .children
            .iter()
            .map(|child| (child.target.id, child.target))
            .collect();
        one.install_initial(
            vec![
                StoredNode {
                    routing: root.clone(),
                    postings: None,
                },
                left,
            ]
            .into_iter()
            .chain(spare)
            .collect(),
        )
        .unwrap();
        two.install_initial(vec![right]).unwrap();
        let endpoint = two.clone();
        let saved_object_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed_reads = saved_object_reads.clone();
        let server = RpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "concurrent-save".into(),
            TransportConfig::default(),
            Arc::new(move |r: EngineRequest| {
                if matches!(r, EngineRequest::ReadSavedObject(_)) {
                    observed_reads.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                endpoint.handle(r)
            }),
        )
        .unwrap();
        let client = RpcClient::new(
            server.data_addr(),
            server.control_addr(),
            "concurrent-save".into(),
            TransportConfig::default(),
        )
        .unwrap();
        let mut cluster = Cluster::new(one.clone(), BTreeMap::from([(2, client)]), root.clone());
        cluster.shutdown_deadline = deadline;
        cluster.publication = Publication::new(
            BoundaryView {
                navigation_topology: None,
                root: root.clone(),
                shared: BTreeMap::from([(root.target.id, root)]),
                units,
            }
            .prepared()
            .unwrap(),
            1_000_000,
        );
        let cluster = Arc::new(cluster);
        one.bind_cluster(&cluster);
        let save = dir.path().join("saved");
        Self {
            _dir: dir,
            save,
            cluster,
            source,
            peer: two,
            vectors,
            server,
            saved_object_reads,
        }
    }
    fn check(&self, descriptor: &GenerationDescriptor) {
        let reader =
            SavedReader::open(GenerationStore::open(&self.save).unwrap(), descriptor).unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            [0, 1].into_iter().collect()
        );
        // A separately opened input adapter checks real corpus bytes/identity.
        let source = CanonicalVectorSource::open(self.vectors.clone())
            .unwrap()
            .preload()
            .unwrap();
        assert_eq!(reader.validate_codes(&source).unwrap(), (3, 2));
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.shutdown();
    }
}

#[derive(Default)]
struct State {
    started: BTreeSet<u64>,
    materialized: BTreeSet<u64>,
    released: bool,
    failure_seen: bool,
    peak: usize,
}
#[derive(Default)]
struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}
impl Gate {
    fn wait(&self, predicate: impl Fn(&State) -> bool) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut state = self.state.lock();
        while !predicate(&state) {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or("test owner gate timed out")?;
            self.changed.wait_for(&mut state, remaining);
        }
        Ok(())
    }
    fn release(&self) {
        self.state.lock().released = true;
        self.changed.notify_all();
    }
}

#[test]
fn concurrent_owner_materialization_joins_before_copy_and_publication_and_replays_exactly() {
    let fixture = Fixture::new(3921);
    let gate = Arc::new(Gate::default());
    let observed = gate.clone();
    let guard = install_hook(
        3921,
        Arc::new(move |owner, materialized| {
            if !materialized {
                {
                    let mut s = observed.state.lock();
                    s.started.insert(owner);
                    s.peak = s.peak.max(s.started.len());
                    observed.changed.notify_all();
                }
                // A sequential implementation fails this gate rather than merely
                // running more slowly: the other owner cannot reach its start.
                observed.wait(|s| s.started.len() == 2)?;
            } else {
                {
                    observed.state.lock().materialized.insert(owner);
                    observed.changed.notify_all();
                }
                if owner == 2 {
                    observed.wait(|s| s.released)?;
                }
            }
            Ok(())
        }),
    );
    let (send, recv) = mpsc::channel();
    std::thread::scope(|scope| {
        let cluster = &fixture.cluster;
        let path = &fixture.save;
        let vectors = fixture.vectors.clone();
        scope.spawn(move || {
            send.send(cluster.save_completed_profiled(path, vectors, "generation", "attempt"))
                .unwrap();
        });
        gate.wait(|s| s.materialized.len() == 2).unwrap();
        assert_eq!(gate.state.lock().peak, 2);
        assert!(!fixture.save.join("latest.json").exists());
        assert!(!fixture.save.join("descriptor.json").exists());
        assert!(!fixture.save.join("generations/generation.json").exists());
        // Only the local owner's vector descriptor, posting, routing,
        // navigation and validation objects exist here. Remote payloads still
        // live solely in its staging store.
        assert_eq!(
            std::fs::read_dir(fixture.save.join("objects"))
                .unwrap()
                .count(),
            5
        );
        assert!(matches!(recv.try_recv(), Err(mpsc::TryRecvError::Empty)));
        gate.release();
        let (descriptor, profile) = recv.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(profile.owner_materialization_max_concurrency, 2);
        assert!(profile.owner_materialization_wall_nanos > 0);
        assert_eq!(profile.owners.len(), 2);
        assert_eq!(profile.local_reuse.objects, 6);
        assert_eq!(profile.remote_copy.objects, 6);
        fixture.check(&descriptor);
        drop(guard);
        let (replayed, replay) = fixture
            .cluster
            .save_completed_profiled(
                &fixture.save,
                fixture.vectors.clone(),
                "generation",
                "attempt",
            )
            .unwrap();
        assert_eq!(replayed, descriptor);
        assert_eq!(replay.owners[&1].store_io.new_names, 0);
        assert_eq!(replay.owners[&2].store_io.new_names, 0);
        fixture.check(&replayed);
        eprintln!("sealed owner save: observed_in_flight_owner_calls=2, configured_bound=2, local_reused=6, remote_copied=6, replay_descriptor_exact=true");
    });
}

#[test]
fn concurrent_owner_failure_joins_other_writer_and_preserves_previous_latest() {
    let fixture = Fixture::new(3922);
    let previous = fixture
        .cluster
        .save_completed(
            &fixture.save,
            fixture.vectors.clone(),
            "previous",
            "previous-attempt",
        )
        .unwrap();
    let previous_latest = std::fs::read(fixture.save.join("latest.json")).unwrap();
    let previous_descriptor = std::fs::read(fixture.save.join("descriptor.json")).unwrap();
    let gate = Arc::new(Gate::default());
    let observed = gate.clone();
    let _guard = install_hook(
        3922,
        Arc::new(move |owner, materialized| {
            if materialized {
                return Ok(());
            }
            {
                let mut s = observed.state.lock();
                s.started.insert(owner);
                observed.changed.notify_all();
            }
            observed.wait(|s| s.started.len() == 2)?;
            if owner == 2 {
                observed.state.lock().failure_seen = true;
                observed.changed.notify_all();
                return Err("injected sealed remote owner failure".into());
            }
            observed.wait(|s| s.released)
        }),
    );
    let (send, recv) = mpsc::channel();
    std::thread::scope(|scope| {
        let cluster = &fixture.cluster;
        let path = &fixture.save;
        let vectors = fixture.vectors.clone();
        scope.spawn(move || {
            send.send(cluster.save_completed(path, vectors, "failed", "failed-attempt"))
                .unwrap();
        });
        gate.wait(|s| s.failure_seen).unwrap();
        // The failed peer does not let the coordinator return with the local
        // immutable writer still running, or publish any partial new state.
        assert!(matches!(recv.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert!(!fixture.save.join("generations/failed.json").exists());
        assert_eq!(
            std::fs::read(fixture.save.join("latest.json")).unwrap(),
            previous_latest
        );
        gate.release();
        let error = recv
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err();
        assert!(
            error.contains("injected sealed remote owner failure"),
            "{error}"
        );
    });
    assert_eq!(
        std::fs::read(fixture.save.join("latest.json")).unwrap(),
        previous_latest
    );
    assert_eq!(
        std::fs::read(fixture.save.join("descriptor.json")).unwrap(),
        previous_descriptor
    );
    assert!(!fixture.save.join("generations/failed.json").exists());
    fixture.check(&previous);
    eprintln!("sealed owner failure: joined_remaining_writer=true, partial_generation_published=false, previous_latest_exact=true, previous_membership_and_codes_valid=true");
}

#[test]
fn durable_owners_save_without_relay_and_reader_survives_writer_files_removal() {
    use super::super::generation_backend::mock;
    let fixture = Fixture::new(3929);
    let (config, objects) = mock::register();
    let (descriptor, profile) = fixture
        .cluster
        .save_completed_durable_profiled(
            &fixture.save,
            &config,
            fixture.vectors.clone(),
            "durable",
            "direct-save",
        )
        .unwrap();
    assert_eq!(profile.remote_copy.objects, 0);
    assert_eq!(profile.remote_copy.bytes, 0);
    assert_eq!(profile.direct_durable.objects, 12);
    assert_eq!(profile.owners.len(), 2);
    assert_eq!(
        fixture
            .saved_object_reads
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(!fixture.save.join("objects").exists());
    let (replayed, replay) = fixture
        .cluster
        .save_completed_durable_profiled(
            &fixture.save,
            &config,
            fixture.vectors.clone(),
            "durable",
            "direct-save",
        )
        .unwrap();
    assert_eq!(replayed, descriptor);
    assert_eq!(replay.owners[&1].store_io.new_names, 0);
    assert_eq!(replay.owners[&2].store_io.new_names, 0);
    let (changed, _) = mock::register();
    assert!(fixture
        .cluster
        .save_completed_durable_profiled(
            &fixture.save,
            &changed,
            fixture.vectors.clone(),
            "durable",
            "direct-save",
        )
        .unwrap_err()
        .contains("conflicting frozen"));

    // Retain only the independent corpus in a separate reader directory. Dropping
    // the fixture removes every writer file and shuts down the TCP owner server.
    let independent = tempfile::tempdir().unwrap();
    let vector_path = independent.path().join("vectors.f32");
    std::fs::copy(&fixture.vectors.path, &vector_path).unwrap();
    let writer_path = fixture._dir.path().to_path_buf();
    drop(fixture);
    assert!(!writer_path.exists());
    objects.reads.lock().clear();
    let reader =
        SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &descriptor).unwrap();
    // Opening reads routing and sibling arrays, never postings or exact centroids.
    assert!(objects
        .reads
        .lock()
        .iter()
        .all(|key| !key.contains("postings") && !key.contains("validation")));
    assert!(objects
        .reads
        .lock()
        .iter()
        .any(|key| key.contains("navigation")));
    let source = reader.open_vector_source_at(Some(&vector_path)).unwrap();
    assert_eq!(
        reader.validate_membership().unwrap(),
        BTreeSet::from([0, 1])
    );
    assert_eq!(reader.validate_codes(&source).unwrap(), (3, 2));
    assert!(objects
        .reads
        .lock()
        .iter()
        .any(|key| key.contains("postings")));
}

#[test]
fn durable_failed_owner_joins_sibling_and_preserves_previous_generation() {
    use super::super::generation_backend::mock;
    let fixture = Fixture::new(3930);
    let (config, objects) = mock::register();
    let (previous, _) = fixture
        .cluster
        .save_completed_durable_profiled(
            &fixture.save,
            &config,
            fixture.vectors.clone(),
            "previous",
            "previous-attempt",
        )
        .unwrap();
    let gate = Arc::new(Gate::default());
    let observed = gate.clone();
    let _guard = install_hook(
        3930,
        Arc::new(move |owner, materialized| {
            if materialized {
                return Ok(());
            }
            observed.state.lock().started.insert(owner);
            observed.changed.notify_all();
            observed.wait(|s| s.started.len() == 2)?;
            if owner == 2 {
                observed.state.lock().failure_seen = true;
                observed.changed.notify_all();
                return Err("injected durable owner failure".into());
            }
            observed.wait(|s| s.released)
        }),
    );
    let (send, recv) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            send.send(fixture.cluster.save_completed_durable_profiled(
                &fixture.save,
                &config,
                fixture.vectors.clone(),
                "failed",
                "failed-attempt",
            ))
            .unwrap();
        });
        gate.wait(|s| s.failure_seen).unwrap();
        assert!(matches!(recv.try_recv(), Err(mpsc::TryRecvError::Empty)));
        assert_eq!(
            GenerationStore::open_durable(&config)
                .unwrap()
                .latest()
                .unwrap(),
            Some(previous.clone())
        );
        assert!(!objects
            .objects
            .lock()
            .contains_key("generations/failed.json"));
        gate.release();
        assert!(recv
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err()
            .contains("injected durable owner failure"));
    });
    assert_eq!(
        GenerationStore::open_durable(&config)
            .unwrap()
            .latest()
            .unwrap(),
        Some(previous.clone())
    );
    assert!(!objects
        .objects
        .lock()
        .contains_key("generations/failed.json"));
    let reader =
        SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &previous).unwrap();
    assert_eq!(
        reader.validate_membership().unwrap(),
        BTreeSet::from([0, 1])
    );
    assert_eq!(reader.validate_codes(&*fixture.source).unwrap(), (3, 2));
}

#[test]
fn durable_second_generation_reuses_unchanged_blocks_across_all_transfer_paths() {
    use super::super::generation_backend::mock;
    let fixture = Fixture::with_spare_leaves(3931, 8);
    let (config, objects) = mock::register();
    let save = |generation: &str| {
        fixture
            .cluster
            .save_completed_durable_profiled(
                &fixture.save,
                &config,
                fixture.vectors.clone(),
                generation,
                generation,
            )
            .unwrap()
    };
    let (first, _) = save("first");
    let store = GenerationStore::open_durable(&config).unwrap();
    let references = |descriptor: &GenerationDescriptor| {
        store
            .reopen(descriptor)
            .unwrap()
            .owners
            .iter()
            .flat_map(|reference| {
                let owner: OwnerManifest =
                    serde_json::from_slice(&store.read_object(reference).unwrap()).unwrap();
                owner.postings
            })
            .map(|reference| (reference.path.clone(), reference))
            .collect::<BTreeMap<_, _>>()
    };
    let initial = references(&first);
    objects.writes.lock().clear();
    let (unchanged, profile) = save("unchanged");
    assert_eq!(references(&unchanged), initial);
    assert_eq!(
        profile
            .owners
            .values()
            .map(|p| p.posting_blocks_reused)
            .sum::<u64>(),
        10
    );
    assert_eq!(
        profile
            .owners
            .values()
            .map(|p| p.posting_blocks_materialized)
            .sum::<u64>(),
        0
    );
    assert!(objects
        .writes
        .lock()
        .iter()
        .all(|key| !key.starts_with("objects/postings-")));
    assert!(fixture
        .cluster
        .save_completed_durable_profiled(
            &fixture.save,
            &config,
            fixture.vectors.clone(),
            "first",
            "first"
        )
        .is_err());
    assert_eq!(store.latest().unwrap(), Some(unchanged));
    assert_eq!(
        SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &first)
            .unwrap()
            .validate_membership()
            .unwrap(),
        BTreeSet::from([0, 1])
    );
    let base = 1u128 << 64;
    let target = |offset, authority| NodeRef {
        id: base + offset,
        authority,
        placement: 0,
    };
    for (epoch, name, source, destination, batched) in [
        (1, "pair", target(1, 1), target(2, 2), false),
        (2, "pair-back", target(2, 2), target(3, 1), false),
        (3, "local-bulk", target(3, 1), target(1, 1), false),
        (4, "owner-batch", target(1, 1), target(2, 2), true),
    ] {
        for owner in [1, 2] {
            fixture
                .cluster
                .request(owner, EngineRequest::OpenEpoch { run: 3931, epoch })
                .unwrap();
        }
        if batched {
            fixture
                .cluster
                .transfer_owner_batch_for_job(
                    source,
                    BTreeMap::from([(destination, vec![0])]),
                    None,
                    None,
                )
                .unwrap();
        } else {
            fixture
                .cluster
                .transfer(source, destination, vec![0])
                .unwrap();
        }
        objects.writes.lock().clear();
        let (next, profile) = save(name);
        assert_eq!(
            profile
                .owners
                .values()
                .map(|p| p.posting_blocks_reused)
                .sum::<u64>(),
            8,
            "{name}"
        );
        assert_eq!(
            profile
                .owners
                .values()
                .map(|p| p.posting_blocks_materialized)
                .sum::<u64>(),
            2,
            "{name}"
        );
        assert_eq!(
            profile
                .owners
                .values()
                .map(|p| p.posting_vectors_scanned)
                .sum::<u64>(),
            2
        );
        assert_eq!(
            objects
                .writes
                .lock()
                .iter()
                .filter(|key| key.starts_with("objects/postings-"))
                .count(),
            2
        );
        let reader =
            SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &next).unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            BTreeSet::from([0, 1])
        );
        assert_eq!(reader.validate_codes(&*fixture.source).unwrap(), (11, 2));
        assert_eq!(store.latest().unwrap(), Some(next));
        let old =
            SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &first).unwrap();
        assert_eq!(old.validate_membership().unwrap(), BTreeSet::from([0, 1]));
        assert_eq!(old.validate_codes(&*fixture.source).unwrap(), (11, 2));
        assert_eq!(references(&first), initial);
    }
}

#[test]
fn durable_cache_rejects_replaced_residents_and_tracks_structural_snapshots() {
    use super::super::{
        engine::Resident,
        generation_backend::mock,
        protocol::{publication::PublicationProposal, AttemptId},
    };
    let fixture = Fixture::with_spare_leaves(3932, 2);
    let (config, _) = mock::register();
    let save = |name: &str| {
        fixture
            .cluster
            .save_completed_durable_profiled(
                &fixture.save,
                &config,
                fixture.vectors.clone(),
                name,
                name,
            )
            .unwrap()
    };
    let (first, _) = save("first");
    let base = 1u128 << 64;
    let left = base + 1;
    // Holding the saved routing Arc forces an in-place revision edit to produce
    // a different immutable image. Even a metadata-only change invalidates it.
    {
        let handle = fixture.cluster.local.nodes.get(&left).unwrap().clone();
        Arc::make_mut(&mut handle.write().routing).revision += 1;
    }
    let (_, changed) = save("routing-change");
    assert_eq!(changed.owners[&1].posting_blocks_materialized, 1);
    // Replacement with even the exact same routing Arc must not match a cache
    // entry for the retired resident allocation (split/install/handoff boundary).
    let replace = |owner: &OwnerService,
                   id: u128,
                   routing: Arc<super::super::routing::RoutingNode>,
                   postings: PostingList| {
        owner.nodes.insert(
            id,
            Arc::new(parking_lot::RwLock::new(Resident {
                routing,
                postings: Some(postings),
                reserved: None,
                preparing_split: false,
                transfer_source: None,
                claim_producer: None,
            })),
        );
    };
    {
        let old = fixture.cluster.local.nodes.get(&left).unwrap().clone();
        let old = old.read();
        replace(
            &fixture.cluster.local,
            left,
            old.routing.clone(),
            old.postings.clone().unwrap(),
        );
    }
    let (_, replaced) = save("resident-replaced");
    assert_eq!(replaced.owners[&1].posting_blocks_materialized, 1);
    let publish_shape = |sequence: u64| {
        let old = fixture.cluster.publication.view();
        let mut view = old.value.clone();
        view.root.children = [&fixture.cluster.local, &fixture.peer]
            .into_iter()
            .flat_map(|owner| {
                owner
                    .nodes
                    .iter()
                    .filter_map(|node| {
                        let node = node.read();
                        node.routing.leaf.then(|| Child {
                            target: node.routing.target,
                            centroid: node.routing.centroid.clone(),
                            leaf: true,
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        view.root
            .children
            .sort_unstable_by_key(|child| child.target.id);
        view.units = view
            .root
            .children
            .iter()
            .map(|child| (child.target.id, child.target))
            .collect();
        view.shared.insert(view.root.target.id, view.root.clone());
        let attempt = AttemptId {
            authority: 1,
            sequence,
        };
        fixture
            .cluster
            .publication
            .prepare(
                attempt,
                PublicationProposal {
                    expected_sequence: old.sequence,
                    required_owners: BTreeSet::new(),
                    value: view.prepared().unwrap(),
                },
            )
            .unwrap();
        assert!(fixture.cluster.publication.acquire_turn(attempt).unwrap());
        fixture.cluster.publication.publish(attempt).unwrap();
        fixture.cluster.publication.release_turn(attempt).unwrap();
    };
    // These controlled sealed snapshots exercise persistence of the shapes
    // produced by split, merge and handoff, not their mutation protocols.
    let (_, removed) = fixture.cluster.local.nodes.remove(&(base + 3)).unwrap();
    let template = removed.read().routing.clone();
    for id in [base + 30, base + 31] {
        let mut routing = (*template).clone();
        routing.target.id = id;
        replace(
            &fixture.cluster.local,
            id,
            Arc::new(routing),
            PostingList::default(),
        );
    }
    publish_shape(1);
    let (split, profile) = save("split-shape");
    assert_eq!(profile.owners[&1].posting_blocks_materialized, 2);
    for id in [base + 30, base + 31] {
        fixture.cluster.local.nodes.remove(&id).unwrap();
    }
    let mut merged = (*template).clone();
    merged.target.id = base + 32;
    replace(
        &fixture.cluster.local,
        merged.target.id,
        Arc::new(merged.clone()),
        PostingList::default(),
    );
    publish_shape(2);
    let (merge, profile) = save("merge-shape");
    assert_eq!(profile.owners[&1].posting_blocks_materialized, 1);
    fixture
        .cluster
        .local
        .nodes
        .remove(&merged.target.id)
        .unwrap();
    merged.target.authority = 2;
    merged.target.placement += 1;
    replace(
        &fixture.peer,
        merged.target.id,
        Arc::new(merged),
        PostingList::default(),
    );
    publish_shape(3);
    let (moved, profile) = save("ownership-shape");
    assert_eq!(profile.owners[&1].posting_blocks_materialized, 0);
    assert_eq!(profile.owners[&2].posting_blocks_materialized, 1);
    for (descriptor, nodes) in [(first, 5), (split, 6), (merge, 5), (moved, 5)] {
        let reader =
            SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &descriptor)
                .unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            BTreeSet::from([0, 1])
        );
        assert_eq!(reader.validate_codes(&*fixture.source).unwrap(), (nodes, 2));
    }
}

#[test]
fn expired_save_joins_local_writer_and_preserves_previous_latest() {
    use super::super::generation_backend::mock;
    let deadline = Instant::now() + Duration::from_secs(1);
    let fixture = Fixture::with_deadline(3999, 0, Some(deadline));
    let (config, objects) = mock::register();
    let (previous, _) = fixture
        .cluster
        .save_completed_durable_profiled(
            &fixture.save,
            &config,
            fixture.vectors.clone(),
            "previous",
            "previous-attempt",
        )
        .unwrap();
    let gate = Arc::new(Gate::default());
    let observed = gate.clone();
    let _guard = install_hook(
        3999,
        Arc::new(move |owner, materialized| {
            if materialized {
                return Ok(());
            }
            observed.state.lock().started.insert(owner);
            observed.changed.notify_all();
            observed.wait(|s| s.released)
        }),
    );
    let (send, recv) = mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            send.send(fixture.cluster.save_completed_durable_profiled(
                &fixture.save,
                &config,
                fixture.vectors.clone(),
                "expired",
                "expired-attempt",
            ))
            .unwrap();
        });
        gate.wait(|s| s.started.len() == 2).unwrap();
        std::thread::sleep(
            deadline.saturating_duration_since(Instant::now()) + Duration::from_millis(100),
        );
        assert!(matches!(recv.try_recv(), Err(mpsc::TryRecvError::Empty)));
        gate.release();
        assert!(recv
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap_err()
            .contains("deadline"));
    });
    assert_eq!(
        GenerationStore::open_durable(&config)
            .unwrap()
            .latest()
            .unwrap(),
        Some(previous.clone())
    );
    assert!(!objects
        .objects
        .lock()
        .contains_key("generations/expired.json"));
    let reader =
        SavedReader::open(GenerationStore::open_durable(&config).unwrap(), &previous).unwrap();
    assert_eq!(
        reader.validate_membership().unwrap(),
        [0, 1].into_iter().collect()
    );
}
