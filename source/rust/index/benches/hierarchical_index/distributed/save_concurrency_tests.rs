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
    vectors: VectorFileDescriptor,
    server: RpcServer,
}
impl Fixture {
    fn new(run: u128) -> Self {
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
        root.children = [&left, &right]
            .into_iter()
            .map(|n| Child {
                target: n.routing.target,
                centroid: n.routing.centroid.clone(),
                leaf: true,
            })
            .collect();
        let units = [
            (left.routing.target.id, left.routing.target),
            (right.routing.target.id, right.routing.target),
        ]
        .into_iter()
        .collect();
        one.install_initial(vec![
            StoredNode {
                routing: root.clone(),
                postings: None,
            },
            left,
        ])
        .unwrap();
        two.install_initial(vec![right]).unwrap();
        let endpoint = two.clone();
        let server = RpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "concurrent-save".into(),
            TransportConfig::default(),
            Arc::new(move |r: EngineRequest| endpoint.handle(r)),
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
            vectors,
            server,
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
        // Only the local owner's vector descriptor, posting and routing object
        // exist here. Remote payloads still live solely in its staging store.
        assert_eq!(
            std::fs::read_dir(fixture.save.join("objects"))
                .unwrap()
                .count(),
            3
        );
        assert!(matches!(recv.try_recv(), Err(mpsc::TryRecvError::Empty)));
        gate.release();
        let (descriptor, profile) = recv.recv_timeout(Duration::from_secs(5)).unwrap().unwrap();
        assert_eq!(profile.owner_materialization_max_concurrency, 2);
        assert!(profile.owner_materialization_wall_nanos > 0);
        assert_eq!(profile.owners.len(), 2);
        assert_eq!(profile.local_reuse.objects, 4);
        assert_eq!(profile.remote_copy.objects, 4);
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
        eprintln!("sealed owner save: observed_in_flight_owner_calls=2, configured_bound=2, local_reused=4, remote_copied=4, replay_descriptor_exact=true");
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
