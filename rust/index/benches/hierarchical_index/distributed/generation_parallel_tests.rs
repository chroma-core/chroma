use super::super::super::{
    generation_backend::{ConditionalObjects, ObjectBytes},
    routing::{NodeRef, RoutingNode},
    saved_reader::{save_owner_snapshot_reusing, SavedPostingList},
};
use super::super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[derive(Default)]
struct Overlap {
    arrivals: parking_lot::Mutex<usize>,
    changed: parking_lot::Condvar,
    active: AtomicUsize,
    peak: AtomicUsize,
}
impl Overlap {
    fn run<T>(&self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(active, Ordering::SeqCst);
        let result = (|| {
            let mut arrivals = self.arrivals.lock();
            *arrivals += 1;
            self.changed.notify_all();
            let deadline = Instant::now() + Duration::from_secs(5);
            while *arrivals < 2 {
                if self.changed.wait_until(&mut arrivals, deadline).timed_out() && *arrivals < 2 {
                    return Err(invalid("block operations did not overlap"));
                }
            }
            drop(arrivals);
            operation()
        })();
        self.active.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

#[derive(Default)]
struct Backend {
    objects: parking_lot::Mutex<BTreeMap<String, Vec<u8>>>,
    uploads: Overlap,
    verification: Overlap,
    fail_upload: AtomicBool,
    corrupt_read: AtomicBool,
}
impl ConditionalObjects for Backend {
    fn put(&self, key: &str, bytes: &[u8], _: Option<&str>) -> Result<String> {
        let write = || {
            if key.starts_with("owners/") {
                assert_eq!(self.uploads.active.load(Ordering::SeqCst), 0);
                assert_eq!(self.verification.active.load(Ordering::SeqCst), 0);
            }
            if key.starts_with("objects/postings-") && self.fail_upload.load(Ordering::SeqCst) {
                return Err(invalid("injected upload failure"));
            }
            let mut objects = self.objects.lock();
            if objects.contains_key(key) {
                return Err(GenerationError::Conflict);
            }
            objects.insert(key.into(), bytes.to_vec());
            Ok("etag".into())
        };
        if key.starts_with("objects/postings-") {
            self.uploads.run(write)
        } else {
            write()
        }
    }
    fn get(&self, key: &str) -> Result<Option<ObjectBytes>> {
        let read = || {
            Ok(self.objects.lock().get(key).map(|bytes| {
                let mut bytes = bytes.clone();
                if key.starts_with("objects/postings-") && self.corrupt_read.load(Ordering::SeqCst)
                {
                    bytes[0] ^= 1;
                }
                ObjectBytes {
                    bytes,
                    etag: "etag".into(),
                }
            }))
        };
        if key.starts_with("objects/postings-") {
            self.verification.run(read)
        } else {
            read()
        }
    }
}

#[test]
fn parallel_blocks_join_before_manifest_and_preserve_reuse_and_failure_checks() {
    use chroma_index::quantization::Code;
    for failure in ["none", "upload", "verification"] {
        let backend = Arc::new(Backend::default());
        backend
            .fail_upload
            .store(failure == "upload", Ordering::SeqCst);
        backend
            .corrupt_read
            .store(failure == "verification", Ordering::SeqCst);
        let store = GenerationStore {
            root: PathBuf::new(),
            backend: Some(backend.clone()),
            io: Arc::new(Mutex::new(Default::default())),
        };
        let vectors = store.put_object("vectors", b"independent vectors").unwrap();
        let nodes: Vec<_> = (1..=96)
            .map(|id| RoutingNode {
                navigation: Default::default(),
                target: NodeRef {
                    id,
                    authority: 1,
                    placement: 0,
                },
                parent: None,
                centroid: vec![1.0; 2],
                centroid_code: vec![],
                children: vec![],
                leaf: true,
                revision: 1,
            })
            .collect();
        let postings: Vec<_> = nodes
            .iter()
            .map(|node| SavedPostingList {
                leaf: node.target.id,
                ids: vec![node.target.id as u64],
                codes: Code::<1>::quantize(&[1.0; 2], &[1.0; 2]).as_ref().to_vec(),
                centroid: vec![],
            })
            .collect();
        let save = |attempt: &str, postings: &[SavedPostingList], reused: &BTreeMap<_, _>| {
            save_owner_snapshot_reusing(
                &store,
                "run",
                attempt,
                attempt,
                1,
                nodes.iter().map(|node| node.target.id).collect(),
                FormatContract {
                    version: 1,
                    dimensions: 2,
                    metric: "l2".into(),
                    centroid_bits: 4,
                    posting_bits: 1,
                    vector_bits: 32,
                },
                &nodes,
                postings,
                vec![vectors.clone()],
                None,
                reused,
            )
        };
        let result = save("first", &postings, &BTreeMap::new());
        assert_eq!(backend.uploads.active.load(Ordering::SeqCst), 0);
        assert_eq!(backend.verification.active.load(Ordering::SeqCst), 0);
        assert!((2..=32).contains(&backend.uploads.peak.load(Ordering::SeqCst)));
        if failure != "none" {
            assert!(result.is_err(), "{failure} must fail the save");
            assert!(!backend
                .objects
                .lock()
                .keys()
                .any(|k| k.starts_with("owners/")));
            if failure == "verification" {
                assert!(result.unwrap_err().contains("integrity mismatch"));
            }
            continue;
        }
        let (manifest, blocks) = result.unwrap();
        assert!((2..=32).contains(&backend.verification.peak.load(Ordering::SeqCst)));
        assert_eq!(*backend.uploads.arrivals.lock(), 96);
        assert_eq!(*backend.verification.arrivals.lock(), 96);
        let owner: OwnerManifest =
            serde_json::from_slice(&store.read_object(&manifest).unwrap()).unwrap();
        assert_eq!(owner.vector_count, 96);
        assert_eq!(
            owner.postings,
            blocks
                .values()
                .map(|b| b.object.clone())
                .collect::<Vec<_>>()
        );
        let (_, reused) = save("unchanged", &[], &blocks).unwrap();
        assert_eq!(*backend.uploads.arrivals.lock(), 96);
        assert_eq!(*backend.verification.arrivals.lock(), 96);
        assert_eq!(
            reused.values().map(|b| &b.object).collect::<Vec<_>>(),
            blocks.values().map(|b| &b.object).collect::<Vec<_>>()
        );
        let (replay, _) = save("first", &postings, &BTreeMap::new()).unwrap();
        assert_eq!(replay, manifest);
    }
}

#[test]
fn saving_worker_count_has_bounded_default_and_explicit_errors() {
    assert_eq!(super::save_worker_count(None).unwrap(), 32);
    assert_eq!(super::save_worker_count(Some("1")).unwrap(), 1);
    assert_eq!(super::save_worker_count(Some("32")).unwrap(), 32);
    for invalid in ["0", "33", "-1", "", "lots"] {
        assert!(super::save_worker_count(Some(invalid)).is_err());
    }
}
