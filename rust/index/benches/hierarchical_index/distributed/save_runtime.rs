//! Freeze actual owner records locally, copy only immutable saved objects, then
//! publish the combined generation. Construction posting payloads never travel
//! through an unbounded whole-index Snapshot response.
use super::generation_backend::DurableStoreConfig;
use super::{
    engine::{Cluster, EngineRequest, EngineResponse, OwnerService},
    generation::{
        FormatContract, GenerationDescriptor, GenerationManifest, GenerationStore, ObjectRef,
        OwnerManifest, RootCoverage, StoreIoProfile,
    },
    routing::RoutingNode,
    saved_reader::{
        save_owner_snapshot_reusing, save_shared_routing, SavedPostingBlock, SavedPostingList,
        VectorFileDescriptor,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::{Read, Seek, SeekFrom},
    path::{Component, Path, PathBuf},
    sync::atomic::Ordering,
    time::Instant,
};
type Result<T> = std::result::Result<T, String>;
pub const OBJECT_CHUNK_BYTES: usize = 1024 * 1024;
// Bound simultaneous sealed snapshots, durable owner writers and RPC calls.
// Object copying stays serial after every owner materialization has joined.
const MAX_SAVE_OWNER_CONCURRENCY: usize = 2;
/// One budget covers every owner batch and publication; socket expiry does not
/// cancel synchronous storage calls. The process supervisor remains the hard stop.
/// Saving an owner's full-vector store moves gigabytes, so the budget allows
/// for it.
/// SPANN_SAVE_OPERATION_SECONDS overrides the 1,800-second default; a 100M
/// save that writes both owners' vector stores takes longer.
pub fn save_operation_timeout() -> std::time::Duration {
    std::time::Duration::from_secs(
        std::env::var("SPANN_SAVE_OPERATION_SECONDS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(1800),
    )
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaveOwnerRequest {
    pub run: u128,
    pub epoch: u64,
    pub save_attempt: String,
    pub generation: String,
    pub store_root: String,
    #[serde(default)]
    pub durable_store: Option<DurableStoreConfig>,
    pub roots: Vec<u128>,
    pub shared_nodes: Vec<u128>,
    pub format: FormatContract,
    pub vectors: VectorFileDescriptor,
    /// Also save this owner's full vectors as a blockfile in the same store,
    /// keyed by leaf and posting position, and reference it from the owner
    /// routing object. Set from the coordinator's `Cluster::save_vector_store`.
    #[serde(default)]
    pub vector_store: bool,
}
/// Only metadata and immutable references are retained, never posting payloads.
#[derive(Clone)]
pub(super) struct SavedResident {
    pub resident: std::sync::Weak<parking_lot::RwLock<super::engine::Resident>>,
    pub routing: std::sync::Arc<RoutingNode>,
    pub posting: SavedPostingBlock,
}
pub(super) struct OwnerSaveCache {
    store_root: String,
    durable_store: Option<DurableStoreConfig>,
    format: FormatContract,
    pub nodes: BTreeMap<u128, SavedResident>,
}
pub(super) struct SaveSnapshotNode {
    pub node: super::engine::StoredNode,
    pub resident: std::sync::Weak<parking_lot::RwLock<super::engine::Resident>>,
    pub routing: std::sync::Arc<RoutingNode>,
    pub reused: Option<SavedPostingBlock>,
}
/// Coarse exclusive owner wall phases. Materialization includes serialization,
/// durable writes and their validation, excluding the measured snapshot copy.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OwnerSaveProfile {
    pub snapshot_nanos: u64,
    #[serde(default)]
    pub posting_blocks_reused: u64,
    #[serde(default)]
    pub posting_blocks_materialized: u64,
    #[serde(default)]
    pub posting_vectors_scanned: u64,
    pub materialize_nanos: u64,
    pub store_io: StoreIoProfile,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CopyProfile {
    pub objects: u64,
    pub bytes: u64,
    pub elapsed_nanos: u64,
}
/// Save phases are wall times; nested store synchronization times must not be
/// added to them. Profiles belong to this invocation, including replay writes.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SaveProfile {
    pub close_seal_nanos: u64,
    /// Sum of individual owner RPC/local call wall times, including overlap.
    pub owner_calls_nanos: u64,
    /// Elapsed wall time across the joined owner-materialization barrier.
    #[serde(default)]
    pub owner_materialization_wall_nanos: u64,
    /// Admission bound, not a claim about observed CPU or IO concurrency.
    #[serde(default)]
    pub owner_materialization_max_concurrency: usize,
    pub local_copy: CopyProfile,
    /// Logical immutable objects verified in place, including the owner manifest.
    /// Bytes are verified payload size, not newly written bytes or network work.
    #[serde(default)]
    pub local_reuse: CopyProfile,
    pub remote_copy: CopyProfile,
    /// Owner-acknowledged durable objects; no payload relay to coordinator.
    #[serde(default)]
    pub direct_durable: CopyProfile,
    pub final_manifest_nanos: u64,
    pub publication_nanos: u64,
    pub coordinator_store_io: StoreIoProfile,
    pub owners: BTreeMap<u64, OwnerSaveProfile>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerSaveReceipt {
    pub owner: u64,
    pub manifest: ObjectRef,
    pub objects: Vec<ObjectRef>,
    #[serde(default)]
    pub profile: OwnerSaveProfile,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadSavedObjectRequest {
    pub run: u128,
    pub epoch: u64,
    pub store_root: String,
    pub object: ObjectRef,
    pub offset: u64,
    pub max_bytes: usize,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedObjectChunk {
    pub object: ObjectRef,
    pub offset: u64,
    // Private RPC wire change: matching endpoint binaries are required. The
    // explicit key makes legacy integer-array peers fail closed on decoding.
    #[serde(rename = "bytes_base64", with = "saved_chunk_base64")]
    pub bytes: Vec<u8>,
    pub complete: bool,
}
mod saved_chunk_base64 {
    use super::OBJECT_CHUNK_BYTES;
    use base64::{prelude::BASE64_STANDARD, Engine};
    use serde::{
        de::{Error, Visitor},
        Deserializer, Serializer,
    };
    const MAX_ENCODED: usize = OBJECT_CHUNK_BYTES.div_ceil(3) * 4;
    pub fn serialize<S: Serializer>(
        bytes: &[u8],
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        if bytes.len() > OBJECT_CHUNK_BYTES {
            return Err(serde::ser::Error::custom(
                "saved chunk exceeds decoded byte limit",
            ));
        }
        serializer.serialize_str(&BASE64_STANDARD.encode(bytes))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Vec<u8>, D::Error> {
        struct ChunkVisitor;
        impl Visitor<'_> for ChunkVisitor {
            type Value = Vec<u8>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a bounded canonical padded base64 saved-object chunk")
            }
            fn visit_str<E: Error>(self, encoded: &str) -> std::result::Result<Vec<u8>, E> {
                // Bound the decoder's allocation first. Padding means max+1
                // raw bytes can share max's encoded length, so check both sizes.
                if encoded.len() > MAX_ENCODED {
                    return Err(E::custom("saved chunk exceeds encoded byte limit"));
                }
                let bytes = BASE64_STANDARD.decode(encoded).map_err(E::custom)?;
                if bytes.len() > OBJECT_CHUNK_BYTES {
                    return Err(E::custom("saved chunk exceeds decoded byte limit"));
                }
                Ok(bytes)
            }
        }
        deserializer.deserialize_str(ChunkVisitor)
    }
}

impl OwnerService {
    /// Write this owner's full vectors, leaf by leaf in ascending leaf order
    /// and in each leaf's saved posting order, so a reader finds a vector by
    /// its leaf and posting position. `SPANN_VECTOR_STORE_WRITERS` writers
    /// write contiguous leaf ranges in parallel, each reading its vectors as
    /// it reaches them.
    fn save_vector_store(
        &self,
        request: &SaveOwnerRequest,
        postings: &[SavedPostingList],
        reused: &BTreeMap<u128, SavedPostingBlock>,
    ) -> Result<super::vector_store::VectorStoreRef> {
        use super::vector_store::{
            vector_store_writers, write_owner_vectors_parallel, VectorStoreLocation,
            DEFAULT_VECTOR_BLOCK_SIZE_BYTES,
        };
        if !reused.is_empty() {
            return Err("a vector store needs every posting list; this save reuses unchanged blocks".into());
        }
        let location = match &request.durable_store {
            Some(config) => VectorStoreLocation::Durable(config.clone()),
            None => VectorStoreLocation::Local(PathBuf::from(&request.store_root)),
        };
        let mut leaves: Vec<(u128, &[u64])> = postings.iter().map(|p| (p.leaf, &p.ids[..])).collect();
        leaves.sort_by_key(|(leaf, _)| *leaf);
        let source = &self.vectors;
        let vectors = |id: u64| source.get(id);
        write_owner_vectors_parallel(
            &location,
            self.owner,
            self.dim,
            DEFAULT_VECTOR_BLOCK_SIZE_BYTES,
            vector_store_writers()?,
            &leaves,
            &vectors,
        )
    }
    pub(super) fn save_owner_runtime(
        &self,
        mut request: SaveOwnerRequest,
    ) -> Result<OwnerSaveReceipt> {
        self.ensure_save_quiescent(request.run, request.epoch)?;
        #[cfg(test)]
        save_concurrency_tests::owner_hook(request.run, self.owner, false)?;
        if request.format.dimensions != self.dim
            || request.format.metric
                != match self.metric {
                    chroma_distance::DistanceFunction::Euclidean => "l2",
                    chroma_distance::DistanceFunction::Cosine => "cosine",
                    chroma_distance::DistanceFunction::InnerProduct => "ip",
                }
        {
            return Err("save format differs from owner construction".into());
        }
        if request.vectors.dimensions != self.dim {
            return Err("save vector descriptor dimension mismatch".into());
        }
        request.roots.sort_unstable();
        request.shared_nodes.sort_unstable();
        // Bind the complete frozen body independently of the storage path. A
        // second namespace must not let the same save identity acquire a new
        // root set, corpus descriptor, generation or destination.
        {
            let mut saves = self.save_requests.lock();
            if let Some(old) = saves.get(&request.save_attempt) {
                if old != &request {
                    return Err("conflicting frozen SaveOwner request".into());
                }
            } else {
                if saves.len() >= 1024 {
                    return Err("retained save identity bound reached".into());
                }
                saves.insert(request.save_attempt.clone(), request.clone());
            }
        }
        let materialize = Instant::now();
        let store = match &request.durable_store {
            Some(config) => GenerationStore::open_durable(config),
            None => GenerationStore::open(&request.store_root),
        }
        .map_err(|e| e.to_string())?;
        // This immutable identity has already been checked by the input adapter
        // before construction. A fresh reader checks the real file again.
        let vector_bytes = serde_json::to_vec(&request.vectors).map_err(|e| e.to_string())?;
        use sha2::{Digest, Sha256};
        let vector_ref = store
            .put_object(
                &format!("vectors-{:x}", Sha256::digest(&vector_bytes)),
                &vector_bytes,
            )
            .map_err(|e| e.to_string())?;
        // Serialize saves per owner and retain cache only after an owner manifest
        // succeeds. A later combined-publication failure cannot invalidate these
        // immutable objects or the preceding reader generation.
        let mut cache = self.save_cache.lock();
        let previous = cache.as_ref().filter(|cache| {
            cache.store_root == request.store_root
                && cache.durable_store == request.durable_store
                && cache.format == request.format
        });
        let empty = BTreeMap::new();
        let snapshot_start = Instant::now();
        let (mut snapshot, posting_vectors_scanned) = self.save_quiescent_snapshot(
            request.run,
            request.epoch,
            previous.map(|cache| &cache.nodes).unwrap_or(&empty),
        )?;
        let snapshot_nanos = snapshot_start.elapsed().as_nanos() as u64;
        let shared = request
            .shared_nodes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut routing = vec![];
        let mut postings = vec![];
        let mut reused = BTreeMap::new();
        for saved in &mut snapshot {
            let node = &mut saved.node;
            if shared.contains(&node.routing.target.id) && !node.routing.leaf {
                continue;
            }
            if let Some(block) = &saved.reused {
                reused.insert(node.routing.target.id, block.clone());
            }
            if let Some(p) = node.postings.take() {
                // Saving takes the leaf centroid from its routing node.
                postings.push(SavedPostingList {
                    leaf: node.routing.target.id,
                    ids: p.ids,
                    codes: p.codes,
                    centroid: vec![],
                });
            }
            routing.push(node.routing.clone());
        }
        request.roots.sort_unstable();
        let vector_store = if request.vector_store {
            Some(self.save_vector_store(&request, &postings, &reused)?)
        } else {
            None
        };
        let (manifest, blocks) = save_owner_snapshot_reusing(
            &store,
            &request.run.to_string(),
            &request.save_attempt,
            &request.generation,
            self.owner,
            request.roots.clone(),
            request.format.clone(),
            &routing,
            &postings,
            vec![vector_ref],
            vector_store,
            &reused,
        )?;
        *cache = Some(OwnerSaveCache {
            store_root: request.store_root.clone(),
            durable_store: request.durable_store.clone(),
            format: request.format.clone(),
            nodes: snapshot
                .iter()
                .filter_map(|saved| {
                    blocks.get(&saved.node.routing.target.id).map(|posting| {
                        (
                            saved.node.routing.target.id,
                            SavedResident {
                                resident: saved.resident.clone(),
                                routing: saved.routing.clone(),
                                posting: posting.clone(),
                            },
                        )
                    })
                })
                .collect(),
        });
        let owner: OwnerManifest =
            serde_json::from_slice(&store.read_object(&manifest).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        let mut objects = BTreeMap::new();
        for r in owner.objects() {
            objects.insert(r.path.clone(), r.clone());
        }
        #[cfg(test)]
        save_concurrency_tests::owner_hook(request.run, self.owner, true)?;
        Ok(OwnerSaveReceipt {
            owner: self.owner,
            manifest,
            objects: objects.into_values().collect(),
            profile: OwnerSaveProfile {
                snapshot_nanos,
                posting_blocks_reused: reused.len() as u64,
                posting_blocks_materialized: postings.len() as u64,
                posting_vectors_scanned,
                materialize_nanos: (materialize.elapsed().as_nanos() as u64)
                    .saturating_sub(snapshot_nanos),
                store_io: store.io_profile(),
            },
        })
    }
    pub(super) fn read_saved_object(&self, r: ReadSavedObjectRequest) -> Result<SavedObjectChunk> {
        self.ensure_save_quiescent(r.run, r.epoch)?;
        if r.max_bytes == 0 || r.max_bytes > OBJECT_CHUNK_BYTES || r.offset > r.object.bytes {
            return Err("invalid saved object chunk bounds".into());
        }
        if !Path::new(&r.object.path)
            .components()
            .all(|c| matches!(c, Component::Normal(_)))
        {
            return Err("invalid saved object path".into());
        }
        let mut file = std::fs::File::open(Path::new(&r.store_root).join(&r.object.path))
            .map_err(|e| e.to_string())?;
        if file.metadata().map_err(|e| e.to_string())?.len() != r.object.bytes {
            return Err("saved object size changed".into());
        }
        let count = (r.object.bytes - r.offset).min(r.max_bytes as u64) as usize;
        let mut bytes = vec![0; count];
        file.seek(SeekFrom::Start(r.offset))
            .map_err(|e| e.to_string())?;
        file.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        Ok(SavedObjectChunk {
            complete: r.offset + count as u64 == r.object.bytes,
            object: r.object,
            offset: r.offset,
            bytes,
        })
    }
}
fn remote_bytes(
    cluster: &Cluster,
    owner: u64,
    root: &str,
    reference: &ObjectRef,
    run: u128,
    epoch: u64,
) -> Result<Vec<u8>> {
    let capacity =
        usize::try_from(reference.bytes).map_err(|_| "saved object exceeds address space")?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(|e| e.to_string())?;
    while bytes.len() < capacity {
        let offset = bytes.len() as u64;
        let reply = match cluster.request(
            owner,
            EngineRequest::ReadSavedObject(ReadSavedObjectRequest {
                run,
                epoch,
                store_root: root.into(),
                object: reference.clone(),
                offset,
                max_bytes: OBJECT_CHUNK_BYTES,
            }),
        )? {
            EngineResponse::SavedObject(c) => c,
            _ => return Err("invalid saved object response".into()),
        };
        if reply.object != *reference
            || reply.offset != offset
            || reply.bytes.is_empty()
            || reply.bytes.len() > OBJECT_CHUNK_BYTES
            || reply.bytes.len() > capacity - bytes.len()
            || reply.complete != (offset + reply.bytes.len() as u64 == reference.bytes)
        {
            return Err("incompatible saved object chunk".into());
        }
        bytes.extend(reply.bytes);
    }
    Ok(bytes)
}
// Keep the existing whole-object integrity boundary after chunk decoding and
// reassembly. A valid base64 string alone does not establish object integrity.
fn copy_saved_object(
    store: &GenerationStore,
    object: &ObjectRef,
    bytes: &[u8],
) -> Result<ObjectRef> {
    let name = object
        .path
        .strip_prefix("objects/")
        .ok_or("owner payload outside immutable object namespace")?;
    let copied = store.put_object(name, bytes).map_err(|e| e.to_string())?;
    if copied != *object {
        return Err("copied owner payload integrity mismatch".into());
    }
    Ok(copied)
}
impl Cluster {
    /// Call after construction workers join. Closing blocks new attempts;
    /// sealing fails if any accepted transfer, boundary edit or handoff remains.
    /// Each owner writes into its own local root. The coordinator copies the
    /// immutable objects and publishes one combined descriptor last.
    pub fn save_completed(
        &self,
        store_path: &Path,
        vectors: VectorFileDescriptor,
        generation: &str,
        save_attempt: &str,
    ) -> Result<GenerationDescriptor> {
        self.save_completed_profiled(store_path, vectors, generation, save_attempt)
            .map(|(descriptor, _)| descriptor)
    }
    pub fn save_completed_profiled(
        &self,
        store_path: &Path,
        vectors: VectorFileDescriptor,
        generation: &str,
        save_attempt: &str,
    ) -> Result<(GenerationDescriptor, SaveProfile)> {
        self.save_completed_target(store_path, None, vectors, generation, save_attempt)
    }
    pub fn save_completed_durable_profiled(
        &self,
        export_path: &Path,
        config: &DurableStoreConfig,
        vectors: VectorFileDescriptor,
        generation: &str,
        save_attempt: &str,
    ) -> Result<(GenerationDescriptor, SaveProfile)> {
        config.validate().map_err(|e| e.to_string())?;
        self.save_completed_target(export_path, Some(config), vectors, generation, save_attempt)
    }
    fn save_completed_target(
        &self,
        store_path: &Path,
        durable: Option<&DurableStoreConfig>,
        vectors: VectorFileDescriptor,
        generation: &str,
        save_attempt: &str,
    ) -> Result<(GenerationDescriptor, SaveProfile)> {
        let save_deadline = (Instant::now() + save_operation_timeout()).min(
            self.shutdown_deadline
                .unwrap_or(Instant::now() + save_operation_timeout()),
        );
        let check_save_deadline = || -> Result<()> {
            if Instant::now() >= save_deadline {
                return Err("save operation deadline expired".into());
            }
            Ok(())
        };
        check_save_deadline()?;
        let mut profile = SaveProfile::default();
        let close_seal = Instant::now();
        let run = self.local.run;
        let epoch = self.local.epoch.load(Ordering::Acquire);
        let mut owners = self.peers.keys().copied().collect::<Vec<_>>();
        owners.push(self.local.owner);
        owners.sort_unstable();
        owners.dedup();
        for owner in &owners {
            match self.request(*owner, EngineRequest::CloseEpoch { run, epoch })? {
                EngineResponse::Ack => {}
                _ => return Err("invalid close acknowledgement".into()),
            }
        }
        for owner in &owners {
            match self.request(*owner, EngineRequest::SealEpoch { run, epoch })? {
                EngineResponse::Ack => {}
                _ => return Err("invalid seal acknowledgement".into()),
            }
        }
        profile.close_seal_nanos = close_seal.elapsed().as_nanos() as u64;
        let view = self.publication.view();
        let mut shared = vec![view.value.root.clone()];
        // Before placement the whole tree is one unit whose root is the global
        // root, and later splits under it never republish the shared view. Save
        // the owner's current root then, or its saved children are stale.
        let root = view.value.root.target;
        if root.authority == self.local.owner
            && view.value.units.values().any(|u| u.id == root.id)
        {
            if let Ok(resident) = self.local.resident(root) {
                shared[0] = (*resident.read().routing).clone();
            }
        }
        for n in view.value.shared.values() {
            if n.target.id != shared[0].target.id {
                shared.push(n.clone())
            }
        }
        let roots = view.value.units.values().copied().collect::<Vec<_>>();
        let expected_roots = roots
            .iter()
            .map(|r| RootCoverage {
                root: r.id,
                owner: r.authority,
            })
            .collect::<Vec<_>>();
        let format = FormatContract {
            version: 1,
            dimensions: self.local.dim,
            metric: match self.local.metric {
                chroma_distance::DistanceFunction::Euclidean => "l2",
                chroma_distance::DistanceFunction::Cosine => "cosine",
                chroma_distance::DistanceFunction::InnerProduct => "ip",
            }
            .into(),
            centroid_bits: 4,
            posting_bits: 1,
            vector_bits: 32,
        };
        let store = match durable {
            Some(config) => {
                std::fs::create_dir_all(store_path).map_err(|e| e.to_string())?;
                GenerationStore::open_durable(config)
            }
            None => GenerationStore::open(store_path),
        }
        .map_err(|e| e.to_string())?;
        let current_latest = store.latest().map_err(|e| e.to_string())?;
        let publication_identity = serde_json::to_string(&(store_path, durable, run, save_attempt))
            .map_err(|e| e.to_string())?;
        let previous_generation = {
            let mut predecessors = self.save_predecessors.lock();
            if let Some(previous) = predecessors.get(&publication_identity) {
                previous.clone()
            } else {
                if predecessors.len() >= 1024 {
                    return Err("retained save publication identity bound reached".into());
                }
                let previous = current_latest
                    .as_ref()
                    .map(|descriptor| descriptor.generation.clone());
                predecessors.insert(publication_identity, previous.clone());
                previous
            }
        };
        let mut owner_refs = vec![];
        let mut vector_refs: BTreeMap<String, ObjectRef> = BTreeMap::new();
        let mut total = 0u64;
        let mut requests = vec![];
        for owner in owners {
            let owned = roots
                .iter()
                .filter(|r| r.authority == owner)
                .map(|r| r.id)
                .collect::<Vec<_>>();
            if owned.is_empty() {
                continue;
            }
            // Local owner objects already have their final immutable namespace.
            // Remote owners retain their staging store and bounded copy protocol.
            let owner_store = if durable.is_some() || owner == self.local.owner {
                store_path.to_path_buf()
            } else {
                store_path.join(format!("owner-{owner}"))
            };
            let remote_root = owner_store
                .to_str()
                .ok_or("save path is not UTF-8")?
                .to_string();
            requests.push((
                owner,
                remote_root.clone(),
                SaveOwnerRequest {
                    run,
                    epoch,
                    save_attempt: save_attempt.into(),
                    generation: generation.into(),
                    store_root: remote_root,
                    durable_store: durable.cloned(),
                    roots: owned,
                    shared_nodes: shared.iter().map(|n| n.target.id).collect(),
                    format: format.clone(),
                    vectors: vectors.clone(),
                    vector_store: self.save_vector_store.load(Ordering::Relaxed),
                },
            ));
        }
        // All owners are sealed and all requests bind the same frozen view.
        // Each writes its own namespace. No copying or combined publication
        // begins until every started task has joined successfully.
        let materialization = Instant::now();
        let mut completed = Vec::with_capacity(requests.len());
        profile.owner_materialization_max_concurrency = MAX_SAVE_OWNER_CONCURRENCY;
        for batch in requests.chunks(MAX_SAVE_OWNER_CONCURRENCY) {
            let results = std::thread::scope(|scope| {
                let handles = batch
                    .iter()
                    .map(|(owner, remote_root, request)| {
                        scope.spawn(move || {
                            let call = Instant::now();
                            let receipt = match self.request_until(
                                *owner,
                                EngineRequest::SaveOwner(request.clone()),
                                save_deadline,
                            )? {
                                EngineResponse::OwnerSaved(receipt) => receipt,
                                _ => return Err("invalid owner save acknowledgement".into()),
                            };
                            if receipt.owner != *owner {
                                return Err("save acknowledgement owner mismatch".into());
                            }
                            Ok((
                                *owner,
                                remote_root.clone(),
                                receipt,
                                call.elapsed().as_nanos() as u64,
                            ))
                        })
                    })
                    .collect::<Vec<_>>();
                // Join ALL started writers even when another owner fails.
                handles
                    .into_iter()
                    .map(|handle| {
                        handle
                            .join()
                            .unwrap_or_else(|_| Err("owner materialization worker panicked".into()))
                    })
                    .collect::<Vec<Result<_>>>()
            });
            check_save_deadline()?;
            // Inspect failures only after the entire batch has joined. Returning
            // here leaves immutable evidence but creates no combined descriptor.
            for result in results {
                completed.push(result?);
            }
        }
        profile.owner_materialization_wall_nanos = materialization.elapsed().as_nanos() as u64;
        for (owner, remote_root, receipt, call_nanos) in completed {
            profile.owner_calls_nanos += call_nanos;
            profile.owners.insert(owner, receipt.profile.clone());
            let copying = Instant::now();
            let manifest: OwnerManifest = if durable.is_some() {
                store
                    .verify_saved_owner_metadata(&receipt.manifest)
                    .map_err(|e| e.to_string())?
            } else if owner == self.local.owner {
                store
                    .verify_saved_owner(&receipt.manifest)
                    .map_err(|e| e.to_string())?
            } else {
                for object in &receipt.objects {
                    let bytes = remote_bytes(self, owner, &remote_root, object, run, epoch)?;
                    copy_saved_object(&store, object, &bytes)?;
                    profile.remote_copy.objects += 1;
                    profile.remote_copy.bytes += object.bytes;
                }
                let bytes = remote_bytes(self, owner, &remote_root, &receipt.manifest, run, epoch)?;
                serde_json::from_slice(&bytes).map_err(|e| e.to_string())?
            };
            if manifest.owner != owner
                || manifest.generation != generation
                || manifest.save_attempt != save_attempt
                || manifest.run != run.to_string()
            {
                return Err("owner manifest save identity mismatch".into());
            }
            total = total
                .checked_add(manifest.vector_count)
                .ok_or("saved vector total overflow")?;
            for r in &manifest.full_vectors {
                vector_refs.insert(r.path.clone(), r.clone());
            }
            let saved = if durable.is_some() || owner == self.local.owner {
                let objects = manifest
                    .objects()
                    .map(|object| (object.path.clone(), object.clone()))
                    .collect::<BTreeMap<_, _>>();
                if objects.into_values().collect::<Vec<_>>() != receipt.objects {
                    return Err(
                        "local save receipt differs from verified immutable references".into(),
                    );
                }
                let reuse = if durable.is_some() {
                    &mut profile.direct_durable
                } else {
                    &mut profile.local_reuse
                };
                reuse.objects += receipt.objects.len() as u64 + 1;
                reuse.bytes += receipt
                    .objects
                    .iter()
                    .map(|object| object.bytes)
                    .sum::<u64>()
                    + receipt.manifest.bytes;
                reuse.elapsed_nanos += copying.elapsed().as_nanos() as u64;
                receipt.manifest.clone()
            } else {
                let saved = store.save_owner(&manifest).map_err(|e| e.to_string())?;
                if saved != receipt.manifest {
                    return Err("owner manifest integrity mismatch".into());
                }
                profile.remote_copy.objects += 1;
                profile.remote_copy.bytes += saved.bytes;
                profile.remote_copy.elapsed_nanos += copying.elapsed().as_nanos() as u64;
                saved
            };
            owner_refs.push(saved);
        }
        let final_manifest = Instant::now();
        let shared_routing = save_shared_routing(&store, &format!("shared-{generation}"), &shared)?;
        let manifest = GenerationManifest {
            run: run.to_string(),
            save_attempt: save_attempt.into(),
            generation: generation.into(),
            format,
            expected_roots,
            owners: owner_refs,
            shared_routing,
            full_vectors: vector_refs.into_values().collect(),
            total_vectors: total,
        };
        let descriptor = store
            .save_generation(&manifest)
            .map_err(|e| e.to_string())?;
        profile.final_manifest_nanos = final_manifest.elapsed().as_nanos() as u64;
        let publication = Instant::now();
        check_save_deadline()?;
        store
            .publish_latest(&descriptor, previous_generation.as_deref())
            .map_err(|e| e.to_string())?;
        std::fs::write(
            store_path.join("descriptor.json"),
            serde_json::to_vec_pretty(&descriptor).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        profile.publication_nanos = publication.elapsed().as_nanos() as u64;
        profile.coordinator_store_io = store.io_profile();
        Ok((descriptor, profile))
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        engine::{PostingList, StoredNode},
        routing::NodeRef,
        saved_reader::{CanonicalVectorSource, SavedReader, VectorSource},
    };
    use super::*;
    use chroma_distance::DistanceFunction;
    use chroma_index::quantization::Code;
    use std::sync::Arc;
        #[test]
    fn owner_save_writes_a_vector_store_that_matches_the_corpus() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 17;
        let mut bytes = vec![];
        for value in [0.2f32, 0.8] {
            for _ in 0..dim {
                bytes.extend_from_slice(&value.to_le_bytes())
            }
        }
        let path = dir.path().join("vectors.f32");
        std::fs::write(&path, &bytes).unwrap();
        use sha2::{Digest, Sha256};
        let vectors = VectorFileDescriptor {
            path: path.to_str().unwrap().into(),
            rows: 2,
            dimensions: dim,
            stride_bytes: dim as u64 * 4,
            offset_bytes: 0,
            first_id: 0,
            file_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            immutable_locators: vec!["fixture:immutable-source".into()],
        };
        let source = Arc::new(
            CanonicalVectorSource::open(vectors.clone())
                .unwrap()
                .preload()
                .unwrap(),
        );
        let owner = Arc::new(OwnerService::new(
            1,
            91,
            dim,
            DistanceFunction::Euclidean,
            super::super::super::config::HierarchicalSpannConfig::default(),
            source.clone(),
        ));
        let centroid = vec![0.5; dim];
        let root = RoutingNode {
            navigation: Default::default(),
            target: NodeRef {
                id: 1u128 << 80,
                authority: 1,
                placement: 0,
            },
            parent: None,
            centroid: centroid.clone(),
            centroid_code: Code::<4>::quantize(&centroid, &vec![0.; dim])
                .as_ref()
                .to_vec(),
            children: vec![],
            leaf: true,
            revision: 4,
        };
        let mut codes = vec![];
        for id in [0, 1] {
            codes.extend_from_slice(
                Code::<1>::quantize(&source.get(id).unwrap(), &centroid).as_ref(),
            )
        }
        owner
            .install_initial(vec![StoredNode {
                routing: root.clone(),
                postings: Some(PostingList {
                    ids: vec![0, 1],
                    codes,
                }),
            }])
            .unwrap();
        let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
        owner.bind_cluster(&cluster);
        cluster
            .save_vector_store
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let save = dir.path().join("with-vectors");
        let descriptor = cluster.save_completed(&save, vectors.clone(), "gen", "save").unwrap();
        let reader = super::super::saved_reader::SavedReader::open(
            GenerationStore::open(&save).unwrap(),
            &descriptor,
        )
        .unwrap();
        let stores = reader.owner_vector_stores();
        let r = stores.get(&1).expect("owner 1 saved a vector store");
        assert_eq!((r.rows, r.dimensions), (2, dim));
        let location = super::super::vector_store::VectorStoreLocation::Local(save.clone());
        let store = super::super::vector_store::VectorStoreReader::open(&location, r, 1 << 20).unwrap();
        let leaf = 1u128 << 80;
        let posting = reader.posting_list(leaf).unwrap();
        let keys: Vec<(u128, u32)> = (0..posting.ids.len() as u32).map(|o| (leaf, o)).collect();
        for (id, stored) in posting.ids.iter().zip(store.get_many(&keys).unwrap()) {
            assert_eq!(&*stored, &*source.get(*id).unwrap());
        }
    }

    #[test]
    fn frozen_owner_save_reopens_real_postings_and_rejects_live_phase() {
        let dir = tempfile::tempdir().unwrap();
        let dim = 17;
        let mut bytes = vec![];
        for value in [0.2f32, 0.8] {
            for _ in 0..dim {
                bytes.extend_from_slice(&value.to_le_bytes())
            }
        }
        let path = dir.path().join("vectors.f32");
        std::fs::write(&path, &bytes).unwrap();
        use sha2::{Digest, Sha256};
        let vectors = VectorFileDescriptor {
            path: path.to_str().unwrap().into(),
            rows: 2,
            dimensions: dim,
            stride_bytes: dim as u64 * 4,
            offset_bytes: 0,
            first_id: 0,
            file_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            immutable_locators: vec!["fixture:immutable-source".into()],
        };
        let source = Arc::new(
            CanonicalVectorSource::open(vectors.clone())
                .unwrap()
                .preload()
                .unwrap(),
        );
        let owner = Arc::new(OwnerService::new(
            1,
            91,
            dim,
            DistanceFunction::Euclidean,
            super::super::super::config::HierarchicalSpannConfig::default(),
            source.clone(),
        ));
        let centroid = vec![0.5; dim];
        let root = RoutingNode {
            navigation: Default::default(),
            target: NodeRef {
                id: 1u128 << 80,
                authority: 1,
                placement: 0,
            },
            parent: None,
            centroid: centroid.clone(),
            centroid_code: Code::<4>::quantize(&centroid, &vec![0.; dim])
                .as_ref()
                .to_vec(),
            children: vec![],
            leaf: true,
            revision: 4,
        };
        let mut codes = vec![];
        for id in [0, 1] {
            codes.extend_from_slice(
                Code::<1>::quantize(&source.get(id).unwrap(), &centroid).as_ref(),
            )
        }
        owner
            .install_initial(vec![StoredNode {
                routing: root.clone(),
                postings: Some(PostingList {
                    ids: vec![0, 1],
                    codes,
                }),
            }])
            .unwrap();
        let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
        owner.bind_cluster(&cluster);
        let request = SaveOwnerRequest {
            run: 91,
            epoch: 0,
            save_attempt: "save".into(),
            generation: "gen".into(),
            store_root: dir.path().join("owner").to_str().unwrap().into(),
            durable_store: None,
            roots: vec![1u128 << 80],
            shared_nodes: vec![1u128 << 80],
            format: FormatContract {
                version: 1,
                dimensions: dim,
                metric: "l2".into(),
                centroid_bits: 4,
                posting_bits: 1,
                vector_bits: 32,
            },
            vectors: vectors.clone(),
            vector_store: false,
        };
        assert!(owner.save_owner_runtime(request.clone()).is_err());
        let save = dir.path().join("complete");
        let descriptor = cluster
            .save_completed(&save, vectors.clone(), "gen", "save")
            .unwrap();
        let owner_root = save.to_str().unwrap().to_string();
        let mut frozen = request.clone();
        frozen.store_root = owner_root;
        assert!(owner.save_owner_runtime(frozen.clone()).is_ok());
        for change in 0..4 {
            let mut conflicting = frozen.clone();
            match change {
                0 => conflicting.roots.push(999),
                1 => {
                    conflicting.store_root =
                        dir.path().join("other-namespace").to_str().unwrap().into()
                }
                2 => conflicting.vectors.sha256 = "0".repeat(64),
                _ => conflicting.generation = "another-generation".into(),
            }
            assert!(owner
                .save_owner_runtime(conflicting)
                .unwrap_err()
                .contains("conflicting frozen"));
        }
        let reader = SavedReader::open(GenerationStore::open(&save).unwrap(), &descriptor).unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            [0, 1].into_iter().collect()
        );
        assert_eq!(reader.validate_codes(&*source).unwrap(), (1, 2));
        assert_eq!(
            cluster
                .save_completed(&save, vectors, "gen", "save")
                .unwrap(),
            descriptor
        );
    }
    #[test]
    fn two_leaf_save_reuses_verified_local_objects_and_reopens_without_writer_state() {
        use super::super::routing::Child;
        use sha2::{Digest, Sha256};
        let dir = tempfile::tempdir().unwrap();
        let dim = 17;
        let bytes = [0.2_f32, 0.3, 0.7, 0.8]
            .into_iter()
            .flat_map(|value| (0..dim).flat_map(move |_| value.to_le_bytes()))
            .collect::<Vec<_>>();
        let path = dir.path().join("vectors.f32");
        std::fs::write(&path, &bytes).unwrap();
        let vectors = VectorFileDescriptor {
            path: path.to_str().unwrap().into(),
            rows: 4,
            dimensions: dim,
            stride_bytes: dim as u64 * 4,
            offset_bytes: 0,
            first_id: 0,
            file_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(&bytes)),
            immutable_locators: vec!["fixture:immutable-four-vectors".into()],
        };
        let source = Arc::new(
            CanonicalVectorSource::open(vectors.clone())
                .unwrap()
                .preload()
                .unwrap(),
        );
        let owner = Arc::new(OwnerService::new(
            1,
            191,
            dim,
            DistanceFunction::Euclidean,
            super::super::super::config::HierarchicalSpannConfig::default(),
            source.clone(),
        ));
        let mut root = owner.initial_root().routing;
        root.centroid.fill(0.5);
        root.centroid_code = Code::<4>::quantize(&root.centroid, &vec![0.; dim])
            .as_ref()
            .to_vec();
        root.leaf = false;
        let mut nodes = vec![];
        for (offset, center, ids) in [(1, 0.25, vec![0, 1]), (2, 0.75, vec![2, 3])] {
            let mut routing = root.clone();
            routing.target.id += offset;
            routing.parent = Some(root.target);
            routing.centroid.fill(center);
            routing.centroid_code = Code::<4>::quantize(&routing.centroid, &vec![0.; dim])
                .as_ref()
                .to_vec();
            routing.leaf = true;
            let mut codes = vec![];
            for id in &ids {
                codes.extend_from_slice(
                    Code::<1>::quantize(&source.get(*id).unwrap(), &routing.centroid).as_ref(),
                );
            }
            root.children.push(Child {
                target: routing.target,
                centroid: routing.centroid.clone(),
                leaf: true,
            });
            // The parent accumulated child rows after this clone; a leaf has none.
            routing.children.clear();
            nodes.push(StoredNode {
                routing,
                postings: Some(PostingList { ids, codes }),
            });
        }
        nodes.push(StoredNode {
            routing: root.clone(),
            postings: None,
        });
        owner.install_initial(nodes).unwrap();
        let cluster = Arc::new(Cluster::new(owner.clone(), BTreeMap::new(), root));
        owner.bind_cluster(&cluster);
        let save = dir.path().join("saved");
        let (descriptor, profile) = cluster
            .save_completed_profiled(&save, vectors.clone(), "generation", "attempt")
            .unwrap();
        // Six payloads (two postings, routing, navigation, validation, vector
        // descriptor) and the owner manifest are written once directly in the
        // final namespace.
        let local = &profile.owners[&1].store_io;
        assert_eq!(
            (local.immutable_calls, local.new_names, local.replayed_names),
            (7, 7, 0)
        );
        assert_eq!((local.file_sync_calls, local.directory_sync_calls), (7, 8));
        assert_eq!(
            (profile.local_copy.objects, profile.local_copy.bytes),
            (0, 0)
        );
        assert_eq!(profile.local_reuse.objects, 7);
        assert_eq!(profile.local_reuse.bytes, local.immutable_payload_bytes);
        assert!(!save.join("owner-1").exists());
        assert_eq!(
            (profile.remote_copy.objects, profile.remote_copy.bytes),
            (0, 0)
        );
        assert_eq!(local.temporary_bytes_written, local.immutable_payload_bytes);
        let combined = &profile.coordinator_store_io;
        // Add shared navigation, validation and routing, frozen attempt,
        // generation manifest, then sync the latest pointer last. Opening each
        // store also syncs its root directory.
        assert_eq!(
            (
                combined.immutable_calls,
                combined.new_names,
                combined.replayed_names
            ),
            (5, 5, 0)
        );
        assert_eq!(
            (combined.file_sync_calls, combined.directory_sync_calls),
            (6, 7)
        );
        let store = GenerationStore::open(&save).unwrap();
        let generation = store.reopen(&descriptor).unwrap();
        let manifest_ref = &generation.owners[0];
        let manifest: OwnerManifest =
            serde_json::from_slice(&store.read_object(manifest_ref).unwrap()).unwrap();
        assert_eq!(store.verify_saved_owner(manifest_ref).unwrap(), manifest);
        let (replayed, replay) = cluster
            .save_completed_profiled(&save, vectors.clone(), "generation", "attempt")
            .unwrap();
        assert_eq!(replayed, descriptor);
        assert_eq!(
            (
                replay.owners[&1].store_io.new_names,
                replay.owners[&1].store_io.replayed_names
            ),
            (0, 5)
        );
        assert_eq!(replay.owners[&1].posting_blocks_reused, 2);
        assert_eq!(replay.owners[&1].posting_blocks_materialized, 0);
        assert_eq!(
            (
                replay.coordinator_store_io.new_names,
                replay.coordinator_store_io.replayed_names
            ),
            (0, 5)
        );
        drop(cluster);
        drop(owner);
        drop(source);
        let reopened =
            SavedReader::open(GenerationStore::open(&save).unwrap(), &descriptor).unwrap();
        let verified_vectors = CanonicalVectorSource::open(vectors)
            .unwrap()
            .preload()
            .unwrap();
        assert_eq!(
            reopened.validate_membership().unwrap(),
            [0, 1, 2, 3].into_iter().collect()
        );
        // The validator checks the shared root and both leaf centroid codes.
        assert_eq!(reopened.validate_codes(&verified_vectors).unwrap(), (3, 4));
    }
    #[test]
    fn mixed_owner_save_reuses_only_local_objects_and_keeps_remote_copy_validation() {
        use super::super::{
            construction::BoundaryView,
            protocol::publication::Publication,
            routing::Child,
            transport::{RpcClient, RpcServer, TransportConfig},
        };
        use sha2::{Digest, Sha256};
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
            immutable_locators: vec!["fixture:mixed-save".into()],
        };
        let source = Arc::new(
            CanonicalVectorSource::open(vectors.clone())
                .unwrap()
                .preload()
                .unwrap(),
        );
        let make_owner = |id| {
            Arc::new(OwnerService::new(
                id,
                292,
                2,
                DistanceFunction::Euclidean,
                super::super::super::config::HierarchicalSpannConfig::default(),
                source.clone(),
            ))
        };
        let one = make_owner(1);
        let two = make_owner(2);
        let mut root = one.initial_root().routing;
        root.leaf = false;
        root.centroid.fill(0.5);
        root.centroid_code = Code::<4>::quantize(&root.centroid, &[0.; 2])
            .as_ref()
            .to_vec();
        let make_leaf = |id, owner, center, vector| {
            let mut routing = root.clone();
            routing.target = NodeRef {
                id,
                authority: owner,
                placement: 0,
            };
            routing.parent = Some(root.target);
            routing.centroid.fill(center);
            routing.centroid_code = Code::<4>::quantize(&routing.centroid, &[0.; 2])
                .as_ref()
                .to_vec();
            routing.leaf = true;
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
        let left = make_leaf(root.target.id + 1, 1, 0.2, 0);
        let right = make_leaf(root.target.id + 2, 2, 0.8, 1);
        root.children = [&left, &right]
            .into_iter()
            .map(|node| Child {
                target: node.routing.target,
                centroid: node.routing.centroid.clone(),
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
        let mut server = RpcServer::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
            "mixed-save".into(),
            TransportConfig::default(),
            Arc::new(move |request: EngineRequest| endpoint.handle(request)),
        )
        .unwrap();
        let client = RpcClient::new(
            server.data_addr(),
            server.control_addr(),
            "mixed-save".into(),
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
        let (descriptor, profile) = cluster
            .save_completed_profiled(&save, vectors.clone(), "mixed", "mixed-attempt")
            .unwrap();
        assert_eq!(
            (profile.local_copy.objects, profile.local_copy.bytes),
            (0, 0)
        );
        assert_eq!(profile.local_reuse.objects, 6);
        assert_eq!(
            profile.local_reuse.bytes,
            profile.owners[&1].store_io.immutable_payload_bytes
        );
        assert_eq!(profile.remote_copy.objects, 6);
        assert_eq!(
            profile.remote_copy.bytes,
            profile.owners[&2].store_io.immutable_payload_bytes
        );
        assert!(!save.join("owner-1").exists());
        assert!(save.join("owner-2").is_dir());
        assert_eq!(profile.coordinator_store_io.immutable_calls, 11);
        assert_eq!(
            profile.coordinator_store_io.replayed_names, 3,
            "the vector descriptor and both leaf-only owners' content-named \
             navigation and validation objects already exist from the local save"
        );
        let store = GenerationStore::open(&save).unwrap();
        let generation = store.reopen(&descriptor).unwrap();
        let remote = generation
            .owners
            .iter()
            .map(|reference| (reference, store.verify_saved_owner(reference).unwrap()))
            .find(|(_, owner)| owner.owner == 2)
            .unwrap();
        for object in remote.1.objects().chain(std::iter::once(remote.0)) {
            assert_eq!(
                std::fs::read(save.join("owner-2").join(&object.path)).unwrap(),
                std::fs::read(save.join(&object.path)).unwrap()
            );
        }
        server.shutdown();
        drop(server);
        drop(cluster);
        drop(one);
        drop(two);
        drop(source);
        let reader = SavedReader::open(GenerationStore::open(&save).unwrap(), &descriptor).unwrap();
        let independent = CanonicalVectorSource::open(vectors)
            .unwrap()
            .preload()
            .unwrap();
        assert_eq!(
            reader.validate_membership().unwrap(),
            [0, 1].into_iter().collect()
        );
        assert_eq!(reader.validate_codes(&independent).unwrap(), (3, 2));
    }
}

#[cfg(test)]
#[path = "saved_chunk_codec_tests.rs"]
mod saved_chunk_codec_tests;

#[cfg(test)]
#[path = "save_concurrency_tests.rs"]
mod save_concurrency_tests;
