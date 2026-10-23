//! Freeze actual owner records locally, copy only immutable saved objects, then
//! publish the combined generation. Construction posting payloads never travel
//! through an unbounded whole-index Snapshot response.
use super::{
    engine::{Cluster, EngineRequest, EngineResponse, OwnerService},
    generation::{
        FormatContract, GenerationDescriptor, GenerationManifest, GenerationStore, ObjectRef,
        OwnerManifest, RootCoverage, StoreIoProfile,
    },
    routing::RoutingNode,
    saved_reader::{
        save_owner_snapshot, save_shared_routing, SavedPostingList, VectorFileDescriptor,
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SaveOwnerRequest {
    pub run: u128,
    pub epoch: u64,
    pub save_attempt: String,
    pub generation: String,
    pub store_root: String,
    pub roots: Vec<u128>,
    pub shared_nodes: Vec<u128>,
    pub format: FormatContract,
    pub vectors: VectorFileDescriptor,
}
/// Coarse exclusive owner wall phases. Materialization includes serialization,
/// durable writes and their validation, excluding the measured snapshot copy.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct OwnerSaveProfile {
    pub snapshot_nanos: u64,
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
    pub owner_calls_nanos: u64,
    pub local_copy: CopyProfile,
    /// Logical immutable objects verified in place, including the owner manifest.
    /// Bytes are verified payload size, not newly written bytes or network work.
    #[serde(default)]
    pub local_reuse: CopyProfile,
    pub remote_copy: CopyProfile,
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
pub struct SavedObjectChunk {
    pub object: ObjectRef,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub complete: bool,
}
impl OwnerService {
    pub(super) fn save_owner_runtime(
        &self,
        mut request: SaveOwnerRequest,
    ) -> Result<OwnerSaveReceipt> {
        self.ensure_save_quiescent(request.run, request.epoch)?;
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
        let store = GenerationStore::open(&request.store_root).map_err(|e| e.to_string())?;
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
        let snapshot_start = Instant::now();
        let snapshot = self.save_quiescent_snapshot(request.run, request.epoch)?;
        let snapshot_nanos = snapshot_start.elapsed().as_nanos() as u64;
        let shared = request
            .shared_nodes
            .iter()
            .copied()
            .collect::<BTreeSet<_>>();
        let mut routing = vec![];
        let mut postings = vec![];
        for node in snapshot {
            if shared.contains(&node.routing.target.id) && !node.routing.leaf {
                continue;
            }
            if let Some(p) = node.postings {
                postings.push(SavedPostingList {
                    leaf: node.routing.target.id,
                    ids: p.ids,
                    codes: p.codes,
                });
            }
            routing.push(node.routing);
        }
        request.roots.sort_unstable();
        let manifest = save_owner_snapshot(
            &store,
            &request.run.to_string(),
            &request.save_attempt,
            &request.generation,
            self.owner,
            request.roots,
            request.format,
            &routing,
            &postings,
            vec![vector_ref],
        )?;
        let owner: OwnerManifest =
            serde_json::from_slice(&store.read_object(&manifest).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        let mut objects = BTreeMap::new();
        for r in owner
            .nodes
            .into_iter()
            .chain(owner.postings)
            .chain(owner.full_vectors)
        {
            objects.insert(r.path.clone(), r);
        }
        Ok(OwnerSaveReceipt {
            owner: self.owner,
            manifest,
            objects: objects.into_values().collect(),
            profile: OwnerSaveProfile {
                snapshot_nanos,
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
        let store = GenerationStore::open(store_path).map_err(|e| e.to_string())?;
        let mut owner_refs = vec![];
        let mut vector_refs: BTreeMap<String, ObjectRef> = BTreeMap::new();
        let mut total = 0u64;
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
            let owner_store = if owner == self.local.owner {
                store_path.to_path_buf()
            } else {
                store_path.join(format!("owner-{owner}"))
            };
            let remote_root = owner_store
                .to_str()
                .ok_or("save path is not UTF-8")?
                .to_string();
            let owner_call = Instant::now();
            let receipt = match self.request(
                owner,
                EngineRequest::SaveOwner(SaveOwnerRequest {
                    run,
                    epoch,
                    save_attempt: save_attempt.into(),
                    generation: generation.into(),
                    store_root: remote_root.clone(),
                    roots: owned,
                    shared_nodes: shared.iter().map(|n| n.target.id).collect(),
                    format: format.clone(),
                    vectors: vectors.clone(),
                }),
            )? {
                EngineResponse::OwnerSaved(r) => r,
                _ => return Err("invalid owner save acknowledgement".into()),
            };
            profile.owner_calls_nanos += owner_call.elapsed().as_nanos() as u64;
            profile.owners.insert(owner, receipt.profile.clone());
            if receipt.owner != owner {
                return Err("save acknowledgement owner mismatch".into());
            }
            let copying = Instant::now();
            let manifest: OwnerManifest = if owner == self.local.owner {
                store
                    .verify_saved_owner(&receipt.manifest)
                    .map_err(|e| e.to_string())?
            } else {
                for object in &receipt.objects {
                    let bytes = remote_bytes(self, owner, &remote_root, object, run, epoch)?;
                    let name = object
                        .path
                        .strip_prefix("objects/")
                        .ok_or("owner payload outside immutable object namespace")?;
                    let copied = store.put_object(name, &bytes).map_err(|e| e.to_string())?;
                    if copied != *object {
                        return Err("copied owner payload integrity mismatch".into());
                    }
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
            let saved = if owner == self.local.owner {
                let objects = manifest
                    .nodes
                    .iter()
                    .chain(&manifest.postings)
                    .chain(&manifest.full_vectors)
                    .map(|object| (object.path.clone(), object.clone()))
                    .collect::<BTreeMap<_, _>>();
                if objects.into_values().collect::<Vec<_>>() != receipt.objects {
                    return Err(
                        "local save receipt differs from verified immutable references".into(),
                    );
                }
                profile.local_reuse.objects += receipt.objects.len() as u64 + 1;
                profile.local_reuse.bytes += receipt
                    .objects
                    .iter()
                    .map(|object| object.bytes)
                    .sum::<u64>()
                    + receipt.manifest.bytes;
                profile.local_reuse.elapsed_nanos += copying.elapsed().as_nanos() as u64;
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
        store
            .publish_latest(&descriptor, None)
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
        // Four payloads (two postings, routing, vector descriptor) and the
        // owner manifest are written once directly in the final namespace.
        let local = &profile.owners[&1].store_io;
        assert_eq!(
            (local.immutable_calls, local.new_names, local.replayed_names),
            (5, 5, 0)
        );
        assert_eq!((local.file_sync_calls, local.directory_sync_calls), (5, 6));
        assert_eq!(
            (profile.local_copy.objects, profile.local_copy.bytes),
            (0, 0)
        );
        assert_eq!(profile.local_reuse.objects, 5);
        assert_eq!(profile.local_reuse.bytes, local.immutable_payload_bytes);
        assert!(!save.join("owner-1").exists());
        assert_eq!(
            (profile.remote_copy.objects, profile.remote_copy.bytes),
            (0, 0)
        );
        assert_eq!(local.temporary_bytes_written, local.immutable_payload_bytes);
        let combined = &profile.coordinator_store_io;
        // Add shared routing, frozen attempt, generation manifest, then sync the
        // latest pointer last. Opening each store also syncs its root directory.
        assert_eq!(
            (
                combined.immutable_calls,
                combined.new_names,
                combined.replayed_names
            ),
            (3, 3, 0)
        );
        assert_eq!(
            (combined.file_sync_calls, combined.directory_sync_calls),
            (4, 5)
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
        assert_eq!(
            (
                replay.coordinator_store_io.new_names,
                replay.coordinator_store_io.replayed_names
            ),
            (0, 3)
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
        assert_eq!(profile.local_reuse.objects, 4);
        assert_eq!(
            profile.local_reuse.bytes,
            profile.owners[&1].store_io.immutable_payload_bytes
        );
        assert_eq!(profile.remote_copy.objects, 4);
        assert_eq!(
            profile.remote_copy.bytes,
            profile.owners[&2].store_io.immutable_payload_bytes
        );
        assert!(!save.join("owner-1").exists());
        assert!(save.join("owner-2").is_dir());
        assert_eq!(profile.coordinator_store_io.immutable_calls, 7);
        assert_eq!(
            profile.coordinator_store_io.replayed_names, 1,
            "shared vector descriptor already exists from local save"
        );
        let store = GenerationStore::open(&save).unwrap();
        let generation = store.reopen(&descriptor).unwrap();
        let remote = generation
            .owners
            .iter()
            .map(|reference| (reference, store.verify_saved_owner(reference).unwrap()))
            .find(|(_, owner)| owner.owner == 2)
            .unwrap();
        for object in remote
            .1
            .nodes
            .iter()
            .chain(&remote.1.postings)
            .chain(&remote.1.full_vectors)
            .chain(std::iter::once(remote.0))
        {
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
