//! Freeze actual owner records locally, copy only immutable saved objects, then
//! publish the combined generation. Construction posting payloads never travel
//! through an unbounded whole-index Snapshot response.
use super::{
    engine::{Cluster, EngineRequest, EngineResponse, OwnerService},
    generation::{
        FormatContract, GenerationDescriptor, GenerationManifest, GenerationStore, ObjectRef,
        OwnerManifest, RootCoverage,
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
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerSaveReceipt {
    pub owner: u64,
    pub manifest: ObjectRef,
    pub objects: Vec<ObjectRef>,
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
        let snapshot = self.save_quiescent_snapshot(request.run, request.epoch)?;
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
            let remote_root = store_path
                .join(format!("owner-{owner}"))
                .to_str()
                .ok_or("save path is not UTF-8")?
                .to_string();
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
            if receipt.owner != owner {
                return Err("save acknowledgement owner mismatch".into());
            }
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
            }
            let bytes = remote_bytes(self, owner, &remote_root, &receipt.manifest, run, epoch)?;
            let manifest: OwnerManifest =
                serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
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
            let saved = store.save_owner(&manifest).map_err(|e| e.to_string())?;
            if saved != receipt.manifest {
                return Err("owner manifest integrity mismatch".into());
            }
            owner_refs.push(saved);
        }
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
        store
            .publish_latest(&descriptor, None)
            .map_err(|e| e.to_string())?;
        std::fs::write(
            store_path.join("descriptor.json"),
            serde_json::to_vec_pretty(&descriptor).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        Ok(descriptor)
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
        let owner_root = save.join("owner-1").to_str().unwrap().to_string();
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
}
