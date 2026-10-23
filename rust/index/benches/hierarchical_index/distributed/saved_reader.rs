//! A saved reader uses immutable owner payloads and a generation descriptor.
//! Routing codes are persisted ready-to-score four-bit centroids; posting bytes
//! remain the writer's one-bit RaBitQ codes relative to each leaf centroid.
use super::super::common::ReadBeamPolicy;
use super::{
    generation::{
        FormatContract, GenerationDescriptor, GenerationManifest, GenerationStore, ObjectRef,
        OwnerManifest,
    },
    routing::{
        self, ExpandReply, ExpandRequest, ExpansionBackend, ExpansionStatus, NavigationWork,
        NodeRef, RoutingNode, ScoredChild,
    },
};
use chroma_distance::DistanceFunction;
use chroma_index::quantization::{
    Code, PackedFourBitCentroid, PackedFourBitScorer, QuantizedQuery,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

type Result<T> = std::result::Result<T, String>;
/// Stable vector IDs are independent of mutable subtree placement. An adapter
/// may read the retained immutable corpus instead of replicating vectors here.
pub use super::engine::VectorSource;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SavedPostingList {
    pub leaf: u128,
    pub ids: Vec<u64>,
    pub codes: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedPostingReference {
    leaf: u128,
    object: ObjectRef,
}
/// Routing and posting locations are metadata. Posting IDs and codes stay in
/// their separate objects until a scan or an explicit validation requests them.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedOwnerRouting {
    version: u32,
    nodes: Vec<SavedNode>,
    postings: Vec<SavedPostingReference>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SavedChild {
    pub id: u128,
    pub owner: u64,
    pub leaf: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SavedNode {
    pub id: u128,
    pub owner: u64,
    pub parent: Option<u128>,
    pub centroid: Vec<f32>,
    pub centroid_code: Vec<u8>,
    pub children: Vec<SavedChild>,
    pub leaf: bool,
}
impl SavedNode {
    pub fn from_routing(n: &RoutingNode) -> Result<Self> {
        if n.centroid.is_empty() || n.centroid.iter().any(|x| !x.is_finite()) {
            return Err("invalid saved centroid".into());
        }
        let code = Code::<4>::quantize(&n.centroid, &vec![0.0; n.centroid.len()]);
        let centroid_code =
            PackedFourBitCentroid::from_origin_code(&code, n.centroid.len()).to_bytes();
        PackedFourBitCentroid::from_bytes(&centroid_code, n.centroid.len())
            .map_err(|e| e.to_string())?;
        Ok(Self {
            id: n.target.id,
            owner: n.target.authority,
            parent: n.parent.map(|p| p.id),
            centroid: n.centroid.clone(),
            centroid_code,
            children: n
                .children
                .iter()
                .map(|c| SavedChild {
                    id: c.target.id,
                    owner: c.target.authority,
                    leaf: c.leaf,
                })
                .collect(),
            leaf: n.leaf,
        })
    }
    fn routing(&self) -> RoutingNode {
        RoutingNode {
            navigation: Default::default(),
            target: self.reference(),
            parent: self.parent.map(|id| NodeRef {
                id,
                authority: 0,
                placement: 0,
            }),
            centroid: self.centroid.clone(),
            centroid_code: self.centroid_code.clone(),
            children: vec![],
            leaf: self.leaf,
            revision: 0,
        }
    }
    fn reference(&self) -> NodeRef {
        NodeRef {
            id: self.id,
            authority: self.owner,
            placement: 0,
        }
    }
}
/// Freeze the engine snapshot before calling this function. Routing and posting
/// payloads are separate objects, so a reader can load routing without postings.
#[allow(clippy::too_many_arguments)]
pub fn save_owner_snapshot(
    store: &GenerationStore,
    run: &str,
    attempt: &str,
    generation: &str,
    owner: u64,
    roots: Vec<u128>,
    format: FormatContract,
    nodes: &[RoutingNode],
    postings: &[SavedPostingList],
    full_vectors: Vec<ObjectRef>,
) -> Result<ObjectRef> {
    save_owner_snapshot_reusing(
        store,
        run,
        attempt,
        generation,
        owner,
        roots,
        format,
        nodes,
        postings,
        full_vectors,
        &BTreeMap::new(),
    )
    .map(|(manifest, _)| manifest)
}

#[derive(Clone, Debug)]
pub(super) struct SavedPostingBlock {
    pub object: ObjectRef,
    pub count: u64,
}

/// Reuse references only from an owner cache that binds the same resident and
/// immutable routing image in the same storage namespace. The caller validates
/// the complete current membership before omitting unchanged posting bytes.
#[allow(clippy::too_many_arguments)]
pub(super) fn save_owner_snapshot_reusing(
    store: &GenerationStore,
    run: &str,
    attempt: &str,
    generation: &str,
    owner: u64,
    roots: Vec<u128>,
    format: FormatContract,
    nodes: &[RoutingNode],
    postings: &[SavedPostingList],
    full_vectors: Vec<ObjectRef>,
    reused: &BTreeMap<u128, SavedPostingBlock>,
) -> Result<(ObjectRef, BTreeMap<u128, SavedPostingBlock>)> {
    let saved = nodes
        .iter()
        .map(SavedNode::from_routing)
        .collect::<Result<Vec<_>>>()?;
    let mut seen = BTreeSet::new();
    for n in &saved {
        if n.owner != owner || n.centroid.len() != format.dimensions || !seen.insert(n.id) {
            return Err("invalid owner snapshot".into());
        }
    }
    let code_size = Code::<1>::size(format.dimensions);
    let mut posting_ids = BTreeSet::new();
    let mut leaves = reused.keys().copied().collect::<BTreeSet<_>>();
    let mut count = reused.values().try_fold(0u64, |sum, block| {
        sum.checked_add(block.count).ok_or("posting count overflow")
    })?;
    if reused
        .keys()
        .any(|leaf| !saved.iter().any(|node| node.id == *leaf && node.leaf))
    {
        return Err("reused posting has no current leaf".into());
    }
    for p in postings {
        if !leaves.insert(p.leaf)
            || !saved.iter().any(|n| n.id == p.leaf && n.leaf)
            || p.ids.len().checked_mul(code_size) != Some(p.codes.len())
        {
            return Err("invalid posting payload".into());
        }
        for id in &p.ids {
            if !posting_ids.insert(*id) {
                return Err("duplicate vector in owner snapshot".into());
            }
        }
        count = count
            .checked_add(p.ids.len() as u64)
            .ok_or("posting count overflow")?;
    }
    if saved.iter().any(|n| n.leaf && !leaves.contains(&n.id)) {
        return Err("owner leaf has no posting payload".into());
    }
    let name = {
        use sha2::{Digest, Sha256};
        let identity = serde_json::to_vec(&(run, attempt, owner)).map_err(|e| e.to_string())?;
        format!("saved-{:x}", Sha256::digest(identity))
    };
    let mut blocks = reused.clone();
    let written = super::generation::save_blocks_parallel(postings, |posting| {
        use sha2::{Digest, Sha256};
        let bytes = bincode::serialize(&vec![posting])
            .map_err(|e| super::generation::GenerationError::Invalid(e.to_string()))?;
        // Stable content names let equal posting bytes retain their reference
        // even if a routing-only change conservatively invalidates the cache.
        let object =
            store.put_object(&format!("postings-{:x}", Sha256::digest(&bytes)), &bytes)?;
        Ok((
            posting.leaf,
            SavedPostingBlock {
                object,
                count: posting.ids.len() as u64,
            },
        ))
    })
    .map_err(|e| e.to_string())?;
    blocks.extend(written);
    let posting_objects = blocks.values().map(|block| block.object.clone()).collect();
    let index = blocks
        .iter()
        .map(|(leaf, block)| SavedPostingReference {
            leaf: *leaf,
            object: block.object.clone(),
        })
        .collect();
    let metadata = SavedOwnerRouting {
        version: 1,
        nodes: saved,
        postings: index,
    };
    let nodes = store
        .put_object(
            &format!("{name}-nodes"),
            &serde_json::to_vec(&metadata).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
    let manifest = store
        .save_owner_reusing(
            &OwnerManifest {
                run: run.into(),
                save_attempt: attempt.into(),
                generation: generation.into(),
                owner,
                roots,
                vector_count: count,
                format,
                nodes: vec![nodes],
                postings: posting_objects,
                full_vectors,
            },
            &reused
                .values()
                .map(|block| block.object.clone())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| e.to_string())?;
    Ok((manifest, blocks))
}
/// Shared upper-tree nodes are saved in the generation's routing object. Its
/// first item is the global root; all remaining items are shared internal nodes.
pub fn save_shared_routing(
    store: &GenerationStore,
    name: &str,
    nodes: &[RoutingNode],
) -> Result<ObjectRef> {
    if nodes.is_empty() {
        return Err("shared routing needs its global root".into());
    }
    let saved = nodes
        .iter()
        .map(SavedNode::from_routing)
        .collect::<Result<Vec<_>>>()?;
    store
        .put_object(
            name,
            &serde_json::to_vec(&saved).map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())
}

pub struct SavedReader {
    store: GenerationStore,
    pub manifest: GenerationManifest,
    nodes: BTreeMap<u128, SavedNode>,
    codes: BTreeMap<u128, PackedFourBitCentroid>,
    posting_objects: BTreeMap<u128, ObjectRef>,
    root: u128,
    metric: DistanceFunction,
    scorer: PackedFourBitScorer,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct QueryWork {
    pub navigation: NavigationWork,
    pub leaves: usize,
    pub vectors: usize,
    pub posting_code_bytes: u64,
    pub posting_object_bytes: u64,
    pub reranked: usize,
}
impl SavedReader {
    pub fn open(store: GenerationStore, descriptor: &GenerationDescriptor) -> Result<Self> {
        let manifest = store
            .reopen_metadata(descriptor)
            .map_err(|e| e.to_string())?;
        let shared: Vec<SavedNode> = serde_json::from_slice(
            &store
                .read_object(&manifest.shared_routing)
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let root = shared.first().ok_or("missing global root")?.id;
        let mut nodes = BTreeMap::new();
        let mut posting_objects = BTreeMap::new();
        for node in shared {
            if nodes.insert(node.id, node).is_some() {
                return Err("duplicate shared node".into());
            }
        }
        for r in &manifest.owners {
            let owner: OwnerManifest =
                serde_json::from_slice(&store.read_object(r).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            let mut index = vec![];
            for r in &owner.nodes {
                let saved: SavedOwnerRouting =
                    serde_json::from_slice(&store.read_object(r).map_err(|e| e.to_string())?)
                        .map_err(|e| e.to_string())?;
                if saved.version != 1 {
                    return Err("unsupported saved owner routing format".into());
                }
                index.extend(saved.postings);
                for n in saved.nodes {
                    if n.owner != owner.owner {
                        return Err("misowned saved node".into());
                    }
                    if let Some(old) = nodes.get(&n.id) {
                        if n.id != root || old != &n {
                            return Err("duplicate saved node".into());
                        }
                    } else {
                        nodes.insert(n.id, n);
                    }
                }
            }
            let declared = owner
                .postings
                .iter()
                .map(|r| (r.path.clone(), r.clone()))
                .collect::<BTreeMap<_, _>>();
            let mut indexed = BTreeMap::new();
            for entry in index {
                let leaf = nodes
                    .get(&entry.leaf)
                    .ok_or("posting payload names an unknown leaf")?;
                if !leaf.leaf
                    || leaf.owner != owner.owner
                    || declared.get(&entry.object.path) != Some(&entry.object)
                {
                    return Err("posting reference must belong to a real leaf on its declared owner and match its manifest".into());
                }
                if indexed
                    .insert(entry.object.path.clone(), entry.object.clone())
                    .is_some()
                    || posting_objects.insert(entry.leaf, entry.object).is_some()
                {
                    return Err("duplicate leaf posting payload".into());
                }
            }
            if indexed != declared {
                return Err("owner manifest has unindexed posting payloads".into());
            }
        }
        let mut reached = BTreeSet::new();
        let mut pending = vec![root];
        let mut codes = BTreeMap::new();
        while let Some(id) = pending.pop() {
            if !reached.insert(id) {
                return Err("saved topology has a repeated node or cycle".into());
            }
            let n = nodes.get(&id).ok_or("missing saved child")?;
            if n.centroid.len() != manifest.format.dimensions
                || n.centroid.iter().any(|x| !x.is_finite())
                || n.leaf != n.children.is_empty()
            {
                return Err("invalid saved node shape".into());
            }
            codes.insert(
                id,
                PackedFourBitCentroid::from_bytes(&n.centroid_code, manifest.format.dimensions)
                    .map_err(|e| e.to_string())?,
            );
            for c in &n.children {
                let child = nodes.get(&c.id).ok_or("missing saved child")?;
                if child.parent != Some(id) || child.owner != c.owner || child.leaf != c.leaf {
                    return Err("incompatible saved parent/owner/type".into());
                }
                pending.push(c.id)
            }
            if n.leaf && !posting_objects.contains_key(&id) {
                return Err("missing saved leaf postings".into());
            }
        }
        if reached.len() != nodes.len() || nodes[&root].parent.is_some() {
            return Err("unreachable saved node or invalid root".into());
        }
        for coverage in &manifest.expected_roots {
            if nodes.get(&coverage.root).map(|n| n.owner) != Some(coverage.owner) {
                return Err("saved ownership coverage differs from topology".into());
            }
        }
        let metric = match manifest.format.metric.as_str() {
            "l2" => DistanceFunction::Euclidean,
            "cosine" => DistanceFunction::Cosine,
            "ip" => DistanceFunction::InnerProduct,
            _ => return Err("unsupported saved metric".into()),
        };
        Ok(Self {
            store,
            manifest,
            nodes,
            codes,
            posting_objects,
            root,
            metric,
            scorer: PackedFourBitScorer::new(),
        })
    }
    /// Reopen and hash-verify the corpus bound by this generation. The initial
    /// fixture assembles one canonical corpus from its pinned source objects.
    pub fn open_vector_source(&self) -> Result<CanonicalVectorSource> {
        self.open_vector_source_at(None)
    }
    pub fn open_vector_source_at(
        &self,
        path: Option<&std::path::Path>,
    ) -> Result<CanonicalVectorSource> {
        if self.manifest.full_vectors.len() != 1 {
            return Err("canonical fixture expects one complete vector descriptor".into());
        }
        let bytes = self
            .store
            .read_object(&self.manifest.full_vectors[0])
            .map_err(|e| e.to_string())?;
        let mut descriptor: VectorFileDescriptor =
            serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        if descriptor.dimensions != self.manifest.format.dimensions
            || descriptor.rows < self.manifest.total_vectors
        {
            return Err("vector descriptor does not cover this generation".into());
        }
        // Relocation changes only the local filename. SHA256, extents and pinned
        // source locators remain the generation-bound corpus identity.
        if let Some(path) = path {
            descriptor.path = path.to_str().ok_or("corpus path is not UTF-8")?.into();
        }
        CanonicalVectorSource::open(descriptor)
    }
    /// Recompute every persisted code from independent durable vectors. This
    /// checks more than counts: stale postings and centroid visibility fail.
    pub fn validate_codes(&self, source: &dyn VectorSource) -> Result<(u64, u64)> {
        let zero = vec![0.0; self.manifest.format.dimensions];
        for node in self.nodes.values() {
            let code = Code::<4>::quantize(&node.centroid, &zero);
            if PackedFourBitCentroid::from_origin_code(&code, zero.len()).to_bytes()
                != node.centroid_code
            {
                return Err("persisted four-bit centroid differs from its centroid".into());
            }
        }
        let width = Code::<1>::size(zero.len());
        let mut vectors = 0u64;
        self.visit_verified_postings(|leaf, p| {
            let centroid = &self.nodes[&leaf].centroid;
            for (id, saved) in p.ids.iter().zip(p.codes.chunks_exact(width)) {
                let vector = source.get(*id)?;
                if vector.len() != zero.len() {
                    return Err("full-vector dimension mismatch during code validation".into());
                }
                let expected = Code::<1>::quantize(&vector, centroid);
                if expected.as_ref() != saved {
                    return Err(format!(
                        "persisted one-bit posting differs from vector {id}"
                    ));
                }
                vectors += 1;
            }
            Ok(())
        })?;
        Ok((self.nodes.len() as u64, vectors))
    }
    pub fn posting_list(&self, leaf: u128) -> Result<SavedPostingList> {
        let r = self
            .posting_objects
            .get(&leaf)
            .ok_or("unknown posting leaf")?;
        let mut rows: Vec<SavedPostingList> =
            bincode::deserialize(&self.store.read_object(r).map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
        if rows.len() != 1 || rows[0].leaf != leaf {
            return Err("posting object differs from its indexed leaf".into());
        }
        let p = rows.pop().ok_or("missing posting row")?;
        if p.ids
            .len()
            .checked_mul(Code::<1>::size(self.manifest.format.dimensions))
            != Some(p.codes.len())
        {
            return Err("posting code length mismatch".into());
        }
        Ok(p)
    }
    /// Fetch at most one bounded wave of immutable blocks concurrently. Consume
    /// them in leaf order on this thread, retaining all membership/code checks
    /// and deterministic error order without retaining the complete payload set.
    fn visit_verified_postings(
        &self,
        mut visit: impl FnMut(u128, SavedPostingList) -> Result<()>,
    ) -> Result<()> {
        let workers = reader_worker_count(std::env::var("SPANN_READER_WORKERS").ok().as_deref())?;
        let leaves: Vec<_> = self.posting_objects.keys().copied().collect();
        visit_reader_batches(&leaves, workers, |leaf| self.posting_list(*leaf), |leaf, posting| visit(*leaf, posting))
    }

    pub fn validate_membership(&self) -> Result<BTreeSet<u64>> {
        let mut ids = BTreeSet::new();
        self.visit_verified_postings(|_, posting| {
            for id in posting.ids {
                if !ids.insert(id) {
                    return Err("duplicate saved vector membership".into());
                }
            }
            Ok(())
        })?;
        if ids.len() as u64 != self.manifest.total_vectors {
            return Err("saved vector count mismatch".into());
        }
        Ok(ids)
    }
    pub fn query(
        &self,
        query: &[f32],
        k: usize,
        policy: &ReadBeamPolicy,
        rerank: usize,
        vectors: Option<&dyn VectorSource>,
    ) -> Result<(Vec<(u64, f32)>, QueryWork)> {
        if query.len() != self.manifest.format.dimensions || query.iter().any(|x| !x.is_finite()) {
            return Err("invalid query dimension or values".into());
        }
        if k == 0 {
            return Ok((vec![], QueryWork::default()));
        }
        let root = &self.nodes[&self.root];
        let (leaves, navigation) = if root.leaf {
            (vec![(root.reference(), 0.0)], NavigationWork::default())
        } else {
            let (mut results, work) = routing::navigate_batch(
                self,
                &root.routing(),
                &[query.to_vec()],
                policy,
                0,
                0,
                0,
                0,
                None,
            )?;
            (results.remove(0), work)
        };
        let mut work = QueryWork {
            navigation,
            ..Default::default()
        };
        let q_norm = query.iter().map(|x| x * x).sum::<f32>().sqrt();
        let mut results = vec![];
        for (leaf, _) in leaves {
            let p = self.posting_list(leaf.id)?;
            let n = &self.nodes[&leaf.id];
            let residual: Vec<_> = query.iter().zip(&n.centroid).map(|(q, c)| q - c).collect();
            let norm = n.centroid.iter().map(|x| x * x).sum::<f32>().sqrt();
            let dot = n.centroid.iter().zip(query).map(|(c, q)| c * q).sum();
            let qq = QuantizedQuery::new(
                &residual,
                Code::<1>::packed_len(query.len()),
                norm,
                dot,
                q_norm,
            );
            let mut distances = vec![0.0; p.ids.len()];
            Code::<1>::distances_quantized_query(&p.codes, &self.metric, &qq, &mut distances);
            work.leaves += 1;
            work.vectors += p.ids.len();
            work.posting_code_bytes += p.codes.len() as u64;
            work.posting_object_bytes += self.posting_objects[&leaf.id].bytes;
            results.extend(p.ids.into_iter().zip(distances));
        }
        if results.iter().any(|(_, d)| !d.is_finite()) {
            return Err("nonfinite saved posting score".into());
        }
        results.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        results.truncate(k.checked_mul(rerank.max(1)).ok_or("rerank size overflow")?);
        if rerank > 0 {
            let source = vectors.ok_or("reranking requires an independent vector source")?;
            for (id, distance) in &mut results {
                let v = source.get(*id)?;
                if v.len() != query.len() || v.iter().any(|x| !x.is_finite()) {
                    return Err("invalid independent full vector".into());
                }
                *distance = self.metric.distance(query, &v);
                work.reranked += 1;
            }
            if results.iter().any(|(_, d)| !d.is_finite()) {
                return Err("nonfinite saved posting score".into());
            }
            results.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        }
        results.truncate(k);
        Ok((results, work))
    }
}
impl ExpansionBackend for SavedReader {
    fn expand(&self, authority: u64, requests: Vec<ExpandRequest>) -> Result<Vec<ExpandReply>> {
        requests
            .into_iter()
            .map(|r| {
                let query = self.scorer.prepare_query(&r.query);
                let mut children = vec![];
                for target in &r.nodes {
                    let n = self
                        .nodes
                        .get(&target.id)
                        .ok_or("unknown saved expansion node")?;
                    if n.owner != authority || n.leaf {
                        return Err("invalid saved expansion authority or node".into());
                    }
                    for (ordinal, c) in n.children.iter().enumerate() {
                        children.push(ScoredChild {
                            parent: n.id,
                            ordinal,
                            target: self.nodes[&c.id].reference(),
                            leaf: c.leaf,
                            distance: self.scorer.distance(
                                &self.codes[&c.id],
                                &query,
                                &self.metric,
                            ),
                            observed_revision: 0,
                        });
                    }
                }
                Ok(ExpandReply {
                    authority,
                    run: r.run,
                    epoch: r.epoch,
                    shared_sequence: r.shared_sequence,
                    child_counts: r
                        .nodes
                        .iter()
                        .map(|n| (n.id, self.nodes[&n.id].children.len()))
                        .collect(),
                    navigation: r.navigation,
                    step: r.step,
                    query_slot: r.query_slot,
                    nodes: r.nodes,
                    status: ExpansionStatus::Complete,
                    children,
                })
            })
            .collect()
    }
}

/// A canonical little-endian f32 corpus has a stable byte layout independent of
/// the writer placement. Locators record immutable source object versions; the
/// local assembled file is accepted only after a full streaming SHA-256 check.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorFileDescriptor {
    pub path: String,
    pub rows: u64,
    pub dimensions: usize,
    pub stride_bytes: u64,
    pub offset_bytes: u64,
    pub first_id: u64,
    pub file_bytes: u64,
    pub sha256: String,
    pub immutable_locators: Vec<String>,
}
pub struct CanonicalVectorSource {
    file: std::fs::File,
    descriptor: VectorFileDescriptor,
    resident: Option<Vec<std::sync::Arc<[f32]>>>,
}
impl CanonicalVectorSource {
    pub fn open(descriptor: VectorFileDescriptor) -> Result<Self> {
        use sha2::{Digest, Sha256};
        use std::io::Read;
        if descriptor.rows == 0
            || descriptor.dimensions == 0
            || descriptor.dimensions as u64 > u64::MAX / 4
            || descriptor.stride_bytes < descriptor.dimensions as u64 * 4
            || descriptor.sha256.len() != 64
            || !descriptor.sha256.bytes().all(|b| b.is_ascii_hexdigit())
            || descriptor.immutable_locators.is_empty()
        {
            return Err("invalid canonical vector descriptor".into());
        }
        let end = descriptor
            .rows
            .checked_sub(1)
            .and_then(|n| n.checked_mul(descriptor.stride_bytes))
            .and_then(|n| n.checked_add(descriptor.offset_bytes))
            .and_then(|n| n.checked_add(descriptor.dimensions as u64 * 4))
            .ok_or("vector file extent overflow")?;
        descriptor
            .first_id
            .checked_add(descriptor.rows - 1)
            .ok_or("vector id extent overflow")?;
        let mut file = std::fs::File::open(&descriptor.path).map_err(|e| e.to_string())?;
        if file.metadata().map_err(|e| e.to_string())?.len() != descriptor.file_bytes
            || end > descriptor.file_bytes
        {
            return Err("vector file length mismatch".into());
        }
        let mut hash = Sha256::new();
        let mut buffer = vec![0u8; 1024 * 1024];
        loop {
            let n = file.read(&mut buffer).map_err(|e| e.to_string())?;
            if n == 0 {
                break;
            }
            hash.update(&buffer[..n]);
        }
        let digest = format!("{:x}", hash.finalize());
        if !digest.eq_ignore_ascii_case(&descriptor.sha256) {
            return Err("vector corpus SHA-256 mismatch".into());
        }
        Ok(Self {
            file,
            descriptor,
            resident: None,
        })
    }
    /// Match a resident reference workload: decode once in bounded read buffers,
    /// then share immutable per-vector allocations across construction workers.
    pub fn preload(self) -> Result<Self> {
        self.preload_with_limit(12 * 1024 * 1024 * 1024)
    }
    /// Conservatively include immutable float payloads, per-row reference counts
    /// and allocation overhead, the resident handle array, and decode headroom.
    pub fn estimated_resident_bytes(&self) -> Result<u64> {
        let width = (self.descriptor.dimensions as u64)
            .checked_mul(4)
            .ok_or("resident vector width overflow")?;
        let row = width
            .checked_add(80)
            // Include allocator size-class rounding in addition to reference
            // counts and the resident fat-pointer array.
            .and_then(|bytes| bytes.checked_add(width.div_ceil(4)))
            .ok_or("resident row estimate overflow")?;
        self.descriptor
            .rows
            .checked_mul(row)
            .and_then(|total| total.checked_add((8 * 1024 * 1024).max(width)))
            .and_then(|total| total.checked_add(width))
            .ok_or_else(|| "resident cache estimate overflow".into())
    }
    pub fn preload_with_limit(mut self, limit_bytes: u64) -> Result<Self> {
        if self.estimated_resident_bytes()? > limit_bytes {
            return Err("immutable vector cache exceeds configured memory budget".into());
        }
        use std::os::unix::fs::FileExt;
        let rows = usize::try_from(self.descriptor.rows)
            .map_err(|_| "resident row count exceeds address space")?;
        let stride = usize::try_from(self.descriptor.stride_bytes)
            .map_err(|_| "vector stride exceeds address space")?;
        let width = self
            .descriptor
            .dimensions
            .checked_mul(4)
            .ok_or("vector byte width overflow")?;
        let batch = (8 * 1024 * 1024 / stride).max(1);
        let mut resident = Vec::new();
        resident
            .try_reserve_exact(rows)
            .map_err(|e| e.to_string())?;
        for first in (0..rows).step_by(batch) {
            let count = batch.min(rows - first);
            let bytes = (count - 1)
                .checked_mul(stride)
                .and_then(|n| n.checked_add(width))
                .ok_or("preload buffer overflow")?;
            let mut buffer = vec![0; bytes];
            let offset = (first as u64)
                .checked_mul(self.descriptor.stride_bytes)
                .and_then(|n| n.checked_add(self.descriptor.offset_bytes))
                .ok_or("preload offset overflow")?;
            self.file
                .read_exact_at(&mut buffer, offset)
                .map_err(|e| e.to_string())?;
            for row in 0..count {
                let v: Vec<_> = buffer[row * stride..row * stride + width]
                    .chunks_exact(4)
                    .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
                    .collect();
                if v.iter().any(|x| !x.is_finite()) {
                    return Err("nonfinite durable vector".into());
                }
                resident.push(std::sync::Arc::from(v));
            }
        }
        self.resident = Some(resident);
        Ok(self)
    }
    pub fn descriptor(&self) -> &VectorFileDescriptor {
        &self.descriptor
    }
}
impl VectorSource for CanonicalVectorSource {
    fn get(&self, id: u64) -> Result<std::sync::Arc<[f32]>> {
        use std::os::unix::fs::FileExt;
        let row = id
            .checked_sub(self.descriptor.first_id)
            .filter(|n| *n < self.descriptor.rows)
            .ok_or("vector id outside saved corpus")?;
        if let Some(resident) = &self.resident {
            return Ok(resident[row as usize].clone());
        }
        let offset = row
            .checked_mul(self.descriptor.stride_bytes)
            .and_then(|n| n.checked_add(self.descriptor.offset_bytes))
            .ok_or("vector offset overflow")?;
        let bytes = self
            .descriptor
            .dimensions
            .checked_mul(4)
            .ok_or("vector byte length overflow")?;
        let mut raw = vec![0; bytes];
        self.file
            .read_exact_at(&mut raw, offset)
            .map_err(|e| e.to_string())?;
        let v: Vec<_> = raw
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes(b.try_into().unwrap()))
            .collect();
        if v.iter().any(|x| !x.is_finite()) {
            return Err("nonfinite durable full vector".into());
        }
        Ok(v.into())
    }
}
#[cfg(test)]
#[path = "saved_reader_tests.rs"]
mod tests;

fn reader_worker_count(value: Option<&str>) -> Result<usize> {
    match value {
        None => Ok(32),
        Some(value) => value.parse::<usize>().ok()
            .filter(|workers| (1..=32).contains(workers))
            .ok_or_else(|| "SPANN_READER_WORKERS must be an integer from 1 through 32".into()),
    }
}

fn visit_reader_batches<T: Sync, R: Send>(
    items: &[T],
    workers: usize,
    fetch: impl Fn(&T) -> Result<R> + Sync + Send,
    mut visit: impl FnMut(&T, R) -> Result<()>,
) -> Result<()> {
    use rayon::prelude::*;
    if !(1..=32).contains(&workers) {
        return Err("reader worker count must be from 1 through 32".into());
    }
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(workers)
        .thread_name(|i| format!("spann-reader-{i}"))
        .build()
        .map_err(|error| format!("reader worker pool: {error}"))?;
    for batch in items.chunks(workers) {
        // Collect every result before propagating errors, joining all reads.
        let fetched: Vec<_> = pool.install(|| batch.par_iter().map(&fetch).collect());
        for (item, result) in batch.iter().zip(fetched) {
            visit(item, result?)?;
        }
    }
    Ok(())
}
