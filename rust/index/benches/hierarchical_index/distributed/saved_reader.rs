//! A saved reader uses immutable owner payloads and a generation descriptor.
//! Routing codes are persisted ready-to-score four-bit centroids; posting bytes
//! remain the writer's one-bit RaBitQ codes relative to each leaf centroid.
//!
//! Two saved layouts exist. v1 routing nodes carry each node's exact centroid
//! and four-bit code, and the reader regroups child codes into sibling arrays at
//! open. v2 (written by default) keeps routing nodes to topology only: each
//! parent's sibling arrays are persisted ready to install in a binary
//! navigation object, each leaf's exact centroid travels in its posting object,
//! and internal exact centroids sit in a validation object that only
//! `validate_codes` reads. Every machine then holds codes, not exact centroids.
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
    vector_store::VectorStoreRef,
};
use chroma_distance::DistanceFunction;
use chroma_index::quantization::{
    Code, PackedFourBitCentroid, PackedFourBitCentroidBatch, PackedFourBitScorer, QuantizedQuery,
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

type Result<T> = std::result::Result<T, String>;
/// Stable vector IDs are independent of mutable subtree placement. An adapter
/// may read the retained immutable corpus instead of replicating vectors here.
pub use super::engine::VectorSource;
/// One leaf's postings. `centroid` is the exact leaf centroid the one-bit codes
/// are relative to: v2 posting objects carry it and v1 reads leave it empty
/// (see `scoring_centroid`). Writers may leave it empty; saving takes the
/// centroid from the leaf's routing node and rejects a different one.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SavedPostingList {
    pub leaf: u128,
    pub ids: Vec<u64>,
    pub codes: Vec<u8>,
    pub centroid: Vec<f32>,
}
impl SavedPostingList {
    /// The centroid scoring centers the query on: posting-carried in v2, the
    /// routing node's in v1.
    pub fn scoring_centroid<'a>(&'a self, reader: &'a SavedReader) -> Result<&'a [f32]> {
        if !self.centroid.is_empty() {
            return Ok(&self.centroid);
        }
        reader
            .node_centroid(self.leaf)
            .ok_or_else(|| "unknown leaf centroid".into())
    }
}
/// v1 posting object: a bincode list holding exactly one row.
#[derive(Serialize, Deserialize)]
struct SavedPostingRowV1<'a> {
    leaf: u128,
    ids: Cow<'a, [u64]>,
    codes: Cow<'a, [u8]>,
}
/// v2 posting object: one bincode record that also carries the leaf centroid,
/// so only the machine scanning a leaf loads its exact centroid.
#[derive(Serialize, Deserialize)]
struct SavedPostingObjectV2<'a> {
    version: u32,
    leaf: u128,
    centroid: Cow<'a, [f32]>,
    ids: Cow<'a, [u64]>,
    codes: Cow<'a, [u8]>,
}
/// Routing layouts this reader accepts. Writers save `V2` unless a test asks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SavedLayout {
    V1,
    V2,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedPostingReference {
    leaf: u128,
    object: ObjectRef,
}
/// Routing and posting locations are metadata. Posting IDs and codes stay in
/// their separate objects until a scan or an explicit validation requests them.
/// This is the v1 owner routing object (`version: 1`).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedOwnerRouting {
    version: u32,
    nodes: Vec<SavedNode>,
    postings: Vec<SavedPostingReference>,
}
/// v2 owner routing object (`version: 2`). Nodes are topology only; children
/// and their codes come from `navigation`. Both objects are also listed in the
/// owner manifest so copies and integrity checks cover them.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedOwnerRoutingV2 {
    version: u32,
    nodes: Vec<SavedRoutingNodeV2>,
    postings: Vec<SavedPostingReference>,
    navigation: ObjectRef,
    validation: ObjectRef,
    /// The owner's full-vector store, when its save attached one.
    #[serde(default)]
    vectors: Option<VectorStoreRef>,
}
/// v2 shared routing object. v1 shared routing is a bare JSON node list.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedSharedRoutingV2 {
    version: u32,
    nodes: Vec<SavedRoutingNodeV2>,
    navigation: ObjectRef,
    validation: ObjectRef,
}
#[derive(Deserialize)]
struct SavedVersion {
    version: u32,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct SavedRoutingNodeV2 {
    id: u128,
    owner: u64,
    parent: Option<u128>,
    leaf: bool,
}
/// One parent's persisted sibling arrays: child identities in scoring order and
/// the `PackedFourBitCentroidBatch` parts holding their codes in that order.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedParentNavigation {
    parent: u128,
    children: Vec<SavedChild>,
    headers: Vec<[f32; 3]>,
    packed: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedNavigationV2 {
    version: u32,
    dimensions: usize,
    parents: Vec<SavedParentNavigation>,
}
/// Exact internal-node centroids, needed only to recompute persisted codes.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SavedValidationV2 {
    version: u32,
    centroids: Vec<(u128, Vec<f32>)>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SavedChild {
    pub id: u128,
    pub owner: u64,
    pub leaf: bool,
}
/// A v1 routing record, and the reader's resident node for both layouts. v2
/// nodes keep `centroid` and `centroid_code` empty.
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
/// The four-bit code navigation scores: quantized against a zero origin.
fn origin_code(centroid: &[f32]) -> PackedFourBitCentroid {
    let code = Code::<4>::quantize(centroid, &vec![0.0; centroid.len()]);
    PackedFourBitCentroid::from_origin_code(&code, centroid.len())
}
fn valid_centroid(centroid: &[f32], dim: usize) -> bool {
    dim > 0 && centroid.len() == dim && centroid.iter().all(|x| x.is_finite())
}
fn saved_child(c: &routing::Child) -> SavedChild {
    SavedChild {
        id: c.target.id,
        owner: c.target.authority,
        leaf: c.leaf,
    }
}
impl SavedNode {
    pub fn from_routing(n: &RoutingNode) -> Result<Self> {
        if n.centroid.is_empty() || n.centroid.iter().any(|x| !x.is_finite()) {
            return Err("invalid saved centroid".into());
        }
        let centroid_code = origin_code(&n.centroid).to_bytes();
        PackedFourBitCentroid::from_bytes(&centroid_code, n.centroid.len())
            .map_err(|e| e.to_string())?;
        Ok(Self {
            id: n.target.id,
            owner: n.target.authority,
            parent: n.parent.map(|p| p.id),
            centroid: n.centroid.clone(),
            centroid_code,
            children: n.children.iter().map(saved_child).collect(),
            leaf: n.leaf,
        })
    }
    /// Navigation reads only the root centroid's length, so a v2 root, which
    /// has no exact centroid, gets a zero placeholder of the index dimension.
    fn routing(&self, dim: usize) -> RoutingNode {
        RoutingNode {
            navigation: Default::default(),
            target: self.reference(),
            parent: self.parent.map(|id| NodeRef {
                id,
                authority: 0,
                placement: 0,
            }),
            centroid: if self.centroid.is_empty() {
                vec![0.0; dim]
            } else {
                self.centroid.clone()
            },
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
fn content_object(store: &GenerationStore, kind: &str, bytes: &[u8]) -> Result<ObjectRef> {
    use sha2::{Digest, Sha256};
    store
        .put_object(&format!("{kind}-{:x}", Sha256::digest(bytes)), bytes)
        .map_err(|e| e.to_string())
}
/// Write v2 sibling arrays and exact internal centroids for `nodes`. A child's
/// code comes from its own routing node when this save holds it, else from the
/// parent's copy of the child centroid: shared parents like the root list
/// unit roots that other owners save. `validate_codes` checks both against
/// the child's own exact centroid.
fn save_tree_objects_v2(
    store: &GenerationStore,
    nodes: &[RoutingNode],
    dim: usize,
) -> Result<(ObjectRef, ObjectRef)> {
    let local: BTreeMap<u128, &RoutingNode> = nodes.iter().map(|n| (n.target.id, n)).collect();
    let mut parents = vec![];
    let mut centroids = vec![];
    for n in nodes.iter().filter(|n| !n.leaf) {
        let mut codes = Vec::with_capacity(n.children.len());
        for c in &n.children {
            let centroid = local
                .get(&c.target.id)
                .map_or(&c.centroid, |child| &child.centroid);
            if !valid_centroid(centroid, dim) {
                return Err("invalid saved child centroid".into());
            }
            codes.push(origin_code(centroid));
        }
        let (headers, packed) = PackedFourBitCentroidBatch::new(codes, dim).to_parts();
        parents.push(SavedParentNavigation {
            parent: n.target.id,
            children: n.children.iter().map(saved_child).collect(),
            headers,
            packed,
        });
        centroids.push((n.target.id, n.centroid.clone()));
    }
    let navigation = bincode::serialize(&SavedNavigationV2 {
        version: 2,
        dimensions: dim,
        parents,
    })
    .map_err(|e| e.to_string())?;
    let validation = bincode::serialize(&SavedValidationV2 {
        version: 2,
        centroids,
    })
    .map_err(|e| e.to_string())?;
    Ok((
        content_object(store, "navigation", &navigation)?,
        content_object(store, "validation", &validation)?,
    ))
}
/// Freeze the engine snapshot before calling this function. Routing and posting
/// payloads are separate objects, so a reader can load routing without postings.
/// `vectors` attaches the owner's full-vector store to the v2 routing object
/// (`SavedReader::owner_vector_stores`); pass None when the save has none.
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
    vectors: Option<VectorStoreRef>,
) -> Result<ObjectRef> {
    save_owner_snapshot_as(
        SavedLayout::V2,
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
        vectors,
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
/// Reused blocks must have been written in v2 layout; the in-process cache only
/// ever holds blocks this writer saved, so a v1 generation is never a source.
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
    vectors: Option<VectorStoreRef>,
    reused: &BTreeMap<u128, SavedPostingBlock>,
) -> Result<(ObjectRef, BTreeMap<u128, SavedPostingBlock>)> {
    save_owner_snapshot_as(
        SavedLayout::V2,
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
        vectors,
        reused,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn save_owner_snapshot_as(
    layout: SavedLayout,
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
    vectors: Option<VectorStoreRef>,
    reused: &BTreeMap<u128, SavedPostingBlock>,
) -> Result<(ObjectRef, BTreeMap<u128, SavedPostingBlock>)> {
    let saved = match layout {
        SavedLayout::V1 => nodes
            .iter()
            .map(SavedNode::from_routing)
            .collect::<Result<Vec<_>>>()?,
        SavedLayout::V2 => vec![],
    };
    let mut by_id = BTreeMap::new();
    for n in nodes {
        if n.target.authority != owner
            || !valid_centroid(&n.centroid, format.dimensions)
            || by_id.insert(n.target.id, n).is_some()
        {
            return Err("invalid owner snapshot".into());
        }
    }
    match (&vectors, layout) {
        (None, _) => {}
        (Some(v), SavedLayout::V2) if v.owner == owner && v.dimensions == format.dimensions => {}
        _ => return Err("vector store reference differs from its owner snapshot".into()),
    }
    let code_size = Code::<1>::size(format.dimensions);
    let mut posting_ids = BTreeSet::new();
    let mut leaves = reused.keys().copied().collect::<BTreeSet<_>>();
    let mut count = reused.values().try_fold(0u64, |sum, block| {
        sum.checked_add(block.count).ok_or("posting count overflow")
    })?;
    if reused
        .keys()
        .any(|leaf| !by_id.get(leaf).is_some_and(|node| node.leaf))
    {
        return Err("reused posting has no current leaf".into());
    }
    for p in postings {
        if !leaves.insert(p.leaf)
            || !by_id
                .get(&p.leaf)
                .is_some_and(|n| n.leaf && (p.centroid.is_empty() || p.centroid == n.centroid))
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
    if by_id
        .values()
        .any(|n| n.leaf && !leaves.contains(&n.target.id))
    {
        return Err("owner leaf has no posting payload".into());
    }
    let name = {
        use sha2::{Digest, Sha256};
        let identity = serde_json::to_vec(&(run, attempt, owner)).map_err(|e| e.to_string())?;
        format!("saved-{:x}", Sha256::digest(identity))
    };
    let mut blocks = reused.clone();
    let written = super::generation::save_blocks_parallel(postings, |posting| {
        let bytes = match layout {
            // A list, not an array: v1 objects carry bincode's length prefix.
            SavedLayout::V1 => bincode::serialize(&vec![SavedPostingRowV1 {
                leaf: posting.leaf,
                ids: Cow::Borrowed(&posting.ids),
                codes: Cow::Borrowed(&posting.codes),
            }]),
            SavedLayout::V2 => bincode::serialize(&SavedPostingObjectV2 {
                version: 2,
                leaf: posting.leaf,
                centroid: Cow::Borrowed(&by_id[&posting.leaf].centroid),
                ids: Cow::Borrowed(&posting.ids),
                codes: Cow::Borrowed(&posting.codes),
            }),
        }
        .map_err(|e| super::generation::GenerationError::Invalid(e.to_string()))?;
        // Stable content names let equal posting bytes retain their reference
        // even if a routing-only change conservatively invalidates the cache.
        use sha2::{Digest, Sha256};
        let object = store.put_object(&format!("postings-{:x}", Sha256::digest(&bytes)), &bytes)?;
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
    let (metadata, navigation, validation) = match layout {
        SavedLayout::V1 => (
            serde_json::to_vec(&SavedOwnerRouting {
                version: 1,
                nodes: saved,
                postings: index,
            }),
            vec![],
            vec![],
        ),
        SavedLayout::V2 => {
            let (navigation, validation) = save_tree_objects_v2(store, nodes, format.dimensions)?;
            (
                serde_json::to_vec(&SavedOwnerRoutingV2 {
                    version: 2,
                    nodes: nodes
                        .iter()
                        .map(|n| SavedRoutingNodeV2 {
                            id: n.target.id,
                            owner: n.target.authority,
                            parent: n.parent.map(|p| p.id),
                            leaf: n.leaf,
                        })
                        .collect(),
                    postings: index,
                    navigation: navigation.clone(),
                    validation: validation.clone(),
                    vectors,
                }),
                vec![navigation],
                vec![validation],
            )
        }
    };
    let nodes = store
        .put_object(
            &format!("{name}-nodes"),
            &metadata.map_err(|e| e.to_string())?,
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
                navigation,
                validation,
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
    save_shared_routing_as(SavedLayout::V2, store, name, nodes)
}
pub(super) fn save_shared_routing_as(
    layout: SavedLayout,
    store: &GenerationStore,
    name: &str,
    nodes: &[RoutingNode],
) -> Result<ObjectRef> {
    let dim = nodes
        .first()
        .ok_or("shared routing needs its global root")?
        .centroid
        .len();
    let bytes = match layout {
        SavedLayout::V1 => serde_json::to_vec(
            &nodes
                .iter()
                .map(SavedNode::from_routing)
                .collect::<Result<Vec<_>>>()?,
        ),
        SavedLayout::V2 => {
            let mut seen = BTreeSet::new();
            if nodes
                .iter()
                .any(|n| !valid_centroid(&n.centroid, dim) || !seen.insert(n.target.id))
            {
                return Err("invalid shared routing".into());
            }
            let (navigation, validation) = save_tree_objects_v2(store, nodes, dim)?;
            serde_json::to_vec(&SavedSharedRoutingV2 {
                version: 2,
                nodes: nodes
                    .iter()
                    .map(|n| SavedRoutingNodeV2 {
                        id: n.target.id,
                        owner: n.target.authority,
                        parent: n.parent.map(|p| p.id),
                        leaf: n.leaf,
                    })
                    .collect(),
                navigation,
                validation,
            })
        }
    }
    .map_err(|e| e.to_string())?;
    store.put_object(name, &bytes).map_err(|e| e.to_string())
}

/// A parent owns one immutable sibling scoring buffer after reader open.
/// Child references and codes share the same order for the entire generation.
struct SavedChildNavigation {
    children: Vec<(NodeRef, bool)>,
    codes: PackedFourBitCentroidBatch,
}

fn pack_saved_children(
    nodes: &BTreeMap<u128, SavedNode>,
    mut codes: BTreeMap<u128, PackedFourBitCentroid>,
    dim: usize,
) -> Result<BTreeMap<u128, SavedChildNavigation>> {
    let mut navigation = BTreeMap::new();
    for node in nodes.values().filter(|node| !node.leaf) {
        let mut children = Vec::with_capacity(node.children.len());
        let mut siblings = Vec::with_capacity(node.children.len());
        for child in &node.children {
            let target = nodes.get(&child.id).ok_or("missing saved child")?;
            children.push((target.reference(), child.leaf));
            siblings.push(
                codes
                    .remove(&child.id)
                    .ok_or("missing or repeated saved child code")?,
            );
        }
        navigation.insert(
            node.id,
            SavedChildNavigation {
                children,
                codes: PackedFourBitCentroidBatch::new(siblings, dim),
            },
        );
    }
    // The root's own code is validated above but is never scored as a child.
    Ok(navigation)
}

pub struct SavedReader {
    store: GenerationStore,
    pub manifest: GenerationManifest,
    layout: SavedLayout,
    nodes: BTreeMap<u128, SavedNode>,
    navigation: BTreeMap<u128, SavedChildNavigation>,
    posting_objects: BTreeMap<u128, ObjectRef>,
    /// v2 exact internal centroids; opening never reads these objects.
    validation: Vec<ObjectRef>,
    vector_stores: BTreeMap<u64, VectorStoreRef>,
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
/// Install one v2 routing object's topology and its persisted sibling arrays.
/// Each internal node has exactly one navigation entry and each leaf none;
/// children come from the entry, so the topology walk checks them as in v1.
#[allow(clippy::too_many_arguments)]
fn install_v2(
    store: &GenerationStore,
    records: Vec<SavedRoutingNodeV2>,
    navigation: &ObjectRef,
    owner: Option<u64>,
    root: u128,
    dim: usize,
    nodes: &mut BTreeMap<u128, SavedNode>,
    packed: &mut BTreeMap<u128, SavedChildNavigation>,
) -> Result<()> {
    let saved: SavedNavigationV2 =
        bincode::deserialize(&store.read_object(navigation).map_err(|e| e.to_string())?)
            .map_err(|e| e.to_string())?;
    if saved.version != 2 || saved.dimensions != dim {
        return Err("unsupported saved navigation format or dimensions".into());
    }
    let mut entries = BTreeMap::new();
    for entry in saved.parents {
        if entries.insert(entry.parent, entry).is_some() {
            return Err("duplicate saved navigation parent".into());
        }
    }
    for r in records {
        if owner.is_some_and(|o| o != r.owner) {
            return Err("misowned saved node".into());
        }
        let mut node = SavedNode {
            id: r.id,
            owner: r.owner,
            parent: r.parent,
            centroid: vec![],
            centroid_code: vec![],
            children: vec![],
            leaf: r.leaf,
        };
        if let Some(entry) = entries.remove(&r.id) {
            if r.leaf {
                return Err("saved leaf has sibling arrays".into());
            }
            let codes = PackedFourBitCentroidBatch::from_parts(entry.headers, entry.packed, dim)
                .map_err(|e| e.to_string())?;
            if codes.len() != entry.children.len() {
                return Err("saved sibling codes differ from their children".into());
            }
            let children = entry
                .children
                .iter()
                .map(|c| {
                    (
                        NodeRef {
                            id: c.id,
                            authority: c.owner,
                            placement: 0,
                        },
                        c.leaf,
                    )
                })
                .collect();
            node.children = entry.children;
            if packed
                .insert(r.id, SavedChildNavigation { children, codes })
                .is_some()
            {
                return Err("duplicate saved node".into());
            }
        } else if !r.leaf {
            return Err("saved internal node lacks sibling arrays".into());
        }
        if let Some(old) = nodes.get(&node.id) {
            // Only a leaf root appears in both the shared and an owner object.
            if node.id != root || old != &node {
                return Err("duplicate saved node".into());
            }
        } else {
            nodes.insert(node.id, node);
        }
    }
    if !entries.is_empty() {
        return Err("saved navigation names an unknown parent".into());
    }
    Ok(())
}
impl SavedReader {
    pub fn open(store: GenerationStore, descriptor: &GenerationDescriptor) -> Result<Self> {
        let manifest = store
            .reopen_metadata(descriptor)
            .map_err(|e| e.to_string())?;
        let dim = manifest.format.dimensions;
        let shared_bytes = store
            .read_object(&manifest.shared_routing)
            .map_err(|e| e.to_string())?;
        let mut nodes = BTreeMap::new();
        let mut navigation = BTreeMap::new();
        let mut validation = vec![];
        let mut vector_stores = BTreeMap::new();
        let mut posting_objects = BTreeMap::new();
        // v1 shared routing is a bare JSON list; v2 is a versioned object.
        let (layout, root) = if shared_bytes.trim_ascii_start().starts_with(b"[") {
            let shared: Vec<SavedNode> =
                serde_json::from_slice(&shared_bytes).map_err(|e| e.to_string())?;
            let root = shared.first().ok_or("missing global root")?.id;
            for node in shared {
                if nodes.insert(node.id, node).is_some() {
                    return Err("duplicate shared node".into());
                }
            }
            (SavedLayout::V1, root)
        } else {
            let shared: SavedSharedRoutingV2 =
                serde_json::from_slice(&shared_bytes).map_err(|e| e.to_string())?;
            if shared.version != 2 {
                return Err("unsupported saved shared routing format".into());
            }
            let root = shared.nodes.first().ok_or("missing global root")?.id;
            let mut ids = BTreeSet::new();
            if shared.nodes.iter().any(|n| !ids.insert(n.id)) {
                return Err("duplicate shared node".into());
            }
            install_v2(
                &store,
                shared.nodes,
                &shared.navigation,
                None,
                root,
                dim,
                &mut nodes,
                &mut navigation,
            )?;
            validation.push(shared.validation);
            (SavedLayout::V2, root)
        };
        for r in &manifest.owners {
            let owner: OwnerManifest =
                serde_json::from_slice(&store.read_object(r).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            let mut index = vec![];
            let mut named_navigation = BTreeMap::new();
            let mut named_validation = BTreeMap::new();
            for r in &owner.nodes {
                let bytes = store.read_object(r).map_err(|e| e.to_string())?;
                let version = serde_json::from_slice::<SavedVersion>(&bytes)
                    .map_err(|e| e.to_string())?
                    .version;
                match (layout, version) {
                    (SavedLayout::V1, 1) => {
                        let saved: SavedOwnerRouting =
                            serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
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
                    (SavedLayout::V2, 2) => {
                        let saved: SavedOwnerRoutingV2 =
                            serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
                        index.extend(saved.postings);
                        install_v2(
                            &store,
                            saved.nodes,
                            &saved.navigation,
                            Some(owner.owner),
                            root,
                            dim,
                            &mut nodes,
                            &mut navigation,
                        )?;
                        named_navigation.insert(saved.navigation.path.clone(), saved.navigation);
                        named_validation
                            .insert(saved.validation.path.clone(), saved.validation.clone());
                        validation.push(saved.validation);
                        if let Some(v) = saved.vectors {
                            if v.owner != owner.owner
                                || v.dimensions != dim
                                || vector_stores.insert(owner.owner, v).is_some()
                            {
                                return Err("invalid owner vector store reference".into());
                            }
                        }
                    }
                    (_, 1 | 2) => return Err("saved routing layouts differ".into()),
                    _ => return Err("unsupported saved owner routing format".into()),
                }
            }
            // Routing-named v2 objects are exactly the ones the manifest declares,
            // so copies and integrity checks cover everything a reader opens.
            let declared = |refs: &[ObjectRef]| -> BTreeMap<String, ObjectRef> {
                refs.iter().map(|r| (r.path.clone(), r.clone())).collect()
            };
            if declared(&owner.navigation) != named_navigation
                || declared(&owner.validation) != named_validation
            {
                return Err("owner manifest differs from its routing navigation objects".into());
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
            let centroid = match layout {
                SavedLayout::V1 => valid_centroid(&n.centroid, dim),
                SavedLayout::V2 => n.centroid.is_empty() && n.centroid_code.is_empty(),
            };
            if !centroid || n.leaf != n.children.is_empty() {
                return Err("invalid saved node shape".into());
            }
            if layout == SavedLayout::V1 {
                codes.insert(
                    id,
                    PackedFourBitCentroid::from_bytes(&n.centroid_code, dim)
                        .map_err(|e| e.to_string())?,
                );
            }
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
        if layout == SavedLayout::V1 {
            navigation = pack_saved_children(&nodes, codes, dim)?;
        }
        Ok(Self {
            store,
            manifest,
            layout,
            nodes,
            navigation,
            posting_objects,
            validation,
            vector_stores,
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
        CanonicalVectorSource::open(self.vector_descriptor_at(path)?)
    }
    /// The generation-bound corpus descriptor, optionally relocated.
    pub fn vector_descriptor_at(
        &self,
        path: Option<&std::path::Path>,
    ) -> Result<VectorFileDescriptor> {
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
        Ok(descriptor)
    }
    /// Recompute every persisted code from independent durable vectors. This
    /// checks more than counts: stale postings and centroid visibility fail.
    /// v2 reads exact internal centroids from the validation objects and leaf
    /// centroids from posting objects, and checks every packed sibling code.
    pub fn validate_codes(&self, source: &dyn VectorSource) -> Result<(u64, u64)> {
        let dim = self.manifest.format.dimensions;
        let width = Code::<1>::size(dim);
        let check_postings = |p: &SavedPostingList, centroid: &[f32]| -> Result<u64> {
            for (id, saved) in p.ids.iter().zip(p.codes.chunks_exact(width)) {
                let vector = source.get(*id)?;
                if vector.len() != dim {
                    return Err("full-vector dimension mismatch during code validation".into());
                }
                let expected = Code::<1>::quantize(&vector, centroid);
                if expected.as_ref() != saved {
                    return Err(format!(
                        "persisted one-bit posting differs from vector {id}"
                    ));
                }
            }
            Ok(p.ids.len() as u64)
        };
        let mut vectors = 0u64;
        if self.layout == SavedLayout::V1 {
            for node in self.nodes.values() {
                if origin_code(&node.centroid).to_bytes() != node.centroid_code {
                    return Err("persisted four-bit centroid differs from its centroid".into());
                }
            }
            self.visit_verified_postings(|leaf, p| {
                vectors += check_postings(&p, &self.nodes[&leaf].centroid)?;
                Ok(())
            })?;
            return Ok((self.nodes.len() as u64, vectors));
        }
        // Every node but the root is scored at one (parent, ordinal) slot.
        let mut slots = BTreeMap::new();
        for (parent, navigation) in &self.navigation {
            for (ordinal, (child, _)) in navigation.children.iter().enumerate() {
                slots.insert(child.id, (*parent, ordinal));
            }
        }
        let mut checked = BTreeSet::new();
        let mut check_code = |id: u128, centroid: &[f32]| -> Result<()> {
            if !valid_centroid(centroid, dim) || !checked.insert(id) {
                return Err("invalid or repeated exact centroid".into());
            }
            let Some((parent, ordinal)) = slots.get(&id) else {
                return if id == self.root {
                    Ok(())
                } else {
                    Err("saved node has no packed parent code".into())
                };
            };
            let saved = self.navigation[parent]
                .codes
                .get(*ordinal)
                .ok_or("missing packed sibling code")?;
            if saved.to_bytes() != origin_code(centroid).to_bytes() {
                return Err("persisted four-bit centroid differs from its centroid".into());
            }
            Ok(())
        };
        for r in &self.validation {
            let saved: SavedValidationV2 =
                bincode::deserialize(&self.store.read_object(r).map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            if saved.version != 2 {
                return Err("unsupported saved validation format".into());
            }
            for (id, centroid) in saved.centroids {
                if !self.nodes.get(&id).is_some_and(|n| !n.leaf) {
                    return Err("validation centroid names no internal node".into());
                }
                check_code(id, &centroid)?;
            }
        }
        self.visit_verified_postings(|leaf, p| {
            check_code(leaf, &p.centroid)?;
            vectors += check_postings(&p, &p.centroid)?;
            Ok(())
        })?;
        if checked.len() != self.nodes.len() {
            return Err("saved node lacks an exact centroid".into());
        }
        Ok((self.nodes.len() as u64, vectors))
    }
    /// Bytes of one persisted 4-bit centroid code, the unit navigation scores:
    /// the `PackedFourBitCentroid::to_bytes` length (16-byte header + nibbles).
    pub fn centroid_code_bytes(&self) -> usize {
        16 + self.manifest.format.dimensions.div_ceil(16) * 8
    }
    pub fn posting_list(&self, leaf: u128) -> Result<SavedPostingList> {
        let r = self
            .posting_objects
            .get(&leaf)
            .ok_or("unknown posting leaf")?;
        let bytes = self.store.read_object(r).map_err(|e| e.to_string())?;
        let dim = self.manifest.format.dimensions;
        let p = match self.layout {
            SavedLayout::V1 => {
                let mut rows: Vec<SavedPostingRowV1> =
                    bincode::deserialize(&bytes).map_err(|e| e.to_string())?;
                if rows.len() != 1 || rows[0].leaf != leaf {
                    return Err("posting object differs from its indexed leaf".into());
                }
                let row = rows.pop().ok_or("missing posting row")?;
                SavedPostingList {
                    leaf: row.leaf,
                    ids: row.ids.into_owned(),
                    codes: row.codes.into_owned(),
                    centroid: vec![],
                }
            }
            SavedLayout::V2 => {
                let row: SavedPostingObjectV2 =
                    bincode::deserialize(&bytes).map_err(|e| e.to_string())?;
                if row.version != 2 || row.leaf != leaf {
                    return Err("posting object differs from its indexed leaf".into());
                }
                if !valid_centroid(&row.centroid, dim) {
                    return Err("invalid posting leaf centroid".into());
                }
                SavedPostingList {
                    leaf: row.leaf,
                    ids: row.ids.into_owned(),
                    codes: row.codes.into_owned(),
                    centroid: row.centroid.into_owned(),
                }
            }
        };
        if p.ids.len().checked_mul(Code::<1>::size(dim)) != Some(p.codes.len()) {
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
        visit_reader_batches(
            &leaves,
            workers,
            |leaf| self.posting_list(*leaf),
            |leaf, posting| visit(*leaf, posting),
        )
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
                &root.routing(self.manifest.format.dimensions),
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
            let centroid = p.scoring_centroid(self)?;
            let residual: Vec<_> = query.iter().zip(centroid).map(|(q, c)| q - c).collect();
            let norm = centroid.iter().map(|x| x * x).sum::<f32>().sqrt();
            let dot = centroid.iter().zip(query).map(|(c, q)| c * q).sum();
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
impl SavedReader {
    /// Score the children of internal nodes in node then child order. Remote
    /// expansion and local navigation share this, so both produce identical
    /// scores, and canonical assembly gives both the same candidate order.
    fn score_children(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        authority: Option<u64>,
        children: &mut Vec<ScoredChild>,
    ) -> Result<()> {
        let query = self.scorer.prepare_query(query);
        for target in nodes {
            let n = self
                .nodes
                .get(&target.id)
                .ok_or("unknown saved expansion node")?;
            if authority.is_some_and(|a| n.owner != a) || n.leaf {
                return Err("invalid saved expansion authority or node".into());
            }
            let navigation = &self.navigation[&n.id];
            let distances = navigation
                .codes
                .distances(&self.scorer, &query, &self.metric);
            for (ordinal, ((target, leaf), distance)) in
                navigation.children.iter().zip(distances).enumerate()
            {
                children.push(ScoredChild {
                    parent: n.id,
                    ordinal,
                    target: *target,
                    leaf: *leaf,
                    distance,
                    observed_revision: 0,
                });
            }
        }
        Ok(())
    }
    /// `score_children` with the nodes split into contiguous runs scored on
    /// rayon's global pool and concatenated in order, so children come out in
    /// exactly the sequential order. A wide beam expands about a thousand
    /// nodes per level, which one thread scores in about 12 ms at 100M.
    fn score_children_parallel(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        run: usize,
        children: &mut Vec<ScoredChild>,
    ) -> Result<()> {
        use rayon::prelude::*;
        if nodes.len() < run {
            return self.score_children(nodes, query, None, children);
        }
        let runs = nodes
            .par_chunks(run)
            .map(|run| {
                let mut scored = Vec::new();
                self.score_children(run, query, None, &mut scored)?;
                Ok(scored)
            })
            .collect::<Result<Vec<_>>>()?;
        for run in runs {
            children.extend(run);
        }
        Ok(())
    }
    /// Walk the whole saved tree in this process. Every routing node is
    /// resident, so no level builds owner requests or copies the query; a wide
    /// level is scored on rayon's global pool. Beam selection and candidate
    /// order match `query` exactly.
    pub fn navigate_local(
        &self,
        query: &[f32],
        policy: &ReadBeamPolicy,
    ) -> Result<(Vec<(NodeRef, f32)>, NavigationWork)> {
        self.navigate_local_with(query, policy, true)
    }
    /// `navigate_local` that scores every level on the calling thread when
    /// `parallel` is false. Both modes return identical leaves and scores.
    pub fn navigate_local_with(
        &self,
        query: &[f32],
        policy: &ReadBeamPolicy,
        parallel: bool,
    ) -> Result<(Vec<(NodeRef, f32)>, NavigationWork)> {
        if query.len() != self.manifest.format.dimensions || query.iter().any(|x| !x.is_finite()) {
            return Err("invalid query dimension or values".into());
        }
        let root = &self.nodes[&self.root];
        if root.leaf {
            return Ok((vec![(root.reference(), 0.0)], NavigationWork::default()));
        }
        let (mut leaves, work) = routing::navigate_batch(
            &LocalNavigation { reader: self, parallel },
            &root.routing(self.manifest.format.dimensions),
            &[query.to_vec()],
            policy,
            0,
            0,
            0,
            0,
            None,
        )?;
        Ok((leaves.remove(0), work))
    }
    /// `navigate_local` that also returns the internal nodes expanded at each
    /// step, root first. Entry `L` holds the nodes the beam kept at level `L`,
    /// so diagnostics see exactly the walk the query made.
    pub fn navigate_traced(
        &self,
        query: &[f32],
        policy: &ReadBeamPolicy,
    ) -> Result<(Vec<(NodeRef, f32)>, Vec<Vec<u128>>)> {
        let root = &self.nodes[&self.root];
        if root.leaf {
            return Ok((vec![(root.reference(), 0.0)], vec![]));
        }
        let backend = TracingNavigation {
            reader: self,
            frontiers: std::sync::Mutex::new(vec![]),
        };
        let (mut leaves, _) = routing::navigate_batch(
            &backend,
            &root.routing(self.manifest.format.dimensions),
            &[query.to_vec()],
            policy,
            0,
            0,
            0,
            0,
            None,
        )?;
        let frontiers = backend.frontiers.into_inner().map_err(|_| "trace lock poisoned")?;
        Ok((leaves.remove(0), frontiers))
    }
    pub fn node_parent(&self, id: u128) -> Option<u128> {
        self.nodes.get(&id).and_then(|n| n.parent)
    }
    pub fn node_child_count(&self, id: u128) -> usize {
        self.nodes.get(&id).map_or(0, |n| n.children.len())
    }
    pub fn root_id(&self) -> u128 {
        self.root
    }
    /// Every leaf in depth-first order, children in saved order, so each
    /// subtree's leaves are consecutive.
    pub fn leaves_depth_first(&self) -> Vec<u128> {
        let mut out = Vec::with_capacity(self.posting_objects.len());
        let mut stack = vec![self.root];
        while let Some(id) = stack.pop() {
            let Some(node) = self.nodes.get(&id) else { continue };
            if node.leaf {
                out.push(id);
            } else {
                stack.extend(node.children.iter().rev().map(|c| c.id));
            }
        }
        out
    }
    /// Every saved node as (id, parent, leaf, owner), in ID order.
    pub fn tree_shape(&self) -> impl Iterator<Item = (u128, Option<u128>, bool, u64)> + '_ {
        self.nodes.values().map(|n| (n.id, n.parent, n.leaf, n.owner))
    }
    /// The root as a navigation entry point. A v2 root has a zero placeholder
    /// centroid, since navigation reads only its length.
    pub fn root_routing(&self) -> RoutingNode {
        self.nodes[&self.root].routing(self.manifest.format.dimensions)
    }
    /// The owner recorded for a saved node, or None for an unknown ID.
    pub fn node_owner(&self, id: u128) -> Option<u64> {
        self.nodes.get(&id).map(|n| n.owner)
    }
    /// Exact float32 centroid of a v1 saved node; the 1-bit posting codes of a
    /// leaf describe each vector's offset from it. None for every v2 node:
    /// v2 leaves carry it in `SavedPostingList::centroid`.
    pub fn node_centroid(&self, id: u128) -> Option<&[f32]> {
        self.nodes
            .get(&id)
            .map(|n| n.centroid.as_slice())
            .filter(|c| !c.is_empty())
    }
    /// Each owner's full-vector store, as its v2 save attached it.
    pub fn owner_vector_stores(&self) -> BTreeMap<u64, VectorStoreRef> {
        self.vector_stores.clone()
    }
    /// Stored size of a leaf's posting object, the bytes a cold fetch reads.
    pub fn posting_object_bytes(&self, leaf: u128) -> Option<u64> {
        self.posting_objects.get(&leaf).map(|r| r.bytes)
    }
    pub fn leaves(&self) -> impl Iterator<Item = u128> + '_ {
        self.posting_objects.keys().copied()
    }
    pub fn metric(&self) -> &DistanceFunction {
        &self.metric
    }
}
/// Nodes per run when local navigation scores a level in parallel; a level
/// with fewer nodes is scored on the calling thread.
const PARALLEL_NAVIGATION_NODES: usize = 16;
/// Navigation backend that treats every saved node as local; `parallel`
/// scores wide levels on rayon's global pool.
struct LocalNavigation<'a> {
    reader: &'a SavedReader,
    parallel: bool,
}
impl ExpansionBackend for LocalNavigation<'_> {
    fn expand_local_nodes(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        _run: u128,
        _epoch: u64,
        _shared_sequence: u64,
        children: &mut Vec<ScoredChild>,
        _remote: &mut Vec<NodeRef>,
    ) -> Result<()> {
        if self.parallel {
            self.reader.score_children_parallel(nodes, query, PARALLEL_NAVIGATION_NODES, children)
        } else {
            self.reader.score_children(nodes, query, None, children)
        }
    }
    fn expand(&self, _authority: u64, _requests: Vec<ExpandRequest>) -> Result<Vec<ExpandReply>> {
        Err("local navigation sends no expansion requests".into())
    }
}
/// Local navigation that records each expanded frontier, for diagnostics.
struct TracingNavigation<'a> {
    reader: &'a SavedReader,
    frontiers: std::sync::Mutex<Vec<Vec<u128>>>,
}
impl ExpansionBackend for TracingNavigation<'_> {
    fn expand_local_nodes(
        &self,
        nodes: &[NodeRef],
        query: &[f32],
        _run: u128,
        _epoch: u64,
        _shared_sequence: u64,
        children: &mut Vec<ScoredChild>,
        _remote: &mut Vec<NodeRef>,
    ) -> Result<()> {
        self.frontiers
            .lock()
            .map_err(|_| "trace lock poisoned")?
            .push(nodes.iter().map(|n| n.id).collect());
        self.reader.score_children(nodes, query, None, children)
    }
    fn expand(&self, _authority: u64, _requests: Vec<ExpandRequest>) -> Result<Vec<ExpandReply>> {
        Err("local navigation sends no expansion requests".into())
    }
}
impl ExpansionBackend for SavedReader {
    fn expand(&self, authority: u64, requests: Vec<ExpandRequest>) -> Result<Vec<ExpandReply>> {
        requests
            .into_iter()
            .map(|r| {
                let mut children = vec![];
                self.score_children(&r.nodes, &r.query, Some(authority), &mut children)?;
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
        // With SPANN_INPUT_RECEIPT_DIR set, a current hash receipt (the
        // reader's mapped-corpus receipt: same file identity and digest) skips
        // the full-file hash. Each 100M ladder save otherwise hashes the
        // 307 GB input three times.
        if let Ok(dir) = std::env::var("SPANN_INPUT_RECEIPT_DIR") {
            super::mapped_vectors::verify_with_receipt(
                std::path::Path::new(&descriptor.path),
                &mut file,
                &descriptor.sha256,
                Some(std::path::Path::new(&dir)),
            )?;
            return Ok(Self {
                file,
                descriptor,
                resident: None,
            });
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
