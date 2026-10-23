//! Exclusive owner mutation POC; structural operations use an opt-in prepared path. Posting/current-version state
//! exists only in actors. The coordinator stores routing and location metadata.
use super::super::{
    common::{effective_beam, ReadBeamPolicy},
    config::{HierarchicalSpannConfig, NavigationMode},
    persistance::*,
};
use super::{
    eligibility::{fp_eligible, QuantizedEligibility},
    HierarchicalSpannIds,
};
use anyhow::{bail, ensure, Context, Result};
use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider, BlockfileReader,
    BlockfileWriter, BlockfileWriterOptions,
};
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use chroma_types::hierarchical_spann::{
    HierarchicalInternalNode, HierarchicalLeafNode, HierarchicalSpannPostingList,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc::{self, SyncSender},
    thread::{self, JoinHandle},
    time::Duration,
};
use uuid::Uuid;
#[path = "authoritative_dynamic.rs"]
mod dynamic;
#[path = "authoritative_phases.rs"]
mod phases;
#[path = "authoritative_preparation.rs"]
mod preparation;
pub use phases::PhaseStatus;
#[path = "authoritative_split.rs"]
mod split;
#[path = "authoritative_structure.rs"]
mod structure;
pub use dynamic::ConstructionLimits;
fn storage<T, E: std::fmt::Display>(r: std::result::Result<T, E>) -> Result<T> {
    r.map_err(|e| anyhow::anyhow!("{e}"))
}
pub(super) fn checked_physical_budget(existing: usize, incoming: usize) -> Result<()> {
    ensure!(
        existing.checked_add(incoming).is_some_and(|n| n <= 256),
        "owner physical row cap"
    );
    Ok(())
}
#[derive(Clone, Debug, Serialize)]
struct Node {
    parent: Option<u32>,
    centroid: Vec<f32>,
    centroid_code: Vec<u8>,
    children: Option<Vec<u32>>,
}
#[derive(Clone)]
struct Routing {
    root: u32,
    next: u32,
    dim: usize,
    nodes: BTreeMap<u32, Node>,
    metric: DistanceFunction,
    config: HierarchicalSpannConfig,
}
impl Routing {
    async fn load(
        p: &BlockfileProvider,
        ids: &HierarchicalSpannIds,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        limits: &ConstructionLimits,
    ) -> Result<Self> {
        ensure!(
            config.write_navigation == NavigationMode::Fp,
            "FP-only authority routing"
        );
        ensure!(
            config.max_replicas == 1
                && config.split_threshold > 0
                && config.split_threshold <= limits.max_leaf_rows,
            "bounded no-replication configuration required"
        );
        let sm = storage(
            p.read::<u32, u32>(BlockfileReaderOptions::new(
                ids.scalar_metadata_id,
                String::new(),
            ))
            .await,
        )?;
        let root = storage(sm.get(PREFIX_ROOT, SINGLETON_KEY).await)?.context("root missing")?;
        let next =
            storage(sm.get(PREFIX_NEXT_NODE, SINGLETON_KEY).await)?.context("next node missing")?;
        let dim = storage(sm.get(PREFIX_DIM, SINGLETON_KEY).await)?.context("dimension missing")?
            as usize;
        ensure!(dim > 0 && dim <= limits.max_dimension, "dimension cap");
        let ln = storage(
            p.read::<u32, HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(
                ids.leaf_node_id,
                String::new(),
            ))
            .await,
        )?;
        let inn = storage(
            p.read::<u32, HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(
                ids.internal_node_id,
                String::new(),
            ))
            .await,
        )?;
        let vectors = storage(
            p.read::<u32, &'static [f32]>(BlockfileReaderOptions::new(
                ids.vector_data_id,
                String::new(),
            ))
            .await,
        )?;
        let mut nodes = BTreeMap::new();
        for (_, id, n) in storage(ln.get_range(""..="", ..).await)? {
            let c = storage(vectors.get(PREFIX_CENTROID, id).await)?
                .context("leaf centroid")?
                .to_vec();
            ensure!(
                nodes
                    .insert(
                        id,
                        Node {
                            parent: (n.parent != NO_PARENT).then_some(n.parent),
                            centroid: c,
                            centroid_code: n.centroid_code.to_vec(),
                            children: None
                        }
                    )
                    .is_none(),
                "duplicate node"
            );
        }
        for (_, id, n) in storage(inn.get_range(""..="", ..).await)? {
            let c = storage(vectors.get(PREFIX_CENTROID, id).await)?
                .context("internal centroid")?
                .to_vec();
            ensure!(
                nodes
                    .insert(
                        id,
                        Node {
                            parent: (n.parent != NO_PARENT).then_some(n.parent),
                            centroid: c,
                            centroid_code: n.centroid_code.to_vec(),
                            children: Some(n.children.to_vec())
                        }
                    )
                    .is_none(),
                "duplicate node"
            );
        }
        ensure!(nodes.len() <= limits.max_nodes, "routing node cap");
        let mut seen = BTreeSet::new();
        let mut q = vec![(root, None)];
        while let Some((id, parent)) = q.pop() {
            ensure!(seen.insert(id), "routing cycle");
            let n = nodes.get(&id).context("missing routing child")?;
            ensure!(
                n.parent == parent
                    && n.centroid.len() == dim
                    && n.centroid.iter().all(|v| v.is_finite()),
                "routing shape"
            );
            if let Some(children) = &n.children {
                ensure!(children.len() <= limits.max_nodes, "child cap");
                q.extend(children.iter().map(|&c| (c, Some(id))));
            }
        }
        ensure!(seen.len() == nodes.len(), "unreachable metadata");
        Ok(Self {
            root,
            next,
            dim,
            nodes,
            metric,
            config,
        })
    }
    fn policy(&self) -> ReadBeamPolicy {
        let c = &self.config;
        if c.write_level_taus.is_empty() && c.write_level_min_pcts.is_empty() {
            ReadBeamPolicy::uniform(Some(c.write_beam_tau), c.write_beam_min, c.write_beam_max)
        } else {
            let mut widths = vec![];
            let mut q = vec![self.root];
            while !q.is_empty() {
                widths.push(q.len());
                q = q
                    .iter()
                    .flat_map(|id| self.nodes[id].children.clone().unwrap_or_default())
                    .collect();
            }
            ReadBeamPolicy::with_level_overrides(
                Some(c.write_beam_tau),
                c.write_beam_min,
                c.write_beam_max,
                c.write_level_taus.clone(),
                c.write_level_min_pcts.clone(),
                widths.into_iter().skip(1).collect(),
            )
        }
    }
    fn candidates(&self, v: &[f32]) -> Result<Vec<(u32, f32)>> {
        ensure!(
            v.len() == self.dim && v.iter().all(|x| x.is_finite()),
            "invalid vector"
        );
        let Some(root) = self.nodes.get(&self.root) else {
            return Ok(vec![]);
        };
        if root.children.is_none() {
            return Ok(vec![(
                self.root,
                self.metric.distance(v, &self.nodes[&self.root].centroid),
            )]);
        }
        let policy = self.policy();
        let mut leaves = vec![];
        let mut beam = vec![self.root];
        let mut level = 0;
        loop {
            let mut scores = vec![];
            for id in &beam {
                if let Some(children) = self.nodes.get(id).and_then(|n| n.children.as_ref()) {
                    for child in children {
                        if let Some(node) = self.nodes.get(child) {
                            scores.push((*child, self.metric.distance(v, &node.centroid)));
                        }
                    }
                }
            }
            if scores.is_empty() {
                break;
            }
            level += 1;
            ensure!(
                level <= 128 && scores.iter().all(|(_, d)| d.is_finite()),
                "routing cap/nonfinite"
            );
            scores.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            let p = policy.level_params(level);
            let n = effective_beam(&scores, p.tau, p.beam_min, p.beam_max);
            scores.truncate(n);
            let mut next = vec![];
            for (id, d) in scores {
                if self.nodes[&id].children.is_none() {
                    leaves.push((id, d));
                } else {
                    next.push(id);
                }
            }
            if next.is_empty() {
                break;
            }
            beam = next;
        }
        leaves.sort_unstable_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        Ok(leaves)
    }
}
#[derive(Clone, Copy, Debug, Serialize)]
struct VectorRef {
    root: Uuid,
    key: u32,
}
#[derive(Clone, Debug, Serialize)]
struct Current {
    version: u8,
    leaf: u32,
    vector: VectorRef,
}
#[derive(Clone, Debug)]
struct Leaf {
    parent: u32,
    centroid: Vec<f32>,
    centroid_code: Vec<u8>,
    ids: Vec<u32>,
    versions: Vec<u8>,
    codes: Vec<u8>,
}
impl Leaf {
    fn bytes(&self) -> usize {
        self.centroid.len() * 4
            + self.centroid_code.len()
            + self.ids.len() * 4
            + self.versions.len()
            + self.codes.len()
    }
}
struct Branch {
    leaf: u32,
    data: Leaf,
    current: BTreeMap<u32, Current>,
}
#[derive(Default, Clone, Debug, Serialize)]
pub struct Counters {
    pub branch_posting_ids_examined: usize,
    pub current_rows: usize,
    pub physical_rows: usize,
    pub vector_gets: usize,
    pub centroid_gets: usize,
    pub returned_centroid_payload_bytes: usize,
    pub actor_commands: usize,
    pub exported_current_records: usize,
    pub accepted_current_records: usize,
    pub returned_vector_payload_bytes: usize,
    pub posting_bootstrap_payload_bytes: usize,
    pub moved_branch_payload_bytes: usize,
    pub registered_rows: usize,
    pub saved_posting_payload_bytes: usize,
    pub vector_root_opens: usize,
    pub vector_reader_cache_clears: usize,
    pub embedding_view_publications: usize,
    pub prepared_posting_payload_bytes: usize,
    pub prepared_current_records: usize,
    pub structural_evaluations: usize,
    pub split_preparation_workers: usize,
    pub split_preparation_calls: usize,
    pub split_preparation_rows: usize,
    pub split_preparation_worker_jobs: usize,
    pub split_preparation_peak_active_fetches: usize,
    pub split_preparation_root_opens: usize,
    pub split_preparation_reader_cache_clears: usize,
    pub split_preparation_payload_bytes: usize,
    pub split_preparation_revalidations: usize,
    pub split_preparation_errors: usize,
    pub split_preparation_ns: u64,
    pub split_preparation_peak_owned_payload_bytes: usize,
    pub split_preparation_temporary_vector_upper_bytes: usize,
    pub split_preparation_reader_payload_upper_bytes: usize,
    pub split_preparation_peak_worker_readers: usize,
    pub split_preparation_provider_obtains: u64,
    pub split_preparation_provider_hits: u64,
    pub split_preparation_provider_misses: u64,
    pub split_preparation_encoded_bytes: u64,
    pub split_preparation_storage_callbacks: u64,
    pub split_preparation_decode_successes: u64,
}
#[derive(Debug, Serialize)]
pub struct Lookup {
    pub owner: usize,
    pub id: u32,
    pub leaf: u32,
    pub version: u8,
    pub vector_bits: Vec<u32>,
    pub thread: String,
}
struct Envelope {
    command: Command,
    reply: SyncSender<std::result::Result<Reply, String>>,
}
enum Command {
    DiagnosticReadEnable,
    DiagnosticReadSnapshot,
    DiagnosticReadFinish,
    Structure(split::OwnerCommand),
    Ready,
    BranchIds(u32),
    Lookup(u32),
    LeafCount(u32),
    CanRegister(u32, u32),
    RegisterNew(u32, u32, VectorRef),
    AcceptBranch(Branch),
    MoveBranch(u32, usize, SyncSender<Envelope>),
    AcceptRow {
        from: usize,
        id: u32,
        previous: u8,
        next: u8,
        leaf: u32,
        vector: VectorRef,
        preserve_version: bool,
    },
    Reroute {
        id: u32,
        from_leaf: u32,
        to: usize,
        to_leaf: u32,
        vector: VectorRef,
        destination: SyncSender<Envelope>,
        preserve_version: bool,
    },
    Scan {
        leaf: u32,
        old: Vec<f32>,
        left: Vec<f32>,
        right: Vec<f32>,
        fp: bool,
    },
    Save {
        posting: BlockfileWriter,
        scalar: BlockfileWriter,
        leaf: BlockfileWriter,
    },
    Stats,
}
enum Reply {
    Structure(split::OwnerReply),
    Ready(Vec<(u32, u32)>),
    Lookup(Lookup),
    Count(usize),
    Ack(Value),
    Scan(Vec<u32>),
    Stats(Counters),
}
fn exchange(send: &SyncSender<Envelope>, command: Command) -> Result<Reply> {
    let (tx, rx) = mpsc::sync_channel(1);
    send.send(Envelope { command, reply: tx })
        .map_err(|_| anyhow::anyhow!("owner stopped"))?;
    rx.recv_timeout(Duration::from_secs(10))
        .context("owner reply timeout")?
        .map_err(anyhow::Error::msg)
}
struct State {
    owner: usize,
    provider: BlockfileProvider,
    dim: usize,
    metric: DistanceFunction,
    leaves: BTreeMap<u32, Leaf>,
    current: BTreeMap<u32, Current>,
    readers: BTreeMap<Uuid, BlockfileReader<'static, u32, &'static [f32]>>,
    counters: Counters,
    max_rows: usize,
    posting_limit: usize,
    physical_rows: usize,
    limits: ConstructionLimits,
    direct: bool,
    prepared: Option<split::PreparedBackup>,
    diagnostic_reads: bool,
    diagnostic_retired: Vec<chroma_blockstore::LoadedBlockObservation>,
    diagnostic_vector_ns: u64,
    // Append-only embeddings use this view; centroid references keep their own roots.
    embedding_view: Option<(Uuid, usize)>,
    phase_registration: Option<std::sync::Arc<phases::RegistrationObservation>>,
    split_preparation_workers: usize,
}
impl State {
    async fn load(
        owner: usize,
        p: BlockfileProvider,
        ids: &HierarchicalSpannIds,
        assigned: &[u32],
        dim: usize,
        metric: DistanceFunction,
        limit: usize,
        limits: ConstructionLimits,
        direct: bool,
    ) -> Result<Self> {
        let mut s = Self {
            owner,
            provider: p,
            dim,
            metric,
            leaves: BTreeMap::new(),
            current: BTreeMap::new(),
            readers: BTreeMap::new(),
            counters: Counters::default(),
            max_rows: limits.max_vectors,
            posting_limit: if direct { limits.max_leaf_rows } else { limit },
            physical_rows: 0,
            limits,
            direct,
            prepared: None,
            diagnostic_reads: false,
            diagnostic_retired: Vec::new(),
            diagnostic_vector_ns: 0,
            embedding_view: None,
            phase_registration: None,
            split_preparation_workers: 0,
        };
        if assigned.is_empty() {
            return Ok(s);
        }
        let pl = storage(
            s.provider
                .read::<u32, HierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
                    ids.posting_list_id,
                    String::new(),
                ))
                .await,
        )?;
        let sm = storage(
            s.provider
                .read::<u32, u32>(BlockfileReaderOptions::new(
                    ids.scalar_metadata_id,
                    String::new(),
                ))
                .await,
        )?;
        let ln = storage(
            s.provider
                .read::<u32, HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(
                    ids.leaf_node_id,
                    String::new(),
                ))
                .await,
        )?;
        for &id in assigned {
            let n = storage(ln.get("", id).await)?.context("assigned leaf missing")?;
            let centroid = s
                .vector(
                    VectorRef {
                        root: ids.vector_data_id,
                        key: id,
                    },
                    PREFIX_CENTROID,
                )
                .await?;
            let posting = storage(pl.get("", id).await)?.context("assigned posting missing")?;
            let leaf = Leaf {
                parent: n.parent,
                centroid,
                centroid_code: n.centroid_code.to_vec(),
                ids: posting.ids.to_vec(),
                versions: posting.versions.to_vec(),
                codes: posting.codes.to_vec(),
            };
            ensure!(
                leaf.ids.len() == n.length as usize
                    && leaf.ids.len() <= s.limits.max_leaf_rows
                    && leaf.versions.len() == leaf.ids.len()
                    && leaf.codes.len() == leaf.ids.len() * Code::<1>::size(dim),
                "invalid assigned posting"
            );
            s.counters.posting_bootstrap_payload_bytes += leaf.bytes();
            for (&vector, &stored) in leaf.ids.iter().zip(&leaf.versions) {
                let current = storage(sm.get(PREFIX_VERSION, vector).await)?
                    .context("owner version missing")?;
                ensure!(current <= u8::MAX as u32, "invalid current version");
                if current as u8 == stored && stored & 128 == 0 {
                    ensure!(
                        s.current
                            .insert(
                                vector,
                                Current {
                                    version: stored,
                                    leaf: id,
                                    vector: VectorRef {
                                        root: ids.vector_data_id,
                                        key: vector
                                    }
                                }
                            )
                            .is_none(),
                        "duplicate owner current membership"
                    );
                }
            }
            s.physical_rows += leaf.ids.len();
            s.leaves.insert(id, leaf);
            ensure!(
                s.physical_rows <= s.limits.max_owner_physical_rows,
                "owner physical row cap"
            );
        }
        ensure!(s.current.len() <= s.limits.max_vectors, "owner live cap");
        Ok(s)
    }
    async fn vector(&mut self, r: VectorRef, prefix: &str) -> Result<Vec<f32>> {
        let diagnostic_start = self.diagnostic_reads.then(std::time::Instant::now);
        let r = if prefix == PREFIX_EMBEDDING {
            self.embedding_view.filter(|(_, count)| (r.key as usize) < *count)
                .map(|(root, _)| VectorRef { root, key:r.key }).unwrap_or(r)
        } else { r };
        if prefix == PREFIX_EMBEDDING {
            self.counters.vector_gets += 1;
        } else {
            self.counters.centroid_gets += 1;
        }
        if !self.readers.contains_key(&r.root) {
            if self.readers.len() >= 16 {
                if self.diagnostic_reads {
                    ensure!(self.diagnostic_retired.len()+self.readers.len()<=256, "diagnostic retired reader cap");
                    for reader in self.readers.values() {
                        ensure!(reader.scalar_preparation_quiescent(), "diagnostic outstanding read");
                        if let Some(v)=reader.loaded_block_observation() {self.diagnostic_retired.push(v);}
                    }
                }
                self.readers.clear();
                self.counters.vector_reader_cache_clears += 1;
            }
            let mut reader = storage(
                self.provider
                    .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(r.root, String::new()))
                    .await,
            )?;
            if self.diagnostic_reads {reader.start_scalar_preparation_observation();}
            self.readers.insert(r.root, reader);
            self.counters.vector_root_opens += 1;
        }
        let reader = &self.readers[&r.root];
        let result = storage(reader.get(prefix, r.key).await)?
            .context("durable vector missing")?
            .to_vec();
        reader.clear_loaded_blocks();
        ensure!(
            result.len() == self.dim && result.iter().all(|v| v.is_finite()),
            "durable vector shape"
        );
        if prefix == PREFIX_EMBEDDING {
            self.counters.returned_vector_payload_bytes += result.len() * 4;
        } else {
            self.counters.returned_centroid_payload_bytes += result.len() * 4;
        }
        if let Some(start)=diagnostic_start {self.diagnostic_vector_ns+=start.elapsed().as_nanos() as u64;}
        Ok(result)
    }
    fn check_budget(&self, incoming: usize) -> Result<()> {
        ensure!(
            self.physical_rows
                .checked_add(incoming)
                .is_some_and(|n| n <= self.limits.max_owner_physical_rows),
            "owner physical row cap"
        );
        Ok(())
    }
    fn live_branch_ids(&mut self, leaf: u32) -> Result<Vec<u32>> {
        let l = self.leaves.get(&leaf).context("branch not owned")?;
        let ids: Vec<_> = l
            .ids
            .iter()
            .zip(&l.versions)
            .filter(|(id, v)| {
                self.current
                    .get(id)
                    .is_some_and(|c| c.leaf == leaf && c.version == **v)
            })
            .map(|(&id, _)| id)
            .collect();
        ensure!(
            ids.iter().copied().collect::<BTreeSet<_>>().len() == ids.len(),
            "duplicate branch current ID"
        );
        self.counters.branch_posting_ids_examined += l.ids.len();
        Ok(ids)
    }
    fn last_code(&self, leaf: u32) -> &[u8] {
        let codes = &self.leaves[&leaf].codes;
        &codes[codes.len() - Code::<1>::size(self.dim)..]
    }
    fn can_register(&self, leaf: u32, id: u32) -> Result<()> {
        let l = self.leaves.get(&leaf).context("leaf not owned")?;
        ensure!(
            !self.current.contains_key(&id)
                && l.ids.len() < self.posting_limit
                && self.current.len() < self.max_rows,
            "registration conflict or structural split required"
        );
        self.check_budget(1)?;
        Ok(())
    }
    async fn register(&mut self, leaf: u32, id: u32, version: u8, vector: VectorRef) -> Result<()> {
        self.can_register(leaf, id)?;
        let values = self.vector(vector, PREFIX_EMBEDDING).await?;
        let l = self.leaves.get_mut(&leaf).unwrap();
        let code = Code::<1>::quantize(&values, &l.centroid);
        l.ids.push(id);
        l.versions.push(version);
        l.codes.extend_from_slice(code.as_ref());
        self.physical_rows += 1;
        self.current.insert(
            id,
            Current {
                version,
                leaf,
                vector,
            },
        );
        self.counters.registered_rows += 1;
        Ok(())
    }
    async fn handle(&mut self, command: Command) -> Result<Reply> {
        self.counters.actor_commands += 1;
        match command {
            Command::DiagnosticReadFinish => {
                ensure!(self.diagnostic_reads, "diagnostic reads not enabled");
                for reader in self.readers.values() {ensure!(reader.scalar_preparation_quiescent(), "diagnostic outstanding read");}
                self.diagnostic_reads=false;
                self.readers.clear();self.diagnostic_retired.clear();
                self.counters.vector_reader_cache_clears += 1;
                Ok(Reply::Ack(json!({"finished":true})))
            }
            Command::DiagnosticReadEnable => {
                ensure!(!self.diagnostic_reads, "diagnostic already enabled");
                for reader in self.readers.values_mut() {reader.start_scalar_preparation_observation();}
                self.diagnostic_reads=true;
                Ok(Reply::Ack(json!({"enabled":true})))
            }
            Command::DiagnosticReadSnapshot => {
                ensure!(self.diagnostic_reads, "diagnostic not enabled");
                let mut observations=self.diagnostic_retired.clone();
                for reader in self.readers.values() {
                    ensure!(reader.scalar_preparation_quiescent(), "diagnostic outstanding read");
                    if let Some(v)=reader.loaded_block_observation() {observations.push(v);}
                }
                ensure!(observations.len()<=272 && observations.iter().all(|v|!v.access_overflow), "diagnostic profile overflow");
                Ok(Reply::Ack(json!({"valid":true,"open_reader_roots":self.readers.keys().collect::<Vec<_>>(),"vector_get_elapsed_ns":self.diagnostic_vector_ns,"readers":observations,"scope":"actual vector/centroid point path; root open excluded; storage-origin Arrow decodes; provider cache reconstruction excluded"})))
            }
            Command::Structure(command) => {
                self.structure_handle(command).await.map(Reply::Structure)
            }
            Command::Ready => Ok(Reply::Ready(
                self.current.iter().map(|(&id, c)| (id, c.leaf)).collect(),
            )),
            Command::Lookup(id) => {
                let c = self.current.get(&id).context("ID not owned")?.clone();
                let v = self.vector(c.vector, PREFIX_EMBEDDING).await?;
                Ok(Reply::Lookup(Lookup {
                    owner: self.owner,
                    id,
                    leaf: c.leaf,
                    version: c.version,
                    vector_bits: v.iter().map(|x| x.to_bits()).collect(),
                    thread: format!("{:?}", thread::current().id()),
                }))
            }
            Command::BranchIds(leaf) => Ok(Reply::Ready(
                self.live_branch_ids(leaf)?
                    .into_iter()
                    .map(|id| (id, leaf))
                    .collect(),
            )),
            Command::LeafCount(id) => Ok(Reply::Count(
                self.leaves.get(&id).context("leaf not owned")?.ids.len(),
            )),
            Command::CanRegister(leaf, id) => {
                self.can_register(leaf, id)?;
                Ok(Reply::Ack(json!({"checked":id})))
            }
            Command::RegisterNew(leaf, id, vector) => {
                let span=self.phase_registration.as_ref().map(|o|phases::RegistrationSpan::begin(o.clone()));
                let registered=self.register(leaf,id,1,vector).await;
                drop(span);
                registered?;
                Ok(Reply::Ack(
                    json!({"owner":self.owner,"id":id,"leaf":leaf,"version":1,"code":self.last_code(leaf)}),
                ))
            }
            Command::AcceptBranch(branch) => {
                self.check_budget(branch.data.ids.len())?;
                ensure!(
                    !self.leaves.contains_key(&branch.leaf)
                        && branch
                            .current
                            .keys()
                            .all(|id| !self.current.contains_key(id))
                        && self.current.len() + branch.current.len() <= self.max_rows,
                    "branch destination conflict"
                );
                let count = branch.current.len();
                self.current.extend(branch.current);
                self.physical_rows += branch.data.ids.len();
                self.leaves.insert(branch.leaf, branch.data);
                Ok(Reply::Ack(json!({"owner":self.owner,"live":count})))
            }
            Command::MoveBranch(leaf, to, destination) => {
                ensure!(to != self.owner, "same owner move");
                let ids = self.live_branch_ids(leaf)?;
                let data = self.leaves.remove(&leaf).context("move leaf not owned")?;
                self.physical_rows -= data.ids.len();
                let mut current = BTreeMap::new();
                for id in &ids {
                    current.insert(*id, self.current.remove(id).unwrap());
                }
                let bytes = data.bytes() + current.len() * (4 + 1 + 4 + 16 + 4);
                self.counters.moved_branch_payload_bytes += bytes;
                let response = exchange(
                    &destination,
                    Command::AcceptBranch(Branch {
                        leaf,
                        data,
                        current,
                    }),
                )?;
                ensure!(matches!(response, Reply::Ack(_)), "move reply");
                Ok(Reply::Ack(
                    json!({"from":self.owner,"to":to,"leaf":leaf,"live":ids.len(),"ids":ids,"moved_payload_bytes":bytes}),
                ))
            }
            Command::AcceptRow {
                from,
                id,
                previous,
                next,
                leaf,
                vector,
                preserve_version,
            } => {
                ensure!(
                    from != self.owner
                        && next
                            == if preserve_version {
                                previous
                            } else {
                                previous.wrapping_add(1) & 127
                            },
                    "version handoff invalid"
                );
                self.register(leaf, id, next, vector).await?;
                self.counters.accepted_current_records += 1;
                Ok(Reply::Ack(
                    json!({"from":from,"to":self.owner,"id":id,"leaf":leaf,"previous":previous,"version":next,"code":self.last_code(leaf)}),
                ))
            }
            Command::Reroute {
                id,
                from_leaf,
                to,
                to_leaf,
                vector,
                destination,
                preserve_version,
            } => {
                let c = self
                    .current
                    .get(&id)
                    .context("source ID not owned")?
                    .clone();
                ensure!(
                    c.leaf == from_leaf
                        && c.version & 128 == 0
                        && (to_leaf != from_leaf || self.prepared.is_some() || self.direct),
                    "source leaf/version mismatch"
                );
                let next = if preserve_version {
                    c.version
                } else {
                    c.version.wrapping_add(1) & 127
                };
                if to == self.owner {
                    self.check_budget(1)?;
                    let l = self
                        .leaves
                        .get(&to_leaf)
                        .context("destination leaf not owned")?;
                    ensure!(
                        l.ids.len() < self.posting_limit,
                        "structural split required"
                    );
                    let values = self.vector(vector, PREFIX_EMBEDDING).await?;
                    self.current.remove(&id);
                    let l = self.leaves.get_mut(&to_leaf).unwrap();
                    l.codes
                        .extend_from_slice(Code::<1>::quantize(&values, &l.centroid).as_ref());
                    l.ids.push(id);
                    l.versions.push(next);
                    self.physical_rows += 1;
                    self.current.insert(
                        id,
                        Current {
                            version: next,
                            leaf: to_leaf,
                            vector,
                        },
                    );
                    self.counters.registered_rows += 1;
                    return Ok(Reply::Ack(
                        json!({"from":self.owner,"to":to,"id":id,"leaf":to_leaf,"previous":c.version,"version":next,"code":self.last_code(to_leaf)}),
                    ));
                }
                ensure!(
                    matches!(
                        exchange(&destination, Command::CanRegister(to_leaf, id))?,
                        Reply::Ack(_)
                    ),
                    "destination admission"
                );
                self.current.remove(&id);
                let r = exchange(
                    &destination,
                    Command::AcceptRow {
                        from: self.owner,
                        id,
                        previous: c.version,
                        next,
                        leaf: to_leaf,
                        vector,
                        preserve_version,
                    },
                )?;
                self.counters.exported_current_records += 1;
                Ok(r)
            }
            Command::Scan {
                leaf,
                old,
                left,
                right,
                fp,
            } => {
                ensure!(
                    [&old, &left, &right]
                        .iter()
                        .all(|v| v.len() == self.dim && v.iter().all(|x| x.is_finite())),
                    "scan centers"
                );
                let l = self.leaves.get(&leaf).context("scan leaf not owned")?;
                let centroid = l.centroid.clone();
                let rows: Vec<_> = l
                    .ids
                    .iter()
                    .zip(&l.versions)
                    .enumerate()
                    .filter_map(|(i, (&id, &v))| {
                        self.current
                            .get(&id)
                            .filter(|c| c.leaf == leaf && c.version == v)
                            .map(|c| {
                                (
                                    id,
                                    c.clone(),
                                    l.codes[i * Code::<1>::size(self.dim)
                                        ..(i + 1) * Code::<1>::size(self.dim)]
                                        .to_vec(),
                                )
                            })
                    })
                    .collect();
                let q = (!fp).then(|| {
                    QuantizedEligibility::new(
                        &centroid,
                        &old,
                        &left,
                        &right,
                        Code::<1>::packed_len(self.dim),
                    )
                });
                let mut eligible = vec![];
                for (id, c, code) in rows {
                    let yes = if fp {
                        let v = self.vector(c.vector, PREFIX_EMBEDDING).await?;
                        fp_eligible(&self.metric, &v, &centroid, &old, &left, &right)
                    } else {
                        q.as_ref().unwrap().eligible(&self.metric, &code)
                    };
                    if yes {
                        eligible.push(id);
                    }
                }
                Ok(Reply::Scan(eligible))
            }
            Command::Save {
                posting,
                scalar,
                leaf,
            } => {
                for (&id, l) in &self.leaves {
                    storage(
                        posting
                            .set(
                                "",
                                id,
                                HierarchicalSpannPostingList {
                                    codes: &l.codes,
                                    ids: &l.ids,
                                    versions: &l.versions,
                                },
                            )
                            .await,
                    )?;
                    storage(
                        leaf.set(
                            "",
                            id,
                            HierarchicalLeafNode {
                                parent: l.parent,
                                length: l.ids.len() as u32,
                                centroid_code: &l.centroid_code,
                            },
                        )
                        .await,
                    )?;
                    self.counters.saved_posting_payload_bytes +=
                        l.ids.len() * 4 + l.versions.len() + l.codes.len();
                }
                for (&id, c) in &self.current {
                    storage(scalar.set(PREFIX_VERSION, id, u32::from(c.version)).await)?;
                }
                Ok(Reply::Ack(
                    json!({"owner":self.owner,"saved_current":self.current.len(),"saved_leaves":self.leaves.len()}),
                ))
            }
            Command::Stats => {
                let mut counters = self.counters.clone();
                counters.current_rows = self.current.len();
                counters.physical_rows = self.physical_rows;
                Ok(Reply::Stats(counters))
            }
        }
    }
}
struct Actor {
    send: Option<SyncSender<Envelope>>,
    join: Option<JoinHandle<()>>,
}
impl Actor {
    fn new(
        owner: usize,
        p: BlockfileProvider,
        ids: HierarchicalSpannIds,
        leaves: Vec<u32>,
        dim: usize,
        metric: DistanceFunction,
        limit: usize,
        limits: ConstructionLimits,
        direct: bool,
    ) -> Result<Self> {
        let (send, recv) = mpsc::sync_channel::<Envelope>(1);
        let (ready, rx) = mpsc::sync_channel(1);
        let join = thread::Builder::new()
            .name(format!("authoritative-owner-{owner}"))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                let state = rt.block_on(State::load(
                    owner, p, &ids, &leaves, dim, metric, limit, limits, direct,
                ));
                match state {
                    Err(e) => {
                        let _ = ready.send(Err(e.to_string()));
                    }
                    Ok(mut state) => {
                        let _ = ready.send(Ok(()));
                        while let Ok(envelope) = recv.recv() {
                            let result = rt.block_on(state.handle(envelope.command));
                            let _ = envelope.reply.send(result.map_err(|e| e.to_string()));
                        }
                    }
                }
            })?;
        let mut actor = Self {
            send: Some(send),
            join: Some(join),
        };
        if let Err(e) = rx
            .recv_timeout(Duration::from_secs(10))
            .context("owner bootstrap timeout")?
            .map_err(anyhow::Error::msg)
        {
            actor.send.take();
            return Err(e);
        }
        Ok(actor)
    }
    fn call(&self, c: Command) -> Result<Reply> {
        exchange(self.send.as_ref().context("owner closed")?, c)
    }
    fn sender(&self) -> Result<SyncSender<Envelope>> {
        Ok(self.send.as_ref().context("owner closed")?.clone())
    }
}
impl Drop for Actor {
    fn drop(&mut self) {
        self.send.take();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}
/// No posting, code, current-version or corpus embedding fields exist here.
/// Locations are metadata for resolving the one authoritative owner.
#[derive(Default, Clone, Serialize)]
struct BeforeScrubAttribution {
    trace_preparations: u64,
    physical_requests: u64,
    physical_rows: u64,
    reference_queries: u64,
    reference_successes: u64,
    batch_validation_calls: u64,
    batch_validation_successes: u64,
    batch_validated_rows: u64,
    trace_preparation_ns: u64,
    semantic_scrub_calls: u64,
    semantic_live_rows: u64,
    semantic_scrub_ns: u64,
}
pub struct AuthoritativeTree {
    routing: Routing,
    owners: BTreeMap<usize, Actor>,
    locations: BTreeMap<u32, (usize, u32)>,
    branches: BTreeMap<u32, usize>,
    provider: BlockfileProvider,
    parent: HierarchicalSpannIds,
    latest_vectors: Uuid,
    construction_embedding_view_active: bool,
    phase: Option<phases::Phases>,
    poisoned: bool,
    pub events: Vec<Value>,
    tombstones: BTreeSet<u32>,
    limits: ConstructionLimits,
    dynamic_mode: bool,
    event_counts: BTreeMap<String, usize>,
    trace_overflow: bool,
    vector_batch_forks: usize,
    placement_id_checks: usize,
    lazy_disabled_before_scrub_trace: bool,
    before_scrub_attribution: Option<BeforeScrubAttribution>,
    lazy_disabled_evaluation_trace: bool,
    elided_trace_reference_checks: usize,
    retained_trace_reference_checks: usize,
    #[cfg(test)]
    balance_rounds: std::collections::VecDeque<Vec<u32>>,
    #[cfg(test)]
    require_split_tape: bool,
    #[cfg(test)]
    split_decisions: std::collections::VecDeque<super::live_owner_tests::SplitDecision>,
}
impl AuthoritativeTree {
    pub async fn open(
        p: BlockfileProvider,
        ids: HierarchicalSpannIds,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        rows: usize,
        owner_slots: usize,
    ) -> Result<Self> {
        Self::open_limited(
            p,
            ids,
            metric,
            config,
            rows,
            owner_slots,
            ConstructionLimits::legacy(),
            false,
        )
        .await
    }
    async fn open_limited(
        p: BlockfileProvider,
        ids: HierarchicalSpannIds,
        metric: DistanceFunction,
        config: HierarchicalSpannConfig,
        rows: usize,
        owner_slots: usize,
        limits: ConstructionLimits,
        dynamic_mode: bool,
    ) -> Result<Self> {
        ensure!(
            rows > 0
                && rows <= limits.max_vectors
                && owner_slots > 0
                && owner_slots <= limits.max_owners,
            "bounded POC rows/owners"
        );
        let routing = Routing::load(&p, &ids, metric.clone(), config.clone(), &limits).await?;
        let leaves: Vec<_> = routing
            .nodes
            .iter()
            .filter(|(_, n)| n.children.is_none())
            .map(|(&id, _)| id)
            .collect();
        let mut owners = BTreeMap::new();
        for owner in 0..owner_slots {
            owners.insert(
                owner,
                Actor::new(
                    owner,
                    p.clone(),
                    ids.clone(),
                    if owner == 0 { leaves.clone() } else { vec![] },
                    routing.dim,
                    metric.clone(),
                    config.split_threshold,
                    limits.clone(),
                    dynamic_mode,
                )?,
            );
        }
        let Reply::Ready(ready) = owners[&0].call(Command::Ready)? else {
            bail!("bootstrap reply");
        };
        let mut locations = BTreeMap::new();
        for (id, leaf) in ready {
            ensure!(
                locations.insert(id, (0, leaf)).is_none(),
                "duplicate initial ID"
            );
        }
        ensure!(
            locations.len() == rows && locations.keys().copied().eq(0..rows as u32),
            "complete dense initial namespace required"
        );
        let latest_vectors = ids.vector_data_id;
        Ok(Self {
            routing,
            owners,
            locations,
            branches: leaves.into_iter().map(|l| (l, 0)).collect(),
            provider: p,
            parent: ids,
            latest_vectors,
            construction_embedding_view_active: false,
            phase: None,
            poisoned: false,
            events: vec![],
            tombstones: BTreeSet::new(),
            limits,
            dynamic_mode,
            event_counts: Default::default(),
            trace_overflow: false,
            vector_batch_forks: 0,
            placement_id_checks: 0,
            lazy_disabled_before_scrub_trace: false,
            before_scrub_attribution: None,
            lazy_disabled_evaluation_trace: false,
            elided_trace_reference_checks: 0,
            retained_trace_reference_checks: 0,
            #[cfg(test)]
            balance_rounds: Default::default(),
            #[cfg(test)]
            require_split_tape: false,
            #[cfg(test)]
            split_decisions: Default::default(),
        })
    }
    fn owner(&self, owner: usize) -> Result<&Actor> {
        self.owners.get(&owner).context("owner missing")
    }
    fn usable(&self) -> Result<()> {
        ensure!(!self.poisoned, "incomplete mutation; snapshot forbidden");
        Ok(())
    }
    pub fn locations(&self) -> &BTreeMap<u32, (usize, u32)> {
        &self.locations
    }
    pub fn branch_owners(&self) -> &BTreeMap<u32, usize> {
        &self.branches
    }
    pub fn centroid(&self, leaf: u32) -> Result<&[f32]> {
        Ok(&self
            .routing
            .nodes
            .get(&leaf)
            .context("unknown leaf")?
            .centroid)
    }
    pub fn candidates(&self, v: &[f32]) -> Result<Vec<(u32, f32)>> {
        self.routing.candidates(v)
    }
    pub fn lookup_at(&self, owner: usize, id: u32) -> Result<Lookup> {
        let Reply::Lookup(r) = self.owner(owner)?.call(Command::Lookup(id))? else {
            bail!("lookup reply")
        };
        Ok(r)
    }
    pub fn leaf_count(&self, leaf: u32) -> Result<usize> {
        let owner = *self.branches.get(&leaf).context("unknown leaf")?;
        let Reply::Count(n) = self.owner(owner)?.call(Command::LeafCount(leaf))? else {
            bail!("count reply")
        };
        Ok(n)
    }
    pub fn move_branch(&mut self, leaf: u32, to: usize) -> Result<()> {
        self.phase_require_reshard()?;
        self.usable()?;
        let from = *self.branches.get(&leaf).context("unknown branch")?;
        ensure!(from != to, "same owner");
        let destination = self.owner(to)?.sender()?;
        let Reply::Ready(rows) = self.owner(from)?.call(Command::BranchIds(leaf))? else {
            bail!("branch ID reply");
        };
        let expected: Vec<_> = rows
            .iter()
            .map(|(id, l)| {
                ensure!(
                    *l == leaf && self.locations.get(id) == Some(&(from, leaf)),
                    "branch location mismatch"
                );
                Ok(*id)
            })
            .collect::<Result<_>>()?;
        self.placement_id_checks += expected.len();
        self.poisoned = true;
        let Reply::Ack(reply) =
            self.owner(from)?
                .call(Command::MoveBranch(leaf, to, destination))?
        else {
            bail!("move reply")
        };
        ensure!(
            reply["live"].as_u64() == Some(expected.len() as u64)
                && reply["ids"] == json!(expected),
            "move coverage"
        );
        for id in expected {
            let loc = self
                .locations
                .get_mut(&id)
                .context("moved ID location missing")?;
            ensure!(*loc == (from, leaf), "moved location changed");
            *loc = (to, leaf);
            self.placement_id_checks += 1;
        }
        self.branches.insert(leaf, to);
        self.record(json!({"kind":"move","reply":reply}));
        self.poisoned = false;
        Ok(())
    }
    async fn save_vector(&mut self, id: u32, values: Vec<f32>) -> Result<VectorRef> {
        let w = storage(
            self.provider
                .write::<u32, Vec<f32>>(
                    BlockfileWriterOptions::new(String::new()).fork(self.latest_vectors),
                )
                .await,
        )?;
        storage(w.set(PREFIX_EMBEDDING, id, values).await)?;
        let f = storage(w.commit::<u32, Vec<f32>>().await)?;
        let root = f.id();
        storage(f.flush::<u32, Vec<f32>>().await)?;
        self.latest_vectors = root;
        Ok(VectorRef { root, key: id })
    }
    pub async fn add_new(&mut self, id: u32, values: Vec<f32>) -> Result<()> {
        ensure!(self.phase.is_none(), "phased construction requires reserved chunk insertion");
        self.usable()?;
        ensure!(
            self.locations.len() < 128
                && id == self.locations.len() as u32
                && !self.locations.contains_key(&id),
            "dense new ID required"
        );
        let candidates = self.routing.candidates(&values)?;
        let leaf = candidates.first().context("no global destination")?.0;
        let owner = *self
            .branches
            .get(&leaf)
            .context("destination owner missing")?;
        self.owner(owner)?.call(Command::CanRegister(leaf, id))?;
        self.poisoned = true;
        let vector = self.save_vector(id, values).await?;
        let Reply::Ack(reply) = self
            .owner(owner)?
            .call(Command::RegisterNew(leaf, id, vector))?
        else {
            bail!("register reply")
        };
        self.locations.insert(id, (owner, leaf));
        self.record(json!({"kind":"add","id":id,"candidates":candidates.iter().map(|(i,d)|(*i,d.to_bits())).collect::<Vec<_>>(),"reply":reply}));
        self.poisoned = false;
        Ok(())
    }
    /// Replace one durable vector and reroute through the reference global policy.
    /// Reject same-leaf replacement: the reference reassign path would not re-encode
    /// its posting, so that update is outside this bounded operation contract.
    pub async fn replace_and_reroute(&mut self, id: u32, values: Vec<f32>) -> Result<()> {
        ensure!(self.phase.is_none(), "phased construction is append-only");
        self.usable()?;
        ensure!(!self.construction_embedding_view_active, "replacement unsupported with append-only construction embedding view");
        let (from, from_leaf) = *self.locations.get(&id).context("ID missing")?;
        let candidates = self.routing.candidates(&values)?;
        let leaf = candidates.first().context("no destination")?.0;
        ensure!(leaf != from_leaf, "same-leaf replacement unsupported");
        let to = *self.branches.get(&leaf).context("target owner missing")?;
        let count = self.leaf_count(leaf)?;
        ensure!(
            count < self.routing.config.split_threshold,
            "structural split required"
        );
        let destination = self.owner(to)?.sender()?;
        self.poisoned = true;
        let vector = self.save_vector(id, values).await?;
        let Reply::Ack(reply) = self.owner(from)?.call(Command::Reroute {
            id,
            from_leaf,
            to,
            to_leaf: leaf,
            vector,
            destination,
            preserve_version: false,
        })?
        else {
            bail!("handoff reply")
        };
        self.locations.insert(id, (to, leaf));
        self.record(json!({"kind":"reroute","id":id,"candidates":candidates.iter().map(|(i,d)|(*i,d.to_bits())).collect::<Vec<_>>(),"reply":reply}));
        self.poisoned = false;
        Ok(())
    }
    pub fn scan_at(
        &self,
        owner: usize,
        leaf: u32,
        old: Vec<f32>,
        left: Vec<f32>,
        right: Vec<f32>,
        fp: bool,
    ) -> Result<Vec<u32>> {
        self.usable()?;
        let Reply::Scan(ids) = self.owner(owner)?.call(Command::Scan {
            leaf,
            old,
            left,
            right,
            fp,
        })?
        else {
            bail!("scan reply")
        };
        Ok(ids)
    }
    pub fn reject_structural_edit(&self) -> Result<()> {
        bail!("fixed-topology authority slice: split/merge/parent edits unsupported")
    }
    /// Diagnostic-only activation at a completed actor command boundary.
    pub fn enable_diagnostic_reads(&self) -> Result<()> {
        for actor in self.owners.values() {ensure!(matches!(actor.call(Command::DiagnosticReadEnable)?,Reply::Ack(_)),"diagnostic enable reply");}
        Ok(())
    }
    /// End a joined measurement interval and release its instrumented reader handles.
    pub fn finish_diagnostic_reads(&self) -> Result<()> {
        for actor in self.owners.values() {ensure!(matches!(actor.call(Command::DiagnosticReadFinish)?,Reply::Ack(_)),"diagnostic finish reply");}
        Ok(())
    }
    pub fn diagnostic_read_snapshot(&self) -> Result<Value> {
        let mut owners=serde_json::Map::new();
        for (id,actor) in &self.owners {let Reply::Ack(value)=actor.call(Command::DiagnosticReadSnapshot)? else {bail!("diagnostic snapshot reply")};owners.insert(id.to_string(),value);}
        Ok(Value::Object(owners))
    }
    pub fn counters(&self) -> Result<BTreeMap<usize, Counters>> {
        self.owners
            .iter()
            .map(|(&i, a)| {
                let Reply::Stats(s) = a.call(Command::Stats)? else {
                    bail!("stats reply")
                };
                Ok((i, s))
            })
            .collect()
    }
    /// Actors set their own postings/versions directly into private fork writers.
    /// The coordinator commits complete roots only after all owner save replies.
    pub async fn save(&mut self) -> Result<HierarchicalSpannIds> {
        self.usable()?;
        self.phase_require_ready()?;
        self.poisoned = true;
        let pl = storage(
            self.provider
                .write::<u32, HierarchicalSpannPostingList<'static>>(
                    BlockfileWriterOptions::new(String::new()).fork(self.parent.posting_list_id),
                )
                .await,
        )?;
        let sm = storage(
            self.provider
                .write::<u32, u32>(
                    BlockfileWriterOptions::new(String::new()).fork(self.parent.scalar_metadata_id),
                )
                .await,
        )?;
        let ln = storage(
            self.provider
                .write::<u32, HierarchicalLeafNode<'static>>(
                    BlockfileWriterOptions::new(String::new()).fork(self.parent.leaf_node_id),
                )
                .await,
        )?;
        let owner_ids: Vec<_> = self.owners.keys().copied().collect();
        for owner in owner_ids {
            let Reply::Ack(reply) = self.owner(owner)?.call(Command::Save {
                posting: pl.clone(),
                scalar: sm.clone(),
                leaf: ln.clone(),
            })?
            else {
                bail!("owner save reply")
            };
            self.record(json!({"kind":"owner-save","owner":owner,"reply":reply}));
        }
        let (saved_vectors, saved_internal) = self.save_structure(&pl, &sm, &ln).await?;
        storage(
            sm.set(
                PREFIX_MAX_VECTOR_ID,
                SINGLETON_KEY,
                *self
                    .locations
                    .keys()
                    .next_back()
                    .context("empty snapshot")?,
            )
            .await,
        )?;
        let pl = storage(
            pl.commit::<u32, HierarchicalSpannPostingList<'static>>()
                .await,
        )?;
        let sm = storage(sm.commit::<u32, u32>().await)?;
        let ln = storage(ln.commit::<u32, HierarchicalLeafNode<'static>>().await)?;
        let ids = HierarchicalSpannIds {
            posting_list_id: pl.id(),
            scalar_metadata_id: sm.id(),
            vector_data_id: saved_vectors,
            leaf_node_id: ln.id(),
            internal_node_id: saved_internal,
        };
        storage(
            pl.flush::<u32, HierarchicalSpannPostingList<'static>>()
                .await,
        )?;
        storage(sm.flush::<u32, u32>().await)?;
        storage(ln.flush::<u32, HierarchicalLeafNode<'static>>().await)?;
        self.latest_vectors = ids.vector_data_id;
        self.tombstones.clear();
        self.parent = ids.clone();
        self.poisoned = false;
        Ok(ids)
    }
}
