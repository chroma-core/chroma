//! Opt-in live neighbor predicate dispatch. Reference mutation stays synchronous
//! and centralized; actors own per-neighbor source payloads, not durable stores.
use super::super::common::TreeNode;
use super::{
    eligibility::{fp_eligible, QuantizedEligibility},
    HierarchicalSpannWriter,
};
use anyhow::{ensure, Context, Result};
use chroma_distance::DistanceFunction;
use chroma_index::quantization::Code;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc::{self, SyncSender},
    thread::{self, JoinHandle},
    time::Duration,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    ReferenceTrace,
    OwnerDispatch,
}
#[derive(Clone, Debug, Serialize)]
pub(super) struct Source {
    pub leaf: u32,
    pub centroid: Vec<f32>,
    pub ids: Vec<u32>,
    pub versions: Vec<u8>,
    pub codes: Vec<u8>,
    pub vectors: Vec<Option<Vec<f32>>>,
    pub old: Vec<f32>,
    pub left: Vec<f32>,
    pub right: Vec<f32>,
    pub fp: bool,
}
impl Source {
    fn hash(&self) -> String {
        format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(self).expect("finite admitted source"))
        )
    }
    fn bytes(&self) -> usize {
        self.centroid.len() * 4
            + self.ids.len() * 4
            + self.versions.len()
            + self.codes.len()
            + self
                .vectors
                .iter()
                .flatten()
                .map(|v| v.len() * 4)
                .sum::<usize>()
            + (self.old.len() + self.left.len() + self.right.len()) * 4
    }
}
struct Prepared {
    source: Source,
    quant: Option<QuantizedEligibility>,
}
impl Prepared {
    fn new(source: Source, dim: usize) -> Result<Self> {
        ensure!(
            source.centroid.len() == dim
                && source.ids.len() <= 100_000
                && source.versions.len() == source.ids.len()
                && source.vectors.len() == source.ids.len()
                && source.ids.len().checked_mul(Code::<1>::size(dim)) == Some(source.codes.len()),
            "bad live source"
        );
        ensure!(
            [&source.centroid, &source.old, &source.left, &source.right]
                .iter()
                .all(|v| v.len() == dim && v.iter().all(|x| x.is_finite()))
                && source
                    .vectors
                    .iter()
                    .flatten()
                    .all(|v| v.len() == dim && v.iter().all(|x| x.is_finite())),
            "bad live source dimensions"
        );
        let quant = (!source.fp).then(|| {
            QuantizedEligibility::new(
                &source.centroid,
                &source.old,
                &source.left,
                &source.right,
                Code::<1>::packed_len(dim),
            )
        });
        Ok(Self { source, quant })
    }
    fn evaluate(
        &self,
        index: usize,
        current: u8,
        metric: &DistanceFunction,
        dim: usize,
    ) -> Result<bool> {
        ensure!(
            index < self.source.ids.len() && self.source.versions[index] == current,
            "actor version/index mismatch"
        );
        if self.source.fp {
            let v = self.source.vectors[index]
                .as_ref()
                .context("missing evaluated FP vector")?;
            Ok(fp_eligible(
                metric,
                v,
                &self.source.centroid,
                &self.source.old,
                &self.source.left,
                &self.source.right,
            ))
        } else {
            let size = Code::<1>::size(dim);
            Ok(self
                .quant
                .as_ref()
                .unwrap()
                .eligible(metric, &self.source.codes[index * size..(index + 1) * size]))
        }
    }
}
enum Command {
    Begin(u64, Source),
    Evaluate(u64, usize, u8),
    End(u64),
}
struct Message {
    command: Command,
    reply: SyncSender<std::result::Result<Value, String>>,
}
struct Actor {
    send: Option<SyncSender<Message>>,
    join: Option<JoinHandle<()>>,
}
impl Actor {
    fn new(owner: usize, dim: usize, metric: DistanceFunction) -> Result<Self> {
        let (send, recv) = mpsc::sync_channel::<Message>(1);
        let join=thread::Builder::new().name(format!("live-owner-{owner}")).spawn(move||{let mut sources=BTreeMap::<u64,Prepared>::new();while let Ok(message)=recv.recv(){let result=(||match message.command{Command::Begin(session,source)=>{ensure!(sources.len()<128 && !sources.contains_key(&session),"live session cap/duplicate");let hash=source.hash();sources.insert(session,Prepared::new(source,dim)?);Ok(json!({"source":hash,"thread":format!("{:?}",thread::current().id())}))},Command::Evaluate(session,index,current)=>{let s=sources.get(&session).context("actor session missing")?;let eligible=s.evaluate(index,current,&metric,dim)?;Ok(json!({"eligible":eligible,"id":s.source.ids[index],"version":current,"thread":format!("{:?}",thread::current().id())}))},Command::End(session)=>{ensure!(sources.remove(&session).is_some(),"actor session missing at end");Ok(json!({"ended":session}))}})();let _=message.reply.send(result.map_err(|e:anyhow::Error|e.to_string()));}})?;
        Ok(Self {
            send: Some(send),
            join: Some(join),
        })
    }
    fn call(&self, command: Command) -> Result<Value> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send
            .as_ref()
            .context("actor closed")?
            .send(Message { command, reply: tx })
            .map_err(|_| anyhow::anyhow!("actor unavailable"))?;
        rx.recv_timeout(Duration::from_secs(5))
            .context("actor reply timeout")?
            .map_err(anyhow::Error::msg)
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
#[derive(Clone, Copy)]
pub(super) struct Session {
    pub id: u64,
    pub owner: usize,
}
#[derive(Default)]
struct State {
    directory: BTreeMap<u32, usize>,
    history: BTreeMap<u32, usize>,
    scope_owners: BTreeMap<u64, usize>,
    next: u64,
    events: Vec<Value>,
    transport: Vec<Value>,
    ledger_bytes: usize,
}
pub struct LiveOwnerHook {
    mode: Mode,
    state: Mutex<State>,
    actors: BTreeMap<usize, Actor>,
}
impl LiveOwnerHook {
    pub fn new(
        mode: Mode,
        directory: BTreeMap<u32, usize>,
        owners: usize,
        dim: usize,
        metric: DistanceFunction,
    ) -> Result<Self> {
        ensure!(
            owners > 0
                && owners <= 128
                && directory.len() <= 100_000
                && directory.values().all(|o| *o < owners),
            "live directory admission"
        );
        let mut actors = BTreeMap::new();
        if mode == Mode::OwnerDispatch {
            for owner in 0..owners {
                actors.insert(owner, Actor::new(owner, dim, metric.clone())?);
            }
        }
        Ok(Self {
            mode,
            state: Mutex::new(State {
                history: directory.clone(),
                directory,
                ..Default::default()
            }),
            actors,
        })
    }
    fn event_locked(s: &mut State, event: Value) {
        s.ledger_bytes = s
            .ledger_bytes
            .checked_add(serde_json::to_vec(&event).unwrap().len())
            .expect("ledger overflow");
        assert!(
            s.ledger_bytes <= 16 * 1024 * 1024 && s.events.len() < 100_000,
            "live trace cap"
        );
        s.events.push(event);
    }
    fn transport_locked(s: &mut State, event: Value) {
        s.ledger_bytes = s
            .ledger_bytes
            .checked_add(serde_json::to_vec(&event).unwrap().len())
            .expect("transport ledger overflow");
        assert!(
            s.ledger_bytes <= 16 * 1024 * 1024 && s.transport.len() < 100_000,
            "live transport trace cap"
        );
        s.transport.push(event);
    }
    pub fn events(&self) -> Vec<Value> {
        self.state.lock().events.clone()
    }
    pub fn transport(&self) -> Vec<Value> {
        self.state.lock().transport.clone()
    }
    pub fn directory(&self) -> BTreeMap<u32, usize> {
        self.state.lock().directory.clone()
    }
    pub(super) fn split(&self, old: u32, left: u32, right: u32) {
        let mut s = self.state.lock();
        let owner = *s
            .directory
            .get(&old)
            .or_else(|| s.history.get(&old))
            .expect("split source owner missing");
        s.directory.remove(&old);
        s.directory.insert(left, owner);
        s.directory.insert(right, owner);
        s.history.insert(left, owner);
        s.history.insert(right, owner);
        Self::event_locked(
            &mut s,
            json!({"kind":"split","old":old,"left":left,"right":right,"owner":owner}),
        );
    }
    pub(super) fn discovery(
        &self,
        old: u32,
        left: u32,
        right: u32,
        neighbors: &[(u32, f32)],
    ) -> u64 {
        let mut s = self.state.lock();
        s.next += 1;
        let scope = s.next;
        let owner = *s.history.get(&old).expect("discovery source owner missing");
        s.scope_owners.insert(scope, owner);
        Self::event_locked(
            &mut s,
            json!({"kind":"discovery","scope":scope,"old":old,"left":left,"right":right,"neighbors":neighbors.iter().map(|(i,d)|(*i,d.to_bits())).collect::<Vec<_>>()}),
        );
        scope
    }
    pub(super) fn before_scrub<I:super::super::common::VectorKey>(&self, w: &HierarchicalSpannWriter<I>, scope: u64, leaf: u32) {
        let rows = w.nodes.get(&leaf).and_then(|n| match n.value() {
            TreeNode::Leaf(l) => Some(
                l.ids
                    .iter()
                    .zip(&l.versions)
                    .map(|(&id, &v)| (id, v, w.current_version_sync(id)))
                    .collect::<Vec<_>>(),
            ),
            _ => None,
        });
        let mut s = self.state.lock();
        Self::event_locked(
            &mut s,
            json!({"kind":"before_scrub","scope":scope,"leaf":leaf,"versions":rows}),
        );
    }
    pub(super) fn begin(&self, scope: u64, source: Source) -> Session {
        let bytes = source.bytes();
        assert!(
            bytes <= 8 * 1024 * 1024,
            "live per-neighbor source payload cap"
        );
        let hash = source.hash();
        let mut s = self.state.lock();
        let owner = *s
            .directory
            .get(&source.leaf)
            .expect("neighbor owner missing");
        s.next += 1;
        let id = s.next;
        let split_owner = s.scope_owners[&scope];
        Self::event_locked(
            &mut s,
            json!({"kind":"begin","scope":scope,"session":id,"owner":owner,"split_owner":split_owner,"leaf":source.leaf,"source":hash,"ids":source.ids,"versions":source.versions}),
        );
        drop(s);
        if self.mode == Mode::OwnerDispatch {
            let reply = self
                .actors
                .get(&owner)
                .expect("owner actor missing")
                .call(Command::Begin(id, source))
                .expect("live source install failed");
            assert_eq!(reply["source"].as_str(), Some(hash.as_str()));
            let mut s = self.state.lock();
            Self::transport_locked(
                &mut s,
                json!({"kind":"install","session":id,"owner":owner,"copied_payload_bytes":bytes,"reply":reply}),
            );
        }
        Session { id, owner }
    }
    pub(super) fn version(&self, session: Session, index: usize, id: u32, stored: u8, current: u8) {
        let mut s = self.state.lock();
        Self::event_locked(
            &mut s,
            json!({"kind":"version","session":session.id,"index":index,"id":id,"stored":stored,"current":current}),
        );
    }
    pub(super) fn evaluate(
        &self,
        session: Session,
        index: usize,
        id: u32,
        current: u8,
        reference: impl FnOnce() -> bool,
    ) -> bool {
        let eligible = if self.mode == Mode::OwnerDispatch {
            let reply = self
                .actors
                .get(&session.owner)
                .expect("owner actor missing")
                .call(Command::Evaluate(session.id, index, current))
                .expect("live predicate failed");
            assert_eq!(reply["id"].as_u64(), Some(u64::from(id)));
            assert_eq!(reply["version"].as_u64(), Some(u64::from(current)));
            let eligible = reply["eligible"].as_bool().expect("bool actor result");
            let mut state = self.state.lock();
            Self::transport_locked(
                &mut state,
                json!({"kind":"evaluate","session":session.id,"index":index,"current":current,"reply":reply}),
            );
            eligible
        } else {
            reference()
        };
        let mut s = self.state.lock();
        Self::event_locked(
            &mut s,
            json!({"kind":"eligible","session":session.id,"index":index,"id":id,"eligible":eligible}),
        );
        eligible
    }
    pub(super) fn end(&self, session: Session) {
        if self.mode == Mode::OwnerDispatch {
            self.actors[&session.owner]
                .call(Command::End(session.id))
                .expect("live session end");
        }
        let mut s = self.state.lock();
        Self::event_locked(&mut s, json!({"kind":"end","session":session.id}));
    }
    pub(super) fn finish_scope(&self, scope: u64) {
        self.state.lock().scope_owners.remove(&scope);
    }
    pub(super) fn add_start(&self, id: u32, vector: &[f32]) {
        let mut s = self.state.lock();
        Self::event_locked(
            &mut s,
            json!({"kind":"add","id":id,"bits":vector.iter().map(|x|x.to_bits()).collect::<Vec<_>>()}),
        );
    }
    pub(super) fn leaf_registered(
        &self,
        leaf: u32,
        id: u32,
        version: u8,
        embedding: &[f32],
        code: &[u8],
    ) {
        let mut s = self.state.lock();
        let owner = *s
            .directory
            .get(&leaf)
            .expect("registered leaf owner missing");
        Self::event_locked(
            &mut s,
            json!({"kind":"leaf_registered","leaf":leaf,"owner":owner,"id":id,"version":version,"bits":embedding.iter().map(|x|x.to_bits()).collect::<Vec<_>>(),"code":code}),
        );
    }
    pub(super) fn add_navigation(
        &self,
        id: u32,
        version: u8,
        candidates: &[(u32, f32)],
        selected: &[u32],
    ) {
        let mut s = self.state.lock();
        Self::event_locked(
            &mut s,
            json!({"kind":"add_destination","id":id,"version":version,"candidates":candidates.iter().map(|(i,d)|(*i,d.to_bits())).collect::<Vec<_>>(),"selected":selected}),
        );
    }
    pub(super) fn navigation(
        &self,
        id: u32,
        from: u32,
        current: u8,
        candidates: &[(u32, f32)],
        selected: &[u32],
    ) {
        let mut s = self.state.lock();
        Self::event_locked(
            &mut s,
            json!({"kind":"destination","id":id,"from":from,"current":current,"candidates":candidates.iter().map(|(i,d)|(*i,d.to_bits())).collect::<Vec<_>>(),"selected":selected}),
        );
    }
    pub(super) fn registered<I:super::super::common::VectorKey>(&self, w: &HierarchicalSpannWriter<I>, id: I, clusters: &[u32]) {
        let current = w.current_version_sync(id);
        self.refresh(w);
        let mut s = self.state.lock();
        let owners: Vec<_> = clusters
            .iter()
            .map(|c| (*c, *s.directory.get(c).expect("registration owner missing")))
            .collect();
        Self::event_locked(
            &mut s,
            json!({"kind":"registered","id":id,"current":current,"destinations":owners}),
        );
    }
    pub(super) fn refresh<I:super::super::common::VectorKey>(&self, w: &HierarchicalSpannWriter<I>) {
        let mut leaves = BTreeSet::new();
        let mut seen = BTreeSet::new();
        let mut pending = vec![w.root_id()];
        while let Some(id) = pending.pop() {
            assert!(
                seen.insert(id) && seen.len() <= 100_000,
                "refresh topology cap"
            );
            let node = w.nodes.get(&id).expect("refresh missing node");
            match node.value() {
                TreeNode::Internal(n) => pending.extend(n.children.iter().copied()),
                TreeNode::Leaf(_) => {
                    leaves.insert(id);
                }
            }
        }
        let mut s = self.state.lock();
        s.directory.retain(|id, _| leaves.contains(id));
        assert_eq!(
            s.directory.keys().copied().collect::<BTreeSet<_>>(),
            leaves,
            "unknown live branch owner"
        );
    }
}
