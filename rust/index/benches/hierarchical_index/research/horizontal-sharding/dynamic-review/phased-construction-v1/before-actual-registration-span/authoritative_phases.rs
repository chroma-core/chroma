//! Opt-in joined construction phases. Placement changes only between completed
//! add/balance phases. Routing is central; owner registration can run concurrently.
use super::*;
use super::split::{OwnerCommand,OwnerReply};
use std::sync::atomic::{AtomicUsize,Ordering};

#[derive(Clone,Copy,Debug,PartialEq,Eq,Serialize)]
pub enum PhaseStatus { Ready, Adding, NeedsBalance, Balancing, NeedsReshard, Resharding }
pub(super) struct Phases {
    status:PhaseStatus,
    remaining:usize,
    workers:usize,
    completed:usize,
    registered:usize,
    worker_jobs:usize,
    peak_outstanding:usize,
    reservations:Vec<Value>,
}
impl AuthoritativeTree {
    pub fn enable_construction_phases(&mut self, owner_registration_workers:usize) -> Result<()> {
        self.usable()?;
        ensure!(self.dynamic_mode && self.limits.max_nodes<=10000 && self.phase.is_none() && (1..=8).contains(&owner_registration_workers),"phase activation differs");
        self.phase=Some(Phases {status:PhaseStatus::Ready,remaining:0,workers:owner_registration_workers,completed:0,registered:0,worker_jobs:0,peak_outstanding:0,reservations:vec![]});
        Ok(())
    }
    pub fn construction_phase_report(&self) -> Value {
        self.phase.as_ref().map(|p|json!({"status":p.status,"remaining_rows":p.remaining,"configured_owner_workers":p.workers,"completed_phases":p.completed,"registered_rows":p.registered,"worker_jobs":p.worker_jobs,"peak_outstanding_owner_requests":p.peak_outstanding,"reservations":p.reservations,"balance_round_limit_hits":self.event_counts.get("balance-round-limit").copied().unwrap_or(0),"balance_execution":"serialized existing global semantics","parallel_scope":"owner registration only; central routing serial; request overlap is not CPU utilization"})).unwrap_or(Value::Null)
    }
    /// Conservatively reserve the whole phase at every possible destination.
    /// The extra physical-row allowance is explicit, not a proof against arbitrary
    /// repeated reassignment. Existing per-registration hard limits remain active.
    pub fn reserve_construction_phase(&mut self, rows:usize, balance_headroom_rows:usize) -> Result<()> {
        self.usable()?;
        self.phase_require_ready()?;
        let p=self.phase.as_ref().context("phases not enabled")?;
        ensure!(p.reservations.len()<32,"phase report cap");
        ensure!(rows>0 && balance_headroom_rows>=rows && self.locations.len().checked_add(rows).is_some_and(|n|n<=self.limits.max_vectors),"phase reservation range/headroom");
        let incoming=rows.checked_add(balance_headroom_rows).context("phase headroom overflow")?;
        let mut capacities=vec![];
        for owner in self.owners.keys().copied().collect::<Vec<_>>() {
            let OwnerReply::PhaseCapacity {physical,live,leaves}=self.structural(owner,OwnerCommand::PhaseCapacity)? else {bail!("phase capacity reply")};
            ensure!(physical.checked_add(incoming).is_some_and(|n|n<=self.limits.max_owner_physical_rows),"conservative owner headroom exhausted");
            ensure!(leaves.iter().all(|(_,n)|n.checked_add(incoming).is_some_and(|v|v<=self.limits.max_leaf_rows)),"conservative leaf headroom exhausted");
            capacities.push(json!({"owner":owner,"physical_rows":physical,"live_rows":live,"leaf_physical_rows":leaves}));
        }
        let p=self.phase.as_mut().unwrap();p.remaining=rows;p.status=PhaseStatus::Adding;
        p.reservations.push(json!({"rows":rows,"balance_headroom_rows":balance_headroom_rows,"owners":capacities}));
        Ok(())
    }
    pub fn finish_construction_add(&mut self) -> Result<()> {
        self.usable()?;self.phase_require_finished_add()?;
        ensure!(self.phase.is_some(),"phases not enabled");
        self.publish_current_embedding_view()?;
        self.phase.as_mut().unwrap().status=PhaseStatus::NeedsBalance;
        Ok(())
    }
    pub fn balance_construction_phase(&mut self) -> Result<()> {
        self.usable()?;
        ensure!(self.phase.as_ref().is_some_and(|p|p.status==PhaseStatus::NeedsBalance),"balance requires completed published add");
        self.phase.as_mut().unwrap().status=PhaseStatus::Balancing;
        self.balance_dynamic_current()?;
        self.phase.as_mut().unwrap().status=PhaseStatus::NeedsReshard;
        Ok(())
    }
    pub fn reshard_construction_phase(&mut self) -> Result<()> {
        self.usable()?;
        let status=self.phase.as_ref().context("phases not enabled")?.status;
        ensure!(matches!(status,PhaseStatus::Ready|PhaseStatus::NeedsReshard),"reshard requires joined phase boundary");
        self.phase.as_mut().unwrap().status=PhaseStatus::Resharding;
        // Direct actor transfers preserve the tree. A failed transfer poisons the
        // instance through the existing operation; there is no recovery protocol.
        if self.limits.move_above.is_some() {if let Err(error)=self.rebalance_dynamic_placement() {self.poisoned=true;return Err(error);}}
        let p=self.phase.as_mut().unwrap();p.status=PhaseStatus::Ready;
        if status==PhaseStatus::NeedsReshard {p.completed+=1;}
        Ok(())
    }
    pub(super) fn phase_require_ready(&self)->Result<()> {ensure!(self.phase.as_ref().is_none_or(|p|p.status==PhaseStatus::Ready),"complete add/balance/reshard before this operation");Ok(())}
    pub(super) fn phase_require_reshard(&self)->Result<()> {ensure!(self.phase.as_ref().is_none_or(|p|p.status==PhaseStatus::Resharding),"placement requires explicit reshard boundary");Ok(())}
    pub(super) fn phase_require_finished_add(&self)->Result<()> {ensure!(self.phase.as_ref().is_none_or(|p|p.status==PhaseStatus::Adding&&p.remaining==0),"publication requires all reserved rows joined");Ok(())}
    pub(super) fn phase_require_balancing(&self)->Result<()> {ensure!(self.phase.as_ref().is_none_or(|p|p.status==PhaseStatus::Balancing),"use explicit balance phase");Ok(())}
    pub(super) fn phase_admit_chunk(&self,rows:usize,cadence:Option<bool>)->Result<()> {
        if let Some(p)=&self.phase {ensure!(p.status==PhaseStatus::Adding && cadence.is_none() && rows>0 && rows<=p.remaining,"chunk outside reserved add phase");}
        Ok(())
    }
    pub(super) fn register_phased_rows(&mut self,rows:&[(u32,Vec<f32>)],root:Uuid)->Result<()> {
        // RegisterNew changes postings/versions only. It cannot edit topology or
        // centroids, so the route for another new ID is independent of earlier adds.
        let mut routes=vec![];let mut candidates_total=0usize;
        let mut owners=BTreeMap::<usize,Vec<(usize,u32,u32)>>::new();
        for (index,(id,values)) in rows.iter().enumerate() {
            let candidates=self.routing.candidates(values)?;
            candidates_total=candidates_total.checked_add(candidates.len()).context("route count overflow")?;
            ensure!(candidates_total<=1_000_000,"phase route preparation cap");
            let leaf=candidates.first().context("phase destination missing")?.0;
            let owner=*self.branches.get(&leaf).context("phase destination owner missing")?;
            self.owner(owner)?;owners.entry(owner).or_default().push((index,leaf,*id));routes.push((owner,leaf,candidates));
        }
        let count=self.phase.as_ref().context("phases not enabled")?.workers.min(owners.len());
        let mut jobs:Vec<Vec<_>>=(0..count).map(|_|vec![]).collect();
        for (index,(owner,work)) in owners.into_iter().enumerate() {jobs[index%count].push((self.owner(owner)?.sender()?,work));}
        let outstanding=AtomicUsize::new(0);let peak=AtomicUsize::new(0);
        let responses=std::thread::scope(|scope| {
            let mut handles=vec![];
            for work in jobs {let active=&outstanding;let high=&peak;handles.push(scope.spawn(move||->Result<Vec<(usize,Value)>> {
                let mut results=vec![];
                for (sender,rows) in work {for (index,leaf,id) in rows {
                    let n=active.fetch_add(1,Ordering::SeqCst)+1;high.fetch_max(n,Ordering::SeqCst);
                    let reply=exchange(&sender,Command::RegisterNew(leaf,id,VectorRef {root,key:id}));
                    active.fetch_sub(1,Ordering::SeqCst);
                    let Reply::Ack(value)=reply? else {bail!("phase register reply")};results.push((index,value));
                }}Ok(results)
            }));}
            // Join every worker even when another reports an error.
            handles.into_iter().map(|h|h.join().map_err(|_|anyhow::anyhow!("phase worker panicked")).and_then(|v|v)).collect::<Vec<_>>()
        });
        let mut replies=BTreeMap::new();for response in responses {for (index,value) in response? {ensure!(replies.insert(index,value).is_none(),"duplicate phase registration reply");}}
        ensure!(replies.len()==rows.len(),"phase reply coverage");
        for (index,(id,values)) in rows.iter().enumerate() {
            let (owner,leaf,candidates)=&routes[index];let reply=&replies[&index];
            ensure!(reply["owner"].as_u64()==Some(*owner as u64)&&reply["id"].as_u64()==Some(*id as u64)&&reply["leaf"].as_u64()==Some(*leaf as u64)&&reply["version"].as_u64()==Some(1),"phase registration scope");
            self.locations.insert(*id,(*owner,*leaf));
            self.record(json!({"kind":"add_destination","id":id,"version":1,"candidates":candidates.iter().map(|(i,d)|(*i,d.to_bits())).collect::<Vec<_>>(),"selected":[leaf]}));
            self.record(json!({"kind":"leaf_registered","leaf":leaf,"owner":owner,"id":id,"version":1,"bits":values.iter().map(|v|v.to_bits()).collect::<Vec<_>>(),"code":reply["code"]}));
        }
        let p=self.phase.as_mut().unwrap();p.remaining-=rows.len();p.registered+=rows.len();p.worker_jobs+=count;p.peak_outstanding=p.peak_outstanding.max(peak.load(Ordering::SeqCst));
        Ok(())
    }
}
