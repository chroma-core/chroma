//! Exclusive coordinator elapsed attribution. Nested work pauses its parent.
use super::*;
use std::sync::{Arc,Mutex};
use std::time::Instant;
#[derive(Clone,Copy)]
pub(super) enum Stage { Other, Input, Clustering, Discovery, Eligibility, Registration, Structure }
impl Stage {
    fn index(self)->usize {self as usize}
    fn names()->[&'static str;7] {["other","split_input_preparation","clustering","neighbor_discovery","eligibility_scrub_trace","rerouting_registration","structural_install_merge"]}
}
struct Clock {
    last:u64,
    nanos:[u64;7],
    calls:[u64;7],
    stack:Vec<(u64,Stage)>,
    next:u64,
    valid:bool,
}
impl Clock {
    fn new()->Self {Self{last:0,nanos:[0;7],calls:[0;7],stack:vec![(0,Stage::Other)],next:1,valid:true}}
    fn charge(&mut self,now:u64) {if let Some((_,stage))=self.stack.last() {self.nanos[stage.index()]+=now.saturating_sub(self.last);}else{self.valid=false;}self.last=now;}
    fn enter(&mut self,stage:Stage,now:u64)->u64 {self.charge(now);let id=self.next;self.next+=1;self.calls[stage.index()]+=1;if self.stack.len()<128 {self.stack.push((id,stage));}else{self.valid=false;}id}
    fn leave(&mut self,id:u64,now:u64) {self.charge(now);if self.stack.last().map(|v|v.0)==Some(id)&&self.stack.len()>1 {self.stack.pop();}else {self.valid=false;}}
}
pub(super) struct Observation { start:Instant, clock:Mutex<Clock>, events_before:BTreeMap<String,usize> }
pub(super) struct Span { observation:Arc<Observation>, id:u64 }
impl Drop for Span {fn drop(&mut self) {let now=self.observation.elapsed();self.observation.clock.lock().unwrap().leave(self.id,now);}}
impl Observation {fn elapsed(&self)->u64 {self.start.elapsed().as_nanos() as u64}}
impl AuthoritativeTree {
    /// Start only around one joined serial balance call, never across add/save.
    pub fn begin_balance_cost_observation(&mut self)->Result<()> {
        self.usable()?;ensure!(self.dynamic_mode&&self.balance_cost.is_none(),"balance cost observation already active/non-dynamic");
        let events_before=self.event_counts.clone();
        self.balance_cost=Some(Arc::new(Observation{start:Instant::now(),clock:Mutex::new(Clock::new()),events_before}));Ok(())
    }
    pub(super) fn balance_cost_span(&self,stage:Stage)->Option<Span> {
        self.balance_cost.as_ref().map(|observation|{let id=observation.clock.lock().unwrap().enter(stage,observation.elapsed());Span{observation:observation.clone(),id}})
    }
    pub fn finish_balance_cost_observation(&mut self)->Result<Value> {
        let observation=self.balance_cost.take().context("balance cost observation absent")?;
        let elapsed=observation.elapsed();let unique=Arc::strong_count(&observation)==1;
        let mut clock=observation.clock.lock().unwrap();clock.charge(elapsed);
        ensure!(unique&&clock.valid&&clock.stack.len()==1,"balance cost observation active/invalid");
        let categories:BTreeMap<_,_>=Stage::names().into_iter().enumerate().map(|(i,name)|(name,json!({"exclusive_ns":clock.nanos[i],"entries":clock.calls[i]}))).collect();
        let sum:u64=clock.nanos.iter().sum();ensure!(sum==elapsed,"balance attribution does not reconcile");
        let work:BTreeMap<_,_>=self.event_counts.iter().map(|(kind,count)|(kind,count.saturating_sub(*observation.events_before.get(kind).unwrap_or(&0)))).collect();
        let mut depth=BTreeMap::new();let mut todo=vec![(self.routing.root,0usize)];let mut child_counts=vec![];
        while let Some((id,d))=todo.pop() {ensure!(depth.insert(id,d).is_none(),"balance shape duplicate/cycle");let node=self.routing.nodes.get(&id).context("balance shape missing node")?;if let Some(children)=&node.children {child_counts.push(children.len());todo.extend(children.iter().map(|id|(*id,d+1)));}}
        ensure!(depth.len()==self.routing.nodes.len(),"balance shape unreachable nodes");
        let mut live_by_leaf:BTreeMap<u32,usize>=self.branches.keys().map(|id|(*id,0)).collect();
        for (_,leaf) in self.locations.values() {*live_by_leaf.get_mut(leaf).context("balance shape location leaf missing")?+=1;}
        let mut leaves_by_owner=BTreeMap::<usize,usize>::new();for owner in self.branches.values() {*leaves_by_owner.entry(*owner).or_default()+=1;}
        let shape=json!({"nodes":depth.len(),"leaves":self.branches.len(),"max_depth":depth.values().max(),"internal_child_counts":child_counts,"live_rows_by_leaf":live_by_leaf,"leaves_by_owner":leaves_by_owner,"shape_collection":"Metadata-only after observed window; no vector fetch. Locations represent unique current global ownership; physical/stale rows remain in owner counters/census."});
        Ok(json!({"valid":true,"observed_window_ns":elapsed,"exclusive_sum_ns":sum,"categories":categories,"work_event_deltas":work,"shape_after":shape,
            "scope":"Coordinator elapsed, including actor waits, serialization and observer overhead; nested work is charged only to its innermost category. Not CPU time. Structural/registration categories exclude their nested clustering, input, eligibility and discovery. Other retains uncategorized sweep/validation/loop work."}))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn balance_cost_nested_recursion_reconciles_without_double_count() {
        let mut c=Clock::new();let structure=c.enter(Stage::Structure,10);let registration=c.enter(Stage::Registration,20);let nested=c.enter(Stage::Structure,30);let cluster=c.enter(Stage::Clustering,40);
        c.leave(cluster,60);c.leave(nested,70);c.leave(registration,80);c.leave(structure,100);c.charge(110);
        assert!(c.valid);assert_eq!(c.nanos,[20,0,20,0,0,20,50]);assert_eq!(c.nanos.iter().sum::<u64>(),110);assert_eq!(c.stack.len(),1);
    }
    #[test]
    fn balance_cost_out_of_order_or_outstanding_scope_is_invalid() {
        let mut c=Clock::new();let a=c.enter(Stage::Input,1);let _b=c.enter(Stage::Clustering,2);assert!(c.stack.len()>1);c.leave(a,3);assert!(!c.valid);
    }
}
