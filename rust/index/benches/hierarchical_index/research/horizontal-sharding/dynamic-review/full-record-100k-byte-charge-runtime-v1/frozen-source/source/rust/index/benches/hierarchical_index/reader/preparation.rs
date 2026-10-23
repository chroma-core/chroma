//! Bounded observations of one exclusively owned selected-posting request.
use std::{collections::BTreeMap,sync::{Arc,Mutex},time::Instant};
use serde::Serialize;

pub(super) fn nanos(start:Instant)->u64 {start.elapsed().as_nanos().min(u64::MAX as u128) as u64}
#[derive(Default,Serialize)]
pub(super) struct LeafPreparation {
    pub node_id:u32,pub started_ns:u64,pub finished_ns:u64,pub cached:bool,pub installed:bool,pub completed:bool,
    pub posting_ns:u64,pub allocation_ns:u64,pub lock_enqueue_ns:u64,pub lock_acquired_ns:u64,pub lock_released_ns:u64,
    pub version_loop_ns:u64,pub lookup_ns:u64,pub filter_copy_ns:u64,pub centroid_ns:u64,pub install_ns:u64,
    pub physical_rows:usize,pub physical_code_bytes:usize,pub current_rows:usize,pub stale_rows:usize,pub deleted_rows:usize,pub missing_rows:usize,pub invalid_versions:usize,
    pub copied_code_bytes:usize,pub ids_capacity:usize,pub versions_capacity:usize,pub codes_capacity:usize,
    pub batches:usize,pub scheduled_point_gets:usize,pub batch_distinct_blocks:BTreeMap<usize,usize>,
}
pub(super) struct Preparation {
    pub start:Instant,pub leaves:Mutex<BTreeMap<u32,LeafPreparation>>,pub overflow:Mutex<bool>,
}
impl Preparation {
    pub fn new()->Arc<Self> {Arc::new(Self{start:Instant::now(),leaves:Mutex::new(BTreeMap::new()),overflow:Mutex::new(false)})}
    pub fn leaf(self:&Arc<Self>,node:u32)->LeafScope {LeafScope{owner:self.clone(),value:LeafPreparation{node_id:node,started_ns:nanos(self.start),..Default::default()}}}
}
pub(super) struct LeafScope {pub owner:Arc<Preparation>,pub value:LeafPreparation}
impl Drop for LeafScope {
    fn drop(&mut self) {self.value.finished_ns=nanos(self.owner.start);if self.value.lock_acquired_ns!=0 && self.value.lock_released_ns==0 {self.value.lock_released_ns=self.value.finished_ns;}let mut leaves=self.owner.leaves.lock().unwrap();if leaves.len()>=256 || leaves.contains_key(&self.value.node_id) {*self.owner.overflow.lock().unwrap()=true;}else{leaves.insert(self.value.node_id,std::mem::take(&mut self.value));}}
}
#[cfg(test)] mod tests {
    use super::*;
    #[test] fn preparation_leaf_limit_and_error_scope() {let p=Preparation::new();{let mut l=p.leaf(7);l.value.missing_rows=1;}assert!(!p.leaves.lock().unwrap()[&7].completed);for i in 8..264 {drop(p.leaf(i));}assert_eq!(p.leaves.lock().unwrap().len(),256);drop(p.leaf(999));assert!(*p.overflow.lock().unwrap());}
    #[test] fn preparation_duplicate_leaf_is_invalid() {let p=Preparation::new();drop(p.leaf(1));drop(p.leaf(1));assert!(*p.overflow.lock().unwrap());}
}
