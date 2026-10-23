//! Bounded home-partition location authority for the experimental writer.
//! Membership versions stay at the physical posting owner. This component only
//! routes immutable identities and sequences changes to their physical location.
use anyhow::{ensure, Result};
use parking_lot::Mutex;
use std::{collections::BTreeMap, ops::Deref, sync::{mpsc, Arc}, thread};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct VectorId64(pub u64);
impl VectorId64 {
    pub fn new(home: u32, local: u32) -> Self { Self((u64::from(home) << 32) | u64::from(local)) }
    pub fn home(self) -> u32 { (self.0 >> 32) as u32 }
    pub fn local(self) -> u32 { self.0 as u32 }
}
impl From<u32> for VectorId64 { fn from(value: u32) -> Self { Self::new(0, value) } }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Location { pub owner: usize, pub epoch: u64 }
#[derive(Clone, Copy)]
enum Record { Pending, Active(Location) }
type Job = Box<dyn FnOnce(&mut BTreeMap<u32, Record>, usize) + Send>;
const STRIPES: usize = 64;
struct PartitionClient {
    sender: Option<mpsc::SyncSender<Job>>,
    thread: Option<thread::JoinHandle<()>>,
    stripes: Vec<Arc<Mutex<()>>>,
}
impl PartitionClient {
    fn new(cap: usize) -> Self {
        let (sender, receiver) = mpsc::sync_channel::<Job>(64);
        let thread = thread::spawn(move || { let mut records = BTreeMap::new(); for job in receiver { job(&mut records, cap); } });
        Self { sender: Some(sender), thread: Some(thread), stripes: (0..STRIPES).map(|_| Arc::new(Mutex::new(()))).collect() }
    }
    fn call<T: Send + 'static>(&self, f: impl FnOnce(&mut BTreeMap<u32, Record>, usize) -> T + Send + 'static) -> T {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender.as_ref().unwrap().send(Box::new(move |records, cap| { let _ = tx.send(f(records, cap)); })).expect("partition authority stopped");
        rx.recv().expect("partition authority failed")
    }
}
impl Drop for PartitionClient { fn drop(&mut self) { self.sender.take(); if let Some(t) = self.thread.take() { t.join().expect("partition authority panic"); } } }

/// The coordinator stores only this explicitly admitted partition-client set.
/// Each local actor owns at most `records_per_partition` records, including
/// pending registrations. This is bounded resident admission, not paging.
pub struct PartitionAuthority {
    partitions: BTreeMap<u32, PartitionClient>,
    pub records_per_partition: usize,
}
pub struct Owner(usize);
impl Deref for Owner { type Target = usize; fn deref(&self) -> &usize { &self.0 } }
pub struct Entry(usize);
impl Entry { pub fn value(&self) -> &usize { &self.0 } }
pub struct Reservation<'a> { authority: &'a PartitionAuthority, id: VectorId64, pending: bool }
impl Reservation<'_> {
    pub fn activate(mut self, owner: usize) {
        if self.pending {
            let key = self.id.local();
            self.authority.partition(self.id).call(move |r, _| { assert!(matches!(r.get(&key), Some(Record::Pending))); r.insert(key, Record::Active(Location { owner, epoch: 0 })); });
            self.pending = false;
        }
    }
}
impl Drop for Reservation<'_> { fn drop(&mut self) { if self.pending { let key = self.id.local(); self.authority.partition(self.id).call(move |r, _| { assert!(matches!(r.remove(&key), Some(Record::Pending))); }); } } }
impl PartitionAuthority {
    pub fn new(homes: &[u32], records_per_partition: usize) -> Result<Self> {
        ensure!(!homes.is_empty() && homes.len() <= 64 && records_per_partition > 0, "partition admission");
        ensure!(records_per_partition.checked_mul(homes.len()).is_some(), "total partition capacity overflow");
        let mut partitions = BTreeMap::new();
        for home in homes { ensure!(!partitions.contains_key(home), "duplicate home partition"); partitions.insert(*home, PartitionClient::new(records_per_partition)); }
        Ok(Self { partitions, records_per_partition })
    }
    pub fn admits(&self,id:VectorId64)->bool{self.partitions.contains_key(&id.home())}
    pub fn remaining(&self,home:u32)->usize{self.partitions.get(&home).expect("unadmitted home").call(|r,cap|cap-r.len())}
    fn partition(&self, id: VectorId64) -> &PartitionClient { self.partitions.get(&id.home()).expect("unadmitted home partition") }
    pub fn location(&self, id: VectorId64) -> Option<Location> {
        let key = id.local(); self.partition(id).call(move |r, _| match r.get(&key) { Some(Record::Active(v)) => Some(*v), _ => None })
    }
    /// The caller holds this identity's stripe until activation or cancellation.
    pub fn reserve(&self, id: VectorId64) -> Result<Reservation<'_>> {
        let key = id.local();
        let pending = self.partition(id).call(move |r, cap| -> Result<bool> { match r.get(&key) { Some(Record::Active(_)) => Ok(false), Some(Record::Pending) => anyhow::bail!("registration already pending"), None => { ensure!(r.len() < cap, "home partition record capacity"); r.insert(key, Record::Pending); Ok(true) } } })?;
        Ok(Reservation { authority: self, id, pending })
    }
    pub fn transfer(&self, id: VectorId64, expected: Location, owner: usize) -> Result<Location> {
        let key = id.local(); self.partition(id).call(move |r, _| { ensure!(matches!(r.get(&key),Some(Record::Active(v)) if *v==expected), "stale ownership epoch"); let epoch = expected.epoch.checked_add(1).ok_or_else(|| anyhow::anyhow!("ownership epoch exhausted"))?; let next = Location { owner, epoch }; r.insert(key, Record::Active(next)); Ok(next) })
    }
    pub fn locks(&self, ids: impl IntoIterator<Item=VectorId64>) -> Vec<Arc<Mutex<()>>> {
        let mut keys: Vec<_> = ids.into_iter().map(|id| (id.home(), id.local() as usize % STRIPES)).collect(); keys.sort_unstable(); keys.dedup();
        keys.into_iter().map(|(home, stripe)| self.partitions.get(&home).expect("unadmitted home partition").stripes[stripe].clone()).collect()
    }
    pub fn total_capacity(&self) -> usize { self.records_per_partition.checked_mul(self.partitions.len()).expect("admitted capacity overflow") }
    // Legacy kernel adapters lift u32 IDs into home zero. Stage 2 replaces these
    // adapters with full-width calls; no global dense identity map is involved.
    pub fn get(&self, id: &u32) -> Option<Owner> { self.location((*id).into()).map(|v| Owner(v.owner)) }
    pub fn contains_key(&self, id: &u32) -> bool { self.get(id).is_some() }
    pub fn insert(&self, id: u32, owner: usize) { let id = id.into(); if let Some(old) = self.location(id) { self.transfer(id, old, owner).expect("authority transfer"); } else { self.reserve(id).expect("authority admission").activate(owner); } }
    #[cfg(test)] pub fn remove(&self, id: &u32) { self.remove_identity((*id).into()); }
    #[cfg(test)] pub fn remove_identity(&self,id:VectorId64){let key=id.local();self.partition(id).call(move|r,_|{r.remove(&key);});}
    #[cfg(test)] pub fn iter(&self) -> std::vec::IntoIter<Entry> { self.partitions.values().flat_map(|p| p.call(|r,_| r.values().filter_map(|v| match v { Record::Active(v)=>Some(Entry(v.owner)), _=>None }).collect::<Vec<_>>())).collect::<Vec<_>>().into_iter() }
    #[cfg(test)] pub fn is_empty(&self) -> bool { self.iter().next().is_none() }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partition_authority_full_identity_admission_epochs_and_stripe_progress() {
        let authority = Arc::new(PartitionAuthority::new(&[0, 1, 2], 2).unwrap());
        assert_eq!(authority.total_capacity(), 6);
        let ids = [VectorId64::new(0, 7), VectorId64::new(1, 7), VectorId64::new(2, 7)];
        for (owner, id) in ids.into_iter().enumerate() {
            let locks=authority.locks([id]); let _guard=locks[0].lock();
            let pending=authority.reserve(id).unwrap(); assert_eq!(authority.location(id),None);
            pending.activate(owner); assert_eq!(authority.location(id),Some(Location{owner,epoch:0}));
        }
        let old=authority.location(ids[1]).unwrap();
        assert_eq!(authority.transfer(ids[1],old,0).unwrap(),Location{owner:0,epoch:1});
        assert!(authority.transfer(ids[1],old,2).is_err());
        let extra=VectorId64::new(0,71);
        { let pending=authority.reserve(extra).unwrap(); assert!(authority.reserve(VectorId64::new(0,135)).is_err()); drop(pending); }
        assert_eq!(authority.location(extra),None);
        authority.reserve(extra).unwrap().activate(2);
        assert!(authority.reserve(VectorId64::new(0,135)).is_err());
        // Three distinct identities collide on one home-zero stripe. Both
        // workers request opposite ID order; stable dedup prevents self-lock.
        assert_eq!(authority.locks([ids[0],extra,ids[0]]).len(),1);
        let (tx,rx)=mpsc::channel();
        let handles:Vec<_>=(0..2).map(|worker| {let a=authority.clone();let tx=tx.clone();thread::spawn(move|| {
            for _ in 0..100 {let order=if worker==0{[ids[0],extra]}else{[extra,ids[0]]};let locks=a.locks(order);let _guards:Vec<_>=locks.iter().map(|v|v.lock()).collect();let current=a.location(ids[0]).unwrap();a.transfer(ids[0],current,worker).unwrap();}
            tx.send(()).unwrap();
        })}).collect();
        for _ in 0..2 {rx.recv_timeout(std::time::Duration::from_secs(5)).expect("stripe progress");}
        for h in handles{h.join().unwrap();}
        assert_eq!(authority.location(ids[0]).unwrap().epoch,200);
        assert!(PartitionAuthority::new(&[0,0],1).is_err());
        assert!(PartitionAuthority::new(&[0,1],usize::MAX).is_err());
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(||authority.location(VectorId64::new(3,7)))).is_err());
    }
}
