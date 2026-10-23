//! Bounded home-partition location authority for the experimental writer.
//! Membership versions stay at the physical posting owner. This component only
//! routes immutable identities and sequences changes to their physical location.
use anyhow::{ensure, Result};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicU64,Ordering};
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
    metadata:Mutex<Option<Arc<super::owner_vector_metadata::Store>>>,
    partitions: BTreeMap<u32, PartitionClient>,
    pub records_per_partition: usize,
    scalar_location_calls:AtomicU64,batch_location_calls:AtomicU64,batch_location_rows:AtomicU64,location_messages:AtomicU64,reserve_messages:AtomicU64,activation_messages:AtomicU64,cancel_messages:AtomicU64,
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
            self.authority.activation_messages.fetch_add(1,Ordering::Relaxed);
            self.authority.partition(self.id).call(move |r, _| { assert!(matches!(r.get(&key), Some(Record::Pending))); r.insert(key, Record::Active(Location { owner, epoch: 0 })); });
            self.pending = false;
        }
    }
}
impl Drop for Reservation<'_> { fn drop(&mut self) { if self.pending { let key = self.id.local(); self.authority.cancel_messages.fetch_add(1,Ordering::Relaxed); self.authority.partition(self.id).call(move |r, _| { assert!(matches!(r.remove(&key), Some(Record::Pending))); }); } } }
/// Pending records stay invisible until their posting/current/handle is installed.
/// The caller owns all identity stripes until activation or cancellation.
pub struct NewBatchReservation<'a>{authority:&'a PartitionAuthority,groups:BTreeMap<u32,Vec<u32>>}
impl NewBatchReservation<'_>{pub fn activate(mut self,owner:usize){
    for(home,keys)in &self.groups{let keys=keys.clone();self.authority.activation_messages.fetch_add(1,Ordering::Relaxed);self.authority.partitions[home].call(move|records,_|{assert!(keys.iter().all(|key|matches!(records.get(key),Some(Record::Pending))),"batch pending activation");for key in keys{records.insert(key,Record::Active(Location{owner,epoch:0}));}});}
    self.groups.clear();
}}
impl Drop for NewBatchReservation<'_>{fn drop(&mut self){for(home,keys)in &self.groups{let keys=keys.clone();self.authority.cancel_messages.fetch_add(1,Ordering::Relaxed);self.authority.partitions[home].call(move|records,_|{for key in keys{assert!(matches!(records.remove(&key),Some(Record::Pending)),"batch cancellation");}});}}}
impl PartitionAuthority {
    pub fn new(homes: &[u32], records_per_partition: usize) -> Result<Self> {
        ensure!(!homes.is_empty() && homes.len() <= 64 && records_per_partition > 0, "partition admission");
        ensure!(records_per_partition.checked_mul(homes.len()).is_some(), "total partition capacity overflow");
        let mut partitions = BTreeMap::new();
        for home in homes { ensure!(!partitions.contains_key(home), "duplicate home partition"); partitions.insert(*home, PartitionClient::new(records_per_partition)); }
        Ok(Self { metadata:Mutex::new(None),partitions, records_per_partition,scalar_location_calls:AtomicU64::new(0),batch_location_calls:AtomicU64::new(0),batch_location_rows:AtomicU64::new(0),location_messages:AtomicU64::new(0),reserve_messages:AtomicU64::new(0),activation_messages:AtomicU64::new(0),cancel_messages:AtomicU64::new(0) })
    }
    pub(super) fn enable_metadata(&self,store:Arc<super::owner_vector_metadata::Store>)->Result<()>{ensure!(self.metadata.lock().is_none()&&store.home_ids()==self.home_ids(),"metadata authority homes/mode");ensure!(self.partitions.values().all(|p|p.call(|r,_|r.is_empty())),"metadata requires empty legacy authority");*self.metadata.lock()=Some(store);Ok(())}
    pub fn home_ids(&self)->Vec<u32>{self.partitions.keys().copied().collect()}
    pub(super) fn metadata(&self)->Option<Arc<super::owner_vector_metadata::Store>>{self.metadata.lock().clone()}
    #[cfg(test)] pub fn resident_records(&self)->usize{self.partitions.values().map(|p|p.call(|r,_|r.len())).sum()}
    pub fn admits(&self,id:VectorId64)->bool{self.partitions.contains_key(&id.home())}
    pub fn remaining(&self,home:u32)->usize{if let Some(store)=self.metadata(){return store.remaining(home);}self.partitions.get(&home).expect("unadmitted home").call(|r,cap|cap-r.len())}
    fn partition(&self, id: VectorId64) -> &PartitionClient { self.partitions.get(&id.home()).expect("unadmitted home partition") }
    pub fn location(&self, id: VectorId64) -> Option<Location> {
        if let Some(store)=self.metadata(){let pinned=store.prepare(&[id],false).expect("paged location prepare");return pinned.get(id).expect("paged location read").filter(|r|r.active).map(|r|Location{owner:r.owner,epoch:r.epoch});}
        let client=self.partition(id);self.scalar_location_calls.fetch_add(1,Ordering::Relaxed);self.location_messages.fetch_add(1,Ordering::Relaxed);let key = id.local(); client.call(move |r, _| match r.get(&key) { Some(Record::Active(v)) => Some(*v), _ => None })
    }
    /// Return at most64 locations in request order. Each admitted home actor
    /// reads its group in one callback. Cross-home consistency requires the
    /// caller to hold all corresponding identity stripes through subsequent
    /// owner reads; callbacks never acquire stripes or call owner actors.
    pub fn locations_batch(&self,ids:&[VectorId64])->Result<Vec<Option<Location>>>{
        ensure!(ids.len()<=64,"location batch capacity");ensure!(ids.iter().all(|id|self.admits(*id)),"unadmitted location batch home");
        if let Some(store)=self.metadata(){let pinned=store.prepare(ids,false)?;return ids.iter().map(|id|Ok(pinned.get(*id)?.filter(|r|r.active).map(|r|Location{owner:r.owner,epoch:r.epoch}))).collect();}
        self.batch_location_calls.fetch_add(1,Ordering::Relaxed);self.batch_location_rows.fetch_add(ids.len() as u64,Ordering::Relaxed);
        let mut groups:BTreeMap<u32,Vec<(usize,u32)>>=BTreeMap::new();for(slot,id) in ids.iter().enumerate(){groups.entry(id.home()).or_default().push((slot,id.local()));}
        let mut result=vec![None;ids.len()];for(home,keys) in groups{self.location_messages.fetch_add(1,Ordering::Relaxed);let values=self.partitions[&home].call(move|records,_|keys.into_iter().map(|(slot,key)|(slot,match records.get(&key){Some(Record::Active(location))=>Some(*location),_=>None})).collect::<Vec<_>>());for(slot,value) in values{result[slot]=value;}}Ok(result)
    }
    pub fn location_observation(&self)->serde_json::Value{serde_json::json!({"scalar_calls":self.scalar_location_calls.load(Ordering::Relaxed),"batch_calls":self.batch_location_calls.load(Ordering::Relaxed),"batch_rows":self.batch_location_rows.load(Ordering::Relaxed),"home_actor_messages":self.location_messages.load(Ordering::Relaxed),"scope":"Location dispatch attempts only; other authority operations and physical-owner callbacks are separate. Counters are cumulative and not CPU/network."})}
    /// The caller holds this identity's stripe until activation or cancellation.
    pub fn reserve(&self, id: VectorId64) -> Result<Reservation<'_>> {
        ensure!(self.metadata().is_none(),"paged registration requires metadata transaction");
        let key = id.local();
        self.reserve_messages.fetch_add(1,Ordering::Relaxed);
        let pending = self.partition(id).call(move |r, cap| -> Result<bool> { match r.get(&key) { Some(Record::Active(_)) => Ok(false), Some(Record::Pending) => anyhow::bail!("registration already pending"), None => { ensure!(r.len() < cap, "home partition record capacity"); r.insert(key, Record::Pending); Ok(true) } } })?;
        Ok(Reservation { authority: self, id, pending })
    }
    /// Reserve fresh identities in at most64 rows. A failed later home rolls
    /// back earlier pending groups before the caller can mutate any posting.
    pub fn reserve_new_batch(&self,ids:&[VectorId64])->Result<NewBatchReservation<'_>>{
        ensure!(self.metadata().is_none(),"paged registration requires metadata transaction");
        ensure!(ids.len()<=64 && ids.iter().all(|id|self.admits(*id)),"new batch identity admission");
        let mut unique=ids.to_vec();unique.sort_unstable();unique.dedup();ensure!(unique.len()==ids.len(),"duplicate new batch identity");
        let mut groups:BTreeMap<u32,Vec<u32>>=BTreeMap::new();for id in ids{groups.entry(id.home()).or_default().push(id.local());}
        let mut reservation=NewBatchReservation{authority:self,groups:BTreeMap::new()};
        for(home,keys)in groups{let request=keys.clone();self.reserve_messages.fetch_add(1,Ordering::Relaxed);self.partitions[&home].call(move|records,cap|->Result<()>{ensure!(request.iter().all(|key|!records.contains_key(key)),"new batch identity already exists");ensure!(records.len().checked_add(request.len()).is_some_and(|n|n<=cap),"new batch home capacity");for key in request{records.insert(key,Record::Pending);}Ok(())})?;reservation.groups.insert(home,keys);}
        Ok(reservation)
    }
    pub fn registration_observation(&self)->serde_json::Value{serde_json::json!({"reserve_home_messages":self.reserve_messages.load(Ordering::Relaxed),"activate_home_messages":self.activation_messages.load(Ordering::Relaxed),"cancel_home_messages":self.cancel_messages.load(Ordering::Relaxed),"scope":"Actual scalar and batch pending reservation/activation/cancellation dispatches. Location operations are reported separately."})}
    pub fn transfer(&self, id: VectorId64, expected: Location, owner: usize) -> Result<Location> {
        ensure!(self.metadata().is_none(),"paged transfer requires pinned metadata transaction");
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
    fn partition_new_batch_pending_rollback_messages_and_stripe_collision(){
        let authority=Arc::new(PartitionAuthority::new(&[0,1],128).unwrap());
        let ids:Vec<_>=(0..64).map(|id|VectorId64::new(0,id)).collect();
        {let locks=authority.locks(ids.clone());let _guards:Vec<_>=locks.iter().map(|v|v.lock()).collect();let pending=authority.reserve_new_batch(&ids).unwrap();assert!(authority.locations_batch(&ids).unwrap().iter().all(Option::is_none));pending.activate(1);}
        assert_eq!(authority.registration_observation()["reserve_home_messages"],1);assert_eq!(authority.registration_observation()["activate_home_messages"],1);
        assert!(authority.reserve_new_batch(&[ids[0]]).is_err());assert!(authority.reserve_new_batch(&[VectorId64::new(1,7);2]).is_err());assert!(authority.reserve_new_batch(&vec![VectorId64::new(1,7);65]).is_err());
        let small=PartitionAuthority::new(&[0,1],1).unwrap();small.reserve(VectorId64::new(1,9)).unwrap().activate(0);
        assert!(small.reserve_new_batch(&[VectorId64::new(0,7),VectorId64::new(1,8)]).is_err());assert_eq!(small.remaining(0),1,"earlier pending home rolled back");
        let (tx,rx)=mpsc::channel();let mut threads=vec![];for worker in 0..2{let authority=authority.clone();let tx=tx.clone();threads.push(thread::spawn(move||{let a=VectorId64::new(1,7+worker*128);let b=VectorId64::new(1,71+worker*128);let request=if worker==0{vec![a,b]}else{vec![b,a]};let locks=authority.locks(request.clone());assert_eq!(locks.len(),1,"colliding IDs share one guard");let _guards:Vec<_>=locks.iter().map(|v|v.lock()).collect();authority.reserve_new_batch(&request).unwrap().activate(worker as usize);tx.send(()).unwrap();}));}for _ in 0..2{rx.recv_timeout(std::time::Duration::from_secs(3)).unwrap();}for t in threads{t.join().unwrap();}
        println!("NEW_BATCH_AUTHORITY {}",authority.registration_observation());
    }

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
    #[test]
    fn partition_locations_batch_order_messages_missing_and_stripe_progress(){
        let authority=Arc::new(PartitionAuthority::new(&[0,1,2],64).unwrap());for key in 0..32{authority.reserve(VectorId64::new(0,key)).unwrap().activate(key as usize%2);}for id in [VectorId64::new(0,64),VectorId64::new(1,7),VectorId64::new(2,7)]{authority.reserve(id).unwrap().activate(1);}
        let pending_id=VectorId64::new(1,8);let pending=authority.reserve(pending_id).unwrap();
        let messages=||authority.location_observation()["home_actor_messages"].as_u64().unwrap();
        let single:Vec<_>=(0..64).map(|slot|VectorId64::new(0,slot%40)).collect();let mut evidence=Vec::new();
        for(name,ids,homes) in [("single-home",single,1),("mixed-home",vec![VectorId64::new(2,7),VectorId64::new(0,7),VectorId64::new(1,7),VectorId64::new(0,7),pending_id,VectorId64::new(2,63)],3)]{
            let locks=authority.locks(ids.iter().copied());let _guards:Vec<_>=locks.iter().map(|v|v.lock()).collect();let before=messages();let expected:Vec<_>=ids.iter().map(|id|authority.location(*id)).collect();let scalar=messages()-before;let before=messages();let actual=authority.locations_batch(&ids).unwrap();let batched=messages()-before;assert_eq!(actual,expected);assert_eq!(scalar,ids.len() as u64);assert_eq!(batched,homes);evidence.push(serde_json::json!({"case":name,"rows":ids.len(),"scalar_messages":scalar,"batch_home_messages":batched}));}
        let before=messages();assert!(authority.locations_batch(&[]).unwrap().is_empty());assert_eq!(messages(),before);assert!(authority.locations_batch(&vec![VectorId64::new(0,7);65]).is_err());assert!(authority.locations_batch(&[VectorId64::new(0,7),VectorId64::new(9,7)]).is_err());assert_eq!(messages(),before,"cap and unadmitted home reject before dispatch");drop(pending);
        let first=VectorId64::new(0,0);let collision=VectorId64::new(0,64);let other=VectorId64::new(1,7);assert_eq!(authority.locks([first,collision,other,first]).len(),2);let(tx,rx)=mpsc::channel();let mut workers=Vec::new();
        for worker in 0..2{let authority=authority.clone();let tx=tx.clone();workers.push(thread::spawn(move||{for _ in 0..20{let ids=if worker==0{[first,collision,other,first]}else{[other,first,collision,first]};let locks=authority.locks(ids);let _guards:Vec<_>=locks.iter().map(|v|v.lock()).collect();let values=authority.locations_batch(&ids).unwrap();assert_eq!(values.len(),4);let index=ids.iter().position(|id|*id==first).unwrap();authority.transfer(first,values[index].unwrap(),worker).unwrap();}tx.send(()).unwrap();}));}
        for _ in 0..2{rx.recv_timeout(std::time::Duration::from_secs(5)).expect("batched opposite-order stripe progress");}for worker in workers{worker.join().unwrap();}assert_eq!(authority.location(first).unwrap().epoch,40);println!("PARTITION_LOCATION_FIXED {}",serde_json::json!({"fixed":evidence,"progress_transfers":40,"counts":authority.location_observation()}));
    }

}
