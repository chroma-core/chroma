//! A complete structural view for experimental owner construction. Ordinary
//! reference writers do not enable this stronger observation policy.
use anyhow::{ensure,Result};
use parking_lot::{RwLock,RwLockReadGuard,RwLockWriteGuard};
use std::{cell::RefCell,collections::{BTreeMap,BTreeSet},sync::atomic::{AtomicBool,AtomicU64,Ordering},time::Instant};
thread_local!{static DETACHED:RefCell<BTreeMap<usize,BTreeSet<u32>>>=RefCell::new(BTreeMap::new());static EXCLUDED:RefCell<BTreeMap<usize,BTreeSet<u32>>>=RefCell::new(BTreeMap::new());static SCOPES:RefCell<BTreeMap<usize,(usize,usize)>>=RefCell::new(BTreeMap::new());}
#[cfg(test)]
type MergePause=(std::sync::mpsc::Sender<()>,std::sync::mpsc::Receiver<()>);
#[derive(Default)]
pub struct Visibility{#[cfg(test)] pub merge_pause:parking_lot::Mutex<Option<MergePause>>,gate:RwLock<()>,failed:AtomicBool,epoch:AtomicU64,reads:AtomicU64,read_attempts:AtomicU64,active_reads:AtomicU64,peak_reads:AtomicU64,read_wait:AtomicU64,write_wait:AtomicU64,missing:[AtomicU64;3],expected_detached:AtomicU64}
pub struct WriteScope<'a>{view:&'a Visibility,_guard:Option<RwLockWriteGuard<'a,()>>}
pub struct Exclusion<'a>{view:&'a Visibility,id:u32}
pub struct ReadScope<'a>{view:&'a Visibility,_guard:Option<RwLockReadGuard<'a,()>>,counted:bool}
impl Visibility{
 fn key(&self)->usize{self as *const _ as usize}
 fn depths(&self)->(usize,usize){SCOPES.with(|v|v.borrow().get(&self.key()).copied().unwrap_or_default())}
 pub fn owns_write(&self)->bool{self.depths().0>0}
 pub fn failed(&self)->bool{self.failed.load(Ordering::Acquire)}
 pub fn poison(&self){self.failed.store(true,Ordering::Release);}
 pub fn write_scope(&self)->WriteScope<'_>{assert!(!self.failed(),"poisoned topology visibility");let(w,r)=self.depths();assert!(r==0,"topology read-to-write upgrade forbidden");let started=Instant::now();let guard=(w==0).then(||self.gate.write());self.write_wait.fetch_add(started.elapsed().as_nanos() as u64,Ordering::Relaxed);assert!(!self.failed(),"poisoned after queued structural writer");SCOPES.with(|v|v.borrow_mut().entry(self.key()).or_default().0+=1);WriteScope{view:self,_guard:guard}}
 pub fn read_scope(&self)->Result<ReadScope<'_>>{self.read_attempts.fetch_add(1,Ordering::Relaxed);let(w,r)=self.depths();let started=Instant::now();let guard=(w==0 && r==0).then(||self.gate.read());self.read_wait.fetch_add(started.elapsed().as_nanos() as u64,Ordering::Relaxed);ensure!(!self.failed(),"poisoned topology visibility");SCOPES.with(|v|v.borrow_mut().entry(self.key()).or_default().1+=1);let counted=w==0 && r==0;if counted{self.reads.fetch_add(1,Ordering::Relaxed);let active=self.active_reads.fetch_add(1,Ordering::Relaxed)+1;self.peak_reads.fetch_max(active,Ordering::Relaxed);}Ok(ReadScope{view:self,_guard:guard,counted})}
 pub fn detached(&self,id:u32){assert!(self.owns_write(),"detach tracking requires write scope");DETACHED.with(|v|{let mut v=v.borrow_mut();let ids=v.entry(self.key()).or_default();assert!(ids.len()<4096 || ids.contains(&id),"structural detached-ID admission");ids.insert(id);});}
 pub fn installed(&self,id:u32){DETACHED.with(|v|{if let Some(ids)=v.borrow_mut().get_mut(&self.key()){ids.remove(&id);}});}
 /// Merge discovery excludes only its own detached source, for this call.
 pub fn exclude_detached(&self,id:u32)->Exclusion<'_>{assert!(self.owns_write() && DETACHED.with(|v|v.borrow().get(&self.key()).is_some_and(|ids|ids.contains(&id))),"navigation exclusion requires transaction-owned detach");EXCLUDED.with(|v|{assert!(v.borrow_mut().entry(self.key()).or_default().insert(id),"duplicate navigation exclusion");});Exclusion{view:self,id}}
 pub fn permits_missing(&self,kind:usize,id:u32)->bool{kind==2 && self.owns_write() && DETACHED.with(|v|v.borrow().get(&self.key()).is_some_and(|ids|ids.contains(&id))) && EXCLUDED.with(|v|v.borrow().get(&self.key()).is_some_and(|ids|ids.contains(&id)))}
 pub fn expected_missing(&self){self.expected_detached.fetch_add(1,Ordering::Relaxed);}
 #[cfg(test)] pub fn pause_merge_for_test(&self){let pause=self.merge_pause.lock().take();if let Some((entered,resume))=pause{entered.send(()).unwrap();resume.recv_timeout(std::time::Duration::from_secs(3)).expect("queued merge resume");}}
 pub fn missing(&self,kind:usize){self.missing[kind].fetch_add(1,Ordering::Relaxed);self.poison();}
 pub fn report(&self)->serde_json::Value{serde_json::json!({"policy":"complete-owner-structural-view-v1","completed_structural_scopes":self.epoch.load(Ordering::Acquire),"read_scope_attempts":self.read_attempts.load(Ordering::Relaxed),"read_scopes":self.reads.load(Ordering::Relaxed),"peak_concurrent_read_scopes":self.peak_reads.load(Ordering::Relaxed),"read_gate_wait_elapsed_ns":self.read_wait.load(Ordering::Relaxed),"write_gate_wait_elapsed_ns":self.write_wait.load(Ordering::Relaxed),"expected_merge_source_exclusions":self.expected_detached.load(Ordering::Relaxed),"detached_id_capacity_per_transaction":4096,"missing_root_records":self.missing[0].load(Ordering::Relaxed),"missing_parent_records":self.missing[1].load(Ordering::Relaxed),"missing_child_records":self.missing[2].load(Ordering::Relaxed),"poisoned":self.failed(),"scope":"Owner construction only. Concurrent readers share one complete structural interval; membership checks remain live after discovery. Wait sums overlap across threads and are not exclusive wall or CPU."})}
}
fn leave(view:&Visibility,write:bool){SCOPES.with(|v|{let mut v=v.borrow_mut();let d=v.get_mut(&view.key()).unwrap();if write{d.0-=1;if d.0==0{if !view.failed(){view.epoch.fetch_add(1,Ordering::Release);}DETACHED.with(|v|{v.borrow_mut().remove(&view.key());});EXCLUDED.with(|v|{v.borrow_mut().remove(&view.key());});}}else{d.1-=1;}if *d==(0,0){v.remove(&view.key());}});}
impl Drop for WriteScope<'_>{fn drop(&mut self){if std::thread::panicking(){self.view.poison();}leave(self.view,true);}}
impl Drop for ReadScope<'_>{fn drop(&mut self){if self.counted{self.view.active_reads.fetch_sub(1,Ordering::Relaxed);}leave(self.view,false);}}

impl Drop for Exclusion<'_>{fn drop(&mut self){EXCLUDED.with(|v|{if let Some(ids)=v.borrow_mut().get_mut(&self.view.key()){ids.remove(&self.id);}});}}
