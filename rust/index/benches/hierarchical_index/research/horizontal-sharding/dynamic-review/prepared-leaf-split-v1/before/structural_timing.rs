//! Measure the actual structural mutex, including work done by scope destructors.
//! Concurrent wait sums are not elapsed wall time or CPU usage.
use parking_lot::{ReentrantMutex,ReentrantMutexGuard};
use std::{cell::RefCell,collections::HashMap,sync::atomic::{AtomicU64,Ordering},time::Instant};
thread_local!{static HELD:RefCell<HashMap<usize,(usize,Instant)>>=RefCell::new(HashMap::new());}
pub(super) struct StructuralMutex{inner:ReentrantMutex<()>,outer:AtomicU64,nested:AtomicU64,wait:AtomicU64,hold:AtomicU64}
pub(super) struct Guard<'a>{mutex:&'a StructuralMutex,guard:Option<ReentrantMutexGuard<'a,()>>}
impl StructuralMutex{
 pub fn new(_:())->Self{Self{inner:ReentrantMutex::new(()),outer:AtomicU64::new(0),nested:AtomicU64::new(0),wait:AtomicU64::new(0),hold:AtomicU64::new(0)}}
 fn key(&self)->usize{self as *const Self as usize}
 pub fn lock(&self)->Guard<'_>{let started=Instant::now();let guard=self.inner.lock();let acquired=Instant::now();HELD.with(|held|{let mut held=held.borrow_mut();if let Some((depth,_))=held.get_mut(&self.key()){*depth+=1;self.nested.fetch_add(1,Ordering::Relaxed);}else{held.insert(self.key(),(1,acquired));self.outer.fetch_add(1,Ordering::Relaxed);self.wait.fetch_add(acquired.duration_since(started).as_nanos().min(u64::MAX as u128)as u64,Ordering::Relaxed);}});Guard{mutex:self,guard:Some(guard)}}
 pub fn report(&self)->serde_json::Value{serde_json::json!({"outer_acquisitions":self.outer.load(Ordering::Relaxed),"reentrant_acquisitions":self.nested.load(Ordering::Relaxed),"mutex_wait_elapsed_ns":self.wait.load(Ordering::Relaxed),"outer_mutex_hold_elapsed_ns":self.hold.load(Ordering::Relaxed),"scope":"Actual structural mutex only. Wait measures outer lock invocation through acquisition; hold samples acquisition through the timestamp immediately before final same-thread mutex release, including topology Mutation drop/flush and unwinding; unlock/bookkeeping overhead is excluded and this is a measured boundary, not exact hardware occupancy. Reentrant holds are counted once. Every use of the structural mutex is included, including scheduler revalidation and phase movement. Counters are cumulative elapsed, not CPU; waits from different threads overlap. Active holds are absent until release; clock/RAII bookkeeping adds small measurement overhead."})}
}
impl Drop for Guard<'_>{fn drop(&mut self){let released=Instant::now();drop(self.guard.take());HELD.with(|held|{let mut held=held.borrow_mut();let(depth,start)=held.get_mut(&self.mutex.key()).expect("structural timing scope");*depth-=1;if *depth==0{let elapsed=released.duration_since(*start).as_nanos().min(u64::MAX as u128)as u64;held.remove(&self.mutex.key());self.mutex.hold.fetch_add(elapsed,Ordering::Relaxed);}});}}
#[cfg(test)]mod tests{
 use super::*;use std::{sync::{Arc,mpsc},time::Duration};
 #[test]fn structural_mutex_timing_includes_blocked_drop_and_reentrant_scope(){
  struct Flush{entered:mpsc::Sender<()>,resume:mpsc::Receiver<()>}
  impl Drop for Flush{fn drop(&mut self){self.entered.send(()).unwrap();self.resume.recv_timeout(Duration::from_secs(5)).unwrap();}}
  let mutex=Arc::new(StructuralMutex::new(()));let(entered,waiting)=mpsc::channel();let(resume,resuming)=mpsc::channel();let done=mpsc::channel();let writer=mutex.clone();let thread=std::thread::spawn(move||{let _outer=writer.lock();{let _nested=writer.lock();}let _flush=Flush{entered,resume:resuming};});waiting.recv_timeout(Duration::from_secs(5)).unwrap();let before=mutex.report();assert_eq!(before["outer_acquisitions"],1);assert_eq!(before["reentrant_acquisitions"],1);assert_eq!(before["outer_mutex_hold_elapsed_ns"],0,"blocked destructor remains inside active hold");let reader=mutex.clone();let(start,start_rx)=mpsc::channel();let waiter=std::thread::spawn(move||{start.send(()).unwrap();let _guard=reader.lock();done.0.send(()).unwrap();});start_rx.recv().unwrap();assert!(done.1.recv_timeout(Duration::from_millis(30)).is_err());resume.send(()).unwrap();thread.join().unwrap();done.1.recv_timeout(Duration::from_secs(5)).unwrap();waiter.join().unwrap();let after=mutex.report();assert_eq!(after["outer_acquisitions"],2);assert_eq!(after["reentrant_acquisitions"],1);assert!(after["outer_mutex_hold_elapsed_ns"].as_u64().unwrap()>=30_000_000);assert!(after["mutex_wait_elapsed_ns"].as_u64().unwrap()>0);println!("STRUCTURAL_MUTEX_TIMING {}",after);
 }
}
