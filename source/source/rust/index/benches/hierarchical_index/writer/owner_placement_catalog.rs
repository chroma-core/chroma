//! A placement phase reads a bounded set of owner-local leaf candidates. Entries
//! remain catalogued until authoritative node metadata says they are stale.
use super::owner_work_queue::{Queue,Claim,Reservation as QueueReservation,Limits as QueueLimits,CHECK};
use anyhow::{ensure,Result};
use chroma_blockstore::provider::BlockfileProvider;
use std::sync::atomic::{AtomicU64,Ordering};
#[derive(Clone,Copy)]pub struct Limits{pub cache_pages_per_owner:usize,pub active_pages_per_owner:usize,pub pages_per_phase:usize,pub moves_per_phase:usize}
impl Default for Limits{fn default()->Self{Self{cache_pages_per_owner:1,active_pages_per_owner:4096,pages_per_phase:64,moves_per_phase:64}}}
pub struct Catalog{owners:Vec<Queue>,pub limits:Limits,claims:AtomicU64,stale:AtomicU64,moves:AtomicU64}
pub struct Reservation<'a>{_owners:Vec<QueueReservation<'a>>}
pub struct Page{pub owner:usize,pub claim:Claim}
impl Catalog{
 pub fn new(provider:BlockfileProvider,owners:usize,limits:Limits)->Result<Self>{ensure!(owners>0&&owners<=8&&limits.pages_per_phase>=owners&&limits.moves_per_phase>0,"placement catalog admission");let owners=(0..owners).map(|_|Queue::new(provider.clone(),1,QueueLimits{cache_pages_per_home:limits.cache_pages_per_owner,max_active_pages:limits.active_pages_per_owner})).collect::<Result<_>>()?;Ok(Self{owners,limits,claims:AtomicU64::new(0),stale:AtomicU64::new(0),moves:AtomicU64::new(0)})}
 pub fn reserve(&self,records:usize)->Result<Reservation<'_>>{Ok(Reservation{_owners:self.owners.iter().map(|q|q.reserve(records)).collect::<Result<_>>()?})}
 pub fn mark(&self,owner:usize,id:u32)->Result<()>{ensure!(owner<self.owners.len(),"placement owner admission");self.owners[owner].mark(id,CHECK)}
 pub fn pages(&self,owner:usize)->usize{self.owners[owner].queued_pages()}
 pub fn claim(&self,owner:usize)->Result<Option<Page>>{let page=self.owners[owner].claim()?;if page.is_some(){self.claims.fetch_add(1,Ordering::Relaxed);}Ok(page.map(|claim|Page{owner,claim}))}
 pub fn ack(&self,page:Page,keep:&[bool])->Result<()>{self.stale.fetch_add(keep.iter().filter(|v|!**v).count()as u64,Ordering::Relaxed);self.owners[page.owner].ack(page.claim,keep)}
 pub fn moved(&self){self.moves.fetch_add(1,Ordering::Relaxed);}
 pub fn flush(&self)->Result<()>{for q in &self.owners{q.flush()?;}Ok(())}
 pub fn report(&self)->serde_json::Value{serde_json::json!({"policy":"bounded-owner-page-greedy-v1","max_candidate_rows":64,"page_cap_per_phase":self.limits.pages_per_phase,"move_cap_per_phase":self.limits.moves_per_phase,"claimed_pages":self.claims.load(Ordering::Relaxed),"stale_rows":self.stale.load(Ordering::Relaxed),"leaf_moves":self.moves.load(Ordering::Relaxed),"owners":self.owners.iter().map(|q|q.report()).collect::<Vec<_>>(),"scope":"Catalog roots/cursors, bounded durable queue pages and <=64 candidate metadata rows. Provider indexes are separate. This is page-local greedy selection, not a global descending leaf sort."})}
}
#[cfg(test)]mod tests{
 use super::*;use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;use chroma_cache::new_cache_for_test;use chroma_storage::{local::LocalStorage,Storage};
 #[tokio::test(flavor="multi_thread",worker_threads=2)]async fn placement_catalog_bounded_pages_stale_newer_and_credits(){let dir=tempfile::tempdir().unwrap();let provider=BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(dir.path().to_str().unwrap())),16*1024,new_cache_for_test(),new_cache_for_test(),2));let c=Catalog::new(provider,2,Limits{cache_pages_per_owner:1,active_pages_per_owner:2,pages_per_phase:2,moves_per_phase:1}).unwrap();c.mark(0,1).unwrap();c.mark(0,2).unwrap();c.mark(1,65).unwrap();c.flush().unwrap();let page=c.claim(0).unwrap().unwrap();assert_eq!(page.claim.tickets.iter().map(|v|v.id).collect::<Vec<_>>(),vec![1,2]);c.mark(0,1).unwrap();c.ack(page,&[false,true]).unwrap();let page=c.claim(0).unwrap().unwrap();assert_eq!(page.claim.tickets.iter().map(|v|v.id).collect::<Vec<_>>(),vec![1,2]);c.ack(page,&[false,true]).unwrap();let page=c.claim(0).unwrap().unwrap();assert_eq!(page.claim.tickets[0].id,2);c.ack(page,&[true]).unwrap();{let _credit=c.reserve(1).unwrap();std::thread::scope(|s|assert!(s.spawn(||c.mark(0,128)).join().unwrap().is_err()));c.mark(0,128).unwrap();}assert!(c.mark(0,256).is_err());assert_eq!(c.pages(0),2);assert_eq!(c.pages(1),1);let unsuitable=c.claim(0).unwrap().unwrap();assert_eq!(unsuitable.claim.tickets[0].id,2);c.ack(unsuitable,&[true]).unwrap();let later=c.claim(0).unwrap().unwrap();assert_eq!(later.claim.tickets[0].id,128,"persistent unsuitable first page rotates behind later pages");c.ack(later,&[false]).unwrap();println!("PLACEMENT_CORE {}",c.report());}
}
