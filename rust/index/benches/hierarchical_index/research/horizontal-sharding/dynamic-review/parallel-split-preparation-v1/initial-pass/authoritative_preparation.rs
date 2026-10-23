//! Read-only parallel preparation of one live split. Decisions and mutations
//! remain serialized; no future leaf or clustering decision is prefetched.
use super::*;
use chroma_index::spann::utils::EmbeddingPoint;
use std::sync::{Arc,atomic::{AtomicUsize,Ordering}};
use std::time::Instant;

#[derive(Default)]
struct Work {
    active:AtomicUsize,
    peak:AtomicUsize,
    calls:AtomicUsize,
}
struct FetchSpan(Arc<Work>);
impl FetchSpan {
    fn begin(work:Arc<Work>)->Self {
        work.calls.fetch_add(1,Ordering::SeqCst);
        let n=work.active.fetch_add(1,Ordering::SeqCst)+1;
        work.peak.fetch_max(n,Ordering::SeqCst);Self(work)
    }
}
impl Drop for FetchSpan {fn drop(&mut self) {self.0.active.fetch_sub(1,Ordering::SeqCst);}}
#[derive(Default)]
struct FetchStats {opens:usize,clears:usize,payload:usize}
struct Fetcher {
    provider:BlockfileProvider,
    readers:BTreeMap<Uuid,BlockfileReader<'static,u32,&'static [f32]>>,
    dim:usize,
    stats:FetchStats,
}
impl Fetcher {
    async fn get(&mut self,reference:VectorRef)->Result<Arc<[f32]>> {
        if !self.readers.contains_key(&reference.root) {
            if self.readers.len()>=16 {self.readers.clear();self.stats.clears+=1;}
            let reader=storage(self.provider.read::<u32,&'static [f32]>(BlockfileReaderOptions::new(reference.root,String::new())).await)?;
            self.readers.insert(reference.root,reader);self.stats.opens+=1;
        }
        let reader=&self.readers[&reference.root];
        // Every worker owns its reader pin maps. The borrowed value is copied
        // before clearing; no clone/future can keep a borrow across this clear.
        let values={storage(reader.get(PREFIX_EMBEDDING,reference.key).await)?.context("durable vector missing")?.to_vec()};
        reader.clear_loaded_blocks();
        ensure!(values.len()==self.dim && values.iter().all(|v|v.is_finite()),"durable vector shape");
        self.stats.payload+=values.len()*4;
        Ok(Arc::from(values))
    }
}
fn same_rows(a:&[(u32,u8,VectorRef)],b:&[(u32,u8,VectorRef)])->bool {
    a.len()==b.len() && a.iter().zip(b).all(|(a,b)|a.0==b.0&&a.1==b.1&&a.2.root==b.2.root&&a.2.key==b.2.key)
}
impl State {
    fn split_rows_snapshot(&self,leaf:u32)->Result<Vec<(u32,u8,VectorRef)>> {
        let l=self.leaves.get(&leaf).context("split source not owned")?;
        Ok(l.ids.iter().zip(&l.versions).filter_map(|(&id,&version)|self.current.get(&id).filter(|c|c.leaf==leaf&&c.version==version).map(|c|(id,version,c.vector))).collect())
    }
    pub(super) fn parallel_split_input(&mut self,leaf:u32,rows:Vec<(u32,u8,VectorRef)>)->Result<Vec<EmbeddingPoint>> {
        ensure!(self.split_preparation_workers>0&&self.split_preparation_workers<=8&&rows.len()<=self.limits.max_leaf_rows,"split preparation bound");
        let start=Instant::now();let view=self.embedding_view;let count=rows.len();
        let workers=self.split_preparation_workers.min(count);
        self.counters.split_preparation_calls+=1;self.counters.split_preparation_rows+=count;
        self.counters.split_preparation_worker_jobs+=workers;
        let work=Arc::new(Work::default());
        let outcome=(||->Result<Vec<EmbeddingPoint>> {
            if count==0 {return Ok(vec![]);}
            let chunk=(count+workers-1)/workers;
            let mut jobs=vec![];
            for (part,group) in rows.chunks(chunk).enumerate() {
                let copied:Vec<_>=group.iter().enumerate().map(|(offset,(id,version,reference))|{
                    let effective=view.filter(|(_,count)|(reference.key as usize)<*count).map(|(root,_)|VectorRef{root,key:reference.key}).unwrap_or(*reference);
                    (part*chunk+offset,*id,*version,effective)
                }).collect();jobs.push(copied);
            }
            let joined=std::thread::scope(|scope| {
                let mut handles=vec![];
                for rows in jobs {
                    let p=self.provider.clone();let dim=self.dim;let observed=work.clone();
                    handles.push(scope.spawn(move||->Result<(Vec<(usize,Result<EmbeddingPoint>)>,FetchStats)> {
                        let rt=tokio::runtime::Builder::new_current_thread().enable_all().build()?;
                        rt.block_on(async move {
                            let mut fetcher=Fetcher{provider:p,readers:BTreeMap::new(),dim,stats:FetchStats::default()};
                            let mut points=vec![];
                            for (index,id,version,reference) in rows {
                                let span=FetchSpan::begin(observed.clone());let result=fetcher.get(reference).await.map(|values|(id,u32::from(version),values));drop(span);
                                // Join all read-only work, then select the earliest input error.
                                points.push((index,result));
                            }
                            Ok((points,fetcher.stats))
                        })
                    }));
                }
                handles.into_iter().map(|h|h.join().map_err(|_|anyhow::anyhow!("split preparation worker panicked")).and_then(|v|v)).collect::<Vec<_>>()
            });
            let mut ordered=BTreeMap::new();
            for joined in joined {
                let (points,stats)=joined?;
                self.counters.split_preparation_root_opens+=stats.opens;
                self.counters.split_preparation_reader_cache_clears+=stats.clears;
                self.counters.split_preparation_payload_bytes+=stats.payload;
                for (index,value) in points {ensure!(ordered.insert(index,value).is_none(),"split preparation duplicate reply");}
            }
            ensure!(work.active.load(Ordering::SeqCst)==0&&work.calls.load(Ordering::SeqCst)==count&&ordered.len()==count,"split preparation incomplete");
            let mut points=Vec::with_capacity(count);
            for index in 0..count {points.push(ordered.remove(&index).context("split preparation reply missing")??);}
            let fresh=self.split_rows_snapshot(leaf)?;
            ensure!(same_rows(&rows,&fresh)&&self.embedding_view==view,"split preparation snapshot changed");
            self.counters.split_preparation_revalidations+=1;
            Ok(points)
        })();
        self.counters.split_preparation_peak_active_fetches=self.counters.split_preparation_peak_active_fetches.max(work.peak.load(Ordering::SeqCst));
        self.counters.split_preparation_ns+=start.elapsed().as_nanos() as u64;
        if outcome.is_err() {self.counters.split_preparation_errors+=1;}
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn split_preparation_snapshot_binds_order_version_and_reference() {
        let root=Uuid::new_v4();let row=(7,1,VectorRef {root,key:7});let rows=vec![row,(8,2,VectorRef {root,key:8})];
        assert!(same_rows(&rows,&rows));
        for changed in [vec![rows[1],rows[0]],vec![(7,2,row.2),rows[1]],vec![(7,1,VectorRef{root:Uuid::new_v4(),key:7}),rows[1]],vec![(7,1,VectorRef{root,key:9}),rows[1]],vec![row]] {assert!(!same_rows(&rows,&changed));}
    }
}
