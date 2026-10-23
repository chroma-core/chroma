//! Connected 64-bit vector identity path. Topology keeps its separate u32 IDs.
//! Each home has an explicitly capped resident authority actor; this is not a
//! paged or cross-host implementation. Query generations never consult live actors.
use super::*;
use super::super::super::{common::{InternalNode,ReadBeamPolicy,code_slice},config::NavigationMode,persistance::*};
use chroma_blockstore::{provider::BlockfileProvider,arrow::provider::BlockfileReaderOptions,BlockfileWriterOptions};
use chroma_types::hierarchical_spann::{HierarchicalLeafNode,HierarchicalInternalNode};
use simsimd::SpatialSimilarity;
use std::collections::HashMap;
use chroma_index::quantization::QuantizedQuery;

const FORMAT:u32=0x6401;
fn metric_tag(metric:&DistanceFunction)->u32{match metric{DistanceFunction::Euclidean=>1,DistanceFunction::InnerProduct=>2,DistanceFunction::Cosine=>3}}
const FORMAT_NAME:&str="hierarchical-spann-posting64-v1";
fn version_prefix(id:u64)->String{format!("version/home/{:08x}",(id>>32) as u32)}
fn vector_prefix(id:u64)->String{if id>>32==0{PREFIX_EMBEDDING.into()}else{format!("embedding/home/{:08x}",(id>>32) as u32)}}
#[derive(Clone)]
pub struct Snapshot64 { pub format:String,pub roots:super::super::HierarchicalSpannIds,pub generation:u64,pub dim:usize,pub metric:DistanceFunction,pub config:HierarchicalSpannConfig,pub max_leaf_rows:usize }
/// Exclusive public phase methods exclude mutation during placement/publication.
/// Kernel routing and parallel subtree balancing remain the reference algorithms.
pub struct WideOwnerWriter {
    kernel:HierarchicalSpannWriter<u64>,
    previous:Option<Snapshot64>,
    failed:bool,
    #[cfg(test)] fail_before_ack:bool,
}
impl WideOwnerWriter {
    pub fn new(dim:usize,metric:DistanceFunction,config:HierarchicalSpannConfig,owners:usize,homes:&[u32],records_per_home:usize,provider:BlockfileProvider,limits:super::super::owner_vector_store::VectorLimits)->Result<Self>{
        ensure!(owners>0 && owners<=8 && dim>0 && dim<=768 && config.write_navigation==NavigationMode::Fp && config.fp_npa && config.max_replicas==1,"wide owner admission requires FP and one current copy");
        let authority=super::super::partition_authority::PartitionAuthority::new(homes,records_per_home)?;
        let store=Arc::new(super::super::owner_vector_store::OwnerVectorStore::new(provider,dim,owners,limits)?);
        ensure!(config.split_threshold<=store.limits.split_rows,"wide split trigger exceeds reservation");
        let mut backend=Backend::<u64>::with_authority_capacity(owners,records_per_home);backend.authority=authority;
        *backend.vector_store.lock()=Some(store);
        backend.predicate_batching.store(1,Ordering::Relaxed);backend.scrub_batch_policy.store(1,Ordering::Relaxed);backend.split_input_policy.store(1,Ordering::Relaxed);
        let backend=Arc::new(backend);let mut kernel=HierarchicalSpannWriter::<u64>::new_identity(dim,metric,config);
        let (_,node)=kernel.nodes.remove(&0).unwrap();let TreeNode::Leaf(leaf)=node else{unreachable!()};kernel.nodes.insert(0,TreeNode::Leaf(backend.install(0,leaf)));kernel.owner_backend=Some(backend);
        Ok(Self{kernel,previous:None,failed:false,#[cfg(test)] fail_before_ack:false})
    }
    fn backend(&self)->&Arc<Backend<u64>>{self.kernel.owner_backend.as_ref().unwrap()}
    pub fn add_phase(&mut self,rows:&[(u64,Vec<f32>)],threads:usize)->Result<()> {
        ensure!(!self.failed && !rows.is_empty() && threads>0 && threads<=4,"wide add phase state");
        ensure!(rows.windows(2).all(|v|v[0].0<v[1].0) && rows.iter().all(|(_,v)|v.len()==self.kernel.dim && v.iter().all(|x|x.is_finite())),"wide input order/shape");
        // Validate the entire logical phase and admitted homes before staging.
        let mut counts=BTreeMap::<u32,usize>::new();
        for (id,_) in rows{let identity=super::super::partition_authority::VectorId64(*id);ensure!(self.backend().authority.admits(identity),"unadmitted home");ensure!(self.backend().current(*id).is_none(),"wide append-only duplicate");*counts.entry(identity.home()).or_default()+=1;}
        for(home,count) in counts{ensure!(self.backend().authority.remaining(home)>=count,"home record capacity before staging");}
        self.failed=true;let store=self.backend().vector_store().unwrap();
        for chunk in rows.chunks(store.limits.append_rows){store.stage_identity(chunk)?;thread::scope(|scope|{for group in chunk.chunks(chunk.len().div_ceil(threads)){let kernel=&self.kernel;scope.spawn(move||{for(id,v) in group{kernel.add(*id,v);}});}});store.finish_stage();}
        self.failed=false;Ok(())
    }
    pub fn balance_phase(&mut self,threads:usize)->Result<()> {ensure!(!self.failed && threads>0 && threads<=4,"wide balance state");self.failed=true;self.kernel.balance_index_parallel(threads);self.failed=false;Ok(())}
    /// Moves one existing leaf at an exclusive completed phase boundary. There
    /// is no actor-to-actor wait; the coordinator holds one fixed stripe while
    /// it removes source authority, installs destination state and advances epoch.
    pub fn move_leaf(&mut self,leaf:u32,destination:usize)->Result<usize>{
        ensure!(!self.failed && destination<self.backend().actors.len(),"wide placement state");let b=self.backend();let source=*b.leaf_owner.get(&leaf).ok_or_else(||anyhow::anyhow!("unknown leaf"))?;if source==destination{return Ok(0);}
        let rows=b.actor(source).call(move|s|s.leaves.get(&leaf).map(|v|v.ids.len())).ok_or_else(||anyhow::anyhow!("source leaf missing"))?;
        ensure!(b.actor(destination).call(|s|s.physical).checked_add(rows).is_some_and(|v|v<=b.physical_capacity.load(Ordering::Relaxed)),"destination physical capacity");
        self.failed=true;let b=self.backend();
        let payload=b.actor(source).call(move|s|{let p=s.leaves.remove(&leaf).unwrap();s.physical-=p.ids.len();p});
        let mut moved=0;
        for (&id,&version) in payload.ids.iter().zip(&payload.versions){let locks=b.authority.locks([super::super::partition_authority::VectorId64(id)]);let _guard=locks[0].lock();let identity=super::super::partition_authority::VectorId64(id);let location=b.authority.location(identity).ok_or_else(||anyhow::anyhow!("move authority missing"))?;if location.owner!=source{continue;}let current=b.actor(source).call(move|s|s.current.read().get(&id).copied());if current!=Some(version){continue;}let handle=b.actor(source).call(move|s|{s.current.write().remove(&id);s.vectors.remove(&id).expect("move immutable handle")});b.actor(destination).call(move|s|{assert!(s.current.write().insert(id,version).is_none());assert!(s.vectors.insert(id,handle).is_none());});b.authority.transfer(identity,location,destination)?;moved+=1;}
        b.actor(destination).call(move|s|{s.physical+=payload.ids.len();assert!(s.leaves.insert(leaf,payload).is_none());});b.leaf_owner.insert(leaf,destination);b.moves.fetch_add(1,Ordering::Relaxed);self.failed=false;Ok(moved)
    }
    /// Legacy publication is a checked, explicit compatibility boundary. The
    /// wide path cannot reinterpret its tagged postings as legacy values.
    pub fn require_legacy32(&self)->Result<()> {for actor in &self.backend().actors{ensure!(actor.call(|s|s.current.read().keys().all(|id|u32::try_from(*id).is_ok())),"legacy32 publication rejects high vector identity before writes");}Ok(())}
    pub async fn save(&mut self,p:&BlockfileProvider)->Result<Snapshot64>{
        ensure!(!self.failed,"wide publication after failed phase");let b=self.backend();let parent=self.previous.as_ref();let generation=parent.map_or(Some(1),|v|v.generation.checked_add(1)).ok_or_else(||anyhow::anyhow!("generation exhausted"))?;
        let mut changed:BTreeSet<_>=self.kernel.dirty_nodes.iter().map(|v|*v).collect();changed.extend(self.kernel.tombstones.iter().map(|v|*v));
        let mut versions=b.publication_dirty_versions();versions.extend(self.kernel.dirty_versions.iter().map(|v|*v));
        let store=b.vector_store().unwrap();let vector_parent=store.latest()?;
        if changed.is_empty() && versions.is_empty() && parent.is_some_and(|v|v.roots.vector_data_id==vector_parent){return Ok(parent.unwrap().clone());}
        // Preflight all leaf copies and codec sizes before opening any writer.
        // The metadata key sets remain O(changed keys), not a constant bound.
        for id in &changed{if self.kernel.nodes.get(id).is_some_and(|n|matches!(n.value(),TreeNode::Leaf(_))){let leaf=b.publication_leaf(*id,OWNER_PUBLICATION_LEAF_BYTES)?;ensure!(leaf.ids.len()<=store.limits.split_rows,"saved leaf row admission");super::super::posting64::encode(&leaf.ids,&leaf.versions,&leaf.codes,self.kernel.code_size())?;}}
        let option=|root:Option<uuid::Uuid>|{let mut o=BlockfileWriterOptions::new(String::new()).ordered_mutations();if let Some(root)=root{o=o.fork(root);}o};
        let pl=p.write::<u32,Vec<u32>>(option(parent.map(|v|v.roots.posting_list_id))).await.map_err(|e|anyhow::anyhow!("{e}"))?;
        let sm=p.write::<u32,u32>(option(parent.map(|v|v.roots.scalar_metadata_id))).await.map_err(|e|anyhow::anyhow!("{e}"))?;
        let vd=p.write::<u32,Vec<f32>>(option(Some(vector_parent))).await.map_err(|e|anyhow::anyhow!("{e}"))?;
        let ln=p.write::<u32,HierarchicalLeafNode<'_>>(option(parent.map(|v|v.roots.leaf_node_id))).await.map_err(|e|anyhow::anyhow!("{e}"))?;
        let inn=p.write::<u32,HierarchicalInternalNode<'_>>(option(parent.map(|v|v.roots.internal_node_id))).await.map_err(|e|anyhow::anyhow!("{e}"))?;
        macro_rules! io{($v:expr)=>{$v.map_err(|e|anyhow::anyhow!("{e}"))?};}
        io!(sm.set(PREFIX_DIM,SINGLETON_KEY,self.kernel.dim as u32).await);io!(sm.set("format",SINGLETON_KEY,FORMAT).await);io!(sm.set("generation_hi",SINGLETON_KEY,(generation>>32) as u32).await);io!(sm.set("generation_lo",SINGLETON_KEY,generation as u32).await);io!(sm.set("metric",SINGLETON_KEY,metric_tag(&self.kernel.distance_fn)).await);io!(sm.set(PREFIX_NEXT_NODE,SINGLETON_KEY,self.kernel.next_node_id.load(Ordering::Relaxed)).await);io!(sm.set(PREFIX_ROOT,SINGLETON_KEY,self.kernel.root_id()).await);
        for ids in versions.iter().copied().collect::<Vec<_>>().chunks(64){for(id,v) in b.publication_versions(ids)?{io!(sm.set(&version_prefix(id),id as u32,u32::from(v)).await);}}
        for id in &changed{
            if let Some(node)=self.kernel.nodes.get(id){let centroid=node.centroid().to_vec();match node.value(){
                TreeNode::Leaf(meta)=>{let parent=meta.parent_id.unwrap_or(NO_PARENT);let length=u32::try_from(meta.length)?;let code=meta.centroid_code.clone();drop(node);let leaf=b.publication_leaf(*id,OWNER_PUBLICATION_LEAF_BYTES)?;io!(ln.set("",*id,HierarchicalLeafNode{parent,length,centroid_code:&code}).await);io!(inn.delete::<_,HierarchicalInternalNode<'_>>("",*id).await);let words=super::super::posting64::encode(&leaf.ids,&leaf.versions,&leaf.codes,self.kernel.code_size())?;io!(pl.set("",*id,words).await);}
                TreeNode::Internal(v)=>{let parent=v.parent_id.unwrap_or(NO_PARENT);let children=v.children.clone();let code=v.centroid_code.clone();drop(node);io!(inn.set("",*id,HierarchicalInternalNode{parent,children:&children,centroid_code:&code}).await);io!(ln.delete::<_,HierarchicalLeafNode<'_>>("",*id).await);io!(pl.delete::<_,Vec<u32>>("",*id).await);}
            }io!(vd.set(PREFIX_CENTROID,*id,centroid).await);
            }else{io!(ln.delete::<_,HierarchicalLeafNode<'_>>("",*id).await);io!(inn.delete::<_,HierarchicalInternalNode<'_>>("",*id).await);io!(pl.delete::<_,Vec<u32>>("",*id).await);io!(vd.delete::<_,Vec<f32>>(PREFIX_CENTROID,*id).await);}
        }
        let pl=io!(pl.commit::<u32,Vec<u32>>().await);let sm=io!(sm.commit::<u32,u32>().await);let vd=io!(vd.commit::<u32,Vec<f32>>().await);let ln=io!(ln.commit::<u32,HierarchicalLeafNode<'_>>().await);let inn=io!(inn.commit::<u32,HierarchicalInternalNode<'_>>().await);
        let roots=super::super::HierarchicalSpannIds{posting_list_id:pl.id(),scalar_metadata_id:sm.id(),vector_data_id:vd.id(),leaf_node_id:ln.id(),internal_node_id:inn.id()};
        io!(pl.flush::<u32,Vec<u32>>().await);io!(sm.flush::<u32,u32>().await);io!(vd.flush::<u32,Vec<f32>>().await);io!(ln.flush::<u32,HierarchicalLeafNode<'_>>().await);io!(inn.flush::<u32,HierarchicalInternalNode<'_>>().await);
        let snapshot=Snapshot64{format:FORMAT_NAME.into(),roots,generation,dim:self.kernel.dim,metric:self.kernel.distance_fn.clone(),config:self.kernel.config.clone(),max_leaf_rows:store.limits.split_rows};
        #[cfg(test)] if self.fail_before_ack{anyhow::bail!("injected after all new64 flushes before acknowledgement");}
        store.acknowledge_published_root(snapshot.roots.vector_data_id);b.acknowledge_publication();self.kernel.dirty_nodes.clear();self.kernel.tombstones.clear();self.kernel.dirty_versions.clear();self.kernel.dirty_embeddings.clear();self.previous=Some(snapshot.clone());Ok(snapshot)
    }
}

pub struct Reader64 {
    routing:HierarchicalSpannWriter<u64>,snapshot:Snapshot64,
    posting:chroma_blockstore::BlockfileReader<'static,u32,&'static [u32]>,
    scalar:chroma_blockstore::BlockfileReader<'static,u32,u32>,
    vectors:chroma_blockstore::BlockfileReader<'static,u32,&'static [f32]>,
}
#[derive(Debug)] pub struct Query64 {pub results:Vec<(u64,f32)>,pub selected_leaves:Vec<u32>,pub physical_rows:usize,pub live_rows:usize,pub candidates:usize,pub exact_reranks:usize}
impl Reader64 {
    pub async fn open(p:&BlockfileProvider,snapshot:Snapshot64,node_cap:usize)->Result<Self>{
        ensure!(snapshot.format==FORMAT_NAME && node_cap>0 && node_cap<=4096 && snapshot.max_leaf_rows>0,"new64 format/admission");
        macro_rules! io{($v:expr)=>{$v.map_err(|e|anyhow::anyhow!("{e}"))?};}
        let scalar=io!(p.read::<u32,u32>(BlockfileReaderOptions::new(snapshot.roots.scalar_metadata_id,String::new())).await);
        ensure!(io!(scalar.get("format",SINGLETON_KEY).await)==Some(FORMAT) && io!(scalar.get(PREFIX_DIM,SINGLETON_KEY).await)==Some(snapshot.dim as u32),"snapshot format/dimension mismatch");
        ensure!(io!(scalar.get("generation_hi",SINGLETON_KEY).await)==Some((snapshot.generation>>32) as u32) && io!(scalar.get("generation_lo",SINGLETON_KEY).await)==Some(snapshot.generation as u32) && io!(scalar.get("metric",SINGLETON_KEY).await)==Some(metric_tag(&snapshot.metric)),"snapshot generation/metric mismatch");
        let vectors=io!(p.read::<u32,&[f32]>(BlockfileReaderOptions::new(snapshot.roots.vector_data_id,String::new())).await);
        let ln=io!(p.read::<u32,HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(snapshot.roots.leaf_node_id,String::new())).await);let inn=io!(p.read::<u32,HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(snapshot.roots.internal_node_id,String::new())).await);
        let routing=HierarchicalSpannWriter::<u64>::new_identity(snapshot.dim,snapshot.metric.clone(),snapshot.config.clone());routing.nodes.clear();
        for(_,id,v) in io!(ln.get_range(""..="",..).await){ensure!(routing.nodes.len()<node_cap && !routing.nodes.contains_key(&id),"new64 node capacity/duplicate");let centroid=io!(vectors.get(PREFIX_CENTROID,id).await).ok_or_else(||anyhow::anyhow!("missing centroid"))?.to_vec();ensure!(centroid.len()==snapshot.dim && centroid.iter().all(|v|v.is_finite()),"centroid shape");routing.nodes.insert(id,TreeNode::Leaf(LeafNode{centroid,centroid_code:v.centroid_code.to_vec(),ids:vec![],versions:vec![],codes:vec![],length:v.length as usize,parent_id:(v.parent!=NO_PARENT).then_some(v.parent)}));}
        for(_,id,v) in io!(inn.get_range(""..="",..).await){ensure!(routing.nodes.len()<node_cap && !routing.nodes.contains_key(&id),"new64 node capacity/duplicate");let centroid=io!(vectors.get(PREFIX_CENTROID,id).await).ok_or_else(||anyhow::anyhow!("missing centroid"))?.to_vec();ensure!(centroid.len()==snapshot.dim && centroid.iter().all(|v|v.is_finite()),"centroid shape");routing.nodes.insert(id,TreeNode::Internal(InternalNode{centroid,centroid_code:v.centroid_code.to_vec(),children:v.children.to_vec(),parent_id:(v.parent!=NO_PARENT).then_some(v.parent)}));}
        let root=io!(scalar.get(PREFIX_ROOT,SINGLETON_KEY).await).ok_or_else(||anyhow::anyhow!("missing root"))?;routing.root_id.store(root,Ordering::Relaxed);
        let mut pending=vec![(root,None)];let mut seen=BTreeSet::new();while let Some((id,parent))=pending.pop(){ensure!(seen.insert(id),"saved topology cycle");let node=routing.nodes.get(&id).ok_or_else(||anyhow::anyhow!("missing saved child"))?;match node.value(){TreeNode::Leaf(v)=>ensure!(v.parent_id==parent,"leaf parent"),TreeNode::Internal(v)=>{ensure!(v.parent_id==parent,"internal parent");pending.extend(v.children.iter().map(|child|(*child,Some(id))));}}}ensure!(seen.len()==routing.nodes.len(),"unreachable saved topology");
        let posting=io!(p.read::<u32,&[u32]>(BlockfileReaderOptions::new(snapshot.roots.posting_list_id,String::new())).await);vectors.clear_loaded_blocks();scalar.clear_loaded_blocks();
        Ok(Self{routing,snapshot,posting,scalar,vectors})
    }
    async fn live_posting(&self,leaf:u32)->Result<super::super::posting64::Posting64>{
        let words=self.posting.get("",leaf).await.map_err(|e|anyhow::anyhow!("{e}"))?.ok_or_else(||anyhow::anyhow!("missing new64 posting"))?;
        let posting=super::super::posting64::decode(words,self.routing.code_size(),self.snapshot.max_leaf_rows)?;
        ensure!(self.routing.nodes.get(&leaf).is_some_and(|v|matches!(v.value(),TreeNode::Leaf(n) if n.length==posting.ids.len())),"posting cardinality");Ok(posting)
    }
    pub async fn query(&mut self,query:&[f32],k:usize,rerank:usize,policy:&ReadBeamPolicy)->Result<Query64>{
        ensure!(query.len()==self.snapshot.dim && query.iter().all(|v|v.is_finite()) && k>0 && rerank>=k && rerank<=65536,"query64 admission");
        let leaves=self.routing.navigate_with_policy(query,NavigationMode::Fp,policy);let mut best=HashMap::<u64,f32>::new();let mut physical_rows=0;let mut live_rows=0;let qnorm=f32::dot(query,query).unwrap() as f32;let qnorm=qnorm.sqrt();
        for (leaf,_) in &leaves{let p=self.live_posting(*leaf).await?;physical_rows+=p.ids.len();let centroid=self.routing.nodes.get(leaf).unwrap().centroid().to_vec();let residual:Vec<_>=query.iter().zip(&centroid).map(|(q,c)|q-c).collect();let cnorm=(f32::dot(&centroid,&centroid).unwrap() as f32).sqrt();let cdot=f32::dot(&centroid,query).unwrap() as f32;let qq=QuantizedQuery::new(&residual,self.routing.padded_bytes(),cnorm,cdot,qnorm);
            for(i,id) in p.ids.iter().enumerate(){let current=self.scalar.get(&version_prefix(*id),*id as u32).await.map_err(|e|anyhow::anyhow!("{e}"))?.ok_or_else(||anyhow::anyhow!("missing pinned current version"))?;ensure!(current<=255,"invalid current version");if current as u8 & DELETED_BIT!=0 || current as u8!=p.versions[i]{continue;}live_rows+=1;let score=Code::<1,_>::new(code_slice(&p.codes,i,self.routing.code_size())).distance_quantized_query(&self.snapshot.metric,&qq);ensure!(score.is_finite(),"nonfinite posting score");best.entry(*id).and_modify(|v|{if score<*v{*v=score;}}).or_insert(score);}
            self.posting.clear_loaded_blocks();self.scalar.clear_loaded_blocks();
        }
        let candidates=best.len();let mut scored:Vec<_>=best.into_iter().collect();if scored.len()>rerank{scored.select_nth_unstable_by(rerank-1,|a,b|a.1.partial_cmp(&b.1).unwrap());scored.truncate(rerank);}scored.sort_unstable_by(|a,b|a.1.partial_cmp(&b.1).unwrap());let exact_reranks=scored.len();
        for(id,score) in &mut scored{let vector=self.vectors.get(&vector_prefix(*id),*id as u32).await.map_err(|e|anyhow::anyhow!("{e}"))?.ok_or_else(||anyhow::anyhow!("missing pinned embedding"))?;ensure!(vector.len()==self.snapshot.dim,"pinned embedding shape");*score=self.snapshot.metric.distance(query,vector);ensure!(score.is_finite(),"exact score finite");}scored.sort_unstable_by(|a,b|a.1.partial_cmp(&b.1).unwrap());scored.truncate(k);self.vectors.clear_loaded_blocks();
        Ok(Query64{results:scored,selected_leaves:leaves.into_iter().map(|v|v.0).collect(),physical_rows,live_rows,candidates,exact_reranks})
    }
    pub async fn census(&mut self)->Result<BTreeMap<u64,(u8,Vec<u8>,Vec<u32>)>>{let mut result=BTreeMap::new();let leaves:Vec<_>=self.routing.nodes.iter().filter_map(|v|matches!(v.value(),TreeNode::Leaf(_)).then_some(*v.key())).collect();for leaf in leaves{let p=self.live_posting(leaf).await?;for(i,id) in p.ids.iter().enumerate(){let current=self.scalar.get(&version_prefix(*id),*id as u32).await.map_err(|e|anyhow::anyhow!("{e}"))?.ok_or_else(||anyhow::anyhow!("missing census version"))?;if current as u8!=p.versions[i] || current as u8 & DELETED_BIT!=0{continue;}let vector=self.vectors.get(&vector_prefix(*id),*id as u32).await.map_err(|e|anyhow::anyhow!("{e}"))?.ok_or_else(||anyhow::anyhow!("missing census embedding"))?;let code=code_slice(&p.codes,i,self.routing.code_size()).to_vec();ensure!(result.insert(*id,(current as u8,code,vector.iter().map(|v|v.to_bits()).collect())).is_none(),"duplicate current membership");}self.posting.clear_loaded_blocks();self.scalar.clear_loaded_blocks();self.vectors.clear_loaded_blocks();}Ok(result)}
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage,Storage};
    fn provider(path:&std::path::Path)->BlockfileProvider{let storage=Storage::Local(LocalStorage::new(path.to_str().unwrap()));BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(storage,16*1024,new_cache_for_test(),new_cache_for_test(),2))}
    fn config()->HierarchicalSpannConfig{HierarchicalSpannConfig{branching_factor:4,split_threshold:16,merge_threshold:0,write_beam_min:2,write_beam_max:4,..Default::default()}}
    fn rows()->Vec<(u64,Vec<f32>)>{(0..3u64).flat_map(|home|(1..=32u64).map(move|local|{let id=(home<<32)|local;(id,(0..32).map(|d|if d==2{-0.0}else{(((home*211+local*17+d*13)%97) as f32)/19.+if d==0{(local%8) as f32*16.}else{0.}}).collect())})).collect()}
    fn bits(rows:&[(u64,f32)])->Vec<(u64,u32)>{rows.iter().map(|(id,d)|(*id,d.to_bits())).collect()}
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn wide_identity_insert_move_split_save_routed_query_and_pinned_generation(){
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let input=rows();let mut owner=WideOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,&[0,1,2],40,p.clone(),super::super::super::owner_vector_store::VectorLimits::default()).unwrap();
        owner.add_phase(&input[..48],4).unwrap();owner.balance_phase(4).unwrap();assert!(owner.kernel.stats.splits.load(Ordering::Relaxed)>0);
        assert!(owner.require_legacy32().is_err());let first=owner.save(&p).await.unwrap();let mut old=Reader64::open(&p,first.clone(),256).await.unwrap();let old_census=old.census().await.unwrap();assert_eq!(old_census.len(),48);
        let policy=ReadBeamPolicy::uniform(Some(2.0),256,256);let old_hits=old.query(&input[6].1,8,96,&policy).await.unwrap();assert!(old_hits.results.iter().any(|v|v.0==7));
        let leaf=owner.kernel.nodes.iter().find_map(|n|{if matches!(n.value(),TreeNode::Leaf(_)) && owner.backend().snapshot(*n.key()).unwrap().ids.iter().any(|id|*id>u32::MAX as u64){Some(*n.key())}else{None}}).unwrap();
        let before=owner.backend().snapshot(leaf).unwrap();let moved_id=before.ids.iter().copied().find(|id|owner.backend().current(*id).is_some()).unwrap();let identity=super::super::super::partition_authority::VectorId64(moved_id);let location=owner.backend().authority.location(identity).unwrap();let version=owner.backend().current(moved_id).unwrap();
        let moved=owner.move_leaf(leaf,1).unwrap();assert!(moved>0);let now=owner.backend().authority.location(identity).unwrap();assert_eq!(now.owner,1);assert_eq!(now.epoch,location.epoch+1);assert_eq!(owner.backend().current(moved_id),Some(version));assert!(owner.backend().actor(0).call(move|s|!s.current.read().contains_key(&moved_id) && !s.vectors.contains_key(&moved_id)));assert!(owner.backend().authority.transfer(identity,location,0).is_err());
        let splits=owner.kernel.stats.splits.load(Ordering::Relaxed);owner.add_phase(&input[48..],4).unwrap();owner.balance_phase(4).unwrap();assert!(owner.kernel.stats.splits.load(Ordering::Relaxed)>splits,"structural mutation after ownership move");
        let previous_vector=owner.backend().vector_store().unwrap().latest().unwrap();let dirty=owner.backend().publication_dirty_versions();owner.fail_before_ack=true;assert!(owner.save(&p).await.is_err());assert_eq!(owner.previous.as_ref().unwrap().generation,1);assert_eq!(owner.backend().vector_store().unwrap().latest().unwrap(),previous_vector);assert_eq!(owner.backend().publication_dirty_versions(),dirty);assert_eq!(old.census().await.unwrap(),old_census);owner.fail_before_ack=false;
        let second=owner.save(&p).await.unwrap();assert_eq!(second.generation,2);let mut saved=Reader64::open(&p,second.clone(),256).await.unwrap();let census=saved.census().await.unwrap();assert_eq!(census.len(),input.len());
        for(id,v) in &input{assert_eq!(census[id].2,v.iter().map(|v|v.to_bits()).collect::<Vec<_>>());assert_eq!(census[id].0,owner.backend().current(*id).unwrap());}
        for node in owner.kernel.nodes.iter(){if matches!(node.value(),TreeNode::Leaf(_)){let leaf=owner.backend().snapshot(*node.key()).unwrap();for(i,id) in leaf.ids.iter().enumerate(){if owner.backend().current(*id)==Some(leaf.versions[i]){assert_eq!(census[id].1,code_slice(&leaf.codes,i,owner.kernel.code_size()));let vector=input.iter().find(|v|v.0==*id).unwrap();assert_eq!(census[id].1,Code::<1>::quantize(&vector.1,&leaf.centroid).as_ref());}}}}
        let all=saved.query(&input[6].1,96,96,&policy).await.unwrap();assert_eq!(all.results.len(),96);let actual:BTreeMap<_,_>=all.results.iter().map(|(id,d)|(*id,d.to_bits())).collect();for(id,v) in &input{assert_eq!(actual[id],DistanceFunction::Euclidean.distance(&input[6].1,v).to_bits());}
        for id in [7,(1u64<<32)|7,(2u64<<32)|7]{assert!(actual.contains_key(&id));}
        let narrow=saved.query(&input[6].1,4,8,&ReadBeamPolicy::uniform(Some(2.0),1,1)).await.unwrap();assert!(narrow.selected_leaves.len()<saved.routing.nodes.iter().filter(|n|matches!(n.value(),TreeNode::Leaf(_))).count());assert!(narrow.physical_rows<all.physical_rows);assert!(narrow.exact_reranks<=8);assert!(narrow.candidates>=narrow.results.len());
        assert_eq!(old.census().await.unwrap(),old_census);assert_eq!(bits(&old.query(&input[6].1,8,96,&policy).await.unwrap().results),bits(&old_hits.results));
        let mut bad=second.clone();bad.generation+=1;assert!(Reader64::open(&p,bad,256).await.is_err());let mut bad=second;bad.format="legacy32".into();assert!(Reader64::open(&p,bad,256).await.is_err());
        let root_before=owner.backend().vector_store().unwrap().latest().unwrap();assert!(owner.add_phase(&[(7,input[0].1.clone())],1).is_err());assert!(owner.add_phase(&[((3u64<<32)|7,input[0].1.clone())],1).is_err());assert_eq!(owner.backend().vector_store().unwrap().latest().unwrap(),root_before);
        let reused=owner.save(&p).await.unwrap();assert_eq!(reused.generation,2);assert_eq!(reused.roots.posting_list_id,owner.previous.as_ref().unwrap().roots.posting_list_id);
        println!("WIDE_IDENTITY {}",serde_json::json!({"rows":input.len(),"homes":3,"physical_owners":2,"moves":moved,"generations":2,"splits":owner.kernel.stats.splits.load(Ordering::Relaxed),"all_selected_leaves":all.selected_leaves.len(),"narrow_selected_leaves":narrow.selected_leaves.len(),"all_physical_rows":all.physical_rows,"narrow_physical_rows":narrow.physical_rows,"narrow_exact_reranks":narrow.exact_reranks,"authority_total_cap":owner.backend().authority.total_capacity(),"vector_store":owner.backend().vector_store().unwrap().report()}));
        drop(owner);assert_eq!(saved.census().await.unwrap(),census);assert_eq!(old.census().await.unwrap(),old_census);
    }
    #[tokio::test(flavor="multi_thread",worker_threads=2)]
    async fn wide_identity_transfer_gap_and_rejected_phase_preserve_authority(){
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let input=rows();let mut owner=WideOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,&[0,1,2],4,p.clone(),super::super::super::owner_vector_store::VectorLimits::default()).unwrap();
        let selected:Vec<_>=[7,(1u64<<32)|7,(2u64<<32)|7].iter().map(|id|input.iter().find(|v|v.0==*id).unwrap().clone()).collect();owner.add_phase(&selected,1).unwrap();
        let before=owner.save(&p).await.unwrap();let mut old=Reader64::open(&p,before,32).await.unwrap();let old_census=old.census().await.unwrap();let b=owner.backend().clone();
        let mut target=shell(&b.snapshot(0).unwrap());target.parent_id=None;b.leaf_owner.insert(1,1);b.install(1,target);
        let high=(1u64<<32)|7;assert_eq!(b.bump(high,Some(1)),Some(2));
        let (removed,seen)=mpsc::channel();let (resume,resumed)=mpsc::channel();*b.transfer_pause.lock()=Some((removed,resumed));let moving=b.clone();let vector=selected[1].1.clone();let handle=thread::spawn(move||moving.register(1,high,2,&vector));seen.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
        let (done,read)=mpsc::channel();let reading=b.clone();let reader=thread::spawn(move||{done.send(reading.current(high)).unwrap();});assert!(read.recv_timeout(std::time::Duration::from_millis(30)).is_err());assert_eq!(b.current(7),Some(1),"same low key in another home is independent");resume.send(()).unwrap();assert!(handle.join().unwrap().is_some());assert_eq!(read.recv_timeout(std::time::Duration::from_secs(5)).unwrap(),Some(2));reader.join().unwrap();assert_eq!(b.authority.location(super::super::super::partition_authority::VectorId64(high)).unwrap().epoch,1);
        b.actor(1).call(move|s|{s.current.write().insert(high,127);});assert_eq!(b.bump(high,Some(127)),None);assert_eq!(b.current(high),Some(127),"no seven-bit rollover revival");
        // Inject a missing location after an exclusive phase. A partial move
        // cannot return a usable writer or publish its incomplete state.
        b.authority.remove_identity(super::super::super::partition_authority::VectorId64(7));assert!(owner.move_leaf(0,1).is_err());assert!(owner.failed);assert!(owner.save(&p).await.is_err());assert!(owner.add_phase(&[(8,input[0].1.clone())],1).is_err());assert_eq!(old.census().await.unwrap(),old_census);
        let mut limited=WideOwnerWriter::new(32,DistanceFunction::Euclidean,config(),2,&[1],2,p,super::super::super::owner_vector_store::VectorLimits::default()).unwrap();assert!(limited.add_phase(&input[32..35],1).is_err());assert!(limited.backend().vector_store().unwrap().latest().is_err());assert!(!limited.failed);assert!(limited.backend().actor(0).call(|s|s.current.read().is_empty() && s.vectors.is_empty()));
    }

}
