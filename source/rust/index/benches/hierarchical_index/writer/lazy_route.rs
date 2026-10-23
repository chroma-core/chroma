//! Fixed-parent routing without loading mutable writers for every owner.
//! The caller separately audits immutable inputs; this is not a storage lease.
use super::{HierarchicalSpannIds, HierarchicalSpannWriter};
use super::distributed::{global_write_count, WriteChild, WriteDestination};
use super::super::{common::{NodeId, ReadBeamPolicy, TreeNode}, config::HierarchicalSpannConfig, persistance::{PREFIX_CENTROID,PREFIX_DIM,PREFIX_ROOT,SINGLETON_KEY,NO_PARENT}};
use anyhow::{ensure,Context,Result};
use chroma_blockstore::{BlockfileReader,arrow::provider::BlockfileReaderOptions,provider::BlockfileProvider};
use chroma_distance::DistanceFunction;
use chroma_types::hierarchical_spann::{HierarchicalLeafNode,HierarchicalInternalNode};
use serde::{Serialize,Deserialize};
use sha2::{Digest,Sha256};
use std::collections::{BTreeMap,HashSet};

#[derive(Clone,Debug,Serialize,Deserialize,PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub root:NodeId,pub dimension:usize,pub widths:Vec<usize>,pub nodes:usize,
    pub topology_sha256:String,pub policy:serde_json::Value,
    pub centroid_payload_bytes:usize,pub child_payload_bytes:usize,
}
fn settings(c:&HierarchicalSpannConfig)->serde_json::Value {
    serde_json::json!({"tau_bits":c.write_beam_tau.to_bits(),"min":c.write_beam_min,"max":c.write_beam_max,"level_tau_bits":c.write_level_taus.iter().map(|v|v.map(f64::to_bits)).collect::<Vec<_>>(),"level_min_pct_bits":c.write_level_min_pcts.iter().map(|v|v.to_bits()).collect::<Vec<_>>()})
}
impl Bootstrap {
    /// Capture complete node identity/edges/centroid bits before any registration.
    /// Posting length is excluded because add-only registration changes it.
    pub fn capture(w:&HierarchicalSpannWriter)->Result<Self> {
        let mut rows=BTreeMap::new();let mut cb=0usize;let mut eb=0usize;
        for row in w.nodes.iter() {
            let (leaf,parent,children)=match row.value(){TreeNode::Leaf(n)=>(true,n.parent_id,vec![]),TreeNode::Internal(n)=>(false,n.parent_id,n.children.clone())};
            let centroid=row.value().centroid();ensure!(centroid.len()==w.dim && centroid.iter().all(|v|v.is_finite()),"invalid bootstrap centroid");
            cb=cb.checked_add(centroid.len()*4).context("centroid bytes overflow")?;eb=eb.checked_add(children.len()*4).context("edge bytes overflow")?;
            rows.insert(*row.key(),serde_json::json!({"leaf":leaf,"parent":parent,"children":children,"centroid_bits":centroid.iter().map(|v|v.to_bits()).collect::<Vec<_>>()}));
        }
        let widths=w.level_node_counts();ensure!(widths.iter().sum::<usize>()==rows.len(),"bootstrap requires exactly reachable nodes");
        Ok(Self{root:w.root_id(),dimension:w.dim,widths,nodes:rows.len(),topology_sha256:format!("{:x}",Sha256::digest(serde_json::to_vec(&rows)?)),policy:settings(&w.config),centroid_payload_bytes:cb,child_payload_bytes:eb})
    }
}
#[derive(Clone)]
struct Node {centroid:Vec<f32>,children:Vec<NodeId>,parent:Option<NodeId>,leaf:bool}
#[derive(Default,Clone,Debug,Serialize)]
pub struct RoutingMetrics {
    pub fetched_nodes:usize,pub node_requests:usize,pub cache_hits:usize,
    pub centroid_payload_bytes:usize,pub child_payload_bytes:usize,
    pub cached_nodes_peak:usize,pub transient_centroid_bytes_peak:usize,pub transient_child_bytes_peak:usize,
    pub frontier_peak:usize,pub retained_leaves_peak:usize,pub visited_peak:usize,
    pub frontier_capacity_peak:usize,pub leaves_capacity_peak:usize,pub visited_capacity_peak:usize,
    pub owned_payload_and_slots_peak_estimate:usize,
}
pub struct LazyOwner {
    leaf:BlockfileReader<'static,u32,HierarchicalLeafNode<'static>>,
    internal:BlockfileReader<'static,u32,HierarchicalInternalNode<'static>>,
    vector:BlockfileReader<'static,u32,&'static [f32]>,
    pub bootstrap:Bootstrap, config:HierarchicalSpannConfig, metric:DistanceFunction,
    cache:BTreeMap<NodeId,Node>,pub metrics:RoutingMetrics,
}
impl LazyOwner {
    pub async fn open(p:&BlockfileProvider,ids:HierarchicalSpannIds,metric:DistanceFunction,config:HierarchicalSpannConfig,b:Bootstrap)->Result<Self> {
        ensure!(config.max_replicas==1 && b.dimension>0 && b.dimension<=4096 && b.nodes>0 && b.nodes<=100_000 && b.widths.len()<=128 && b.widths.first()==Some(&1) && b.widths.iter().try_fold(0usize,|a,n|a.checked_add(*n))==Some(b.nodes) && b.policy==settings(&config),"bootstrap policy/count mismatch");
        let scalar=p.read::<u32,u32>(BlockfileReaderOptions::new(ids.scalar_metadata_id,String::new())).await.map_err(|e|anyhow::anyhow!("scalar open {e}"))?;
        ensure!(scalar.get(PREFIX_ROOT,SINGLETON_KEY).await.map_err(|e|anyhow::anyhow!("root read {e}"))?==Some(b.root) && scalar.get(PREFIX_DIM,SINGLETON_KEY).await.map_err(|e|anyhow::anyhow!("dimension read {e}"))?==Some(b.dimension as u32),"bootstrap root/dimension mismatch");
        let leaf=p.read::<u32,HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(ids.leaf_node_id,String::new())).await.map_err(|e|anyhow::anyhow!("leaf open {e}"))?;
        let internal=p.read::<u32,HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(ids.internal_node_id,String::new())).await.map_err(|e|anyhow::anyhow!("internal open {e}"))?;
        let vector=p.read::<u32,&'static [f32]>(BlockfileReaderOptions::new(ids.vector_data_id,String::new())).await.map_err(|e|anyhow::anyhow!("vector open {e}"))?;
        Ok(Self{leaf,internal,vector,bootstrap:b,config,metric,cache:BTreeMap::new(),metrics:RoutingMetrics::default()})
    }
    async fn node(&mut self,id:NodeId)->Result<Node> {
        self.metrics.node_requests+=1;
        if let Some(node)=self.cache.get(&id){self.metrics.cache_hits+=1;self.metrics.transient_centroid_bytes_peak=self.metrics.transient_centroid_bytes_peak.max(node.centroid.len()*4);self.metrics.transient_child_bytes_peak=self.metrics.transient_child_bytes_peak.max(node.children.len()*4);return Ok(node.clone());}
        ensure!(self.cache.len()<256,"routing node cache cap exceeded before fetch");
        let (leaf,parent,children)=if let Some(n)=self.internal.get("",id).await.map_err(|e|anyhow::anyhow!("node read {e}"))? {(false,n.parent,{ensure!(n.children.len()<=256,"bounded routing child array required");n.children.to_vec()})} else {let n=self.leaf.get("",id).await.map_err(|e|anyhow::anyhow!("node read {e}"))?.context("missing reached node")?;(true,n.parent,vec![])};
        ensure!(children.len()<=256,"bounded routing child array required");
        let value=self.vector.get(PREFIX_CENTROID,id).await.map_err(|e|anyhow::anyhow!("centroid read {e}"))?.context("missing full precision centroid")?;ensure!(value.len()==self.bootstrap.dimension,"centroid dimension before copy");let centroid=value.to_vec();
        ensure!(centroid.len()==self.bootstrap.dimension && centroid.iter().all(|v|v.is_finite()),"invalid reached centroid");
        let n=Node{centroid,children,parent:if parent==NO_PARENT{None}else{Some(parent)},leaf};
        self.metrics.fetched_nodes+=1;self.metrics.centroid_payload_bytes+=n.centroid.len()*4;self.metrics.child_payload_bytes+=n.children.len()*4;
        self.cache.insert(id,n.clone());self.metrics.cached_nodes_peak=self.cache.len();self.metrics.transient_centroid_bytes_peak=self.metrics.transient_centroid_bytes_peak.max(n.centroid.len()*4);self.metrics.transient_child_bytes_peak=self.metrics.transient_child_bytes_peak.max(n.children.len()*4);Ok(n)
    }
    fn score(&self,vector:&[f32],centroid:&[f32])->Result<f32>{let d=self.metric.distance(vector,centroid);ensure!(d.is_finite(),"nonfinite route score");Ok(d)}
    fn policy(&self,widths:Vec<usize>)->ReadBeamPolicy {let c=&self.config;ReadBeamPolicy::with_level_overrides(Some(c.write_beam_tau),c.write_beam_min,c.write_beam_max,c.write_level_taus.clone(),c.write_level_min_pcts.clone(),widths)}
}
fn observe_state(owners:&mut [LazyOwner],frontier:&Vec<(usize,WriteChild)>,leaves:&Vec<WriteDestination>,visited:&HashSet<(usize,NodeId)>) {
    // Numeric payload and declared slot estimate only. BTree allocator headers,
    // provider pins/decode/cache, parser buffers and input vectors are separate.
    let slots=frontier.capacity()*std::mem::size_of::<(usize,WriteChild)>()+leaves.capacity()*std::mem::size_of::<WriteDestination>()+visited.capacity()*std::mem::size_of::<(usize,NodeId)>();
    let cached:usize=owners.iter().map(|o|o.metrics.centroid_payload_bytes+o.metrics.child_payload_bytes+o.cache.len()*std::mem::size_of::<(NodeId,Node)>()+o.metrics.transient_centroid_bytes_peak+o.metrics.transient_child_bytes_peak).sum();
    for o in owners {o.metrics.frontier_peak=o.metrics.frontier_peak.max(frontier.len());o.metrics.retained_leaves_peak=o.metrics.retained_leaves_peak.max(leaves.len());o.metrics.visited_peak=o.metrics.visited_peak.max(visited.len());o.metrics.frontier_capacity_peak=o.metrics.frontier_capacity_peak.max(frontier.capacity());o.metrics.leaves_capacity_peak=o.metrics.leaves_capacity_peak.max(leaves.capacity());o.metrics.visited_capacity_peak=o.metrics.visited_capacity_peak.max(visited.capacity());o.metrics.owned_payload_and_slots_peak_estimate=o.metrics.owned_payload_and_slots_peak_estimate.max(cached+slots);}
}
/// All rows route against the same immutable topology; this never registers IDs.
pub async fn route(owners:&mut [LazyOwner],vector:&[f32],boundaries:&[Vec<f32>])->Result<WriteDestination> {
    ensure!(!owners.is_empty() && owners.len()<=128 && boundaries.len()==owners.len(),"bounded complete owner set required");
    let mut widths=Vec::<usize>::new();let dim=owners[0].bootstrap.dimension;let policy=owners[0].bootstrap.policy.clone();let metric=owners[0].metric.clone();
    for o in owners.iter(){ensure!(o.bootstrap.dimension==dim && o.bootstrap.policy==policy && o.metric==metric,"owner policy mismatch");for (level,&n) in o.bootstrap.widths.iter().enumerate(){if widths.len()<=level{widths.resize(level+1,0);}widths[level]=widths[level].checked_add(n).context("width overflow")?;}}
    ensure!(vector.len()==dim && vector.iter().all(|v|v.is_finite()) && boundaries.iter().all(|v|v.len()==dim && v.iter().all(|x|x.is_finite())),"invalid route vector/alias");
    let policy=owners[0].policy(widths);let mut frontier=Vec::new();let mut leaves=Vec::new();let mut visited=HashSet::new();
    for (owner,o) in owners.iter_mut().enumerate(){let id=o.bootstrap.root;let n=o.node(id).await?;ensure!(n.parent.is_none(),"root has parent");frontier.push((owner,WriteChild{parent_id:id,node_id:id,is_leaf:n.leaf,distance:o.score(vector,&boundaries[owner])?}));}
    for level in 1..=128 {
        ensure!(frontier.len()<=32_768 && leaves.len()<=32_768 && visited.len()<=32_768,"routing state cap exceeded");
        observe_state(owners,&frontier,&leaves,&visited);
        frontier.sort_by(|a,b|a.1.distance.total_cmp(&b.1.distance).then(a.0.cmp(&b.0)).then(a.1.node_id.cmp(&b.1.node_id)));
        let distances:Vec<_>=frontier.iter().map(|(_,n)|n.distance).collect();let count=global_write_count(&distances,&policy,level).map_err(anyhow::Error::msg)?;ensure!(count>0,"empty route frontier");
        let mut parents=vec![Vec::new();owners.len()];
        for (owner,n) in frontier.into_iter().take(count){ensure!(visited.len()<32_768 && visited.insert((owner,n.node_id)),"route cycle");if n.is_leaf{ensure!(leaves.len()<32_768,"retained leaf cap before insertion");leaves.push(WriteDestination{owner,leaf:n.node_id,distance:n.distance});}else{parents[owner].push(n.node_id);}}
        observe_state(owners,&Vec::new(),&leaves,&visited);
        if parents.iter().all(Vec::is_empty){return leaves.into_iter().min_by(|a,b|a.distance.total_cmp(&b.distance).then(a.owner.cmp(&b.owner)).then(a.leaf.cmp(&b.leaf))).context("no reached leaf");}
        frontier=Vec::new();
        for (owner,ids) in parents.iter().enumerate(){for &id in ids {let o=&mut owners[owner];let parent=o.node(id).await?;ensure!(!parent.leaf,"expanded leaf");o.metrics.transient_centroid_bytes_peak=o.metrics.transient_centroid_bytes_peak.max(parent.centroid.len()*4);o.metrics.transient_child_bytes_peak=o.metrics.transient_child_bytes_peak.max(parent.children.len()*4);
            let parent_children_len=parent.children.len();for child in parent.children {let n=o.node(child).await?;o.metrics.transient_centroid_bytes_peak=o.metrics.transient_centroid_bytes_peak.max((parent.centroid.len()+n.centroid.len())*4);o.metrics.transient_child_bytes_peak=o.metrics.transient_child_bytes_peak.max((parent_children_len+n.children.len())*4);ensure!(n.parent==Some(id),"child parent differs");ensure!(frontier.len()<32_768,"frontier cap before insertion");frontier.push((owner,WriteChild{parent_id:id,node_id:child,is_leaf:n.leaf,distance:o.score(vector,&n.centroid)?}));}
        }}
    }
    anyhow::bail!("route exceeds depth bound")
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::distributed::{append_counted_global,CountedWriteOwnership};
    use super::super::super::common::{InternalNode,LeafNode};
    use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{Storage,local::LocalStorage};
    use std::sync::atomic::Ordering;
    fn fixture(second:bool)->HierarchicalSpannWriter {
        let mut c=HierarchicalSpannConfig::default();c.max_replicas=1;c.write_beam_min=128;c.write_beam_max=128;c.write_beam_tau=1000.;
        let w=HierarchicalSpannWriter::new(2,DistanceFunction::Euclidean,c);w.nodes.clear();
        let leaves:Vec<(u32,f32,u32)>=if second{vec![(1,4.,0),(2,6.,0)]}else{vec![(1,0.,0),(3,10.,2)]};
        w.nodes.insert(0,TreeNode::Internal(InternalNode{centroid:vec![if second{9.}else{0.},-0.],centroid_code:w.quantize_origin(&[if second{9.}else{0.},-0.]),children:if second{vec![1,2]}else{vec![1,2]},parent_id:None}));
        if !second{w.nodes.insert(2,TreeNode::Internal(InternalNode{centroid:vec![10.,-0.],centroid_code:w.quantize_origin(&[10.,-0.]),children:vec![3],parent_id:Some(0)}));}
        for (id,x,parent) in leaves{w.nodes.insert(id,TreeNode::Leaf(LeafNode{centroid:vec![x,-0.],centroid_code:w.quantize_origin(&[x,-0.]),ids:vec![],versions:vec![],codes:vec![],length:0,parent_id:Some(parent)}));}
        w.next_node_id.store(if second{3}else{4},Ordering::Relaxed);for n in w.nodes.iter(){w.mark_node_dirty(*n.key());}w
    }
    fn provider(path:&std::path::Path)->BlockfileProvider {BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(LocalStorage::new(path.to_str().unwrap())),3*1024*1024,new_cache_for_test(),new_cache_for_test(),16))}
    #[tokio::test]
    async fn lazy_routes_equal_sequential_adds_and_topology_stays_fixed() {
        let dirs=[tempfile::tempdir().unwrap(),tempfile::tempdir().unwrap()];let mut eager=Vec::new();let mut lazy=Vec::new();let mut original=Vec::new();
        for (i,dir) in dirs.iter().enumerate(){let w=fixture(i==1);let p=provider(dir.path());let ids=w.commit(&p,None).await.unwrap().flush().await.unwrap();let reopened=HierarchicalSpannWriter::open(&p,ids.clone(),w.distance_fn.clone(),w.config.clone()).await.unwrap();let b=Bootstrap::capture(&reopened).unwrap();original.push(b.clone());lazy.push(LazyOwner::open(&provider(dir.path()),ids,w.distance_fn.clone(),w.config.clone(),b).await.unwrap());eager.push(reopened);}
        let aliases=vec![vec![0.,-0.],vec![9.,-0.]];let queries=vec![vec![4.,-0.],vec![4.5,0.],vec![-0.,0.],vec![5.,0.]];
        let mut upfront=Vec::new();for q in &queries{upfront.push(route(&mut lazy,q,&aliases).await.unwrap());}
        let mut ownership=CountedWriteOwnership::from_audited_counts(vec![0,0],128).unwrap();
        for (id,q) in queries.iter().enumerate(){let mut sessions=eager.iter_mut().map(|w|w.write_session("parent".into()).unwrap()).collect::<Vec<_>>();let expected=append_counted_global(&mut sessions,"parent",&mut ownership,id as u64,q,Some(&aliases)).unwrap();assert_eq!((expected.owner,expected.leaf,expected.distance.to_bits()),(upfront[id].owner,upfront[id].leaf,upfront[id].distance.to_bits()));drop(sessions);for (w,b) in eager.iter().zip(&original){assert_eq!(&Bootstrap::capture(w).unwrap(),b);}}
        assert!(lazy.iter().all(|o|o.metrics.cache_hits>0));assert_eq!(upfront[0].owner,1); // crossing: alias A is nearer, but a B leaf wins.
        let q=&queries[1];assert_eq!(DistanceFunction::Euclidean.distance(q,&aliases[0]).to_bits(),DistanceFunction::Euclidean.distance(q,&aliases[1]).to_bits());
    }
    #[test]
    fn preselected_registration_checks_new_id_and_preserves_the_topology() {
        let mut w=fixture(false);let before=Bootstrap::capture(&w).unwrap();let mut ownership=CountedWriteOwnership::from_audited_counts(vec![0],8).unwrap();
        {let mut session=w.write_session("parent".into()).unwrap();assert_eq!(super::super::distributed::append_preselected_counted(&mut session,"parent",&mut ownership,0,0,1,&[0.,-0.]).unwrap(),0);assert!(super::super::distributed::append_preselected_counted(&mut session,"parent",&mut ownership,0,0,1,&[0.,-0.]).is_err());assert!(super::super::distributed::append_preselected_counted(&mut session,"parent",&mut ownership,1,0,0,&[0.,-0.]).is_err());}
        assert_eq!(ownership.next_global_id(),1);assert_eq!(Bootstrap::capture(&w).unwrap(),before);
    }
    #[tokio::test]
    async fn lazy_admission_rejects_wrong_widths_policy_root_and_vector() {
        let dir=tempfile::tempdir().unwrap();let p=provider(dir.path());let w=fixture(false);let ids=w.commit(&p,None).await.unwrap().flush().await.unwrap();let b=Bootstrap::capture(&w).unwrap();
        for kind in 0..3 {let mut bad=b.clone();match kind{0=>bad.widths[0]=2,1=>bad.policy=serde_json::Value::Null,_=>bad.root=999};assert!(LazyOwner::open(&p,ids.clone(),w.distance_fn.clone(),w.config.clone(),bad).await.is_err());}
        let mut owners=vec![LazyOwner::open(&p,ids,w.distance_fn.clone(),w.config.clone(),b).await.unwrap()];assert!(route(&mut owners,&[f32::NAN,0.],&[vec![0.,0.]]).await.is_err());assert_eq!(owners[0].metrics.fetched_nodes,0);
    }
}
