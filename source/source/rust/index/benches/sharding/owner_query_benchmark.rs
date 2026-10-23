//! Saved-query comparison. Each worker owns its reader; shared provider caches
//! and the operating-system cache remain outside application-cold semantics.
use anyhow::{ensure,Result};
use serde::Deserialize;
use serde_json::json;
use std::{path::{Path,PathBuf},time::Instant,fs};
use crate::hierarchical_index::{writer::reference_owner_adapter::wide_identity::{Reader64,Snapshot64},writer::HierarchicalSpannIds,common::ReadBeamPolicy,saved_topology};
#[derive(Deserialize)]struct Spec{snapshot:PathBuf,store:PathBuf,queries:PathBuf,output:PathBuf}
#[derive(Deserialize)]struct Query{id:u64,vector:Vec<f32>,neighbors:Vec<u64>}
pub async fn run(path:&Path)->Result<()>{
 let s:Spec=serde_json::from_slice(&fs::read(path)?)?;ensure!(!s.output.exists(),"new query output required");
 let d:serde_json::Value=serde_json::from_slice(&fs::read(&s.snapshot)?)?;
 let roots:HierarchicalSpannIds=HierarchicalSpannIds{posting_list_id:d["roots"]["posting_list_id"].as_str().unwrap().parse()?,scalar_metadata_id:d["roots"]["scalar_metadata_id"].as_str().unwrap().parse()?,vector_data_id:d["roots"]["vector_data_id"].as_str().unwrap().parse()?,leaf_node_id:d["roots"]["leaf_node_id"].as_str().unwrap().parse()?,internal_node_id:d["roots"]["internal_node_id"].as_str().unwrap().parse()?};
 let config=crate::dynamic_construct::read_config(&json!({"version":2,"protocol":"explicit-fp-global-v1","config":d["config"]}))?;
 let snap=Snapshot64{format:d["format"].as_str().unwrap().into(),generation:d["generation"].as_u64().unwrap(),dim:d["dimension"].as_u64().unwrap() as usize,metric:crate::metric(d["metric"].as_str().unwrap())?,config,max_leaf_rows:d["max_leaf_rows"].as_u64().unwrap() as usize,roots};
 let queries:Vec<Query>=serde_json::from_slice(&fs::read(&s.queries)?)?;ensure!(queries.len()==100 && queries.iter().all(|q|q.vector.len()==snap.dim&&q.neighbors.len()==100),"100 fixed top100 queries");
 let provider=crate::provider_cache::provider(&s.store,crate::provider_cache::ProviderCachePolicy::default()).await?;
 let limits=saved_topology::Limits{entries:8,cache_payload_bytes:65536,record_payload_bytes:8192,wave_rows:8,children:100};
 let shape=Reader64::open_with_limits(&provider,snap.clone(),4096,limits).await?.level_counts()?;
 let widths=shape.iter().skip(1).copied().collect::<Vec<_>>();let rt=tokio::runtime::Handle::current();let mut rows=Vec::new();
 for tau in [0.7,1.,2.] {for factor in [1usize,8] {
  let policy=ReadBeamPolicy::with_level_overrides(Some(tau),10,256,vec![None,None,None],vec![50.,5.,0.],widths.clone());
  let mut readers=Vec::new();for _ in 0..32{readers.push(Reader64::open_with_limits(&provider,snap.clone(),4096,limits).await?);}
  for pass in ["fresh-reader","reused-reader"] {let start=Instant::now();
   let reports=std::thread::scope(|scope|->Result<Vec<serde_json::Value>>{let mut jobs=Vec::new();for(worker,reader)in readers.iter_mut().enumerate(){let q=&queries;let rt=&rt;let policy=&policy;jobs.push(scope.spawn(move||->Result<Vec<serde_json::Value>>{let mut result=Vec::new();for i in (worker..q.len()).step_by(32){let query=&q[i];let now=Instant::now();let r=rt.block_on(async{if factor==1{reader.query_quantized(&query.vector,100,policy).await}else{reader.query(&query.vector,100,800,policy).await}})?;let elapsed=now.elapsed().as_secs_f64();let hits=r.results.iter().filter(|(id,_)|query.neighbors.contains(id)).count();result.push(json!({"id":query.id,"seconds":elapsed,"recall100":hits as f64/100.,"scanned_vectors":r.live_rows,"physical_rows":r.physical_rows,"candidate_count":r.candidates,"reranks":r.exact_reranks,"leaves":r.selected_leaves.len(),"expanded_internals":r.navigation_work.expanded_internals,"centroid_distances":r.navigation_work.distance_evaluations}));}Ok(result)}));}let mut all=Vec::new();for job in jobs{all.extend(job.join().map_err(|_|anyhow::anyhow!("query worker panic"))??);}Ok(all)})?;
   let row=json!({"tau":tau,"rerank_factor":factor,"pass":pass,"wall_seconds":start.elapsed().as_secs_f64(),"queries":reports});println!("{}",serde_json::to_string(&row)?);rows.push(row);
  }
 }}
 fs::write(&s.output,serde_json::to_vec_pretty(&json!({"level_counts":shape,"query_workers":32,"rows":rows,"scope":"32 independent saved readers share provider cache. FP navigation, one-bit postings. Fresh reader does not clear provider or OS cache; reused reader retains its small topology cache, while posting/scalar pins clear per leaf and vector pins per query. These latency/cache semantics differ from the ordinary reference."}))?)?;Ok(())
}
