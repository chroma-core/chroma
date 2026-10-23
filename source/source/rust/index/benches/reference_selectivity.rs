//! Read-only reference-manifest bridge and bounded exact-neighbor attribution.
#![recursion_limit="256"]
#![allow(dead_code)]
#[path="hierarchical_index/mod.rs"] mod hierarchical_index;
#[path="sharding/provider_cache.rs"] mod provider_cache;
use anyhow::{ensure,Context,Result};
use hierarchical_index::{reader::HierarchicalSpannReader,writer::HierarchicalSpannIds,config::HierarchicalSpannConfig,common::ReadBeamPolicy};
use chroma_distance::DistanceFunction;
use serde_json::{Value,json};
use sha2::{Sha256,Digest};
use std::{path::Path,io::{BufRead,Write},collections::BTreeSet};
fn checked(pin:&Value)->Result<Vec<u8>> {
 let p=Path::new(pin["path"].as_str().context("pin path")?);ensure!(!p.is_symlink() && p.is_file(),"unsafe pin");
 let data=std::fs::read(p)?;ensure!(data.len() as u64==pin["bytes"].as_u64().context("pin bytes")? && format!("{:x}",Sha256::digest(&data))==pin["sha256"].as_str().context("pin sha")?,"pin differs");Ok(data)
}
#[tokio::main]
async fn main()->Result<()> {
 let a:Vec<_>=std::env::args().collect();ensure!(a.len()==3 && a[1]=="--bridge","usage --bridge FILE");
 let b:Value=serde_json::from_slice(&std::fs::read(&a[2])?)?;
 ensure!(b["protocol"]=="global-reference-readonly-bridge-v1" && b["rows"]==2000000 && b["dimension"]==768 && b["metric"]=="ip","bridge scope");
 let m:Value=serde_json::from_slice(&checked(&b["manifest"])?)?;
 ensure!(m["rows"]==b["rows"] && m["block_ids"]==b["block_ids"] && m["config"]==b["config"] && m["root"]==b["expected_root"] && m["integrity_audit"]==b["saved_audit"],"bridge manifest mismatch");
 ensure!(m["dimension"]==768 && m["metric"]=="ip" && m["treatment"]=="reference","saved identity");
 ensure!(b["mapping"]["sha256"]==m["id_mapping"]["sha256"] && b["mapping"]["bytes"]==m["id_mapping"]["bytes"],"mapping manifest binding");
 let map=checked(&b["mapping"])?;ensure!(map.len()==16000000,"map bytes");for (i,x) in map.chunks_exact(8).enumerate(){ensure!(u64::from_le_bytes(x.try_into().unwrap())==i as u64,"map identity");}
 let store=Path::new(b["store"].as_str().context("store")?);let parent=Path::new(b["manifest"]["path"].as_str().unwrap()).parent().unwrap();ensure!(store==parent.join("store") && !store.is_symlink() && store.join("data").is_dir(),"store binding");
 let c=&b["config"];
 let config=HierarchicalSpannConfig{branching_factor:c["branching_factor"].as_u64().context("branch")? as usize,split_threshold:c["split_threshold"].as_u64().context("split")? as usize,merge_threshold:c["merge_threshold"].as_u64().context("merge")? as usize,write_beam_tau:c["write_beam_tau"].as_f64().context("tau")?,write_beam_min:c["write_beam_min"].as_u64().context("beammin")? as usize,write_beam_max:c["write_beam_max"].as_u64().context("beammax")? as usize,write_level_taus:serde_json::from_value(c["write_level_taus"].clone())?,write_level_min_pcts:serde_json::from_value(c["write_level_min_pcts"].clone())?,max_replicas:c["max_replicas"].as_u64().context("replicas")? as usize,write_rng_epsilon:c["write_rng_epsilon"].as_f64().context("epsilon")? as f32,write_rng_factor:c["write_rng_factor"].as_f64().context("factor")? as f32,reassign_neighbor_count:c["reassign_neighbor_count"].as_u64().context("neighbors")? as usize,write_navigation:hierarchical_index::config::NavigationMode::Fp,fp_npa:c["fp_npa"].as_bool().context("npa")?};
 let expected=json!({"branching_factor":100,"split_threshold":2048,"merge_threshold":512,"write_beam_tau":1.5,"write_beam_min":10,"write_beam_max":16,"write_level_taus":[],"write_level_min_pcts":[],"max_replicas":1,"write_rng_epsilon":0.0,"write_rng_factor":1.0,"reassign_neighbor_count":32,"write_navigation":"fp","fp_npa":true});ensure!(b["config"]==expected,"effective config differs");
 let ids=&b["block_ids"];let parse=|k:&str|->Result<uuid::Uuid>{Ok(ids[k].as_str().context("root")?.parse()?)};
 let roots=HierarchicalSpannIds{internal_node_id:parse("internal_node_id")?,leaf_node_id:parse("leaf_node_id")?,posting_list_id:parse("posting_list_id")?,scalar_metadata_id:parse("scalar_metadata_id")?,vector_data_id:parse("vector_data_id")?};
 let provider=provider_cache::provider(store,provider_cache::ProviderCachePolicy::default()).await?;
 let mut reader=HierarchicalSpannReader::open(&provider,roots,DistanceFunction::InnerProduct,config).await.map_err(|e|anyhow::anyhow!("open {e}"))?;
 let root=reader.describe_root().map_err(|e|anyhow::anyhow!("root {e}"))?;ensure!(root.id==b["expected_root"]["id"].as_u64().unwrap() as u32,"actual root differs");
 let memory=reader.memory_usage();let nodes=memory.leaf_count+memory.internal_count;ensure!(nodes==1780 && memory.leaf_count==1752 && memory.dim==768,"actual topology differs");
 let depth=reader.depth();ensure!(depth>0 && depth<=16,"depth cap");
 let fp=reader.prepare_fp_navigation_with_limits(4096,16*1024*1024).await.map_err(|e|anyhow::anyhow!("FP admission {e}"))?;
 let mut out=std::io::stdout().lock();writeln!(out,"{}",json!({"kind":"startup","depth":depth,"nodes":nodes,"leaves":memory.leaf_count,"internal_nodes":memory.internal_count,"fp":fp.report(),"budgets":{"rows":2000000,"fp_nodes":4096,"fp_payload_bytes":16777216,"gt_ids":100,"beam":32,"k":100,"rerank_factor":4,"candidate_budget":400,"dimension":768,"metric":"ip","tau":2.0,"navigation":"fp","construction_config":b["config"]}}))?;out.flush()?;
 let mut count=0;
 for line in std::io::stdin().lock().lines(){let line=line?;ensure!(line.len()<=65536 && count<32,"request cap");let q:Value=serde_json::from_str(&line)?;
  let vector:Vec<f32>=serde_json::from_value(q["query"].clone())?;ensure!(vector.len()==768 && vector.iter().all(|x|x.is_finite()),"query dimensions/finite");
  let gt:Vec<u32>=serde_json::from_value(q["ground_truth"].clone())?;ensure!(gt.len()==100 && gt.iter().all(|i|*i<2000000) && gt.iter().copied().collect::<BTreeSet<_>>().len()==100,"GT range/unique");
  let ((hits,scanned,leaves,t),report)=reader.search_with_policy_lazy_fp_gt(&vector,100,4,&ReadBeamPolicy::uniform(Some(2.0),32,32),&fp,&gt).await.map_err(|e|anyhow::anyhow!("query {e}"))?;
  ensure!(hits.len()==100 && hits.iter().all(|(i,d)|*i<2000000 && d.is_finite()),"hit integrity");
  writeln!(out,"{}",json!({"query_id":q["query_id"],"snapshot_sha256":b["manifest"]["sha256"],"gt":report,"ids":hits.iter().map(|x|x.0).collect::<Vec<_>>(),"distance_bits":hits.iter().map(|x|x.1.to_bits()).collect::<Vec<_>>(),"work":t.work,"posting_rows":t.work.posting_rows_scored_before_dedup,"distinct_candidates":scanned,"selected_leaves":leaves,"timing_ns":{"navigation":t.navigate_nanos,"quantize":t.quantize_nanos,"distance":t.distance_nanos,"selection":t.sort_dedup_nanos,"rerank":t.rerank_nanos}}))?;out.flush()?;count+=1;
 }
 ensure!(count==32,"exact warmup+retained request count");Ok(())
}
