//! Explicit FP paired construction. Actors and saved roots remain local.
use crate::hierarchical_index::{
    config::{HierarchicalSpannConfig, NavigationMode},
    reader::{AuditLimits, HierarchicalSpannReader},
    writer::{authoritative_owner::{AuthoritativeTree, ConstructionLimits}, HierarchicalSpannIds, HierarchicalSpannWriter},
};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{fs::{self, File, OpenOptions}, io::{Read, Write}, path::{Path, PathBuf}, time::Instant};
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Input { pub path: PathBuf, pub bytes: u64, pub sha256: String }
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub branching_factor: usize, pub split_threshold: usize, pub merge_threshold: usize,
    pub write_beam_tau: f64, pub write_beam_min: usize, pub write_beam_max: usize,
    pub write_rng_epsilon: f32, pub write_rng_factor: f32, pub reassign_neighbor_count: usize,
    pub write_navigation: String, pub fp_npa: bool,
}
impl Config {
    fn effective(&self) -> Result<HierarchicalSpannConfig> {
        ensure!(self.write_navigation == "fp" && self.fp_npa, "both arms require explicit FP navigation and FP NPA");
        ensure!(self.branching_factor >= 2 && self.split_threshold > 0 && self.merge_threshold <= self.split_threshold
            && self.write_beam_min > 0 && self.write_beam_max >= self.write_beam_min
            && self.write_beam_tau.is_finite() && self.write_beam_tau >= 0.
            && self.write_rng_epsilon.is_finite() && self.write_rng_factor.is_finite()
            && self.write_rng_factor > 0. && self.reassign_neighbor_count > 0, "invalid construction configuration");
        Ok(HierarchicalSpannConfig {branching_factor:self.branching_factor,split_threshold:self.split_threshold,merge_threshold:self.merge_threshold,
            write_beam_tau:self.write_beam_tau,write_beam_min:self.write_beam_min,write_beam_max:self.write_beam_max,
            write_level_taus:vec![],write_level_min_pcts:vec![],max_replicas:1,
            write_rng_epsilon:self.write_rng_epsilon,write_rng_factor:self.write_rng_factor,
            reassign_neighbor_count:self.reassign_neighbor_count,write_navigation:NavigationMode::Fp,fp_npa:true})
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedConfig { version:u32, protocol:String, config:Config }
pub fn read_config(value:&Value)->Result<HierarchicalSpannConfig> {
    let saved:SavedConfig=serde_json::from_value(value.clone())?;
    ensure!(saved.version==2 && saved.protocol=="explicit-fp-global-v1","unsupported saved construction configuration");
    saved.config.effective()
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub format_version:u32, pub treatment:String, pub source_id:String, pub vectors:Input,
    pub rows:usize, pub dimension:usize, pub metric:String, pub config:Config,
    pub storage_chunk:usize, pub balance_every:usize, pub owner_slots:usize,
    pub owner_vector_threshold:usize, pub max_nodes:usize, pub max_owner_physical_rows:usize,
    pub max_leaf_rows:usize, pub output_dir:PathBuf,
}
fn sha(bytes:&[u8])->String {format!("{:x}",Sha256::digest(bytes))}
fn ids_value(ids:&HierarchicalSpannIds)->Value {json!({"posting_list_id":ids.posting_list_id,"scalar_metadata_id":ids.scalar_metadata_id,"vector_data_id":ids.vector_data_id,"leaf_node_id":ids.leaf_node_id,"internal_node_id":ids.internal_node_id})}
fn config_value(c:&Config)->Value {let mut v=serde_json::to_value(c).unwrap();v["max_replicas"]=json!(1);v["write_level_taus"]=json!([]);v["write_level_min_pcts"]=json!([]);v}
fn write_new(path:&Path,value:&Value)->Result<()> {let mut f=OpenOptions::new().write(true).create_new(true).open(path)?;serde_json::to_writer_pretty(&mut f,value)?;f.write_all(b"\n")?;f.sync_all()?;Ok(())}
fn read_rows(f:&mut File, count:usize, dimension:usize, digest:&mut Sha256)->Result<Vec<Vec<f32>>> {
    let mut result=Vec::with_capacity(count);let mut raw=vec![0u8;dimension*4];
    for _ in 0..count {f.read_exact(&mut raw)?;digest.update(&raw);let v:Vec<_>=raw.chunks_exact(4).map(|b|f32::from_le_bytes(b.try_into().unwrap())).collect();ensure!(v.iter().all(|x|x.is_finite()),"nonfinite input vector");result.push(v);}
    Ok(result)
}
pub async fn run(path:&Path)->Result<()> {
    let mut bytes=vec![];File::open(path)?.take(1024*1024+1).read_to_end(&mut bytes)?;ensure!(bytes.len()<=1024*1024,"spec exceeds1MiB");
    let spec:Spec=serde_json::from_slice(&bytes)?;let config=spec.config.effective()?;
    ensure!(spec.format_version==1 && matches!(spec.treatment.as_str(),"reference"|"authoritative"),"unsupported construction protocol/treatment");
    ensure!(spec.rows>0 && spec.rows<=u32::MAX as usize && spec.dimension>0 && spec.dimension<=4096
        && spec.storage_chunk>0 && spec.storage_chunk<=8192 && spec.balance_every>0
        && spec.owner_slots>0 && spec.owner_slots<=128 && spec.owner_vector_threshold>0
        && spec.max_nodes>spec.config.branching_factor && spec.max_nodes<=1_000_000
        && spec.max_leaf_rows>=spec.config.split_threshold && spec.max_leaf_rows<=262144
        && spec.max_owner_physical_rows>=spec.max_leaf_rows && !spec.source_id.is_empty(),"construction limits differ");
    let expected=spec.rows.checked_mul(spec.dimension).and_then(|x|x.checked_mul(4)).context("input length overflow")?;
    ensure!(spec.vectors.bytes==expected as u64,"input envelope length differs");
    let mut input=File::open(&spec.vectors.path)?;ensure!(input.metadata()?.len()==spec.vectors.bytes,"input file length differs");
    let mut hash=Sha256::new();let mut buf=[0u8;65536];loop {let n=input.read(&mut buf)?;if n==0 {break}hash.update(&buf[..n]);}
    ensure!(format!("{:x}",hash.finalize())==spec.vectors.sha256,"input digest differs");
    let metric=crate::metric(&spec.metric)?;ensure!(spec.metric!="cosine","cosine normalization is not part of this protocol; use l2 or ip");
    fs::create_dir(&spec.output_dir).context("output directory must be new")?;
    let store=spec.output_dir.join("store");fs::create_dir(&store)?;
    let provider=crate::provider(&store).await?;
    let started=Instant::now();let mut input=File::open(&spec.vectors.path)?;let mut consumed=Sha256::new();
    let first=read_rows(&mut input,1,spec.dimension,&mut consumed)?.remove(0);
    let mut reference=HierarchicalSpannWriter::new(spec.dimension,metric.clone(),config.clone());reference.add(0,&first);
    let mut dynamic=None;
    let mut genesis=None;
    if spec.treatment=="authoritative" {
        let ids=reference.commit(&provider,None).await.map_err(|e|anyhow::anyhow!("genesis commit: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("genesis flush: {e}"))?;
        genesis=Some(ids_value(&ids));
        dynamic=Some(AuthoritativeTree::open_dynamic(provider.clone(),ids,metric.clone(),config.clone(),1,spec.owner_slots,
            ConstructionLimits{max_vectors:spec.rows,max_nodes:spec.max_nodes,max_owners:spec.owner_slots,max_leaf_rows:spec.max_leaf_rows,
                max_owner_physical_rows:spec.max_owner_physical_rows,max_batch_rows:spec.storage_chunk,max_dimension:spec.dimension,max_events:100000,trace:false,move_above:Some(spec.owner_vector_threshold.min(spec.rows))}).await?);
    }
    let mut at=1usize;let mut cuts=vec![];let mut reference_round_limit_hits=0;
    // Both arms include the one-row genesis in the same logical balance schedule.
    if spec.balance_every==1 && spec.rows>1 {if let Some(t)=dynamic.as_mut(){t.balance_dynamic_current()?;}else{reference.balance_index();}cuts.push(json!({"rows":1}));}
    while at<spec.rows {
        let next_cut=((at/spec.balance_every)+1).saturating_mul(spec.balance_every).min(spec.rows);
        let end=(at+spec.storage_chunk).min(next_cut);
        let vectors=read_rows(&mut input,end-at,spec.dimension,&mut consumed)?;
        if let Some(tree)=dynamic.as_mut() {let rows:Vec<_>=vectors.into_iter().enumerate().map(|(i,v)|((at+i)as u32,v)).collect();tree.register_dynamic_batch(&rows).await?;}
        else {for (i,v) in vectors.iter().enumerate() {reference.add((at+i)as u32,v);}}
        at=end;
        if at==next_cut {
            let before=Instant::now();
            if let Some(tree)=dynamic.as_mut() {tree.balance_dynamic_current()?;cuts.push(json!({"rows":at,"elapsed_seconds":before.elapsed().as_secs_f64()}));}
            else {reference.balance_index();let pending=reference.pending_balance_leaves();reference_round_limit_hits+=usize::from(pending>0);cuts.push(json!({"rows":at,"pending_leaves":pending,"round_limit_hit":pending>0,"elapsed_seconds":before.elapsed().as_secs_f64()}));}
        }
    }
    if spec.rows==1 {if let Some(t)=dynamic.as_mut(){t.balance_dynamic_current()?;}else{reference.balance_index();}cuts.push(json!({"rows":1}));}
    ensure!(format!("{:x}",consumed.finalize())==spec.vectors.sha256,"consumed input differs");let mut extra=[0u8;1];ensure!(input.read(&mut extra)?==0,"input grew while consumed");
    let construction_seconds=started.elapsed().as_secs_f64();let save_started=Instant::now();
    let (ids,accounting,branches)=if let Some(tree)=dynamic.as_mut() {let ids=tree.save().await?;(ids,tree.construction_accounting(),json!(tree.branch_owners()))}
        else {(reference.commit(&provider,None).await.map_err(|e|anyhow::anyhow!("commit: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("flush: {e}"))?,json!({"round_limit_hits":reference_round_limit_hits}),json!({}))};
    let save_seconds=save_started.elapsed().as_secs_f64();drop(dynamic);drop(reference);
    let audit_started=Instant::now();let reader=HierarchicalSpannReader::open(&provider,ids.clone(),metric,config).await.map_err(|e|anyhow::anyhow!("saved reader: {e}"))?;
    let audit=reader.audit_reachable_ids(spec.rows as u64,AuditLimits{max_bitmap_bytes:(spec.rows as u64+7)/8,max_nodes:spec.max_nodes,max_leaf_entries:spec.max_leaf_rows}).await.map_err(|e|anyhow::anyhow!("saved census: {e}"))?;
    audit.require_single_current_copy().map_err(|e|anyhow::anyhow!("single current copy: {e}"))?;
    let root=reader.describe_root().map_err(|e|anyhow::anyhow!("root: {e}"))?;
    let mut map=Sha256::new();for id in 0..spec.rows as u64 {map.update(id.to_le_bytes());}
    let saved_config=json!({"version":2,"protocol":"explicit-fp-global-v1","config":spec.config});
    let checkpoint=json!({"generation":1,"source_id":spec.source_id,"shard_id":"global","metric":spec.metric,"dimension":spec.dimension,"vector_count":spec.rows,"global_id_start":0,"global_id_end":spec.rows,"id_offset":0,"id_stride":1,"id_mapping":null,"routing_boundary_hash":"","root_ref":{"kind":root.kind,"id":root.id},"integrity_audit":audit,"construction":saved_config,"block_ids":ids_value(&ids)});
    write_new(&store.join("manifest.json"),&checkpoint)?;
    let result=json!({"format_version":1,"treatment":spec.treatment,"source_id":spec.source_id,"consumed_spec_sha256":sha(&bytes),"input":spec.vectors,
        "rows":spec.rows,"dimension":spec.dimension,"metric":spec.metric,"config":config_value(&spec.config),"spec":spec,
        "id_mapping":{"format":"dense-u64-le-identity","rows":spec.rows,"local_start":0,"global_start":0,"sha256":format!("{:x}",map.finalize())},
        "block_ids":ids_value(&ids),"genesis":genesis,"root":root,"branch_owners":branches,"integrity_audit":audit,"balance_cuts":cuts,"accounting":accounting,
        "timings":{"construction_seconds":construction_seconds,"save_seconds":save_seconds,"audit_seconds":audit_started.elapsed().as_secs_f64()},
        "execution":"local actors/shared global roots; independent stochastic split decisions; no tape; no cross-host latency claim"});
    write_new(&spec.output_dir.join("manifest.json"),&result)?;println!("{}",serde_json::to_string(&result)?);Ok(())
}
