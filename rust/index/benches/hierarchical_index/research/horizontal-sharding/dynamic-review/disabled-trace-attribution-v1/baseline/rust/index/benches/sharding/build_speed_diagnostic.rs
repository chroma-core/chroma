//! Tiny construction attribution. This does not change the normal build path.
use crate::{dynamic_construct::{Config, Input}, hierarchical_index::{
    reader::{AuditLimits, HierarchicalSpannReader},
    writer::{HierarchicalSpannWriter, authoritative_owner::{AuthoritativeTree, ConstructionLimits}},
}};
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest,Sha256};
use std::{fs::{self,File,OpenOptions},io::{Read,Write},path::{Path,PathBuf},sync::{Arc,Mutex},time::Instant};

#[derive(Deserialize,Serialize)]
#[serde(deny_unknown_fields)]
pub struct Spec {
    pub vectors:Input, pub prefix_sha256:String, pub rows:usize, pub dimension:usize,
    pub metric:String, pub config:Config, pub cell:String, pub threads:usize,
    pub balance_every:usize, pub output:PathBuf,
}
fn config(spec:&Spec)->Result<crate::hierarchical_index::config::HierarchicalSpannConfig> {
    crate::dynamic_construct::read_config(&json!({"version":2,"protocol":"explicit-fp-global-v1","config":spec.config}))
}
fn write(path:&Path,value:&serde_json::Value)->Result<()> {
    let mut f=OpenOptions::new().write(true).create_new(true).open(path)?;
    serde_json::to_writer_pretty(&mut f,value)?;f.write_all(b"\n")?;Ok(())
}
pub async fn run(path:&Path)->Result<()> {
    let mut raw=Vec::new();File::open(path)?.take(65537).read_to_end(&mut raw)?;ensure!(raw.len()<=65536,"spec cap");let s:Spec=serde_json::from_slice(&raw)?;
    ensure!(s.rows>=2 && s.rows<=8192 && s.dimension>0 && s.dimension<=768 && s.balance_every>0
        && s.balance_every<=s.rows && s.threads>0 && s.threads<=8,"diagnostic bounds");
    ensure!(["serial-serial","parallel-serial","serial-parallel","parallel-parallel","authoritative"].contains(&s.cell.as_str()),"unknown cell");
    ensure!(s.metric=="ip" || s.metric=="l2","metric");
    let c=config(&s)?;let metric=crate::metric(&s.metric)?;
    let input_started=Instant::now();let n=s.rows.checked_mul(s.dimension).and_then(|n|n.checked_mul(4)).context("prefix extent")?;
    let mut input=File::open(&s.vectors.path)?;ensure!(input.metadata()?.len()==s.vectors.bytes && s.vectors.bytes>=n as u64,"source extent");
    let mut bytes=vec![0;n];input.read_exact(&mut bytes)?;
    ensure!(format!("{:x}",Sha256::digest(&bytes))==s.prefix_sha256,"prefix hash");
    let vectors:Vec<Vec<f32>>=bytes.chunks_exact(s.dimension*4).map(|r|r.chunks_exact(4).map(|b|f32::from_le_bytes(b.try_into().unwrap())).collect()).collect();
    ensure!(vectors.iter().flatten().all(|v|v.is_finite()),"nonfinite vector");drop(bytes);
    let input_seconds=input_started.elapsed().as_secs_f64();fs::create_dir(&s.output)?;
    let storage=Arc::new(Mutex::new(chroma_storage::local::LocalReadObservation::default()));
    let provider=crate::provider_cache::observed_provider(&s.output.join("store"),storage.clone()).await?;
    let reference=HierarchicalSpannWriter::new(s.dimension,metric.clone(),c.clone());
    let setup=Instant::now();let mut dynamic=None;
    if s.cell=="authoritative" {
        reference.add(0,&vectors[0]);
        let ids=reference.commit(&provider,None).await.map_err(|e|anyhow::anyhow!("genesis: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("genesis flush: {e}"))?;
        let tree=AuthoritativeTree::open_dynamic(provider.clone(),ids,metric.clone(),c.clone(),1,1,
            ConstructionLimits{max_vectors:s.rows,max_nodes:10000,max_owners:1,max_leaf_rows:262144,max_owner_physical_rows:262144,max_batch_rows:8192,max_dimension:s.dimension,max_events:100000,trace:false,move_above:None}).await?;
        tree.enable_diagnostic_reads()?;dynamic=Some(tree);
    }
    let setup_seconds=setup.elapsed().as_secs_f64();let setup_storage=storage.lock().unwrap().clone();
    let mut cuts=vec![];let mut start=if dynamic.is_some(){1}else{0};
    while start<s.rows {
        let end=(((start/s.balance_every)+1)*s.balance_every).min(s.rows);let add=Instant::now();
        if let Some(t)=dynamic.as_mut() {
            let rows:Vec<_>=(start..end).map(|i|(i as u32,vectors[i].clone())).collect();
            t.register_dynamic_batch(&rows).await?;
        } else if s.cell.starts_with("parallel") {
            let chunk=(end-start+s.threads-1)/s.threads;
            std::thread::scope(|scope|{for (i,rows) in vectors[start..end].chunks(chunk).enumerate(){let writer=&reference;scope.spawn(move||{for (j,v) in rows.iter().enumerate(){writer.add((start+i*chunk+j)as u32,v);}});}});
        } else {for i in start..end {reference.add(i as u32,&vectors[i]);}}
        let add_seconds=add.elapsed().as_secs_f64();let add_storage=storage.lock().unwrap().clone();
        let add_reads=dynamic.as_ref().map(|t|t.diagnostic_read_snapshot()).transpose()?;
        let balance=Instant::now();
        if let Some(t)=dynamic.as_mut(){t.balance_dynamic_current()?;}
        else if s.cell.ends_with("parallel"){reference.balance_index_parallel(s.threads);}
        else{reference.balance_index();}
        let balance_seconds=balance.elapsed().as_secs_f64();
        cuts.push(json!({"rows":end,"add_seconds":add_seconds,"balance_seconds":balance_seconds,
            "storage_after_add":add_storage,"storage_after_balance":storage.lock().unwrap().clone(),
            "vector_reads_after_add":add_reads,"vector_reads_after_balance":dynamic.as_ref().map(|t|t.diagnostic_read_snapshot()).transpose()?,
            "owner_counters":dynamic.as_ref().map(|t|t.counters()).transpose()?}));
        start=end;
    }
    let stats=reference.stats.snapshot();let before_save=storage.lock().unwrap().clone();
    let save=Instant::now();let ids=if let Some(t)=dynamic.as_mut(){t.save().await?}else{reference.commit(&provider,None).await.map_err(|e|anyhow::anyhow!("save: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("flush: {e}"))?};
    let save_seconds=save.elapsed().as_secs_f64();drop(dynamic);drop(reference);
    let audit_start=Instant::now();let reader=HierarchicalSpannReader::open(&provider,ids,metric,c).await.map_err(|e|anyhow::anyhow!("reader: {e}"))?;
    let census=reader.audit_reachable_ids(s.rows as u64,AuditLimits{max_bitmap_bytes:(s.rows as u64+7)/8,max_nodes:10000,max_leaf_entries:262144}).await.map_err(|e|anyhow::anyhow!("census: {e}"))?;
    census.require_single_current_copy().map_err(|e|anyhow::anyhow!("current copies: {e}"))?;
    let result=json!({"status":"passed","spec":s,"spec_sha256":format!("{:x}",Sha256::digest(&raw)),"input_seconds":input_seconds,"setup_seconds":setup_seconds,"setup_storage":setup_storage,"cuts":cuts,
        "reference_stats":{"calls":stats.calls,"nanos":stats.nanos,"add_substeps":stats.add_substeps,"split_substeps":stats.split_substeps},
        "storage_before_save":before_save,"save_seconds":save_seconds,"audit_seconds":audit_start.elapsed().as_secs_f64(),"census":census,
        "scope":"Local actors, independent stochastic trees; cumulative counters. Encoded successfulLocalStorage returns include roots; vector reader counters exclude root open/cache reconstruction. Summed spans overlap, not CPU fractions. Observer overhead included; no physicalIO or coldOS claim."});
    write(&s.output.join("result.json"),&result)?;println!("{}",serde_json::to_string(&result)?);Ok(())
}
