//! One bounded construction arm; independent schedules are not tree-equality tests.
use crate::{dynamic_construct::{Config,Input},hierarchical_index::{writer::{HierarchicalSpannWriter,reference_owner_adapter::ConcurrentOwnerWriter},reader::{HierarchicalSpannReader,AuditLimits}}};
use anyhow::{ensure,Result,Context};
use serde::{Deserialize,Serialize};
use serde_json::json;
use sha2::{Digest,Sha256};
use std::{fs::{self,File,OpenOptions},io::{Read,Write},path::{Path,PathBuf},time::Instant};
#[derive(Deserialize,Serialize)]
#[serde(deny_unknown_fields)]
struct Spec {protocol:String,source_id:String,vectors:Input,prefix_sha256:String,rows:usize,dimension:usize,metric:String,config:Config,treatment:String,threads:usize,balance_every:usize,owner_slots:usize,move_above:usize,output:PathBuf}
pub async fn run(path:&Path)->Result<()> {
    let mut raw=vec![];File::open(path)?.take(65537).read_to_end(&mut raw)?;ensure!(raw.len()<=65536,"spec cap");let s:Spec=serde_json::from_slice(&raw)?;
    ensure!(s.protocol=="reference-owner-parallel-v1"&&!s.source_id.is_empty()&&["reference","owner"].contains(&s.treatment.as_str()),"diagnostic protocol/treatment");
    ensure!(s.rows>0&&s.rows<=8192&&s.dimension>0&&s.dimension<=768&&s.threads>0&&s.threads<=8&&s.balance_every>0&&s.balance_every<=s.rows&&s.owner_slots>0&&s.owner_slots<=8&&s.move_above>0,"diagnostic bounds");
    let config=crate::dynamic_construct::read_config(&json!({"version":2,"protocol":"explicit-fp-global-v1","config":s.config}))?;let metric=crate::metric(&s.metric)?;
    let input_time=Instant::now();let count=s.rows.checked_mul(s.dimension).and_then(|v|v.checked_mul(4)).context("prefix extent")?;
    let mut file=File::open(&s.vectors.path)?;ensure!(file.metadata()?.len()==s.vectors.bytes&&s.vectors.bytes>=count as u64,"source extent");let mut bytes=vec![0;count];file.read_exact(&mut bytes)?;ensure!(format!("{:x}",Sha256::digest(&bytes))==s.prefix_sha256,"consumed prefix SHA");
    let vectors:Vec<Vec<f32>>=bytes.chunks_exact(s.dimension*4).map(|row|row.chunks_exact(4).map(|v|f32::from_le_bytes(v.try_into().unwrap())).collect()).collect();ensure!(vectors.iter().flatten().all(|v|v.is_finite()),"nonfinite input");drop(bytes);let input_seconds=input_time.elapsed().as_secs_f64();
    fs::create_dir(&s.output)?;let provider=crate::provider_cache::provider(&s.output.join("store"),crate::provider_cache::ProviderCachePolicy::default()).await?;
    let reference=HierarchicalSpannWriter::new(s.dimension,metric.clone(),config.clone());let mut owner=if s.treatment=="owner"{Some(ConcurrentOwnerWriter::new(s.dimension,metric.clone(),config.clone(),s.owner_slots,8192)?)}else{None};
    let mut phases=vec![];
    for start in (0..s.rows).step_by(s.balance_every) {let end=(start+s.balance_every).min(s.rows);let rows:Vec<_>=(start..end).map(|id|(id as u32,vectors[id].clone())).collect();let add=Instant::now();
        if let Some(owner)=owner.as_mut(){owner.add_phase(&rows,s.threads)?;}else {let chunk=rows.len().div_ceil(s.threads);std::thread::scope(|scope|{for rows in rows.chunks(chunk){let writer=&reference;scope.spawn(move||for (id,v) in rows{writer.add(*id,v);});}});}
        let add_seconds=add.elapsed().as_secs_f64();let balance=Instant::now();if let Some(owner)=owner.as_mut(){owner.balance_phase(s.threads)?;}else{reference.balance_index_parallel(s.threads);}let balance_seconds=balance.elapsed().as_secs_f64();
        let reshard=Instant::now();let moves=if let Some(owner)=owner.as_mut(){owner.reshard(s.move_above)?}else{0};let reshard_seconds=reshard.elapsed().as_secs_f64();phases.push(json!({"rows":end,"add_seconds":add_seconds,"balance_seconds":balance_seconds,"reshard_seconds":reshard_seconds,"placement_moves":moves}));
    }
    let save=Instant::now();let ids=if let Some(owner)=&owner {owner.save(&provider).await?}else{reference.commit(&provider,None).await.map_err(|e|anyhow::anyhow!("reference commit: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("reference flush: {e}"))?};let save_seconds=save.elapsed().as_secs_f64();let report=owner.as_ref().map(|o|o.report());drop(owner);drop(reference);
    let audit=Instant::now();let reader=HierarchicalSpannReader::open(&provider,ids.clone(),metric,config).await.map_err(|e|anyhow::anyhow!("reader: {e}"))?;let census=reader.audit_reachable_ids(s.rows as u64,AuditLimits{max_bitmap_bytes:(s.rows as u64).div_ceil(8),max_nodes:10000,max_leaf_entries:1048576}).await.map_err(|e|anyhow::anyhow!("census: {e}"))?;census.require_single_current_copy().map_err(|e|anyhow::anyhow!("current membership: {e}"))?;
    for (id,vector) in vectors.iter().enumerate(){ensure!(reader.verify_vector_bits(id as u32,vector).await.map_err(|e|anyhow::anyhow!("vector proof: {e}"))?,"saved vector bits mismatch");}
    let mut exact=reader.rerank_selected(&vectors[0],&(0..s.rows as u32).collect::<Vec<_>>()).await.map_err(|e|anyhow::anyhow!("exact rerank: {e}"))?;exact.sort_by_key(|v|v.0);let exact_bits:Vec<_>=exact.iter().map(|(id,d)|(*id,d.to_bits())).collect();let audit_seconds=audit.elapsed().as_secs_f64();
    let result=json!({"status":"passed","spec":s,"spec_sha256":format!("{:x}",Sha256::digest(&raw)),"input_seconds":input_seconds,"phases":phases,"owner_report":report,"save_seconds":save_seconds,"audit_seconds":audit_seconds,"roots":{"posting_list_id":ids.posting_list_id,"scalar_metadata_id":ids.scalar_metadata_id,"vector_data_id":ids.vector_data_id,"leaf_node_id":ids.leaf_node_id,"internal_node_id":ids.internal_node_id},"census":census,"all_id_exact_fp_distance_bits":exact_bits,"scope":"Explicit FP both arms; actual reference parallel global routing and subtree scheduler. Local owner actors/shared immutable embedding cache; no durable remote-vector execution. Placement only after joined balance. Full joined snapshot/save/audit charged separately. Independent stochastic trees; all-ID exact rerank parity is not ANN quality/speedup. Full source SHA supplied externally; consumed prefix SHA verified here. No2M readiness."});
    let mut file=OpenOptions::new().write(true).create_new(true).open(s.output.join("result.json"))?;serde_json::to_writer_pretty(&mut file,&result)?;file.write_all(b"\n")?;println!("{}",serde_json::to_string(&result)?);Ok(())
}
