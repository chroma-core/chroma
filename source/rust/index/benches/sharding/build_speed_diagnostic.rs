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
    #[serde(default)] pub lazy_disabled_evaluation_trace:bool,
    #[serde(default)] pub profile_before_scrub:bool,
    #[serde(default)] pub lazy_disabled_before_scrub_trace:bool,
    #[serde(default)] pub current_embedding_view:bool,
    #[serde(default)] pub storage_chunk_rows:usize,
    #[serde(default)] pub vector_probe_rounds:usize,
    #[serde(default,skip_serializing_if="zero_workers")] pub phased_owner_workers:usize,
    #[serde(default,skip_serializing_if="zero_workers")] pub split_preparation_workers:usize,
    #[serde(default,skip_serializing_if="false_value")] pub balance_cost_observation:bool,
}
fn false_value(value:&bool)->bool {!*value}
fn zero_workers(value:&usize)->bool {*value==0}
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
    ensure!(s.storage_chunk_rows<=8192 && s.vector_probe_rounds<=2 && (s.vector_probe_rounds==0 || s.balance_every==s.rows),"phase diagnostic cap");
    ensure!((!s.current_embedding_view && s.vector_probe_rounds==0 && s.storage_chunk_rows==0) || s.cell=="authoritative","phase diagnostic requires authoritative cell");
    ensure!(s.phased_owner_workers==0 || (s.cell=="authoritative" && s.current_embedding_view && s.vector_probe_rounds==0 && s.phased_owner_workers<=s.threads),"phased diagnostic policy differs");
    ensure!(s.split_preparation_workers==0 || (s.cell=="authoritative" && s.split_preparation_workers<=s.threads),"split preparation diagnostic policy differs");
    ensure!(!s.balance_cost_observation || s.cell=="authoritative","balance observation requires authoritative");
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
        let mut tree=tree;tree.set_lazy_disabled_evaluation_trace(s.lazy_disabled_evaluation_trace)?;
        if s.profile_before_scrub {tree.enable_before_scrub_attribution()?;}
        tree.set_lazy_disabled_before_scrub_trace(s.lazy_disabled_before_scrub_trace)?;
        if s.phased_owner_workers>0 {tree.enable_construction_phases(s.phased_owner_workers)?;}
        if s.split_preparation_workers>0 {tree.set_split_preparation_workers(s.split_preparation_workers)?;}
        tree.enable_diagnostic_reads()?;dynamic=Some(tree);
    }
    let setup_seconds=setup.elapsed().as_secs_f64();let setup_storage=storage.lock().unwrap().clone();
    let genesis_vector_root=if s.vector_probe_rounds>0 {dynamic.as_ref().map(|t|t.construction_phase_state()).transpose()?} else {None};
    let mut cuts=vec![];let mut start=if dynamic.is_some(){1}else{0};
    while start<s.rows {
        let end=(((start/s.balance_every)+1)*s.balance_every).min(s.rows);
        let reserve=Instant::now();if s.phased_owner_workers>0 {dynamic.as_mut().unwrap().reserve_construction_phase(end-start,end-start)?;}let reservation_seconds=reserve.elapsed().as_secs_f64();
        let add=Instant::now();
        let mut chunk_roots=vec![];
        if let Some(t)=dynamic.as_mut() {
            let rows:Vec<_>=(start..end).map(|i|(i as u32,vectors[i].clone())).collect();
            let chunk=if s.storage_chunk_rows==0 {rows.len()} else {s.storage_chunk_rows};
            for chunk in rows.chunks(chunk) {t.register_dynamic_batch(chunk).await?;if s.vector_probe_rounds>0 {let state=t.construction_phase_state()?;chunk_roots.push(json!({"start":chunk[0].0,"end":chunk.last().unwrap().0+1,"root":state["embedding_root"]}));}}
        } else if s.cell.starts_with("parallel") {
            let chunk=(end-start+s.threads-1)/s.threads;
            std::thread::scope(|scope|{for (i,rows) in vectors[start..end].chunks(chunk).enumerate(){let writer=&reference;scope.spawn(move||{for (j,v) in rows.iter().enumerate(){writer.add((start+i*chunk+j)as u32,v);}});}});
        } else {for i in start..end {reference.add(i as u32,&vectors[i]);}}
        let add_seconds=add.elapsed().as_secs_f64();let add_storage=storage.lock().unwrap().clone();
        let add_reads=dynamic.as_ref().map(|t|t.diagnostic_read_snapshot()).transpose()?;
        let prebalance_phase=if s.vector_probe_rounds>0 {dynamic.as_ref().map(|t|t.construction_phase_state()).transpose()?} else {None};
        let publication=Instant::now();
        if s.phased_owner_workers>0 {dynamic.as_mut().unwrap().finish_construction_add()?;}
        else if s.current_embedding_view {dynamic.as_mut().unwrap().publish_current_embedding_view()?;}
        let publication_seconds=publication.elapsed().as_secs_f64();
        let before_probe_storage=storage.lock().unwrap().clone();
        let before_probe_counters=dynamic.as_ref().map(|t|t.counters()).transpose()?;
        let before_probe_reads=dynamic.as_ref().map(|t|t.diagnostic_read_snapshot()).transpose()?;
        let probe=Instant::now();let mut probe_hash=Sha256::new();
        for _ in 0..s.vector_probe_rounds {for id in 0..end as u32 {
            let value=dynamic.as_ref().unwrap().lookup_at(0,id)?;
            probe_hash.update(id.to_le_bytes());for bits in value.vector_bits {probe_hash.update(bits.to_le_bytes());}
        }}
        let probe_seconds=probe.elapsed().as_secs_f64();
        let probe_report=json!({"rounds":s.vector_probe_rounds,"ids_per_round":end,"result_sha256":format!("{:x}",probe_hash.finalize()),"seconds":probe_seconds,"storage_before":before_probe_storage,"storage_after":storage.lock().unwrap().clone(),"counters_before":before_probe_counters,"counters_after":dynamic.as_ref().map(|t|t.counters()).transpose()?,"reads_before":before_probe_reads,"reads_after":dynamic.as_ref().map(|t|t.diagnostic_read_snapshot()).transpose()?});
        if s.vector_probe_rounds>0 {dynamic.as_ref().unwrap().finish_diagnostic_reads()?;}
        let preparation_before=dynamic.as_ref().map(|t|t.counters()).transpose()?;
        if s.balance_cost_observation {dynamic.as_mut().unwrap().begin_balance_cost_observation()?;}
        let balance=Instant::now();
        if let Some(t)=dynamic.as_mut(){if s.phased_owner_workers>0 {t.balance_construction_phase()?;}else{t.balance_dynamic_current()?;}}
        else if s.cell.ends_with("parallel"){reference.balance_index_parallel(s.threads);}
        else{reference.balance_index();}
        let balance_ns=balance.elapsed().as_nanos() as u64;let balance_seconds=balance_ns as f64/1e9;
        let balance_cost=if s.balance_cost_observation {let mut value=dynamic.as_mut().unwrap().finish_balance_cost_observation()?;let window=value["observed_window_ns"].as_u64().unwrap();value["cli_balance_wall_ns"]=json!(balance_ns);value["observation_boundary_extra_ns"]=json!(window as i128-balance_ns as i128);value["boundary_scope"]=json!("Observation starts just before the CLI balance timer and ends just after it. Exclusive sum equals CLI wall plus this signed boundary difference; shape/report collection follows the observation.");Some(value)} else {None};
        let preparation_after=dynamic.as_ref().map(|t|t.counters()).transpose()?;
        let preparation_profile=match (&preparation_before,&preparation_after) {
            (Some(before),Some(after))=>{
                let delta=|f:fn(&crate::hierarchical_index::writer::authoritative_owner::Counters)->u64|after.iter().map(|(owner,c)|f(c).saturating_sub(before.get(owner).map(f).unwrap_or(0))).sum::<u64>();
                let ns=delta(|c|c.split_preparation_ns);
                Some(json!({"elapsed_ns":ns,"balance_elapsed_fraction":ns as f64/(balance_seconds*1e9),
                    "calls":delta(|c|c.split_preparation_calls as u64),"rows":delta(|c|c.split_preparation_rows as u64),
                    "worker_jobs":delta(|c|c.split_preparation_worker_jobs as u64),"root_opens":delta(|c|c.split_preparation_root_opens as u64),
                    "reader_map_clears":delta(|c|c.split_preparation_reader_cache_clears as u64),"copied_vector_payload_bytes":delta(|c|c.split_preparation_payload_bytes as u64),
                    "provider_obtains":delta(|c|c.split_preparation_provider_obtains),"provider_hits":delta(|c|c.split_preparation_provider_hits),"provider_misses":delta(|c|c.split_preparation_provider_misses),
                    "successful_storage_origin_encoded_bytes":delta(|c|c.split_preparation_encoded_bytes),"storage_callbacks":delta(|c|c.split_preparation_storage_callbacks),"storage_origin_decode_successes":delta(|c|c.split_preparation_decode_successes),
                    "scope":"Sum of nonoverlapping current SplitInput preparation spans inside serial balance, including worker setup/fetch/copy/revalidation; not CPU fraction. Root-open storage/decode and provider-cache reconstruction excluded from these block counters; stage LocalStorage totals include root returns. Peak fields in owner_counters are cumulative operation maxima/estimates, not RSS."}))
            },_=>None,
        };
        let reshard=Instant::now();if s.phased_owner_workers>0 {dynamic.as_mut().unwrap().reshard_construction_phase()?;}let reshard_seconds=reshard.elapsed().as_secs_f64();
        cuts.push(json!({"rows":end,"add_seconds":add_seconds,"balance_seconds":balance_seconds,
            "balance_cost":balance_cost,"split_preparation":preparation_profile,"reservation_seconds":reservation_seconds,"reshard_seconds":reshard_seconds,"construction_phase":dynamic.as_ref().map(|t|t.construction_phase_report()),"publication_seconds":publication_seconds,"chunk_roots":chunk_roots,"prebalance_phase":prebalance_phase,"probe":probe_report,"storage_after_add":add_storage,"storage_after_balance":storage.lock().unwrap().clone(),
            "vector_reads_after_add":add_reads,"vector_reads_after_balance":if s.vector_probe_rounds>0 {None} else {dynamic.as_ref().map(|t|t.diagnostic_read_snapshot()).transpose()?},
            "trace_attribution":dynamic.as_ref().map(|t|t.disabled_trace_attribution()),
            "before_scrub_profile":dynamic.as_ref().map(|t|t.before_scrub_profile()),
            "owner_counters":dynamic.as_ref().map(|t|t.counters()).transpose()?}));
        start=end;
    }
    let stats=reference.stats.snapshot();let before_save=storage.lock().unwrap().clone();
    let save=Instant::now();let ids=if let Some(t)=dynamic.as_mut(){t.save().await?}else{reference.commit(&provider,None).await.map_err(|e|anyhow::anyhow!("save: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("flush: {e}"))?};
    let save_seconds=save.elapsed().as_secs_f64();drop(dynamic);drop(reference);
    let audit_start=Instant::now();let reader=HierarchicalSpannReader::open(&provider,ids,metric,c).await.map_err(|e|anyhow::anyhow!("reader: {e}"))?;
    let census=reader.audit_reachable_ids(s.rows as u64,AuditLimits{max_bitmap_bytes:(s.rows as u64+7)/8,max_nodes:10000,max_leaf_entries:262144}).await.map_err(|e|anyhow::anyhow!("census: {e}"))?;
    census.require_single_current_copy().map_err(|e|anyhow::anyhow!("current copies: {e}"))?;
    let result=json!({"status":"passed","spec":s,"spec_sha256":format!("{:x}",Sha256::digest(&raw)),"input_seconds":input_seconds,"setup_seconds":setup_seconds,"setup_storage":setup_storage,"genesis_vector_view":genesis_vector_root,"cuts":cuts,
        "reference_stats":{"calls":stats.calls,"nanos":stats.nanos,"add_substeps":stats.add_substeps,"split_substeps":stats.split_substeps},
        "storage_before_save":before_save,"save_seconds":save_seconds,"audit_seconds":audit_start.elapsed().as_secs_f64(),"census":census,
        "scope":"Local actors, independent stochastic trees; cumulative counters. Encoded successfulLocalStorage returns include roots; vector reader counters exclude root open/cache reconstruction. Summed spans overlap, not CPU fractions. Observer overhead included; no physicalIO or coldOS claim."});
    write(&s.output.join("result.json"),&result)?;println!("{}",serde_json::to_string(&result)?);Ok(())
}
