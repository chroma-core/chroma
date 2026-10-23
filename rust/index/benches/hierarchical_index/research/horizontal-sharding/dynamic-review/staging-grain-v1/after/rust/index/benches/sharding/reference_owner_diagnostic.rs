//! One bounded construction arm; independent schedules are not tree-equality tests.
use crate::{dynamic_construct::{Config,Input},hierarchical_index::{writer::{HierarchicalSpannWriter,reference_owner_adapter::ConcurrentOwnerWriter},reader::{HierarchicalSpannReader,AuditLimits}}};
use anyhow::{ensure,Result,Context};
use serde::{Deserialize,Serialize};
use serde_json::json;
use sha2::{Digest,Sha256};
use std::{fs::{self,File,OpenOptions},io::{Read,Write,BufWriter,Seek,SeekFrom},path::{Path,PathBuf},time::Instant};
#[derive(Deserialize,Serialize)]
#[serde(deny_unknown_fields)]
struct Spec {protocol:String,source_id:String,vectors:Input,prefix_sha256:String,rows:usize,dimension:usize,metric:String,config:Config,treatment:String,threads:usize,balance_every:usize,owner_slots:usize,move_above:usize,#[serde(default)] predicate_batching:bool,#[serde(default)] construction_observation:bool,#[serde(default)] skip_placement:bool,#[serde(default)] direct_current_reads:bool,#[serde(default)] batched_scrub_validation:bool,#[serde(default)] publish_checkpoint:bool,#[serde(default)] live_prefix_validation:bool,#[serde(default)] batched_split_input_validation:bool,#[serde(default)] bounded_vectors:bool,#[serde(default="staging_rows")] staging_rows:usize,#[serde(default="staging_bytes")] staging_working_bytes:usize,#[serde(default="split_rows")] split_rows:usize,#[serde(default="split_bytes")] split_working_bytes:usize,#[serde(default="physical_capacity")] max_owner_physical_rows:usize,#[serde(default="audit_nodes")] max_audit_nodes:usize,#[serde(default="audit_rows")] max_audit_rows:usize,output:PathBuf}
fn staging_rows()->usize{64}
fn staging_bytes()->usize{1024*1024}
fn split_rows()->usize{256}
fn split_bytes()->usize{16*1024*1024}
fn physical_capacity()->usize{1048576}
fn audit_nodes()->usize{10000}
fn audit_rows()->usize{1048576}
fn read_rows(file:&mut File,start:usize,count:usize,dim:usize)->Result<Vec<(u32,Vec<f32>)>> {
    read_rows_with_limit(file,start,count,dim,64)
}
fn read_rows_with_limit(file:&mut File,start:usize,count:usize,dim:usize,limit:usize)->Result<Vec<(u32,Vec<f32>)>> {
    ensure!(count<=limit,"input chunk bound");let mut bytes=vec![0;count.checked_mul(dim).and_then(|n|n.checked_mul(4)).context("chunk extent")?];file.read_exact(&mut bytes)?;
    let rows:Vec<_>=bytes.chunks_exact(dim*4).enumerate().map(|(i,row)|((start+i) as u32,row.chunks_exact(4).map(|v|f32::from_le_bytes(v.try_into().unwrap())).collect::<Vec<_>>())).collect();
    ensure!(rows.iter().all(|(_,v)|v.iter().all(|x|x.is_finite())),"nonfinite input");Ok(rows)
}
pub async fn run(path:&Path)->Result<()> {
    let native_started=Instant::now();
    let mut raw=vec![];File::open(path)?.take(65537).read_to_end(&mut raw)?;ensure!(raw.len()<=65536,"spec cap");let s:Spec=serde_json::from_slice(&raw)?;
    ensure!(s.protocol=="reference-owner-parallel-v1"&&!s.source_id.is_empty()&&["reference","owner"].contains(&s.treatment.as_str()),"diagnostic protocol/treatment");
    ensure!(s.rows>0&&s.rows<u32::MAX as usize&&s.dimension>0&&s.dimension<=768&&s.threads>0&&s.threads<=8&&s.balance_every>0&&s.balance_every<=s.rows&&s.owner_slots>0&&s.owner_slots<=8&&s.move_above>0,"diagnostic bounds");
    ensure!(!s.predicate_batching || s.treatment=="owner", "predicate policy requires owner treatment");
    ensure!(!s.batched_split_input_validation || (s.treatment=="owner" && !s.direct_current_reads), "split input batching requires owner treatment and actor current reads");
    ensure!(!s.live_prefix_validation || (s.treatment=="owner" && s.predicate_batching && !s.direct_current_reads), "live prefix requires owner treatment, predicate batching and actor current reads");
    ensure!(!s.direct_current_reads || s.treatment=="owner", "direct current policy requires owner treatment");
    ensure!(!s.batched_scrub_validation || s.treatment=="owner", "batched scrub policy requires owner treatment");
    ensure!(!s.bounded_vectors || (s.treatment=="owner" && s.threads<=4 && s.config.split_threshold<=s.split_rows),"bounded vector treatment/workers/split limit");
    ensure!(s.max_owner_physical_rows>0 && s.max_owner_physical_rows<u32::MAX as usize && s.max_audit_nodes>0 && s.max_audit_rows>=s.rows,"explicit capacity/audit bounds");
    ensure!(crate::hierarchical_index::writer::owner_vector_store::staging_working_bytes(s.staging_rows,s.dimension)?<=s.staging_working_bytes,"configured input staging budget");
    let config=crate::dynamic_construct::read_config(&json!({"version":2,"protocol":"explicit-fp-global-v1","config":s.config}))?;let metric=crate::metric(&s.metric)?;
    let input_time=Instant::now();let count=s.rows.checked_mul(s.dimension).and_then(|v|v.checked_mul(4)).context("prefix extent")?;
    let mut file=File::open(&s.vectors.path)?;ensure!(file.metadata()?.len()==s.vectors.bytes&&s.vectors.bytes>=count as u64,"source extent");let mut hash=Sha256::new();let mut source_read_calls=0usize;
    for start in (0..s.rows).step_by(64){let rows=read_rows(&mut file,start,(s.rows-start).min(64),s.dimension)?;source_read_calls+=1;for (_,v) in rows{for x in v{hash.update(x.to_bits().to_le_bytes());}}}
    ensure!(format!("{:x}",hash.finalize())==s.prefix_sha256,"consumed prefix SHA");file.seek(SeekFrom::Start(0))?;let input_seconds=input_time.elapsed().as_secs_f64();
    fs::create_dir(&s.output)?;let provider=crate::provider_cache::provider(&s.output.join("store"),crate::provider_cache::ProviderCachePolicy::default()).await?;
    let setup_started=Instant::now();let reference=HierarchicalSpannWriter::new(s.dimension,metric.clone(),config.clone());let mut owner=if s.treatment=="owner"{Some(ConcurrentOwnerWriter::new(s.dimension,metric.clone(),config.clone(),s.owner_slots,s.rows)?)}else{None};
    if let Some(owner)=owner.as_mut(){owner.set_physical_capacity(s.max_owner_physical_rows)?;if s.bounded_vectors{owner.enable_bounded_vectors(provider.clone(),crate::hierarchical_index::writer::owner_vector_store::VectorLimits{append_rows:s.staging_rows,staging_working_bytes:s.staging_working_bytes,split_rows:s.split_rows,split_working_bytes:s.split_working_bytes,..Default::default()})?;}}
    if let Some(owner)=owner.as_mut(){owner.set_predicate_batching(s.predicate_batching)?;}
    reference.stats.observe_current_versions.store(s.construction_observation,std::sync::atomic::Ordering::Relaxed);
    if let Some(owner)=owner.as_mut(){owner.set_construction_observation(s.construction_observation)?;owner.set_direct_current_reads(s.direct_current_reads)?;owner.set_batched_scrub_validation(s.batched_scrub_validation)?;owner.set_live_prefix_validation(s.live_prefix_validation)?;owner.set_batched_split_input_validation(s.batched_split_input_validation)?;}
    let setup_seconds=setup_started.elapsed().as_secs_f64();let mut phases=vec![];let mut phase_count=0u64;let mut total_phase_seconds=0.;let mut total_copy_seconds=0.;let mut streaming_read_calls=0usize;let mut construction_hash=Sha256::new();
    for start in (0..s.rows).step_by(s.balance_every) {let end=(start+s.balance_every).min(s.rows);let phase_started=Instant::now();let messages_before=owner.as_ref().map(|v|v.message_observation());let counts_before=owner.as_ref().map(|v|v.construction_counts()).unwrap_or_else(||reference.stats.construction_counts());let add=Instant::now();let mut row_copy_seconds=0.;
        if let Some(owner)=owner.as_mut(){owner.begin_add_phase(end-start,s.threads)?;}
        for chunk_start in (start..end).step_by(s.staging_rows){let preparation_started=Instant::now();let rows=read_rows_with_limit(&mut file,chunk_start,(end-chunk_start).min(s.staging_rows),s.dimension,s.staging_rows)?;streaming_read_calls+=1;for(_,v) in &rows{for x in v{construction_hash.update(x.to_bits().to_le_bytes());}}row_copy_seconds+=preparation_started.elapsed().as_secs_f64();
            if let Some(owner)=owner.as_mut(){owner.add_chunk(&rows)?;}else{let chunk=rows.len().div_ceil(s.threads);std::thread::scope(|scope|{for rows in rows.chunks(chunk){let writer=&reference;scope.spawn(move||for(id,v) in rows{writer.add(*id,v);});}});}
        }
        let add_seconds=add.elapsed().as_secs_f64();let messages_after_add=owner.as_ref().map(|v|v.message_observation());let counts_after_add=owner.as_ref().map(|v|v.construction_counts()).unwrap_or_else(||reference.stats.construction_counts());let balance=Instant::now();if let Some(owner)=owner.as_mut(){owner.balance_phase(s.threads)?;}else{reference.balance_index_parallel(s.threads);}let balance_seconds=balance.elapsed().as_secs_f64();
        let messages_after_balance=owner.as_ref().map(|v|v.message_observation());let counts_after_balance=owner.as_ref().map(|v|v.construction_counts()).unwrap_or_else(||reference.stats.construction_counts());
        let reshard=Instant::now();let moves=if !s.skip_placement {if let Some(owner)=owner.as_mut(){owner.reshard(s.move_above)?}else{0}}else{0};let reshard_seconds=reshard.elapsed().as_secs_f64();phase_count+=1;total_phase_seconds+=phase_started.elapsed().as_secs_f64();total_copy_seconds+=row_copy_seconds;if phases.len()==32{phases.remove(0);}
        phases.push(json!({"rows":end,"row_copy_seconds":row_copy_seconds,"row_copy_payload_bytes":(end-start)*s.dimension*4,"phase_elapsed_seconds":phase_started.elapsed().as_secs_f64(),"add_seconds":add_seconds,"balance_seconds":balance_seconds,"reshard_seconds":reshard_seconds,"placement_moves":moves,"messages_before":messages_before,"messages_after_add":messages_after_add,"messages_after_balance":messages_after_balance,"counts_before":counts_before,"counts_after_add":counts_after_add,"counts_after_balance":counts_after_balance}));
    }
    ensure!(format!("{:x}",construction_hash.finalize())==s.prefix_sha256,"construction prefix changed after verification");
    let reference_shape=if owner.is_none(){Some(reference.construction_shape())}else{None};
    let save=Instant::now();let ids=if let Some(owner)=&owner {owner.save(&provider).await?}else{reference.commit(&provider,None).await.map_err(|e|anyhow::anyhow!("reference commit: {e}"))?.flush().await.map_err(|e|anyhow::anyhow!("reference flush: {e}"))?};let save_seconds=save.elapsed().as_secs_f64();let report=owner.as_ref().map(|o|o.report());drop(owner);drop(reference);
    let audit=Instant::now();let mut reader=HierarchicalSpannReader::open(&provider,ids.clone(),metric,config).await.map_err(|e|anyhow::anyhow!("reader: {e}"))?;let census=reader.audit_reachable_ids(s.rows as u64,AuditLimits{max_bitmap_bytes:(s.rows as u64).div_ceil(8),max_nodes:s.max_audit_nodes,max_leaf_entries:s.max_audit_rows}).await.map_err(|e|anyhow::anyhow!("census: {e}"))?;census.require_single_current_copy().map_err(|e|anyhow::anyhow!("current membership: {e}"))?;
    file.seek(SeekFrom::Start(0))?;let mut exact_bits=vec![];let mut exact_hash=Sha256::new();let mut validation_read_calls=0usize;
    let query=read_rows(&mut file,0,1,s.dimension)?.remove(0).1;file.seek(SeekFrom::Start(0))?;
    for start in (0..s.rows).step_by(64){let rows=read_rows(&mut file,start,(s.rows-start).min(64),s.dimension)?;validation_read_calls+=1;
        for (id,vector) in &rows{ensure!(reader.verify_vector_bits(*id,vector).await.map_err(|e|anyhow::anyhow!("vector proof: {e}"))?,"saved vector bits mismatch");}
        let local:Vec<_>=rows.iter().map(|(id,_)|*id).collect();let mut exact=reader.rerank_selected_releasing_vectors(&query,&local).await.map_err(|e|anyhow::anyhow!("exact rerank: {e}"))?;exact.sort_by_key(|v|v.0);
        for (id,d) in exact{let bits=d.to_bits();exact_hash.update(id.to_le_bytes());exact_hash.update(bits.to_le_bytes());if s.rows<=16384{exact_bits.push((id,bits));}}
    }
    let exact_sha=format!("{:x}",exact_hash.finalize());let audit_seconds=audit.elapsed().as_secs_f64();
    // Publish once from the audited roots. No downstream manifest rewriting is needed.
    let checkpoint_publication=if s.publish_checkpoint {
        let started=Instant::now();let store=s.output.join("store");let root=reader.describe_root().map_err(|e|anyhow::anyhow!("root: {e}"))?;
        let write_started=Instant::now();let mut file=BufWriter::with_capacity(65536,OpenOptions::new().write(true).create_new(true).open(store.join("id-map-1.u64le"))?);let mut hash=Sha256::new();
        for id in 0..s.rows as u64 {let bytes=id.to_le_bytes();file.write_all(&bytes)?;hash.update(bytes);}
        file.flush()?;file.get_ref().sync_all()?;drop(file);let map_write_seconds=write_started.elapsed().as_secs_f64();
        let mapping=crate::id_map::IdMapping{path:"id-map-1.u64le".into(),sha256:format!("{:x}",hash.finalize()),count:s.rows as u64,bytes:(s.rows as u64).checked_mul(8).context("map extent")?};
        let verify_started=Instant::now();drop(crate::id_map::IdLookup::open(&store,&mapping)?);let map_verify_seconds=verify_started.elapsed().as_secs_f64();
        let checkpoint=json!({"generation":1,"source_id":s.source_id,"shard_id":"global","metric":s.metric,"dimension":s.dimension,"vector_count":s.rows,"global_id_start":0,"global_id_end":s.rows,"id_offset":0,"id_stride":1,"id_mapping":mapping,"routing_boundary_hash":"","root_ref":{"kind":root.kind,"id":root.id},"integrity_audit":census,"construction":{"version":2,"protocol":"explicit-fp-global-v1","config":s.config},"construction_input":{"spec_sha256":format!("{:x}",Sha256::digest(&raw)),"vectors":s.vectors,"prefix_sha256":s.prefix_sha256},"block_ids":{"posting_list_id":ids.posting_list_id,"scalar_metadata_id":ids.scalar_metadata_id,"vector_data_id":ids.vector_data_id,"leaf_node_id":ids.leaf_node_id,"internal_node_id":ids.internal_node_id}});
        let manifest_bytes=serde_json::to_vec_pretty(&checkpoint)?;let manifest_sha256=format!("{:x}",Sha256::digest(&manifest_bytes));let mut file=OpenOptions::new().write(true).create_new(true).open(store.join("manifest.json"))?;file.write_all(&manifest_bytes)?;file.sync_all()?;
        Some(json!({"generation":1,"manifest_path":store.join("manifest.json"),"manifest_bytes":manifest_bytes.len(),"manifest_sha256":manifest_sha256,"id_mapping":mapping,"map_write_bytes":mapping.bytes,"map_verify_bytes":mapping.bytes,"map_write_seconds":map_write_seconds,"map_verify_seconds":map_verify_seconds,"publication_seconds":started.elapsed().as_secs_f64(),"scope":"Original create-new manifest; streamed dense identity map; write/verify payload separately charged, not physical I/O."}))
    }else{None};
    let native_before_serialize_seconds=native_started.elapsed().as_secs_f64();
    let phase_elapsed_seconds=total_phase_seconds;
    let publication_seconds=checkpoint_publication.as_ref().and_then(|v|v["publication_seconds"].as_f64()).unwrap_or(0.);
    let accounted=input_seconds+setup_seconds+phase_elapsed_seconds+save_seconds+audit_seconds+publication_seconds;
    let result=json!({"timing_accounting":{"native_before_serialize_seconds":native_before_serialize_seconds,"setup_seconds":setup_seconds,"phase_elapsed_seconds":phase_elapsed_seconds,"publication_seconds":publication_seconds,"other_before_serialize_seconds":(native_before_serialize_seconds-accounted).max(0.),"scope":"Disjoint enclosing phases; rowcopy/add/balance/reshard timers are nested within phase_elapsed. Owner reservation is included in add, not separately exclusive. Result encoding/stdout/filefsync excluded; whole child process wall retained externally."},"source_read_calls":source_read_calls,"source_read_chunk_rows":64,"streaming_read_calls":streaming_read_calls,"validation_read_calls":validation_read_calls,"construction_staging_chunk_rows":s.staging_rows,"input_returned_chunk_payload_cap_bytes":s.staging_rows.max(64)*s.dimension*4,"input_decode_buffer_payload_cap_bytes":s.staging_rows.max(64)*s.dimension*4,"input_decode_peak_payload_cap_bytes":2*s.staging_rows.max(64)*s.dimension*4,"phase_report_capacity":32,"phase_count":phase_count,"phase_reports_dropped":phase_count.saturating_sub(phases.len() as u64),"row_copy_seconds_total":total_copy_seconds,"source_prefix_read_bytes":count,"randomness":{"split_initialization":"existing thread_rng; seed/choices unavailable and uncontrolled","scheduler":"independent nondeterministic thread schedules; no seeded tree parity claim"},"status":"passed","checkpoint_publication":checkpoint_publication,"construction_policy":if s.treatment=="owner"&&s.predicate_batching&&s.batched_scrub_validation&&!s.direct_current_reads{"owner-batched-v1"}else{"explicit-ablation"},"spec":s,"spec_sha256":format!("{:x}",Sha256::digest(&raw)),"input_seconds":input_seconds,"phases":phases,"owner_report":report,"reference_shape":reference_shape,"save_seconds":save_seconds,"audit_seconds":audit_seconds,"roots":{"posting_list_id":ids.posting_list_id,"scalar_metadata_id":ids.scalar_metadata_id,"vector_data_id":ids.vector_data_id,"leaf_node_id":ids.leaf_node_id,"internal_node_id":ids.internal_node_id},"census":census,"all_id_exact_fp_distance_bits":if s.rows<=16384{Some(exact_bits)}else{None},"all_id_exact_fp_distance_sha256":exact_sha,"scope":"Explicit FP both arms; actual reference parallel global routing and subtree scheduler. Local owner actors; bounded_vectors selects durable append-only staging, otherwise the reference embedding map remains. No cross-host execution. Placement only after joined balance. Full joined snapshot/save/audit charged separately. Independent stochastic trees; all-ID exact rerank parity is not ANN quality/speedup. Full source SHA supplied externally; consumed prefix SHA verified here. Configured split row and modeled working-byte admission are explicit. Central routing, provider caches and full joined publication remain; streaming ingestion alone does not establish million-vector performance readiness."});
    let mut file=OpenOptions::new().write(true).create_new(true).open(s.output.join("result.json"))?;serde_json::to_writer_pretty(&mut file,&result)?;file.write_all(b"\n")?;println!("{}",serde_json::to_string(&result)?);Ok(())
}
