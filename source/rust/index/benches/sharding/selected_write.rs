//! Exclusive owner-local append sessions; the external coordinator owns global routing.
use super::*;
use hierarchical_index::writer::distributed::{global_write_count, WriteChild};
use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, BufReader, Seek, SeekFrom};

const FRAME_BYTES: usize = 64 * 1024;
const REPLY_BYTES: usize = 256 * 1024;
const MAX_COMMANDS: u64 = 256;

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[serde(deny_unknown_fields)]
struct Row {
    global_id: u64, record_offset_id: u32, user_id: String,
    log_offset: i64, embedding_bits: Vec<u32>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionSpec {
    base: write_plan::Spec,
    authorization: String, session_id: String, attempt_id: String,
    collection_version: i32, parent_log_position: i64, pulled_log_offset: i64,
    ownership_generation: u64, source_map_sha256: String,
    parent_deployment_file: PathBuf, registered_bundle_file: PathBuf,
    registered_bundle_sha256: String, materialized_export_file: PathBuf,
    materialized_export_sha256: String, model_file: PathBuf, model_sha256: String,
    worker_id: String, instance_id: String, agent_run_id: String, source_replica_id: String,
    rows: Vec<Row>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    request_id: String, session_id: String, attempt_id: String,
    authorization: String, ownership_generation: u64,
    sequence: u64, row_id: Option<u64>, command: Command,
}
#[derive(Deserialize)]
#[serde(tag="op", rename_all="snake_case", deny_unknown_fields)]
enum Command {
    Info,
    Root,
    Expand { parents: Vec<u32> },
    Count { level: usize, global_level_widths: Vec<usize>, sorted_distance_bits: Vec<u32> },
    Register { leaf: u32, local_id: u32, distance_bits: u32, route_sha256: String },
    Commit { routes: Vec<serde_json::Value> },
    Abort,
}
fn sha(value: &str) -> bool { value.len()==64 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) }
fn child(value: WriteChild) -> Result<serde_json::Value> {
    ensure!(value.distance.is_finite(),"nonfinite write child");
    let bits=value.distance.to_bits();let mut out=serde_json::to_value(value)?;out["distance_bits"]=bits.into();Ok(out)
}
fn read_frame<R: BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>> {
    let mut data=Vec::new();let n=reader.take(FRAME_BYTES as u64+1).read_until(b'\n',&mut data)?;
    if n==0 {return Ok(None)}
    ensure!(n<=FRAME_BYTES && data.last()==Some(&b'\n'),"oversized or incomplete write frame");
    Ok(Some(data))
}
fn bounded_json(path:&Path, maximum:u64)->Result<(serde_json::Value,String)> {
    let mut bytes=Vec::new();
    File::open(path)?.take(maximum+1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() as u64<=maximum,"session artifact exceeds admission");
    use sha2::{Digest,Sha256};
    let hash=format!("{:x}",Sha256::digest(&bytes));
    Ok((serde_json::from_slice(&bytes)?,hash))
}
fn echo(spec:&SessionSpec,frame:&Frame,result:serde_json::Value)->serde_json::Value {
    serde_json::json!({"request_id":frame.request_id,"sequence":frame.sequence,"row_id":frame.row_id,
        "session_id":spec.session_id,"attempt_id":spec.attempt_id,
        "parent_sha256":spec.base.parent_sha256,"ownership_generation":spec.ownership_generation,
        "source_snapshot_sha256":spec.base.owners[0].manifest_sha256,
        "source_map_sha256":spec.source_map_sha256,"result":result})
}
fn emit(value:&serde_json::Value)->Result<Vec<u8>> {
    let bytes=serde_json::to_vec(value)?;ensure!(bytes.len()<REPLY_BYTES,"serialized write reply exceeds admission");
    let mut stdout=std::io::stdout().lock();stdout.write_all(&bytes)?;stdout.write_all(b"\n")?;stdout.flush()?;Ok(bytes)
}

pub async fn run(path:&Path)->Result<()> {
    let (spec_json,spec_sha)=bounded_json(path,FRAME_BYTES as u64)?;
    let spec:SessionSpec=serde_json::from_value(spec_json)?;
    ensure!(spec.base.owners.len()==1 && spec.rows.len()==2 && spec.base.dimension>0 && spec.base.dimension<=16 && spec.base.max_rows<=128 && spec.base.old_count+2<=spec.base.max_rows as u64,"tiny owner session required");
    ensure!(sha(&spec.base.parent_sha256) && sha(&spec.source_map_sha256) && sha(&spec.materialized_export_sha256) && sha(&spec.registered_bundle_sha256) && sha(&spec.model_sha256),"exact artifact hashes required");
    ensure!(!spec.authorization.is_empty() && spec.authorization.len()<=256 && !spec.session_id.is_empty() && spec.session_id.len()<=96 && !spec.attempt_id.is_empty() && spec.attempt_id.len()<=96,"bounded session scope required");
    let owner=&spec.base.owners[0];let (parent,parent_sha)=bounded_json(&spec.parent_deployment_file,32*1024*1024)?;
    ensure!(parent_sha==spec.base.parent_sha256 && parent["fence"]==spec.ownership_generation && parent["tree"]["source"]["row_count"]==spec.base.old_count,"session parent differs");
    let source=&parent["owners"][&owner.subtree_id];let remote=&source["remote"];
    ensure!(remote["worker_id"]==spec.worker_id && remote["instance_id"]==spec.instance_id && remote["agent_run_id"]==spec.agent_run_id && remote["replica_id"]==spec.source_replica_id,"source physical tuple differs");
    let (bundle,bundle_sha)=bounded_json(&spec.registered_bundle_file,32*1024*1024)?;
    ensure!(bundle_sha==spec.registered_bundle_sha256 && bundle["deployment"]==parent && bundle["collection_parent"]["collection"]["version"].as_i64().and_then(|v|v.checked_add(1))==Some(spec.collection_version as i64) && bundle["compaction"]["pulled_log_offset"]==spec.parent_log_position,"registered collection fence differs");
    let (export,export_sha)=bounded_json(&spec.materialized_export_file,2*1024*1024)?;let captured=&export["captured"];
    ensure!(export_sha==spec.materialized_export_sha256 && export["registered_bundle"]["sha256"]==spec.registered_bundle_sha256 && captured["parent_deployment_sha256"]==spec.base.parent_sha256 && captured["ownership_generation"]==spec.ownership_generation && captured["collection_id"]==bundle["collection_parent"]["collection"]["collection_id"] && captured["collection_version"]==spec.collection_version && captured["parent_log_position"]==spec.parent_log_position && captured["pulled_log_offset"]==spec.pulled_log_offset && captured["rows"]==serde_json::to_value(&spec.rows)?,"actual materialized input differs");
    ensure!(spec.parent_log_position.checked_add(2)==Some(spec.pulled_log_offset),"append log range differs");
    let (model,model_sha)=bounded_json(&spec.model_file,2*1024*1024)?;ensure!(model_sha==spec.model_sha256 && parent["tree"]["model"]["sha256"]==spec.model_sha256,"committed model differs");
    let slot=parent["tree"]["subtrees"].as_array().context("owner slots required")?.iter().find(|r|r["subtree_id"]==owner.subtree_id).context("source owner missing")?;
    let index=slot["centroid_index"].as_u64().context("centroid index missing")? as usize;
    let centroid:Vec<f32>=serde_json::from_value(model["centroids"][index].clone())?;
    ensure!(centroid.iter().map(|v|v.to_bits()).eq(owner.centroid.iter().map(|v|v.to_bits())) && slot["snapshot"]["sha256"]==owner.manifest_sha256 && slot["generation"]==owner.generation && slot["local_to_global"]["sha256"]==spec.source_map_sha256,"committed source root/alias differs");
    let manifest=owner.checkpoint_dir.join(format!("manifest-{}.json",owner.generation));
    let (manifest_json,manifest_sha)=bounded_json(&manifest,2*1024*1024)?;
    ensure!(manifest_sha==owner.manifest_sha256,"source manifest differs");
    let mut m:Manifest=serde_json::from_value(manifest_json)?;
    ensure!(m.shard_id==owner.subtree_id && m.generation==owner.generation && m.dimension==spec.base.dimension && m.metric==spec.base.metric && m.source_id==spec.base.source_id && slot["routing_boundary_hash"].as_str()==Some(m.routing_boundary_hash.as_str()) && parent["tree"]["dimension"]==m.dimension && parent["tree"]["metric"]==m.metric && parent["tree"]["source"]["source_id"]==m.source_id,"source interpretation differs");
    let (marker,_)=bounded_json(&owner.checkpoint_dir.join("global-write-private.json"),4096)?;
    ensure!(marker["parent_sha256"]==spec.base.parent_sha256 && marker["manifest_sha256"]==owner.manifest_sha256 && marker["subtree_id"]==owner.subtree_id,"private clone marker differs");
    ensure!(write_plan::digest(&spec.base.canonical_vectors)?==spec.base.canonical_sha256 && write_plan::digest(&spec.base.append_vectors)?==spec.base.append_sha256,"candidate vector input differs");
    let row_bytes=(spec.base.dimension*4) as u64;let mut canonical=File::open(&spec.base.canonical_vectors)?;
    ensure!(canonical.metadata()?.len()==(spec.base.old_count+2)*row_bytes && spec.base.append_vectors.metadata()?.len()==2*row_bytes,"candidate shape differs");
    let mut vectors=BTreeMap::new();let mut users=HashSet::new();let mut offsets=HashSet::new();let mut suffix=File::open(&spec.base.append_vectors)?;
    for (i,row) in spec.rows.iter().enumerate() {
        ensure!(row.global_id==spec.base.old_count+i as u64 && row.embedding_bits.len()==spec.base.dimension && row.log_offset==spec.parent_log_position+i as i64+1 && row.record_offset_id>0 && offsets.insert(row.record_offset_id) && !row.user_id.is_empty() && users.insert(&row.user_id),"declared append row differs");
        canonical.seek(SeekFrom::Start(row.global_id*row_bytes))?;let mut raw=vec![0;row_bytes as usize];canonical.read_exact(&mut raw)?;let bits:Vec<_>=raw.chunks_exact(4).map(|b|u32::from_le_bytes(b.try_into().unwrap())).collect();let mut append_raw=vec![0;row_bytes as usize];suffix.read_exact(&mut append_raw)?;
        ensure!(bits==row.embedding_bits && append_raw==raw,"materialized vector bits differ");let mut vector:Vec<_>=bits.into_iter().map(f32::from_bits).collect();ensure!(vector.iter().all(|v|v.is_finite()),"nonfinite source vector");if spec.base.metric=="cosine" {normalize(&mut vector)?;}vectors.insert(row.global_id,vector);
    }
    let mapping=m.id_mapping.as_ref().context("explicit source map required")?;ensure!(mapping.sha256==spec.source_map_sha256,"source map identity differs");let mut lookup=id_map::IdLookup::open(&owner.checkpoint_dir,mapping)?;let mut ids=Vec::new();let mut seen=HashSet::new();for local in 0..m.vector_count {let id=lookup.get(u32::try_from(local)?)?;ensure!(id<spec.base.old_count && seen.insert(id),"source local/global ownership differs");ids.push(id);}
    let provider=provider(&owner.checkpoint_dir).await?;let parent_reader=HierarchicalSpannReader::open(&provider,m.block_ids.ids()?,metric(&m.metric)?,read_config(&m.construction)?).await.map_err(|e|anyhow::anyhow!("source open: {e}"))?;
    parent_reader.audit_reachable_ids(m.vector_count,hierarchical_index::reader::AuditLimits{max_bitmap_bytes:1_000_000,max_nodes:1_000_000,max_leaf_entries:100_000}).await.map_err(|e|anyhow::anyhow!("source audit: {e}"))?.require_single_current_copy().map_err(|e|anyhow::anyhow!("source single-copy audit: {e}"))?;drop(parent_reader);
    let mut writer=HierarchicalSpannWriter::open(&provider,m.block_ids.ids()?,metric(&m.metric)?,read_config(&m.construction)?).await.map_err(|e|anyhow::anyhow!("writer open: {e}"))?;
    let mut reached_internal:BTreeMap<u64,HashSet<u32>>=BTreeMap::new();
    let mut reached:BTreeMap<u64,BTreeMap<u32,u32>>=BTreeMap::new();let mut registrations=BTreeMap::<u64,serde_json::Value>::new();let mut previous:Option<(u64,Vec<u8>,Vec<u8>)>=None;let mut expected=1;
    let stdin=std::io::stdin();let mut input=BufReader::new(stdin.lock());
    while let Some(bytes)=read_frame(&mut input)? {
        let frame:Frame=serde_json::from_slice(&bytes)?;
        if let Some((seq,prior,reply))=&previous {if frame.sequence==*seq {ensure!(*prior==bytes,"conflicting write replay");let mut out=std::io::stdout().lock();out.write_all(reply)?;out.write_all(b"\n")?;out.flush()?;continue;}}
        ensure!(frame.sequence==expected && expected<=MAX_COMMANDS && frame.session_id==spec.session_id && frame.attempt_id==spec.attempt_id && frame.authorization==spec.authorization && frame.ownership_generation==spec.ownership_generation,"write command scope/sequence differs");
        let row_tag=frame.row_id.map(|id|id.to_string()).unwrap_or_else(||"all".into());ensure!(frame.request_id==format!("{}:{}:{}:{}",spec.session_id,spec.attempt_id,row_tag,frame.sequence),"scoped request identity differs");
        let result:Result<(serde_json::Value,bool)>=async {
            let vector=frame.row_id.and_then(|id|vectors.get(&id));
            let result=match &frame.command {
                Command::Info=>{ensure!(frame.row_id.is_none(),"info row differs");let widths=writer.level_node_counts();let session=writer.write_session(spec.authorization.clone()).map_err(anyhow::Error::msg)?;serde_json::json!({"op":"info","spec_sha256":spec_sha,"policy":session.policy_settings(),"local_level_widths":widths,"old_local_count":m.vector_count})},
                Command::Root=>{let v=vector.context("undeclared row")?;let session=writer.write_session(spec.authorization.clone()).map_err(anyhow::Error::msg)?;let value=session.score_boundary(&spec.authorization,v,&owner.centroid).map_err(anyhow::Error::msg)?;if value.is_leaf {reached.entry(frame.row_id.unwrap()).or_default().insert(value.node_id,value.distance.to_bits());}else{reached_internal.entry(frame.row_id.unwrap()).or_default().insert(value.node_id);}serde_json::json!({"op":"root","root":child(value)?})},
                Command::Expand{parents}=>{ensure!(!parents.is_empty() && parents.len()<=1024,"bounded selected parents required");let v=vector.context("undeclared row")?;let row=frame.row_id.context("expansion row missing")?;let unique:HashSet<_>=parents.iter().copied().collect();ensure!(unique.len()==parents.len() && parents.iter().all(|id|reached_internal.get(&row).is_some_and(|set|set.contains(id))),"expansion parents must be distinct returned internal nodes");let session=writer.write_session(spec.authorization.clone()).map_err(anyhow::Error::msg)?;let children=session.expand(&spec.authorization,parents,v).map_err(anyhow::Error::msg)?;ensure!(children.len()<=1024,"child reply exceeds admission");let mut output=Vec::new();for value in children {if value.is_leaf {reached.entry(frame.row_id.unwrap()).or_default().insert(value.node_id,value.distance.to_bits());}else{reached_internal.entry(frame.row_id.unwrap()).or_default().insert(value.node_id);}output.push(child(value)?);}serde_json::json!({"op":"expand","children":output})},
                Command::Count{level,global_level_widths,sorted_distance_bits}=>{ensure!(vector.is_some() && *level>0 && *level<=128 && global_level_widths.len()<=128 && !sorted_distance_bits.is_empty() && sorted_distance_bits.len()<=1024 && global_level_widths.iter().all(|v|*v<=100_000),"bounded global count required");let local=writer.level_node_counts();ensure!(global_level_widths.len()>=local.len() && global_level_widths.first().copied()==Some(parent["tree"]["subtrees"].as_array().unwrap().len()) && local.iter().enumerate().all(|(i,n)|global_level_widths[i]>=*n),"global widths exclude captured source");let distances:Vec<_>=sorted_distance_bits.iter().copied().map(f32::from_bits).collect();let session=writer.write_session(spec.authorization.clone()).map_err(anyhow::Error::msg)?;let count=global_write_count(&distances,&session.policy(global_level_widths.clone()),*level).map_err(anyhow::Error::msg)?;serde_json::json!({"op":"count","count":count})},
                Command::Register{leaf,local_id,distance_bits,route_sha256}=>{let id=frame.row_id.context("registration row missing")?;let v=vector.context("undeclared registration row")?;ensure!(sha(route_sha256) && !registrations.contains_key(&id) && *local_id as usize==ids.len() && reached.get(&id).and_then(|r|r.get(leaf))==Some(distance_bits),"selected reached leaf/ID/grant differs");let mut session=writer.write_session(spec.authorization.clone()).map_err(anyhow::Error::msg)?;session.register_new(&spec.authorization,*leaf,*local_id,v).map_err(anyhow::Error::msg)?;ids.push(id);let route=serde_json::json!({"id":id,"subtree_id":owner.subtree_id,"leaf_id":leaf,"local_id":local_id,"distance":f32::from_bits(*distance_bits)});registrations.insert(id,route);serde_json::json!({"op":"registered","global_id":id,"local_id":local_id,"leaf_id":leaf,"route_sha256":route_sha256})},
                Command::Commit{routes}=>{ensure!(frame.row_id.is_none() && routes.len()==spec.rows.len() && !registrations.is_empty(),"complete route plan required");for (i,route) in routes.iter().enumerate() {ensure!(route["id"]==spec.rows[i].global_id,"global route order differs");if route["subtree_id"]==owner.subtree_id {ensure!(registrations.get(&spec.rows[i].global_id)==Some(route),"saved route grant differs");}else {ensure!(!registrations.contains_key(&spec.rows[i].global_id),"registered row claims another owner");}}
                    ensure!(write_plan::digest(path)?==spec_sha && write_plan::digest(&spec.base.canonical_vectors)?==spec.base.canonical_sha256 && write_plan::digest(&spec.materialized_export_file)?==spec.materialized_export_sha256,"commit inputs changed");let saved=write_plan::save_owner(&mut writer,owner,&mut m,&ids,&mut canonical,routes,&spec.base).await?.context("changed selected writer required")?;ensure!(write_plan::digest(&spec.base.canonical_vectors)?==spec.base.canonical_sha256,"canonical input changed during flush");return Ok((serde_json::json!({"op":"saved","spec_sha256":spec_sha,"receipt":saved,"ids":ids}),true));},
                Command::Abort=>{ensure!(frame.row_id.is_none(),"abort row differs");return Ok((serde_json::json!({"op":"aborted"}),true));},
            };Ok((result,false))
        }.await;
        match result {Ok((value,terminal))=>{let reply=emit(&echo(&spec,&frame,value))?;previous=Some((frame.sequence,bytes,reply));expected+=1;if terminal{return Ok(())}},Err(error)=>{emit(&echo(&spec,&frame,serde_json::json!({"op":"failed","error":error.to_string(),"poisoned":true})))?;return Err(error)}}
    }
    bail!("write session ended without commit or explicit abort; private writer discarded")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn frames_reject_truncation_and_oversize_before_json() {
        assert!(read_frame(&mut std::io::Cursor::new(b"{}")).is_err());
        assert!(read_frame(&mut std::io::Cursor::new(vec![b'x';FRAME_BYTES+1])).is_err());
        assert_eq!(read_frame(&mut std::io::Cursor::new(b"{}\n")).unwrap().unwrap(),b"{}\n");
        assert!(read_frame(&mut std::io::Cursor::new(b"")).unwrap().is_none());
    }
}
