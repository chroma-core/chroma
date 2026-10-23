//! Read-only semantic audit for the fixed-input proportional-validation POC.
use super::*;
use crate::canonical_view::{View,ViewSpec};
use sha2::{Digest,Sha256};
use std::sync::{Arc,Mutex};
use chroma_blockstore::arrow::provider::ArrowBlockfileProvider;
use chroma_storage::{Storage,local::{LocalStorage,LocalReadObservation}};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    assumption:String,checkpoint_dir:PathBuf,generation:u64,manifest_sha256:String,
    map:PathBuf,map_sha256:String,view:ViewSpec,
    subtree_id:String,source_id:String,metric:String,routing_boundary_hash:String,root_ref:serde_json::Value,
    selected:Vec<(u32,u32)>,
}
fn consumed(path:&Path,cap:usize)->Result<Vec<u8>> {
    let mut data=Vec::new();File::open(path)?.take(cap as u64+1).read_to_end(&mut data)?;
    ensure!(data.len()<=cap,"bounded audit input exceeded");Ok(data)
}
fn sha(data:&[u8])->String {format!("{:x}",Sha256::digest(data))}
pub async fn run(path:&Path)->Result<()> {
    let spec:Spec=serde_json::from_slice(&consumed(path,64*1024)?)?;
    ensure!(spec.assumption=="fixed-immutable-input-poc" && spec.checkpoint_dir.is_absolute() && spec.selected.len()<=128,"fixed input audit required");
    let manifest_bytes=consumed(&spec.checkpoint_dir.join(format!("manifest-{}.json",spec.generation)),64*1024)?;
    ensure!(sha(&manifest_bytes)==spec.manifest_sha256,"manifest bytes differ");
    let manifest:Manifest=serde_json::from_slice(&manifest_bytes)?;
    ensure!(manifest.generation==spec.generation && manifest.dimension==spec.view.dimension && manifest.vector_count>0 && manifest.vector_count<=128,"audit shape differs");
    ensure!(manifest.shard_id==spec.subtree_id && manifest.source_id==spec.source_id && manifest.metric==spec.metric && manifest.routing_boundary_hash==spec.routing_boundary_hash && manifest.root_ref.as_ref()==Some(&spec.root_ref),"snapshot owner/source/root differs");
    let map_bytes=consumed(&spec.map,128*8)?;
    ensure!(sha(&map_bytes)==spec.map_sha256 && map_bytes.len()==manifest.vector_count as usize*8,"audit map differs");
    let mapping=manifest.id_mapping.as_ref().context("saved explicit map required")?;
    ensure!(mapping.sha256==spec.map_sha256,"persisted map differs");
    let ids:Vec<u64>=map_bytes.chunks_exact(8).map(|b|u64::from_le_bytes(b.try_into().unwrap())).collect();
    let mut seen=std::collections::HashSet::new();ensure!(ids.iter().all(|id|seen.insert(*id)),"duplicate local map");
    let mut view=View::new(spec.view)?;
    let observation=Arc::new(Mutex::new(LocalReadObservation::default()));
    let storage=LocalStorage::new(spec.checkpoint_dir.join("data").to_str().context("path encoding")?).with_read_observation(observation.clone());
    let policy=provider_cache::ProviderCachePolicy::default();
    let provider=BlockfileProvider::ArrowBlockfileProvider(ArrowBlockfileProvider::new(Storage::Local(storage),3*1024*1024,
        policy.blocks().build_memory_persistent().await.map_err(|e|anyhow::anyhow!("cache {e}"))?,
        policy.roots().build_memory_persistent().await.map_err(|e|anyhow::anyhow!("cache {e}"))?,2));
    let reader=HierarchicalSpannReader::open(&provider,manifest.block_ids.ids()?,metric(&manifest.metric)?,read_config(&manifest.construction)?).await.map_err(|e|anyhow::anyhow!("open {e}"))?;
    let root=reader.describe_root().map_err(|e|anyhow::anyhow!("root {e}"))?;
    ensure!(serde_json::json!({"kind":root.kind,"id":root.id})==spec.root_ref,"actual saved root differs");
    let coverage=reader.audit_reachable_ids(manifest.vector_count,hierarchical_index::reader::AuditLimits{max_bitmap_bytes:1024,max_nodes:4096,max_leaf_entries:4096}).await.map_err(|e|anyhow::anyhow!("saved audit {e}"))?;
    coverage.require_single_current_copy().map_err(|e|anyhow::anyhow!("singlecopy {e}"))?;
    for (local,&global) in ids.iter().enumerate() {
        let bytes=view.read_rows(global,1)?;let mut expected:Vec<f32>=bytes.chunks_exact(4).map(|b|f32::from_le_bytes(b.try_into().unwrap())).collect();
        ensure!(expected.iter().all(|v|v.is_finite()),"nonfinite canonical input");
        if manifest.metric=="cosine" {normalize(&mut expected)?;}
        ensure!(reader.verify_vector_bits(local as u32,&expected).await.map_err(|e|anyhow::anyhow!("bits {e}"))?,"saved vector bits differ");
    }
    for &(leaf,local) in &spec.selected {ensure!(reader.verify_live_leaf_member(leaf,local).await.map_err(|e|anyhow::anyhow!("membership {e}"))?,"selected membership differs");}
    let reads=observation.lock().unwrap().clone();ensure!(!reads.overflowed,"native read accounting exceeded");
    println!("{}",serde_json::to_string(&serde_json::json!({"status":"audited-fixed-input","snapshot_sha256":spec.manifest_sha256,"map_sha256":spec.map_sha256,"coverage":coverage,"global_ids":ids,"selected":spec.selected,"canonical_reads":view.reads,"storage_encoded_reads":reads,"coverage_notes":{"root_and_block_storage_reads":true,"provider_requests_cache_hits_decode_allocations":false,"physical_io_rss":false},"manifest_returned_bytes":manifest_bytes.len(),"map_returned_bytes":map_bytes.len()}))?);
    Ok(())
}
