//! Actual global selected-leaf append with fixed immutable parent inputs.
//! Canonical spans are borrowed; only changed owner checkpoints are copied.
use super::*;
use crate::canonical_view::{View,ViewSpec,CanonicalRows};
use hierarchical_index::writer::distributed::{append_owned_global,append_counted_global,CountedWriteOwnership,WriteOwnership};
use chroma_storage::local::LocalReadObservation;
use std::{collections::BTreeMap,sync::{Arc,Mutex}};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    assumption:String,parent_sha256:String,source_id:String,dimension:usize,metric:String,
    #[serde(default)]
    counted_parent:bool,
    #[serde(default)]
    shared_stores:Option<BTreeMap<String,PathBuf>>,
    old_count:u64,view:ViewSpec,owners:Vec<write_plan::Owner>,output:PathBuf,
}
pub async fn run(path:&Path)->Result<()> {
    let mut raw=Vec::new();File::open(path)?.take(64*1024+1).read_to_end(&mut raw)?;ensure!(raw.len()<=64*1024,"bounded POC spec required");
    let spec:Spec=serde_json::from_slice(&raw)?;
    ensure!(spec.assumption=="fixed-immutable-input-poc" && spec.dimension==spec.view.dimension && spec.old_count>0 && spec.old_count<spec.view.rows && spec.view.rows<=128 && spec.owners.len()==2 && spec.parent_sha256.len()==64,"bounded fixed-input append required");
    ensure!(spec.output.is_absolute() && !spec.output.exists(),"fresh private POC output required");
    for owner in &spec.owners {ensure!(!owner.subtree_id.is_empty() && owner.subtree_id.len()<=64 && owner.subtree_id.bytes().all(|b|b.is_ascii_alphanumeric() || b==b'-' || b==b'_'),"bounded logical owner ID required");ensure!(owner.checkpoint_dir.is_absolute() && !spec.output.starts_with(&owner.checkpoint_dir) && !owner.checkpoint_dir.starts_with(&spec.output),"output overlaps immutable input");}
    if let Some(stores)=spec.shared_stores.as_ref() {
        ensure!(!stores.is_empty() && stores.len()<=spec.owners.len(),"bounded owned shared stores required");
        for (sid,path) in stores {
            ensure!(spec.owners.iter().any(|o|&o.subtree_id==sid) && path.is_absolute() && path.is_dir() && path.canonicalize()?==*path,"exact owned shared store required");
            for owner in &spec.owners {let source=owner.checkpoint_dir.canonicalize()?;ensure!(!path.starts_with(&source) && !source.starts_with(path),"shared store overlaps immutable fixture");}
            ensure!(!spec.output.starts_with(path) && !path.starts_with(&spec.output),"shared store overlaps attempt output");
        }
    }
    fs::create_dir_all(&spec.output)?;
    let mut view=View::new(spec.view)?;let mut writers=Vec::new();let mut manifests=Vec::new();let mut maps=Vec::new();let mut boundaries=Vec::new();let mut observations=Vec::new();let mut metadata_reads=BTreeMap::new();let mut previous=None;let mut read_stores=Vec::new();
    for owner in &spec.owners {
        ensure!(previous.as_ref().is_none_or(|s:&String|s<&owner.subtree_id),"stable unique owner order required");previous=Some(owner.subtree_id.clone());
        let read_store=spec.shared_stores.as_ref().and_then(|stores|stores.get(&owner.subtree_id)).unwrap_or(&owner.checkpoint_dir).clone();
        let mut bytes=Vec::new();File::open(read_store.join(format!("manifest-{}.json",owner.generation)))?.take(64*1024+1).read_to_end(&mut bytes)?;
        ensure!(bytes.len()<=64*1024 && {use sha2::{Digest,Sha256};format!("{:x}",Sha256::digest(&bytes))}==owner.manifest_sha256,"actual parent manifest differs");
        let m:Manifest=serde_json::from_slice(&bytes)?;ensure!(m.generation==owner.generation && m.shard_id==owner.subtree_id && m.source_id==spec.source_id && m.dimension==spec.dimension && m.metric==spec.metric,"parent semantics differ");
        let map=m.id_mapping.as_ref().context("explicit map required")?;ensure!(map.count==m.vector_count && map.count<=128 && map.bytes==map.count*8,"bounded parent map required");
        let map_path=Path::new(&map.path);ensure!(!map_path.is_absolute() && map_path.components().count()==1,"checkpoint-local map required");
        let ids=if spec.counted_parent {Vec::new()} else { read_map(&read_store,map)? };
        metadata_reads.insert(owner.subtree_id.clone(),serde_json::json!({"manifest_bytes":bytes.len(),"map_bytes":if spec.counted_parent {0}else{map.bytes},"loaded_old_ids":ids.len()}));
        let observation=Arc::new(Mutex::new(LocalReadObservation::default()));let provider=provider_cache::observed_provider(&read_store,observation.clone()).await?;
        let writer=HierarchicalSpannWriter::open(&provider,m.block_ids.ids()?,metric(&m.metric)?,read_config(&m.construction)?).await.map_err(|e|anyhow::anyhow!("immutable parent open {e}"))?;
        read_stores.push(read_store);writers.push(writer);manifests.push(m);maps.push(ids);boundaries.push(owner.centroid.clone());observations.push(observation);
    }
    let mut counted=CountedWriteOwnership::from_audited_counts(manifests.iter().map(|m|m.vector_count as usize).collect(),128).map_err(anyhow::Error::msg)?;
    ensure!(counted.next_global_id()==spec.old_count,"parent count differs");
    let mut ownership=if spec.counted_parent {None}else{Some(WriteOwnership::new(maps,128).map_err(anyhow::Error::msg)?)};
    let mut sessions=writers.iter_mut().map(|w|w.write_session(spec.parent_sha256.clone()).map_err(anyhow::Error::msg)).collect::<Result<Vec<_>>>()?;
    let mut routes=Vec::new();
    for id in spec.old_count..view.spec.rows {
        let bytes=view.canonical_row(id,spec.dimension)?;let mut vector:Vec<_>=bytes.chunks_exact(4).map(|b|f32::from_le_bytes(b.try_into().unwrap())).collect();ensure!(vector.iter().all(|v|v.is_finite()),"finite append rows required");if spec.metric=="cosine"{normalize(&mut vector)?;}
        let route=if let Some(full)=ownership.as_mut() {
            append_owned_global(&mut sessions,&spec.parent_sha256,full,id,&vector,Some(&boundaries))
        }else{append_counted_global(&mut sessions,&spec.parent_sha256,&mut counted,id,&vector,Some(&boundaries))}.map_err(anyhow::Error::msg)?;
        routes.push(serde_json::json!({"id":id,"subtree_id":spec.owners[route.owner].subtree_id,"leaf_id":route.leaf,"local_id":ownership.as_ref().map_or_else(||counted.count(route.owner),|o|o.maps()[route.owner].len())-1,"distance":route.distance}));
    }drop(sessions);
    let legacy=write_plan::Spec{parent_sha256:spec.parent_sha256.clone(),source_id:spec.source_id.clone(),dimension:spec.dimension,metric:spec.metric.clone(),old_count:spec.old_count,canonical_vectors:PathBuf::new(),canonical_sha256:String::new(),append_vectors:PathBuf::new(),append_sha256:String::new(),owners:Vec::new(),max_rows:128};
    let mut saved=Vec::new();let mut copies=BTreeMap::new();
    let mut owner_results=Vec::new();
    for (index,owner) in spec.owners.iter().enumerate() {
        let count=ownership.as_ref().map_or_else(||counted.count(index),|o|o.maps()[index].len());
        if count==manifests[index].vector_count as usize {
            owner_results.push(if let Some(full)=ownership.as_ref(){serde_json::json!({"subtree_id":owner.subtree_id,"ids":full.maps()[index]})}else{serde_json::json!({"subtree_id":owner.subtree_id,"count":count,"unchanged_map":manifests[index].id_mapping})});
            copies.insert(owner.subtree_id.clone(),serde_json::json!({"copied_bytes":0,"copied_files":0}));continue;
        }
        let selected_map=if let Some(full)=ownership.as_ref(){full.maps()[index].clone()}else{
            let map=manifests[index].id_mapping.as_ref().context("parent map required")?;
            let mut ids=read_map(&read_stores[index],map)?;
            let row=metadata_reads.get_mut(&owner.subtree_id).unwrap();row["map_bytes"]=serde_json::json!(map.bytes);row["loaded_old_ids"]=serde_json::json!(ids.len());
            ids.extend_from_slice(counted.additions(index));ids
        };
        owner_results.push(serde_json::json!({"subtree_id":owner.subtree_id,"ids":selected_map}));
        let mut counts=(0,0);
        let target=if let Some(stores)=spec.shared_stores.as_ref(){stores.get(&owner.subtree_id).context("selected owner has no bootstrapped shared store")?.clone()}else{
            let target=spec.output.join(format!("index-{}",owner.subtree_id));crate::poc_copy::copy_tree(&owner.checkpoint_dir,&target,&mut counts)?;target
        };
        let destination=write_plan::Owner{subtree_id:owner.subtree_id.clone(),checkpoint_dir:target.clone(),generation:owner.generation,manifest_sha256:owner.manifest_sha256.clone(),centroid:owner.centroid.clone()};
        let observation=Arc::new(Mutex::new(LocalReadObservation::default()));
        let receipt=if spec.shared_stores.is_some(){
            write_plan::save_owner_in_shared_store(&mut writers[index],&destination,&mut manifests[index],&selected_map,&mut view,&routes,&legacy,Some(observation.clone())).await?
        }else{write_plan::save_owner_observed(&mut writers[index],&destination,&mut manifests[index],&selected_map,&mut view,&routes,&legacy,Some(observation.clone())).await?}.context("selected writer must save")?;
        let reads=observation.lock().unwrap().clone();ensure!(!reads.overflowed,"saved read accounting exceeded");
        saved.push(serde_json::json!({"checkpoint_dir":target,"receipt":receipt,"encoded_reads":reads}));
        copies.insert(owner.subtree_id.clone(),serde_json::json!({"copied_bytes":counts.0,"copied_files":counts.1}));
    }
    let mut parent_reads=BTreeMap::new();for (owner,observation) in spec.owners.iter().zip(observations) {let reads=observation.lock().unwrap().clone();ensure!(!reads.overflowed,"parent read accounting exceeded");parent_reads.insert(owner.subtree_id.clone(),reads);}
    println!("{}",serde_json::to_string(&serde_json::json!({"status":"saved-poc-lazy-global-writes","assumption":spec.assumption,"parent_sha256":spec.parent_sha256,"routes":routes,"saved":saved,"copies":copies,"canonical_reads":view.reads,"canonical_materialization_bytes":0,"parent_encoded_reads":parent_reads,"parent_map_manifest_reads":metadata_reads,"save_layout":if spec.shared_stores.is_some(){"shared-generation-only"}else{"private-checkpoint-clone"},"owners":owner_results,"coordination":{"counted_parent":spec.counted_parent,"old_version_probes":ownership.as_ref().map_or(0,|o|o.old_version_probes),"parent_admission":"full parent audit is a separate fixed-input experiment prerequisite; counts alone do not prove coverage"},"remaining_linear_costs":["all-owner writer node/centroid metadata and per-row topology-width checks","selected-leaf version reads and selected-owner full map at save","baseline-only all maps/dense-prefix admission/per-row OLD version probes","full changedowner vector/map inputs and audit","record/legacy oracle separate"]}))?);Ok(())
}

fn read_map(directory:&Path,map:&id_map::IdMapping)->Result<Vec<u64>> {
    let mut data=Vec::new();File::open(directory.join(&map.path))?.take(map.bytes+1).read_to_end(&mut data)?;
    ensure!(data.len()==map.bytes as usize && {use sha2::{Digest,Sha256};format!("{:x}",Sha256::digest(&data))}==map.sha256,"parent map differs");
    Ok(data.chunks_exact(8).map(|b|u64::from_le_bytes(b.try_into().unwrap())).collect())
}
