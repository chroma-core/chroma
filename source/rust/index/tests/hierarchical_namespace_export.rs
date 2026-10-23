//! Local fixture export of the actual parent-record materializer and writer.
//! No collection registration or network call occurs in this target.
#[path="../../worker/src/execution/orchestration/hierarchical_materialized_input.rs"]
mod hierarchical_materialized_input;
#[path="../../worker/src/execution/orchestration/hierarchical_record_namespace.rs"]
mod hierarchical_record_namespace;
use hierarchical_record_namespace::*;
use chroma_segment::{blockfile_record::{RecordSegmentWriter,RecordSegmentReaderOptions},types::materialize_logs};
use chroma_types::{Chunk,Collection,LogRecord,Operation,OperationRecord,SegmentScope};
use sha2::{Digest,Sha256};
use std::{path::{Path,PathBuf},fs::{self,File,OpenOptions},io::{Read,Write},collections::BTreeMap};
fn digest(bytes:&[u8])->String {format!("{:x}",Sha256::digest(bytes))}
fn read(path:&Path,cap:usize)->Vec<u8> {let mut b=Vec::new();File::open(path).unwrap().take(cap as u64+1).read_to_end(&mut b).unwrap();assert!(b.len()<=cap);b}
fn checked(a:&Artifact,cap:usize)->Vec<u8> {assert!(a.bytes<=cap as u64);let b=read(Path::new(&a.path),cap);assert_eq!(b.len() as u64,a.bytes);assert_eq!(digest(&b),a.sha256);b}
fn write(path:&Path,bytes:&[u8])->Artifact {let mut f=OpenOptions::new().create_new(true).write(true).open(path).unwrap();f.write_all(bytes).unwrap();f.sync_all().unwrap();Artifact{path:path.canonicalize().unwrap().to_str().unwrap().into(),bytes:bytes.len() as u64,sha256:digest(bytes)}}
fn provider(root:&Path)->chroma_blockstore::provider::BlockfileProvider {
    use chroma_blockstore::arrow::config::{BlockManagerConfig,TEST_MAX_BLOCK_SIZE_BYTES};
    chroma_blockstore::provider::BlockfileProvider::new_arrow(chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(root.to_str().unwrap())),TEST_MAX_BLOCK_SIZE_BYTES,chroma_cache::new_cache_for_test(),chroma_cache::new_cache_for_test(),BlockManagerConfig::default_num_concurrent_block_flushes())
}
fn raw(offset:i64,user:&str,vector:Vec<f32>)->LogRecord {LogRecord{log_offset:offset,record:OperationRecord{id:user.into(),embedding:Some(vector),encoding:None,metadata:None,document:None,operation:Operation::Add}}}
fn inventory(root:&Path)->BTreeMap<String,Artifact> {
    fn visit(root:&Path,dir:&Path,out:&mut BTreeMap<String,Artifact>){for e in fs::read_dir(dir).unwrap(){let p=e.unwrap().path();assert!(!p.is_symlink());if p.is_dir(){visit(root,&p,out)}else{let b=read(&p,4*1024*1024);out.insert(p.strip_prefix(root).unwrap().to_str().unwrap().into(),Artifact{path:p.to_str().unwrap().into(),bytes:b.len() as u64,sha256:digest(&b)});}}}
    let mut out=BTreeMap::new();visit(root,root,&mut out);out
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct NewRow{log_offset:i64,user_id:String,embedding_bits:Vec<u32>}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Input{collection_id:chroma_types::CollectionUuid,record_segment_id:String,tenant:String,database:String,database_id:chroma_types::DatabaseUuid,version:i32,log_position:i64,parent_deployment_sha256:String,ownership_generation:u64,canonical:Artifact,identity_source:Artifact,owner_maps:Vec<Artifact>,rows:Vec<NewRow>}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore="requires reviewed pinned input and fresh local evidence root"]
async fn export_actual_namespace_append() {
    let input_path=PathBuf::from(std::env::var("SPANN_NAMESPACE_INPUT").unwrap());let bytes=read(&input_path,65536);let input:Input=serde_json::from_slice(&bytes).unwrap();
    assert_eq!(input.rows.len(),2);assert_eq!(input.version,3);assert_eq!(input.log_position,20);
    let root=PathBuf::from(std::env::var("SPANN_NAMESPACE_OUTPUT").unwrap());assert!(root.is_absolute());fs::create_dir(&root).unwrap();let store=root.join("record-store");fs::create_dir(&store).unwrap();
    write(&root.join("intent.json"),&serde_json::to_vec(&serde_json::json!({"phase":"preparing-private-records","input_sha256":digest(&bytes)})).unwrap());
    let canonical=checked(&input.canonical,256);assert_eq!(canonical.len(),256);
    let vectors:Vec<Vec<f32>>=canonical.chunks_exact(16).map(|b|b.chunks_exact(4).map(|v|f32::from_le_bytes(v.try_into().unwrap())).collect()).collect();assert!(vectors.iter().flatten().all(|v|v.is_finite()));
    let identities:Vec<SourceIdentity>=serde_json::from_slice(&checked(&input.identity_source,65536)).unwrap();assert_eq!(identities.len(),16);
    for (i,row) in identities.iter().enumerate(){assert_eq!(row.global_id,i as u64);}
    let maps:Vec<Vec<u8>>=input.owner_maps.iter().map(|a|checked(a,128)).collect();
    let mut collection=Collection::test_collection(4);collection.collection_id=input.collection_id;collection.database_id=input.database_id;collection.tenant=input.tenant;collection.database=input.database;collection.version=input.version;collection.log_position=input.log_position;collection.total_records_post_compaction=16;
    let mut segment=chroma_types::test_segment(collection.collection_id,SegmentScope::RECORD);segment.id=input.record_segment_id.parse().unwrap();segment.r#type=chroma_types::SegmentType::BlockfileRecord;
    let permutation:Vec<usize>=(0..16).map(|i|(i*5+3)%16).collect();
    let logs=Chunk::new(permutation.iter().enumerate().map(|(i,g)|raw(5+i as i64,&identities[*g].user_id,vectors[*g].clone())).collect::<Vec<_>>().into());
    let materialized=materialize_logs(&None,logs,None,&RecordSegmentReaderOptions::default()).await.unwrap();let p=provider(&store);
    let writer=RecordSegmentWriter::from_segment(&collection.tenant,&collection.database_id,&segment,&p,None,None).await.unwrap();writer.apply_materialized_log_chunk(&None,&materialized).await.unwrap();segment.file_path=writer.commit().await.unwrap().flush().await.unwrap();drop(materialized);drop(p);
    let rows:Vec<_>=(0..16).map(|g|NamespaceRow{global_id:g as u64,record_offset_id:permutation.iter().position(|x|*x==g).unwrap() as u32+1,user_id:identities[g].user_id.clone()}).collect();
    let mapping=write(&root.join("parent-mapping.json"),&serde_json::to_vec(&rows).unwrap());
    let ns=NamespaceRoot{format_version:1,collection:collection_binding(&collection).unwrap(),record_segment_id:segment.id.to_string(),record_files:segment.file_path.clone(),construction_parent_deployment_sha256:input.parent_deployment_sha256.clone(),construction_parent_ownership_generation:input.ownership_generation,row_count:16,identity_source:input.identity_source.clone(),mapping,canonical_vectors:input.canonical.clone(),owner_maps:input.owner_maps,parent_namespace_sha256:None};
    let parent_root=write(&root.join("parent-namespace.json"),&serde_json::to_vec(&ns).unwrap());
    let p=provider(&store);let audited=audit_construction(&collection,&segment,&p,&ConstructionNamespaceContext{root:parent_root.clone(),parent:hierarchical_materialized_input::ParentOwnershipBinding{deployment_sha256:input.parent_deployment_sha256,ownership_generation:input.ownership_generation,next_global_id:16}}).await.unwrap();
    let logs=Chunk::new(input.rows.into_iter().map(|r|raw(r.log_offset,&r.user_id,r.embedding_bits.into_iter().map(f32::from_bits).collect())).collect::<Vec<_>>().into());
    let saved=audited.flush_record_append(&p,22,logs).await.unwrap();
    let candidate:Vec<u8>=canonical.iter().copied().chain(saved.captured().rows.iter().flat_map(|r|r.embedding_bits.iter().flat_map(|v|v.to_le_bytes()))).collect();
    let all_map=(0..18u64).flat_map(|v|v.to_le_bytes()).collect::<Vec<_>>();saved.audit_saved(&provider(&store),&candidate,&[all_map]).await.unwrap();
    assert_eq!(saved.captured().rows.iter().map(|r|(r.record_offset_id,r.global_id)).collect::<Vec<_>>(),vec![(17,16),(18,17)]);
    let exported=serde_json::json!({"format_version":1,"captured":saved.captured(),"parent_namespace":parent_root,"parent_record_files":segment.file_path,"parent_collection_binding":collection_binding(&collection).unwrap(),"candidate_record_files":saved.segment().file_path,"namespace_rows":saved.namespace_rows(),"candidate_canonical":write(&root.join("candidate.f32"),&candidate),"record_store":store,"record_inventory":inventory(&store)});
    let output=write(&root.join("export.json"),&serde_json::to_vec(&exported).unwrap());
    let runtime=std::env::current_exe().unwrap();let runtime_bytes=read(&runtime,256*1024*1024);
    let proof=serde_json::json!({"format_version":1,"export":output,"input":{"path":input_path,"bytes":bytes.len(),"sha256":digest(&bytes)},"runtime_binary":{"path":runtime,"bytes":runtime_bytes.len(),"sha256":digest(&runtime_bytes)},"producer_sources":{
        "rust/index/tests/hierarchical_namespace_export.rs":digest(include_bytes!("hierarchical_namespace_export.rs")),
        "rust/worker/src/execution/orchestration/hierarchical_record_namespace.rs":digest(include_bytes!("../../worker/src/execution/orchestration/hierarchical_record_namespace.rs")),
        "rust/worker/src/execution/orchestration/hierarchical_materialized_input.rs":digest(include_bytes!("../../worker/src/execution/orchestration/hierarchical_materialized_input.rs")),
        "rust/segment/src/types.rs":digest(include_bytes!("../../segment/src/types.rs")),
        "rust/segment/src/blockfile_record.rs":digest(include_bytes!("../../segment/src/blockfile_record.rs"))}});
    write(&root.join("export.receipt.json"),&serde_json::to_vec(&proof).unwrap());File::open(&root).unwrap().sync_all().unwrap();
}

fn restore_collection(binding:&serde_json::Value)->Collection {
    let mut c:Collection=serde_json::from_value(binding["wire"].clone()).unwrap();
    c.total_records_post_compaction=serde_json::from_value(binding["total_records_post_compaction"].clone()).unwrap();
    c.size_bytes_post_compaction=serde_json::from_value(binding["size_bytes_post_compaction"].clone()).unwrap();
    c.last_compaction_time_secs=serde_json::from_value(binding["last_compaction_time_secs"].clone()).unwrap();
    c.version_file_path=serde_json::from_value(binding["version_file_path"].clone()).unwrap();
    c.root_collection_id=serde_json::from_value(binding["root_collection_id"].clone()).unwrap();
    c.lineage_file_path=serde_json::from_value(binding["lineage_file_path"].clone()).unwrap();
    c.updated_at=serde_json::from_value(binding["updated_at"].clone()).unwrap();
    c.database_id=serde_json::from_value(binding["database_id"].clone()).unwrap();
    c.compaction_failure_count=serde_json::from_value(binding["compaction_failure_count"].clone()).unwrap();c
}
#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore="requires reviewed readonly candidate audit input"]
async fn audit_saved_namespace_candidate() {
    #[derive(serde::Deserialize)]#[serde(deny_unknown_fields)]
    struct AuditInput{candidate:Artifact,parent:Artifact,export:Artifact,store:String,output:String}
    let input_path=PathBuf::from(std::env::var("SPANN_NAMESPACE_AUDIT_INPUT").unwrap());let bytes=read(&input_path,65536);let input:AuditInput=serde_json::from_slice(&bytes).unwrap();
    let old:NamespaceRoot=serde_json::from_slice(&checked(&input.parent,2*1024*1024)).unwrap();
    let candidate:NamespaceRoot=serde_json::from_slice(&checked(&input.candidate,2*1024*1024)).unwrap();
    let export:serde_json::Value=serde_json::from_slice(&checked(&input.export,2*1024*1024)).unwrap();
    assert_eq!(export["parent_namespace"],serde_json::to_value(&input.parent).unwrap());
    assert_eq!(export["candidate_record_files"],serde_json::to_value(&candidate.record_files).unwrap());
    assert_eq!(candidate.parent_namespace_sha256.as_deref(),Some(input.parent.sha256.as_str()));
    assert_eq!(candidate.row_count,old.row_count+2);
    assert_eq!(candidate.construction_parent_deployment_sha256,old.construction_parent_deployment_sha256);
    assert_eq!(candidate.construction_parent_ownership_generation,old.construction_parent_ownership_generation);
    let old_rows:Vec<NamespaceRow>=serde_json::from_slice(&checked(&old.mapping,65536)).unwrap();
    let rows:Vec<NamespaceRow>=serde_json::from_slice(&checked(&candidate.mapping,65536)).unwrap();
    assert_eq!(&rows[..old_rows.len()],old_rows.as_slice());assert_eq!(serde_json::to_value(&rows).unwrap(),export["namespace_rows"]);
    let parent_canonical=checked(&old.canonical_vectors,256);let canonical=checked(&candidate.canonical_vectors,288);assert_eq!(&canonical[..256],parent_canonical.as_slice());
    let appended:Vec<u8>=export["captured"]["rows"].as_array().unwrap().iter().flat_map(|r|r["embedding_bits"].as_array().unwrap().iter().flat_map(|b|u32::try_from(b.as_u64().unwrap()).unwrap().to_le_bytes())).collect();assert_eq!(&canonical[256..],appended.as_slice());
    let mut expected=restore_collection(&old.collection);
    expected.version=expected.version.checked_add(1).unwrap();expected.log_position+=2;expected.total_records_post_compaction+=2;
    expected.size_bytes_post_compaction=restore_collection(&candidate.collection).size_bytes_post_compaction;
    assert_eq!(collection_binding(&expected).unwrap(),candidate.collection);
    assert_eq!(export["captured"]["parent_deployment_sha256"],old.construction_parent_deployment_sha256);
    assert_eq!(export["captured"]["ownership_generation"],old.construction_parent_ownership_generation);
    let p=provider(Path::new(&input.store));
    for (ns,reference) in [(&old,&input.parent),(&candidate,&input.candidate)] {
        let collection=restore_collection(&ns.collection);let mut segment=chroma_types::test_segment(collection.collection_id,SegmentScope::RECORD);segment.id=ns.record_segment_id.parse().unwrap();segment.r#type=chroma_types::SegmentType::BlockfileRecord;segment.file_path=ns.record_files.clone();
        let admitted=audit_construction(&collection,&segment,&p,&ConstructionNamespaceContext{root:reference.clone(),parent:hierarchical_materialized_input::ParentOwnershipBinding{deployment_sha256:ns.construction_parent_deployment_sha256.clone(),ownership_generation:ns.construction_parent_ownership_generation,next_global_id:ns.row_count}}).await.unwrap();assert_eq!(admitted.rows().len() as u64,ns.row_count);
    }
    write(Path::new(&input.output),&serde_json::to_vec(&serde_json::json!({"candidate":input.candidate,"parent":input.parent,"export":input.export,"rows":rows,"all_three_maps_and_vectors_verified":true})).unwrap());
}
