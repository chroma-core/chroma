//! Compile the actual namespace adapter against actual record/materializer APIs.
//! No worker server or publication path is substituted by this focused target.
#[path = "../../worker/src/execution/orchestration/hierarchical_materialized_input.rs"]
mod hierarchical_materialized_input;
#[path = "../../worker/src/execution/orchestration/hierarchical_record_namespace.rs"]
mod hierarchical_record_namespace;

use chroma_segment::{blockfile_record::{RecordSegmentWriter,RecordSegmentReaderOptions},types::materialize_logs};
use chroma_types::{Chunk,Collection,DatabaseUuid,LogRecord,Operation,OperationRecord,SegmentScope};
use hierarchical_record_namespace::{Artifact,NamespaceRoot,NamespaceRow,SourceIdentity,audit_bound};
use sha2::{Digest,Sha256};
use std::{fs::{self,OpenOptions},io::Write,path::Path};
fn artifact(path:&Path,bytes:&[u8])->Artifact {
    let mut f=OpenOptions::new().create_new(true).write(true).open(path).unwrap();f.write_all(bytes).unwrap();f.sync_all().unwrap();
    Artifact{path:path.canonicalize().unwrap().to_str().unwrap().into(),bytes:bytes.len() as u64,sha256:format!("{:x}",Sha256::digest(bytes))}
}
fn provider(root:&Path)->chroma_blockstore::provider::BlockfileProvider {
    use chroma_blockstore::arrow::config::{BlockManagerConfig,TEST_MAX_BLOCK_SIZE_BYTES};
    chroma_blockstore::provider::BlockfileProvider::new_arrow(chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(root.to_str().unwrap())),TEST_MAX_BLOCK_SIZE_BYTES,
        chroma_cache::new_cache_for_test(),chroma_cache::new_cache_for_test(),BlockManagerConfig::default_num_concurrent_block_flushes())
}
fn log(offset:i64,user:&str,vector:Vec<f32>)->LogRecord {
    LogRecord{log_offset:offset,record:OperationRecord{id:user.into(),embedding:Some(vector),encoding:None,metadata:None,document:None,operation:Operation::Add}}
}
fn binding()->hierarchical_materialized_input::ParentOwnershipBinding {
    hierarchical_materialized_input::ParentOwnershipBinding{deployment_sha256:"a".repeat(64),ownership_generation:1,next_global_id:4}
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore="requires separately admitted bounded actual record audit fixture"]
async fn actual_permuted_parent_audit_and_distinct_namespace_append() {
    let root=std::path::PathBuf::from(std::env::var("SPANN_RECORD_NAMESPACE_EVIDENCE").unwrap());
    assert!(root.is_absolute());fs::create_dir(&root).unwrap();let store=root.join("record-store");fs::create_dir(&store).unwrap();
    let mut collection=Collection::test_collection(2);collection.version=3;collection.log_position=10;collection.total_records_post_compaction=4;
    let mut segment=chroma_types::test_segment(collection.collection_id,SegmentScope::RECORD);segment.r#type=chroma_types::SegmentType::BlockfileRecord;
    let database=DatabaseUuid::new();
    // Serialize the independent shared identity source BEFORE either proposed
    // mapping or record insertion. Two different users intentionally tie.
    let vectors=vec![vec![1f32,-0f32],vec![1f32,-0f32],vec![2f32,3f32],vec![4f32,5f32]];
    let source:Vec<_>=(0..4).map(|i|SourceIdentity{global_id:i,user_id:format!("user-{i}")}).collect();
    let identities=artifact(&root.join("shared-source-identities.json"),&serde_json::to_vec(&source).unwrap());
    let canonical=artifact(&root.join("canonical.f32"),&vectors.iter().flatten().flat_map(|v|v.to_le_bytes()).collect::<Vec<_>>());
    let permutation=[2usize,0,3,1];
    let input=Chunk::new(permutation.iter().enumerate().map(|(i,g)|log(7+i as i64,&format!("user-{g}"),vectors[*g].clone())).collect::<Vec<_>>().into());
    let materialized=materialize_logs(&None,input,None,&RecordSegmentReaderOptions::default()).await.unwrap();
    let p=provider(&store);let writer=RecordSegmentWriter::from_segment(&collection.tenant,&database,&segment,&p,None,None).await.unwrap();
    writer.apply_materialized_log_chunk(&None,&materialized).await.unwrap();segment.file_path=writer.commit().await.unwrap().flush().await.unwrap();drop(p);drop(materialized);
    let rows:Vec<_>=(0..4usize).map(|g|NamespaceRow{global_id:g as u64,record_offset_id:permutation.iter().position(|v|*v==g).unwrap() as u32+1,user_id:format!("user-{g}")}).collect();
    let mapping=artifact(&root.join("mapping.json"),&serde_json::to_vec(&rows).unwrap());
    let owner0=artifact(&root.join("owner0.u64"),&[0u64,2].iter().flat_map(|v|v.to_le_bytes()).collect::<Vec<_>>());
    let owner1=artifact(&root.join("owner1.u64"),&[1u64,3].iter().flat_map(|v|v.to_le_bytes()).collect::<Vec<_>>());
    let namespace=NamespaceRoot{format_version:1,collection:collection.clone(),record_segment_id:segment.id.to_string(),record_files:segment.file_path.clone(),parent_deployment_sha256:"a".repeat(64),ownership_generation:1,row_count:4,
        identity_source:identities,mapping,canonical_vectors:canonical,owner_maps:vec![owner0,owner1],parent_namespace_sha256:None};
    let root_artifact=artifact(&root.join("namespace-root.json"),&serde_json::to_vec(&namespace).unwrap());
    let p=provider(&store);let audited=audit_bound(&collection,&segment,&p,&root_artifact,binding()).await.unwrap();assert_eq!(audited.rows(),rows);
    let new_logs=Chunk::new(vec![log(11,"new-a",vec![6.,7.]),log(12,"new-b",vec![8.,9.])].into());
    let (_actual,captured)=audited.materialize_append(&collection,12,new_logs.clone()).await.unwrap();
    assert_eq!(captured.rows.iter().map(|r|(r.record_offset_id,r.global_id)).collect::<Vec<_>>(),vec![(5,4),(6,5)]);
    assert!(hierarchical_materialized_input::capture_add_new(&collection,12,&binding(),&new_logs,&_actual).await.is_err());
    let mut extended=rows.clone();extended.extend(captured.rows.iter().map(|r|NamespaceRow{global_id:r.global_id,record_offset_id:r.record_offset_id,user_id:r.user_id.clone()}));
    audited.validate_extension(&extended).unwrap();
    let mut changed_old=extended.clone();changed_old[0].record_offset_id=99;
    assert!(audited.validate_extension(&changed_old).is_err());
    let mut changed=collection.clone();changed.dimension=Some(3);
    assert!(audited.materialize_append(&changed,12,new_logs).await.is_err());
    let existing=Chunk::new(vec![log(11,"user-0",vectors[0].clone())].into());
    assert!(audited.materialize_append(&collection,11,existing).await.is_err());
    let mut swapped=rows.clone();swapped[0].user_id="user-1".into();swapped[1].user_id="user-0".into();
    let mut bad=namespace.clone();bad.mapping=artifact(&root.join("swapped-equal-vectors.json"),&serde_json::to_vec(&swapped).unwrap());
    let bad_root=artifact(&root.join("bad-namespace-root.json"),&serde_json::to_vec(&bad).unwrap());
    assert!(audit_bound(&collection,&segment,&p,&bad_root,binding()).await.is_err());
    // Replace only the reverse-map reference with a real flushed blockfile
    // containing an extra alias; count() and forward/data sets stay unchanged.
    let reverse=p.write::<&str,u32>(chroma_blockstore::BlockfileWriterOptions::new(segment.construct_prefix_path(&collection.tenant,&database))).await.unwrap();
    for row in &rows { reverse.set("",row.user_id.as_str(),row.record_offset_id).await.unwrap(); }
    reverse.set("","hidden-alias",1).await.unwrap();
    let flushed=reverse.commit::<&str,u32>().await.unwrap();let key=chroma_segment::types::ChromaSegmentFlusher::flush_key(flushed.prefix_path(),&flushed.id());flushed.flush::<&str,u32>().await.unwrap();
    let mut alias_segment=segment.clone();alias_segment.file_path.insert(chroma_types::USER_ID_TO_OFFSET_ID.into(),vec![key]);
    let mut alias_root=namespace.clone();alias_root.record_files=alias_segment.file_path.clone();
    let alias_artifact=artifact(&root.join("alias-namespace-root.json"),&serde_json::to_vec(&alias_root).unwrap());
    assert!(audit_bound(&collection,&alias_segment,&provider(&store),&alias_artifact,binding()).await.is_err());
    let receipt=serde_json::json!({"scope":"constructed actual record-store audit/materializer component, no hierarchical writer/publication","namespace_root":root_artifact,
        "rows":rows,"new_rows":captured.rows,"equal_vector_swap_rejected":true,"actual_extra_reverse_alias_rejected":true,"changed_dimension_rejected":true,"existing_user_rejected":true,"record_segment_files":segment.file_path});
    artifact(&root.join("receipt.json"),&serde_json::to_vec_pretty(&receipt).unwrap());
}
