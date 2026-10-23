//! Real retained record-segment gate. No hierarchical writer is called: its
//! dense global namespace deliberately rejects the one-based record offsets.
#[path = "../../worker/src/execution/orchestration/hierarchical_materialized_input.rs"]
mod adapter;
use adapter::{capture_add_new, private_add_allocator, ParentOwnershipBinding};
use chroma_segment::{blockfile_record::{RecordSegmentReader,RecordSegmentWriter,RecordSegmentReaderOptions},types::materialize_logs};
use chroma_types::{Chunk,Collection,DatabaseUuid,LogRecord,Operation,OperationRecord,SegmentScope};
use sha2::{Digest,Sha256};
use std::{collections::BTreeMap,fs::{self,File,OpenOptions},io::{Read,Write},path::{Path,PathBuf},sync::atomic::Ordering};

fn digest(bytes:&[u8])->String {format!("{:x}",Sha256::digest(bytes))}
fn read_bounded(path:&Path,maximum:usize)->Vec<u8> {let mut bytes=Vec::new();File::open(path).unwrap().take(maximum as u64+1).read_to_end(&mut bytes).unwrap();assert!(bytes.len()<=maximum);bytes}
fn inventory(root:&Path)->BTreeMap<String,serde_json::Value> {
    fn visit(root:&Path,path:&Path,result:&mut BTreeMap<String,serde_json::Value>) {
        for entry in fs::read_dir(path).unwrap() {let path=entry.unwrap().path();assert!(!path.is_symlink());if path.is_dir() {visit(root,&path,result);} else {let bytes=read_bounded(&path,8*1024*1024);result.insert(path.strip_prefix(root).unwrap().to_str().unwrap().into(),serde_json::json!({"bytes":bytes.len(),"sha256":digest(&bytes)}));}}
    }
    let mut result=BTreeMap::new();visit(root,root,&mut result);result
}
fn raw(log_offset:i64,user:&str,embedding:Option<Vec<f32>>,operation:Operation)->LogRecord {
    LogRecord {log_offset,record:OperationRecord {id:user.into(),embedding,encoding:None,metadata:None,document:None,operation}}
}
fn provider(root:&Path)->chroma_blockstore::provider::BlockfileProvider {
    use chroma_blockstore::arrow::config::{BlockManagerConfig,TEST_MAX_BLOCK_SIZE_BYTES};
    chroma_blockstore::provider::BlockfileProvider::new_arrow(
        chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(root.to_str().unwrap())),
        TEST_MAX_BLOCK_SIZE_BYTES,chroma_cache::new_cache_for_test(),chroma_cache::new_cache_for_test(),
        BlockManagerConfig::default_num_concurrent_block_flushes())
}

#[tokio::test(flavor="multi_thread",worker_threads=2)]
#[ignore="requires an admitted fresh local evidence root and exact parent descriptor"]
async fn retained_parent_record_namespace_and_existing_users() {
    let descriptor_path=PathBuf::from(std::env::var("SPANN_PARENT_RECORD_DESCRIPTOR").expect("pinned descriptor required"));
    let expected=std::env::var("SPANN_PARENT_RECORD_DESCRIPTOR_SHA").expect("exact descriptor SHA required");
    let semantic_sha=std::env::var("SPANN_PARENT_RECORD_DEPLOYMENT_SHA").expect("validated semantic parent hash required");
    assert!(semantic_sha.len()==64 && semantic_sha.bytes().all(|b|b.is_ascii_hexdigit() && !b.is_ascii_uppercase()));
    let bytes=read_bounded(&descriptor_path,2*1024*1024);assert_eq!(digest(&bytes),expected);
    let parent:serde_json::Value=serde_json::from_slice(&bytes).unwrap();
    assert_eq!(parent["tree"]["source"]["row_count"],16);assert_eq!(parent["tree"]["dimension"],4);assert_eq!(parent["tree"]["metric"],"l2");
    let canonical=&parent["canonical_vectors"];
    let vector_bytes=read_bounded(Path::new(canonical["path"].as_str().unwrap()),256);
    assert_eq!(vector_bytes.len(),256);assert_eq!(canonical["bytes"],256);assert_eq!(digest(&vector_bytes),canonical["sha256"].as_str().unwrap());
    let vectors:Vec<Vec<f32>>=vector_bytes.chunks_exact(16).map(|row|row.chunks_exact(4).map(|v|f32::from_le_bytes(v.try_into().unwrap())).collect()).collect();
    assert!(vectors.iter().flatten().all(|v|v.is_finite()));
    let root=PathBuf::from(std::env::var("SPANN_PARENT_RECORD_EVIDENCE").expect("fresh evidence root required"));
    assert!(root.is_absolute());fs::create_dir(&root).unwrap();let storage_root=root.join("record-store");fs::create_dir(&storage_root).unwrap();
    let mut collection=Collection::test_collection(4);collection.version=3;collection.log_position=100;collection.total_records_post_compaction=16;
    let mut segment=chroma_types::test_segment(collection.collection_id,SegmentScope::RECORD);
    segment.r#type=chroma_types::SegmentType::BlockfileRecord;
    let database=DatabaseUuid::new();let options=RecordSegmentReaderOptions::default();
    let initial_logs=Chunk::new(vectors.iter().enumerate().map(|(i,v)|raw(85+i as i64,&format!("parent-{i}"),Some(v.clone()),Operation::Add)).collect::<Vec<_>>().into());
    // Use the production default: an absent record reader starts at offset 1.
    let initial=materialize_logs(&None,initial_logs,None,&options).await.unwrap();
    assert_eq!(initial.iter().map(|r|r.get_offset_id()).collect::<Vec<_>>(),(1..=16).collect::<Vec<_>>());
    let writer_provider=provider(&storage_root);
    let writer=RecordSegmentWriter::from_segment(&collection.tenant,&database,&segment,&writer_provider,None,None).await.unwrap();
    writer.apply_materialized_log_chunk(&None,&initial).await.unwrap();
    segment.file_path=writer.commit().await.unwrap().flush().await.unwrap();
    drop(initial);drop(writer_provider);
    // Reopen through a fresh provider; do not establish parent presence from
    // a vector count, an unflushed writer, or the writer's provider cache.
    let reopened_provider=provider(&storage_root);
    let reader=RecordSegmentReader::from_segment(&segment,&reopened_provider,None).await.unwrap();
    assert_eq!(reader.count().await.unwrap(),16);assert_eq!(reader.get_max_offset_id(),16);
    let mut checked_parent=Vec::new();
    for (i,expected_vector) in vectors.iter().enumerate() {
        let user=format!("parent-{i}");let id=reader.get_offset_id_for_user_id(&user,&options).await.unwrap().unwrap();assert_eq!(id,i as u32+1);
        let record=reader.get_data_for_offset_id(id).await.unwrap().unwrap();assert_eq!(record.id,user);
        let actual_bits:Vec<u32>=record.embedding.iter().map(|v|v.to_bits()).collect();assert_eq!(actual_bits,expected_vector.iter().map(|v|v.to_bits()).collect::<Vec<_>>());
        checked_parent.push(serde_json::json!({"observed_global_row":i,"record_offset_id":id,"user_id":user,"embedding_bits":actual_bits}));
    }
    assert_eq!(reader.get_offset_id_for_user_id("new-a",&options).await.unwrap(),None);
    let baseline=inventory(&storage_root);
    let binding=ParentOwnershipBinding {deployment_sha256:semantic_sha.clone(),ownership_generation:parent["fence"].as_u64().unwrap(),next_global_id:16};
    let mut outcomes=Vec::new();
    let cases=vec![
        ("new-users-one-based-namespace",vec![raw(101,"new-a",Some(vec![-0.,1.,0.,0.]),Operation::Add),raw(102,"new-b",Some(vec![17.,1.,0.,0.]),Operation::Add)]),
        ("existing-add-no-op",vec![raw(101,"parent-0",Some(vec![999.,1.,0.,0.]),Operation::Add)]),
        ("existing-add-plus-new",vec![raw(101,"parent-0",Some(vec![999.,1.,0.,0.]),Operation::Add),raw(102,"new-a",Some(vec![17.,1.,0.,0.]),Operation::Add)]),
        ("duplicate-new-user",vec![raw(101,"new-a",Some(vec![17.,1.,0.,0.]),Operation::Add),raw(102,"new-a",Some(vec![18.,1.,0.,0.]),Operation::Add)]),
        ("folded-new-update",vec![raw(101,"new-a",Some(vec![17.,1.,0.,0.]),Operation::Add),raw(102,"new-a",Some(vec![18.,1.,0.,0.]),Operation::Update)]),
        ("existing-update",vec![raw(101,"parent-0",Some(vec![999.,1.,0.,0.]),Operation::Update)]),
        ("existing-delete",vec![raw(101,"parent-0",None,Operation::Delete)]),
    ];
    for (name,rows) in cases {
        let count=rows.len();let logs=Chunk::new(rows.into());let allocator=private_add_allocator(Some(&reader),count).unwrap();assert_eq!(allocator.load(Ordering::SeqCst),17);
        let result=materialize_logs(&Some(reader.clone()),logs.clone(),Some(allocator.clone()),&options).await.unwrap();
        let ids=result.iter().map(|r|r.get_offset_id()).collect::<Vec<_>>();
        if name=="new-users-one-based-namespace" {assert_eq!(ids,vec![17,18]);}
        if name=="existing-add-no-op" {assert_eq!(result.len(),0);assert_eq!(allocator.load(Ordering::SeqCst),17);}
        if name=="existing-add-plus-new" {assert_eq!(result.len(),1);assert_eq!(allocator.load(Ordering::SeqCst),18);}
        let raw_rows=logs.iter().map(|(r,_)|serde_json::json!({"log_offset":r.log_offset,"user_id":r.record.id,"operation":format!("{:?}",r.record.operation),"embedding_bits":r.record.embedding.as_ref().map(|v|v.iter().map(|v|v.to_bits()).collect::<Vec<_>>())})).collect::<Vec<_>>();
        let mut materialized_rows=Vec::new();
        for row in &result {
            materialized_rows.push(serde_json::json!({"record_offset_id":row.get_offset_id(),"user_id":row.get_user_id(Some(&reader)).await.unwrap(),"operation":format!("{:?}",row.get_operation()),"embedding_bits":row.embeddings_ref_from_log().map(|v|v.iter().map(|v|v.to_bits()).collect::<Vec<_>>())}));
        }
        let rejected=capture_add_new(&collection,100+count as i64,&binding,&logs,&result).await.unwrap_err();
        if name=="new-users-one-based-namespace" {assert!(rejected.contains("dense parent namespace"));}
        assert!(!root.join("hierarchical-private").exists());
        outcomes.push(serde_json::json!({"case":name,"raw_count":count,"raw_rows":raw_rows,"materialized_count":result.len(),"materialized_rows":materialized_rows,"actual_record_offsets":ids,"allocator_before":17,"allocator_after":allocator.load(Ordering::SeqCst),"rejected":rejected,"hierarchical_writer_called":false}));
    }
    assert_eq!(inventory(&storage_root),baseline);
    assert_eq!(digest(&read_bounded(&descriptor_path,2*1024*1024)),expected);
    assert_eq!(digest(&read_bounded(Path::new(canonical["path"].as_str().unwrap()),256)),canonical["sha256"].as_str().unwrap());
    let receipt=serde_json::json!({"scope":"real-parent-record-and-namespace-gate-only","parent_descriptor":{"path":descriptor_path,"bytes":bytes.len(),"sha256":expected},"parent_semantic_deployment_sha256":semantic_sha,"canonical":canonical,"collection_id":collection.collection_id,"database_id":database.0.to_string(),"parent_log_position":100,"record_segment":{"segment_id":segment.id.to_string(),"collection_id":segment.collection,"type":"urn:chroma:segment/record/blockfile","scope":"RECORD","file_paths":segment.file_path},"record_max_offset":16,"default_next_record_offset":17,"hierarchical_next_global_id":16,"parent_records":checked_parent,"cases":outcomes,"parent_store_inventory":baseline,"parent_store_unchanged":true,"saved_leaf_positive_blocked":true,"reason":"explicit audited record/global namespace bridge required; no remap performed"});
    let mut f=OpenOptions::new().create_new(true).write(true).open(root.join("receipt.json")).unwrap();f.write_all(&serde_json::to_vec_pretty(&receipt).unwrap()).unwrap();f.sync_all().unwrap();File::open(&root).unwrap().sync_all().unwrap();
}
