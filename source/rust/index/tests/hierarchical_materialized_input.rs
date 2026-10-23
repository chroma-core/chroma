//! Actual materializer to typed input; no lower writer or publication is run.
#[path = "../../worker/src/execution/orchestration/hierarchical_materialized_input.rs"]
mod adapter;
use adapter::*;
use chroma_segment::{blockfile_record::RecordSegmentReaderOptions, types::materialize_logs};
use chroma_types::{Chunk, Collection, LogRecord, Operation, OperationRecord, ScalarEncoding};
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};

fn row(offset:i64, user:&str, first:f32)->LogRecord {
    LogRecord {log_offset:offset,record:OperationRecord {id:user.into(),embedding:Some(vec![first,1.,0.,0.]),encoding:Some(ScalarEncoding::FLOAT32),metadata:None,document:None,operation:Operation::Add}}
}
fn collection()->Collection {
    let mut c=Collection::test_collection(4);c.version=3;c.log_position=10;c.total_records_post_compaction=16;c
}
fn binding()->ParentOwnershipBinding {ParentOwnershipBinding {deployment_sha256:"a".repeat(64),ownership_generation:1,next_global_id:16}}
fn chunk(rows:Vec<LogRecord>)->Chunk<LogRecord> {Chunk::new(rows.into())}

#[tokio::test]
async fn exact_materializer_ids_user_join_and_vector_bits() {
    let logs=chunk(vec![row(11,"z-user",-0.0),row(12,"a-user",2.)]);
    let allocator=Arc::new(AtomicU32::new(16));
    let materialized=materialize_logs(&None,logs.clone(),Some(allocator.clone()),&RecordSegmentReaderOptions::default()).await.unwrap();
    let captured=collection();
    let output=capture_add_new(&captured,12,&binding(),&logs,&materialized).await.unwrap();
    assert_eq!(allocator.load(Ordering::SeqCst),18);
    assert_eq!(output.collection_id,captured.collection_id);
    assert_eq!(output.rows.iter().map(|r|(r.global_id,r.record_offset_id,r.log_offset,r.user_id.as_str())).collect::<Vec<_>>(),vec![(16,16,11,"z-user"),(17,17,12,"a-user")]);
    assert_eq!(output.rows[0].embedding_bits[0],(-0.0f32).to_bits());
    assert_eq!(output.parent_deployment_sha256,"a".repeat(64));
    assert_eq!(output.ownership_generation,1);
}

#[tokio::test]
async fn unsupported_raw_payloads_and_folded_operations_reject() {
    let good=vec![row(11,"z-user",1.),row(12,"a-user",2.)];
    let materialized=materialize_logs(&None,chunk(good.clone()),Some(Arc::new(AtomicU32::new(16))),&RecordSegmentReaderOptions::default()).await.unwrap();
    for op in [Operation::Update,Operation::Upsert,Operation::Delete,Operation::BackfillFn] {
        let mut bad=good.clone();bad[1].record.operation=op;
        assert!(capture_add_new(&collection(),12,&binding(),&chunk(bad),&materialized).await.is_err());
    }
    let mut cases=Vec::new();
    let mut rows=good.clone();rows[0].record.metadata=Some(Default::default());cases.push(rows);
    let mut rows=good.clone();rows[0].record.document=Some("not silently dropped".into());cases.push(rows);
    let mut rows=good.clone();rows[0].record.embedding=None;cases.push(rows);
    let mut rows=good.clone();rows[0].record.embedding=Some(vec![1.]);cases.push(rows);
    let mut rows=good.clone();rows[0].record.embedding.as_mut().unwrap()[0]=f32::INFINITY;cases.push(rows);
    let mut rows=good.clone();rows[0].record.encoding=Some(ScalarEncoding::INT32);cases.push(rows);
    let mut rows=good.clone();rows[0].log_offset=12;cases.push(rows);
    let mut rows=good.clone();rows[1].record.id=rows[0].record.id.clone();cases.push(rows);
    for rows in cases {assert!(capture_add_new(&collection(),12,&binding(),&chunk(rows),&materialized).await.is_err());}
    let mut folded=vec![row(11,"same",1.),row(12,"same",2.)];folded[1].record.operation=Operation::Update;
    let logs=chunk(folded);
    let result=materialize_logs(&None,logs.clone(),Some(Arc::new(AtomicU32::new(16))),&RecordSegmentReaderOptions::default()).await.unwrap();
    assert_eq!(result.len(),1);
    assert!(capture_add_new(&collection(),12,&binding(),&logs,&result).await.is_err());
}

#[tokio::test]
async fn namespace_bits_visibility_and_overflow_fail_closed() {
    let good=chunk(vec![row(11,"z-user",-0.0),row(12,"a-user",2.)]);
    let allocator=Arc::new(AtomicU32::new(17));
    let wrong=materialize_logs(&None,good.clone(),Some(allocator.clone()),&RecordSegmentReaderOptions::default()).await.unwrap();
    assert!(capture_add_new(&collection(),12,&binding(),&good,&wrong).await.is_err());
    // Rejection does not roll back IDs already consumed by materialization.
    assert_eq!(allocator.load(Ordering::SeqCst),19);
    let result=materialize_logs(&None,good.clone(),Some(Arc::new(AtomicU32::new(16))),&RecordSegmentReaderOptions::default()).await.unwrap();
    let changed=chunk(vec![row(11,"z-user",0.0),row(12,"a-user",2.)]);
    assert!(capture_add_new(&collection(),12,&binding(),&changed,&result).await.is_err());
    let mut hidden=chunk(vec![row(11,"z-user",-0.0),row(12,"a-user",2.),row(13,"hidden",3.)]);
    hidden.set_visibility(vec![true,true,false]);
    assert!(capture_add_new(&collection(),13,&binding(),&hidden,&result).await.is_err());
    let mut overflow=binding();overflow.next_global_id=u64::MAX;
    assert!(capture_add_new(&collection(),12,&overflow,&good,&result).await.is_err());
    assert!(capture_add_new(&collection(),i64::MAX,&binding(),&good,&result).await.is_err());
    let mut bad_parent=binding();bad_parent.deployment_sha256="caller-label".into();
    assert!(capture_add_new(&collection(),12,&bad_parent,&good,&result).await.is_err());
}

#[tokio::test]
async fn owned_source_payload_offsets_and_byte_budget_reject_before_export() {
    let raw=chunk(vec![row(11,"z-user",1.),row(12,"a-user",2.)]);
    let mut variants=Vec::new();
    let mut rows=vec![row(11,"z-user",1.),row(12,"a-user",2.)];rows[0].record.metadata=Some(Default::default());variants.push(rows);
    let mut rows=vec![row(11,"z-user",1.),row(12,"a-user",2.)];rows[0].record.document=Some(String::new());variants.push(rows);
    variants.push(vec![row(21,"z-user",1.),row(22,"a-user",2.)]);
    for rows in variants {
        let materialized=materialize_logs(&None,chunk(rows),Some(Arc::new(AtomicU32::new(16))),&RecordSegmentReaderOptions::default()).await.unwrap();
        assert!(capture_add_new(&collection(),12,&binding(),&raw,&materialized).await.is_err());
    }
    let materialized=materialize_logs(&None,raw.clone(),Some(Arc::new(AtomicU32::new(16))),&RecordSegmentReaderOptions::default()).await.unwrap();
    let mut huge_dimension=collection();huge_dimension.dimension=Some(i32::MAX);
    assert!(capture_add_new(&huge_dimension,12,&binding(),&raw,&materialized).await.unwrap_err().contains("byte budget"));
}

/// This ignored method runs only as the reviewed two-row export stage. Its
/// source is the actual materializer, not a fabricated materialized JSON row.
#[tokio::test]
#[ignore = "requires an admitted fresh fixture input/output path"]
async fn export_admitted_materialized_source_fixture() {
    use serde::Deserialize;
    use sha2::{Digest, Sha256};
    use std::{fs::{File,OpenOptions},io::{Read,Write},path::Path};
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct InputRow {log_offset:i64,user_id:String,embedding_bits:Vec<u32>}
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {collection_id:chroma_types::CollectionUuid,collection_version:i32,parent_log_position:i64,pulled_log_offset:i64,dimension:i32,parent_deployment_sha256:String,ownership_generation:u64,next_global_id:u64,rows:Vec<InputRow>}
    fn sha(bytes:&[u8])->String {format!("{:x}",Sha256::digest(bytes))}
    fn file_ref(path:&Path)->serde_json::Value {
        let mut file=File::open(path).unwrap();let mut digest=Sha256::new();let mut bytes=0u64;let mut buffer=[0u8;65536];
        loop {let n=file.read(&mut buffer).unwrap();if n==0 {break;}digest.update(&buffer[..n]);bytes+=n as u64;assert!(bytes<=256*1024*1024);}
        serde_json::json!({"path":path.to_str().unwrap(),"bytes":bytes,"sha256":format!("{:x}",digest.finalize())})
    }
    fn write_new(path:&Path,bytes:&[u8]) {let mut f=OpenOptions::new().create_new(true).write(true).open(path).unwrap();f.write_all(bytes).unwrap();f.sync_all().unwrap();}
    let input_path=std::path::PathBuf::from(std::env::var("SPANN_MATERIALIZER_INPUT").expect("admitted input required"));
    let output_path=std::path::PathBuf::from(std::env::var("SPANN_MATERIALIZER_OUTPUT").expect("admitted output required"));
    assert!(input_path.is_absolute() && output_path.is_absolute());
    let mut bytes=Vec::new();File::open(&input_path).unwrap().take(65537).read_to_end(&mut bytes).unwrap();assert!(bytes.len()<=65536);
    let fixture:Input=serde_json::from_slice(&bytes).unwrap();assert_eq!(fixture.rows.len(),2);assert_eq!(fixture.dimension,4);
    let mut captured=collection();captured.collection_id=fixture.collection_id;captured.version=fixture.collection_version;captured.log_position=fixture.parent_log_position;captured.dimension=Some(fixture.dimension);
    let logs=chunk(fixture.rows.into_iter().map(|r|LogRecord {log_offset:r.log_offset,record:OperationRecord {id:r.user_id,embedding:Some(r.embedding_bits.into_iter().map(f32::from_bits).collect()),encoding:Some(ScalarEncoding::FLOAT32),metadata:None,document:None,operation:Operation::Add}}).collect());
    let result=materialize_logs(&None,logs.clone(),Some(Arc::new(AtomicU32::new(u32::try_from(fixture.next_global_id).unwrap()))),&RecordSegmentReaderOptions::default()).await.unwrap();
    let binding=ParentOwnershipBinding {deployment_sha256:fixture.parent_deployment_sha256,ownership_generation:fixture.ownership_generation,next_global_id:fixture.next_global_id};
    let captured=capture_add_new(&captured,fixture.pulled_log_offset,&binding,&logs,&result).await.unwrap();
    let encoded=serde_json::to_vec(&captured).unwrap();write_new(&output_path,&encoded);
    let proof=serde_json::json!({"format_version":1,"export":file_ref(&output_path),"fixture_input":file_ref(&input_path),"runtime_binary":file_ref(&std::env::current_exe().unwrap()),"producer_sources":{
        "rust/segment/src/types.rs":sha(include_bytes!("../../segment/src/types.rs")),
        "rust/worker/src/execution/orchestration/hierarchical_materialized_input.rs":sha(include_bytes!("../../worker/src/execution/orchestration/hierarchical_materialized_input.rs")),
        "rust/index/tests/hierarchical_materialized_input.rs":sha(include_bytes!("hierarchical_materialized_input.rs"))}});
    write_new(&output_path.with_extension("receipt.json"),&serde_json::to_vec(&proof).unwrap());
    File::open(output_path.parent().unwrap()).unwrap().sync_all().unwrap();
    println!("materialized_export_rows={}",captured.rows.len());
}
