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
