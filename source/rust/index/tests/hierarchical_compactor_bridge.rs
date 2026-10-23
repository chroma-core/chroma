#[path = "../../worker/src/execution/orchestration/hierarchical_register_bridge.rs"]
mod bridge;
use bridge::*;
use chroma_types::{Collection, SegmentFlushInfo, SegmentScope, SegmentType, test_segment};

#[test]
fn registration_preserves_context_and_other_segment_keys() {
    let mut collection=Collection::test_collection(4);
    collection.version=3; collection.log_position=10;
    let mut vector=test_segment(collection.collection_id,SegmentScope::VECTOR);
    vector.r#type=SegmentType::Spann;
    vector.file_path.insert(HIERARCHY_BUNDLE.into(),vec!["old".into()]);
    vector.file_path.insert("preserved".into(),vec!["other-root".into()]);
    let other=test_segment(collection.collection_id,SegmentScope::METADATA);
    let parent=vec![SegmentFlushInfo{segment_id:vector.id,file_paths:vector.file_path.clone()},SegmentFlushInfo{segment_id:other.id,file_paths:other.file_path.clone()}];
    let expected=CapturedContext{collection_id:collection.collection_id,tenant:collection.tenant.clone(),database:collection.database.clone(),collection_version:3,parent_log_position:10,pulled_log_offset:12,vector_segment_id:vector.id.0,parent_vector_files:vector.file_path.clone()};
    let reference=serde_json::json!({"path":"/owned/bundle.json","sha256":"a".repeat(64),"bytes":123}).to_string();
    let result=attach_descriptor(&collection,12,&vector,&parent,parent.clone(),&expected,reference.clone()).unwrap();
    assert_eq!(result[0].file_paths["preserved"],vec!["other-root"]);
    assert_eq!(result[0].file_paths[HIERARCHY_BUNDLE],vec![reference.clone()]);
    assert_eq!(result[1].file_paths,parent[1].file_paths);
    assert!(attach_descriptor(&collection,13,&vector,&parent,parent.clone(),&expected,reference.clone()).is_err());
    collection.version=4;
    assert!(attach_descriptor(&collection,12,&vector,&parent,parent.clone(),&expected,reference.clone()).is_err());
    collection.version=3;
    let mut duplicate=parent.clone();duplicate.push(parent[0].clone());
    assert!(attach_descriptor(&collection,12,&vector,&parent,duplicate,&expected,reference.clone()).is_err());
    let mut changed=parent.clone();changed[0].file_paths.insert("preserved".into(),vec!["replacement".into()]);
    assert!(attach_descriptor(&collection,12,&vector,&parent,changed,&expected,reference).is_err());
}
