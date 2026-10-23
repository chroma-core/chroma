//! Bounded, opt-in record-offset/global-ID consistency audit.
//! The caller must obtain the source and parent bindings from its captured
//! registered descriptor. This module validates correspondence, not authority.
use super::hierarchical_materialized_input::{capture_correspondence, CapturedMaterializedInput, ParentOwnershipBinding};
use chroma_blockstore::provider::BlockfileProvider;
use chroma_segment::{blockfile_record::{RecordSegmentReader, RecordSegmentReaderOptions}, types::MaterializeLogsResult};
use chroma_types::{Chunk, Collection, LogRecord, Segment};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceRow {
    pub global_id: u64,
    pub record_offset_id: u32,
    pub user_id: String,
}

/// The shared input assigns user identity independently of record insertion
/// order. Its digest must come from the captured source descriptor, not the
/// proposed namespace file. Equal vectors do not establish user identity.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceIdentity {
    pub global_id: u64,
    pub user_id: String,
}

/// Every artifact is read and hashed again during each admission. A root hash
/// is an identity supplied by the captured registered bundle, not an immutable
/// provider receipt or permission to skip later validation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Artifact {
    pub path: String,
    pub bytes: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NamespaceRoot {
    pub format_version: u32,
    pub collection: serde_json::Value,
    pub record_segment_id: String,
    pub record_files: std::collections::HashMap<String,Vec<String>>,
    pub construction_parent_deployment_sha256: String,
    pub construction_parent_ownership_generation: u64,
    pub row_count: u64,
    pub identity_source: Artifact,
    pub mapping: Artifact,
    pub canonical_vectors: Artifact,
    pub owner_maps: Vec<Artifact>,
    pub parent_namespace_sha256: Option<String>,
}

/// Preserve serialized configuration and every context field that Collection's
/// public wire format deliberately skips. No field is defaulted by a roundtrip.
pub fn collection_binding(collection:&Collection)->Result<serde_json::Value,String> {
    #[derive(Serialize)]
    struct Binding<'a> {
        wire: &'a Collection,
        total_records_post_compaction:u64,
        size_bytes_post_compaction:u64,
        last_compaction_time_secs:u64,
        version_file_path:&'a Option<String>,
        root_collection_id:&'a Option<chroma_types::CollectionUuid>,
        lineage_file_path:&'a Option<String>,
        updated_at:std::time::SystemTime,
        database_id:chroma_types::DatabaseUuid,
        compaction_failure_count:i32,
    }
    serde_json::to_value(Binding{wire:collection,total_records_post_compaction:collection.total_records_post_compaction,
        size_bytes_post_compaction:collection.size_bytes_post_compaction,last_compaction_time_secs:collection.last_compaction_time_secs,
        version_file_path:&collection.version_file_path,root_collection_id:&collection.root_collection_id,
        lineage_file_path:&collection.lineage_file_path,updated_at:collection.updated_at,database_id:collection.database_id,
        compaction_failure_count:collection.compaction_failure_count}).map_err(|e|e.to_string())
}

fn sha(value:&str)->bool {value.len()==64 && value.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))}
fn read_artifact(artifact:&Artifact, limit:usize)->Result<Vec<u8>,String> {
    use std::io::Read;
    let path=std::path::Path::new(&artifact.path);
    if !path.is_absolute() || path.components().any(|c|matches!(c,std::path::Component::ParentDir))
        || !sha(&artifact.sha256) || artifact.bytes>limit as u64 {
        return Err("invalid bounded artifact descriptor".into());
    }
    let canonical=path.canonicalize().map_err(|e|e.to_string())?;
    if canonical!=path || !path.is_file() { return Err("canonical regular artifact path required".into()); }
    let file=std::fs::File::open(path).map_err(|e|e.to_string())?;
    let mut bytes=Vec::new();file.take(artifact.bytes+1).read_to_end(&mut bytes).map_err(|e|e.to_string())?;
    if bytes.len() as u64!=artifact.bytes || digest(&bytes)!=artifact.sha256 {return Err("artifact bytes/hash differ".into());}
    Ok(bytes)
}

/// A construction-only identity binding supplied by the internal builder.
/// This is not a current registered-deployment view. Reopening a published
/// namespace requires a separate captured complete-bundle resolver; that path
/// is intentionally not implemented or admitted here.
pub struct ConstructionNamespaceContext {
    pub root: Artifact,
    pub parent: ParentOwnershipBinding,
}

/// Fully check the namespace built against this exact construction parent.
/// The construction parent identity must not be substituted for the current
/// enclosing deployment when an already published namespace is reused.
pub async fn audit_construction<'a>(collection:&Collection,segment:&Segment,provider:&BlockfileProvider,
    construction:&ConstructionNamespaceContext)->Result<AuditedNamespace<'a>,String> {
    let root_bytes=read_artifact(&construction.root,2*1024*1024)?;
    let captured_parent=&construction.parent;
    let root:NamespaceRoot=serde_json::from_slice(&root_bytes).map_err(|e|e.to_string())?;
    if root.format_version!=1 || root.collection!=collection_binding(collection)? || root.record_segment_id!=segment.id.to_string()
        || root.record_files!=segment.file_path || root.row_count>100_000 || root.owner_maps.len()>128
        || !sha(&root.construction_parent_deployment_sha256) || root.construction_parent_ownership_generation==0
        || !sha(&captured_parent.deployment_sha256) || captured_parent.ownership_generation==0
        || captured_parent.next_global_id!=root.row_count
        || captured_parent.deployment_sha256!=root.construction_parent_deployment_sha256
        || captured_parent.ownership_generation!=root.construction_parent_ownership_generation
        || root.parent_namespace_sha256.as_ref().is_some_and(|s|!sha(s)) {
        return Err("registered namespace context differs".into());
    }
    let identity=read_artifact(&root.identity_source,16*1024*1024)?;
    let mapping=read_artifact(&root.mapping,24*1024*1024)?;
    let dimension=collection.dimension.filter(|d|*d>0).ok_or("positive dimension required")? as usize;
    let expected=(root.row_count as usize).checked_mul(dimension).and_then(|n|n.checked_mul(4)).ok_or("vector bytes overflow")?;
    if expected>512*1024*1024 || root.canonical_vectors.bytes!=expected as u64 {return Err("bounded canonical shape differs".into());}
    let canonical=read_artifact(&root.canonical_vectors,expected)?;
    let mut owner_maps=Vec::new();let mut owner_bytes=0usize;
    for artifact in &root.owner_maps {
        owner_bytes=owner_bytes.checked_add(usize::try_from(artifact.bytes).map_err(|_|"map bytes overflow")?).ok_or("map bytes overflow")?;
        if owner_bytes>800_000 {return Err("aggregate map bytes exceed prototype limit".into());}
        owner_maps.push(read_artifact(artifact,800_000)?);
    }
    audit(collection,segment,provider,captured_parent.clone(),&identity,&root.identity_source.sha256,&mapping,&canonical,&owner_maps).await
}

pub struct AuditedNamespace<'a> {
    reader: RecordSegmentReader<'a>,
    rows: Vec<NamespaceRow>,
    parent: ParentOwnershipBinding,
    collection: Collection,
    record_files: std::collections::HashMap<String, Vec<String>>,
}

fn digest(bytes: &[u8]) -> String { format!("{:x}", Sha256::digest(bytes)) }

fn validate_identity(source: &[SourceIdentity], rows: &[NamespaceRow]) -> Result<(BTreeSet<String>, BTreeMap<u32,usize>),String> {
    if source.len()!=rows.len() || source.len()>100_000 { return Err("bounded complete identity table required".into()); }
    let mut users = BTreeSet::new();
    let mut offsets = BTreeMap::new();
    for (i, (identity, row)) in source.iter().zip(rows.iter()).enumerate() {
        if identity.global_id != i as u64 || row.global_id != i as u64
            || identity.user_id != row.user_id || row.user_id.is_empty()
            || row.record_offset_id == 0 || !users.insert(row.user_id.clone())
            || offsets.insert(row.record_offset_id, i).is_some()
        { return Err("source identity or namespace bijection differs".into()); }
    }
    Ok((users,offsets))
}

/// Validate bounded serialized inputs before opening the exact record segment.
/// Canonical vectors and owner maps are supplied as owned, already descriptor-
/// checked bytes by the internal caller; this does not create a trust receipt.
async fn audit<'a>(
    collection: &Collection,
    segment: &Segment,
    provider: &BlockfileProvider,
    parent: ParentOwnershipBinding,
    identity_bytes: &[u8],
    expected_identity_sha256: &str,
    mapping_bytes: &[u8],
    canonical_vectors: &[u8],
    owner_maps: &[Vec<u8>],
) -> Result<AuditedNamespace<'a>, String> {
    if identity_bytes.len() > 16*1024*1024 || mapping_bytes.len() > 24*1024*1024
        || digest(identity_bytes) != expected_identity_sha256
        || segment.collection != collection.collection_id
        || segment.scope != chroma_types::SegmentScope::RECORD
        || segment.r#type != chroma_types::SegmentType::BlockfileRecord
        || parent.next_global_id > 100_000 || parent.ownership_generation == 0
        || owner_maps.len()>128
        || parent.deployment_sha256.len() != 64
    { return Err("exact bounded parent/source binding required".into()); }
    let source: Vec<SourceIdentity> = serde_json::from_slice(identity_bytes).map_err(|e| e.to_string())?;
    let rows: Vec<NamespaceRow> = serde_json::from_slice(mapping_bytes).map_err(|e| e.to_string())?;
    let n = usize::try_from(parent.next_global_id).map_err(|_| "row count overflow")?;
    let dimension = collection.dimension.filter(|d| *d > 0).ok_or("positive dimension required")? as usize;
    let bytes = n.checked_mul(dimension).and_then(|v| v.checked_mul(4)).ok_or("vector bytes overflow")?;
    if bytes > 512*1024*1024 || canonical_vectors.len() != bytes || source.len() != n || rows.len() != n {
        return Err("complete bounded source/table/vector coverage required".into());
    }
    let (users, offsets) = validate_identity(&source, &rows)?;
    let mut seen_global = vec![false; n];
    for map in owner_maps {
        if map.len()%8 != 0 || map.len() > 800_000 { return Err("invalid owner map bytes".into()); }
        for value in map.chunks_exact(8) {
            let id = u64::from_le_bytes(value.try_into().unwrap());
            let slot = usize::try_from(id).ok().filter(|i| *i<n).ok_or("owner global ID out of range")?;
            if std::mem::replace(&mut seen_global[slot], true) { return Err("duplicate owner global ID".into()); }
        }
    }
    if seen_global.iter().any(|v| !v) { return Err("missing owner global ID".into()); }
    let reader = RecordSegmentReader::from_segment(segment, provider, None).await.map_err(|e| e.to_string())?;
    if reader.count().await.map_err(|e| e.to_string())? != n { return Err("record count differs".into()); }
    let expected_offsets: BTreeSet<u32> = offsets.keys().copied().collect();
    {
        let stream=reader.get_offset_stream(..);
        futures::pin_mut!(stream);
        let mut seen=BTreeSet::new();
        while let Some(offset)=stream.try_next().await.map_err(|e|e.to_string())? {
            if seen.len()>=n || !expected_offsets.contains(&offset) || !seen.insert(offset) {
                return Err("record offset enumeration differs".into());
            }
        }
        if seen!=expected_offsets { return Err("record offset coverage differs".into()); }
    }
    {
        let stream = reader.get_data_stream(..).await;
        futures::pin_mut!(stream);
        let mut seen = BTreeSet::new();
        while let Some((offset,data)) = stream.try_next().await.map_err(|e| e.to_string())? {
            if seen.len()>=n { return Err("extra record data row".into()); }
            let i = *offsets.get(&offset).ok_or("unmapped record data")?;
            if !seen.insert(offset) || data.id != rows[i].user_id || data.embedding.len()!=dimension {
                return Err("record identity/data shape differs".into());
            }
            let vector = &canonical_vectors[i*dimension*4..(i+1)*dimension*4];
            for (value,bits) in data.embedding.iter().zip(vector.chunks_exact(4)) {
                if !value.is_finite() || value.to_bits()!=u32::from_le_bytes(bits.try_into().unwrap()) {
                    return Err("canonical record vector bits differ".into());
                }
            }
        }
        if seen != expected_offsets { return Err("record data coverage differs".into()); }
    }
    {
        let stream = reader.get_user_offset_stream();
        futures::pin_mut!(stream);
        let mut seen = BTreeSet::new();
        while let Some((user,offset)) = stream.try_next().await.map_err(|e| e.to_string())? {
            if seen.len()>=n { return Err("extra reverse record row".into()); }
            let i = *offsets.get(&offset).ok_or("unmapped reverse offset")?;
            if rows[i].user_id != user || !seen.insert(user.to_owned()) { return Err("reverse record alias differs".into()); }
        }
        if seen != users { return Err("reverse record coverage differs".into()); }
    }
    let options = RecordSegmentReaderOptions::default();
    for row in &rows {
        if reader.get_offset_id_for_user_id(&row.user_id,&options).await.map_err(|e| e.to_string())? != Some(row.record_offset_id)
            || reader.get_user_id_for_offset_id(row.record_offset_id).await.map_err(|e| e.to_string())? != row.user_id {
            return Err("record lookup directions differ".into());
        }
    }
    Ok(AuditedNamespace {reader,rows,parent,collection:collection.clone(),record_files:segment.file_path.clone()})
}

impl AuditedNamespace<'_> {
    pub fn rows(&self) -> &[NamespaceRow] { &self.rows }
    pub fn record_files(&self) -> &std::collections::HashMap<String,Vec<String>> { &self.record_files }
    /// Preserve the exact old table, including user identity and record offsets.
    /// A replacement with equivalent vector values is not an append.
    pub fn validate_extension(&self, candidate: &[NamespaceRow]) -> Result<(),String> {
        if candidate.len()<self.rows.len() || candidate.len()>100_000 || candidate[..self.rows.len()]!=self.rows {
            return Err("namespace append changes parent assignments".into());
        }
        let mut users=BTreeSet::new();let mut offsets=BTreeSet::new();
        for (i,row) in candidate.iter().enumerate() {
            if row.global_id!=i as u64 || row.record_offset_id==0 || row.user_id.is_empty()
                || !users.insert(&row.user_id) || !offsets.insert(row.record_offset_id) {
                return Err("namespace extension is not a bijection".into());
            }
        }
        Ok(())
    }
    /// Own the actual unpartitioned materialization allocator. This prevents a
    /// caller from substituting an offset-remapped result. Failure discards this
    /// attempt; no writer is called here and allocator rollback is unsupported.
    pub async fn materialize_append(&self, collection:&Collection, target:i64, logs:Chunk<LogRecord>)
        -> Result<(MaterializeLogsResult,CapturedMaterializedInput),String> {
        if collection!=&self.collection { return Err("captured parent collection differs".into()); }
        let allocator=super::hierarchical_materialized_input::private_add_allocator(Some(&self.reader),logs.total_len())?;
        let result=chroma_segment::types::materialize_logs(&Some(self.reader.clone()),logs.clone(),Some(allocator),
            &RecordSegmentReaderOptions::default()).await.map_err(|e|e.to_string())?;
        let captured=self.capture_append(collection,target,&logs,&result).await?;
        Ok((result,captured))
    }
    /// Materialization is private to this admitted parent. Rejection can advance
    /// the allocator; discard the entire private attempt rather than retry it.
    async fn capture_append(&self, collection: &Collection, target: i64, logs: &Chunk<LogRecord>,
        materialized: &MaterializeLogsResult) -> Result<CapturedMaterializedInput,String> {
        if collection!=&self.collection {
            return Err("captured parent collection fence differs".into());
        }
        let options = RecordSegmentReaderOptions::default();
        for (row,_) in logs.iter() {
            if self.reader.get_offset_id_for_user_id(&row.record.id,&options).await.map_err(|e|e.to_string())?.is_some() {
                return Err("existing parent user is not AddNew".into());
            }
        }
        let mut captured = capture_correspondence(collection,target,&self.parent,logs,materialized).await?;
        let first = self.reader.get_max_offset_id().checked_add(1).ok_or("record namespace exhausted")?;
        let mut actual = BTreeSet::new();
        for (i,row) in captured.rows.iter_mut().enumerate() {
            if !actual.insert(row.record_offset_id) { return Err("duplicate new record offset".into()); }
            row.global_id=self.parent.next_global_id.checked_add(i as u64).ok_or("global ID overflow")?;
        }
        let expected: BTreeSet<u32> = (0..captured.rows.len()).map(|i| first.checked_add(i as u32).ok_or("record namespace exhausted")).collect::<Result<_,_>>()?;
        if actual!=expected { return Err("private record allocator namespace differs".into()); }
        Ok(captured)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn permuted_offsets_preserve_independent_user_identity() {
        let source=vec![SourceIdentity{global_id:0,user_id:"a".into()},SourceIdentity{global_id:1,user_id:"b".into()}];
        let mut rows=vec![NamespaceRow{global_id:0,record_offset_id:9,user_id:"a".into()},NamespaceRow{global_id:1,record_offset_id:2,user_id:"b".into()}];
        assert_eq!(validate_identity(&source,&rows).unwrap().1[&9],0);
        // Equal vector values cannot justify swapping these independently
        // assigned users: identity validation does not inspect vectors.
        rows[0].user_id="b".into();rows[1].user_id="a".into();
        assert!(validate_identity(&source,&rows).is_err());
    }
    #[test]
    fn same_fence_changed_dimension_is_not_same_captured_collection() {
        let parent=Collection::test_collection(4);
        let mut changed=parent.clone();changed.dimension=Some(5);
        assert_ne!(parent,changed);
        let bound=collection_binding(&parent).unwrap();
        let encoded=serde_json::to_vec(&bound).unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&encoded).unwrap(),bound);
        assert_ne!(bound,collection_binding(&changed).unwrap());
        changed=parent.clone();changed.total_records_post_compaction+=1;
        assert_ne!(bound,collection_binding(&changed).unwrap());
    }
    #[test]
    fn duplicate_offsets_hidden_rows_and_old_assignment_changes_reject() {
        let source=vec![SourceIdentity{global_id:0,user_id:"a".into()},SourceIdentity{global_id:1,user_id:"b".into()}];
        let mut rows=vec![NamespaceRow{global_id:0,record_offset_id:2,user_id:"a".into()},NamespaceRow{global_id:1,record_offset_id:2,user_id:"b".into()}];
        assert!(validate_identity(&source,&rows).is_err());
        rows[1].record_offset_id=1;
        assert!(validate_identity(&source[..1],&rows).is_err());
        rows[0].global_id=1;
        assert!(validate_identity(&source,&rows).is_err());
    }
}
