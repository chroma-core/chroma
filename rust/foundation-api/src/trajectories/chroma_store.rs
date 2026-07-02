use std::collections::BTreeMap;

use chroma::{
    types::{IncludeList, Key, Metadata, UpdateMetadata},
    ConditionalCollectionTransaction,
};
use uuid::Uuid;

use super::chunkset::{push_json_record, read_json};
use super::error::TrajectoryError;
use super::ids::uuid_to_tid;
use super::limits::VALUE_MAX_BYTES;
use super::metadata::update_metadata_from_metadata;
use super::model::{GenerateTrajectoryFile, TrajectoryEntry, WriteState};
use super::record_format::{
    collect_entry_records, collect_file_records, collect_finalization_records,
    load_one_from_documents, trajectory_header_key, validate_trajectory_header, ChromaRecord,
    TrajectoryHeader,
};
use super::validate::{validate_entry, validate_file};

const TRAJECTORY_FILTER_PAGE_LIMIT: u32 = 300;

/// Load one generated trajectory from Chroma by UUID.
///
/// When `require_finalized` is true, open trajectories are rejected before their
/// entries are returned.
///
/// # Errors
///
/// Returns [`TrajectoryError`] when Chroma access fails, required records are missing,
/// stored records violate the key/value schema, or a finalized trajectory was
/// required but the stored write state is [`WriteState::Open`].
pub async fn chroma_load_generate_trajectory(
    txn: &mut ConditionalCollectionTransaction,
    tid: Uuid,
    require_finalized: bool,
) -> Result<GenerateTrajectoryFile, TrajectoryError> {
    let records = chroma_records_for_tid(txn, tid).await?;
    load_one_from_documents(&records, tid, require_finalized)
}

/// Save one complete trajectory as finalized Chroma records.
///
/// This operation upserts the records that represent `file`.
///
/// # Errors
///
/// Returns [`TrajectoryError`] when `file` violates local size or shape constraints, or
/// when Chroma rejects the write.
pub async fn chroma_save_generate_trajectory(
    txn: &mut ConditionalCollectionTransaction,
    file: &GenerateTrajectoryFile,
) -> Result<(), TrajectoryError> {
    validate_file(file)?;

    let mut records = Vec::new();
    collect_file_records(&mut records, file, WriteState::Finalized)?;
    chroma_upsert_records(txn, records).await
}

/// Save many complete trajectories as finalized Chroma records.
///
/// This operation validates every file before writing the accumulated record set.
///
/// # Errors
///
/// Returns [`TrajectoryError`] when any file violates local size or shape constraints,
/// or when Chroma rejects the write.
pub async fn chroma_save_all_generate_trajectories(
    txn: &mut ConditionalCollectionTransaction,
    files: &[GenerateTrajectoryFile],
) -> Result<(), TrajectoryError> {
    let mut records = Vec::new();
    for file in files {
        validate_file(file)?;
        collect_file_records(&mut records, file, WriteState::Finalized)?;
    }
    chroma_upsert_records(txn, records).await
}

/// Create an open trajectory that starts with zero committed entries.
///
/// The root metadata is written immediately and entries can later be added with
/// [`chroma_extend_open_trajectory`].
///
/// # Errors
///
/// Returns [`TrajectoryError::InvalidValue`] when the file already contains entries.
/// Returns other [`TrajectoryError`] values when validation or Chroma insertion fails.
pub async fn chroma_create_open_trajectory(
    txn: &mut ConditionalCollectionTransaction,
    file: &GenerateTrajectoryFile,
) -> Result<(), TrajectoryError> {
    if !file.trajectory.actions_and_observations.is_empty() {
        return Err(TrajectoryError::InvalidValue(format!(
            "chroma_create_open_trajectory requires zero committed entries, got {}",
            file.trajectory.actions_and_observations.len()
        )));
    }

    validate_file(file)?;
    let mut records = Vec::new();
    collect_file_records(&mut records, file, WriteState::Open)?;
    chroma_add_records(txn, records).await
}

/// Add entries after checking the expected current entry count.
///
/// This is the optimistic-concurrency form of
/// [`chroma_extend_open_trajectory`]. The returned value is the next entry index
/// after the add succeeds.
///
/// # Errors
///
/// Returns [`TrajectoryError::EntryCountMismatch`] when the persisted entry count is not
/// `expected_entry_index`, [`TrajectoryError::NotOpen`] when the trajectory is already
/// finalized, or another [`TrajectoryError`] for validation and Chroma failures.
pub async fn chroma_extend_open_trajectory_at<'a, I>(
    txn: &mut ConditionalCollectionTransaction,
    tid_uuid: Uuid,
    expected_entry_index: usize,
    entries: I,
) -> Result<usize, TrajectoryError>
where
    I: IntoIterator<Item = &'a TrajectoryEntry>,
{
    let tid = uuid_to_tid(tid_uuid)?;
    let mut header = chroma_read_trajectory_header(txn, tid_uuid).await?;
    validate_trajectory_header(&header, &tid)?;
    if header.write_state != WriteState::Open {
        return Err(TrajectoryError::NotOpen {
            tid: tid_uuid,
            write_state: header.write_state,
        });
    }
    if header.entries != expected_entry_index {
        return Err(TrajectoryError::EntryCountMismatch {
            tid: tid_uuid,
            expected: expected_entry_index,
            actual: header.entries,
        });
    }

    let mut records = Vec::new();
    let mut next_entry = expected_entry_index;
    for entry in entries {
        validate_entry(entry)?;
        collect_entry_records(&mut records, &tid, next_entry, entry)?;
        next_entry = next_entry.checked_add(1).ok_or_else(|| {
            TrajectoryError::InvalidValue("trajectory entry index overflow".to_string())
        })?;
    }

    header.entries = next_entry;
    push_json_record(
        &mut records,
        &format!("gt/{tid}/header"),
        &header,
        VALUE_MAX_BYTES,
    )?;

    let header_key = format!("gt/{tid}/header");
    let mut entry_records = Vec::new();
    let mut header_record = None;
    for record in records {
        if record.id == header_key {
            header_record = Some(record);
        } else {
            entry_records.push(record);
        }
    }

    chroma_add_records(txn, entry_records).await?;
    let Some(header_record) = header_record else {
        return Err(TrajectoryError::MissingKey(header_key));
    };
    chroma_update_records(txn, vec![header_record]).await?;
    Ok(header.entries)
}

/// Add entries to the current end of an open trajectory.
///
/// The returned value is the next entry index after the add succeeds.
///
/// # Errors
///
/// Returns [`TrajectoryError`] when the trajectory header cannot be read, the trajectory
/// is not open, any entry is invalid, or Chroma rejects the write.
pub async fn chroma_extend_open_trajectory<'a, I>(
    txn: &mut ConditionalCollectionTransaction,
    tid_uuid: Uuid,
    entries: I,
) -> Result<usize, TrajectoryError>
where
    I: IntoIterator<Item = &'a TrajectoryEntry>,
{
    let header = chroma_read_trajectory_header(txn, tid_uuid).await?;
    chroma_extend_open_trajectory_at(txn, tid_uuid, header.entries, entries).await
}

/// Mark an open trajectory as finalized using the complete final file.
///
/// The persisted entry count must match `file`; the final write upserts the root
/// metadata, citations, header, and complete entry payload so the stored
/// trajectory is the same entity as the supplied file.
///
/// # Errors
///
/// Returns [`TrajectoryError::NotOpen`] when the stored trajectory is not open,
/// [`TrajectoryError::EntryCountMismatch`] when the stored count and file count differ,
/// or another [`TrajectoryError`] for validation and Chroma failures.
pub async fn chroma_finalize_open_trajectory(
    txn: &mut ConditionalCollectionTransaction,
    file: &GenerateTrajectoryFile,
) -> Result<(), TrajectoryError> {
    validate_file(file)?;

    let tid_uuid = file.trajectory.id;
    let tid = uuid_to_tid(tid_uuid)?;
    let mut header = chroma_read_trajectory_header(txn, tid_uuid).await?;
    validate_trajectory_header(&header, &tid)?;
    if header.write_state != WriteState::Open {
        return Err(TrajectoryError::NotOpen {
            tid: tid_uuid,
            write_state: header.write_state,
        });
    }

    let actual = file.trajectory.actions_and_observations.len();
    if header.entries != actual {
        return Err(TrajectoryError::EntryCountMismatch {
            tid: tid_uuid,
            expected: header.entries,
            actual,
        });
    }

    let mut records = Vec::new();
    collect_finalization_records(&mut records, &mut header, file, &tid)?;

    chroma_upsert_records(txn, records).await
}

/// Load all Chroma documents whose metadata belongs to one trajectory UUID.
async fn chroma_records_for_tid(
    txn: &mut ConditionalCollectionTransaction,
    tid_uuid: Uuid,
) -> Result<BTreeMap<String, String>, TrajectoryError> {
    let mut offset = 0u32;
    let mut documents = BTreeMap::new();

    loop {
        let response = txn
            .get(
                None,
                Some(Key::field("tid").eq(tid_uuid.to_string())),
                Some(TRAJECTORY_FILTER_PAGE_LIMIT),
                Some(offset),
                Some(IncludeList::default_get()),
            )
            .await?;
        let count = u32::try_from(response.ids.len())?;
        let page = documents_from_get_response(response.ids, response.documents)?;
        documents.extend(page);

        if count < TRAJECTORY_FILTER_PAGE_LIMIT {
            break;
        }
        offset = offset.checked_add(count).ok_or_else(|| {
            TrajectoryError::InvalidValue(format!("trajectory {tid_uuid} read offset overflowed"))
        })?;
    }

    Ok(documents)
}

/// Load and decode the trajectory header document for one trajectory UUID.
async fn chroma_read_trajectory_header(
    txn: &mut ConditionalCollectionTransaction,
    tid_uuid: Uuid,
) -> Result<TrajectoryHeader, TrajectoryError> {
    let key = trajectory_header_key(tid_uuid)?;
    let response = txn
        .get(
            Some(vec![key.clone()]),
            None,
            Some(1),
            Some(0),
            Some(IncludeList::default_get()),
        )
        .await?;
    if response.ids.is_empty() {
        return Err(TrajectoryError::MissingKey(key));
    }
    let documents = documents_from_get_response(response.ids, response.documents)?;
    read_json(&documents, &key)
}

/// Pair Chroma get-response ids with their returned documents.
fn documents_from_get_response(
    ids: Vec<String>,
    documents: Option<Vec<Option<String>>>,
) -> Result<BTreeMap<String, String>, TrajectoryError> {
    let documents = documents.unwrap_or_default();
    let mut out = BTreeMap::new();
    for (index, id) in ids.into_iter().enumerate() {
        let document = documents
            .get(index)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                TrajectoryError::InvalidValue(format!("Chroma record {id} is missing its document"))
            })?;
        out.insert(id, document.clone());
    }
    Ok(out)
}

/// Add new records to Chroma without replacing existing ids.
async fn chroma_add_records(
    txn: &mut ConditionalCollectionTransaction,
    records: Vec<ChromaRecord>,
) -> Result<(), TrajectoryError> {
    if records.is_empty() {
        return Ok(());
    }
    let len = records.len();
    let (ids, documents, metadatas) = add_parts(records);
    ensure_ids_absent(txn, ids.clone()).await?;
    txn.add(
        ids,
        fixed_embeddings(len),
        Some(documents),
        None,
        Some(metadatas),
    )
    .await?;
    Ok(())
}

/// Prove exact record ids are absent in the transaction snapshot before add.
async fn ensure_ids_absent(
    txn: &mut ConditionalCollectionTransaction,
    ids: Vec<String>,
) -> Result<(), TrajectoryError> {
    if ids.is_empty() {
        return Ok(());
    }
    let response = txn
        .get(Some(ids), None, None, Some(0), Some(IncludeList::empty()))
        .await?;
    if let Some(id) = response.ids.into_iter().next() {
        return Err(TrajectoryError::InvalidValue(format!(
            "Chroma record {id} already exists"
        )));
    }
    Ok(())
}

/// Upsert records into Chroma, replacing existing ids when present.
async fn chroma_upsert_records(
    txn: &mut ConditionalCollectionTransaction,
    records: Vec<ChromaRecord>,
) -> Result<(), TrajectoryError> {
    if records.is_empty() {
        return Ok(());
    }
    let len = records.len();
    let (ids, documents, metadatas) = update_parts(records);
    txn.upsert(
        ids,
        fixed_embeddings(len),
        Some(documents),
        None,
        Some(metadatas),
    )
    .await?;
    Ok(())
}

/// Update existing Chroma records without changing their embeddings.
async fn chroma_update_records(
    txn: &mut ConditionalCollectionTransaction,
    records: Vec<ChromaRecord>,
) -> Result<(), TrajectoryError> {
    if records.is_empty() {
        return Ok(());
    }
    let len = records.len();
    let (ids, documents, metadatas) = update_parts(records);
    txn.update(
        ids,
        Some(vec![None; len]),
        Some(documents),
        None,
        Some(metadatas),
    )
    .await?;
    Ok(())
}

/// Split records into add-call ids, optional documents, and metadata.
fn add_parts(
    records: Vec<ChromaRecord>,
) -> (Vec<String>, Vec<Option<String>>, Vec<Option<Metadata>>) {
    let mut ids = Vec::with_capacity(records.len());
    let mut documents = Vec::with_capacity(records.len());
    let mut metadatas = Vec::with_capacity(records.len());
    for record in records {
        ids.push(record.id);
        documents.push(Some(record.document));
        metadatas.push(Some(record.metadata));
    }
    (ids, documents, metadatas)
}

/// Split records into update-call ids, optional documents, and update metadata.
fn update_parts(
    records: Vec<ChromaRecord>,
) -> (
    Vec<String>,
    Vec<Option<String>>,
    Vec<Option<UpdateMetadata>>,
) {
    let mut ids = Vec::with_capacity(records.len());
    let mut documents = Vec::with_capacity(records.len());
    let mut metadatas = Vec::with_capacity(records.len());
    for record in records {
        ids.push(record.id);
        documents.push(Some(record.document));
        metadatas.push(Some(update_metadata_from_metadata(record.metadata)));
    }
    (ids, documents, metadatas)
}

/// Produce deterministic placeholder embeddings for metadata-only storage.
fn fixed_embeddings(len: usize) -> Vec<Vec<f32>> {
    vec![vec![0.0]; len]
}
