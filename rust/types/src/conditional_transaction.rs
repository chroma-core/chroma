use std::collections::BTreeSet;

use chroma_api_types::{OccReadMode, OccReadToken};
use chroma_error::{ChromaError, ErrorCodes};
use thiserror::Error;

use crate::{
    AddCollectionRecordsRequest, CollectionUuid, DatabaseName, DeleteCollectionRecordsRequest,
    GetRequest, GetResponse, Operation, OperationRecord, ScalarEncoding,
    UpdateCollectionRecordsRequest, UpdateMetadata, UpdateMetadataValue,
    UpsertCollectionRecordsRequest, CHROMA_DOCUMENT_KEY, CHROMA_URI_KEY,
};

/// One buffered write operation in transaction call order.
///
/// The per-operation `ids` preserve the ids and order that belong to this
/// specific write call. `ConditionalTransactionState::buffered_write_ids` is a
/// separate membership index used for fast validation such as read-after-write
/// and duplicate-write checks; it does not replace this ordered payload state.
#[derive(Clone, Debug, PartialEq)]
pub enum ConditionalBufferedWrite {
    Add(AddCollectionRecordsRequest),
    Update(UpdateCollectionRecordsRequest),
    Upsert(UpsertCollectionRecordsRequest),
    Delete(DeleteCollectionRecordsRequest),
}

impl ConditionalBufferedWrite {
    pub fn operation(&self) -> Operation {
        match self {
            Self::Add(_) => Operation::Add,
            Self::Update(_) => Operation::Update,
            Self::Upsert(_) => Operation::Upsert,
            Self::Delete(_) => Operation::Delete,
        }
    }

    pub fn ids(&self) -> &[String] {
        match self {
            Self::Add(request) => &request.ids,
            Self::Update(request) => &request.ids,
            Self::Upsert(request) => &request.ids,
            Self::Delete(request) => request.ids.as_deref().unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ConditionalCommitRequest {
    pub buffered_writes: Vec<ConditionalBufferedWrite>,
    pub observed_log_offset: Option<i64>,
    pub read_ids: Vec<String>,
}

impl ConditionalCommitRequest {
    pub fn record_count(&self) -> usize {
        self.buffered_writes
            .iter()
            .map(|write| write.ids().len())
            .sum()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub struct ConditionalCommitResult {
    pub first_inserted_record_offset: Option<i64>,
    pub record_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ConditionalCommitAction {
    NoOp(ConditionalCommitResult),
    Append(ConditionalCommitRequest),
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ConditionalTransactionState {
    /// Ids whose values have contributed to the transaction's read snapshot.
    ///
    /// Point reads add their requested ids, while filter reads add only ids
    /// returned by the query. This set becomes the conditional read set checked
    /// against writes appended after the captured read token.
    read_ids: BTreeSet<String>,
    /// First OCC read token observed by the transaction.
    ///
    /// Later reads must reuse this token so every read observes one stable log
    /// snapshot. Its log upper-bound offset is the observed log position for
    /// conditional write validation.
    read_token: Option<OccReadToken>,
    /// Ids known to exist in the captured read snapshot.
    ///
    /// Returned ids are marked present for both point and filter reads. A later
    /// unfiltered point read can remove an id from this set by proving absence.
    known_present: BTreeSet<String>,
    /// Ids known not to exist in the captured read snapshot.
    ///
    /// Only unfiltered point reads can prove absence, because filtered reads can
    /// omit ids for reasons other than nonexistence.
    known_absent: BTreeSet<String>,
    /// Buffered write calls waiting to be emitted at commit time.
    ///
    /// The vector preserves transaction call order, and each entry preserves
    /// the id order from the write call that produced it.
    buffered_writes: Vec<ConditionalBufferedWrite>,
    /// Membership index for ids affected by buffered writes.
    ///
    /// This mirrors the union of ids in `buffered_writes` so validation can
    /// cheaply reject read-after-write and duplicate buffered-write ids.
    buffered_write_ids: BTreeSet<String>,
    /// Whether this state has been closed against further transactional work.
    ///
    /// Once set, new reads are rejected and the state is not reopened.
    closed: bool,
}

impl ConditionalTransactionState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn read_ids(&self) -> &BTreeSet<String> {
        &self.read_ids
    }

    pub fn read_token(&self) -> Option<OccReadToken> {
        self.read_token
    }

    pub fn known_present(&self) -> &BTreeSet<String> {
        &self.known_present
    }

    pub fn known_absent(&self) -> &BTreeSet<String> {
        &self.known_absent
    }

    pub fn buffered_writes(&self) -> &[ConditionalBufferedWrite] {
        &self.buffered_writes
    }

    pub fn buffered_write_ids(&self) -> &BTreeSet<String> {
        &self.buffered_write_ids
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn prepare_commit(
        &mut self,
    ) -> Result<ConditionalCommitAction, ConditionalTransactionError> {
        if self.closed {
            return Err(ConditionalTransactionError::Closed);
        }

        if self.buffered_writes.is_empty() {
            self.closed = true;
            return Ok(ConditionalCommitAction::NoOp(ConditionalCommitResult {
                first_inserted_record_offset: None,
                record_count: 0,
            }));
        }

        let observed_log_offset = self.read_token.map(observed_log_offset).transpose()?;

        Ok(ConditionalCommitAction::Append(ConditionalCommitRequest {
            buffered_writes: self.buffered_writes.clone(),
            observed_log_offset,
            read_ids: self.read_ids.iter().cloned().collect(),
        }))
    }

    pub fn finish_commit(
        &mut self,
        first_inserted_record_offset: Option<i64>,
    ) -> Result<ConditionalCommitResult, ConditionalTransactionError> {
        if self.closed {
            return Err(ConditionalTransactionError::Closed);
        }

        let result = ConditionalCommitResult {
            first_inserted_record_offset,
            record_count: self
                .buffered_writes
                .iter()
                .map(|write| write.ids().len())
                .sum(),
        };
        self.closed = true;
        Ok(result)
    }

    /// Buffers an already-normalized add request.
    ///
    /// Callers should build this request through the normal write preparation
    /// path, including validation and embedding preparation, before handing it
    /// to transaction state. The transaction layer then enforces only local
    /// transaction preconditions and preserves the prepared payload for commit.
    pub fn buffer_add(
        &mut self,
        request: AddCollectionRecordsRequest,
    ) -> Result<(), ConditionalTransactionError> {
        self.buffer_write(ConditionalBufferedWrite::Add(request))
    }

    /// Buffers an already-normalized update request.
    pub fn buffer_update(
        &mut self,
        request: UpdateCollectionRecordsRequest,
    ) -> Result<(), ConditionalTransactionError> {
        self.buffer_write(ConditionalBufferedWrite::Update(request))
    }

    /// Buffers an already-normalized upsert request.
    pub fn buffer_upsert(
        &mut self,
        request: UpsertCollectionRecordsRequest,
    ) -> Result<(), ConditionalTransactionError> {
        self.buffer_write(ConditionalBufferedWrite::Upsert(request))
    }

    /// Buffers an already-normalized explicit-id delete request.
    pub fn buffer_delete(
        &mut self,
        request: DeleteCollectionRecordsRequest,
    ) -> Result<(), ConditionalTransactionError> {
        self.buffer_write(ConditionalBufferedWrite::Delete(request))
    }

    pub fn prepare_get_request(
        &self,
        request: GetRequest,
    ) -> Result<GetRequest, ConditionalTransactionError> {
        self.validate_get_request(&request)?;
        Ok(match self.read_token {
            Some(read_token) => request.with_occ_read_token(read_token),
            None => request.with_occ_read_token_generation(),
        })
    }

    pub fn finish_get(
        &mut self,
        request: &GetRequest,
        response: GetResponse,
    ) -> Result<GetResponse, ConditionalTransactionError> {
        self.record_get_response(request, &response)?;
        Ok(response)
    }

    pub fn record_get_response(
        &mut self,
        request: &GetRequest,
        response: &GetResponse,
    ) -> Result<(), ConditionalTransactionError> {
        self.validate_get_request(request)?;
        let read_token = match request.occ_read_mode() {
            OccReadMode::Capture => response
                .occ_read_token()
                .ok_or(ConditionalTransactionError::MissingReadToken)?,
            OccReadMode::AtToken(read_token) => read_token,
            OccReadMode::None => return Err(ConditionalTransactionError::MissingReadToken),
        };
        observed_log_offset(read_token)?;
        if let Some(existing_read_token) = self.read_token {
            if existing_read_token != read_token {
                return Err(ConditionalTransactionError::ReadTokenMismatch {
                    expected_log_upper_bound_offset: existing_read_token.log_upper_bound_offset(),
                    actual_log_upper_bound_offset: read_token.log_upper_bound_offset(),
                });
            }
        }

        let returned_ids: BTreeSet<&str> = response.ids.iter().map(String::as_str).collect();
        if let Some(id) = response
            .ids
            .iter()
            .find(|id| self.buffered_write_ids.contains(id.as_str()))
        {
            return Err(ConditionalTransactionError::ReadAfterBufferedWrite { id: id.clone() });
        }

        let mut next_read_ids = self.read_ids.clone();
        let mut next_known_present = self.known_present.clone();
        let mut next_known_absent = self.known_absent.clone();

        if let Some(ids) = &request.ids {
            for id in ids {
                next_read_ids.insert(id.clone());
            }
            for id in &response.ids {
                next_read_ids.insert(id.clone());
                next_known_present.insert(id.clone());
                next_known_absent.remove(id);
            }
            if request.r#where.is_none() {
                for id in ids {
                    if !returned_ids.contains(id.as_str()) {
                        next_known_absent.insert(id.clone());
                        next_known_present.remove(id);
                    }
                }
            }
        } else {
            for id in &response.ids {
                next_read_ids.insert(id.clone());
                next_known_present.insert(id.clone());
                next_known_absent.remove(id);
            }
        }

        self.read_ids = next_read_ids;
        self.known_present = next_known_present;
        self.known_absent = next_known_absent;
        if self.read_token.is_none() {
            self.read_token = Some(read_token);
        }

        Ok(())
    }

    fn buffer_write(
        &mut self,
        write: ConditionalBufferedWrite,
    ) -> Result<(), ConditionalTransactionError> {
        self.validate_buffered_write(&write)?;
        for id in write.ids() {
            self.buffered_write_ids.insert(id.clone());
        }
        self.buffered_writes.push(write);
        Ok(())
    }

    fn validate_buffered_write(
        &self,
        write: &ConditionalBufferedWrite,
    ) -> Result<(), ConditionalTransactionError> {
        if self.closed {
            return Err(ConditionalTransactionError::Closed);
        }
        if let ConditionalBufferedWrite::Delete(request) = write {
            if request.r#where.is_some() || request.limit.is_some() || write.ids().is_empty() {
                return Err(ConditionalTransactionError::PredicateDeleteUnsupported);
            }
        }

        let mut call_ids = BTreeSet::new();
        for id in write.ids() {
            if !call_ids.insert(id.clone()) {
                return Err(ConditionalTransactionError::DuplicateWriteIdInRequest {
                    id: id.clone(),
                });
            }
            if self.buffered_write_ids.contains(id) {
                return Err(ConditionalTransactionError::DuplicateBufferedWrite { id: id.clone() });
            }
            self.validate_write_precondition(write.operation(), id)?;
        }
        Ok(())
    }

    fn validate_write_precondition(
        &self,
        operation: Operation,
        id: &str,
    ) -> Result<(), ConditionalTransactionError> {
        match operation {
            Operation::Add if !self.known_absent.contains(id) => {
                Err(ConditionalTransactionError::AddRequiresKnownAbsent { id: id.to_string() })
            }
            Operation::Update if !self.known_present.contains(id) => {
                Err(ConditionalTransactionError::UpdateRequiresKnownPresent { id: id.to_string() })
            }
            Operation::Delete if !self.known_present.contains(id) => {
                Err(ConditionalTransactionError::DeleteRequiresKnownPresent { id: id.to_string() })
            }
            Operation::Add | Operation::Update | Operation::Upsert | Operation::Delete => Ok(()),
            Operation::BackfillFn => {
                Err(ConditionalTransactionError::UnsupportedWriteOperation { operation })
            }
        }
    }

    fn validate_get_request(
        &self,
        request: &GetRequest,
    ) -> Result<(), ConditionalTransactionError> {
        if self.closed {
            return Err(ConditionalTransactionError::Closed);
        }

        match &request.ids {
            Some(ids) => {
                if let Some(id) = ids
                    .iter()
                    .find(|id| self.buffered_write_ids.contains(id.as_str()))
                {
                    return Err(ConditionalTransactionError::ReadAfterBufferedWrite {
                        id: id.clone(),
                    });
                }
            }
            None if !matches!(request.limit, Some(limit) if limit > 0) => {
                return Err(ConditionalTransactionError::FilterReadRequiresPositiveLimit);
            }
            None => {}
        }

        Ok(())
    }
}

fn observed_log_offset(read_token: OccReadToken) -> Result<i64, ConditionalTransactionError> {
    i64::try_from(read_token.log_upper_bound_offset()).map_err(|_| {
        ConditionalTransactionError::ReadTokenOutOfRange {
            log_upper_bound_offset: read_token.log_upper_bound_offset(),
        }
    })
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConditionalTransactionError {
    #[error("conditional transaction is closed")]
    Closed,
    #[error("transactional filter reads require a positive limit")]
    FilterReadRequiresPositiveLimit,
    #[error("cannot transactionally read id {id:?} after buffering a write for it")]
    ReadAfterBufferedWrite { id: String },
    #[error("transactional predicate deletes are not supported")]
    PredicateDeleteUnsupported,
    #[error("transactional add for id {id:?} requires a prior read proving the id is absent")]
    AddRequiresKnownAbsent { id: String },
    #[error("transactional update for id {id:?} requires a prior read proving the id is present")]
    UpdateRequiresKnownPresent { id: String },
    #[error("transactional delete for id {id:?} requires a prior read proving the id is present")]
    DeleteRequiresKnownPresent { id: String },
    #[error("transactional write request contains duplicate id {id:?}")]
    DuplicateWriteIdInRequest { id: String },
    #[error("transaction already has a buffered write for id {id:?}")]
    DuplicateBufferedWrite { id: String },
    #[error("transactional writes do not support operation {operation:?}")]
    UnsupportedWriteOperation { operation: Operation },
    #[error(
        "conditional transactions require the gRPC log implementation; configured log is {implementation}"
    )]
    UnsupportedLogImplementation { implementation: String },
    #[error("transactional get response did not include an OCC read token")]
    MissingReadToken,
    #[error(
        "transactional read token changed from log upper bound offset {expected_log_upper_bound_offset} to {actual_log_upper_bound_offset}"
    )]
    ReadTokenMismatch {
        expected_log_upper_bound_offset: u64,
        actual_log_upper_bound_offset: u64,
    },
    #[error("transactional read token offset {log_upper_bound_offset} exceeds i64 range")]
    ReadTokenOutOfRange { log_upper_bound_offset: u64 },
}

impl ChromaError for ConditionalTransactionError {
    fn code(&self) -> ErrorCodes {
        match self {
            ConditionalTransactionError::Closed
            | ConditionalTransactionError::FilterReadRequiresPositiveLimit
            | ConditionalTransactionError::ReadAfterBufferedWrite { .. }
            | ConditionalTransactionError::PredicateDeleteUnsupported
            | ConditionalTransactionError::AddRequiresKnownAbsent { .. }
            | ConditionalTransactionError::UpdateRequiresKnownPresent { .. }
            | ConditionalTransactionError::DeleteRequiresKnownPresent { .. }
            | ConditionalTransactionError::DuplicateWriteIdInRequest { .. }
            | ConditionalTransactionError::DuplicateBufferedWrite { .. }
            | ConditionalTransactionError::UnsupportedWriteOperation { .. }
            | ConditionalTransactionError::UnsupportedLogImplementation { .. } => {
                ErrorCodes::InvalidArgument
            }
            ConditionalTransactionError::MissingReadToken
            | ConditionalTransactionError::ReadTokenMismatch { .. }
            | ConditionalTransactionError::ReadTokenOutOfRange { .. } => {
                ErrorCodes::FailedPrecondition
            }
        }
    }
}

#[derive(Debug, Error)]
pub enum ConditionalCommitError {
    #[error(transparent)]
    Transaction(#[from] ConditionalTransactionError),
    #[error(
        "conditional transactions require the gRPC log implementation; configured log is {implementation}"
    )]
    TransactionsNotSupported { implementation: String },
    #[error("Backoff and retry")]
    Backoff,
    #[error("Invalid database name")]
    InvalidDatabaseName,
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),
    #[error(transparent)]
    Other(#[from] Box<dyn ChromaError>),
}

impl ChromaError for ConditionalCommitError {
    fn code(&self) -> ErrorCodes {
        match self {
            ConditionalCommitError::Transaction(err) => err.code(),
            ConditionalCommitError::TransactionsNotSupported { .. } => ErrorCodes::Unimplemented,
            ConditionalCommitError::Backoff => ErrorCodes::ResourceExhausted,
            ConditionalCommitError::InvalidDatabaseName => ErrorCodes::InvalidArgument,
            ConditionalCommitError::InvalidArgument(_) => ErrorCodes::InvalidArgument,
            ConditionalCommitError::Other(err) => err.code(),
        }
    }
}

fn conditional_commit_invalid_argument(message: impl Into<String>) -> ConditionalCommitError {
    ConditionalCommitError::InvalidArgument(message.into())
}

fn buffered_write_scope(write: &ConditionalBufferedWrite) -> (&str, &str, CollectionUuid) {
    match write {
        ConditionalBufferedWrite::Add(request) => (
            &request.tenant_id,
            &request.database_name,
            request.collection_id,
        ),
        ConditionalBufferedWrite::Update(request) => (
            &request.tenant_id,
            &request.database_name,
            request.collection_id,
        ),
        ConditionalBufferedWrite::Upsert(request) => (
            &request.tenant_id,
            &request.database_name,
            request.collection_id,
        ),
        ConditionalBufferedWrite::Delete(request) => (
            &request.tenant_id,
            &request.database_name,
            request.collection_id,
        ),
    }
}

pub fn validate_conditional_commit_scope(
    request: &ConditionalCommitRequest,
) -> Result<(String, DatabaseName, CollectionUuid), ConditionalCommitError> {
    let Some(first_write) = request.buffered_writes.first() else {
        return Err(conditional_commit_invalid_argument(
            "conditional commit append request must contain at least one write",
        ));
    };
    let (tenant_id, database_name, collection_id) = buffered_write_scope(first_write);
    for write in &request.buffered_writes[1..] {
        let (write_tenant_id, write_database_name, write_collection_id) =
            buffered_write_scope(write);
        if write_tenant_id != tenant_id
            || write_database_name != database_name
            || write_collection_id != collection_id
        {
            return Err(conditional_commit_invalid_argument(
                "conditional transaction contains writes for multiple collection scopes",
            ));
        }
    }
    let database_name = DatabaseName::new(database_name.to_string())
        .ok_or(ConditionalCommitError::InvalidDatabaseName)?;
    Ok((tenant_id.to_string(), database_name, collection_id))
}

pub fn buffered_write_to_records(
    write: ConditionalBufferedWrite,
) -> Result<(Vec<OperationRecord>, u64), ConditionalCommitError> {
    match write {
        ConditionalBufferedWrite::Add(AddCollectionRecordsRequest {
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }) => {
            let embeddings = Some(embeddings.into_iter().map(Some).collect());
            to_records(ids, embeddings, documents, uris, metadatas, Operation::Add)
        }
        ConditionalBufferedWrite::Update(UpdateCollectionRecordsRequest {
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }) => to_records(
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            Operation::Update,
        ),
        ConditionalBufferedWrite::Upsert(UpsertCollectionRecordsRequest {
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
            ..
        }) => {
            let embeddings = Some(embeddings.into_iter().map(Some).collect());
            to_records(
                ids,
                embeddings,
                documents,
                uris,
                metadatas,
                Operation::Upsert,
            )
        }
        ConditionalBufferedWrite::Delete(DeleteCollectionRecordsRequest { ids, .. }) => {
            let records = ids
                .unwrap_or_default()
                .into_iter()
                .map(|id| OperationRecord {
                    id,
                    operation: Operation::Delete,
                    document: None,
                    embedding: None,
                    encoding: None,
                    metadata: None,
                })
                .collect::<Vec<_>>();
            let log_size_bytes = records.iter().map(OperationRecord::size_bytes).sum();
            Ok((records, log_size_bytes))
        }
    }
}

fn to_records<V: Into<UpdateMetadataValue>, M: IntoIterator<Item = (String, V)>>(
    ids: Vec<String>,
    embeddings: Option<Vec<Option<Vec<f32>>>>,
    documents: Option<Vec<Option<String>>>,
    uris: Option<Vec<Option<String>>>,
    metadatas: Option<Vec<Option<M>>>,
    operation: Operation,
) -> Result<(Vec<OperationRecord>, u64), ConditionalCommitError> {
    let mut total_bytes = 0;
    let len = ids.len();

    if embeddings.as_ref().is_some_and(|v| v.len() != len)
        || documents.as_ref().is_some_and(|v| v.len() != len)
        || uris.as_ref().is_some_and(|v| v.len() != len)
        || metadatas.as_ref().is_some_and(|v| v.len() != len)
    {
        return Err(conditional_commit_invalid_argument(
            "inconsistent number of IDs, embeddings, documents, URIs and metadatas",
        ));
    }

    let mut embeddings_iter = embeddings.into_iter().flatten();
    let mut documents_iter = documents.into_iter().flatten();
    let mut uris_iter = uris.into_iter().flatten();
    let mut metadatas_iter = metadatas.into_iter().flatten();
    let mut records = Vec::with_capacity(len);

    for id in ids {
        if id.is_empty() {
            return Err(conditional_commit_invalid_argument(
                "empty ID, ID must have at least one character",
            ));
        }

        let embedding = embeddings_iter.next().flatten();
        let document = documents_iter.next().flatten();
        let uri = uris_iter.next().flatten();
        let metadata = metadatas_iter.next().flatten();
        let encoding = embedding.as_ref().map(|_| ScalarEncoding::FLOAT32);

        let mut metadata = metadata
            .map(|m| {
                m.into_iter()
                    .map(|(key, value)| (key, value.into()))
                    .collect::<UpdateMetadata>()
            })
            .unwrap_or_default();
        if let Some(document) = document.clone() {
            metadata.insert(
                CHROMA_DOCUMENT_KEY.to_string(),
                UpdateMetadataValue::Str(document),
            );
        }
        if let Some(uri) = uri {
            metadata.insert(CHROMA_URI_KEY.to_string(), UpdateMetadataValue::Str(uri));
        }

        let record = OperationRecord {
            id,
            embedding,
            encoding,
            metadata: Some(metadata),
            document,
            operation,
        };
        total_bytes += record.size_bytes();
        records.push(record);
    }

    Ok((records, total_bytes))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::{
        CollectionUuid, Include, IncludeList, MetadataComparison, MetadataExpression,
        MetadataValue, PrimitiveOperator, Where,
    };

    use super::*;

    fn string_set(ids: &[&str]) -> BTreeSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    fn request(
        ids: Option<Vec<&str>>,
        where_clause: Option<Where>,
        limit: Option<u32>,
    ) -> GetRequest {
        GetRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid::default(),
            ids.map(|ids| ids.into_iter().map(String::from).collect()),
            where_clause,
            limit,
            0,
            IncludeList(vec![Include::Document, Include::Metadata]),
        )
        .unwrap()
    }

    fn response(ids: &[&str], token_offset: u64) -> GetResponse {
        GetResponse {
            ids: ids.iter().map(|id| (*id).to_string()).collect(),
            include: vec![Include::Document, Include::Metadata],
            occ_read_token: Some(OccReadToken::try_new(token_offset).unwrap()),
            ..Default::default()
        }
    }

    fn response_without_token(ids: &[&str]) -> GetResponse {
        GetResponse {
            ids: ids.iter().map(|id| (*id).to_string()).collect(),
            include: vec![Include::Document, Include::Metadata],
            ..Default::default()
        }
    }

    #[test]
    fn transactions_not_supported_commit_error_uses_unimplemented_code() {
        let err = ConditionalCommitError::TransactionsNotSupported {
            implementation: "sqlite".to_string(),
        };

        assert_eq!(ErrorCodes::Unimplemented, err.code());
        assert_eq!(
            "conditional transactions require the gRPC log implementation; configured log is sqlite",
            err.to_string()
        );
    }

    fn metadata_where() -> Where {
        Where::Metadata(MetadataExpression {
            key: "status".to_string(),
            comparison: MetadataComparison::Primitive(
                PrimitiveOperator::Equal,
                MetadataValue::Str("ready".to_string()),
            ),
        })
    }

    fn add_request(ids: &[&str]) -> AddCollectionRecordsRequest {
        AddCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid::default(),
            ids.iter().map(|id| (*id).to_string()).collect(),
            vec![vec![1.0]; ids.len()],
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn update_request(ids: &[&str]) -> UpdateCollectionRecordsRequest {
        UpdateCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid::default(),
            ids.iter().map(|id| (*id).to_string()).collect(),
            None,
            Some(vec![Some("updated".to_string()); ids.len()]),
            None,
            None,
        )
        .unwrap()
    }

    fn upsert_request(ids: &[&str]) -> UpsertCollectionRecordsRequest {
        UpsertCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid::default(),
            ids.iter().map(|id| (*id).to_string()).collect(),
            vec![vec![2.0]; ids.len()],
            None,
            None,
            None,
        )
        .unwrap()
    }

    fn delete_request(ids: &[&str]) -> DeleteCollectionRecordsRequest {
        DeleteCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid::default(),
            Some(ids.iter().map(|id| (*id).to_string()).collect()),
            None,
            None,
        )
        .unwrap()
    }

    fn predicate_delete_request() -> DeleteCollectionRecordsRequest {
        DeleteCollectionRecordsRequest::try_new(
            "tenant".to_string(),
            "database".to_string(),
            CollectionUuid::default(),
            None,
            Some(metadata_where()),
            Some(1),
        )
        .unwrap()
    }

    fn record_point_read(
        state: &mut ConditionalTransactionState,
        requested_ids: &[&str],
        returned_ids: &[&str],
    ) {
        let request = state
            .prepare_get_request(request(Some(requested_ids.to_vec()), None, None))
            .unwrap();
        state
            .record_get_response(&request, &response(returned_ids, 42))
            .unwrap();
    }

    #[test]
    fn transactional_get_prepares_capture_mode_and_preserves_response() {
        let mut state = ConditionalTransactionState::new();
        let request = request(Some(vec!["doc-1"]), None, None);
        let prepared = state.prepare_get_request(request.clone()).unwrap();

        assert_eq!(prepared.occ_read_mode(), OccReadMode::Capture);
        assert_eq!(prepared.include, request.include);

        let response = response(&["doc-1"], 42);
        let finished = state.finish_get(&prepared, response.clone()).unwrap();

        assert_eq!(finished.ids, response.ids);
        assert_eq!(finished.include, response.include);
        assert_eq!(state.read_token(), Some(OccReadToken::try_new(42).unwrap()));
    }

    #[test]
    fn point_get_includes_absent_ids_in_read_set() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["present", "absent"]), None, None))
            .unwrap();

        state
            .record_get_response(&request, &response(&["present"], 50))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["present", "absent"]));
        assert_eq!(state.known_present(), &string_set(&["present"]));
        assert_eq!(state.known_absent(), &string_set(&["absent"]));
    }

    #[test]
    fn later_reads_reuse_first_read_token() {
        let mut state = ConditionalTransactionState::new();
        let first = state
            .prepare_get_request(request(Some(vec!["first"]), None, None))
            .unwrap();
        state
            .record_get_response(&first, &response(&["first"], 100))
            .unwrap();
        let second = state
            .prepare_get_request(request(Some(vec!["second"]), None, None))
            .unwrap();
        assert_eq!(
            second.occ_read_mode(),
            OccReadMode::AtToken(OccReadToken::try_new(100).unwrap())
        );
        state
            .record_get_response(&second, &response_without_token(&["second"]))
            .unwrap();

        assert_eq!(
            state.read_token(),
            Some(OccReadToken::try_new(100).unwrap())
        );
        assert_eq!(state.read_ids(), &string_set(&["first", "second"]));
    }

    #[test]
    fn later_capture_with_different_token_is_rejected() {
        let mut state = ConditionalTransactionState::new();
        let first = state
            .prepare_get_request(request(Some(vec!["first"]), None, None))
            .unwrap();
        state
            .record_get_response(&first, &response(&["first"], 100))
            .unwrap();
        let stale_capture =
            request(Some(vec!["second"]), None, None).with_occ_read_token_generation();

        assert!(matches!(
            state.record_get_response(&stale_capture, &response(&["second"], 101)),
            Err(ConditionalTransactionError::ReadTokenMismatch {
                expected_log_upper_bound_offset: 100,
                actual_log_upper_bound_offset: 101,
            })
        ));
    }

    #[test]
    fn filter_only_get_requires_positive_limit() {
        let state = ConditionalTransactionState::new();

        assert!(matches!(
            state.prepare_get_request(request(None, Some(metadata_where()), None)),
            Err(ConditionalTransactionError::FilterReadRequiresPositiveLimit)
        ));
        assert!(matches!(
            state.prepare_get_request(request(None, Some(metadata_where()), Some(0))),
            Err(ConditionalTransactionError::FilterReadRequiresPositiveLimit)
        ));

        let prepared = state
            .prepare_get_request(request(None, Some(metadata_where()), Some(1)))
            .unwrap();
        assert_eq!(prepared.occ_read_mode(), OccReadMode::Capture);
    }

    #[test]
    fn filter_only_get_uses_returned_ids_as_read_set() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(None, Some(metadata_where()), Some(10)))
            .unwrap();

        state
            .record_get_response(&request, &response(&["doc-1", "doc-2"], 60))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["doc-1", "doc-2"]));
        assert_eq!(state.known_present(), &string_set(&["doc-1", "doc-2"]));
        assert!(state.known_absent().is_empty());
    }

    #[test]
    fn ids_with_filter_marks_only_returned_ids_present() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(
                Some(vec!["returned", "unknown"]),
                Some(metadata_where()),
                None,
            ))
            .unwrap();

        state
            .record_get_response(&request, &response(&["returned"], 70))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["returned", "unknown"]));
        assert_eq!(state.known_present(), &string_set(&["returned"]));
        assert!(state.known_absent().is_empty());
    }

    #[test]
    fn absent_point_reads_are_tracked() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["missing"]), None, None))
            .unwrap();

        state
            .record_get_response(&request, &response(&[], 80))
            .unwrap();

        assert_eq!(state.read_ids(), &string_set(&["missing"]));
        assert!(state.known_present().is_empty());
        assert_eq!(state.known_absent(), &string_set(&["missing"]));
    }

    #[test]
    fn reading_buffered_write_id_fails_but_other_ids_can_be_read() {
        let mut state = ConditionalTransactionState::new();
        let upsert = upsert_request(&["written"]);
        state.buffer_upsert(upsert.clone()).unwrap();

        assert!(matches!(
            state.prepare_get_request(request(Some(vec!["written"]), None, None)),
            Err(ConditionalTransactionError::ReadAfterBufferedWrite { id })
                if id == "written"
        ));

        let prepared = state
            .prepare_get_request(request(Some(vec!["other"]), None, None))
            .unwrap();
        state
            .record_get_response(&prepared, &response(&["other"], 90))
            .unwrap();
        assert_eq!(
            state,
            ConditionalTransactionState {
                read_ids: string_set(&["other"]),
                read_token: Some(OccReadToken::try_new(90).unwrap()),
                known_present: string_set(&["other"]),
                known_absent: BTreeSet::new(),
                buffered_writes: vec![ConditionalBufferedWrite::Upsert(upsert)],
                buffered_write_ids: string_set(&["written"]),
                closed: false,
            }
        );
    }

    #[test]
    fn missing_occ_read_token_fails_transactional_get() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["doc"]), None, None))
            .unwrap();
        let response = GetResponse {
            ids: vec!["doc".to_string()],
            include: vec![Include::Document],
            ..Default::default()
        };

        assert!(matches!(
            state.record_get_response(&request, &response),
            Err(ConditionalTransactionError::MissingReadToken)
        ));
    }

    #[test]
    fn out_of_range_read_token_fails_transactional_get() {
        let mut state = ConditionalTransactionState::new();
        let request = state
            .prepare_get_request(request(Some(vec!["doc"]), None, None))
            .unwrap();
        let response = response(&["doc"], i64::MAX as u64 + 1);

        assert_eq!(
            state.record_get_response(&request, &response),
            Err(ConditionalTransactionError::ReadTokenOutOfRange {
                log_upper_bound_offset: i64::MAX as u64 + 1
            })
        );
    }

    #[test]
    fn transaction_state_tracks_buffers_and_closed_state() {
        let mut state = ConditionalTransactionState::new();

        assert!(state.buffered_writes().is_empty());
        assert!(state.buffered_write_ids().is_empty());
        assert!(!state.is_closed());

        state.close();

        assert!(state.is_closed());
        assert!(matches!(
            state.prepare_get_request(request(Some(vec!["doc"]), None, None)),
            Err(ConditionalTransactionError::Closed)
        ));
    }

    #[test]
    fn add_requires_known_absent_and_buffers_all_ids_in_order() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(&mut state, &["absent-a", "absent-b"], &[]);
        let add = add_request(&["absent-a", "absent-b"]);

        state.buffer_add(add.clone()).unwrap();

        assert_eq!(
            state,
            ConditionalTransactionState {
                read_ids: string_set(&["absent-a", "absent-b"]),
                read_token: Some(OccReadToken::try_new(42).unwrap()),
                known_present: BTreeSet::new(),
                known_absent: string_set(&["absent-a", "absent-b"]),
                buffered_writes: vec![ConditionalBufferedWrite::Add(add)],
                buffered_write_ids: string_set(&["absent-a", "absent-b"]),
                closed: false,
            }
        );
    }

    #[test]
    fn add_without_known_absent_fails_without_mutating_state() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(&mut state, &["present"], &["present"]);
        let before = state.clone();

        assert_eq!(
            state.buffer_add(add_request(&["present"])),
            Err(ConditionalTransactionError::AddRequiresKnownAbsent {
                id: "present".to_string(),
            })
        );
        assert_eq!(state, before);
    }

    #[test]
    fn update_and_delete_require_known_present() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(
            &mut state,
            &["present-update", "present-delete", "absent"],
            &["present-update", "present-delete"],
        );
        let update = update_request(&["present-update"]);
        let delete = delete_request(&["present-delete"]);

        state.buffer_update(update.clone()).unwrap();
        state.buffer_delete(delete.clone()).unwrap();

        assert_eq!(
            state,
            ConditionalTransactionState {
                read_ids: string_set(&["present-update", "present-delete", "absent"]),
                read_token: Some(OccReadToken::try_new(42).unwrap()),
                known_present: string_set(&["present-update", "present-delete"]),
                known_absent: string_set(&["absent"]),
                buffered_writes: vec![
                    ConditionalBufferedWrite::Update(update),
                    ConditionalBufferedWrite::Delete(delete),
                ],
                buffered_write_ids: string_set(&["present-update", "present-delete"]),
                closed: false,
            }
        );
    }

    #[test]
    fn update_or_delete_without_known_present_fails_without_mutating_state() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(&mut state, &["absent"], &[]);
        let before = state.clone();

        assert_eq!(
            state.buffer_update(update_request(&["absent"])),
            Err(ConditionalTransactionError::UpdateRequiresKnownPresent {
                id: "absent".to_string(),
            })
        );
        assert_eq!(state, before);

        assert_eq!(
            state.buffer_delete(delete_request(&["absent"])),
            Err(ConditionalTransactionError::DeleteRequiresKnownPresent {
                id: "absent".to_string(),
            })
        );
        assert_eq!(state, before);
    }

    #[test]
    fn upsert_requires_no_prior_presence_knowledge() {
        let mut state = ConditionalTransactionState::new();
        let upsert = upsert_request(&["unknown"]);

        state.buffer_upsert(upsert.clone()).unwrap();

        assert_eq!(
            state,
            ConditionalTransactionState {
                read_ids: BTreeSet::new(),
                read_token: None,
                known_present: BTreeSet::new(),
                known_absent: BTreeSet::new(),
                buffered_writes: vec![ConditionalBufferedWrite::Upsert(upsert)],
                buffered_write_ids: string_set(&["unknown"]),
                closed: false,
            }
        );
    }

    #[test]
    fn predicate_delete_is_rejected_without_mutating_state() {
        let mut state = ConditionalTransactionState::new();
        let before = state.clone();

        assert_eq!(
            state.buffer_delete(predicate_delete_request()),
            Err(ConditionalTransactionError::PredicateDeleteUnsupported)
        );
        assert_eq!(state, before);
    }

    #[test]
    fn duplicate_write_ids_fail_all_or_nothing() {
        let mut state = ConditionalTransactionState::new();
        let upsert = upsert_request(&["first"]);
        state.buffer_upsert(upsert.clone()).unwrap();
        let before = state.clone();

        assert_eq!(
            state.buffer_upsert(upsert_request(&["second", "second"])),
            Err(ConditionalTransactionError::DuplicateWriteIdInRequest {
                id: "second".to_string(),
            })
        );
        assert_eq!(state, before);

        assert_eq!(
            state.buffer_upsert(upsert_request(&["first"])),
            Err(ConditionalTransactionError::DuplicateBufferedWrite {
                id: "first".to_string(),
            })
        );
        assert_eq!(state, before);
    }

    #[test]
    fn failed_multi_id_write_does_not_partially_buffer_valid_ids() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(&mut state, &["absent-a"], &[]);
        let before = state.clone();

        assert_eq!(
            state.buffer_add(add_request(&["absent-a", "unknown"])),
            Err(ConditionalTransactionError::AddRequiresKnownAbsent {
                id: "unknown".to_string(),
            })
        );
        assert_eq!(state, before);
    }

    #[test]
    fn buffered_writes_preserve_call_order_and_per_method_id_order() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(
            &mut state,
            &["present", "absent-a", "absent-b"],
            &["present"],
        );
        let add = add_request(&["absent-a", "absent-b"]);
        let upsert = upsert_request(&["unknown-a", "unknown-b"]);
        let update = update_request(&["present"]);

        state.buffer_add(add.clone()).unwrap();
        state.buffer_upsert(upsert.clone()).unwrap();
        state.buffer_update(update.clone()).unwrap();

        assert_eq!(
            state,
            ConditionalTransactionState {
                read_ids: string_set(&["present", "absent-a", "absent-b"]),
                read_token: Some(OccReadToken::try_new(42).unwrap()),
                known_present: string_set(&["present"]),
                known_absent: string_set(&["absent-a", "absent-b"]),
                buffered_writes: vec![
                    ConditionalBufferedWrite::Add(add),
                    ConditionalBufferedWrite::Upsert(upsert),
                    ConditionalBufferedWrite::Update(update),
                ],
                buffered_write_ids: string_set(&[
                    "absent-a",
                    "absent-b",
                    "unknown-a",
                    "unknown-b",
                    "present",
                ]),
                closed: false,
            }
        );
    }

    #[test]
    fn no_pending_writes_commit_is_successful_noop_and_closes() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(&mut state, &["present"], &["present"]);

        let action = state.prepare_commit().unwrap();

        assert_eq!(
            action,
            ConditionalCommitAction::NoOp(ConditionalCommitResult {
                first_inserted_record_offset: None,
                record_count: 0,
            })
        );
        assert!(state.is_closed());
    }

    #[test]
    fn prepare_commit_carries_buffered_writes_condition_inputs_and_record_count() {
        let mut state = ConditionalTransactionState::new();
        record_point_read(
            &mut state,
            &["present", "absent-a", "absent-b"],
            &["present"],
        );
        let add = add_request(&["absent-a", "absent-b"]);
        let update = update_request(&["present"]);
        state.buffer_add(add.clone()).unwrap();
        state.buffer_update(update.clone()).unwrap();

        let action = state.prepare_commit().unwrap();

        assert_eq!(
            action,
            ConditionalCommitAction::Append(ConditionalCommitRequest {
                buffered_writes: vec![
                    ConditionalBufferedWrite::Add(add),
                    ConditionalBufferedWrite::Update(update),
                ],
                observed_log_offset: Some(42),
                read_ids: vec![
                    "absent-a".to_string(),
                    "absent-b".to_string(),
                    "present".to_string(),
                ],
            })
        );
        match action {
            ConditionalCommitAction::Append(request) => assert_eq!(request.record_count(), 3),
            ConditionalCommitAction::NoOp(_) => panic!("pending writes should require append"),
        }
        assert!(!state.is_closed());
    }

    #[test]
    fn finish_commit_returns_result_and_closes_transaction() {
        let mut state = ConditionalTransactionState::new();
        state
            .buffer_upsert(upsert_request(&["first", "second"]))
            .unwrap();

        let result = state.finish_commit(Some(123)).unwrap();

        assert_eq!(
            result,
            ConditionalCommitResult {
                first_inserted_record_offset: Some(123),
                record_count: 2,
            }
        );
        assert!(state.is_closed());
    }

    #[test]
    fn operations_after_successful_commit_fail_as_closed() {
        let mut state = ConditionalTransactionState::new();
        state.buffer_upsert(upsert_request(&["written"])).unwrap();
        state.finish_commit(None).unwrap();

        assert!(matches!(
            state.prepare_get_request(request(Some(vec!["other"]), None, None)),
            Err(ConditionalTransactionError::Closed)
        ));
        assert_eq!(
            state.buffer_upsert(upsert_request(&["other"])),
            Err(ConditionalTransactionError::Closed)
        );
        assert_eq!(
            state.prepare_commit(),
            Err(ConditionalTransactionError::Closed)
        );
        assert_eq!(
            state.finish_commit(None),
            Err(ConditionalTransactionError::Closed)
        );
    }
}
