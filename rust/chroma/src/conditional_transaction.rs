//! Conditional transaction support for collection-scoped optimistic writes.

use std::ops::AsyncFn;

use chroma_types::{
    AddCollectionRecordsRequest, AddCollectionRecordsResponse, ConditionalCommitAction,
    ConditionalCommitPayload, ConditionalCommitResult, ConditionalGetRequestPayload,
    ConditionalGetResponse, ConditionalTransactionOperationPayload,
    ConditionalTransactionReadPayload, ConditionalTransactionState, DeleteCollectionRecordsRequest,
    DeleteCollectionRecordsResponse, GetRequest, GetResponse, IncludeList, Metadata, OccReadMode,
    OccReadToken, UpdateCollectionRecordsRequest, UpdateCollectionRecordsResponse, UpdateMetadata,
    UpsertCollectionRecordsRequest, UpsertCollectionRecordsResponse, Where,
    CONDITIONAL_WRITE_CONFLICT_MESSAGE,
};
use reqwest::{Method, StatusCode};

use crate::{client::ChromaHttpClientError, ChromaCollection, IntoOptionalEmbeddings};

/// A collection-scoped conditional transaction.
///
/// Reads execute immediately through Chroma's conditional read endpoint and
/// capture a stable read token. Writes are buffered locally and sent only when
/// [`commit`](Self::commit) is awaited. Dropping this value does not commit;
/// any uncommitted buffered writes are discarded with the transaction.
pub struct ConditionalCollectionTransaction {
    collection: ChromaCollection,
    state: ConditionalTransactionState,
    read_token: Option<u64>,
    operations: Vec<ConditionalTransactionOperationPayload>,
    commit_blocked_by_run: bool,
    retryable_operation_error: bool,
}

impl ConditionalCollectionTransaction {
    pub(crate) fn new(collection: ChromaCollection) -> Self {
        Self {
            collection,
            state: ConditionalTransactionState::new(),
            read_token: None,
            operations: Vec::new(),
            commit_blocked_by_run: false,
            retryable_operation_error: false,
        }
    }

    /// Runs an async callback in a fresh transaction attempt and commits it.
    ///
    /// The callback is rerun from the beginning when a transactional operation
    /// or the implicit commit fails with a retryable OCC error. The return
    /// value from the successful callback attempt is preserved.
    pub async fn run<F, T>(
        mut self,
        callback: F,
        max_retries: usize,
    ) -> Result<T, ChromaHttpClientError>
    where
        F: for<'txn> AsyncFn(
            &'txn mut ConditionalCollectionTransaction,
        ) -> Result<T, ChromaHttpClientError>,
    {
        let mut attempt = 0;
        loop {
            let mut txn = if attempt == 0 {
                self
            } else {
                self.collection.conditional()
            };
            txn.commit_blocked_by_run = true;
            let result = callback(&mut txn).await;
            txn.commit_blocked_by_run = false;

            let value = match result {
                Ok(value) => value,
                Err(err)
                    if txn.retryable_operation_error
                        && is_conditional_run_retryable_error(&err)
                        && attempt < max_retries =>
                {
                    attempt += 1;
                    self = txn;
                    continue;
                }
                Err(err) => return Err(err),
            };

            match txn.commit().await {
                Ok(_) => return Ok(value),
                Err(err) if is_conditional_run_retryable_error(&err) && attempt < max_retries => {
                    attempt += 1;
                    self = txn;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Retrieves records in this transaction, capturing or reusing the OCC
    /// read token that pins the transaction's read snapshot.
    pub async fn get(
        &mut self,
        ids: Option<Vec<String>>,
        r#where: Option<Where>,
        limit: Option<u32>,
        offset: Option<u32>,
        include: Option<IncludeList>,
    ) -> Result<GetResponse, ChromaHttpClientError> {
        let request = GetRequest::try_new(
            self.collection.collection.tenant.clone(),
            self.collection.collection.database.clone(),
            self.collection.collection.collection_id,
            ids,
            r#where,
            limit,
            offset.unwrap_or_default(),
            include.unwrap_or_else(IncludeList::default_get),
        )?;
        let prepared_request = self.state.prepare_get_request(request)?;
        let read_token = match prepared_request.occ_read_mode() {
            OccReadMode::None | OccReadMode::Capture => None,
            OccReadMode::AtToken(read_token) => Some(read_token.log_upper_bound_offset()),
        };
        let request_payload = prepared_request.clone().into_payload()?;
        let response: ConditionalGetResponse = self
            .send_and_track_retryable(
                true,
                "conditional_get",
                "conditional/get",
                Method::POST,
                Some(ConditionalGetRequestPayload {
                    ids: request_payload.ids.clone(),
                    where_fields: request_payload.where_fields.clone(),
                    limit: request_payload.limit,
                    offset: request_payload.offset,
                    include: request_payload.include.clone(),
                    read_token,
                }),
            )
            .await?;

        let read_token = OccReadToken::try_new(response.read_token)?;
        let get_response = GetResponse {
            ids: response.ids,
            embeddings: response.embeddings,
            documents: response.documents,
            uris: response.uris,
            metadatas: response.metadatas,
            include: response.include,
            occ_read_token: Some(read_token),
        };
        let returned_ids = get_response.ids.clone();
        let get_response = self.state.finish_get(&prepared_request, get_response)?;
        self.read_token = Some(read_token.log_upper_bound_offset());
        self.operations
            .push(ConditionalTransactionOperationPayload::Get(
                ConditionalTransactionReadPayload {
                    request: request_payload,
                    expected_ids: returned_ids,
                },
            ));
        Ok(get_response)
    }

    /// Buffers an add operation in this transaction.
    pub async fn add(
        &mut self,
        ids: Vec<String>,
        embeddings: impl IntoOptionalEmbeddings,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<Metadata>>>,
    ) -> Result<AddCollectionRecordsResponse, ChromaHttpClientError> {
        let embeddings = self
            .collection
            .resolve_embeddings(embeddings.into_optional_embeddings(), &documents)
            .await?;
        let request = AddCollectionRecordsRequest::try_new(
            self.collection.collection.tenant.clone(),
            self.collection.collection.database.clone(),
            self.collection.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        self.state.buffer_add(request.clone())?;
        self.operations
            .push(ConditionalTransactionOperationPayload::Add(
                request.into_payload(),
            ));
        Ok(AddCollectionRecordsResponse::default())
    }

    /// Buffers an update operation in this transaction.
    pub async fn update(
        &mut self,
        ids: Vec<String>,
        embeddings: Option<Vec<Option<Vec<f32>>>>,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpdateCollectionRecordsResponse, ChromaHttpClientError> {
        let embeddings = self
            .collection
            .resolve_update_embeddings(embeddings, &documents)
            .await?;
        let request = UpdateCollectionRecordsRequest::try_new(
            self.collection.collection.tenant.clone(),
            self.collection.collection.database.clone(),
            self.collection.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        self.state.buffer_update(request.clone())?;
        self.operations
            .push(ConditionalTransactionOperationPayload::Update(
                request.into_payload(),
            ));
        Ok(UpdateCollectionRecordsResponse {})
    }

    /// Buffers an upsert operation in this transaction.
    pub async fn upsert(
        &mut self,
        ids: Vec<String>,
        embeddings: impl IntoOptionalEmbeddings,
        documents: Option<Vec<Option<String>>>,
        uris: Option<Vec<Option<String>>>,
        metadatas: Option<Vec<Option<UpdateMetadata>>>,
    ) -> Result<UpsertCollectionRecordsResponse, ChromaHttpClientError> {
        let embeddings = self
            .collection
            .resolve_embeddings(embeddings.into_optional_embeddings(), &documents)
            .await?;
        let request = UpsertCollectionRecordsRequest::try_new(
            self.collection.collection.tenant.clone(),
            self.collection.collection.database.clone(),
            self.collection.collection.collection_id,
            ids,
            embeddings,
            documents,
            uris,
            metadatas,
        )?;
        self.state.buffer_upsert(request.clone())?;
        self.operations
            .push(ConditionalTransactionOperationPayload::Upsert(
                request.into_payload(),
            ));
        Ok(UpsertCollectionRecordsResponse {})
    }

    /// Buffers an explicit-id delete operation in this transaction.
    pub async fn delete(
        &mut self,
        ids: Vec<String>,
    ) -> Result<DeleteCollectionRecordsResponse, ChromaHttpClientError> {
        let request = DeleteCollectionRecordsRequest::try_new(
            self.collection.collection.tenant.clone(),
            self.collection.collection.database.clone(),
            self.collection.collection.collection_id,
            Some(ids),
            None,
            None,
        )?;
        self.state.buffer_delete(request.clone())?;
        self.operations
            .push(ConditionalTransactionOperationPayload::Delete(
                request.into_payload()?,
            ));
        Ok(DeleteCollectionRecordsResponse { deleted: 0 })
    }

    /// Commits all buffered writes as one conditional append.
    ///
    /// No writes are sent until this method is awaited. Calling this method
    /// inside [`run`](Self::run) is rejected because `run` commits implicitly
    /// after the callback succeeds.
    pub async fn commit(&mut self) -> Result<ConditionalCommitResult, ChromaHttpClientError> {
        if self.commit_blocked_by_run {
            return Err(ChromaHttpClientError::ConditionalCommitInsideRun);
        }

        match self.state.prepare_commit()? {
            ConditionalCommitAction::NoOp(result) => return Ok(result),
            ConditionalCommitAction::Append(_) => {}
        }

        let result: ConditionalCommitResult = self
            .send_and_track_retryable(
                false,
                "conditional_commit",
                "conditional/commit",
                Method::POST,
                Some(ConditionalCommitPayload {
                    read_token: self.read_token,
                    operations: self.operations.clone(),
                }),
            )
            .await?;
        self.state.close();
        Ok(result)
    }

    async fn send_and_track_retryable<Body, Response>(
        &mut self,
        read_only: bool,
        operation: &str,
        path: &str,
        method: Method,
        body: Option<Body>,
    ) -> Result<Response, ChromaHttpClientError>
    where
        Body: serde::Serialize,
        Response: serde::de::DeserializeOwned,
    {
        let result = self
            .collection
            .send(read_only, operation, path, method, body)
            .await;
        if matches!(&result, Err(err) if is_conditional_run_retryable_error(err)) {
            self.retryable_operation_error = true;
        }
        result
    }
}

pub(crate) fn is_conditional_run_retryable_error(err: &ChromaHttpClientError) -> bool {
    match err {
        ChromaHttpClientError::ApiError(message, status) => {
            *status == StatusCode::PRECONDITION_FAILED
                || (*status == StatusCode::CONFLICT
                    && (message.contains(CONDITIONAL_WRITE_CONFLICT_MESSAGE)
                        || message.contains("ConditionalWriteConflictError")))
                || (*status == StatusCode::TOO_MANY_REQUESTS && message.contains("Backoff"))
        }
        ChromaHttpClientError::StaleReadError(_) => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use chroma_types::Collection;
    use httpmock::{HttpMockResponse, MockServer};
    use serde_json::json;

    use crate::{
        client::{ChromaHttpClientOptions, ChromaRetryOptions},
        ChromaHttpClient,
    };

    use super::*;

    fn collection(server: &MockServer) -> ChromaCollection {
        let client = ChromaHttpClient::new(ChromaHttpClientOptions {
            endpoint: server.base_url().parse().unwrap(),
            tenant_id: Some("tenant".to_string()),
            database_name: Some("database".to_string()),
            retry_options: ChromaRetryOptions {
                max_retries: 0,
                ..Default::default()
            },
            ..Default::default()
        });
        ChromaCollection::new(
            client,
            Collection {
                tenant: "tenant".to_string(),
                database: "database".to_string(),
                ..Default::default()
            },
        )
    }

    fn collection_path(collection: &ChromaCollection, suffix: &str) -> String {
        format!(
            "/api/v2/tenants/tenant/databases/database/collections/{}/{}",
            collection.id(),
            suffix
        )
    }

    #[tokio::test]
    async fn manual_commit_replays_reads_and_buffered_writes() {
        let server = MockServer::start_async().await;
        let collection = collection(&server);
        let get_path = collection_path(&collection, "conditional/get");
        let commit_path = collection_path(&collection, "conditional/commit");

        let get_mock = server
            .mock_async(|when, then| {
                when.method("POST").path(get_path).json_body(json!({
                    "ids": ["id1"],
                    "where": null,
                    "where_document": null,
                    "limit": null,
                    "offset": 0,
                    "include": ["documents", "metadatas"],
                    "read_token": null,
                }));
                then.status(200).json_body(json!({
                    "ids": [],
                    "embeddings": null,
                    "documents": null,
                    "uris": null,
                    "metadatas": null,
                    "include": ["documents", "metadatas"],
                    "read_token": 42,
                }));
            })
            .await;
        let commit_mock = server
            .mock_async(|when, then| {
                when.method("POST").path(commit_path).json_body(json!({
                    "read_token": 42,
                    "operations": [
                        {
                            "operation": "get",
                            "payload": {
                                "ids": ["id1"],
                                "where": null,
                                "where_document": null,
                                "limit": null,
                                "offset": 0,
                                "include": ["documents", "metadatas"],
                                "expected_ids": [],
                            },
                        },
                        {
                            "operation": "add",
                            "payload": {
                                "ids": ["id1"],
                                "embeddings": [[1.0, 2.0]],
                                "documents": null,
                                "uris": null,
                                "metadatas": null,
                            },
                        },
                    ],
                }));
                then.status(200).json_body(json!({
                    "first_inserted_record_offset": 7,
                    "record_count": 1,
                }));
            })
            .await;

        let mut txn = collection.conditional();
        let response = txn
            .get(Some(vec!["id1".to_string()]), None, None, None, None)
            .await
            .unwrap();
        assert!(response.ids.is_empty());
        txn.add(
            vec!["id1".to_string()],
            vec![vec![1.0, 2.0]],
            None,
            None,
            None,
        )
        .await
        .unwrap();

        let result = txn.commit().await.unwrap();

        assert_eq!(result.first_inserted_record_offset, Some(7));
        assert_eq!(result.record_count, 1);
        get_mock.assert();
        commit_mock.assert();
    }

    #[tokio::test]
    async fn dropping_transaction_discards_buffered_writes() {
        let server = MockServer::start_async().await;
        let collection = collection(&server);
        let commit_path = collection_path(&collection, "conditional/commit");
        let commit_mock = server
            .mock_async(|when, then| {
                when.method("POST").path(commit_path);
                then.status(200).json_body(json!({
                    "first_inserted_record_offset": 7,
                    "record_count": 1,
                }));
            })
            .await;

        {
            let mut txn = collection.conditional();
            txn.upsert(
                vec!["id1".to_string()],
                vec![vec![1.0, 2.0]],
                None,
                None,
                None,
            )
            .await
            .unwrap();
        }

        assert_eq!(commit_mock.calls(), 0);
    }

    #[tokio::test]
    async fn run_retries_commit_conflict_with_fresh_transaction() {
        let server = MockServer::start_async().await;
        let collection = collection(&server);
        let commit_path = collection_path(&collection, "conditional/commit");
        let commit_calls = Arc::new(AtomicUsize::new(0));
        let commit_mock = server
            .mock_async(|when, then| {
                when.method("POST").path(commit_path);
                let commit_calls = Arc::clone(&commit_calls);
                then.respond_with(move |_| {
                    if commit_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        return HttpMockResponse::builder()
                            .status(409)
                            .body(
                                json!({
                                    "error": "ConditionalWriteConflictError",
                                    "message": "conditional write conflict",
                                })
                                .to_string(),
                            )
                            .build();
                    }
                    HttpMockResponse::builder()
                        .status(200)
                        .body(
                            json!({
                                "first_inserted_record_offset": 8,
                                "record_count": 1,
                            })
                            .to_string(),
                        )
                        .build()
                });
            })
            .await;
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = collection
            .conditional()
            .run(
                async |txn| {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    txn.upsert(
                        vec![format!("id{attempt}")],
                        vec![vec![attempt as f32]],
                        None,
                        None,
                        None,
                    )
                    .await?;
                    Ok(format!("value{attempt}"))
                },
                1,
            )
            .await
            .unwrap();

        assert_eq!(result, "value2");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(commit_mock.calls(), 2);
    }

    #[tokio::test]
    async fn run_does_not_retry_user_created_retryable_error() {
        let server = MockServer::start_async().await;
        let collection = collection(&server);
        let attempts = Arc::new(AtomicUsize::new(0));

        let err = collection
            .conditional()
            .run(
                async |_txn| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(ChromaHttpClientError::ApiError(
                        "ConditionalWriteConflictError: conditional write conflict".to_string(),
                        StatusCode::CONFLICT,
                    ))
                },
                3,
            )
            .await
            .unwrap_err();

        assert!(is_conditional_run_retryable_error(&err));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
