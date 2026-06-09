use crate::fn_consumer::config::GrpcWorkQueueConfig;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::chroma_proto::{
    work_queue_service_client::WorkQueueServiceClient, FinishWorkRequest,
    FlushCollectionCompactionRequest, GetWorkRequest, GetWorkResponse, PushWorkRequest,
    WorkItemResult,
};
use chroma_types::CollectionFlushInfo;
use std::time::Duration;
use tonic::transport::Endpoint;
use tonic::Request;
use tower::ServiceBuilder;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WorkQueueClient {
    client:
        WorkQueueServiceClient<chroma_tracing::GrpcClientTraceService<tonic::transport::Channel>>,
}

#[allow(dead_code)]
impl WorkQueueClient {
    pub async fn try_from_config(
        config: &GrpcWorkQueueConfig,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let endpoint = format!("http://{}:{}", config.host, config.port);

        let endpoint = Endpoint::from_shared(endpoint)
            .map_err(|e| {
                Box::new(WorkQueueClientError::ConnectionError(e.to_string()))
                    as Box<dyn ChromaError>
            })?
            .connect_timeout(Duration::from_millis(config.connect_timeout_ms))
            .timeout(Duration::from_millis(config.request_timeout_ms));

        let channel = endpoint.connect().await.map_err(|e| {
            let err: Box<dyn ChromaError> =
                Box::new(WorkQueueClientError::ConnectionError(e.to_string()));
            err
        })?;
        let channel = ServiceBuilder::new()
            .layer(chroma_tracing::GrpcClientTraceLayer)
            .service(channel);
        let client = WorkQueueServiceClient::new(channel);

        Ok(Self { client })
    }

    pub async fn new(endpoint: String) -> Result<Self, Box<dyn ChromaError>> {
        let channel = Endpoint::from_shared(endpoint)
            .map_err(|e| {
                Box::new(WorkQueueClientError::ConnectionError(e.to_string()))
                    as Box<dyn ChromaError>
            })?
            .connect()
            .await
            .map_err(|e| {
                let err: Box<dyn ChromaError> =
                    Box::new(WorkQueueClientError::ConnectionError(e.to_string()));
                err
            })?;
        let channel = ServiceBuilder::new()
            .layer(chroma_tracing::GrpcClientTraceLayer)
            .service(channel);
        let client = WorkQueueServiceClient::new(channel);

        Ok(Self { client })
    }

    pub async fn push_work(
        &mut self,
        fn_id: String,
        input_coll_id: String,
        completion_offset: i64,
    ) -> Result<(), Box<dyn ChromaError>> {
        let request = Request::new(PushWorkRequest {
            fn_id,
            input_coll_id,
            completion_offset,
        });

        self.client
            .push_work(request)
            .await
            .map_err(|e| Box::new(WorkQueueClientError::RequestError(e)) as Box<dyn ChromaError>)?;

        Ok(())
    }

    pub async fn finish_work(
        &mut self,
        fn_id: String,
        input_coll_id: String,
        completion_offset: i64,
        output_collection_flush: CollectionFlushInfo,
    ) -> Result<(), Box<dyn ChromaError>> {
        self.finish_work_batch(
            vec![(fn_id, input_coll_id, completion_offset)],
            output_collection_flush,
        )
        .await
    }

    pub async fn finish_work_batch(
        &mut self,
        work_items: Vec<(String, String, i64)>,
        output_collection_flush: CollectionFlushInfo,
    ) -> Result<(), Box<dyn ChromaError>> {
        let Some((first_fn_id, first_input_coll_id, first_completion_offset)) = work_items.first()
        else {
            return Err(Box::new(WorkQueueClientError::ConversionError(
                "finish_work_batch requires at least one work item".to_string(),
            )));
        };

        let request = Request::new(FinishWorkRequest {
            fn_id: first_fn_id.clone(),
            input_coll_id: first_input_coll_id.clone(),
            completion_offset: *first_completion_offset,
            output_collection_flush: Some(
                FlushCollectionCompactionRequest::try_from(output_collection_flush).map_err(
                    |e| {
                        Box::new(WorkQueueClientError::ConversionError(e.to_string()))
                            as Box<dyn ChromaError>
                    },
                )?,
            ),
            work_items: work_items
                .into_iter()
                .map(|(fn_id, input_coll_id, completion_offset)| WorkItemResult {
                    fn_id,
                    input_coll_id,
                    completion_offset,
                })
                .collect(),
        });

        self.client.finish_work(request).await.map_err(|e| {
            let err: Box<dyn ChromaError> = Box::new(WorkQueueClientError::RequestError(e));
            err
        })?;

        Ok(())
    }

    pub async fn get_work(
        &mut self,
        shard_id: String,
        limit: u32,
    ) -> Result<GetWorkResponse, Box<dyn ChromaError>> {
        let request = Request::new(GetWorkRequest { shard_id, limit });

        let response =
            self.client.get_work(request).await.map_err(|e| {
                Box::new(WorkQueueClientError::RequestError(e)) as Box<dyn ChromaError>
            })?;

        Ok(response.into_inner())
    }
}

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum WorkQueueClientError {
    #[error("Failed to connect: {0}")]
    ConnectionError(String),

    #[error("Request failed: {0}")]
    RequestError(tonic::Status),

    #[error("Failed to convert request: {0}")]
    ConversionError(String),
}

impl ChromaError for WorkQueueClientError {
    fn code(&self) -> ErrorCodes {
        match self {
            WorkQueueClientError::ConnectionError(_) => ErrorCodes::Unavailable,
            WorkQueueClientError::ConversionError(_) => ErrorCodes::Internal,
            WorkQueueClientError::RequestError(status) => match status.code() {
                tonic::Code::Unavailable => ErrorCodes::Unavailable,
                tonic::Code::DeadlineExceeded => ErrorCodes::DeadlineExceeded,
                tonic::Code::ResourceExhausted => ErrorCodes::ResourceExhausted,
                tonic::Code::Aborted => ErrorCodes::Aborted,
                tonic::Code::InvalidArgument => ErrorCodes::InvalidArgument,
                tonic::Code::NotFound => ErrorCodes::NotFound,
                tonic::Code::AlreadyExists => ErrorCodes::AlreadyExists,
                tonic::Code::PermissionDenied => ErrorCodes::PermissionDenied,
                tonic::Code::Unauthenticated => ErrorCodes::Unauthenticated,
                tonic::Code::FailedPrecondition => ErrorCodes::FailedPrecondition,
                tonic::Code::OutOfRange => ErrorCodes::OutOfRange,
                tonic::Code::Unimplemented => ErrorCodes::Unimplemented,
                tonic::Code::Internal => ErrorCodes::Internal,
                tonic::Code::DataLoss => ErrorCodes::Internal,
                tonic::Code::Unknown => ErrorCodes::Internal,
                _ => ErrorCodes::Internal,
            },
        }
    }
}
