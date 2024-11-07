use crate::execution::dispatcher::Dispatcher;
use crate::execution::operator::{wrap, TaskResult};
use crate::execution::operators::count_records::{
    CountRecordsError, CountRecordsInput, CountRecordsOperator, CountRecordsOutput,
};
use crate::execution::operators::pull_log::{PullLogsInput, PullLogsOperator, PullLogsOutput};
use crate::execution::orchestration::common::terminate_with_error;
use crate::log::log::PullLogsError;
use crate::sysdb::sysdb::{GetCollectionsError, GetSegmentsError};
use crate::system::{Component, ComponentContext, ComponentHandle, Handler};
use crate::{log::log::Log, sysdb::sysdb::SysDb, system::System};
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Collection, CollectionUuid, Segment, SegmentType, SegmentUuid};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::Span;
use uuid::Uuid;

#[derive(Debug)]
pub(crate) struct CountQueryOrchestrator {
    // Component Execution
    system: System,
    // Query state
    metadata_segment_id: Uuid,
    collection_id: CollectionUuid,
    // State fetched or created for query execution
    record_segment: Option<Segment>,
    collection: Option<Collection>,
    // Services
    log: Box<Log>,
    sysdb: Box<SysDb>,
    dispatcher: ComponentHandle<Dispatcher>,
    blockfile_provider: BlockfileProvider,
    // Result channel
    result_channel: Option<tokio::sync::oneshot::Sender<Result<usize, Box<dyn ChromaError>>>>,
    // Request version context
    collection_version: u32,
    log_position: u64,
}

#[derive(Error, Debug)]
enum CountQueryOrchestratorError {
    #[error("Blockfile metadata segment with id: {0} not found")]
    BlockfileMetadataSegmentNotFound(Uuid),
    #[error("Get segments error: {0}")]
    GetSegmentsError(#[from] GetSegmentsError),
    #[error("Record segment not found for collection: {0}")]
    RecordSegmentNotFound(CollectionUuid),
    #[error("System Time Error")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Collection not found for id: {0}")]
    CollectionNotFound(CollectionUuid),
    #[error("Get collection error: {0}")]
    GetCollectionError(#[from] GetCollectionsError),
    #[error("Collection version mismatch")]
    CollectionVersionMismatch,
}

impl ChromaError for CountQueryOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            CountQueryOrchestratorError::BlockfileMetadataSegmentNotFound(_) => {
                ErrorCodes::NotFound
            }
            CountQueryOrchestratorError::GetSegmentsError(e) => e.code(),
            CountQueryOrchestratorError::RecordSegmentNotFound(_) => ErrorCodes::NotFound,
            CountQueryOrchestratorError::SystemTimeError(_) => ErrorCodes::Internal,
            CountQueryOrchestratorError::CollectionNotFound(_) => ErrorCodes::NotFound,
            CountQueryOrchestratorError::GetCollectionError(e) => e.code(),
            CountQueryOrchestratorError::CollectionVersionMismatch => ErrorCodes::VersionMismatch,
        }
    }
}

impl CountQueryOrchestrator {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        system: System,
        metadata_segment_id: &Uuid,
        collection_id: &CollectionUuid,
        log: Box<Log>,
        sysdb: Box<SysDb>,
        dispatcher: ComponentHandle<Dispatcher>,
        blockfile_provider: BlockfileProvider,
        collection_version: u32,
        log_position: u64,
    ) -> Self {
        Self {
            system,
            metadata_segment_id: *metadata_segment_id,
            collection_id: *collection_id,
            record_segment: None,
            collection: None,
            log,
            sysdb,
            dispatcher,
            blockfile_provider,
            result_channel: None,
            collection_version,
            log_position,
        }
    }

    async fn start(&mut self, ctx: &ComponentContext<Self>) {
        println!("Starting Count Query Orchestrator");
        // Populate the orchestrator with the initial state - The Record Segment and the Collection
        let metdata_segment = self
            .get_metadata_segment_from_id(
                self.sysdb.clone(),
                &self.metadata_segment_id,
                &self.collection_id,
            )
            .await;

        let metadata_segment = match metdata_segment {
            Ok(segment) => segment,
            Err(e) => {
                tracing::error!("Error getting metadata segment: {:?}", e);
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        let collection_id = metadata_segment.collection;

        let record_segment = self
            .get_record_segment_from_collection_id(self.sysdb.clone(), &collection_id)
            .await;

        let record_segment = match record_segment {
            Ok(segment) => segment,
            Err(e) => {
                tracing::error!("Error getting record segment: {:?}", e);
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        let collection = match self
            .get_collection_from_id(self.sysdb.clone(), &collection_id, ctx)
            .await
        {
            Ok(collection) => collection,
            Err(e) => {
                tracing::error!("Error getting collection: {:?}", e);
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        // If the collection version does not match the request version then we terminate with an error
        if collection.version as u32 != self.collection_version {
            terminate_with_error(
                self.result_channel.take(),
                Box::new(CountQueryOrchestratorError::CollectionVersionMismatch),
                ctx,
            );
            return;
        }

        self.record_segment = Some(record_segment);
        self.collection = Some(collection);
        self.pull_logs(ctx).await;
    }

    // shared
    async fn pull_logs(&mut self, ctx: &ComponentContext<Self>) {
        println!("Count query orchestrator pulling logs");

        let operator = PullLogsOperator::new(self.log.clone());
        let end_timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
        let end_timestamp = match end_timestamp {
            Ok(end_timestamp) => end_timestamp.as_nanos() as i64,
            Err(e) => {
                tracing::error!("Error getting system time: {:?}", e);
                terminate_with_error(
                    self.result_channel.take(),
                    Box::new(CountQueryOrchestratorError::SystemTimeError(e)),
                    ctx,
                );
                return;
            }
        };

        let collection = self
            .collection
            .as_ref()
            .expect("Invariant violation. Collection is not set before pull logs state.");
        let input = PullLogsInput::new(
            collection.collection_id,
            // The collection log position is inclusive, and we want to start from the next log.
            // Note that we query using the incoming log position this is critical for correctness
            // TODO: We should make all the log service code use u64 instead of i64
            (self.log_position as i64) + 1,
            100,
            None,
            Some(end_timestamp),
        );

        let task = wrap(operator, input, ctx.receiver());
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                // Log an error - this implies the dispatcher was dropped somehow
                // and is likely fatal
                println!("Error sending Count Query task: {:?}", e);
            }
        }
    }

    // shared
    async fn get_metadata_segment_from_id(
        &self,
        mut sysdb: Box<SysDb>,
        metadata_segment_id: &Uuid,
        collection_id: &CollectionUuid,
    ) -> Result<Segment, Box<dyn ChromaError>> {
        let segments = sysdb
            .get_segments(
                Some(SegmentUuid(*metadata_segment_id)),
                None,
                None,
                *collection_id,
            )
            .await;
        let segment = match segments {
            Ok(segments) => {
                if segments.is_empty() {
                    return Err(Box::new(
                        CountQueryOrchestratorError::BlockfileMetadataSegmentNotFound(
                            *metadata_segment_id,
                        ),
                    ));
                }
                segments[0].clone()
            }
            Err(e) => {
                return Err(Box::new(CountQueryOrchestratorError::GetSegmentsError(e)));
            }
        };

        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(Box::new(
                CountQueryOrchestratorError::BlockfileMetadataSegmentNotFound(*metadata_segment_id),
            ));
        }
        Ok(segment)
    }

    // shared
    async fn get_record_segment_from_collection_id(
        &self,
        mut sysdb: Box<SysDb>,
        collection_id: &CollectionUuid,
    ) -> Result<Segment, Box<dyn ChromaError>> {
        let segments = sysdb
            .get_segments(
                None,
                Some(SegmentType::BlockfileRecord.into()),
                None,
                *collection_id,
            )
            .await;

        match segments {
            Ok(segments) => {
                if segments.is_empty() {
                    return Err(Box::new(
                        CountQueryOrchestratorError::RecordSegmentNotFound(*collection_id),
                    ));
                }
                // Unwrap is safe as we know at least one segment exists from
                // the check above
                Ok(segments.into_iter().next().unwrap())
            }
            Err(e) => Err(Box::new(CountQueryOrchestratorError::GetSegmentsError(e))),
        }
    }

    // shared
    async fn get_collection_from_id(
        &self,
        mut sysdb: Box<SysDb>,
        collection_id: &CollectionUuid,
        _ctx: &ComponentContext<Self>,
    ) -> Result<Collection, Box<dyn ChromaError>> {
        let collections = sysdb
            .get_collections(Some(*collection_id), None, None, None)
            .await;

        match collections {
            Ok(collections) => {
                if collections.is_empty() {
                    return Err(Box::new(CountQueryOrchestratorError::CollectionNotFound(
                        *collection_id,
                    )));
                }
                // Unwrap is safe as we know at least one collection exists from
                // the check above
                Ok(collections.into_iter().next().unwrap())
            }
            Err(e) => Err(Box::new(CountQueryOrchestratorError::GetCollectionError(e))),
        }
    }

    ///  Run the orchestrator and return the result.
    ///  # Note
    ///  Use this over spawning the component directly. This method will start the component and
    ///  wait for it to finish before returning the result.
    pub(crate) async fn run(mut self) -> Result<usize, Box<dyn ChromaError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = self.system.clone().start_component(self);
        let result = rx.await;
        handle.stop();
        result.unwrap()
    }
}

#[async_trait]
impl Component for CountQueryOrchestrator {
    fn get_name() -> &'static str {
        "Count Query Orchestrator"
    }

    fn queue_size(&self) -> usize {
        1000 // TODO: make this configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        self.start(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<PullLogsOutput, PullLogsError>> for CountQueryOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PullLogsOutput, PullLogsError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = message.into_inner();
        match message {
            Ok(logs) => {
                let operator = CountRecordsOperator::new();
                let input = CountRecordsInput::new(
                    self.record_segment
                        .as_ref()
                        .expect("Expect segment")
                        .clone(),
                    self.blockfile_provider.clone(),
                    logs.logs(),
                );
                let msg = wrap(operator, input, ctx.receiver());
                match self.dispatcher.send(msg, None).await {
                    Ok(_) => (),
                    Err(e) => {
                        // Log an error - this implies the dispatcher was dropped somehow
                        // and is likely fatal
                        println!("Error sending Count Query task: {:?}", e);
                    }
                }
            }
            Err(e) => {
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<CountRecordsOutput, CountRecordsError>> for CountQueryOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<CountRecordsOutput, CountRecordsError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = message.into_inner();
        let msg = match message {
            Ok(m) => m,
            Err(e) => {
                return terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        };
        let channel = self
            .result_channel
            .take()
            .expect("Expect channel to be present");
        match channel.send(Ok(msg.count)) {
            Ok(_) => (),
            Err(_) => {
                // Log an error - this implied the listener was dropped
                println!("[CountQueryOrchestrator] Result channel dropped before sending result");
            }
        }
    }
}
