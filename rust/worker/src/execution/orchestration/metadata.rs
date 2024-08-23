use crate::execution::dispatcher::Dispatcher;
use crate::execution::operator::{wrap, TaskResult};
use crate::execution::operators::merge_metadata_results::{
    MergeMetadataResultsOperator, MergeMetadataResultsOperatorError,
    MergeMetadataResultsOperatorInput, MergeMetadataResultsOperatorOutput,
};
use crate::execution::operators::metadata_filtering::{
    MetadataFilteringError, MetadataFilteringInput, MetadataFilteringOperator,
    MetadataFilteringOutput,
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
use chroma_types::{Chunk, Segment};
use chroma_types::{Collection, LogRecord, Metadata, SegmentType};
use chroma_types::{Where, WhereDocument};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::Span;
use uuid::Uuid;

#[derive(Debug)]
enum ExecutionState {
    Pending,
    PullLogs,
    Filter, // Filter logs and search metadata segment
    MergeResults,
}

// Returns the ids, metadata, and documents
type MetadataQueryOrchestratorResult =
    Result<(Vec<String>, Vec<Option<Metadata>>, Vec<Option<String>>), Box<dyn ChromaError>>;

#[derive(Debug)]
pub(crate) struct MetadataQueryOrchestrator {
    state: ExecutionState,
    // Component Execution
    system: System,
    // Query state
    metadata_segment_id: Uuid,
    collection_id: Uuid,
    query_ids: Option<Vec<String>>,
    // State fetched or created for query execution
    record_segment: Option<Segment>,
    metadata_segment: Option<Segment>,
    collection: Option<Collection>,
    // State machine management
    merge_dependency_count: u32,
    // Services
    log: Box<Log>,
    sysdb: Box<SysDb>,
    dispatcher: ComponentHandle<Dispatcher>,
    blockfile_provider: BlockfileProvider,
    // Query params
    where_clause: Option<Where>,
    where_document_clause: Option<WhereDocument>,
    // Result channel
    result_channel: Option<tokio::sync::oneshot::Sender<MetadataQueryOrchestratorResult>>,
}

#[derive(Error, Debug)]
enum MetadataQueryOrchestratorError {
    #[error("Blockfile metadata segment with id: {0} not found")]
    BlockfileMetadataSegmentNotFound(Uuid),
    #[error("Get segments error: {0}")]
    GetSegmentsError(#[from] GetSegmentsError),
    #[error("Record segment not found for collection: {0}")]
    RecordSegmentNotFound(Uuid),
    #[error("System Time Error")]
    SystemTimeError(#[from] std::time::SystemTimeError),
    #[error("Collection not found for id: {0}")]
    CollectionNotFound(Uuid),
    #[error("Get collection error: {0}")]
    GetCollectionError(#[from] GetCollectionsError),
}

impl ChromaError for MetadataQueryOrchestratorError {
    fn code(&self) -> ErrorCodes {
        match self {
            MetadataQueryOrchestratorError::BlockfileMetadataSegmentNotFound(_) => {
                ErrorCodes::NotFound
            }
            MetadataQueryOrchestratorError::GetSegmentsError(e) => e.code(),
            MetadataQueryOrchestratorError::RecordSegmentNotFound(_) => ErrorCodes::NotFound,
            MetadataQueryOrchestratorError::SystemTimeError(_) => ErrorCodes::Internal,
            MetadataQueryOrchestratorError::CollectionNotFound(_) => ErrorCodes::NotFound,
            MetadataQueryOrchestratorError::GetCollectionError(e) => e.code(),
        }
    }
}

impl MetadataQueryOrchestrator {
    pub(crate) fn new(
        system: System,
        metadata_segment_id: &Uuid,
        collection_id: &Uuid,
        query_ids: Option<Vec<String>>,
        log: Box<Log>,
        sysdb: Box<SysDb>,
        dispatcher: ComponentHandle<Dispatcher>,
        blockfile_provider: BlockfileProvider,
        where_clause: Option<Where>,
        where_document_clause: Option<WhereDocument>,
    ) -> Self {
        Self {
            state: ExecutionState::Pending,
            system,
            metadata_segment_id: *metadata_segment_id,
            collection_id: *collection_id,
            query_ids,
            record_segment: None,
            metadata_segment: None,
            collection: None,
            merge_dependency_count: 2,
            log,
            sysdb,
            dispatcher,
            blockfile_provider,
            where_clause,
            where_document_clause,
            result_channel: None,
        }
    }

    async fn start(&mut self, ctx: &ComponentContext<Self>) {
        tracing::info!("Starting Metadata Query Orchestrator");
        // Populate the orchestrator with the initial state - The Metadata Segment, The Record Segment and the Collection
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
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        let collection_id = metadata_segment.collection;
        self.metadata_segment = Some(metadata_segment);

        let record_segment = self
            .get_record_segment_from_collection_id(self.sysdb.clone(), &collection_id)
            .await;

        let record_segment = match record_segment {
            Ok(segment) => segment,
            Err(e) => {
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
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        self.record_segment = Some(record_segment);
        self.collection = Some(collection);
    }

    async fn pull_logs(&mut self, ctx: &ComponentContext<Self>) {
        println!("Pulling logs");
        self.state = ExecutionState::PullLogs;

        let operator = PullLogsOperator::new(self.log.clone());
        let end_timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
        let end_timestamp = match end_timestamp {
            Ok(end_timestamp) => end_timestamp.as_nanos() as i64,
            Err(e) => {
                terminate_with_error(
                    self.result_channel.take(),
                    Box::new(MetadataQueryOrchestratorError::SystemTimeError(e)),
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
            collection.id,
            // The collection log position is inclusive, and we want to start from the next log.
            collection.log_position + 1,
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
                println!("Error sending Metadata Query task: {:?}", e);
            }
        }
    }

    async fn filter(&mut self, mut logs: Chunk<LogRecord>, ctx: &ComponentContext<Self>) {
        tracing::debug!("Filtering logs and searching metadata segment");
        self.state = ExecutionState::Filter;

        let input = MetadataFilteringInput::new(
            logs,
            self.record_segment
                .as_ref()
                .expect("Expected record segment to be set")
                .clone(),
            self.metadata_segment
                .as_ref()
                .expect("Expected metadata segment to be set")
                .clone(),
            self.blockfile_provider.clone(),
            self.where_clause.clone(),
            self.where_document_clause.clone(),
            self.query_ids.clone(),
        );

        let op = MetadataFilteringOperator::new();
        let task = wrap(op, input, ctx.receiver());
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                // Log an error - this implies the dispatcher was dropped somehow
                // and is likely fatal
                println!("Error sending Metadata Query task: {:?}", e);
            }
        }
    }

    async fn get_metadata_segment_from_id(
        &self,
        mut sysdb: Box<SysDb>,
        metadata_segment_id: &Uuid,
        collection_id: &Uuid,
    ) -> Result<Segment, Box<dyn ChromaError>> {
        let segments = sysdb
            .get_segments(Some(*metadata_segment_id), None, None, *collection_id)
            .await;
        let segment = match segments {
            Ok(segments) => {
                if segments.is_empty() {
                    return Err(Box::new(
                        MetadataQueryOrchestratorError::BlockfileMetadataSegmentNotFound(
                            *metadata_segment_id,
                        ),
                    ));
                }
                segments[0].clone()
            }
            Err(e) => {
                return Err(Box::new(MetadataQueryOrchestratorError::GetSegmentsError(
                    e,
                )));
            }
        };

        if segment.r#type != SegmentType::BlockfileMetadata {
            return Err(Box::new(
                MetadataQueryOrchestratorError::BlockfileMetadataSegmentNotFound(
                    *metadata_segment_id,
                ),
            ));
        }
        Ok(segment)
    }

    async fn get_record_segment_from_collection_id(
        &self,
        mut sysdb: Box<SysDb>,
        collection_id: &Uuid,
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
                        MetadataQueryOrchestratorError::RecordSegmentNotFound(*collection_id),
                    ));
                }
                // Unwrap is safe as we know at least one segment exists from
                // the check above
                return Ok(segments.into_iter().next().unwrap());
            }
            Err(e) => {
                return Err(Box::new(MetadataQueryOrchestratorError::GetSegmentsError(
                    e,
                )));
            }
        };
    }

    async fn get_collection_from_id(
        &self,
        mut sysdb: Box<SysDb>,
        collection_id: &Uuid,
        ctx: &ComponentContext<Self>,
    ) -> Result<Collection, Box<dyn ChromaError>> {
        let collections = sysdb
            .get_collections(Some(*collection_id), None, None, None)
            .await;

        match collections {
            Ok(collections) => {
                if collections.is_empty() {
                    return Err(Box::new(
                        MetadataQueryOrchestratorError::CollectionNotFound(*collection_id),
                    ));
                }
                // Unwrap is safe as we know at least one collection exists from
                // the check above
                return Ok(collections.into_iter().next().unwrap());
            }
            Err(e) => {
                return Err(Box::new(
                    MetadataQueryOrchestratorError::GetCollectionError(e),
                ));
            }
        };
    }

    ///  Run the orchestrator and return the result.
    ///  # Note
    ///  Use this over spawning the component directly. This method will start the component and
    ///  wait for it to finish before returning the result.
    pub(crate) async fn run(mut self) -> MetadataQueryOrchestratorResult {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = self.system.clone().start_component(self);
        let result = rx.await;
        handle.stop();
        result.unwrap()
    }
}

#[async_trait]
impl Component for MetadataQueryOrchestrator {
    fn get_name() -> &'static str {
        "Metadata Query Orchestrator"
    }

    fn queue_size(&self) -> usize {
        1000 // TODO: make this configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        self.start(ctx).await;
        self.pull_logs(ctx).await;
    }
}

#[async_trait]
impl Handler<TaskResult<PullLogsOutput, PullLogsError>> for MetadataQueryOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PullLogsOutput, PullLogsError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = message.into_inner();
        match message {
            Ok(logs) => {
                let logs = logs.logs();
                self.filter(logs, ctx).await;
            }
            Err(e) => {
                tracing::error!("Error pulling logs: {:?}", e);
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<MetadataFilteringOutput, MetadataFilteringError>>
    for MetadataQueryOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MetadataFilteringOutput, MetadataFilteringError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = message.into_inner();
        let output = match message {
            Ok(output) => output,
            Err(e) => {
                tracing::error!("Error merging metadata results: {:?}", e);
                return terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        };

        self.state = ExecutionState::MergeResults;

        let operator = MergeMetadataResultsOperator::new();
        let input = MergeMetadataResultsOperatorInput::new(
            output.log_records,
            output.user_supplied_filtered_offset_ids,
            output.where_condition_filtered_offset_ids,
            self.record_segment
                .as_ref()
                .expect("Invariant violation. Record segment is not set.")
                .clone(),
            self.blockfile_provider.clone(),
        );

        let task = wrap(operator, input, ctx.receiver());
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                // Log an error - this implies the dispatcher was dropped somehow
                // and is likely fatal
                println!("Error sending Metadata Query task: {:?}", e);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<MergeMetadataResultsOperatorOutput, MergeMetadataResultsOperatorError>>
    for MetadataQueryOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MergeMetadataResultsOperatorOutput, MergeMetadataResultsOperatorError>,
        ctx: &ComponentContext<Self>,
    ) {
        let message = message.into_inner();
        let output = match message {
            Ok(output) => output,
            Err(e) => {
                tracing::error!("Error merging metadata results: {:?}", e);
                return terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        };

        let result_channel = self
            .result_channel
            .take()
            .expect("Invariant violation. Result channel is not set.");

        let output = (output.ids, output.metadata, output.documents);
        tracing::trace!("Merged metadata results: {:?}", output);

        match result_channel.send(Ok(output)) {
            Ok(_) => (),
            Err(e) => {
                // Log an error - this implied the listener was dropped
                println!(
                    "[MetadataQueryOrchestrator] Result channel dropped before sending result"
                );
            }
        }
    }
}
