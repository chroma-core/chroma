use super::super::operator::wrap;
use super::super::operators::pull_log::{PullLogsInput, PullLogsOperator};
use super::common::{
    get_collection_by_id, get_hnsw_segment_by_id, get_record_segment_by_collection_id,
    terminate_with_error,
};
use crate::distance::DistanceFunction;
use crate::execution::data::data_chunk::Chunk;
use crate::execution::dispatcher::Dispatcher;
use crate::execution::operator::TaskResult;
use crate::execution::operators::brute_force_knn::{
    BruteForceKnnOperator, BruteForceKnnOperatorError, BruteForceKnnOperatorInput,
    BruteForceKnnOperatorOutput,
};
use crate::execution::operators::hnsw_knn::{
    HnswKnnOperator, HnswKnnOperatorInput, HnswKnnOperatorOutput,
};
use crate::execution::operators::merge_knn_results::{
    MergeKnnResultsOperator, MergeKnnResultsOperatorInput, MergeKnnResultsOperatorOutput,
};
use crate::execution::operators::normalize_vectors::normalize;
use crate::execution::operators::pull_log::PullLogsOutput;
use crate::execution::operators::record_segment_prefetch::{
    Keys, OffsetIdToDataKeys, OffsetIdToUserIdKeys, RecordSegmentPrefetchIoInput,
    RecordSegmentPrefetchIoOperator, RecordSegmentPrefetchIoOperatorError,
    RecordSegmentPrefetchIoOutput,
};
use crate::index::hnsw_provider::HnswIndexProvider;
use crate::index::IndexConfig;
use crate::log::log::PullLogsError;
use crate::segment::distributed_hnsw_segment::{
    DistributedHNSWSegmentFromSegmentError, DistributedHNSWSegmentReader,
};
use crate::sysdb::sysdb::{GetCollectionsError, GetSegmentsError, SysDb};
use crate::system::{ComponentContext, ComponentHandle, System};
use crate::{
    log::log::Log,
    system::{Component, Handler, ReceiverForMessage},
};
use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{Collection, LogRecord, Segment, VectorQueryResult};
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tracing::{trace, Span};
use uuid::Uuid;

/**  The state of the orchestrator.
In chroma, we have a relatively fixed number of query plans that we can execute. Rather
than a flexible state machine abstraction, we just manually define the states that we
expect to encounter for a given query plan. This is a bit more rigid, but it's also simpler and easier to
understand. We can always add more abstraction later if we need it.
```plaintext

                               ┌───► Brute Force ─────┐
                               │                      │
  Pending ─► PullLogs ─► Group │                      ├─► MergeResults ─► Finished
                               │                      │
                               └───► HNSW ────────────┘

```
*/
#[derive(Debug)]
enum ExecutionState {
    Pending,
    PullLogs,
    Partition,
    QueryKnn, // This is both the Brute force and HNSW query state
    MergeResults,
    Finished,
}

#[derive(Error, Debug)]
enum HnswSegmentQueryError {
    #[error(transparent)]
    HnswSegmentQueryError(#[from] super::common::GetHnswSegmentByIdError),
    #[error("Get segments error: {0}")]
    GetSegmentsError(#[from] GetSegmentsError),
    #[error("Collection: {0} not found")]
    CollectionNotFound(Uuid),
    #[error("Get collection error: {0}")]
    GetCollectionError(#[from] GetCollectionsError),
    #[error("Record segment not found for collection: {0}")]
    RecordSegmentNotFound(Uuid),
    #[error("HNSW segment has no collection")]
    HnswSegmentHasNoCollection,
    #[error("Collection has no dimension set")]
    CollectionHasNoDimension,
}

impl ChromaError for HnswSegmentQueryError {
    fn code(&self) -> ErrorCodes {
        match self {
            HnswSegmentQueryError::HnswSegmentQueryError(e) => e.code(),
            HnswSegmentQueryError::GetSegmentsError(_) => ErrorCodes::Internal,
            HnswSegmentQueryError::CollectionNotFound(_) => ErrorCodes::NotFound,
            HnswSegmentQueryError::GetCollectionError(_) => ErrorCodes::Internal,
            HnswSegmentQueryError::RecordSegmentNotFound(_) => ErrorCodes::NotFound,
            HnswSegmentQueryError::HnswSegmentHasNoCollection => ErrorCodes::InvalidArgument,
            HnswSegmentQueryError::CollectionHasNoDimension => ErrorCodes::InvalidArgument,
        }
    }
}

#[derive(Debug)]
pub(crate) struct HnswQueryOrchestrator {
    state: ExecutionState,
    // Component Execution
    system: System,
    // Query state
    query_vectors: Vec<Vec<f32>>,
    k: i32,
    allowed_ids: Arc<[String]>,
    include_embeddings: bool,
    hnsw_segment_id: Uuid,
    // State fetched or created for query execution
    hnsw_segment: Option<Segment>,
    record_segment: Option<Segment>,
    collection: Option<Collection>,
    index_config: Option<IndexConfig>,
    // query_vectors index to the result
    hnsw_result_offset_ids: HashMap<usize, Vec<usize>>,
    hnsw_result_distances: HashMap<usize, Vec<f32>>,
    brute_force_result_user_ids: HashMap<usize, Vec<String>>,
    brute_force_result_distances: HashMap<usize, Vec<f32>>,
    brute_force_result_embeddings: HashMap<usize, Vec<Vec<f32>>>,
    // Task id to query_vectors index
    hnsw_task_id_to_query_index: HashMap<Uuid, usize>,
    brute_force_task_id_to_query_index: HashMap<Uuid, usize>,
    merge_task_id_to_query_index: HashMap<Uuid, usize>,
    // Result state
    results: Option<Vec<Vec<VectorQueryResult>>>,
    // State machine management
    merge_dependency_count: u32,
    finish_dependency_count: u32,
    // Services
    log: Box<Log>,
    sysdb: Box<SysDb>,
    dispatcher: ComponentHandle<Dispatcher>,
    hnsw_index_provider: HnswIndexProvider,
    blockfile_provider: BlockfileProvider,
    // Result channel
    result_channel: Option<
        tokio::sync::oneshot::Sender<Result<Vec<Vec<VectorQueryResult>>, Box<dyn ChromaError>>>,
    >,
}

impl HnswQueryOrchestrator {
    pub(crate) fn new(
        system: System,
        query_vectors: Vec<Vec<f32>>,
        k: i32,
        allowed_ids: Vec<String>,
        include_embeddings: bool,
        segment_id: Uuid,
        log: Box<Log>,
        sysdb: Box<SysDb>,
        hnsw_index_provider: HnswIndexProvider,
        blockfile_provider: BlockfileProvider,
        dispatcher: ComponentHandle<Dispatcher>,
    ) -> Self {
        // Set the merge dependency count to the number of query vectors * 2
        // N for the HNSW query and N for the Brute force query
        let merge_dependency_count = (query_vectors.len() * 2) as u32;
        // Set the finish dependency count to the number of query vectors
        // since each query vector will have a merge task
        let finish_dependency_count = query_vectors.len() as u32;
        // pre-allocate the result vectors
        let results = Some(Vec::with_capacity(query_vectors.len()));
        tracing::info!(
            "Performing KNN for k = {}, allowed_ids = {:?}, num query vectors = {:?}",
            k,
            allowed_ids,
            query_vectors.len()
        );

        HnswQueryOrchestrator {
            state: ExecutionState::Pending,
            system,
            merge_dependency_count,
            finish_dependency_count,
            query_vectors,
            k,
            allowed_ids: allowed_ids.into(),
            include_embeddings,
            hnsw_segment_id: segment_id,
            hnsw_segment: None,
            record_segment: None,
            collection: None,
            index_config: None,
            hnsw_result_offset_ids: HashMap::new(),
            hnsw_result_distances: HashMap::new(),
            brute_force_result_user_ids: HashMap::new(),
            brute_force_result_distances: HashMap::new(),
            brute_force_result_embeddings: HashMap::new(),
            hnsw_task_id_to_query_index: HashMap::new(),
            brute_force_task_id_to_query_index: HashMap::new(),
            merge_task_id_to_query_index: HashMap::new(),
            results,
            log,
            sysdb,
            dispatcher,
            hnsw_index_provider,
            blockfile_provider,
            result_channel: None,
        }
    }

    async fn pull_logs(
        &mut self,
        self_address: Box<dyn ReceiverForMessage<TaskResult<PullLogsOutput, PullLogsError>>>,
    ) {
        self.state = ExecutionState::PullLogs;
        let operator = PullLogsOperator::new(self.log.clone());
        let end_timestamp = SystemTime::now().duration_since(UNIX_EPOCH);
        let end_timestamp = match end_timestamp {
            // TODO: change protobuf definition to use u64 instead of i64
            Ok(end_timestamp) => end_timestamp.as_nanos() as i64,
            Err(e) => {
                // Log an error and reply + return
                return;
            }
        };

        let collection = self
            .collection
            .as_ref()
            .expect("State machine invariant violation. The collection is not set when pulling logs. This should never happen.");

        let input = PullLogsInput::new(
            collection.id,
            // The collection log position is inclusive, and we want to start from the next log
            collection.log_position + 1,
            100,
            None,
            Some(end_timestamp),
        );
        let task = wrap(operator, input, self_address);
        // Wrap the task with current span as the parent. The worker then executes it
        // inside a child span with this parent.
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                // TODO: log an error and reply to caller
            }
        }
    }

    async fn brute_force_query(
        &mut self,
        logs: Chunk<LogRecord>,
        self_address: Box<
            dyn ReceiverForMessage<
                TaskResult<BruteForceKnnOperatorOutput, BruteForceKnnOperatorError>,
            >,
        >,
    ) {
        self.state = ExecutionState::QueryKnn;
        let distance_function = &self
            .index_config
            .as_ref()
            .expect("Invariant violation. Index config is not set")
            .distance_function;

        // TODO: We shouldn't have to clone query vectors here. We should be able to pass a Arc<[f32]>-like to the input
        for (i, query_vector) in self.query_vectors.iter().enumerate() {
            let bf_input = BruteForceKnnOperatorInput {
                log: logs.clone(),
                query: query_vector.clone(),
                k: self.k as usize,
                distance_metric: distance_function.clone(),
                allowed_ids: self.allowed_ids.clone(),
                record_segment_definition: self
                    .record_segment
                    .as_ref()
                    .expect("Invariant violation. Record segment is not set")
                    .clone(),
                blockfile_provider: self.blockfile_provider.clone(),
            };
            let operator = Box::new(BruteForceKnnOperator {});
            let task = wrap(operator, bf_input, self_address.clone());
            self.brute_force_task_id_to_query_index.insert(task.id(), i);
            match self.dispatcher.send(task, Some(Span::current())).await {
                Ok(_) => (),
                Err(e) => {
                    // Log an error
                    println!("Error sending Brute Force KNN task: {:?}", e);
                }
            }
        }
    }

    async fn hnsw_segment_query(&mut self, logs: Chunk<LogRecord>, ctx: &ComponentContext<Self>) {
        self.state = ExecutionState::QueryKnn;

        let hnsw_segment = self
            .hnsw_segment
            .as_ref()
            .expect("Invariant violation. HNSW Segment is not set");
        let dimensionality = self
            .collection
            .as_ref()
            .expect("Invariant violation. Collection is not set")
            .dimension
            .expect("Invariant violation. Collection dimension is not set");

        // Fetch the data needed for the duration of the query - The HNSW Segment, The record Segment and the Collection
        let hnsw_segment_reader = match DistributedHNSWSegmentReader::from_segment(
            // These unwraps are safe because we have already checked that the segments are set in the orchestrator on_start
            hnsw_segment,
            dimensionality as usize,
            self.hnsw_index_provider.clone(),
        )
        .await
        {
            Ok(reader) => reader,
            Err(e) => {
                match *e {
                    DistributedHNSWSegmentFromSegmentError::Uninitialized => {
                        tracing::info!("[HnswQueryOperation]: Uninitialied reader {:?}", *e);
                        // no task, decrement the merge dependency count and return
                        // with an empty result
                        for (i, _) in self.query_vectors.iter().enumerate() {
                            self.merge_dependency_count -= 1;
                            self.hnsw_result_distances.insert(i, Vec::new());
                            self.hnsw_result_offset_ids.insert(i, Vec::new());
                        }
                        return;
                    }
                    _ => {
                        tracing::error!("[HnswQueryOperation]: Error creating distributed hnsw segment reader {:?}", *e);
                        terminate_with_error(self.result_channel.take(), e, ctx);
                        return;
                    }
                }
            }
        };
        println!("Created HNSW Segment Reader: {:?}", hnsw_segment_reader);

        let record_segment = self
            .record_segment
            .as_ref()
            .expect("Invariant violation. Record Segment is not set");

        // Dispatch a query task per query vector
        for (i, query_vector) in self.query_vectors.iter().enumerate() {
            let operator = Box::new(HnswKnnOperator {});
            let input = HnswKnnOperatorInput {
                segment: hnsw_segment_reader.clone(),
                query: query_vector.clone(),
                k: self.k as usize,
                record_segment: record_segment.clone(),
                blockfile_provider: self.blockfile_provider.clone(),
                allowed_ids: self.allowed_ids.clone(),
                logs: logs.clone(),
            };
            let task = wrap(operator, input, ctx.receiver());
            self.hnsw_task_id_to_query_index.insert(task.id(), i);
            match self.dispatcher.send(task, Some(Span::current())).await {
                Ok(_) => (),
                Err(e) => {
                    // Log an error
                    println!("Error sending HNSW KNN task: {:?}", e);
                }
            }
        }
    }

    async fn merge_results(&mut self, ctx: &ComponentContext<Self>) {
        self.state = ExecutionState::MergeResults;
        for i in 0..self.query_vectors.len() {
            self.merge_results_for_index(ctx, i).await;
        }
    }

    async fn prefetch_record_data(&mut self, ctx: &ComponentContext<Self>, offset_ids: Vec<u32>) {
        let record_segment = self
            .record_segment
            .as_ref()
            .expect("Invariant violation. Record Segment is not set");
        // TODO: Divide this into multiple tasks based on some criteria.
        let offsetid_to_data_keys =
            Keys::OffsetIdToDataKeys(OffsetIdToDataKeys { keys: offset_ids });
        let prefetch_input = RecordSegmentPrefetchIoInput {
            keys: offsetid_to_data_keys,
            segment: record_segment.clone(),
            provider: self.blockfile_provider.clone(),
        };
        let operator = RecordSegmentPrefetchIoOperator::new();
        let prefetch_task = wrap(operator, prefetch_input, ctx.receiver());
        match self
            .dispatcher
            .send(prefetch_task, Some(Span::current()))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                // Log an error
                tracing::error!("Error sending record segment Prefetch data task: {:?}", e);
            }
        }
    }

    async fn prefetch_user_ids(&mut self, ctx: &ComponentContext<Self>, offset_ids: Vec<u32>) {
        let record_segment = self
            .record_segment
            .as_ref()
            .expect("Invariant violation. Record Segment is not set");
        // TODO: Divide this into multiple tasks based on some criteria.
        let offsetid_to_userid_keys =
            Keys::OffsetIdToUserIdKeys(OffsetIdToUserIdKeys { keys: offset_ids });
        let prefetch_input = RecordSegmentPrefetchIoInput {
            keys: offsetid_to_userid_keys,
            segment: record_segment.clone(),
            provider: self.blockfile_provider.clone(),
        };
        let operator = RecordSegmentPrefetchIoOperator::new();
        let prefetch_task = wrap(operator, prefetch_input, ctx.receiver());
        match self
            .dispatcher
            .send(prefetch_task, Some(Span::current()))
            .await
        {
            Ok(_) => (),
            Err(e) => {
                // Log an error
                tracing::error!("Error sending Prefetch data task: {:?}", e);
            }
        }
    }

    async fn merge_results_for_index(
        &mut self,
        ctx: &ComponentContext<Self>,
        query_vector_index: usize,
    ) {
        let hnsw_result_offset_ids = self
            .hnsw_result_offset_ids
            .remove(&query_vector_index)
            .expect(
                "Invariant violation. HNSW result offset ids are not set for query vector index",
            );

        // Eagerly dispatch prefetch tasks.
        let offset_ids_to_prefetch: Vec<u32> =
            hnsw_result_offset_ids.iter().map(|x| *x as u32).collect();
        self.prefetch_record_data(ctx, offset_ids_to_prefetch.clone())
            .await;
        self.prefetch_user_ids(ctx, offset_ids_to_prefetch).await;

        let record_segment = self
            .record_segment
            .as_ref()
            .expect("Invariant violation. Record Segment is not set");

        let hnsw_result_distances = self
            .hnsw_result_distances
            .remove(&query_vector_index)
            .expect(
                "Invariant violation. HNSW result distances are not set for query vector index",
            );

        let brute_force_result_user_ids = self.brute_force_result_user_ids.remove(&query_vector_index).expect("Invariant violation. Brute force result user ids are not set for query vector index");
        let brute_force_result_distances = self.brute_force_result_distances.remove(&query_vector_index).expect("Invariant violation. Brute force result distances are not set for query vector index");
        let brute_force_result_embeddings = self
            .brute_force_result_embeddings
            .remove(&query_vector_index);

        tracing::info!(
            "[HnswQueryOperation]: Brute force {} user ids, hnsw {} offset ids",
            brute_force_result_user_ids.len(),
            hnsw_result_offset_ids.len()
        );

        let operator = Box::new(MergeKnnResultsOperator {});
        let input = MergeKnnResultsOperatorInput::new(
            hnsw_result_offset_ids,
            hnsw_result_distances,
            brute_force_result_user_ids,
            brute_force_result_distances,
            brute_force_result_embeddings,
            self.include_embeddings,
            self.k as usize,
            record_segment.clone(),
            self.blockfile_provider.clone(),
        );

        let task = wrap(operator, input, ctx.receiver());
        self.merge_task_id_to_query_index
            .insert(task.id(), query_vector_index);
        match self.dispatcher.send(task, Some(Span::current())).await {
            Ok(_) => (),
            Err(e) => {
                // Log an error
                println!("Error sending Merge KNN task: {:?}", e);
            }
        }
    }

    fn terminate_with_empty_response(&mut self, ctx: &ComponentContext<Self>) {
        let result_channel = self
            .result_channel
            .take()
            .expect("Invariant violation. Result channel is not set.");
        let mut empty_resp = vec![];
        for _ in 0..self.query_vectors.len() {
            empty_resp.push(vec![]);
        }
        match result_channel.send(Ok(empty_resp)) {
            Ok(_) => (),
            Err(e) => {
                // Log an error - this implied the listener was dropped
                tracing::error!(
                    "[HnswQueryOrchestrator] Result channel dropped before sending empty response"
                );
            }
        }
        // Cancel the orchestrator so it stops processing
        ctx.cancellation_token.cancel();
    }

    ///  Run the orchestrator and return the result.
    ///  # Note
    ///  Use this over spawning the component directly. This method will start the component and
    ///  wait for it to finish before returning the result.
    pub(crate) async fn run(mut self) -> Result<Vec<Vec<VectorQueryResult>>, Box<dyn ChromaError>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.result_channel = Some(tx);
        let mut handle = self.system.clone().start_component(self);
        let result = rx.await;
        handle.stop();
        result.unwrap()
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Component for HnswQueryOrchestrator {
    fn get_name() -> &'static str {
        "HNSW Query orchestrator"
    }

    fn queue_size(&self) -> usize {
        1000 // TODO: make configurable
    }

    async fn on_start(&mut self, ctx: &crate::system::ComponentContext<Self>) -> () {
        // Populate the orchestrator with the initial state - The HNSW Segment, The Record Segment and the Collection
        let hnsw_segment =
            match get_hnsw_segment_by_id(self.sysdb.clone(), &self.hnsw_segment_id).await {
                Ok(segment) => segment,
                Err(e) => {
                    terminate_with_error(self.result_channel.take(), e, ctx);
                    return;
                }
            };

        let collection_id = match &hnsw_segment.collection {
            Some(collection_id) => collection_id,
            None => {
                terminate_with_error(
                    self.result_channel.take(),
                    Box::new(HnswSegmentQueryError::HnswSegmentHasNoCollection),
                    ctx,
                );
                return;
            }
        };

        let collection = match get_collection_by_id(self.sysdb.clone(), collection_id).await {
            Ok(collection) => collection,
            Err(e) => {
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        };

        // If segment is uninitialized and dimension is not set then we assume
        // that this is a query before any add so return empty response.
        if hnsw_segment.file_path.len() <= 0 && collection.dimension.is_none() {
            self.terminate_with_empty_response(ctx);
            return;
        }

        // Validate that the collection has a dimension set. Downstream steps will rely on this
        // so that they can unwrap the dimension without checking for None
        if collection.dimension.is_none() {
            terminate_with_error(
                self.result_channel.take(),
                Box::new(HnswSegmentQueryError::CollectionHasNoDimension),
                ctx,
            );
            return;
        };

        let record_segment =
            match get_record_segment_by_collection_id(self.sysdb.clone(), collection_id).await {
                Ok(segment) => segment,
                Err(e) => {
                    terminate_with_error(self.result_channel.take(), e, ctx);
                    return;
                }
            };

        match IndexConfig::from_segment(&hnsw_segment, collection.dimension.unwrap()) {
            Ok(index_config) => {
                self.index_config = Some(index_config);

                // Normalize the query vectors if we are using the cosine similarity
                if self.index_config.as_ref().unwrap().distance_function == DistanceFunction::Cosine
                {
                    for query_vector in self.query_vectors.iter_mut() {
                        *query_vector = normalize(query_vector);
                    }
                }
            }
            Err(e) => {
                terminate_with_error(self.result_channel.take(), e, ctx);
                return;
            }
        }

        self.record_segment = Some(record_segment);
        self.hnsw_segment = Some(hnsw_segment);
        self.collection = Some(collection);

        self.pull_logs(ctx.receiver()).await;
    }
}

// ============== Handlers ==============

#[async_trait]
impl Handler<TaskResult<PullLogsOutput, PullLogsError>> for HnswQueryOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<PullLogsOutput, PullLogsError>,
        ctx: &crate::system::ComponentContext<HnswQueryOrchestrator>,
    ) {
        let message = message.into_inner();
        self.state = ExecutionState::Partition;

        match message {
            Ok(pull_logs_output) => {
                let logs = pull_logs_output.logs();
                self.brute_force_query(logs.clone(), ctx.receiver()).await;
                self.hnsw_segment_query(logs, ctx).await;
            }
            Err(e) => {
                terminate_with_error(self.result_channel.take(), Box::new(e), ctx);
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<BruteForceKnnOperatorOutput, BruteForceKnnOperatorError>>
    for HnswQueryOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<BruteForceKnnOperatorOutput, BruteForceKnnOperatorError>,
        ctx: &crate::system::ComponentContext<HnswQueryOrchestrator>,
    ) {
        let task_id = message.id();
        let message = message.into_inner();
        let query_index = self
            .brute_force_task_id_to_query_index
            .remove(&task_id)
            .expect("Invariant violation. Brute force task id is not set for query vector index");

        match message {
            Ok(output) => {
                let mut user_ids = output.user_ids;
                let mut embeddings = None;
                if self.include_embeddings {
                    embeddings = Some(output.embeddings);
                }
                self.brute_force_result_user_ids
                    .insert(query_index, user_ids);
                self.brute_force_result_distances
                    .insert(query_index, output.distances);
                if let Some(embeddings) = embeddings {
                    self.brute_force_result_embeddings
                        .insert(query_index, embeddings);
                }
            }
            Err(e) => {
                // TODO: handle this error, technically never happens
            }
        }

        self.merge_dependency_count -= 1;

        if self.merge_dependency_count == 0 {
            self.merge_results(ctx).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<HnswKnnOperatorOutput, Box<dyn ChromaError>>> for HnswQueryOrchestrator {
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<HnswKnnOperatorOutput, Box<dyn ChromaError>>,
        ctx: &ComponentContext<Self>,
    ) {
        let task_id = message.id();
        let message = message.into_inner();
        let query_index = self
            .hnsw_task_id_to_query_index
            .remove(&task_id)
            .expect("Invariant violation. HNSW task id is not set for query vector index");
        match message {
            Ok(output) => {
                self.hnsw_result_offset_ids
                    .insert(query_index, output.offset_ids);
                self.hnsw_result_distances
                    .insert(query_index, output.distances);
            }
            Err(e) => {
                terminate_with_error(self.result_channel.take(), e.boxed(), ctx);
            }
        }

        self.merge_dependency_count -= 1;

        if self.merge_dependency_count == 0 {
            self.merge_results(ctx).await;
        }
    }
}

#[async_trait]
impl Handler<TaskResult<MergeKnnResultsOperatorOutput, Box<dyn ChromaError>>>
    for HnswQueryOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        message: TaskResult<MergeKnnResultsOperatorOutput, Box<dyn ChromaError>>,
        ctx: &crate::system::ComponentContext<HnswQueryOrchestrator>,
    ) {
        let task_id = message.id();
        let message = message.into_inner();
        let query_index = self
            .merge_task_id_to_query_index
            .remove(&task_id)
            .expect("Invariant violation. Merge task id is not set for query vector index");

        self.state = ExecutionState::Finished;

        let (mut output_ids, mut output_distances, output_vectors) = match message {
            Ok(output) => (output.user_ids, output.distances, output.vectors),
            Err(e) => {
                terminate_with_error(self.result_channel.take(), e.boxed(), ctx);
                return;
            }
        };

        let mut query_results = Vec::new();
        if self.include_embeddings {
            for ((index, distance), vector) in
                output_ids.drain(..).zip(output_distances.drain(..)).zip(
                    output_vectors
                        .expect("Embeddings are expected if include_embeddings is set")
                        .drain(..),
                )
            {
                let query_result = VectorQueryResult {
                    id: index,
                    distance: distance,
                    vector: Some(vector),
                };
                query_results.push(query_result);
            }
        } else {
            for (index, distance) in output_ids.drain(..).zip(output_distances.drain(..)) {
                let query_result = VectorQueryResult {
                    id: index,
                    distance: distance,
                    vector: None,
                };
                query_results.push(query_result);
            }
        }
        trace!("Merged results: {:?}", query_results);

        let results_slice = self
            .results
            .as_mut()
            .expect("Invariant violation. Results are not set")
            .spare_capacity_mut();
        results_slice[query_index].write(query_results);
        self.finish_dependency_count -= 1;

        if self.finish_dependency_count == 0 {
            let result_channel = match self.result_channel.take() {
                Some(tx) => tx,
                None => {
                    // Log an error - this is an invariant violation, the result channel should always be set
                    return;
                }
            };

            unsafe {
                // Safety: We have ensured that the results are set and the length is equal to the number of query vectors
                // https://doc.rust-lang.org/stable/std/mem/union.MaybeUninit.html#out-pointers
                self.results
                    .as_mut()
                    .expect("Invariant violation. Results are not set")
                    .set_len(self.query_vectors.len());
            }

            match result_channel.send(Ok(self
                .results
                .take()
                .expect("Invariant violation. Results are not set")))
            {
                Ok(_) => (),
                Err(e) => {
                    // Log an error
                }
            }
        }
    }
}

#[async_trait]
impl Handler<TaskResult<RecordSegmentPrefetchIoOutput, RecordSegmentPrefetchIoOperatorError>>
    for HnswQueryOrchestrator
{
    type Result = ();

    async fn handle(
        &mut self,
        _message: TaskResult<RecordSegmentPrefetchIoOutput, RecordSegmentPrefetchIoOperatorError>,
        _ctx: &ComponentContext<Self>,
    ) {
        // Nothing to do.
    }
}
