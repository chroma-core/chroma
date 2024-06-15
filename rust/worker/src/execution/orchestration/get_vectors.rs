#[derive(Debug)]
enum ExecutionState {
    Pending,
    PullLogs,
    // IMPL NOTE: read vectors should filter out the vectors that are not present in the index.
    ReadVectors,
    MergeResults,
}

#[derive(Debug)]
pub struct GetVectorsOrchestrator {
    state: ExecutionState,
    // Component Execution
    system: System,
    // Query state
    get_ids: Vec<String>,
    hnsw_segment_id: Uuid,
    // State fetched or created for query execution
    record_segment: Option<Segment>,
    // // query_vectors index to the result
    // hnsw_result_offset_ids: HashMap<usize, Vec<usize>>,
    // hnsw_result_distances: HashMap<usize, Vec<f32>>,
    // brute_force_result_user_ids: HashMap<usize, Vec<String>>,
    // brute_force_result_distances: HashMap<usize, Vec<f32>>,
    // brute_force_result_embeddings: HashMap<usize, Vec<Vec<f32>>>,
    // // Task id to query_vectors index
    // hnsw_task_id_to_query_index: HashMap<Uuid, usize>,
    // brute_force_task_id_to_query_index: HashMap<Uuid, usize>,
    // merge_task_id_to_query_index: HashMap<Uuid, usize>,
    // // Result state
    // results: Option<Vec<Vec<VectorQueryResult>>>,
    // // State machine management
    // merge_dependency_count: u32,
    // finish_dependency_count: u32,
    // // Services
    // log: Box<dyn Log>,
    // sysdb: Box<dyn SysDb>,
    // dispatcher: Box<dyn Receiver<TaskMessage>>,
    // hnsw_index_provider: HnswIndexProvider,
    // blockfile_provider: BlockfileProvider,
    // Result channel
    // result_channel: Option<
    //     tokio::sync::oneshot::Sender<Result<Vec<Vec<VectorQueryResult>>, Box<dyn ChromaError>>>,
    // >,
}

impl GetVectorsOrchestrator {
    pub fn new(system: System, get_ids: Vec<String>, hnsw_segment_id: Uuid) -> Self {
        Self {
            state: ExecutionState::Pending,
            system,
            get_ids,
            hnsw_segment_id,
            record_segment: None,
        }
    }
}

// ============== Component Implementation ==============

#[async_trait]
impl Component for GetVectorOrchestrator {
    fn get_name(&self) -> &'static str {
        "GetVectorOrchestrator"
    }

    fn queue_size(&self) -> usize {
        1000
    }
}

// ============== Handlers ==============
