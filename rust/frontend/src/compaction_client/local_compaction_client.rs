use chroma_config::{registry::Registry, Configurable};
use chroma_error::ChromaError;
use chroma_log::LocalCompactionManager;
use chroma_system::{ComponentHandle, System};
use chroma_types::{ManualCompactionError, ManualCompactionRequest, ManualCompactionResponse};

#[derive(Debug, Clone)]
pub(super) struct LocalCompactionClient {
    handle: ComponentHandle<LocalCompactionManager>,
}

impl LocalCompactionClient {
    pub async fn manually_compact(
        &mut self,
        request: ManualCompactionRequest,
    ) -> Result<ManualCompactionResponse, ManualCompactionError> {
        // let compact_message = CompactionMessage {
        //     collection_id: request.collection_id,
        //     start_offset: start_log_offset,
        //     total_records,
        // };
        // self.handle
        //     .request(compact_message, None)
        //     .await
        //     .unwrap()
        //     .unwrap();
        // todo

        Ok(ManualCompactionResponse {})
    }
}

#[async_trait::async_trait]
impl Configurable<((), System)> for LocalCompactionClient {
    async fn try_from_config(
        (_config, _system): &((), System),
        registry: &Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        // Assume the registry has a compaction manager handle
        let handle = registry
            .get::<ComponentHandle<LocalCompactionManager>>()
            .map_err(|err| err.boxed())?;
        Ok(Self { handle })
    }
}
