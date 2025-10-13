use std::sync::Arc;

use chroma_error::ChromaError;
use chroma_types::{
    chroma_proto::{scrub_log_request::LogToScrub, ScrubLogRequest, ScrubLogResponse},
    CollectionUuid,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;
use wal3::{Limits, LogReader, LogReaderOptions};

use crate::{LogServer, MarkDirty};

impl LogServer {
    pub async fn scrub_log(
        &self,
        request: Request<ScrubLogRequest>,
    ) -> Result<Response<ScrubLogResponse>, Status> {
        let scrub_log = request.into_inner();

        let path = match scrub_log.log_to_scrub {
            Some(LogToScrub::CollectionId(x)) => {
                let collection_id = Uuid::parse_str(&x)
                    .map(CollectionUuid)
                    .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
                collection_id.storage_prefix_for_log()
            }
            Some(LogToScrub::DirtyLog(host)) => MarkDirty::path_for_hostname(&host),
            None => {
                return Err(Status::not_found("log not found because it's null"));
            }
        };

        let reader = LogReader::open(LogReaderOptions::default(), Arc::clone(&self.storage), path)
            .await
            .map_err(|err| Status::new(err.code().into(), err.to_string()))?;

        let limits = Limits {
            max_files: Some(scrub_log.max_files_to_read.into()),
            max_bytes: Some(scrub_log.max_bytes_to_read),
            max_records: None,
        };

        let result = reader.scrub(limits).await;

        match result {
            Ok(success) => Ok(Response::new(ScrubLogResponse {
                calculated_setsum: success.calculated_setsum.hexdigest(),
                bytes_read: success.bytes_read,
                errors: vec![],
                short_read: success.short_read,
            })),
            Err(errors) => {
                let errors = errors
                    .into_iter()
                    .map(|err| err.to_string())
                    .collect::<Vec<_>>();
                Ok(Response::new(ScrubLogResponse {
                    calculated_setsum: "<not calculated; bytes_read will be off>".to_string(),
                    bytes_read: 0,
                    errors,
                    short_read: false,
                }))
            }
        }
    }
}
