use chroma_error::ChromaError;
use chroma_types::{
    chroma_proto::{scrub_log_request::LogToScrub, ScrubLogRequest, ScrubLogResponse},
    CollectionUuid, DatabaseName, TopologyName,
};
use tonic::{Request, Response, Status};
use uuid::Uuid;
use wal3::{Limits, LogReaderOptions};

use crate::LogServer;

impl LogServer {
    pub async fn scrub_log(
        &self,
        request: Request<ScrubLogRequest>,
    ) -> Result<Response<ScrubLogResponse>, Status> {
        let scrub_log = request.into_inner();

        let reader = match scrub_log.log_to_scrub {
            Some(LogToScrub::CollectionId(x)) => {
                let database_name = DatabaseName::new(&scrub_log.database_name)
                    .ok_or_else(|| Status::invalid_argument("Database name invalid"))?;
                let topology_name = database_name
                    .topology()
                    .map(TopologyName::new)
                    .transpose()
                    .map_err(|err| Status::invalid_argument(err.to_string()))?;

                let collection_id = Uuid::parse_str(&x)
                    .map(CollectionUuid)
                    .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
                self.make_log_reader(topology_name.as_ref(), collection_id)
                    .await
                    .map_err(|err| Status::new(err.code().into(), err.to_string()))?
            }
            Some(LogToScrub::DirtyLog(host)) => {
                if host != self.config.my_member_id {
                    return Err(Status::failed_precondition(format!(
                        "can only scrub our own dirty log: I am {}, but was asked for {}",
                        self.config.my_member_id, host
                    )));
                }
                let dirty_log = self
                    .dirty_log
                    .as_ref()
                    .ok_or_else(|| Status::failed_precondition("dirty log not configured"))?;
                dirty_log
                    .reader(LogReaderOptions::default())
                    .await
                    .ok_or_else(|| Status::unavailable("Failed to get dirty log reader"))?
            }
            None => {
                return Err(Status::not_found("log not found because it's null"));
            }
        };

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
