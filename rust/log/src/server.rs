use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_storage::config::StorageConfig;
use chroma_storage::Storage;
use chroma_types::chroma_proto::{
    log_service_server::LogService, GetAllCollectionInfoToCompactRequest,
    GetAllCollectionInfoToCompactResponse, PullLogsRequest, PullLogsResponse, PushLogsRequest,
    PushLogsResponse, UpdateCollectionLogOffsetRequest, UpdateCollectionLogOffsetResponse,
};
use chroma_types::CollectionUuid;
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;
use wal3::{LogWriter, LogWriterOptions};

use crate::state_hash_table::StateHashTable;

///////////////////////////////////////// state maintenance ////////////////////////////////////////

// NOTE(rescrv):  This code dynamically opens and closes logs.  An opened log will stay open until
// no one is writing to it.  It will then stay open for an additional, configurable time period.
// The mechanism that powers this is a state hash table whose reference is retained by a
// background future per log.  The future holds a reference for X seconds and then does a dance to
// drop it in a way that ensures another future will always be spawned if things race.
//
// The key to this is the active log struct.  State hash table gives us the ability to treat it as
// a something of a dynamic singleton.  We just need to make sure that if we initialize the log, we
// clean it up eventually.  Assuming no bugs, there are three outcomes:
// - We don't collect logs so space grows unbounded.
// - We collect logs too aggressively, so logs must be reopened (a get from S3).
// - We collect logs too which resembles the first case.
//
// Bugs likely to be encountered include (and reason for not worrying about it):
// - Durability bugs.  The log protocol is responsible for ensuring they do not happen.
// - De-sync between the task pinning the log in memory and the log itself.  The state hash table
//   will guarantee collection once all references are dropped.
// - Dropping the log before timeout.  The log will have to be reopened to write to it at the cost
//   of getting the manifest once.
// - Failing to drop the log when it can be dropped.  We'll use more memory than necessary.
//
// The logic here satisfies the observations of this note.  It follows these rules:
// - Always acquire a handle.  Trust the state hash table to do the right thing.
// - Given a handle it will either have a Some(log) or None.  If it has a log, it will be

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct LogKey {
    collection_id: CollectionUuid,
}

impl crate::state_hash_table::Key for LogKey {}

#[derive(Debug)]
struct ActiveLog {
    /// A tokio mutex so that it may be held across open calls to the log writer.  To keep the log
    /// writer in sync, every time a writer is created here, a background task that watches
    /// collect_after will set this to None and exit itself.  Thus, we should spawn one background
    /// task for each None->Some transition on this field.
    log: Option<Arc<LogWriter>>,
    /// An instant in time after which the background task will set the log to None and exit.
    /// Writers to the log should bump this to be into the future to "heartbeat" the log.  The
    /// method for this is called `keep_alive`.
    collect_after: Instant,
    /// The number of times this log has been recycled (i.e., the log gets set to none, but a
    /// handle isn't dropped).
    epoch: u64,
}

impl ActiveLog {
    pub fn keep_alive(&mut self, keep_alive: Duration) {
        let now = Instant::now();
        let when = if keep_alive > Duration::ZERO {
            now.checked_add(keep_alive).unwrap_or(now)
        } else {
            now
        };
        if self.collect_after < when {
            self.collect_after = when;
        }
    }
}

impl Default for ActiveLog {
    fn default() -> Self {
        Self {
            log: None,
            collect_after: Instant::now(),
            epoch: 0,
        }
    }
}

/// An in-memory stub for the log writer.
#[derive(Debug)]
struct LogStub {
    active: tokio::sync::Mutex<ActiveLog>,
}

impl Default for LogStub {
    fn default() -> Self {
        Self {
            active: tokio::sync::Mutex::new(ActiveLog::default()),
        }
    }
}

impl crate::state_hash_table::Value for LogStub {
    fn finished(&self) -> bool {
        // NOTE(rescrv):  I'm doing something funky here w.r.t. state hash table.  I'm always
        // returning true, and relying upon its handle tracking to only drop the value when it is
        // no longer referenced.  Simpler than the alternative of trying to decide when to drop and
        // getting it wrong.
        true
    }
}

impl From<LogKey> for LogStub {
    fn from(_: LogKey) -> LogStub {
        LogStub::default()
    }
}

/// Force a lifetime to outlive the Arc'd log writer.
struct LogRef<'a> {
    log: Arc<LogWriter>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

impl std::ops::Deref for LogRef<'_> {
    type Target = LogWriter;

    fn deref(&self) -> &Self::Target {
        &self.log
    }
}

async fn get_log_from_handle<'a>(
    handle: &'a crate::state_hash_table::Handle<LogKey, LogStub>,
    options: &LogWriterOptions,
    storage: &Arc<Storage>,
    prefix: &str,
) -> Result<LogRef<'a>, wal3::Error> {
    let mut active = handle.active.lock().await;
    active.keep_alive(Duration::from_secs(60));
    if let Some(log) = active.log.as_ref() {
        return Ok(LogRef {
            log: Arc::clone(log),
            _phantom: std::marker::PhantomData,
        });
    }
    let opened = LogWriter::open(
        options.clone(),
        Arc::clone(storage),
        prefix,
        // TODO(rescrv):  Configurable params.
        "log writer",
    )
    .await?;
    let opened = Arc::new(opened);
    active.log = Some(Arc::clone(&opened));
    let handle_clone = handle.clone();
    let epoch = active.epoch;
    // NOTE(rescrv):  This task will exit only after the log's keep alive is in the past.  If
    // everyone who calls get_log keeps it alive (top of this call), then this task will stay alive
    // forever.
    tokio::task::spawn(async move {
        loop {
            let sleep = {
                let mut active = handle_clone.active.lock().await;
                let now = Instant::now();
                if now >= active.collect_after {
                    active.log = None;
                    active.epoch += 1;
                    return;
                } else if active.epoch != epoch {
                    return;
                }
                active.collect_after - now
            };
            tokio::time::sleep(sleep).await;
        }
    });
    Ok(LogRef {
        log: opened,
        _phantom: std::marker::PhantomData,
    })
}

////////////////////////////////////// storage_prefix_for_log //////////////////////////////////////

pub fn storage_prefix_for_log(collection: CollectionUuid) -> String {
    format!("logs/{}", collection)
}

///////////////////////////////////////////// LogServer ////////////////////////////////////////////

pub struct LogServer {
    config: LogServerConfig,
    storage: Arc<Storage>,
    open_logs: Arc<StateHashTable<LogKey, LogStub>>,
}

#[async_trait]
impl LogService for LogServer {
    async fn push_logs(
        &self,
        request: Request<PushLogsRequest>,
    ) -> Result<Response<PushLogsResponse>, Status> {
        let push_logs = request.into_inner();
        let collection_id = Uuid::parse_str(&push_logs.collection_id)
            .map(CollectionUuid)
            .map_err(|_| Status::invalid_argument("Failed to parse collection id"))?;
        let prefix = storage_prefix_for_log(collection_id);
        let key = LogKey { collection_id };
        let handle = self.open_logs.get_or_create_state(key);
        let log = get_log_from_handle(&handle, &self.config.log, &self.storage, &prefix)
            .await
            // TODO(rescrv): better error handling.
            .map_err(|err| Status::unknown(err.to_string()))?;
        let mut messages = Vec::with_capacity(push_logs.records.len());
        for record in push_logs.records {
            let mut buf = vec![];
            record
                .encode(&mut buf)
                .map_err(|err| Status::unknown(err.to_string()))?;
            messages.push(buf);
        }
        if messages.len() > i32::MAX as usize {
            return Err(Status::invalid_argument("Too many records"));
        }
        let record_count = messages.len() as i32;
        log.append_many(messages)
            .await
            .map_err(|err| Status::unknown(err.to_string()))?;
        Ok(Response::new(PushLogsResponse { record_count }))
    }

    async fn pull_logs(
        &self,
        _request: Request<PullLogsRequest>,
    ) -> Result<Response<PullLogsResponse>, Status> {
        todo!("Implement wal3 backed pull_logs here")
    }

    async fn get_all_collection_info_to_compact(
        &self,
        _request: Request<GetAllCollectionInfoToCompactRequest>,
    ) -> Result<Response<GetAllCollectionInfoToCompactResponse>, Status> {
        todo!("Implement wal3 backed get_all_collection_info_to_compact here")
    }

    async fn update_collection_log_offset(
        &self,
        _request: Request<UpdateCollectionLogOffsetRequest>,
    ) -> Result<Response<UpdateCollectionLogOffsetResponse>, Status> {
        todo!("Implement wal3 backed update_collection_log_offset here")
    }
}

impl LogServer {
    pub(crate) async fn run(log_server: LogServer) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", log_server.config.port).parse().unwrap();
        println!("Log listening on {}", addr);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<chroma_types::chroma_proto::log_service_server::LogServiceServer<Self>>()
            .await;

        let server = Server::builder().add_service(health_service).add_service(
            chroma_types::chroma_proto::log_service_server::LogServiceServer::new(
                log_server.clone(),
            ),
        );

        let server = server.serve_with_shutdown(addr, async {
            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(sigterm) => sigterm,
                Err(e) => {
                    tracing::error!("Failed to create signal handler: {:?}", e);
                    return;
                }
            };
            sigterm.recv().await;
            tracing::info!("Received SIGTERM, shutting down");
        });

        server.await?;

        Ok(())
    }
}

/////////////////////////// Config ///////////////////////////

fn default_otel_service_name() -> String {
    "rust-log-service".to_string()
}

fn default_port() -> u16 {
    50051
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct OpenTelemetryConfig {
    pub endpoint: String,
    #[serde(default = "default_otel_service_name")]
    pub service_name: String,
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct LogServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    pub opentelemetry: Option<OpenTelemetryConfig>,
    pub storage: StorageConfig,
    pub log: LogWriterOptions,
}

impl Default for LogServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            opentelemetry: None,
            storage: StorageConfig::default(),
            log: LogWriterOptions::default(),
        }
    }
}

#[async_trait]
impl Configurable<LogServerConfig> for LogServer {
    async fn try_from_config(
        config: &LogServerConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        let storage = Storage::try_from_config(&config.storage, registry).await?;
        let storage = Arc::new(storage);
        Ok(Self {
            config: config.clone(),
            open_logs: Arc::new(StateHashTable::default()),
            storage,
        })
    }
}
