use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use figment::providers::{Env, Format, Yaml};
use futures::stream::StreamExt;
use tokio::signal::unix::{signal, SignalKind};
use tonic::{transport::Server, Request, Response, Status};

use chroma_config::helpers::{deserialize_duration_from_seconds, serialize_duration_to_seconds};
use chroma_config::Configurable;
use chroma_error::ChromaError;
use chroma_storage::config::StorageConfig;
use chroma_storage::Storage;
use chroma_sysdb::{SysDb, SysDbConfig};
use chroma_tracing::OtelFilter;
use chroma_tracing::OtelFilterLevel;
use chroma_types::chroma_proto::heap_tender_service_server::{
    HeapTenderService, HeapTenderServiceServer,
};
use chroma_types::chroma_proto::{HeapSummaryRequest, HeapSummaryResponse};
use chroma_types::{dirty_log_path_from_hostname, CollectionUuid, DirtyMarker, ScheduleEntry};
use s3heap::{Configuration, DummyScheduler, Error, HeapWriter, Schedule, Triggerable};
use wal3::{
    Cursor, CursorName, CursorStore, CursorStoreOptions, LogPosition, LogReader, LogReaderOptions,
    Witness,
};

///////////////////////////////////////////// constants ////////////////////////////////////////////

const DEFAULT_CONFIG_PATH: &str = "./chroma_config.yaml";

const CONFIG_PATH_ENV_VAR: &str = "CONFIG_PATH";

/// The path for the heap tended to on behalf of this hostname.
pub fn heap_path_from_hostname(hostname: &str) -> String {
    format!("heap/{}", hostname)
}

/// The cursor name used by HeapTender to track its position in the dirty log.
pub static HEAP_TENDER_CURSOR_NAME: CursorName =
    unsafe { CursorName::from_string_unchecked("heap_tender") };

//////////////////////////////////////////// HeapTender ////////////////////////////////////////////

/// Manages heap compaction by reading dirty logs and coordinating with HeapWriter.
pub struct HeapTender {
    #[allow(dead_code)]
    sysdb: SysDb,
    reader: LogReader,
    cursor: CursorStore,
    writer: HeapWriter,
}

impl HeapTender {
    /// Creates a new HeapTender.
    pub fn new(sysdb: SysDb, reader: LogReader, cursor: CursorStore, writer: HeapWriter) -> Self {
        Self {
            sysdb,
            reader,
            cursor,
            writer,
        }
    }

    /// Tends to the heap by reading and coalescing the dirty log, then updating the cursor.
    pub async fn tend_to_heap(&self) -> Result<(), Error> {
        let (witness, cursor, tended) = self.read_and_coalesce_dirty_log().await?;
        if !tended.is_empty() {
            let collection_ids = tended.iter().map(|t| t.0).collect::<Vec<_>>();
            let scheduled = self
                .sysdb
                .clone()
                .peek_schedule_by_collection_id(&collection_ids)
                .await?;
            let triggerables: Vec<Option<Schedule>> = scheduled
                .into_iter()
                .map(|s: ScheduleEntry| -> Result<_, Error> {
                    let triggerable = Triggerable {
                        partitioning: s3heap::UnitOfPartitioningUuid::new(s.collection_id.0),
                        scheduling: s3heap::UnitOfSchedulingUuid::new(s.task_id),
                    };
                    if let Some(next_scheduled) = s.when_to_run {
                        let schedule = Schedule {
                            triggerable,
                            next_scheduled,
                            nonce: s.task_run_nonce,
                        };
                        Ok(Some(schedule))
                    } else {
                        Ok(None)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?;
            let triggerables: Vec<Schedule> = triggerables.into_iter().flatten().collect();
            tracing::info!("JUST {} TRIGGERABLES", triggerables.len());
            if !triggerables.is_empty() {
                self.writer.push(&triggerables).await?;
            }
            if let Some(witness) = witness.as_ref() {
                self.cursor
                    .save(&HEAP_TENDER_CURSOR_NAME, &cursor, witness)
                    .await?;
            } else {
                self.cursor
                    .init(&HEAP_TENDER_CURSOR_NAME, cursor.clone())
                    .await?;
            }
        }
        Ok(())
    }

    /// Reads the dirty log and coalesces entries by collection.
    pub async fn read_and_coalesce_dirty_log(
        &self,
    ) -> Result<(Option<Witness>, Cursor, Vec<(CollectionUuid, LogPosition)>), Error> {
        let witness = self.cursor.load(&HEAP_TENDER_CURSOR_NAME).await?;
        let position = match self.reader.oldest_timestamp().await {
            Ok(position) => position,
            Err(wal3::Error::UninitializedLog) => {
                tracing::info!("empty dirty log");
                let default_cursor = Cursor {
                    position: LogPosition::from_offset(0),
                    epoch_us: 0,
                    writer: "heap-tender".to_string(),
                };
                return Ok((witness, default_cursor, vec![]));
            }
            Err(e) => return Err(Error::Wal3(e)),
        };
        let default = Cursor {
            position,
            epoch_us: position.offset(),
            writer: "heap-tender".to_string(),
        };
        let start_cursor = witness
            .as_ref()
            .map(|w| w.cursor())
            .unwrap_or(&default)
            .clone();
        let mut limit_cursor = start_cursor.clone();
        tracing::info!("cursoring from {start_cursor:?}");
        let dirty_fragments = match self
            .reader
            .scan(
                start_cursor.position,
                wal3::Limits {
                    max_files: None,
                    max_bytes: None,
                    max_records: None,
                },
            )
            .await
        {
            Ok(dirty_fragments) => dirty_fragments,
            Err(wal3::Error::UninitializedLog) => {
                tracing::info!("empty dirty log");
                return Ok((witness, limit_cursor, vec![]));
            }
            Err(e) => {
                return Err(Error::Wal3(e));
            }
        };
        let dirty_futures = dirty_fragments
            .iter()
            .map(|fragment| async {
                let (_, records, _) = self.reader.read_parquet(fragment).await?;
                let dirty_markers = records
                    .into_iter()
                    .map(|x| -> Result<(LogPosition, DirtyMarker), Error> {
                        let dirty = serde_json::from_slice::<DirtyMarker>(&x.1)?;
                        Ok((x.0, dirty))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok::<_, Error>(dirty_markers)
            })
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(dirty_futures);
        let mut buffered = stream.buffer_unordered(10);
        let mut collections: HashMap<CollectionUuid, LogPosition> = HashMap::default();
        while let Some(res) = buffered.next().await {
            for (position, marker) in res? {
                limit_cursor.position = std::cmp::max(limit_cursor.position, position + 1u64);
                if let DirtyMarker::MarkDirty {
                    collection_id,
                    log_position,
                    num_records,
                    reinsert_count,
                    ..
                } = marker
                {
                    if reinsert_count == 0 {
                        let collection_position = collections.entry(collection_id).or_default();
                        *collection_position = std::cmp::max(
                            *collection_position,
                            LogPosition::from_offset(
                                log_position
                                    .checked_add(num_records)
                                    .ok_or(Error::Internal("log position overflow".to_string()))?,
                            ),
                        );
                    }
                }
            }
        }
        Ok((witness, limit_cursor, collections.into_iter().collect()))
    }

    async fn background(tender: Arc<Self>, poll_interval: Duration) {
        loop {
            tokio::time::sleep(poll_interval).await;
            if let Err(err) = tender.tend_to_heap().await {
                tracing::error!("could not roll up dirty log: {err:?}");
            }
        }
    }
}

///////////////////////////////////////// HeapTenderServer /////////////////////////////////////////

struct HeapTenderServer {
    config: HeapTenderServerConfig,
    tender: Arc<HeapTender>,
}

impl HeapTenderServer {
    async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        let addr = format!("[::]:{}", self.config.port).parse().unwrap();
        println!("Heap tender listening on {}", addr);

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<HeapTenderServiceServer<Self>>()
            .await;

        let max_encoding_message_size = self.config.max_encoding_message_size;
        let max_decoding_message_size = self.config.max_decoding_message_size;
        let shutdown_grace_period = self.config.grpc_shutdown_grace_period;
        let tender = Arc::clone(&self.tender);
        let background =
            tokio::task::spawn(HeapTender::background(tender, self.config.poll_interval));

        let server = Server::builder()
            .layer(chroma_tracing::GrpcServerTraceLayer)
            .add_service(health_service)
            .add_service(
                HeapTenderServiceServer::new(self)
                    .max_decoding_message_size(max_decoding_message_size)
                    .max_encoding_message_size(max_encoding_message_size),
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
            tracing::info!("Received SIGTERM, waiting for grace period...");
            tokio::time::sleep(shutdown_grace_period).await;
            tracing::info!("Grace period ended, shutting down server...");
        });

        let res = server.await;
        background.abort();
        Ok(res?)
    }
}

#[async_trait::async_trait]
impl Configurable<HeapTenderServerConfig> for HeapTenderServer {
    async fn try_from_config(
        config: &HeapTenderServerConfig,
        registry: &chroma_config::registry::Registry,
    ) -> Result<Self, Box<dyn ChromaError>> {
        match &config.sysdb {
            chroma_sysdb::SysDbConfig::Grpc(_) => {}
            chroma_sysdb::SysDbConfig::Sqlite(_) => {
                panic!("Expected grpc sysdb config, got sqlite sysdb config")
            }
        };
        let sysdb = SysDb::try_from_config(&config.sysdb, registry).await?;
        let storage = Storage::try_from_config(&config.storage, registry).await?;
        let dirty_log_prefix = dirty_log_path_from_hostname(&config.my_member_id);
        let reader = LogReader::new(
            config.reader.clone(),
            Arc::new(storage.clone()),
            dirty_log_prefix.clone(),
        );
        let cursor = CursorStore::new(
            config.cursor.clone(),
            Arc::new(storage.clone()),
            dirty_log_prefix.clone(),
            "s3heap-tender".to_string(),
        );
        let heap_prefix = heap_path_from_hostname(&config.my_member_id);
        let scheduler = Arc::new(DummyScheduler) as _;
        let writer = HeapWriter::new(storage, heap_prefix, Arc::clone(&scheduler))
            .map_err(|e| -> Box<dyn chroma_error::ChromaError> { Box::new(e) })?;
        let tender = Arc::new(HeapTender {
            sysdb,
            reader,
            cursor,
            writer,
        });
        Ok(Self {
            config: config.clone(),
            tender,
        })
    }
}

#[async_trait::async_trait]
impl HeapTenderService for HeapTenderServer {
    async fn summary(
        &self,
        _request: Request<HeapSummaryRequest>,
    ) -> Result<Response<HeapSummaryResponse>, Status> {
        todo!();
    }
}

//////////////////////////////////////////// RootConfig ////////////////////////////////////////////

/// Root configuration for the heap tender service.
///
/// This is the top-level configuration structure loaded from YAML config files
/// and environment variables. It contains all configuration sections needed to
/// run the heap tender service.
#[derive(serde::Deserialize, serde::Serialize)]
pub struct RootConfig {
    /// Configuration for the heap tender gRPC service.
    #[serde(default)]
    pub heap_tender_service: HeapTenderServerConfig,
}

impl RootConfig {
    /// # Description
    /// Load the config from the default location.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The default location is the current working directory, with the filename chroma_config.yaml.
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    pub fn load() -> Self {
        Self::load_from_path(DEFAULT_CONFIG_PATH)
    }

    /// # Description
    /// Load the config from a specific location.
    /// # Arguments
    /// - path: The path to the config file.
    /// # Returns
    /// The config object.
    /// # Panics
    /// - If the config file cannot be read.
    /// - If the config file is not valid YAML.
    /// - If the config file does not contain the required fields.
    /// - If the config file contains invalid values.
    /// - If the environment variables contain invalid values.
    /// # Notes
    /// The environment variables are prefixed with CHROMA_ and are uppercase.
    /// Values in the envionment variables take precedence over values in the YAML file.
    // NOTE:  Copied to ../load/src/config.rs.
    pub fn load_from_path(path: &str) -> Self {
        println!("loading config from {path}");
        println!(
            r#"Full config is:
================================================================================
{}
================================================================================
"#,
            std::fs::read_to_string(path)
                .expect("should be able to open and read config to string")
        );
        // Unfortunately, figment doesn't support environment variables with underscores. So we have to map and replace them.
        // Excluding our own environment variables, which are prefixed with CHROMA_.
        let mut f = figment::Figment::from(Env::prefixed("CHROMA_").map(|k| match k {
            k if k == "my_member_id" => k.into(),
            k => k.as_str().replace("__", ".").into(),
        }));
        if std::path::Path::new(path).exists() {
            f = figment::Figment::from(Yaml::file(path)).merge(f);
        }
        // Apply defaults - this seems to be the best way to do it.
        // https://github.com/SergioBenitez/Figment/issues/77#issuecomment-1642490298
        // f = f.join(Serialized::default(
        //     "worker.num_indexing_threads",
        //     num_cpus::get(),
        // ));
        let res = f.extract();
        match res {
            Ok(config) => config,
            Err(e) => panic!("Error loading config: {}", e),
        }
    }
}

/// Configuration for OpenTelemetry tracing and observability.
///
/// Controls how the service exports traces and metrics to an OpenTelemetry collector.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct OpenTelemetryConfig {
    /// The OpenTelemetry collector endpoint URL.
    #[serde(default = "OpenTelemetryConfig::default_endpoint")]
    pub endpoint: String,
    /// The service name reported in traces.
    #[serde(default = "OpenTelemetryConfig::default_otel_service_name")]
    pub service_name: String,
    /// Trace level filters for different crates.
    #[serde(default = "OpenTelemetryConfig::default_otel_filters")]
    pub filters: Vec<OtelFilter>,
}

impl OpenTelemetryConfig {
    fn default_endpoint() -> String {
        "http://otel-collector:4317".to_string()
    }

    fn default_otel_service_name() -> String {
        "rust-log-service".to_string()
    }

    fn default_otel_filters() -> Vec<OtelFilter> {
        vec![OtelFilter {
            crate_name: "s3heap".to_string(),
            filter_level: OtelFilterLevel::Trace,
        }]
    }
}

/// Configuration for the heap tender gRPC service.
///
/// Contains all settings needed to run the heap tender server, including network
/// configuration, storage backend settings, and gRPC message size limits.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct HeapTenderServerConfig {
    /// The port to bind the gRPC server to.
    #[serde(default = "HeapTenderServerConfig::default_port")]
    pub port: u16,
    /// The member ID of this service instance in the cluster.
    #[serde(default = "HeapTenderServerConfig::default_my_member_id")]
    pub my_member_id: String,
    /// Optional OpenTelemetry configuration for tracing.
    #[serde(default)]
    pub opentelemetry: Option<OpenTelemetryConfig>,
    /// Configuration for the sysdb backend.
    #[serde(default = "HeapTenderServerConfig::default_sysdb_config")]
    pub sysdb: SysDbConfig,
    /// Configuration for the S3 storage backend.
    #[serde(default)]
    pub storage: StorageConfig,
    /// wal3 configuration of the dirty log reader.
    #[serde(default)]
    pub reader: LogReaderOptions,
    /// wal3 configuration of the cursor store.
    #[serde(default)]
    pub cursor: CursorStoreOptions,
    /// Configuration of the HeapWriter.
    #[serde(default)]
    pub writer: Configuration,
    /// Periodicity of poll from end of one poll of the dirty log to start of the next.
    #[serde(default = "HeapTenderServerConfig::default_poll_interval")]
    pub poll_interval: Duration,
    /// Maximum size in bytes for outgoing gRPC messages.
    #[serde(default = "HeapTenderServerConfig::default_max_encoding_message_size")]
    pub max_encoding_message_size: usize,
    /// Maximum size in bytes for incoming gRPC messages.
    #[serde(default = "HeapTenderServerConfig::default_max_decoding_message_size")]
    pub max_decoding_message_size: usize,
    /// Grace period to wait before shutting down the gRPC server on SIGTERM.
    #[serde(
        rename = "grpc_shutdown_grace_period_seconds",
        deserialize_with = "deserialize_duration_from_seconds",
        serialize_with = "serialize_duration_to_seconds",
        default = "HeapTenderServerConfig::default_grpc_shutdown_grace_period"
    )]
    pub grpc_shutdown_grace_period: Duration,
}

impl HeapTenderServerConfig {
    fn default_port() -> u16 {
        50052
    }

    fn default_my_member_id() -> String {
        "rust-log-service-0".to_string()
    }

    fn default_max_encoding_message_size() -> usize {
        32_000_000
    }

    fn default_max_decoding_message_size() -> usize {
        32_000_000
    }

    fn default_poll_interval() -> Duration {
        Duration::from_secs(10)
    }

    fn default_sysdb_config() -> SysDbConfig {
        SysDbConfig::Grpc(Default::default())
    }

    fn default_grpc_shutdown_grace_period() -> Duration {
        Duration::from_secs(1)
    }
}

impl Default for HeapTenderServerConfig {
    fn default() -> Self {
        Self {
            port: HeapTenderServerConfig::default_port(),
            my_member_id: HeapTenderServerConfig::default_my_member_id(),
            opentelemetry: None,
            sysdb: HeapTenderServerConfig::default_sysdb_config(),
            storage: StorageConfig::default(),
            reader: LogReaderOptions::default(),
            cursor: CursorStoreOptions::default(),
            writer: Configuration::default(),
            poll_interval: Self::default_poll_interval(),
            max_encoding_message_size: Self::default_max_encoding_message_size(),
            max_decoding_message_size: Self::default_max_decoding_message_size(),
            grpc_shutdown_grace_period: Self::default_grpc_shutdown_grace_period(),
        }
    }
}

//////////////////////////////////////////// entrypoint ////////////////////////////////////////////

/// Main entrypoint for the heap tender service.
///
/// Loads configuration from YAML files and environment variables, initializes
/// OpenTelemetry tracing if configured, creates the heap tender server, and runs
/// the gRPC service until a SIGTERM is received.
pub async fn entrypoint() {
    let config = match std::env::var(CONFIG_PATH_ENV_VAR) {
        Ok(config_path) => RootConfig::load_from_path(&config_path),
        Err(_) => RootConfig::load(),
    };
    let config = config.heap_tender_service;
    eprintln!("my_member_id: {}", config.my_member_id);
    let registry = chroma_config::registry::Registry::new();
    if let Some(otel_config) = &config.opentelemetry {
        eprintln!("enabling tracing");
        chroma_tracing::init_otel_tracing(
            &otel_config.service_name,
            &otel_config.filters,
            &otel_config.endpoint,
        );
    } else {
        eprintln!("tracing disabled");
    }
    let heap_tender_server = HeapTenderServer::try_from_config(&config, &registry)
        .await
        .expect("Failed to create heap tender server");

    let server_join_handle = tokio::spawn(async move {
        match heap_tender_server.run().await {
            Ok(_) => {}
            Err(e) => {
                tracing::error!("Server terminated with error: {:?}", e);
            }
        }
    });

    match server_join_handle.await {
        Ok(_) => {}
        Err(e) => {
            tracing::error!("Error terminating server: {:?}", e);
        }
    }
}
