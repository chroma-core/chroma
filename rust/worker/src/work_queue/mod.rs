pub(crate) mod config;
pub mod server;
pub(crate) mod state;
#[cfg(test)]
pub(crate) mod tests;
pub mod types;
pub mod work_queue_client;
pub(crate) mod work_queue_manager;
pub(crate) mod work_queue_server;

pub use server::service_entrypoint;

// Match tonic's default receive limit explicitly so fn-consumer configuration
// validation does not silently drift from the work queue server's wire limit.
pub(crate) const GRPC_MAX_DECODING_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
pub(crate) const GET_WORK_RETRY_PUSHBACK_MS_METADATA: &str = "grpc-retry-pushback-ms";
