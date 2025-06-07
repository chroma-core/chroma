use worker::compaction_service_entrypoint;

#[cfg(all(not(target_env = "msvc"), not(target_os = "ios"), not(target_os = "tvos")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(not(target_env = "msvc"), not(target_os = "ios"), not(target_os = "tvos")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    compaction_service_entrypoint().await;
}
