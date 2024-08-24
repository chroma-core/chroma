#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;
use worker::compaction_service_entrypoint;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    compaction_service_entrypoint().await;
}
