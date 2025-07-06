use garbage_collector_library::garbage_collector_service_entrypoint;
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[tokio::main]
async fn main() {
    info!("Starting garbage collector service");

    match garbage_collector_service_entrypoint().await {
        Ok(_) => info!("Garbage collector service completed successfully"),
        Err(e) => {
            error!("Garbage collector service failed: {:?}", e);
            panic!("Garbage collector service failed: {:?}", e);
        }
    }
}
