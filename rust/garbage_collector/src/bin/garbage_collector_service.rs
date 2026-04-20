use chroma_system::thread_stack_size_bytes;
use garbage_collector_library::garbage_collector_service_entrypoint;
use tracing::{error, info};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("chroma-gc")
        .thread_stack_size(thread_stack_size_bytes())
        .build()
        .unwrap()
        .block_on(async {
            info!("Starting garbage collector service");

            match garbage_collector_service_entrypoint().await {
                Ok(_) => info!("Garbage collector service completed successfully"),
                Err(e) => {
                    error!("Garbage collector service failed: {:?}", e);
                    panic!("Garbage collector service failed: {:?}", e);
                }
            }
        });
}
