use garbage_collector_library::garbage_collector_service_entrypoint;
use tracing::{error, info};

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
