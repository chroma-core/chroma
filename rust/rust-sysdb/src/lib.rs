use crate::server::SysdbService;

pub mod server;

pub async fn sysdb_service_entrypoint() {
    // TODO(Sanket): Config.
    let port = 50051;
    let service = SysdbService::new(port);
    if let Err(e) = service.run().await {
        // TODO(Sanket): Switch to tracing instead of println.
        println!("Server error: {} exiting", e);
    }
}
