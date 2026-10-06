use chroma_frontend::{
    auth::{AuthenticateAndAuthorize, DashboardAuth},
    frontend_service_entrypoint,
};
use chroma_system::thread_stack_size_bytes;
use std::sync::Arc;

fn main() {
    let auth: Arc<dyn AuthenticateAndAuthorize> =
        match DashboardAuth::from_env().expect("Invalid dashboard auth configuration") {
            Some(auth) => Arc::new(auth),
            None => Arc::new(()),
        };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("chroma-frontend")
        .thread_stack_size(thread_stack_size_bytes())
        .build()
        .unwrap()
        .block_on(frontend_service_entrypoint(auth, Arc::new(()) as _, true));
}
