use chroma_frontend::frontend_service_entrypoint;
use chroma_system::thread_stack_size_bytes;
use std::sync::Arc;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("chroma-frontend")
        .thread_stack_size(thread_stack_size_bytes())
        .build()
        .unwrap()
        .block_on(frontend_service_entrypoint(
            Arc::new(()) as _,
            Arc::new(()) as _,
            true,
        ));
}
