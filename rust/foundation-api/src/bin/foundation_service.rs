use chroma_system::thread_stack_size_bytes;
use foundation_api::foundation_service_entrypoint;
use std::sync::Arc;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("foundation-api")
        .thread_stack_size(thread_stack_size_bytes())
        .build()
        .unwrap()
        .block_on(foundation_service_entrypoint(Arc::new(()) as _, true));
}
