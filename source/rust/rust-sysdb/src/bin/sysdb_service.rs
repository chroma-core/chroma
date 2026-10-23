use chroma_system::thread_stack_size_bytes;
use rust_sysdb::sysdb_service_entrypoint;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("chroma-rust-sysdb")
        .thread_stack_size(thread_stack_size_bytes())
        .build()
        .unwrap()
        .block_on(async {
            Box::pin(sysdb_service_entrypoint()).await;
        });
}
