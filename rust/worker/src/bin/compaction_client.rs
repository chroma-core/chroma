use chroma_system::thread_stack_size_bytes;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("chroma-compaction-client")
        .thread_stack_size(thread_stack_size_bytes())
        .build()
        .unwrap()
        .block_on(worker::compaction_client_entrypoint());
}
