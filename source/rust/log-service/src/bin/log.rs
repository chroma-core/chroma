fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        // NOTE(rescrv):  Default stack size overflows on request to move logs.
        .thread_stack_size(16 * 1024 * 1024)
        .build()
        .unwrap()
        .block_on(chroma_log_service::log_entrypoint());
}
