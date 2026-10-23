use chroma_system::thread_stack_size_bytes;
use worker::compaction_service_entrypoint;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("chroma-compaction")
        .thread_stack_size(thread_stack_size_bytes())
        .build()
        .unwrap()
        .block_on(compaction_service_entrypoint());
}
