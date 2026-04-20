use worker::{compaction_service_entrypoint, load_root_config};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    let root_config = load_root_config();
    let runtime =
        chroma_system::build_tokio_main_runtime(&root_config.compaction_service.dispatcher)
            .expect("failed to build chroma-main tokio runtime");
    runtime.block_on(compaction_service_entrypoint());
}
