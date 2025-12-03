//! Writes a disk cache config to stdout.

use clap::Parser;

use chroma_cache::{CacheConfig, FoyerCacheConfig};

fn main() {
    let config = FoyerCacheConfig::parse();
    if config.dir.is_none() {
        panic!("Disk cache is required for disk cache config writer");
    }
    let out = serde_yaml::to_string(&CacheConfig::Disk(config)).unwrap();
    print!("{out}");
}
