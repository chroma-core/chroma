//! Writes a memory cache config to stdout.

use clap::Parser;

use chroma_cache::{CacheConfig, DiskFieldValue, FoyerCacheConfig};

fn main() {
    let mut config = FoyerCacheConfig::parse();
    config.disk = DiskFieldValue::MultiDisk(vec![]);
    let out = serde_yaml::to_string(&CacheConfig::Memory(config)).unwrap();
    print!("{out}");
}
