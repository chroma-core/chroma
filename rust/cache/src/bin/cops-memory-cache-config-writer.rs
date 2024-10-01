use clap::Parser;

use chroma_cache::{CacheConfig, FoyerCacheConfig};

fn main() {
    let mut config = FoyerCacheConfig::parse();
    config.dir = None;
    let out = serde_yaml::to_string(&CacheConfig::Memory(config)).unwrap();
    print!("{out}");
}
