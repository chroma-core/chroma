use anyhow::{anyhow, Result};
use futures::io::BufReader;
use tokio::{
    fs::{File, ReadDir},
    io::AsyncWriteExt,
};

use super::util::get_or_populate_cached_dataset_file;

pub struct RustStack {
    pub train: BufReader<File>,
}

impl RustStack {
    pub async fn init() -> Result<Self> {
        let dir = read_dir("~/Desktop/rust-stack");
    }
}
