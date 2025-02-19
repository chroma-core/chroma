use std::path::PathBuf;

use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use futures::TryStreamExt;
use tar::Archive;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use super::{types::Record, util::get_dataset_cache_path};

pub struct Gist1MDataset {
    base_dir: PathBuf,
    download_url: String,
    base_file_path: PathBuf,
    query_file_path: PathBuf,
    dimension: usize,
    num_records: usize,
}

const FLOAT32_SIZE: usize = 4;

impl Gist1MDataset {
    const DISPLAY_NAME: &'static str = "Gist";
    const NAME: &'static str = "gist";

    pub async fn init() -> Result<Self> {
        let download_url = "https://huggingface.co/datasets/fzliu/gist1m/resolve/main/gist.tar.gz";
        let client = reqwest::Client::new();
        let response = client.get(download_url).send().await?;
        if !response.status().is_success() {
            return Err(anyhow!(
                "Failed to download gist dataset, got status code {}",
                response.status()
            ));
        }

        // Create async stream reader
        let byte_stream = response.bytes_stream();
        let stream_reader = StreamReader::new(
            byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        );

        // Create async gzip decoder
        let mut gzip_decoder = GzipDecoder::new(stream_reader);

        // Create dataset directory
        let dataset_dir = get_dataset_cache_path("gist", None).await?;

        // Extract the contents to a path.
        let tar_path = dataset_dir.join("output.tar");
        let mut writer = tokio::fs::File::create(tar_path.clone()).await?;

        tokio::io::copy(&mut gzip_decoder, &mut writer).await?;

        let tar_file = std::fs::File::open(&tar_path)?;
        let mut archive = Archive::new(tar_file);
        archive.unpack(&dataset_dir)?;

        tokio::fs::remove_file(tar_path).await?;

        Ok(Self {
            base_dir: dataset_dir.clone(),
            download_url: download_url.to_string(),
            base_file_path: dataset_dir.join("gist_base.fvecs"),
            query_file_path: dataset_dir.join("gist_query.fvecs"),
            dimension: 960,
            num_records: 1000000,
        })
    }

    pub async fn create_records_stream(
        &self,
    ) -> anyhow::Result<impl futures::Stream<Item = anyhow::Result<super::types::Record>>> {
        let num_records = 10000;
        let chunk_size = FLOAT32_SIZE + self.dimension * FLOAT32_SIZE;
        let total_bytes = num_records * chunk_size;
        let file = tokio::fs::File::open(self.base_file_path.clone()).await?;
        let mut buf_read = BufReader::new(file);
        let mut buf = vec![0u8; total_bytes];
        // TODO(Sanket): For now reading everything in one go. Need to read in chunks.
        let _ = buf_read.read_exact(&mut buf).await?;

        let mut vec_records = Vec::new();
        for bin_of_vec in buf.chunks(chunk_size) {
            let (_, vec_values) = bin_of_vec.split_at(FLOAT32_SIZE);
            let emb = vec_values
                .chunks(FLOAT32_SIZE)
                .map(|byte_slice| byte_slice.try_into().expect("Slice with wrong length!"))
                .map(f32::from_le_bytes)
                .collect::<Vec<f32>>();
            vec_records.push(Record {
                document: "".to_string(),
                metadata: Default::default(),
                embedding: Some(emb),
            });
        }
        println!("Read {:?} records", vec_records.len());

        Ok(futures::stream::iter(vec_records.into_iter().map(Ok)))
    }
}
