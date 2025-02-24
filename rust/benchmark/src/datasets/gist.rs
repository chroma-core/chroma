use std::{collections::HashMap, path::PathBuf, pin::Pin};

use anyhow::{anyhow, Result};
use async_compression::tokio::bufread::GzipDecoder;
use async_stream::stream;
use futures::TryStreamExt;
use tar::Archive;
use tokio::io::{AsyncReadExt, BufReader};
use tokio_util::io::StreamReader;

use super::{types::Record, util::get_dataset_cache_path};

pub struct Gist1MDataset {
    base_file_path: PathBuf,
}

const FLOAT32_SIZE: usize = 4;
const U32_SIZE: usize = 4;

impl Gist1MDataset {
    const NAME: &'static str = "gist";
    const DOWNLOAD_URL: &'static str =
        "https://huggingface.co/datasets/fzliu/gist1m/resolve/main/gist.tar.gz";
    pub const DIMENSION: usize = 960;
    const NUM_RECORDS: usize = 1000000;

    pub async fn init() -> Result<Self> {
        println!("Downloading GIST1M dataset...");
        // let client = reqwest::Client::new();
        // let response = client.get(Self::DOWNLOAD_URL).send().await?;
        // if !response.status().is_success() {
        //     return Err(anyhow!(
        //         "Failed to download gist dataset, got status code {}",
        //         response.status()
        //     ));
        // }

        // // Create async stream reader
        // let byte_stream = response.bytes_stream();
        // let stream_reader = StreamReader::new(
        //     byte_stream.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e)),
        // );

        // // Create async gzip decoder
        // let mut gzip_decoder = GzipDecoder::new(stream_reader);

        // Create dataset directory
        let dataset_dir = get_dataset_cache_path(Self::NAME, None).await?;

        // Extract the contents to a path.
        // let tar_path = dataset_dir.join("output.tar");
        // let mut writer = tokio::fs::File::create(tar_path.clone()).await?;

        // tokio::io::copy(&mut gzip_decoder, &mut writer).await?;
        // println!("Extracting tar files to {:?}", dataset_dir);

        // let tar_file = std::fs::File::open(&tar_path)?;
        // let mut archive = Archive::new(tar_file);
        // archive.unpack(&dataset_dir)?;

        // println!("Extracted files to {:?}", dataset_dir);

        // tokio::fs::remove_file(tar_path).await?;

        Ok(Self {
            base_file_path: dataset_dir.join(Self::NAME).join("gist_base.fvecs"),
        })
    }

    pub async fn create_records_stream(
        &self,
        num_records: usize,
    ) -> anyhow::Result<Pin<Box<dyn futures::Stream<Item = anyhow::Result<super::types::Record>>>>>
    {
        let record_size = U32_SIZE + Self::DIMENSION * FLOAT32_SIZE;
        let file = tokio::fs::File::open(self.base_file_path.clone()).await?;
        // read and yield records.
        let mut buf_read = BufReader::new(file);
        let mut record_buf = vec![0u8; record_size];
        let mut record_count = 0;
        Ok(Box::pin(stream! {
            loop {
                if record_count >= num_records.min(Self::NUM_RECORDS) {
                    break;
                }

                buf_read.read_exact(&mut record_buf).await?;

                let (_, vec_values) = record_buf.split_at(FLOAT32_SIZE);
                let emb = vec_values
                    .chunks(FLOAT32_SIZE)
                    .map(|byte_slice| byte_slice.try_into().expect("Slice with wrong length!"))
                    .map(f32::from_le_bytes)
                    .collect::<Vec<f32>>();
                let record = Record {
                    document: "".to_string(),
                    metadata: HashMap::default(),
                    embedding: Some(emb),
                };

                record_count += 1;

                yield Ok(record);
            }
        }))
    }
}
