use std::{
    io::SeekFrom,
    ops::{Bound, RangeBounds},
};

use anyhow::{anyhow, Ok, Result};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader},
};

use super::util::get_or_populate_cached_dataset_file;

pub struct Sift1MData {
    pub base: BufReader<File>,
    pub query: BufReader<File>,
    pub ground: BufReader<File>,
}

impl Sift1MData {
    pub async fn init() -> Result<Self> {
        let base = get_or_populate_cached_dataset_file(
            "gist",
            "base.fvecs",
            None,
            |mut writer| async move {
                let client = reqwest::Client::new();
                let response = client
                    .get(
                        "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift_base.fvecs",
                    )
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Failed to download Sift1M base data, got status code {}",
                        response.status()
                    ));
                }

                writer.write_all(&response.bytes().await?).await?;

                Ok(())
            },
        ).await?;
        let query = get_or_populate_cached_dataset_file(
            "gist",
            "query.fvecs",
            None,
            |mut writer| async move {
                let client = reqwest::Client::new();
                let response = client
                    .get(
                        "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift_query.fvecs",
                    )
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Failed to download Sift1M query data, got status code {}",
                        response.status()
                    ));
                }

                writer.write_all(&response.bytes().await?).await?;

                Ok(())
            },
        ).await?;
        let ground = get_or_populate_cached_dataset_file(
            "gist",
            "groundtruth.fvecs",
            None,
            |mut writer| async move {
                let client = reqwest::Client::new();
                let response = client
                    .get(
                        "https://huggingface.co/datasets/qbo-odp/sift1m/resolve/main/sift_groundtruth.ivecs",
                    )
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!(
                        "Failed to download Sift1M ground data, got status code {}",
                        response.status()
                    ));
                }

                writer.write_all(&response.bytes().await?).await?;

                Ok(())
            },
        ).await?;
        Ok(Self {
            base: BufReader::new(File::open(base).await?),
            query: BufReader::new(File::open(query).await?),
            ground: BufReader::new(File::open(ground).await?),
        })
    }

    pub fn collection_size() -> usize {
        1000000
    }

    pub fn query_size() -> usize {
        10000
    }

    pub fn dimension() -> usize {
        128
    }

    pub fn k() -> usize {
        100
    }

    pub async fn data_range(&mut self, range: impl RangeBounds<usize>) -> Result<Vec<Vec<f32>>> {
        let lower_bound = match range.start_bound() {
            Bound::Included(include) => *include,
            Bound::Excluded(exclude) => exclude + 1,
            Bound::Unbounded => 0,
        };
        let upper_bound = match range.end_bound() {
            Bound::Included(include) => include + 1,
            Bound::Excluded(exclude) => *exclude,
            Bound::Unbounded => usize::MAX,
        }
        .min(Self::collection_size());

        if lower_bound >= upper_bound {
            return Ok(Vec::new());
        }

        let start = SeekFrom::Start(
            (size_of::<u32>() + lower_bound * Self::dimension() * size_of::<f32>()) as u64,
        );
        self.base.seek(start).await?;
        let batch_size = upper_bound - lower_bound;
        let mut base_bytes = vec![0; batch_size * Self::dimension() * size_of::<f32>()];
        self.base.read_exact(&mut base_bytes).await?;
        let embedding_f32s: Vec<_> = base_bytes
            .chunks(size_of::<f32>())
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        Ok(embedding_f32s
            .chunks(Self::dimension())
            .map(|embedding| embedding.to_vec())
            .collect())
    }

    pub async fn query(&mut self) -> Result<Vec<(Vec<f32>, Vec<usize>)>> {
        let mut query_bytes = Vec::new();
        self.query.read_to_end(&mut query_bytes).await?;
        let (_, embeddings_bytes) = query_bytes.split_at(size_of::<u32>());
        let embedding_f32s: Vec<_> = embeddings_bytes
            .chunks(size_of::<f32>())
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();

        let mut ground_bytes = Vec::new();
        self.ground.read_to_end(&mut ground_bytes).await?;
        let (_, embeddings_bytes) = query_bytes.split_at(size_of::<u32>());
        let ground_u32s: Vec<_> = embeddings_bytes
            .chunks(size_of::<u32>())
            .map(|c| u32::from_le_bytes(c.try_into().unwrap()) as usize)
            .collect();
        Ok(embedding_f32s
            .chunks(Self::dimension())
            .zip(ground_u32s.chunks(Self::k()))
            .map(|(embedding, ground)| (embedding.to_vec(), ground.to_vec()))
            .collect())
    }
}
