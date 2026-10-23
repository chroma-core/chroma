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
            "sift1m",
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
            "sift1m",
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
            "sift1m",
            "groundtruth.ivecs",
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

        let vector_size = size_of::<u32>() + Self::dimension() * size_of::<f32>();

        let start = SeekFrom::Start((lower_bound * vector_size) as u64);
        self.base.seek(start).await?;
        let batch_size = upper_bound - lower_bound;
        let mut base_bytes = vec![0; batch_size * vector_size];
        self.base.read_exact(&mut base_bytes).await?;
        read_raw_vec(&base_bytes, |bytes| {
            Ok(f32::from_le_bytes(bytes.try_into()?))
        })
    }

    pub async fn query(&mut self) -> Result<Vec<(Vec<f32>, Vec<u32>)>> {
        let mut query_bytes = Vec::new();
        self.query.read_to_end(&mut query_bytes).await?;
        let queries = read_raw_vec(&query_bytes, |bytes| {
            Ok(f32::from_le_bytes(bytes.try_into()?))
        })?;

        let mut ground_bytes = Vec::new();
        self.ground.read_to_end(&mut ground_bytes).await?;
        let grounds = read_raw_vec(&ground_bytes, |bytes| {
            Ok(u32::from_le_bytes(bytes.try_into()?))
        })?;
        if queries.len() != grounds.len() {
            return Err(anyhow!(
                "Queries and grounds count mismatch: {} != {}",
                queries.len(),
                grounds.len()
            ));
        }
        Ok(queries.into_iter().zip(grounds).collect())
    }
}

fn read_raw_vec<T>(
    raw_bytes: &[u8],
    convert_from_bytes: impl Fn(&[u8]) -> Result<T>,
) -> Result<Vec<Vec<T>>> {
    let mut result = Vec::new();
    let mut bytes = raw_bytes;
    while !bytes.is_empty() {
        let (dimension_bytes, rem_bytes) = bytes.split_at(size_of::<u32>());
        let dimension = u32::from_le_bytes(dimension_bytes.try_into()?);
        let (embedding_bytes, rem_bytes) = rem_bytes.split_at(dimension as usize * size_of::<T>());
        let embedding = embedding_bytes
            .chunks(size_of::<T>())
            .map(&convert_from_bytes)
            .collect::<Result<Vec<T>>>()?;
        if embedding.len() != dimension as usize {
            return Err(anyhow!(
                "Embedding dimension mismatch: {} != {}",
                embedding.len(),
                dimension
            ));
        }
        result.push(embedding);
        bytes = rem_bytes;
    }
    Ok(result)
}
