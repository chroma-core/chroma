//! Deep10M dataset: 10M vectors, 96 dimensions, Euclidean distance.
//!
//! Source: Yandex Research (Babenko & Lempitsky, CVPR 2016).
//! Base vectors (10M) from HuggingFace `2026peng/deep1b`.
//! Queries (10K) and ground truth (100-NN) from Yandex cloud storage.
//!
//! All files use a simple binary format:
//!   .fbin: [num_vectors: u32, dim: u32, data: f32 * num_vectors * dim]
//!   .ibin: [num_vectors: u32, dim: u32, data: i32 * num_vectors * dim]

use std::fs::{self, File};
use std::io::{self, Read as _, Write as _};
use std::path::PathBuf;
use std::sync::Arc;

use chroma_distance::DistanceFunction;

use super::{Dataset, LazyShardLoader, Query};

const REPO_ID: &str = "2026peng/deep1b";
pub const DIMENSION: usize = 96;
pub const DATA_LEN: usize = 10_000_000;
const K: usize = 100;

const QUERY_URL: &str =
    "https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/query.public.10K.fbin";
const GT_URL: &str =
    "https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/groundtruth.public.10K.ibin";

fn cache_dir() -> PathBuf {
    dirs::home_dir()
        .expect("failed to get home directory")
        .join(".cache/deep10m")
}

fn download_if_missing(url: &str, dest: &PathBuf) -> io::Result<()> {
    if dest.exists() {
        return Ok(());
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    println!("  Downloading {}...", url);
    let resp = ureq::get(url).call().map_err(|e| {
        io::Error::new(io::ErrorKind::ConnectionRefused, format!("HTTP error: {e}"))
    })?;
    let mut reader = resp.into_reader();
    let tmp = dest.with_extension("tmp");
    let mut file = File::create(&tmp)?;
    let mut buf = vec![0u8; 1 << 20];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        file.write_all(&buf[..n])?;
    }
    fs::rename(&tmp, dest)?;
    Ok(())
}

fn read_fbin(path: &PathBuf, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
    let mut file = File::open(path)?;
    let mut header = [0u8; 8];
    file.read_exact(&mut header)?;
    let num_vectors = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let dim = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;

    let end = (offset + limit).min(num_vectors);
    if offset >= end {
        return Ok(Vec::new());
    }

    let bytes_per_vec = dim * 4;
    let skip_bytes = 8 + offset * bytes_per_vec;

    use std::io::Seek;
    file.seek(std::io::SeekFrom::Start(skip_bytes as u64))?;

    let count = end - offset;
    let mut buf = vec![0u8; count * bytes_per_vec];
    file.read_exact(&mut buf)?;

    let floats: &[f32] =
        unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const f32, count * dim) };

    let result: Vec<(u32, Arc<[f32]>)> = (0..count)
        .map(|i| {
            let start = i * dim;
            let vec: Arc<[f32]> = Arc::from(&floats[start..start + dim]);
            ((offset + i) as u32, vec)
        })
        .collect();

    Ok(result)
}

fn read_fbin_all(path: &PathBuf) -> io::Result<Vec<Vec<f32>>> {
    let mut file = File::open(path)?;
    let mut header = [0u8; 8];
    file.read_exact(&mut header)?;
    let num_vectors = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let dim = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;

    let mut buf = vec![0u8; num_vectors * dim * 4];
    file.read_exact(&mut buf)?;

    let floats: &[f32] =
        unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const f32, num_vectors * dim) };

    let result: Vec<Vec<f32>> = (0..num_vectors)
        .map(|i| floats[i * dim..(i + 1) * dim].to_vec())
        .collect();

    Ok(result)
}

fn read_ibin_all(path: &PathBuf) -> io::Result<Vec<Vec<u32>>> {
    let mut file = File::open(path)?;
    let mut header = [0u8; 8];
    file.read_exact(&mut header)?;
    let num_vectors = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let dim = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;

    let mut buf = vec![0u8; num_vectors * dim * 4];
    file.read_exact(&mut buf)?;

    let ints: &[i32] =
        unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const i32, num_vectors * dim) };

    let result: Vec<Vec<u32>> = (0..num_vectors)
        .map(|i| {
            ints[i * dim..(i + 1) * dim]
                .iter()
                .map(|&v| v as u32)
                .collect()
        })
        .collect();

    Ok(result)
}

pub struct Deep10M {
    loader: LazyShardLoader,
}

impl Deep10M {
    pub async fn load() -> io::Result<Self> {
        println!("Loading Deep-10M from HuggingFace Hub + Yandex...");

        let loader = LazyShardLoader::new(REPO_ID, vec!["base.10M.fbin".to_string()])?;

        let query_path = cache_dir().join("query.public.10K.fbin");
        let gt_path = cache_dir().join("groundtruth.public.10K.ibin");
        download_if_missing(QUERY_URL, &query_path)?;
        download_if_missing(GT_URL, &gt_path)?;

        Ok(Self { loader })
    }
}

impl Dataset for Deep10M {
    fn name(&self) -> &str {
        "deep-10m"
    }

    fn dimension(&self) -> usize {
        DIMENSION
    }

    fn data_len(&self) -> usize {
        DATA_LEN
    }

    fn k(&self) -> usize {
        K
    }

    fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let base_path = self.loader.get(0)?;
        read_fbin(&base_path, offset, limit)
    }

    fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        if distance_function != DistanceFunction::Euclidean {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Deep10M dataset only has L2 ground truth",
            ));
        }

        let query_path = cache_dir().join("query.public.10K.fbin");
        let gt_path = cache_dir().join("groundtruth.public.10K.ibin");

        let query_vectors = read_fbin_all(&query_path)?;
        let gt_neighbors = read_ibin_all(&gt_path)?;

        if query_vectors.len() != gt_neighbors.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "query/GT count mismatch: {} queries vs {} GT entries",
                    query_vectors.len(),
                    gt_neighbors.len()
                ),
            ));
        }

        let queries: Vec<Query> = query_vectors
            .into_iter()
            .zip(gt_neighbors)
            .map(|(vector, neighbors)| Query {
                vector,
                neighbors,
                max_vector_id: DATA_LEN as u64,
            })
            .collect();

        Ok(queries)
    }
}
