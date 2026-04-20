//! BigANN SIFT1B (TEXMEX): 128-byte SIFT descriptors, up to 1B base vectors, L2 / Euclidean.
//!
//! Official corpus and formats: http://corpus-texmex.irisa.fr/ (ANN_SIFT1B: `bigann_base.bvecs`,
//! `bigann_query.bvecs`, ground truth tar with subset `idx_*.ivecs` for 1M,2M,5M,10M,20M,50M,
//! 100M,200M,500M,1B). NeurIPS'21 track context: https://big-ann-benchmarks.com/neurips21.html
//!
//! A HuggingFace mirror exists (`fzliu/sift1b`) but row groups are huge; this module reads the
//! standard TEXMEX **local** `.bvecs` / `.ivecs` layout for reliable random access on `load_range`.
//!
//! ## Setup
//!
//! 1. Download and unpack TEXMEX files under one directory (e.g. `/data/bigann/`):
//!    - `bigann_base.bvecs` (92 GiB compressed; use uncompressed `.bvecs` for seeking)
//!    - `bigann_query.bvecs`
//!    - Ground truth: unpack `bigann_gnd.tar.gz` so that `idx_100M.ivecs` (or your subset) is
//!      reachable (see resolution order below).
//!
//! 2. Environment:
//!    - **`CHROMA_BIGANN_SIFT1B_DIR`** (required): directory containing `bigann_base.bvecs` and
//!      `bigann_query.bvecs`.
//!    - **`CHROMA_BIGANN_SIFT1B_NUM_VECTORS`** (optional): indexed corpus size (default `100000000`).
//!      Must match a TEXMEX subset for which you have `idx_<suffix>.ivecs` (e.g. `100M`).
//!    - **`CHROMA_BIGANN_GROUNDTRUTH`** (optional): explicit path to one `.ivecs` file (overrides
//!      automatic `idx_<suffix>.ivecs` lookup).

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chroma_distance::DistanceFunction;

use super::{Dataset, Query};

pub const DIMENSION: usize = 128;
pub const DATA_LEN_FULL: usize = 1_000_000_000;
/// Ground truth vector count in official BigANN `.ivecs` files for SIFT1B.
pub const GT_K: usize = 1000;

const BASE_FILE: &str = "bigann_base.bvecs";
const QUERY_FILE: &str = "bigann_query.bvecs";

const BYTES_PER_BASE_VEC: usize = 4 + DIMENSION;

/// Subset sizes (first *n* base vectors) for which TEXMEX ships `idx_<suffix>.ivecs`.
const SUBSET_SUFFIXES: &[(usize, &str)] = &[
    (1_000_000, "1M"),
    (2_000_000, "2M"),
    (5_000_000, "5M"),
    (10_000_000, "10M"),
    (20_000_000, "20M"),
    (50_000_000, "50M"),
    (100_000_000, "100M"),
    (200_000_000, "200M"),
    (500_000_000, "500M"),
    (1_000_000_000, "1B"),
];

fn subset_suffix(n: usize) -> Option<&'static str> {
    SUBSET_SUFFIXES
        .iter()
        .find(|(size, _)| *size == n)
        .map(|(_, s)| *s)
}

fn resolve_ground_truth_path(dir: &Path, data_len: usize) -> io::Result<PathBuf> {
    if let Ok(p) = std::env::var("CHROMA_BIGANN_GROUNDTRUTH") {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Ok(path);
        }
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "CHROMA_BIGANN_GROUNDTRUTH={} is not a file",
                path.display()
            ),
        ));
    }

    let suffix = subset_suffix(data_len).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "CHROMA_BIGANN_SIFT1B_NUM_VECTORS={data_len} has no standard TEXMEX subset; use one of {:?} or set CHROMA_BIGANN_GROUNDTRUTH to a .ivecs file",
                SUBSET_SUFFIXES.iter().map(|(n, _)| *n).collect::<Vec<_>>()
            ),
        )
    })?;

    let fname = format!("idx_{suffix}.ivecs");
    let candidates = [
        dir.join(&fname),
        dir.join("gnd").join(&fname),
        dir.join("bigann_gnd").join(&fname),
    ];
    for c in &candidates {
        if c.is_file() {
            return Ok(c.clone());
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!(
            "Ground truth not found for subset {suffix}. Tried: {}. Set CHROMA_BIGANN_GROUNDTRUTH or unpack bigann_gnd.tar.gz.",
            candidates
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ))
}

fn read_next_bvec_as_f32<R: Read>(r: &mut R) -> io::Result<Arc<[f32]>> {
    let mut hdr = [0u8; 4];
    r.read_exact(&mut hdr)?;
    let d = i32::from_le_bytes(hdr) as usize;
    if d != DIMENSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("bvec dimension {d}, expected {DIMENSION}"),
        ));
    }
    let mut buf = vec![0u8; d];
    r.read_exact(&mut buf)?;
    let floats: Vec<f32> = buf.into_iter().map(|b| b as f32).collect();
    Ok(Arc::from(floats.into_boxed_slice()))
}

/// One row of a `.ivecs` file: leading int32 dimension `k`, then `k` int32 ids.
fn read_next_ivecs_row<R: Read>(r: &mut R) -> io::Result<Vec<u32>> {
    let mut kbuf = [0u8; 4];
    r.read_exact(&mut kbuf)?;
    let k = i32::from_le_bytes(kbuf) as usize;
    if k > 1_000_000 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("ivecs row claims k={k}, too large"),
        ));
    }
    let mut out = Vec::with_capacity(k);
    for _ in 0..k {
        let mut b = [0u8; 4];
        r.read_exact(&mut b)?;
        out.push(i32::from_le_bytes(b) as u32);
    }
    Ok(out)
}

pub struct BigAnnSift1b {
    dir: PathBuf,
    data_len: usize,
}

impl BigAnnSift1b {
    pub async fn load() -> io::Result<Self> {
        let dir_str = std::env::var("CHROMA_BIGANN_SIFT1B_DIR").map_err(|_| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "Set CHROMA_BIGANN_SIFT1B_DIR to the directory containing bigann_base.bvecs and bigann_query.bvecs (TEXMEX BigANN / ANN_SIFT1B).",
            )
        })?;
        let dir = PathBuf::from(dir_str);
        let base_path = dir.join(BASE_FILE);
        if !base_path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing {} under {}", BASE_FILE, dir.display()),
            ));
        }
        let query_path = dir.join(QUERY_FILE);
        if !query_path.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("missing {} under {}", QUERY_FILE, dir.display()),
            ));
        }

        let data_len: usize = std::env::var("CHROMA_BIGANN_SIFT1B_NUM_VECTORS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000_000);

        if data_len == 0 || data_len > DATA_LEN_FULL {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "CHROMA_BIGANN_SIFT1B_NUM_VECTORS must be in 1..={DATA_LEN_FULL}, got {data_len}"
                ),
            ));
        }

        let meta = std::fs::metadata(&base_path)?;
        let max_vecs = meta.len() as usize / BYTES_PER_BASE_VEC;
        if data_len > max_vecs {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "CHROMA_BIGANN_SIFT1B_NUM_VECTORS={data_len} but {} only has ~{} vectors ({:.2} GiB)",
                    base_path.display(),
                    max_vecs,
                    meta.len() as f64 / (1024.0 * 1024.0 * 1024.0)
                ),
            ));
        }

        // Sanity: first vector dimension.
        let mut f = File::open(&base_path)?;
        read_next_bvec_as_f32(&mut f)?;
        drop(f);

        let _ = resolve_ground_truth_path(&dir, data_len)?;

        println!(
            "  BigANN SIFT1B: dir={} | num_vectors={} | dim={}",
            dir.display(),
            data_len,
            DIMENSION
        );
        Ok(Self { dir, data_len })
    }
}

impl Dataset for BigAnnSift1b {
    fn name(&self) -> &str {
        "bigann-sift1b"
    }

    fn dimension(&self) -> usize {
        DIMENSION
    }

    fn data_len(&self) -> usize {
        self.data_len
    }

    fn k(&self) -> usize {
        GT_K
    }

    fn load_range(&self, offset: usize, limit: usize) -> io::Result<Vec<(u32, Arc<[f32]>)>> {
        let end = (offset + limit).min(self.data_len);
        if offset >= end {
            return Ok(Vec::new());
        }

        let mut file = File::open(self.dir.join(BASE_FILE))?;
        file.seek(SeekFrom::Start((offset * BYTES_PER_BASE_VEC) as u64))?;

        let n = end - offset;
        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let emb = read_next_bvec_as_f32(&mut file)?;
            out.push(((offset + i) as u32, emb));
        }
        Ok(out)
    }

    fn queries(&self, distance_function: DistanceFunction) -> io::Result<Vec<Query>> {
        if distance_function != DistanceFunction::Euclidean {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "BigANN SIFT1B ground truth is L2 (Euclidean); use --metric l2",
            ));
        }

        let gt_path = resolve_ground_truth_path(&self.dir, self.data_len)?;
        let mut qf = File::open(self.dir.join(QUERY_FILE))?;
        let mut gf = File::open(&gt_path)?;

        let mut query_vectors: Vec<Arc<[f32]>> = Vec::new();
        loop {
            match read_next_bvec_as_f32(&mut qf) {
                Ok(v) => query_vectors.push(v),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        let mut neighbors_per_query: Vec<Vec<u32>> = Vec::new();
        loop {
            match read_next_ivecs_row(&mut gf) {
                Ok(v) => neighbors_per_query.push(v),
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }

        if query_vectors.len() != neighbors_per_query.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "query / ground-truth row count mismatch: {} queries vs {} gt rows (files {} vs {})",
                    query_vectors.len(),
                    neighbors_per_query.len(),
                    QUERY_FILE,
                    gt_path.display()
                ),
            ));
        }

        let max_id = self.data_len as u64;
        let queries: Vec<Query> = query_vectors
            .into_iter()
            .zip(neighbors_per_query)
            .map(|(vector, neighbors)| Query {
                vector: vector.to_vec(),
                neighbors,
                max_vector_id: max_id,
            })
            .collect();

        println!(
            "  BigANN SIFT1B queries: {} with GT from {}",
            queries.len(),
            gt_path.display()
        );
        Ok(queries)
    }
}
