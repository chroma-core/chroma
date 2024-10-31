use anyhow::Result;
use async_tempfile::TempFile;
use std::{future::Future, path::PathBuf};
use tokio::io::AsyncWrite;

pub(crate) fn get_dir_for_persistent_dataset_files() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("dataset_files")
}

async fn get_dataset_cache_path(
    dataset_name: &str,
    cache_dir: Option<PathBuf>,
) -> Result<PathBuf, std::io::Error> {
    let base_dir = cache_dir.unwrap_or(
        dirs::cache_dir()
            .expect("Failed to get cache directory")
            .join("chroma-test-datasets"),
    );
    let dataset_dir = base_dir.join(dataset_name);

    tokio::fs::create_dir_all(&dataset_dir).await?;

    Ok(dataset_dir)
}

/// Calls the populate callback to create a cached dataset file if it doesn't exist, and returns the path to the cached file.
pub(crate) async fn get_or_populate_cached_dataset_file<F, Fut>(
    dataset_name: &str,
    file_name: &str,
    cache_dir: Option<PathBuf>,
    populate: F,
) -> Result<PathBuf>
where
    F: FnOnce(Box<dyn AsyncWrite + Unpin + Send>) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let dataset_dir = get_dataset_cache_path(dataset_name, cache_dir).await?;
    let file_path = dataset_dir.join(file_name);

    if !file_path.exists() {
        // We assume that dataset creation was successful if the file exists, so we use a temporary file to avoid scenarios where the file is partially written and then the callback fails.
        let temp = TempFile::new().await?;
        populate(Box::new(
            temp.try_clone().await.expect("Failed to clone file handle"),
        ))
        .await?;
        tokio::fs::rename(temp.file_path(), &file_path).await?;
    }

    Ok(file_path)
}
