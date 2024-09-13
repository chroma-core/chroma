use async_tempfile::TempFile;
use std::{collections::HashMap, future::Future, path::PathBuf};
use tokio::io::AsyncWrite;
use tokio_stream::Stream;

#[derive(Debug, Clone)]
pub struct TestDatasetDocument {
    pub content: String,
    pub metadata: HashMap<String, String>,
}

pub trait TestDataset
where
    Self: Sized,
{
    fn init() -> impl Future<Output = Result<Self, std::io::Error>> + Send;
    fn create_documents_stream(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = TestDatasetDocument>, std::io::Error>> + Send;
}

async fn get_dataset_cache_path(dataset_name: &str) -> Result<PathBuf, std::io::Error> {
    let base_dir = dirs::cache_dir().unwrap();
    let dataset_dir = base_dir.join("chroma-test-datasets").join(dataset_name);

    tokio::fs::create_dir_all(&dataset_dir).await?;

    Ok(dataset_dir)
}

pub(crate) async fn get_or_populate_cached_dataset<F, Fut>(
    dataset_name: &str,
    file_name: &str,
    populate: F,
) -> Result<PathBuf, std::io::Error>
where
    F: FnOnce(Box<dyn AsyncWrite + Unpin + Send>) -> Fut,
    Fut: Future<Output = std::io::Result<()>>,
{
    let dataset_dir = get_dataset_cache_path(dataset_name).await?;
    let file_path = dataset_dir.join(file_name);

    if !file_path.exists() {
        // We assume that dataset creation was successful if the file exists, so we use a temporary file to avoid scenarios where the file is partially written and then the callback fails.
        let temp = TempFile::new()
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        populate(Box::new(temp.try_clone().await.unwrap())).await?;
        tokio::fs::rename(temp.file_path(), &file_path).await?;
    }

    Ok(file_path)
}
