use std::{collections::HashMap, future::Future, path::PathBuf};
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

pub(crate) async fn get_dataset_cache_path(dataset_name: &str) -> Result<PathBuf, std::io::Error> {
    let base_dir = dirs::cache_dir().unwrap();
    let dataset_dir = base_dir.join("chroma-test-datasets").join(dataset_name);

    tokio::fs::create_dir_all(&dataset_dir).await?;

    Ok(dataset_dir)
}
