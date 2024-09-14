use anyhow::Result;
use std::{collections::HashMap, future::Future};
use tokio_stream::Stream;

#[derive(Debug, Clone)]
pub struct BenchmarkDatasetDocument {
    pub content: String,
    pub metadata: HashMap<String, String>,
}

pub trait BenchmarkDataset
where
    Self: Sized,
{
    fn init() -> impl Future<Output = Result<Self>> + Send;
    fn create_documents_stream(
        &self,
    ) -> impl Future<Output = Result<impl Stream<Item = Result<BenchmarkDatasetDocument>>>> + Send;
}
