use benchmark_datasets::{datasets::scidocs::SciDocsDataset, traits::TestDataset};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let scidocs = SciDocsDataset::init().await.unwrap();
    println!("SciDocs dataset initialized");

    let mut documents_stream = scidocs.create_documents_stream().await.unwrap();

    while let Some(document) = documents_stream.next().await {
        println!("{:?}", document);
    }
}
