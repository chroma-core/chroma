use benchmark_datasets::{
    datasets::{scidocs::SciDocsDataset, wikipedia::WikipediaDataset},
    types::BenchmarkDataset,
};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    // let scidocs = SciDocsDataset::init().await.unwrap();
    // println!("SciDocs dataset initialized");

    // let mut documents_stream = scidocs.create_documents_stream().await.unwrap();

    // while let Some(document) = documents_stream.next().await {
    //     println!("{:?}", document);
    // }

    let wikipedia = WikipediaDataset::init().await.unwrap();
    println!("Wikipedia dataset initialized");

    let mut documents_stream = wikipedia.create_documents_stream().await.unwrap();

    while let Some(document) = documents_stream.next().await {
        println!("{:?}", document);
    }
}
