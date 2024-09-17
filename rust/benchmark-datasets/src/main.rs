use benchmark_datasets::{
    datasets::{ms_marco_queries, scidocs::SciDocsDataset},
    types::{BenchmarkDataset, QueryDataset},
};
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    #[arg(short, long)]
    update_queries: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    if !args.update_queries {
        return;
    }

    let scidocs = SciDocsDataset::init().await.unwrap();
    println!("SciDocs dataset initialized");

    // let mut documents_stream = scidocs.create_documents_stream().await.unwrap();

    // let wikipedia = WikipediaDataset::init().await.unwrap();
    // println!("Wikipedia dataset initialized");

    // let mut documents_stream = wikipedia.create_documents_stream().await.unwrap();

    let ms_marco_queries = ms_marco_queries::MicrosoftMarcoQueriesDataset::init()
        .await
        .unwrap();

    let subset = ms_marco_queries
        .get_or_create_frozen_subset(&scidocs, 2, 10)
        .await
        .unwrap();

    println!("Frozen query set created: {:?}", subset);
}
