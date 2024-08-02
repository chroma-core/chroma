use arrow::array::Int32Array;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_index::fulltext::{tokenizer::TantivyChromaTokenizer, types::FullTextIndexWriter};
use serde_json;
use std::io::Read;
use tantivy::tokenizer::NgramTokenizer;

// Datasets are expected to be in jsonl format, each dataset has an adapter that can be used to load the dataset
// we generally use hugingface datasets for this purpose, but in the absence of a good rust library
// we just manually download the dataset
// We may want to move this out to a separate level in the future when its used in more places
enum BenchmarkDataset {
    SCIDOCS,
}

impl BenchmarkDataset {
    fn get_url(&self) -> &str {
        match self {
            BenchmarkDataset::SCIDOCS => {
                "https://huggingface.co/datasets/BeIR/scidocs/resolve/main/corpus.jsonl.gz"
            }
        }
    }
}

// Simple FTS focused dataset
struct Dataset {
    documents: Vec<String>,
}

fn download_dataset(dataset: BenchmarkDataset) -> Result<Dataset, Box<dyn std::error::Error>> {
    let url = dataset.get_url();
    // TODO: cache the dataset
    let download = reqwest::blocking::get(url)?;
    let as_bytes = download.bytes()?;
    let mut decoder = flate2::read::GzDecoder::new(as_bytes.as_ref());
    let mut as_string = String::new();
    decoder.read_to_string(&mut as_string)?;

    let deser = serde_json::Deserializer::from_str(&as_string).into_iter::<serde_json::Value>();
    let mut ret_docs = Vec::new();
    for row in deser {
        let row = row?;
        let text = row["text"].as_str().unwrap();
        ret_docs.push(text.to_string());
    }

    Ok(Dataset {
        documents: ret_docs,
    })
}

fn get_fts_index_writer() -> FullTextIndexWriter<'static> {
    // let provider = BlockfileProvider::new_memory();
    let tempfile = tempfile::tempdir().unwrap();
    let storage = chroma_storage::Storage::Local(chroma_storage::local::LocalStorage::new(
        tempfile.path().to_path_buf().to_str().unwrap(),
    ));
    let max_block_size_bytes = 8 * 1024 * 1024; // 8MB
    let block_cache =
        chroma_cache::cache::Cache::Unbounded(chroma_cache::cache::UnboundedCache::new());
    let sparse_index_cache =
        chroma_cache::cache::Cache::Unbounded(chroma_cache::cache::UnboundedCache::new());
    let provider = BlockfileProvider::new_arrow(
        storage,
        max_block_size_bytes,
        block_cache,
        sparse_index_cache,
    );
    let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
    let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
    let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
        NgramTokenizer::new(3, 3, false).unwrap(),
    )));
    return FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
}

#[tokio::main]
async fn main() {
    let dataset = download_dataset(BenchmarkDataset::SCIDOCS).unwrap();
    println!(
        "Downloaded dataset with {} documents",
        dataset.documents.len()
    );
    let fts_index_writer = get_fts_index_writer();

    // for (idx, doc) in dataset.documents.iter().enumerate() {
    //     let start = std::time::Instant::now();
    //     fts_index_writer
    //         .add_document(doc, idx as i32)
    //         .await
    //         .unwrap();
    //     println!("Added document {} in {:?}", idx, start.elapsed());
    // }
    // add the dataset 4 times, offsetting the doc ids

    for i in 0..4 {
        for (idx, doc) in dataset.documents.iter().enumerate() {
            let start = std::time::Instant::now();
            fts_index_writer
                .add_document(doc, idx as i32 + i * dataset.documents.len() as i32)
                .await
                .unwrap();
            println!("Added document {} in {:?}", idx, start.elapsed());
        }
    }
}
