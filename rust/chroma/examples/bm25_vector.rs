use chroma::types::{Schema, Space, SparseVectorIndexConfig, VectorIndexConfig};
use chroma::ChromaHttpClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = ChromaHttpClient::cloud().expect("a cloud client");
    let _collection = client
        .get_or_create_collection(
            "MY_COLLECTION_NAME",
            Some(
                Schema::default()
                    .create_index(
                        Some("bm25_vector"),
                        SparseVectorIndexConfig {
                            embedding_function: None,
                            source_key: None,
                            bm25: Some(true),
                        }
                        .into(),
                    )?
                    .create_index(
                        None,
                        VectorIndexConfig {
                            space: Some(Space::Cosine),
                            embedding_function: None,
                            source_key: None,
                            hnsw: None,
                            spann: None,
                        }
                        .into(),
                    )?,
            ),
            None,
        )
        .await?;
    Ok(())
}
