//! Collection-scoped conditional transaction example.
//!
//! Run against a Chroma server that supports conditional transactions:
//!
//! ```bash
//! cargo run -p chroma --example conditional_transactions
//! ```
//!
//! Optional environment:
//!
//! ```text
//! CHROMA_ENDPOINT=http://localhost:8000
//! CHROMA_TENANT=default_tenant
//! CHROMA_DATABASE=default_database
//! CHROMA_API_KEY=...
//! ```

use std::error::Error;

use chroma::{
    client::ChromaHttpClientError,
    types::{IncludeList, Metadata, UpdateMetadata},
    ChromaHttpClient,
};

const COLLECTION_NAME: &str = "transactional_chroma_rust_example";
const RECORD_ID: &str = "txn-doc";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let client = ChromaHttpClient::from_env()?;

    let _ = client.delete_collection(COLLECTION_NAME).await;
    let collection = client
        .get_or_create_collection(COLLECTION_NAME, None, None)
        .await?;

    let outcome = collection
        .conditional()
        .run(
            async |txn| {
                let existing = txn
                    .get(
                        Some(vec![RECORD_ID.to_string()]),
                        None,
                        Some(1),
                        Some(0),
                        Some(IncludeList::default_get()),
                    )
                    .await?;

                if existing.ids.is_empty() {
                    txn.add(
                        vec![RECORD_ID.to_string()],
                        vec![vec![1.0, 0.0, 0.0]],
                        None,
                        None,
                        Some(vec![Some(metadata("created-by-run", 1))]),
                    )
                    .await?;
                    return Ok::<_, ChromaHttpClientError>("created");
                }

                txn.update(
                    vec![RECORD_ID.to_string()],
                    None,
                    None,
                    None,
                    Some(vec![Some(update_metadata("updated-by-run", 1))]),
                )
                .await?;
                Ok::<_, ChromaHttpClientError>("updated")
            },
            3,
        )
        .await?;
    println!("run() transaction {outcome} {RECORD_ID:?}");

    let mut txn = collection.conditional();
    let before = txn
        .get(
            Some(vec![RECORD_ID.to_string()]),
            None,
            Some(1),
            Some(0),
            Some(IncludeList::default_get()),
        )
        .await?;
    if before.ids.is_empty() {
        return Err(format!("{RECORD_ID:?} disappeared before manual commit").into());
    }

    txn.update(
        vec![RECORD_ID.to_string()],
        None,
        None,
        None,
        Some(vec![Some(update_metadata("updated-by-manual-commit", 2))]),
    )
    .await?;
    let committed = txn.commit().await?;
    println!("manual commit wrote {} record(s)", committed.record_count);

    let after = collection
        .get(
            Some(vec![RECORD_ID.to_string()]),
            None,
            Some(1),
            Some(0),
            Some(IncludeList::default_get()),
        )
        .await?;
    println!("final metadata: {:?}", after.metadatas);

    Ok(())
}

fn metadata(status: &str, version: i64) -> Metadata {
    let mut metadata = Metadata::new();
    metadata.insert("status".to_string(), status.into());
    metadata.insert("version".to_string(), version.into());
    metadata
}

fn update_metadata(status: &str, version: i64) -> UpdateMetadata {
    let mut metadata = UpdateMetadata::new();
    metadata.insert("status".to_string(), status.into());
    metadata.insert("version".to_string(), version.into());
    metadata
}
