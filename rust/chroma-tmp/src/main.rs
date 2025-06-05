use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use guacamole::combinators::*;
use guacamole::Guacamole;

use tokio::sync::Semaphore;

use chromadb::client::{ChromaAuthMethod, ChromaClientOptions, ChromaTokenHeader};
use chromadb::collection::{CollectionEntries, GetOptions};
use chromadb::ChromaClient;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Connection {
    pub url: String,
    pub api_key: Option<String>,
    pub database: String,
}

pub async fn client(connection: Connection) -> ChromaClient {
    let auth = if let Some(api_key) = connection.api_key.clone() {
        ChromaAuthMethod::TokenAuth {
            token: api_key,
            header: ChromaTokenHeader::XChromaToken,
        }
    } else {
        ChromaAuthMethod::None
    };
    ChromaClient::new(ChromaClientOptions {
        url: Some(connection.url.clone()),
        auth,
        database: connection.database.clone(),
    })
    .await
    .unwrap()
}

#[tokio::main]
async fn main() {
    let success = Arc::new(AtomicU64::new(0));
    let failure = Arc::new(AtomicU64::new(0));
    let client = client(Connection {
        url: "https://api.devchroma.com:8000".to_string(),
        api_key: std::env::var("CHROMA_API_KEY").ok(),
        database: std::env::var("CHROMA_DATABASE").expect("set CHROMA_DATABASE"),
    })
    .await;
    let mut collections = vec![];
    let mut collection_locks: HashMap<String, Arc<tokio::sync::Mutex<()>>> = HashMap::default();
    const COLLECTIONS: usize = 1000;
    for i in 1..=COLLECTIONS {
        let collection = client
            .get_or_create_collection(&format!("many-collections-{i}"), None)
            .await
            .unwrap();
        println!("loaded {collection:?}");
        collection_locks.insert(
            collection.id().to_string(),
            Arc::new(tokio::sync::Mutex::new(())),
        );
        collections.push(collection);
    }
    const MAGIC_CONSTANT: usize = 2000;
    let (tx, mut rx) = tokio::sync::mpsc::channel(MAGIC_CONSTANT);
    tokio::task::spawn(async move {
        while let Some(x) = rx.recv().await {
            let _ = x.await;
        }
    });
    let mut guac = Guacamole::new(0);
    let semaphore = Arc::new(Semaphore::new(MAGIC_CONSTANT));
    let start = std::time::Instant::now();
    let mut i = 0;
    while start.elapsed() < std::time::Duration::from_secs(600) {
        let collection = collections[i % collections.len()].clone();
        i += 1;
        let mut ids = vec![];
        let mut documents = vec![];
        let mut embeddings = vec![];
        for i in 0..100 {
            ids.push(format!("key-{i}"));
            documents.push("dummy");
            embeddings.push(vec![
                any::<f32>(&mut guac),
                any::<f32>(&mut guac),
                any::<f32>(&mut guac),
            ]);
        }
        let per_collection = collection_locks
            .get(collections[i % collections.len()].id())
            .map(Arc::clone)
            .unwrap();
        let semaphore = Arc::clone(&semaphore);
        let success = Arc::clone(&success);
        let failure = Arc::clone(&failure);
        let handle = tokio::task::spawn(async move {
            let _guard = per_collection.lock().await;
            let _permit = semaphore.acquire().await;
            let mut results = vec![];
            for i in 0..4 {
                let collection = collection.clone();
                let success = Arc::clone(&success);
                let failure = Arc::clone(&failure);
                let jh = tokio::task::spawn(async move {
                    let is_err = collection
                        .get(GetOptions {
                            ids: vec![format!("key-{i}")],
                            where_metadata: None,
                            limit: Some(10),
                            offset: None,
                            where_document: None,
                            include: None,
                        })
                        .await
                        .is_err();
                    if is_err {
                        failure.fetch_add(1, Ordering::Relaxed);
                    } else {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                });
                results.push(jh);
            }
            futures::future::join_all(results).await;
            let ids = ids.iter().map(String::as_str).collect::<Vec<_>>();
            let entries = CollectionEntries {
                ids,
                embeddings: Some(embeddings),
                metadatas: None,
                documents: Some(documents),
            };
            let _results = collection.upsert(entries, None).await;
        });
        let _ = tx.send(handle).await;
    }
    drop(tx);
}
