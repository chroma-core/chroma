pub mod client;
mod collection;
pub mod embed;
pub mod types;

pub use client::ChromaHttpClient;
pub use client::ChromaHttpClientOptions;
pub use collection::ChromaCollection;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ChromaAuthMethod, ChromaHttpClientOptions};
    use futures_util::FutureExt;
    use std::sync::LazyLock;

    static CHROMA_CLIENT_OPTIONS: LazyLock<ChromaHttpClientOptions> = LazyLock::new(|| {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(err) => {
                if err.not_found() {
                    tracing::warn!("No .env file found");
                } else {
                    panic!("Error loading .env file: {}", err);
                }
            }
        };

        ChromaHttpClientOptions {
            endpoint: std::env::var("CHROMA_ENDPOINT")
                .unwrap_or_else(|_| "https://api.trychroma.com".to_string())
                .parse()
                .unwrap(),
            auth_method: ChromaAuthMethod::cloud_api_key(
                &std::env::var("CHROMA_CLOUD_API_KEY").unwrap(),
            )
            .unwrap(),
            ..Default::default()
        }
    });

    pub async fn with_client<F, Fut>(callback: F)
    where
        F: FnOnce(ChromaHttpClient) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let client = ChromaHttpClient::new(CHROMA_CLIENT_OPTIONS.clone());

        // Create isolated database for test
        let database_name = format!("test_db_{}", uuid::Uuid::new_v4());
        client.create_database(database_name.clone()).await.unwrap();
        client.set_database_name(database_name.clone());

        let result = std::panic::AssertUnwindSafe(callback(client.clone()))
            .catch_unwind()
            .await;

        // Delete test database
        if let Err(err) = client.delete_database(database_name.clone()).await {
            tracing::error!("Failed to delete test database {}: {}", database_name, err);
        }

        result.unwrap();
    }
}
