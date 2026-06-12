//! SPLADE sparse embedding for wiki pages.
//!
//! A SPLADE sparse vector is computed client-side for every chunk and stored
//! under the `sparse_embedding` metadata key; the dense Qwen vector is produced
//! by the collection's schema-bound embedding function on `add`. Build a Chroma
//! Cloud SPLADE embedding function scoped to the caller's `x-chroma-token` (so
//! embed usage bills to the user) and embed documents in batches of
//! [`EMBED_BATCH_SIZE`] — the limit the Chroma Cloud embedding service accepts
//! per request — concatenating the per-batch results in input order.

use chroma::embed::chroma_cloud::{ChromaCloudEmbeddingError, ChromaCloudSpladeEmbeddingFunction};
use chroma::embed::EmbeddingFunction;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::SparseVector;

/// Metadata key under which the SPLADE sparse vector is stored on each chunk;
/// it must match the key the wiki search path queries.
pub const SPARSE_KEY: &str = "sparse_embedding";

/// Maximum documents per Chroma Cloud embedding request. The service rejects
/// larger calls with a 413, so documents are sliced into batches of this size
/// and the resulting vectors concatenated.
pub const EMBED_BATCH_SIZE: usize = 100;

/// Errors raised while computing sparse embeddings.
#[derive(Debug, thiserror::Error)]
pub enum WikiEmbedError {
    /// The downstream Chroma Cloud embedding service returned an error.
    #[error("sparse embedding failed: {0}")]
    Embedding(#[from] ChromaCloudEmbeddingError),
}

impl ChromaError for WikiEmbedError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

/// Builds per-request SPLADE sparse embedders scoped to the caller's token.
#[derive(Clone, Debug, Default)]
pub struct WikiEmbedder {
    embed_url: Option<String>,
}

impl WikiEmbedder {
    /// Creates an embedder.
    ///
    /// `embed_url` overrides the Chroma Cloud embedding endpoint; `None` uses
    /// the SDK default (`CHROMA_EMBED_URL` env var, else
    /// `https://embed.trychroma.com`).
    pub fn new(embed_url: Option<String>) -> Self {
        Self { embed_url }
    }

    /// Computes a SPLADE sparse vector for each document, in input order.
    ///
    /// The embedding function is scoped to `token` so embed usage bills to the
    /// caller. Documents are embedded in batches of [`EMBED_BATCH_SIZE`] and
    /// the per-batch results concatenated; an empty input issues no request.
    pub async fn embed_sparse(
        &self,
        token: &str,
        documents: &[&str],
    ) -> Result<Vec<SparseVector>, WikiEmbedError> {
        if documents.is_empty() {
            return Ok(Vec::new());
        }
        // Default builder => model `prithivida/Splade_PP_en_v1`, tokens not
        // included.
        let mut builder = ChromaCloudSpladeEmbeddingFunction::builder().api_key(token);
        if let Some(embed_url) = &self.embed_url {
            builder = builder.embed_url(embed_url.clone());
        }
        let embedding_function = builder.build()?;

        let mut embeddings = Vec::with_capacity(documents.len());
        for batch in documents.chunks(EMBED_BATCH_SIZE) {
            embeddings.extend(embedding_function.embed_strs(batch).await?);
        }
        Ok(embeddings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::MockServer;
    use serde_json::json;

    #[tokio::test]
    async fn embed_sparse_forwards_token_and_parses_sorted_vectors() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method("POST")
                    .path("/embed_sparse")
                    .header("x-chroma-token", "user-token")
                    .header("x-chroma-embedding-model", "prithivida/Splade_PP_en_v1");
                then.status(200).json_body(json!({
                    "embeddings": [{ "indices": [3, 1], "values": [0.3, 0.1] }]
                }));
            })
            .await;

        let embedder = WikiEmbedder::new(Some(server.base_url()));
        let embeddings = embedder.embed_sparse("user-token", &["doc"]).await.unwrap();

        assert_eq!(embeddings.len(), 1);
        assert_eq!(embeddings[0].indices, vec![1, 3]);
        assert_eq!(embeddings[0].values, vec![0.1, 0.3]);
        assert_eq!(mock.calls(), 1);
    }

    #[tokio::test]
    async fn embed_sparse_empty_docs_makes_no_request() {
        let server = MockServer::start_async().await;
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path("/embed_sparse");
                then.status(200).json_body(json!({ "embeddings": [] }));
            })
            .await;

        let embedder = WikiEmbedder::new(Some(server.base_url()));
        let embeddings = embedder.embed_sparse("user-token", &[]).await.unwrap();

        assert!(embeddings.is_empty());
        assert_eq!(mock.calls(), 0);
    }

    #[tokio::test]
    async fn embed_sparse_slices_into_batches_of_100() {
        let server = MockServer::start_async().await;
        // Each call returns a single vector regardless of how many texts it
        // received; we assert only the call count, which pins the 100-doc
        // batch boundary (250 docs => ceil(250 / 100) == 3 requests).
        let mock = server
            .mock_async(|when, then| {
                when.method("POST").path("/embed_sparse");
                then.status(200).json_body(json!({
                    "embeddings": [{ "indices": [0], "values": [1.0] }]
                }));
            })
            .await;

        let embedder = WikiEmbedder::new(Some(server.base_url()));
        let docs = vec!["x"; 250];
        embedder.embed_sparse("user-token", &docs).await.unwrap();

        assert_eq!(mock.calls(), 3);
    }
}
