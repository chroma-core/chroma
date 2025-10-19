use reqwest::RequestBuilder;

use super::EmbeddingFunction;

/////////////////////////////////////// OllamaEmbeddingError ///////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum OllamaEmbeddingError {
    #[error("request failed: {0}")]
    Reqwest(#[from] reqwest::Error),
}

////////////////////////////////////// OllamaEmbeddingFunction /////////////////////////////////////

pub struct OllamaEmbeddingFunction {
    client: reqwest::Client,
    host: String,
    model: String,
}

impl OllamaEmbeddingFunction {
    pub async fn new(
        host: impl Into<String>,
        model: impl Into<String>,
    ) -> Result<Self, OllamaEmbeddingError> {
        let client = reqwest::Client::new();
        let host = host.into();
        let model = model.into();
        let this = Self {
            client,
            host,
            model,
        };
        this.heartbeat().await?;
        Ok(this)
    }

    pub async fn heartbeat(&self) -> Result<(), OllamaEmbeddingError> {
        self.embed(&["heartbeat"]).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl EmbeddingFunction for OllamaEmbeddingFunction {
    type Error = OllamaEmbeddingError;

    async fn embed(&self, batches: &[&str]) -> Result<Vec<Vec<f32>>, Self::Error> {
        let model = &self.model;
        let input = batches;
        let req = EmbedRequest { model, input };
        let resp = req
            .make_request(&self)
            .send()
            .await?
            .error_for_status()?
            .json::<EmbedResponse>()
            .await?;
        Ok(resp.embeddings)
    }
}

/////////////////////////////////////////// EmbedRequest ///////////////////////////////////////////

/// A request to embed multiple input documents.
#[derive(Clone, Debug, serde::Serialize)]
pub struct EmbedRequest<'a> {
    /// The name of the model to use for embedding.
    pub model: &'a str,
    /// The input texts to embed.
    pub input: &'a [&'a str],
}

impl EmbedRequest<'_> {
    /// Create a new RequestBuilder for this embed request.
    pub fn make_request(&self, ef: &OllamaEmbeddingFunction) -> RequestBuilder {
        ef.client.post(format!("{}/api/embed", ef.host)).json(self)
    }
}

/////////////////////////////////////////// EmbedResponse //////////////////////////////////////////

/// A response to an embed response.
#[derive(Clone, Debug, serde::Deserialize)]
pub struct EmbedResponse {
    /// The name of the model used to generate the response.
    pub model: String,
    /// The embeddings of the input, in the same order.
    pub embeddings: Vec<Vec<f32>>,
    /// The duration of the response.
    pub total_duration: Option<f64>,
    /// The duration of loading the model.
    pub load_duration: Option<f64>,
    /// The number of tokens counted in the prompt.
    pub prompt_eval_count: Option<f64>,
}
