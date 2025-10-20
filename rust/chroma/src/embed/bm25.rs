use std::io::{self, Cursor};

use chroma_types::SparseVector;
use murmur3::murmur3_32;
use parking_lot::Mutex;
use tantivy::tokenizer::{
    AlphaNumOnlyFilter, Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer,
    StopWordFilter, TextAnalyzer,
};
use thiserror::Error;

use crate::embed::EmbeddingFunction;

#[derive(Debug, Error)]
pub enum BM25SparseEmbeddingError {
    #[error("Unable to hash token: {0}")]
    Murmur3(#[from] io::Error),
}

pub struct BM25SparseEmbeddingFunction {
    pub analyzer: Mutex<TextAnalyzer>,
    pub avg_len: f32,
    pub b: f32,
    pub k: f32,
}

impl BM25SparseEmbeddingFunction {
    pub fn default_en_analyzer() -> TextAnalyzer {
        TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(AlphaNumOnlyFilter)
            .filter(LowerCaser)
            .filter(
                StopWordFilter::new(Language::English)
                    .expect("English stop word filter should be present"),
            ) // SAFETY(sicheng): Tantivy source code suggests this should not panic. Go to definition for details.
            .filter(RemoveLongFilter::limit(40))
            .filter(Stemmer::new(Language::English))
            .build()
    }

    pub fn encode(&self, text: &str) -> Result<SparseVector, BM25SparseEmbeddingError> {
        let text_len = text.len();
        let mut tokens = Vec::with_capacity(text_len);

        {
            let mut analyzer = self.analyzer.lock();
            let mut token_stream = analyzer.token_stream(text);
            while token_stream.advance() {
                let token = token_stream.token();
                let id = murmur3_32(&mut Cursor::new(&token.text), 0)?;
                tokens.push(id);
            }
        };

        tokens.sort_unstable();

        Ok(SparseVector::from_pairs(
            tokens.chunk_by(|l, r| l == r).map(|chunk| {
                let id = chunk.first().cloned().unwrap_or_default();
                let tf = chunk.len() as f32;
                (
                    id,
                    tf * (self.k + 1.0)
                        / (tf + self.k * (1.0 - self.b + self.b * text_len as f32 / self.avg_len)),
                )
            }),
        ))
    }
}

impl Default for BM25SparseEmbeddingFunction {
    fn default() -> Self {
        Self {
            analyzer: Mutex::new(Self::default_en_analyzer()),
            avg_len: 256.0,
            b: 0.75,
            k: 1.2,
        }
    }
}

#[async_trait::async_trait]
impl EmbeddingFunction for BM25SparseEmbeddingFunction {
    type Embedding = SparseVector;
    type Error = BM25SparseEmbeddingError;

    async fn embed_strs(&self, batches: &[&str]) -> Result<Vec<Self::Embedding>, Self::Error> {
        batches.iter().map(|text| self.encode(text)).collect()
    }
}
