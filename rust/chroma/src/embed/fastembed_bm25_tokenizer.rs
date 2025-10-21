use std::collections::HashSet;

use rust_stemmers::{Algorithm, Stemmer};

use crate::embed::Tokenizer;

/// Tokenizer that mirrors Python fastembed's BM25 behavior.
///
/// Processing pipeline:
/// 1. Remove non-alphanumeric characters (replace with spaces)
/// 2. Convert to lowercase and split on whitespace
/// 3. Filter out stopwords
/// 4. Filter tokens longer than max length
/// 5. Apply Snowball stemming
///
/// This matches the behavior of fastembed's Bm25 class.
pub struct FastembedBM25Tokenizer {
    stemmer: Stemmer,
    stopwords: HashSet<String>,
    token_max_length: usize,
}

impl FastembedBM25Tokenizer {
    /// Create a new tokenizer for English with default settings.
    ///
    /// Default stopwords list matches fastembed's English stopwords.
    /// Default max token length is 40 characters.
    pub fn new() -> Self {
        Self::with_language(Algorithm::English)
    }

    /// Create a tokenizer for a specific language.
    pub fn with_language(language: Algorithm) -> Self {
        let stemmer = Stemmer::create(language);
        let stopwords = Self::default_english_stopwords();
        
        Self {
            stemmer,
            stopwords,
            token_max_length: 40,
        }
    }

    /// Create a tokenizer with custom stopwords.
    pub fn with_stopwords(language: Algorithm, stopwords: HashSet<String>) -> Self {
        let stemmer = Stemmer::create(language);
        
        Self {
            stemmer,
            stopwords,
            token_max_length: 40,
        }
    }

    /// Set the maximum token length (default: 40).
    pub fn with_max_length(mut self, max_length: usize) -> Self {
        self.token_max_length = max_length;
        self
    }

    /// Default English stopwords matching fastembed.
    ///
    /// This list is derived from NLTK's English stopwords, which is what
    /// fastembed uses internally.
    fn default_english_stopwords() -> HashSet<String> {
        let stopwords = [
            "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your",
            "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her",
            "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs",
            "themselves", "what", "which", "who", "whom", "this", "that", "these", "those",
            "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
            "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if",
            "or", "because", "as", "until", "while", "of", "at", "by", "for", "with",
            "about", "against", "between", "into", "through", "during", "before", "after",
            "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over",
            "under", "again", "further", "then", "once", "here", "there", "when", "where",
            "why", "how", "all", "both", "each", "few", "more", "most", "other", "some",
            "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very",
            "s", "t", "can", "will", "just", "don", "should", "now", "d", "ll", "m", "o",
            "re", "ve", "y", "ain", "aren", "couldn", "didn", "doesn", "hadn", "hasn",
            "haven", "isn", "ma", "mightn", "mustn", "needn", "shan", "shouldn", "wasn",
            "weren", "won", "wouldn",
        ];
        
        stopwords.iter().map(|s| s.to_string()).collect()
    }

    /// Remove non-alphanumeric characters, replacing with spaces.
    ///
    /// Matches Python's: re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    fn remove_non_alphanumeric(&self, text: &str) -> String {
        text.chars()
            .map(|c| {
                if c.is_alphanumeric() || c.is_whitespace() {
                    c
                } else {
                    ' '
                }
            })
            .collect()
    }

    /// Tokenize by lowercase and split on whitespace.
    ///
    /// Matches Python's SimpleTokenizer behavior.
    fn simple_tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .map(|s| s.to_string())
            .collect()
    }
}

impl Default for FastembedBM25Tokenizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Tokenizer for FastembedBM25Tokenizer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        // Step 1: Remove non-alphanumeric characters
        let cleaned = self.remove_non_alphanumeric(text);
        
        // Step 2: Lowercase and split on whitespace
        let tokens = self.simple_tokenize(&cleaned);
        
        // Step 3-5: Filter and stem
        let mut result = Vec::new();
        for token in tokens {
            // Skip stopwords
            if self.stopwords.contains(&token) {
                continue;
            }
            
            // Skip tokens that are too long
            if token.len() > self.token_max_length {
                continue;
            }
            
            // Apply stemming
            let stemmed = self.stemmer.stem(&token).to_string();
            
            // Only include non-empty stemmed tokens
            if !stemmed.is_empty() {
                result.push(stemmed);
            }
        }
        
        result
    }
}
