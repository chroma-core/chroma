use std::collections::HashSet;

use rust_stemmers::{Algorithm, Stemmer};

use crate::embed::Tokenizer;

/// Default English stopwords matching fastembed.
///
/// This list is derived from NLTK's English stopwords, which is what
/// fastembed uses internally. Total: 179 stopwords.
const DEFAULT_ENGLISH_STOPWORDS: &[&str] = &[
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "ain",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "aren",
    "aren't",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can",
    "couldn",
    "couldn't",
    "d",
    "did",
    "didn",
    "didn't",
    "do",
    "does",
    "doesn",
    "doesn't",
    "doing",
    "don",
    "don't",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "hadn",
    "hadn't",
    "has",
    "hasn",
    "hasn't",
    "have",
    "haven",
    "haven't",
    "having",
    "he",
    "her",
    "here",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "i",
    "if",
    "in",
    "into",
    "is",
    "isn",
    "isn't",
    "it",
    "it's",
    "its",
    "itself",
    "just",
    "ll",
    "m",
    "ma",
    "me",
    "mightn",
    "mightn't",
    "more",
    "most",
    "mustn",
    "mustn't",
    "my",
    "myself",
    "needn",
    "needn't",
    "no",
    "nor",
    "not",
    "now",
    "o",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "re",
    "s",
    "same",
    "shan",
    "shan't",
    "she",
    "she's",
    "should",
    "should've",
    "shouldn",
    "shouldn't",
    "so",
    "some",
    "such",
    "t",
    "than",
    "that",
    "that'll",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "these",
    "they",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "ve",
    "very",
    "was",
    "wasn",
    "wasn't",
    "we",
    "were",
    "weren",
    "weren't",
    "what",
    "when",
    "where",
    "which",
    "while",
    "who",
    "whom",
    "why",
    "will",
    "with",
    "won",
    "won't",
    "wouldn",
    "wouldn't",
    "y",
    "you",
    "you'd",
    "you'll",
    "you're",
    "you've",
    "your",
    "yours",
    "yourself",
    "yourselves",
];

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

    /// Get default English stopwords as a HashSet.
    fn default_english_stopwords() -> HashSet<String> {
        DEFAULT_ENGLISH_STOPWORDS
            .iter()
            .map(|s| s.to_string())
            .collect()
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
