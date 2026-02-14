use std::collections::HashSet;
use std::sync::LazyLock;

use rust_stemmers::{Algorithm, Stemmer};

use crate::embed::Tokenizer;

/// Default English stopwords array for BM25 tokenization.
///
/// This list is derived from NLTK's English stopwords. Total: 179 stopwords.
const DEFAULT_ENGLISH_STOPWORDS_ARRAY: &[&str] = &[
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

/// Default English stopwords as a HashSet, lazily initialized.
static DEFAULT_ENGLISH_STOPWORDS: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| DEFAULT_ENGLISH_STOPWORDS_ARRAY.iter().copied().collect());

/// Standard BM25 tokenizer with stemming and stopword filtering.
///
/// Processing pipeline:
/// 1. Remove non-alphanumeric characters (replace with spaces)
/// 2. Convert to lowercase and split on whitespace
/// 3. Filter out stopwords
/// 4. Filter tokens longer than max length
/// 5. Apply Snowball stemming
///
/// All fields are public for direct construction and customization.
// NOTE(sicheng): Add config to support more languages
pub struct Bm25Tokenizer {
    /// Snowball stemmer for reducing words to their root form.
    pub stemmer: Stemmer,
    /// Set of stopwords to filter out during tokenization.
    pub stopwords: HashSet<&'static str>,
    /// Maximum token length; longer tokens are discarded.
    pub token_max_length: usize,
}

impl Default for Bm25Tokenizer {
    fn default() -> Self {
        Self {
            stemmer: Stemmer::create(Algorithm::English),
            stopwords: DEFAULT_ENGLISH_STOPWORDS.clone(),
            token_max_length: 40,
        }
    }
}

impl Bm25Tokenizer {
    /// Remove non-alphanumeric characters, replacing with spaces.
    ///
    /// Matches Python's: re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
    fn remove_non_alphanumeric(&self, text: &str) -> String {
        text.chars()
            .map(|c| {
                if c.is_alphanumeric() || c.is_whitespace() || c == '_' {
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

impl Tokenizer for Bm25Tokenizer {
    fn tokenize(&self, text: &str) -> Vec<String> {
        let cleaned = self.remove_non_alphanumeric(text);

        let tokens = self.simple_tokenize(&cleaned);

        let mut result = Vec::new();
        for token in tokens {
            if self.stopwords.contains(token.as_str()) {
                continue;
            }

            if token.len() > self.token_max_length {
                continue;
            }

            let stemmed = self.stemmer.stem(&token).to_string();

            if !stemmed.is_empty() {
                result.push(stemmed);
            }
        }

        result
    }
}
