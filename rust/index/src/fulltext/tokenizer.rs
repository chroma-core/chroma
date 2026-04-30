use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, RemoveLongFilter, SimpleTokenizer, TextAnalyzer, Token,
    TokenFilter, TokenStream, Tokenizer,
};
use thiserror::Error;

const MIN_TOKEN_LENGTH: usize = 2;
const MAX_TOKEN_LENGTH: usize = 128;

/// `TokenFilter` that removes tokens shorter than a given number of bytes.
#[derive(Clone)]
struct RemoveShortFilter {
    min_length: usize,
}

impl TokenFilter for RemoveShortFilter {
    type Tokenizer<T: Tokenizer> = RemoveShortFilterWrapper<T>;

    fn transform<T: Tokenizer>(self, tokenizer: T) -> Self::Tokenizer<T> {
        RemoveShortFilterWrapper {
            min_length: self.min_length,
            inner: tokenizer,
        }
    }
}

#[derive(Clone)]
struct RemoveShortFilterWrapper<T> {
    min_length: usize,
    inner: T,
}

impl<T: Tokenizer> Tokenizer for RemoveShortFilterWrapper<T> {
    type TokenStream<'a> = RemoveShortFilterStream<T::TokenStream<'a>>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> Self::TokenStream<'a> {
        RemoveShortFilterStream {
            min_length: self.min_length,
            inner: self.inner.token_stream(text),
        }
    }
}

struct RemoveShortFilterStream<T> {
    min_length: usize,
    inner: T,
}

impl<T: TokenStream> TokenStream for RemoveShortFilterStream<T> {
    fn advance(&mut self) -> bool {
        while self.inner.advance() {
            if self.inner.token().text.len() >= self.min_length {
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.inner.token()
    }

    fn token_mut(&mut self) -> &mut Token {
        self.inner.token_mut()
    }
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Query \"{query}\" has no token with at least {MIN_TOKEN_LENGTH} characters")]
    NoSelectiveToken { query: String },
}

/// A decomposed full-text query.
///
/// `prefix` is set if the query starts with its first token (the token may be
/// a suffix of a longer word in the document). `suffix` is set if the query
/// ends with its last token (the token may be a prefix of a longer word).
/// `tokens` contains all middle tokens that must match as complete words,
/// plus any boundary tokens that are not prefix/suffix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextQuery {
    /// First token if the query starts with it — may be a suffix of a longer word.
    pub prefix: Option<String>,
    /// Tokens that must match as complete words.
    pub tokens: Vec<String>,
    /// Last token if the query ends with it — may be a prefix of a longer word.
    pub suffix: Option<String>,
}

/// Word-based text analyzer for the full-text index.
///
/// Pipeline: `SimpleTokenizer` (split on non-alphanumeric) → `LowerCaser`
/// → `AsciiFoldingFilter` (Unicode normalization) → `RemoveShortFilter(2)`
/// → `RemoveLongFilter(128)`.
#[derive(Clone)]
pub struct WordAnalyzer {
    inner: TextAnalyzer,
}

impl WordAnalyzer {
    pub fn new() -> Self {
        Self {
            inner: TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(AsciiFoldingFilter)
                .filter(RemoveShortFilter {
                    min_length: MIN_TOKEN_LENGTH,
                })
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LENGTH))
                .build(),
        }
    }

    /// Tokenize text, returning owned token strings.
    pub fn tokenize(&mut self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut stream = self.inner.token_stream(text);
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        tokens
    }

    /// Decompose a query string into a [`FullTextQuery`].
    ///
    /// Returns `Err` if no token survives the analyzer.
    pub fn tokenize_query(&mut self, query: &str) -> Result<FullTextQuery, QueryError> {
        let mut stream = self.inner.token_stream(query);

        let mut prefix = None;
        let mut tokens = Vec::new();
        let mut last_offset_to = 0;

        while let Some(token) = stream.next() {
            if tokens.is_empty() && prefix.is_none() {
                // First token.
                if token.offset_from == 0 {
                    prefix = Some(token.text.clone());
                } else {
                    tokens.push(token.text.clone());
                }
            } else {
                tokens.push(token.text.clone());
            }
            last_offset_to = token.offset_to;
        }

        if prefix.is_none() && tokens.is_empty() {
            return Err(QueryError::NoSelectiveToken {
                query: query.to_string(),
            });
        }

        let suffix = if last_offset_to == query.len() {
            tokens.pop().or(prefix.clone())
        } else {
            None
        };

        Ok(FullTextQuery {
            prefix,
            tokens,
            suffix,
        })
    }
}

impl Default for WordAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_tokenization() {
        let mut a = WordAnalyzer::new();
        assert_eq!(a.tokenize("Hello World"), vec!["hello", "world"]);
    }

    #[test]
    fn test_short_tokens_removed() {
        let mut a = WordAnalyzer::new();
        assert_eq!(
            a.tokenize("a is the big house"),
            vec!["is", "the", "big", "house"]
        );
    }

    #[test]
    fn test_unicode_normalization() {
        let mut a = WordAnalyzer::new();
        assert_eq!(
            a.tokenize("café résumé naïve über"),
            vec!["cafe", "resume", "naive", "uber"]
        );
    }

    #[test]
    fn test_punctuation_splits() {
        let mut a = WordAnalyzer::new();
        assert_eq!(
            a.tokenize("hello, world! foo-bar"),
            vec!["hello", "world", "foo", "bar"]
        );
    }

    #[test]
    fn test_code_tokenization() {
        let mut a = WordAnalyzer::new();
        assert_eq!(
            a.tokenize("item.price = calculate_total(items)"),
            vec!["item", "price", "calculate", "total", "items"]
        );
    }

    #[test]
    fn test_long_tokens_removed() {
        let mut a = WordAnalyzer::new();
        let text = format!("hello {} world", "a".repeat(129));
        assert_eq!(a.tokenize(&text), vec!["hello", "world"]);
    }

    #[test]
    fn test_empty_input() {
        let mut a = WordAnalyzer::new();
        assert!(a.tokenize("").is_empty());
    }

    #[test]
    fn test_only_single_char_tokens() {
        let mut a = WordAnalyzer::new();
        assert!(a.tokenize("a b c").is_empty());
    }

    #[test]
    fn test_mixed_case_and_numbers() {
        let mut a = WordAnalyzer::new();
        assert_eq!(
            a.tokenize("HTTP2 StatusCode 404"),
            vec!["http2", "statuscode", "404"]
        );
    }

    // --- tokenize_query tests ---

    fn query(q: &str) -> FullTextQuery {
        WordAnalyzer::new().tokenize_query(q).unwrap()
    }

    #[test]
    fn test_query_single_word() {
        let q = query("hello");
        assert_eq!(q.prefix.as_deref(), Some("hello"));
        assert!(q.tokens.is_empty());
        assert_eq!(q.suffix.as_deref(), Some("hello"));
    }

    #[test]
    fn test_query_two_words() {
        let q = query("hello world");
        assert_eq!(q.prefix.as_deref(), Some("hello"));
        assert!(q.tokens.is_empty());
        assert_eq!(q.suffix.as_deref(), Some("world"));
    }

    #[test]
    fn test_query_three_words() {
        let q = query("hello beautiful world");
        assert_eq!(q.prefix.as_deref(), Some("hello"));
        assert_eq!(q.tokens, vec!["beautiful"]);
        assert_eq!(q.suffix.as_deref(), Some("world"));
    }

    #[test]
    fn test_query_many_words() {
        let q = query("the quick brown fox");
        assert_eq!(q.prefix.as_deref(), Some("the"));
        assert_eq!(q.tokens, vec!["quick", "brown"]);
        assert_eq!(q.suffix.as_deref(), Some("fox"));
    }

    #[test]
    fn test_query_leading_punctuation() {
        let q = query(". start end");
        assert!(q.prefix.is_none());
        assert_eq!(q.tokens, vec!["start"]);
        assert_eq!(q.suffix.as_deref(), Some("end"));
    }

    #[test]
    fn test_query_trailing_punctuation() {
        let q = query("start end .");
        assert_eq!(q.prefix.as_deref(), Some("start"));
        assert_eq!(q.tokens, vec!["end"]);
        assert!(q.suffix.is_none());
    }

    #[test]
    fn test_query_both_punctuation() {
        let q = query(". hello .");
        assert!(q.prefix.is_none());
        assert_eq!(q.tokens, vec!["hello"]);
        assert!(q.suffix.is_none());
    }

    #[test]
    fn test_query_leading_space() {
        let q = query(" hello world");
        assert!(q.prefix.is_none());
        assert_eq!(q.tokens, vec!["hello"]);
        assert_eq!(q.suffix.as_deref(), Some("world"));
    }

    #[test]
    fn test_query_code() {
        let q = query("calculate_total");
        assert_eq!(q.prefix.as_deref(), Some("calculate"));
        assert!(q.tokens.is_empty());
        assert_eq!(q.suffix.as_deref(), Some("total"));
    }

    #[test]
    fn test_query_unicode() {
        let q = query("café résumé");
        assert_eq!(q.prefix.as_deref(), Some("cafe"));
        assert!(q.tokens.is_empty());
        assert_eq!(q.suffix.as_deref(), Some("resume"));
    }

    #[test]
    fn test_query_short_tokens_filtered() {
        let q = query("a beautiful day");
        assert!(q.prefix.is_none());
        assert_eq!(q.tokens, vec!["beautiful"]);
        assert_eq!(q.suffix.as_deref(), Some("day"));
    }

    #[test]
    fn test_query_reject_empty() {
        assert!(WordAnalyzer::new().tokenize_query("").is_err());
    }

    #[test]
    fn test_query_reject_whitespace() {
        assert!(WordAnalyzer::new().tokenize_query("   ").is_err());
    }

    #[test]
    fn test_query_accept_two_chars() {
        assert!(WordAnalyzer::new().tokenize_query("ab").is_ok());
    }

    #[test]
    fn test_query_reject_single_char() {
        assert!(WordAnalyzer::new().tokenize_query("a").is_err());
    }

    #[test]
    fn test_query_reject_no_tokens() {
        assert!(WordAnalyzer::new().tokenize_query("...").is_err());
    }

    #[test]
    fn test_query_error_contains_query() {
        let err = WordAnalyzer::new().tokenize_query("a").unwrap_err();
        assert!(err.to_string().contains("a"));
    }
}
