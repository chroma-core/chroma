use std::collections::HashSet;
use std::io::Cursor;

use murmur3::murmur3_32;
use tantivy::tokenizer::{
    AsciiFoldingFilter, LowerCaser, NgramTokenizer, RemoveLongFilter, SimpleTokenizer,
    TextAnalyzer, Token, TokenFilter, TokenStream, Tokenizer,
};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TRIGRAM_LENGTH: usize = 3;
const MAX_TOKEN_LENGTH: usize = 128;
const DEFAULT_HASH_BITS: u32 = 24;

/// Murmur3 seed. Fixed — changing it invalidates all existing blockfiles.
const HASH_SEED: u32 = 0x5f3759df;

/// Boundary characters hashed for cross-token transitions.
const TRANSITION_CHARS: usize = 2;

// ---------------------------------------------------------------------------
// RemoveShortFilter
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum TokenizerError {
    #[error("Query \"{query}\" has no token with at least {TRIGRAM_LENGTH} characters")]
    NoSelectiveToken { query: String },
    #[error("Hash error: {0}")]
    Hash(#[from] std::io::Error),
    #[error("Invalid tokenizer configuration: {0}")]
    Config(String),
}

// ---------------------------------------------------------------------------
// Output types
// ---------------------------------------------------------------------------

/// Decomposed document for the index writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentTokens {
    /// Unique bucket IDs this document contributes to (sorted, deduplicated).
    pub buckets: Vec<u32>,
    /// `(trigram, positional_key, bucket_id)` for the trigram index.
    /// Keys: 0 = prefix (first trigram), 1 = infix, 2 = suffix (last trigram).
    /// Single-trigram tokens emit both key=0 and key=2.
    pub trigrams: Vec<(String, u32, u32)>,
    /// `(transition_hash, prev_bucket, curr_bucket)` for adjacent token pairs.
    pub transitions: Vec<(u32, u32, u32)>,
}

/// A single token's lookup strategy during query evaluation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenLookup {
    /// Body token: direct bucket ID lookup.
    Direct(u32),
    /// Partial token: ordered trigrams for resolution via the trigram index.
    /// First element is the token's first trigram, last is last.
    Trigram(Vec<String>),
}

/// Decomposed query for the index reader.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryPlan {
    /// Ordered token lookups. The index uses position to determine which
    /// trigram positional keys to consult (first Trigram = prefix-side,
    /// last Trigram = suffix-side).
    pub lookups: Vec<TokenLookup>,
    /// Transition hashes between adjacent query tokens.
    pub transitions: Vec<u32>,
    /// Single partial token — no transitions, no adjacent pairs.
    pub singleton: bool,
}

// ---------------------------------------------------------------------------
// String helpers
// ---------------------------------------------------------------------------

fn last_n_chars(s: &str, n: usize) -> &str {
    let skip = s.chars().count().saturating_sub(n);
    let byte_offset = s.char_indices().nth(skip).map_or(0, |(i, _)| i);
    &s[byte_offset..]
}

fn first_n_chars(s: &str, n: usize) -> &str {
    let end = s.char_indices().nth(n).map_or(s.len(), |(i, _)| i);
    &s[..end]
}

// ---------------------------------------------------------------------------
// WordAnalyzer
// ---------------------------------------------------------------------------

/// Word-based text analyzer for the full-text index.
///
/// Owns the linguistic pipeline (tokenization, trigram extraction) and the
/// hash mapping (token → bucket ID, transition → hash). The index receives
/// fully resolved numeric keys and never performs string analysis.
///
/// Two internal pipelines:
/// - **Word**: `SimpleTokenizer` → `LowerCaser` → `AsciiFoldingFilter`
///   → `RemoveShortFilter(3)` → `RemoveLongFilter(128)`.
/// - **Trigram**: `NgramTokenizer(3,3)` → `LowerCaser`.
#[derive(Clone)]
pub struct WordAnalyzer {
    num_buckets: u32,
    trigram_tokenizer: TextAnalyzer,
    word_tokenizer: TextAnalyzer,
}

impl WordAnalyzer {
    pub fn new(hash_bits: u32) -> Result<Self, TokenizerError> {
        let ngram = NgramTokenizer::new(TRIGRAM_LENGTH, TRIGRAM_LENGTH, false)
            .map_err(|e| TokenizerError::Config(e.to_string()))?;
        Ok(Self {
            num_buckets: 1u32 << hash_bits,
            trigram_tokenizer: TextAnalyzer::builder(ngram).filter(LowerCaser).build(),
            word_tokenizer: TextAnalyzer::builder(SimpleTokenizer::default())
                .filter(LowerCaser)
                .filter(AsciiFoldingFilter)
                .filter(RemoveShortFilter {
                    min_length: TRIGRAM_LENGTH,
                })
                .filter(RemoveLongFilter::limit(MAX_TOKEN_LENGTH))
                .build(),
        })
    }

    /// Decompose a document into bucket IDs, trigram entries, and transitions.
    pub fn tokenize_document(&mut self, text: &str) -> Result<DocumentTokens, TokenizerError> {
        let tokens = self.tokenize(text);

        let mut seen = HashSet::new();
        let mut trigrams = Vec::new();
        let mut transitions = Vec::new();
        let mut prev: Option<(String, u32)> = None;

        for token in &tokens {
            let bucket = self.hash_token(token)?;

            if seen.insert(bucket) {
                self.emit_trigrams(token, bucket, &mut trigrams);
            }

            if let Some((ref prev_tok, prev_bucket)) = prev {
                let h = self.hash_transition(
                    last_n_chars(prev_tok, TRANSITION_CHARS),
                    first_n_chars(token, TRANSITION_CHARS),
                )?;
                transitions.push((h, prev_bucket, bucket));
            }
            prev = Some((token.clone(), bucket));
        }

        let mut buckets: Vec<u32> = seen.into_iter().collect();
        buckets.sort_unstable();
        Ok(DocumentTokens {
            buckets,
            trigrams,
            transitions,
        })
    }

    /// Decompose a query into a plan the index reader can execute directly.
    pub fn plan_query(&mut self, query: &str) -> Result<QueryPlan, TokenizerError> {
        let (tokens, has_prefix, has_suffix) = {
            let mut tokens = Vec::new();
            let mut first_offset_from = None;
            let mut last_offset_to = 0;
            let mut stream = self.word_tokenizer.token_stream(query);
            while let Some(token) = stream.next() {
                if first_offset_from.is_none() {
                    first_offset_from = Some(token.offset_from);
                }
                last_offset_to = token.offset_to;
                tokens.push(token.text.clone());
            }
            (
                tokens,
                first_offset_from == Some(0),
                last_offset_to == query.len(),
            )
        };

        if tokens.is_empty() {
            return Err(TokenizerError::NoSelectiveToken {
                query: query.to_string(),
            });
        }

        let singleton = tokens.len() == 1 && has_prefix && has_suffix;

        let lookups = tokens
            .iter()
            .enumerate()
            .map(|(i, token)| {
                if (i == 0 && has_prefix) || (i == tokens.len() - 1 && has_suffix) {
                    Ok(TokenLookup::Trigram(self.trigrams(token)))
                } else {
                    Ok(TokenLookup::Direct(self.hash_token(token)?))
                }
            })
            .collect::<Result<Vec<_>, TokenizerError>>()?;

        let transitions = tokens
            .windows(2)
            .map(|w| {
                self.hash_transition(
                    last_n_chars(&w[0], TRANSITION_CHARS),
                    first_n_chars(&w[1], TRANSITION_CHARS),
                )
            })
            .collect::<Result<Vec<_>, TokenizerError>>()?;

        Ok(QueryPlan {
            lookups,
            singleton,
            transitions,
        })
    }

    // --- Private helpers ---

    fn tokenize(&mut self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut stream = self.word_tokenizer.token_stream(text);
        while let Some(token) = stream.next() {
            tokens.push(token.text.clone());
        }
        tokens
    }

    fn trigrams(&mut self, s: &str) -> Vec<String> {
        let mut trigrams = Vec::new();
        let mut stream = self.trigram_tokenizer.token_stream(s);
        while let Some(token) = stream.next() {
            trigrams.push(token.text.clone());
        }
        trigrams
    }

    fn hash_token(&self, token: &str) -> Result<u32, TokenizerError> {
        let hash = murmur3_32(&mut Cursor::new(token.as_bytes()), HASH_SEED)?;
        Ok(hash % self.num_buckets)
    }

    /// Hash a cross-token transition. The index applies flag bits to
    /// distinguish transition keys from bucket keys in the blockfile.
    fn hash_transition(&self, prev_suffix: &str, curr_prefix: &str) -> Result<u32, TokenizerError> {
        // Max bytes: each side is at most TRANSITION_CHARS chars × 4 bytes/char (UTF-8),
        // plus 1 null separator.
        const CAP: usize = TRANSITION_CHARS * 4 * 2 + 1;
        let mut buf = [0u8; CAP];
        let a = prev_suffix.as_bytes();
        let b = curr_prefix.as_bytes();
        let len = a.len() + 1 + b.len();
        buf[..a.len()].copy_from_slice(a);
        buf[a.len()] = 0;
        buf[a.len() + 1..len].copy_from_slice(b);
        let hash = murmur3_32(&mut Cursor::new(&buf[..len]), HASH_SEED)?;
        Ok(hash % self.num_buckets)
    }

    /// Emit trigram entries with positional keys for a token.
    fn emit_trigrams(&mut self, token: &str, bucket: u32, out: &mut Vec<(String, u32, u32)>) {
        let tris = self.trigrams(token);
        let last = tris.len().saturating_sub(1);
        for (i, tri) in tris.into_iter().enumerate() {
            match i {
                _ if i == 0 && i == last => {
                    // Single-trigram token: both prefix and suffix.
                    out.push((tri.clone(), 0, bucket));
                    out.push((tri, 2, bucket));
                }
                0 => out.push((tri, 0, bucket)),
                i if i == last => out.push((tri, 2, bucket)),
                _ => out.push((tri, 1, bucket)),
            }
        }
    }
}

impl Default for WordAnalyzer {
    fn default() -> Self {
        Self::new(DEFAULT_HASH_BITS).expect("default configuration should be valid")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- tokenize_document ---

    #[test]
    fn test_document_buckets() {
        let mut a = WordAnalyzer::default();
        let dt = a.tokenize_document("hello world hello").unwrap();
        // Deduped: 2 unique tokens.
        assert_eq!(dt.buckets.len(), 2);
        assert!(dt.buckets.contains(&a.hash_token("hello").unwrap()));
        assert!(dt.buckets.contains(&a.hash_token("world").unwrap()));
    }

    #[test]
    fn test_document_filters() {
        let mut a = WordAnalyzer::default();
        // "a" (1 char) and "is" (2 chars) removed; ASCII folding on "café".
        let dt = a.tokenize_document("a is café").unwrap();
        assert_eq!(dt.buckets.len(), 1);
        assert!(dt.buckets.contains(&a.hash_token("cafe").unwrap()));
    }

    #[test]
    fn test_document_trigrams() {
        let mut a = WordAnalyzer::default();
        // Multi-trigram: "hello" → "hel"(0), "ell"(1), "llo"(2).
        let dt = a.tokenize_document("hello the").unwrap();
        let hello_b = a.hash_token("hello").unwrap();
        let the_b = a.hash_token("the").unwrap();
        let hello_tri: Vec<(&str, u32)> = dt
            .trigrams
            .iter()
            .filter(|(_, _, b)| *b == hello_b)
            .map(|(s, k, _)| (s.as_str(), *k))
            .collect();
        assert_eq!(hello_tri, vec![("hel", 0), ("ell", 1), ("llo", 2)]);
        // Single-trigram: "the" → key=0 and key=2, no infix.
        let the_tri: Vec<(&str, u32)> = dt
            .trigrams
            .iter()
            .filter(|(_, _, b)| *b == the_b)
            .map(|(s, k, _)| (s.as_str(), *k))
            .collect();
        assert_eq!(the_tri, vec![("the", 0), ("the", 2)]);
    }

    #[test]
    fn test_document_transitions() {
        let mut a = WordAnalyzer::default();
        let dt = a.tokenize_document("hello world peace").unwrap();
        assert_eq!(dt.transitions.len(), 2);
        assert_eq!(dt.transitions[0].1, a.hash_token("hello").unwrap());
        assert_eq!(dt.transitions[0].2, a.hash_token("world").unwrap());
        assert_eq!(dt.transitions[1].1, a.hash_token("world").unwrap());
        assert_eq!(dt.transitions[1].2, a.hash_token("peace").unwrap());
        // Filtered tokens produce no pairs: only "the" survives → 0 transitions.
        let dt2 = a.tokenize_document("a is the").unwrap();
        assert!(dt2.transitions.is_empty());
    }

    #[test]
    fn test_document_empty() {
        let mut a = WordAnalyzer::default();
        let dt = a.tokenize_document("").unwrap();
        assert!(dt.buckets.is_empty());
        assert!(dt.trigrams.is_empty());
        assert!(dt.transitions.is_empty());
    }

    // --- plan_query ---

    #[test]
    fn test_query_singleton() {
        let mut a = WordAnalyzer::default();
        let plan = a.plan_query("hello").unwrap();
        assert!(plan.singleton);
        assert_eq!(plan.lookups.len(), 1);
        let TokenLookup::Trigram(ref tris) = plan.lookups[0] else {
            panic!("expected Trigram");
        };
        assert_eq!(tris, &["hel", "ell", "llo"]);
        assert!(plan.transitions.is_empty());
    }

    #[test]
    fn test_query_body_tokens() {
        let mut a = WordAnalyzer::default();
        // Three words: prefix(Trigram) + body(Direct) + suffix(Trigram).
        let plan = a.plan_query("hello beautiful world").unwrap();
        assert!(!plan.singleton);
        assert_eq!(plan.lookups.len(), 3);
        assert!(matches!(&plan.lookups[0], TokenLookup::Trigram(_)));
        assert!(matches!(&plan.lookups[1], TokenLookup::Direct(_)));
        assert!(matches!(&plan.lookups[2], TokenLookup::Trigram(_)));
        assert_eq!(plan.transitions.len(), 2);
    }

    #[test]
    fn test_query_boundaries() {
        let mut a = WordAnalyzer::default();
        // Leading space → first token becomes Direct (not prefix).
        let plan = a.plan_query(" hello world").unwrap();
        assert!(matches!(&plan.lookups[0], TokenLookup::Direct(_)));
        assert!(matches!(&plan.lookups[1], TokenLookup::Trigram(_)));
        // Trailing punctuation → last token becomes Direct (not suffix).
        let plan = a.plan_query("hello world.").unwrap();
        assert!(matches!(&plan.lookups[0], TokenLookup::Trigram(_)));
        assert!(matches!(&plan.lookups[1], TokenLookup::Direct(_)));
        // Both → all Direct, verify hash value.
        let plan = a.plan_query(" hello ").unwrap();
        assert_eq!(plan.lookups.len(), 1);
        let TokenLookup::Direct(bucket) = plan.lookups[0] else {
            panic!("expected Direct");
        };
        assert_eq!(bucket, a.hash_token("hello").unwrap());
    }

    #[test]
    fn test_query_reject_no_tokens() {
        let mut a = WordAnalyzer::default();
        assert!(a.plan_query("").is_err());
        assert!(a.plan_query("ab").is_err());
        assert!(a.plan_query("   ").is_err());
    }

    // --- hash_bits ---

    #[test]
    fn test_hash_bits() {
        let mut a = WordAnalyzer::new(16).unwrap();
        assert!(a.hash_token("hello").unwrap() < (1 << 16));
        let dt = a.tokenize_document("hello world").unwrap();
        for &bucket in &dt.buckets {
            assert!(bucket < (1 << 16));
        }
    }
}
