use crate::blockstore::{Blockfile, BlockfileKey, Key, PositionalPostingListBuilder, Value};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::fulltext::tokenizer::ChromaTokenizer;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FullTextIndexError {
    #[error("Already in a transaction")]
    AlreadyInTransaction,
    #[error("Not in a transaction")]
    NotInTransaction,
}

impl ChromaError for FullTextIndexError {
    fn code(&self) -> ErrorCodes {
        match self {
            FullTextIndexError::AlreadyInTransaction => ErrorCodes::FailedPrecondition,
            FullTextIndexError::NotInTransaction => ErrorCodes::FailedPrecondition,
        }
    }
}

pub(crate) trait FullTextIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;

    // Must be done inside a transaction.
    fn add_document(&mut self, document: &str, offset_id: i32) -> Result<(), Box<dyn ChromaError>>;
    // Only searches committed state.
    fn search(&mut self, query: &str) -> Result<Vec<i32>, Box<dyn ChromaError>>;
}

pub(crate) struct BlockfileFullTextIndex {
    posting_lists_blockfile: Box<dyn Blockfile>,
    frequencies_blockfile: Box<dyn Blockfile>,
    tokenizer: Box<dyn ChromaTokenizer>,
    in_transaction: bool,

    // term -> positional posting list builder for that term
    uncommitted: HashMap<String, PositionalPostingListBuilder>,
    uncommitted_frequencies: HashMap<String, i32>,
}

impl BlockfileFullTextIndex {
    pub(crate) fn new(
        posting_lists_blockfile: Box<dyn Blockfile>,
        frequencies_blockfile: Box<dyn Blockfile>,
        tokenizer: Box<dyn ChromaTokenizer>,
    ) -> Self {
        BlockfileFullTextIndex {
            posting_lists_blockfile,
            frequencies_blockfile,
            tokenizer,
            in_transaction: false,
            uncommitted: HashMap::new(),
            uncommitted_frequencies: HashMap::new(),
        }
    }
}

impl FullTextIndex for BlockfileFullTextIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(FullTextIndexError::AlreadyInTransaction));
        }
        match self.posting_lists_blockfile.begin_transaction() {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        match self.frequencies_blockfile.begin_transaction() {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        self.in_transaction = true;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(FullTextIndexError::NotInTransaction));
        }
        self.in_transaction = false;
        for (key, mut value) in self.uncommitted.drain() {
            let positional_posting_list = value.build();
            let blockfilekey = BlockfileKey::new("".to_string(), Key::String(key.to_string()));
            self.posting_lists_blockfile.set(
                blockfilekey,
                Value::PositionalPostingListValue(positional_posting_list),
            );
        }
        for (key, value) in self.uncommitted_frequencies.drain() {
            let blockfilekey = BlockfileKey::new("".to_string(), Key::String(key.to_string()));
            self.frequencies_blockfile
                .set(blockfilekey, Value::IntValue(value));
        }
        match self.posting_lists_blockfile.commit_transaction() {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        match self.frequencies_blockfile.commit_transaction() {
            Ok(_) => {}
            Err(e) => return Err(e),
        }
        self.uncommitted.clear();
        Ok(())
    }

    fn add_document(&mut self, document: &str, offset_id: i32) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(FullTextIndexError::NotInTransaction));
        }
        let tokens = self.tokenizer.encode(document);
        for token in tokens.get_tokens() {
            self.uncommitted_frequencies
                .entry(token.text.to_string())
                .and_modify(|e| *e += 1)
                .or_insert(1);
            let mut builder = self
                .uncommitted
                .entry(token.text.to_string())
                .or_insert(PositionalPostingListBuilder::new());

            // Store starting positions of tokens. These are NOT affected by token filters.
            // For search, we can use the start and end positions to compute offsets to
            // check full string match.
            //
            // See https://docs.rs/tantivy/latest/tantivy/tokenizer/struct.Token.html
            if !builder.contains_doc_id(offset_id) {
                // Casting to i32 is safe since we limit the size of the document.
                builder.add_doc_id_and_positions(offset_id, vec![token.offset_from as i32]);
            } else {
                builder.add_positions_for_doc_id(offset_id, vec![token.offset_from as i32]);
            }
        }
        Ok(())
    }

    fn search(&mut self, query: &str) -> Result<Vec<i32>, Box<dyn ChromaError>> {
        let binding = self.tokenizer.encode(query);
        let tokens = binding.get_tokens();

        // Get query tokens sorted by frequency.
        let mut token_frequencies = vec![];
        for token in tokens {
            let blockfilekey =
                BlockfileKey::new("".to_string(), Key::String(token.text.to_string()));
            let value = self.frequencies_blockfile.get(blockfilekey);
            match value {
                Ok(Value::IntValue(frequency)) => {
                    token_frequencies.push((token.text.to_string(), frequency));
                }
                Ok(_) => {
                    return Ok(vec![]);
                }
                Err(_) => {
                    // TODO error handling from blockfile
                    return Ok(vec![]);
                }
            }
        }
        token_frequencies.sort_by(|a, b| a.1.cmp(&b.1));

        // Populate initial candidates with the least-frequent token's posting list.
        // doc ID -> possible starting locations for the query.
        let mut candidates: HashMap<i32, Vec<i32>> = HashMap::new();
        let blockfilekey =
            BlockfileKey::new("".to_string(), Key::String(tokens[0].text.to_string()));
        let first_token_positional_posting_list =
            match self.posting_lists_blockfile.get(blockfilekey).unwrap() {
                Value::PositionalPostingListValue(arr) => arr,
                _ => panic!("Value is not an arrow struct array"),
            };
        let first_token_offset = tokens[0].offset_from as i32;
        for doc_id in first_token_positional_posting_list.get_doc_ids().values() {
            let positions = first_token_positional_posting_list
                .get_positions_for_doc_id(*doc_id)
                .unwrap();
            let positions_vec: Vec<i32> = positions
                .values()
                .iter()
                .map(|x| *x - first_token_offset)
                .collect();
            candidates.insert(*doc_id, positions_vec);
        }

        // Iterate through the rest of the tokens, intersecting the posting lists with the candidates.
        for (token, _) in token_frequencies[1..].iter() {
            let blockfilekey = BlockfileKey::new("".to_string(), Key::String(token.to_string()));
            let positional_posting_list =
                match self.posting_lists_blockfile.get(blockfilekey).unwrap() {
                    Value::PositionalPostingListValue(arr) => arr,
                    _ => panic!("Value is not an arrow struct array"),
                };
            let token_offset = tokens
                .iter()
                .find(|t| t.text == *token)
                .unwrap()
                .offset_from as i32;
            let mut new_candidates: HashMap<i32, Vec<i32>> = HashMap::new();
            for (doc_id, positions) in candidates.iter() {
                let mut new_positions = vec![];
                for position in positions {
                    if let Some(positions_for_doc_id) =
                        positional_posting_list.get_positions_for_doc_id(*doc_id)
                    {
                        for position_for_doc_id in positions_for_doc_id.values() {
                            if position_for_doc_id - token_offset == *position {
                                new_positions.push(*position);
                            }
                        }
                    }
                }
                if !new_positions.is_empty() {
                    new_candidates.insert(*doc_id, new_positions);
                }
            }
            candidates = new_candidates;
        }

        let mut results = vec![];
        for (doc_id, _) in candidates.drain() {
            results.push(doc_id);
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockstore::provider::{BlockfileProvider, HashMapBlockfileProvider};
    use crate::blockstore::{HashMapBlockfile, KeyType, ValueType};
    use crate::index::fulltext::tokenizer::TantivyChromaTokenizer;
    use tantivy::tokenizer::NgramTokenizer;

    #[test]
    fn test_new() {
        let mut provider = HashMapBlockfileProvider::new();
        let pl_blockfile = provider
            .create("pl", KeyType::String, ValueType::PositionalPostingList)
            .unwrap();
        let freq_blockfile = provider
            .create("freq", KeyType::String, ValueType::Int)
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let _index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
    }

    #[test]
    fn test_index_single_document() {
        let mut provider = HashMapBlockfileProvider::new();
        let pl_blockfile = provider
            .create("pl", KeyType::String, ValueType::PositionalPostingList)
            .unwrap();
        let freq_blockfile = provider
            .create("freq", KeyType::String, ValueType::Int)
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("hello world", 1).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("hello");
        assert_eq!(res.unwrap(), vec![1]);
    }

    #[test]
    fn test_search_absent_token() {
        let mut provider = HashMapBlockfileProvider::new();
        let pl_blockfile = provider
            .create("pl", KeyType::String, ValueType::PositionalPostingList)
            .unwrap();
        let freq_blockfile = provider
            .create("freq", KeyType::String, ValueType::Int)
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("hello world", 1).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("chroma");
        assert!(res.unwrap().is_empty());
    }

    #[test]
    fn test_index_and_search_multiple_documents() {
        let mut provider = HashMapBlockfileProvider::new();
        let pl_blockfile = provider
            .create("pl", KeyType::String, ValueType::PositionalPostingList)
            .unwrap();
        let freq_blockfile = provider
            .create("freq", KeyType::String, ValueType::Int)
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("hello world", 1).unwrap();
        index.add_document("hello chroma", 2).unwrap();
        index.add_document("chroma world", 3).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("hello").unwrap();
        assert!(res.contains(&1));
        assert!(res.contains(&2));

        let res = index.search("world").unwrap();
        assert!(res.contains(&1));
        assert!(res.contains(&3));

        let res = index.search("llo chro").unwrap();
        assert!(res.contains(&2));
    }

    #[test]
    fn test_special_characters_search() {
        let mut provider = HashMapBlockfileProvider::new();
        let pl_blockfile = provider
            .create("pl", KeyType::String, ValueType::PositionalPostingList)
            .unwrap();
        let freq_blockfile = provider
            .create("freq", KeyType::String, ValueType::Int)
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("!!!!", 1).unwrap();
        index.add_document(",,!!", 2).unwrap();
        index.add_document(".!", 3).unwrap();
        index.add_document("!.!.!.!", 4).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("!!").unwrap();
        assert!(res.contains(&1));
        assert!(res.contains(&2));

        let res = index.search(".!").unwrap();
        assert!(res.contains(&3));
        assert!(res.contains(&4));
    }
}
