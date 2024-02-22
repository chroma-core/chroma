use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use std::collections::HashMap;
use crate::blockstore::{Blockfile, BlockfileKey, Key, PositionalPostingListBuilder, Value};
use crate::index::fulltext::tokenizer::{ChromaTokenizer, ChromaTokenStream};

#[derive(Error, Debug)]
pub enum FullTextIndexError {
    #[error("FullText error")]
    FullTextError,
}

impl ChromaError for FullTextIndexError {
    fn code(&self) -> ErrorCodes {
        match self {
            FullTextIndexError::FullTextError => ErrorCodes::Unimplemented,
        }
    }
}

pub(crate) trait FullTextIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;

    fn add_document(&mut self, document: &str, offset_id: i32) -> Result<(), Box<dyn ChromaError>>;
    fn search(&mut self, query: &str) -> Result<Vec<i32>, Box<dyn ChromaError>>;
}

pub(crate) struct BlockfileFullTextIndex {
    blockfile: Box<dyn Blockfile>,
    tokenizer: Box<dyn ChromaTokenizer>,
    in_transaction: bool,
    // term -> positional posting list builder for that term
    uncommitted: HashMap<String, PositionalPostingListBuilder>,
}

impl BlockfileFullTextIndex {
    pub(crate) fn new(blockfile: Box<dyn Blockfile>, tokenizer: Box<dyn ChromaTokenizer>) -> Self {
        BlockfileFullTextIndex {
            blockfile,
            tokenizer,
            in_transaction: false,
            uncommitted: HashMap::new(),
        }
    }
}

impl FullTextIndex for BlockfileFullTextIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(FullTextIndexError::FullTextError));
        }
        self.blockfile.begin_transaction()?;
        self.in_transaction = true;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(FullTextIndexError::FullTextError));
        }
        self.in_transaction = false;
        for (key, mut value) in self.uncommitted.drain() {
            let positional_posting_list = value.build();
            let blockfilekey = BlockfileKey::new(key, Key::String("".to_string()));
            self.blockfile.set(blockfilekey, Value::PositionalPostingListValue(positional_posting_list));
        }
        self.blockfile.commit_transaction()?;
        self.uncommitted.clear();
        Ok(())
    }

    fn add_document(&mut self, document: &str, offset_id: i32) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(FullTextIndexError::FullTextError));
        }
        let tokens = self.tokenizer.encode(document);
        for token in tokens.get_tokens() {
            let mut builder = self.uncommitted.entry(token.text.to_string()).or_insert(PositionalPostingListBuilder::new());
            if !builder.contains_doc_id(offset_id) {
                builder.add_doc_id_and_positions(offset_id, vec![]);
            } else {
                builder.add_positions_for_doc_id(offset_id, vec![]);
            }
        }
        Ok(())
    }

    fn search(&mut self, query: &str) -> Result<Vec<i32>, Box<dyn ChromaError>> {
        let tokens = self.tokenizer.encode(query);
        let mut candidates: HashMap<String, HashMap<i32, Vec<i32>> = HashMap::new();
        for token in tokens.get_tokens() {
            panic!("Not implemented")
        }
        Ok(results)
    }
}

mod test {
    use super::*;

    #[test]
    fn test_blockfile_fulltext_index() {
        let blockfile = Box::new(MemoryBlockfile::new());
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap())));
        let mut index = BlockfileFullTextIndex::new(blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("hello world", 1).unwrap();
        index.add_document("hello chroma", 2).unwrap();
        index.add_document("chroma world", 3).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("hello");
        assert_eq!(res.unwrap(), vec![1, 2]);

        let res = index.search("world");
        assert_eq!(res.unwrap(), vec![1, 3]);

        let res = index.search("llo chro");
        assert_eq!(res.unwrap(), vec![2]);
    }
}