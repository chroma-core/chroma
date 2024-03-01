use crate::errors::{ChromaError, ErrorCodes};
use thiserror::Error;

use std::collections::HashMap;
use crate::blockstore::{Blockfile, BlockfileKey, Key, PositionalPostingListBuilder, Value};
use crate::index::fulltext::tokenizer::{ChromaTokenizer, ChromaTokenStream};
use tantivy::tokenizer::Token;

#[derive(Error, Debug)]
pub enum FullTextIndexError {
    #[error("Already in a transaction")]
    InTransaction,
    #[error("Not in a transaction")]
    NotInTransaction,
    #[error("Document too short")]
    DocumentTooShort,
    #[error("Query too short")]
    QueryTooShort,
    #[error("Negative offset IDs are not allowed")]
    NegativeOffsetId,
}

impl ChromaError for FullTextIndexError {
    fn code(&self) -> ErrorCodes {
        match self {
            FullTextIndexError::InTransaction => ErrorCodes::FailedPrecondition,
            FullTextIndexError::NotInTransaction => ErrorCodes::FailedPrecondition,
            FullTextIndexError::DocumentTooShort => ErrorCodes::InvalidArgument,
            FullTextIndexError::QueryTooShort => ErrorCodes::InvalidArgument,
            FullTextIndexError::NegativeOffsetId => ErrorCodes::InvalidArgument,
        }
    }
}

pub(crate) trait FullTextIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>>;
    fn in_transaction(&self) -> bool;

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
    pub(crate) fn new(posting_lists_blockfile: Box<dyn Blockfile>, frequencies_blockfile: Box<dyn Blockfile>, tokenizer: Box<dyn ChromaTokenizer>) -> Self {
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

impl BlockfileFullTextIndex {
    // Factored this way for testing.
    fn search_immutable(&self, tokens: &Vec<Token>) -> Result<Vec<i32>, Box<dyn ChromaError>> {
        if tokens.is_empty() {
            return Ok(vec![]);
        }
        if tokens.iter().map(|t| t.position_length).sum::<usize>() < 3 {
            return Err(Box::new(FullTextIndexError::QueryTooShort));
        }
        // Get query tokens sorted by frequency.
        let mut token_frequencies = vec![];
        for token in tokens {
            let blockfilekey = BlockfileKey::new("".to_string(), Key::String(token.text.to_string()));
            let value = self.frequencies_blockfile.get(blockfilekey);
            match value {
                Ok(Value::Int32Value(frequency)) => {
                    token_frequencies.push((token.text.to_string(), frequency));
                },
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
        let blockfilekey = BlockfileKey::new("".to_string(), Key::String(tokens[0].text.to_string()));
        let first_token_positional_posting_list = match self.posting_lists_blockfile.get(blockfilekey).unwrap() {
            Value::PositionalPostingListValue(arr) => arr,
            _ => panic!("Value is not an arrow struct array"),
        };
        let first_token_offset = tokens[0].offset_from as i32;
        for doc_id in first_token_positional_posting_list.get_doc_ids().values() {
            let doc_id = *doc_id;
            let positions = first_token_positional_posting_list.get_positions_for_doc_id(doc_id).unwrap();
            let positions_vec = positions.values().iter().map(|x| x - first_token_offset).collect();
            candidates.insert(doc_id, positions_vec);
        }

        // Iterate through the rest of the tokens, intersecting the posting lists with the candidates.
        for (token, _) in token_frequencies[1..].iter() {
            let blockfilekey = BlockfileKey::new("".to_string(), Key::String(token.to_string()));
            let positional_posting_list = match self.posting_lists_blockfile.get(blockfilekey).unwrap() {
                Value::PositionalPostingListValue(arr) => arr,
                _ => panic!("Value is not an arrow struct array"),
            };
            let token_offset = tokens.iter().find(|t| t.text == *token).unwrap().offset_from as i32;
            let mut new_candidates: HashMap<i32, Vec<i32>> = HashMap::new();
            for (doc_id, positions) in candidates.iter() {
                let doc_id = *doc_id;
                let mut new_positions = vec![];
                for position in positions {
                    if let Some(positions_for_doc_id) = positional_posting_list.get_positions_for_doc_id(doc_id) {
                        for position_for_doc_id in positions_for_doc_id.values() {
                            if *position_for_doc_id - token_offset == *position {
                                new_positions.push(*position);
                            }
                        }
                    }
                }
                if !new_positions.is_empty() {
                    new_candidates.insert(doc_id, new_positions);
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

impl FullTextIndex for BlockfileFullTextIndex {
    fn begin_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(FullTextIndexError::InTransaction));
        }
        self.posting_lists_blockfile.begin_transaction()?;
        self.frequencies_blockfile.begin_transaction()?;

        self.in_transaction = true;
        Ok(())
    }

    fn commit_transaction(&mut self) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(FullTextIndexError::NotInTransaction));
        }
        for (key, mut value) in self.uncommitted.drain() {
            let positional_posting_list = value.build();
            let blockfilekey = BlockfileKey::new("".to_string(), Key::String(key.to_string()));
            self.posting_lists_blockfile.set(blockfilekey, Value::PositionalPostingListValue(positional_posting_list));
        }
        for (key, value) in self.uncommitted_frequencies.drain() {
            let blockfilekey = BlockfileKey::new("".to_string(), Key::String(key.to_string()));
            self.frequencies_blockfile.set(blockfilekey, Value::Int32Value(value.try_into().unwrap()));
        }

        self.posting_lists_blockfile.commit_transaction()?;
        self.frequencies_blockfile.commit_transaction()?;
        self.uncommitted.clear();
        self.in_transaction = false;
        Ok(())
    }

    fn in_transaction(&self) -> bool {
        self.in_transaction
    }

    fn add_document(&mut self, document: &str, offset_id: i32) -> Result<(), Box<dyn ChromaError>> {
        if !self.in_transaction {
            return Err(Box::new(FullTextIndexError::NotInTransaction));
        }
        if document.len() < 5 as usize {
            return Err(Box::new(FullTextIndexError::DocumentTooShort));
        }
        if offset_id < 0 {
            return Err(Box::new(FullTextIndexError::NegativeOffsetId));
        }
        let offset_id = offset_id;
        let tokens = self.tokenizer.encode(document);
        for token in tokens.get_tokens() {
            self.uncommitted_frequencies.entry(token.text.to_string()).and_modify(|e| *e += 1).or_insert(1);
            let mut builder = self.uncommitted.entry(token.text.to_string()).or_insert(PositionalPostingListBuilder::new());

            // Store starting positions of tokens. These are NOT affected by token filters.
            // For search, we can use the start and end positions to compute offsets to
            // check full string match.
            //
            // See https://docs.rs/tantivy/latest/tantivy/tokenizer/struct.Token.html 
            if !builder.contains_doc_id(offset_id) {
                builder.add_doc_id_and_positions(offset_id, vec![token.offset_from.try_into().unwrap()]);
            } else {
                builder.add_positions_for_doc_id(offset_id, vec![token.offset_from.try_into().unwrap()]);
            }
        }
        Ok(())
    }

    fn search(&mut self, query: &str) -> Result<Vec<i32>, Box<dyn ChromaError>> {
        if self.in_transaction {
            return Err(Box::new(FullTextIndexError::InTransaction));
        }
        if query.len() < 3 as usize {
            return Err(Box::new(FullTextIndexError::QueryTooShort));
        }
        let binding = self.tokenizer.encode(query);
        let tokens = binding.get_tokens();
        return self.search_immutable(tokens);
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use tantivy::tokenizer::NgramTokenizer;
    use crate::blockstore::HashMapBlockfile;
    use crate::index::fulltext::tokenizer::TantivyChromaTokenizer;

    #[test]
    fn test_new() {
        let pl_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-pl").unwrap());
        let freq_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-freqs").unwrap());
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap())));
        let _index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
    }

    #[test]
    fn test_index_single_document() {
        let pl_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-pl").unwrap());
        let freq_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-freqs").unwrap());
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap())));
        let mut index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("hello world", 1).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("hello");
        assert_eq!(res.unwrap(), vec![1]);
    }

    #[test]
    fn test_search_absent_token() {
        let pl_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-pl").unwrap());
        let freq_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-freqs").unwrap());
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap())));
        let mut index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("hello world", 1).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("chroma");
        assert!(res.unwrap().is_empty());
    }

    #[test]
    fn test_index_and_search_multiple_documents() {
        let pl_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-pl").unwrap());
        let freq_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-freqs").unwrap());
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap())));
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
        let pl_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-pl").unwrap());
        let freq_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-freqs").unwrap());
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap())));
        let mut index = BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer);
        index.begin_transaction().unwrap();
        index.add_document("!!!!!!!!", 1).unwrap();
        index.add_document(",,!!,,,,", 2).unwrap();
        index.add_document(".!.!.!.!.!.!", 3).unwrap();
        index.add_document("!.!.!.!!!!!!", 4).unwrap();
        index.commit_transaction().unwrap();

        let res = index.search("!!!").unwrap();
        assert!(res.contains(&1));
        assert!(res.contains(&4));

        let res = index.search(".!.").unwrap();
        assert!(res.contains(&3));
        assert!(res.contains(&4));
    }

    use proptest::prelude::*;
    use proptest_state_machine::{prop_state_machine, ReferenceStateMachine, StateMachineTest};
    use proptest::test_runner::Config;
    use rand::prelude::IteratorRandom;
    // https://tonsky.me/blog/unicode/
    use stringslice::StringSlice;

    #[derive(Debug, Clone)]
    pub(crate) enum Transition {
        BeginTransaction,
        CommitTransaction,
        AddDocument(String, i32),
        Search(String),
    }

    #[derive(Debug, Clone)]
    pub(crate) struct ReferenceState {
        in_transaction: bool,
        // TODO use a real, tested, external index. This works for now but test
        // time grows quadratically in # of steps, limiting how thorough we
        // can be.
        state: HashMap<String, i32>,
    }

    impl ReferenceState {
        fn new() -> Self {
            ReferenceState {
                in_transaction: false,
                state: HashMap::new(),
            }
        }

        fn search(&self, query: &str) -> Vec<i32> {
            if query.len() < 3 {
                return vec![];
            }
            if self.in_transaction {
                return vec![];
            }
            let mut results = vec![];
            for (doc, id) in &self.state {
                if doc.contains(query) {
                    results.push(*id);
                }
            }
            results
        }

        fn add_document(&mut self, doc: String, id: i32) {
            if doc.len() < 5 {
                return;
            }
            if id < 0 {
                return;
            }
            if !self.in_transaction {
                return;
            }
            self.state.insert(doc, id);
        }
    }

    impl ReferenceStateMachine for ReferenceState {
        type State = ReferenceState;
        type Transition = Transition;

        fn init_state() -> BoxedStrategy<Self::State> {
            Just(ReferenceState::new()).boxed()
        }

        fn transitions(state: &ReferenceState) -> BoxedStrategy<Transition> {
            // Grab a random chunk of a random string in state
            let doc = state.state.keys().choose(&mut rand::thread_rng());
            if doc.is_none() {
                return prop_oneof![
                    Just(Transition::BeginTransaction),
                    (".{4,16000}", (-5..999999)).prop_map(move |(doc, id)| Transition::AddDocument(doc, id)),
                ].boxed();
            }

            let doc = doc.unwrap();
            let start = rand::thread_rng().gen_range(0..(doc.len() / 2));
            let end = rand::thread_rng().gen_range((start + 5)..doc.len());
            let doc = &doc.try_slice(start..end);
            if doc.is_none() {
                return prop_oneof![
                    Just(Transition::BeginTransaction),
                    (".{4,16000}", (-5..999999)).prop_map(move |(doc, id)| Transition::AddDocument(doc, id)),
                ].boxed();
            }
            let doc = doc.unwrap();

            prop_oneof![
                Just(Transition::BeginTransaction),
                Just(Transition::CommitTransaction),
                (".{4,16000}", (-5..999999)).prop_map(move |(doc, id)| Transition::AddDocument(doc, id)),
                ".*{3,1000}".prop_map(Transition::Search),
                Just(Transition::Search(doc.to_string())),
            ].boxed()
        }

        fn apply(mut state: ReferenceState, transition: &Transition) -> Self::State {
            match transition {
                Transition::BeginTransaction => {
                    state.in_transaction = true;
                },
                Transition::CommitTransaction => {
                    state.in_transaction = false;
                },
                Transition::AddDocument(doc, id) => {
                    if !state.in_transaction {
                        return state;
                    }
                    if doc.len() < 5 {
                        return state;
                    }
                    state.add_document(doc.clone(), *id);
                },
                Transition::Search(_) => {
                    // no-op
                },
            }
            state
        }
    }

    impl StateMachineTest for BlockfileFullTextIndex {
        type SystemUnderTest = Self;
        type Reference = ReferenceState;

        fn init_test(_ref_state: &ReferenceState) -> Self::SystemUnderTest {
            let pl_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-pl").unwrap());
            let freq_blockfile = Box::new(HashMapBlockfile::open(&"in-memory-freqs").unwrap());
            let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap())));
            BlockfileFullTextIndex::new(pl_blockfile, freq_blockfile, tokenizer)
        }

        fn apply(
            mut state: Self::SystemUnderTest,
            _ref_state: &<Self::Reference as ReferenceStateMachine>::State,
            transition: Transition,
        ) -> Self::SystemUnderTest {
            match transition {
                Transition::BeginTransaction => {
                    let in_transaction = state.in_transaction();
                    let res = state.begin_transaction();
                    if in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                Transition::CommitTransaction => {
                    let in_transaction = state.in_transaction();
                    let res = state.commit_transaction();
                    if !in_transaction {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                Transition::AddDocument(doc, id) => {
                    let in_transaction = state.in_transaction();
                    let res = state.add_document(&doc, id);
                    if !in_transaction || doc.len() < 5 {
                        assert!(res.is_err());
                    } else {
                        assert!(res.is_ok());
                    }
                },
                Transition::Search(query) => {
                    let in_transaction = state.in_transaction();
                    let res = state.search(&query);
                    if in_transaction || query.len() < 3 {
                        assert!(res.is_err());
                        return state;
                    }
                    let res = res.unwrap();
                    let ref_res = _ref_state.search(&query);
                    for id in &ref_res {
                        assert!(res.contains(&id));
                    }
                    for id in res {
                        assert!(ref_res.contains(&id));
                    }
                },
            }
            state
        }

        fn check_invariants(
            state: &Self::SystemUnderTest,
            ref_state: &<Self::Reference as ReferenceStateMachine>::State,
        ) {
            assert_eq!(state.in_transaction(), ref_state.in_transaction);
            if state.in_transaction() {
                return;
            }
            let mut tokenizer = TantivyChromaTokenizer::new(Box::new(NgramTokenizer::new(1, 1, false).unwrap()));
            for (doc, id) in &ref_state.state {
                let tokens = tokenizer.encode(doc);
                let res = state.search_immutable(&tokens.get_tokens());
                let res = res.unwrap();
                assert!(res.contains(&id));
            }
        }
    }

    prop_state_machine! {
        #![proptest_config(Config {
            // Enable verbose mode to make the state machine test print the
            // transitions for each case.
            verbose: 0,
            cases: 10,
            .. Config::default()
        })]

        #[test]
        fn proptest_fulltext_index(sequential 1..8 => BlockfileFullTextIndex);
    }
}