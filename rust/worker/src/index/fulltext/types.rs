use crate::blockstore::positional_posting_list_value::{
    PositionalPostingListBuilder, PositionalPostingListBuilderError,
};
use crate::blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::fulltext::tokenizer::ChromaTokenizer;

use arrow::array::Int32Array;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum FullTextIndexError {
    #[error("Multiple tokens found in frequencies blockfile")]
    MultipleTokenFrequencies,
    #[error("Empty value in positional posting list")]
    EmptyValueInPositionalPostingList,
    #[error("Invariant violation")]
    InvariantViolation,
    #[error("Positional posting list error: {0}")]
    PositionalPostingListError(#[from] PositionalPostingListBuilderError),
    #[error("Blockfile write error: {0}")]
    BlockfileWriteError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for FullTextIndexError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

pub(crate) struct FullTextIndexWriter<'me> {
    // We use this to implement updates which require read-then-write semantics.
    full_text_index_reader: Option<FullTextIndexReader<'me>>,
    posting_lists_blockfile_writer: BlockfileWriter,
    frequencies_blockfile_writer: BlockfileWriter,
    tokenizer: Arc<Mutex<Box<dyn ChromaTokenizer>>>,

    // term -> positional posting list builder for that term
    uncommitted: Arc<Mutex<HashMap<String, PositionalPostingListBuilder>>>,
    uncommitted_frequencies: Arc<Mutex<HashMap<String, i32>>>,
}

impl<'me> FullTextIndexWriter<'me> {
    pub fn new(
        full_text_index_reader: Option<FullTextIndexReader<'me>>,
        posting_lists_blockfile_writer: BlockfileWriter,
        frequencies_blockfile_writer: BlockfileWriter,
        tokenizer: Box<dyn ChromaTokenizer>,
    ) -> Self {
        FullTextIndexWriter {
            full_text_index_reader,
            posting_lists_blockfile_writer,
            frequencies_blockfile_writer,
            tokenizer: Arc::new(Mutex::new(tokenizer)),
            uncommitted: Arc::new(Mutex::new(HashMap::new())),
            uncommitted_frequencies: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn populate_frequencies_and_posting_lists_from_previous_version(
        &self,
        token: &str,
    ) -> Result<(), FullTextIndexError> {
        let mut uncommitted_frequencies = self.uncommitted_frequencies.lock();
        match uncommitted_frequencies.get(token) {
            Some(_) => return Ok(()),
            None => {
                let frequency = match &self.full_text_index_reader {
                    None => 0,
                    Some(reader) => match reader.get_frequencies_for_token(token).await {
                        Ok(frequency) => frequency,
                        Err(_) => 0,
                    },
                };
                uncommitted_frequencies.insert(token.to_string(), frequency as i32);
            }
        }
        let mut uncommitted = self.uncommitted.lock();
        match uncommitted.get(token) {
            Some(_) => {
                // This should never happen -- if uncommitted has the token, then
                // uncommitted_frequencies should have had it as well.
                tracing::error!(
                    "Error populating frequencies and posting lists from previous version"
                );
                return Err(FullTextIndexError::InvariantViolation);
            }
            None => {
                let mut builder = PositionalPostingListBuilder::new();
                let results = match &self.full_text_index_reader {
                    None => vec![],
                    Some(reader) => match reader.get_all_results_for_token(token).await {
                        Ok(results) => results,
                        Err(_) => vec![],
                    },
                };
                for (doc_id, positions) in results {
                    let res = builder.add_doc_id_and_positions(doc_id as i32, positions);
                    match res {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(FullTextIndexError::PositionalPostingListError(e));
                        }
                    }
                }
                uncommitted.insert(token.to_string(), builder);
            }
        }
        Ok(())
    }

    pub async fn add_document(
        &self,
        document: &str,
        offset_id: i32,
    ) -> Result<(), FullTextIndexError> {
        let tokenizer = self.tokenizer.lock();
        let tokens = tokenizer.encode(document);
        for token in tokens.get_tokens() {
            self.populate_frequencies_and_posting_lists_from_previous_version(token.text.as_str())
                .await?;
            let mut uncommitted_frequencies = self.uncommitted_frequencies.lock();
            uncommitted_frequencies
                .entry(token.text.to_string())
                .and_modify(|e| *e += 1);
            let mut uncommitted = self.uncommitted.lock();
            let builder = uncommitted
                .entry(token.text.to_string())
                .or_insert(PositionalPostingListBuilder::new());

            // Store starting positions of tokens. These are NOT affected by token filters.
            // For search, we can use the start and end positions to compute offsets to
            // check full string match.
            //
            // See https://docs.rs/tantivy/latest/tantivy/tokenizer/struct.Token.html
            if !builder.contains_doc_id(offset_id) {
                // Casting to i32 is safe since we limit the size of the document.
                match builder.add_doc_id_and_positions(offset_id, vec![token.offset_from as i32]) {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(FullTextIndexError::PositionalPostingListError(e));
                    }
                }
            } else {
                match builder.add_positions_for_doc_id(offset_id, vec![token.offset_from as i32]) {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(FullTextIndexError::PositionalPostingListError(e));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn delete_document(
        &self,
        document: &str,
        offset_id: u32,
    ) -> Result<(), FullTextIndexError> {
        let tokenizer = self.tokenizer.lock();
        let tokens = tokenizer.encode(document);
        for token in tokens.get_tokens() {
            self.populate_frequencies_and_posting_lists_from_previous_version(token.text.as_str())
                .await?;
            let mut uncommitted_frequencies = self.uncommitted_frequencies.lock();
            match uncommitted_frequencies.get_mut(token.text.as_str()) {
                Some(frequency) => {
                    *frequency -= 1;
                }
                None => {
                    // Invariant violation -- we just populated this.
                    tracing::error!("Error decrementing frequency for token: {:?}", token.text);
                    return Err(FullTextIndexError::InvariantViolation);
                }
            }
            let mut uncommitted = self.uncommitted.lock();
            match uncommitted.get_mut(token.text.as_str()) {
                Some(builder) => match builder.delete_doc_id(offset_id as i32) {
                    Ok(_) => {}
                    Err(e) => {
                        // This is a fatal invariant violation: we've been asked to
                        // delete a document which doesn't appear in the positional posting list.
                        // It probably indicates data corruption of some sort.
                        tracing::error!(
                            "Error deleting doc ID from positional posting list: {:?}",
                            e
                        );
                        return Err(FullTextIndexError::PositionalPostingListError(e));
                    }
                },
                None => {
                    // Invariant violation -- we just populated this.
                    tracing::error!("Error deleting doc ID from positional posting list");
                    return Err(FullTextIndexError::InvariantViolation);
                }
            }
        }
        Ok(())
    }

    pub async fn update_document(
        &self,
        old_document: &str,
        new_document: &str,
        offset_id: u32,
    ) -> Result<(), FullTextIndexError> {
        self.delete_document(old_document, offset_id).await?;
        self.add_document(new_document, offset_id as i32).await?;
        Ok(())
    }

    pub async fn write_to_blockfiles(&mut self) -> Result<(), FullTextIndexError> {
        let mut uncommitted = self.uncommitted.lock();
        for (key, mut value) in uncommitted.drain() {
            let built_list = value.build();
            for doc_id in built_list.doc_ids.iter() {
                match doc_id {
                    Some(doc_id) => {
                        let positional_posting_list =
                            built_list.get_positions_for_doc_id(doc_id).unwrap();
                        match self
                            .posting_lists_blockfile_writer
                            .set(key.as_str(), doc_id as u32, &positional_posting_list)
                            .await
                        {
                            Ok(_) => {}
                            Err(e) => {
                                return Err(FullTextIndexError::BlockfileWriteError(e));
                            }
                        }
                    }
                    None => {
                        panic!("Positions for doc ID not found in positional posting list -- should never happen")
                    }
                }
            }
        }
        let mut uncommitted_frequencies = self.uncommitted_frequencies.lock();
        for (key, value) in uncommitted_frequencies.drain() {
            // TODO we just have token -> frequency here. Should frequency be the key or should we use an empty key and make it the value?
            match self
                .frequencies_blockfile_writer
                .set(key.as_str(), value as u32, 0)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    return Err(FullTextIndexError::BlockfileWriteError(e));
                }
            }
        }
        Ok(())
    }

    pub fn commit(self) -> Result<FullTextIndexFlusher, Box<dyn ChromaError>> {
        // TODO should we be `await?`ing these? Or can we just return the futures?
        let posting_lists_blockfile_flusher = self
            .posting_lists_blockfile_writer
            .commit::<u32, &Int32Array>()?;
        let frequencies_blockfile_flusher =
            self.frequencies_blockfile_writer.commit::<u32, &str>()?;
        Ok(FullTextIndexFlusher {
            posting_lists_blockfile_flusher,
            frequencies_blockfile_flusher,
        })
    }
}

pub(crate) struct FullTextIndexFlusher {
    posting_lists_blockfile_flusher: BlockfileFlusher,
    frequencies_blockfile_flusher: BlockfileFlusher,
}

impl FullTextIndexFlusher {
    pub async fn flush(self) -> Result<(), Box<dyn ChromaError>> {
        let res = self
            .posting_lists_blockfile_flusher
            .flush::<u32, &Int32Array>()
            .await;
        if res.is_err() {
            return res;
        }
        let res = self
            .frequencies_blockfile_flusher
            .flush::<u32, &str>()
            .await;
        if res.is_err() {
            return res;
        }
        Ok(())
    }

    pub fn pls_id(&self) -> Uuid {
        self.posting_lists_blockfile_flusher.id()
    }

    pub fn freqs_id(&self) -> Uuid {
        self.frequencies_blockfile_flusher.id()
    }
}

pub(crate) struct FullTextIndexReader<'me> {
    posting_lists_blockfile_reader: BlockfileReader<'me, u32, Int32Array>,
    frequencies_blockfile_reader: BlockfileReader<'me, u32, u32>,
    tokenizer: Arc<Mutex<Box<dyn ChromaTokenizer>>>,
}

impl<'me> FullTextIndexReader<'me> {
    pub fn new(
        posting_lists_blockfile_reader: BlockfileReader<'me, u32, Int32Array>,
        frequencies_blockfile_reader: BlockfileReader<'me, u32, u32>,
        tokenizer: Box<dyn ChromaTokenizer>,
    ) -> Self {
        FullTextIndexReader {
            posting_lists_blockfile_reader,
            frequencies_blockfile_reader,
            tokenizer: Arc::new(Mutex::new(tokenizer)),
        }
    }

    pub async fn search(&self, query: &str) -> Result<Vec<i32>, Box<dyn ChromaError>> {
        let tokenizer = self.tokenizer.lock();
        let binding = tokenizer.encode(query);
        let tokens = binding.get_tokens();

        // Get query tokens sorted by frequency.
        let mut token_frequencies: Vec<(String, u32)> = vec![];
        for token in tokens {
            // TODO better error matching (NotFoundError should return Ok(vec![])) but some others should error.
            let res = self
                .frequencies_blockfile_reader
                .get_by_prefix(token.text.as_str())
                .await?;
            if res.len() == 0 {
                return Ok(vec![]);
            }
            if res.len() > 1 {
                return Err(Box::new(FullTextIndexError::MultipleTokenFrequencies));
            }
            let res = res[0];
            // Throw away the "value" since we store frequencies in the keys.
            token_frequencies.push((token.text.to_string(), res.1));
        }
        // TODO sort by frequency. This adds an additional layer of complexity
        // with repeat characters where we need to keep track of which positions
        // for the character have been seen/used in the matching algorithm. By
        // leaving them ordered per the query, we can stick to the more straightforward
        // but less efficient matching algorithm.
        // token_frequencies.sort_by(|a, b| a.1.cmp(&b.1));

        // Populate initial candidates with the least-frequent token's posting list.
        // doc ID -> possible starting locations for the query.
        let mut candidates: HashMap<u32, Vec<i32>> = HashMap::new();
        let first_token = token_frequencies[0].0.as_str();
        let first_token_offset = tokens[0].offset_from as i32;
        let first_token_positional_posting_list = self
            .posting_lists_blockfile_reader
            .get_by_prefix(first_token)
            .await
            .unwrap();
        for (_, doc_id, positions) in first_token_positional_posting_list.iter() {
            let positions_vec: Vec<i32> = positions
                .iter()
                .map(|x| x.unwrap() - first_token_offset)
                .collect();
            candidates.insert(*doc_id, positions_vec);
        }

        // Iterate through the rest of the tokens, intersecting the posting lists with the candidates.
        let mut token_offset = 0;
        for (token, _) in token_frequencies[1..].iter() {
            token_offset += 1;
            let positional_posting_list = self
                .posting_lists_blockfile_reader
                .get_by_prefix(token.as_str())
                .await
                .unwrap();
            // TODO once we sort by frequency, we need to find the token position
            // here, taking into account which positions for repeats of the same
            // token have already been used up.
            // let token_offset = tokens
            //     .iter()
            //     .find(|t| t.text == *token)
            //     .unwrap()
            //     .offset_from as i32;
            let mut new_candidates: HashMap<u32, Vec<i32>> = HashMap::new();
            for (doc_id, positions) in candidates.iter() {
                let mut new_positions = vec![];
                for position in positions {
                    if let Some(positions) =
                        // Find the positional posting list with second item = to doc_id
                        positional_posting_list
                            .iter()
                            .find(|x| x.1 == *doc_id)
                            .map(|x| &x.2)
                    {
                        for pos in positions.iter() {
                            match pos {
                                None => {
                                    // This should never happen since we only store positions for the doc_id
                                    // in the positional posting list.
                                    return Err(Box::new(
                                        FullTextIndexError::EmptyValueInPositionalPostingList,
                                    ));
                                }
                                Some(pos) => {
                                    if pos == position + token_offset {
                                        new_positions.push(*position);
                                    }
                                }
                            }
                        }
                    }
                }
                if !new_positions.is_empty() {
                    new_candidates.insert((*doc_id) as u32, new_positions);
                }
            }
            if new_candidates.is_empty() {
                return Ok(vec![]);
            }
            candidates = new_candidates;
        }

        let mut results = vec![];
        for (doc_id, _) in candidates.drain() {
            results.push(doc_id as i32);
        }
        return Ok(results);
    }

    // We use this to implement deletes in the Writer. A delete() is implemented
    // by copying all the data from the old blockfile to a new one but skipping
    // the deleted offset id.
    async fn get_all_results_for_token(
        &self,
        token: &str,
    ) -> Result<Vec<(u32, Vec<i32>)>, Box<dyn ChromaError>> {
        let positional_posting_list = self
            .posting_lists_blockfile_reader
            .get_by_prefix(token)
            .await?;
        let mut results = vec![];
        for (_, doc_id, positions) in positional_posting_list.iter() {
            let positions_vec: Vec<i32> = positions.iter().map(|x| x.unwrap()).collect();
            results.push((*doc_id, positions_vec));
        }
        Ok(results)
    }

    // Also used to implement deletes in the Writer. When we delete a document,
    // we have to decrement the frequencies of all its tokens.
    async fn get_frequencies_for_token(&self, token: &str) -> Result<u32, Box<dyn ChromaError>> {
        let res = self
            .frequencies_blockfile_reader
            .get_by_prefix(token)
            .await?;
        if res.len() == 0 {
            return Ok(0);
        }
        if res.len() > 1 {
            return Err(Box::new(FullTextIndexError::MultipleTokenFrequencies));
        }
        Ok(res[0].1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockstore::provider::BlockfileProvider;
    use crate::index::fulltext::tokenizer::TantivyChromaTokenizer;
    use tantivy::tokenizer::NgramTokenizer;

    #[test]
    fn test_new_writer() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let _index =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
    }

    #[tokio::test]
    async fn test_new_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_id = freq_blockfile_writer.id();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let _ = FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);
    }

    #[tokio::test]
    async fn test_index_and_search_single_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, vec![1]);

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, vec![1]);

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, vec![1]);
    }

    #[tokio::test]
    async fn test_repeating_character_in_query() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("helo", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_query_of_repeating_character() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("aaa", 1).await.unwrap();
        index_writer.add_document("aaaaa", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("aaaa").await.unwrap();
        assert_eq!(res, vec![2]);
    }

    #[tokio::test]
    async fn test_repeating_character_in_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("helo").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_search_absent_token() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("chroma").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_multiple_candidates_within_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer
            .add_document("hello world hello", 1)
            .await
            .unwrap();
        index_writer.add_document("    hello ", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let mut res = index_reader.search("hello").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1, 2]);

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, vec![1]);
    }

    #[tokio::test]
    async fn test_multiple_simple_documents() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let mut res = index_reader.search("hello").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1, 2]);

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, vec![1]);
    }

    #[tokio::test]
    async fn test_multiple_complex_documents() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();
        index_writer.add_document("world hello", 4).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let mut res = index_reader.search("hello").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1, 2, 4]);

        let mut res = index_reader.search("world").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1, 3, 4]);

        let mut res = index_reader.search("hello world").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1]);

        let mut res = index_reader.search("world hello").await.unwrap();
        res.sort();
        assert_eq!(res, vec![4]);
    }

    #[tokio::test]
    async fn test_index_multiple_character_repeating() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("aaa", 1).await.unwrap();
        index_writer.add_document("aaaa", 2).await.unwrap();
        index_writer.add_document("bbb", 3).await.unwrap();
        index_writer.add_document("aaabbb", 4).await.unwrap();
        index_writer
            .add_document("aabbbbaaaaabbb", 5)
            .await
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let mut res = index_reader.search("aaa").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1, 2, 4, 5]);

        let mut res = index_reader.search("bbb").await.unwrap();
        res.sort();
        assert_eq!(res, vec![3, 4, 5]);

        let mut res = index_reader.search("aaabbb").await.unwrap();
        res.sort();
        assert_eq!(res, vec![4, 5]);
    }

    #[tokio::test]
    async fn test_index_special_characters() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("!!!!!", 1).await.unwrap();
        index_writer
            .add_document("hello world!!!", 2)
            .await
            .unwrap();
        index_writer.add_document(".!.!.!", 3).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("!!!!!").await.unwrap();
        assert_eq!(res, vec![1]);

        let mut res = index_reader.search("!!!").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1, 2]);

        let res = index_reader.search(".!.").await.unwrap();
        assert_eq!(res, vec![3]);
    }

    #[tokio::test]
    async fn test_get_frequencies_for_token() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);

        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.get_frequencies_for_token("h").await.unwrap();
        assert_eq!(res, 2);

        let res = index_reader.get_frequencies_for_token("e").await.unwrap();
        assert_eq!(res, 2);

        let res = index_reader.get_frequencies_for_token("l").await.unwrap();
        assert_eq!(res, 6);
    }

    #[tokio::test]
    async fn test_get_all_results_for_token() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);

        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.get_all_results_for_token("h").await.unwrap();
        assert_eq!(res.len(), 2);

        let res = index_reader.get_all_results_for_token("e").await.unwrap();
        assert_eq!(res.len(), 2);

        let res = index_reader.get_all_results_for_token("l").await.unwrap();
        assert_eq!(res.len(), 3);
    }

    #[tokio::test]
    async fn test_update_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);

        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();
        index_writer
            .update_document("world", "hello", 3)
            .await
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let mut res = index_reader.search("hello").await.unwrap();
        res.sort();
        assert_eq!(res, vec![1, 2, 3]);

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, vec![1]);
    }

    #[tokio::test]
    async fn test_delete_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, &Int32Array>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, &str>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);

        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();
        index_writer.delete_document("world", 3).await.unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, Int32Array>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(Box::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        )));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, vec![1]);
    }
}
