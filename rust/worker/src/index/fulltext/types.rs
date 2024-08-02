use crate::index::fulltext::tokenizer::ChromaTokenizer;
use crate::index::metadata::types::MetadataIndexError;
use crate::utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction};
use arrow::array::Int32Array;
use chroma_blockstore::positional_posting_list_value::{
    PositionalPostingListBuilder, PositionalPostingListBuilderError,
};
use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{BooleanOperator, WhereDocument, WhereDocumentOperator};
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

use super::tokenizer::ChromaTokenStream;

#[derive(Error, Debug)]
pub enum FullTextIndexError {
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

#[derive(Debug)]
pub(crate) struct UncommittedPostings {
    // token -> {doc -> [start positions]}
    positional_postings: HashMap<String, PositionalPostingListBuilder>,
    // (token, doc) pairs that should be deleted from storage.
    deleted_token_doc_pairs: HashSet<(String, i32)>,
}

impl UncommittedPostings {
    pub(crate) fn new() -> Self {
        Self {
            positional_postings: HashMap::new(),
            deleted_token_doc_pairs: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct FullTextIndexWriter<'me> {
    full_text_index_reader: Option<FullTextIndexReader<'me>>,
    posting_lists_blockfile_writer: BlockfileWriter,
    frequencies_blockfile_writer: BlockfileWriter,
    tokenizer: Arc<Mutex<Box<dyn ChromaTokenizer>>>,

    // TODO(Sanket): Move off this tokio::sync::mutex and use
    // a lightweight lock instead. This is needed currently to
    // keep holding the lock across an await point.
    // term -> positional posting list builder for that term
    uncommitted_postings: Arc<tokio::sync::Mutex<UncommittedPostings>>,
    // TODO(Sanket): Move off this tokio::sync::mutex and use
    // a lightweight lock instead. This is needed currently to
    // keep holding the lock across an await point.
    // Value of this map is a tuple (old freq and new freq)
    // because we also need to keep the old frequency
    // around. The reason is (token, freq) is the key in the blockfile hence
    // when freq changes, we need to delete the old (token, freq) key.
    uncommitted_frequencies: Arc<tokio::sync::Mutex<HashMap<String, (i32, i32)>>>,
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
            uncommitted_postings: Arc::new(tokio::sync::Mutex::new(UncommittedPostings::new())),
            uncommitted_frequencies: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn populate_frequencies_and_posting_lists_from_previous_version(
        &self,
        token: &str,
    ) -> Result<(), FullTextIndexError> {
        let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
        match uncommitted_frequencies.get(token) {
            Some(_) => return Ok(()),
            None => {
                let frequency = match &self.full_text_index_reader {
                    // Readers are uninitialized until the first compaction finishes
                    // so there is a case when this is none hence not an error.
                    None => 0,
                    Some(reader) => match reader.get_frequencies_for_token(token).await {
                        Ok(frequency) => frequency,
                        // New token so start with frequency of 0.
                        Err(_) => 0,
                    },
                };
                uncommitted_frequencies
                    .insert(token.to_string(), (frequency as i32, frequency as i32));
            }
        }
        let mut uncommitted_postings = self.uncommitted_postings.lock().await;
        match uncommitted_postings.positional_postings.get(token) {
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
                    // Readers are uninitialized until the first compaction finishes
                    // so there is a case when this is none hence not an error.
                    None => vec![],
                    Some(reader) => match reader.get_all_results_for_token(token).await {
                        Ok(results) => results,
                        // New token so start with empty postings list.
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
                uncommitted_postings
                    .positional_postings
                    .insert(token.to_string(), builder);
            }
        }
        Ok(())
    }

    pub fn encode_tokens(&self, document: &str) -> Box<dyn ChromaTokenStream> {
        let tokenizer = self.tokenizer.lock();
        let tokens = tokenizer.encode(document);
        tokens
    }

    pub async fn add_document(
        &self,
        document: &str,
        offset_id: i32,
    ) -> Result<(), FullTextIndexError> {
        let tokens = self.encode_tokens(document);
        for token in tokens.get_tokens() {
            self.populate_frequencies_and_posting_lists_from_previous_version(token.text.as_str())
                .await?;
            let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
            // The entry should always exist because self.populate_frequencies_and_posting_lists_from_previous_version
            // will have created it if this token is new to the system.
            uncommitted_frequencies
                .entry(token.text.to_string())
                .and_modify(|e| (*e).0 += 1);
            let mut uncommitted_postings = self.uncommitted_postings.lock().await;
            // For a new token, the uncommitted list will not contain any entry so insert
            // an empty builder in that case.
            let builder = uncommitted_postings
                .positional_postings
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
        let tokens = self.encode_tokens(document);
        for token in tokens.get_tokens() {
            self.populate_frequencies_and_posting_lists_from_previous_version(token.text.as_str())
                .await?;
            let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
            match uncommitted_frequencies.get_mut(token.text.as_str()) {
                Some(frequency) => {
                    (*frequency).0 -= 1;
                }
                None => {
                    // Invariant violation -- we just populated this.
                    tracing::error!("Error decrementing frequency for token: {:?}", token.text);
                    return Err(FullTextIndexError::InvariantViolation);
                }
            }
            let mut uncommitted_postings = self.uncommitted_postings.lock().await;
            match uncommitted_postings
                .positional_postings
                .get_mut(token.text.as_str())
            {
                Some(builder) => match builder.delete_doc_id(offset_id as i32) {
                    Ok(_) => {
                        // Track all the deleted (token, doc) pairs. This is needed
                        // to remove the old postings list for this pair from storage.
                        uncommitted_postings
                            .deleted_token_doc_pairs
                            .insert((token.text.clone(), offset_id as i32));
                    }
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
        let mut uncommitted_postings = self.uncommitted_postings.lock().await;
        // Delete (token, doc) pairs from blockfile first. Note that the ordering is
        // important here i.e. we need to delete before inserting the new postings
        // list otherwise we could incorrectly delete posting lists that shouldn't be deleted.
        for (token, offset_id) in uncommitted_postings.deleted_token_doc_pairs.drain() {
            match self
                .posting_lists_blockfile_writer
                .delete::<u32, &Int32Array>(token.as_str(), offset_id as u32)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    return Err(FullTextIndexError::BlockfileWriteError(e));
                }
            }
        }
        for (key, mut value) in uncommitted_postings.positional_postings.drain() {
            let built_list = value.build();
            for doc_id in built_list.doc_ids.iter() {
                match doc_id {
                    Some(doc_id) => {
                        let positional_posting_list =
                            built_list.get_positions_for_doc_id(doc_id).unwrap();
                        // Don't add if postings list is empty for this (token, doc) combo.
                        // This can happen with deletes.
                        if positional_posting_list.len() > 0 {
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
                    }
                    None => {
                        panic!("Positions for doc ID not found in positional posting list -- should never happen")
                    }
                }
            }
        }
        let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
        for (key, value) in uncommitted_frequencies.drain() {
            // Delete only if the token existed previously.
            if value.1 > 0 {
                // Delete the old frequency.
                match self
                    .frequencies_blockfile_writer
                    .delete::<u32, u32>(key.as_str(), value.1 as u32)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(FullTextIndexError::BlockfileWriteError(e));
                    }
                }
            }
            // Insert the new frequency.
            // Add only if the frequency is not zero. This can happen in case of document
            // deletes.
            // TODO we just have token -> frequency here. Should frequency be the key or should we use an empty key and make it the value?
            if value.0 > 0 {
                match self
                    .frequencies_blockfile_writer
                    .set(key.as_str(), value.0 as u32, 0)
                    .await
                {
                    Ok(_) => {}
                    Err(e) => {
                        return Err(FullTextIndexError::BlockfileWriteError(e));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn commit(self) -> Result<FullTextIndexFlusher, FullTextIndexError> {
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
    pub async fn flush(self) -> Result<(), FullTextIndexError> {
        match self
            .posting_lists_blockfile_flusher
            .flush::<u32, &Int32Array>()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(FullTextIndexError::BlockfileWriteError(e));
            }
        };
        match self
            .frequencies_blockfile_flusher
            .flush::<u32, &str>()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(FullTextIndexError::BlockfileWriteError(e));
            }
        };
        Ok(())
    }

    pub fn pls_id(&self) -> Uuid {
        self.posting_lists_blockfile_flusher.id()
    }

    pub fn freqs_id(&self) -> Uuid {
        self.frequencies_blockfile_flusher.id()
    }
}

#[derive(Clone)]
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

    pub fn encode_tokens(&self, document: &str) -> Box<dyn ChromaTokenStream> {
        let tokenizer = self.tokenizer.lock();
        let tokens = tokenizer.encode(document);
        tokens
    }

    pub async fn search(&self, query: &str) -> Result<Vec<i32>, FullTextIndexError> {
        let binding = self.encode_tokens(query);
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
                panic!("Invariant violation. Multiple frequency values found for a token.");
            }
            let res = res[0];
            if res.1 <= 0 {
                panic!("Invariant violation. Zero frequency token found.");
            }
            // Throw away the "value" since we store frequencies in the keys.
            token_frequencies.push((token.text.to_string(), res.1));
        }

        if token_frequencies.len() == 0 {
            return Ok(vec![]);
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
        let first_token_positional_posting_list = self
            .posting_lists_blockfile_reader
            .get_by_prefix(first_token)
            .await
            .unwrap();
        for (_, doc_id, positions) in first_token_positional_posting_list.iter() {
            let positions_vec: Vec<i32> = positions.iter().map(|x| x.unwrap()).collect();
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
                                    return Err(
                                        FullTextIndexError::EmptyValueInPositionalPostingList,
                                    );
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
    ) -> Result<Vec<(u32, Vec<i32>)>, FullTextIndexError> {
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
    async fn get_frequencies_for_token(&self, token: &str) -> Result<u32, FullTextIndexError> {
        let res = self
            .frequencies_blockfile_reader
            .get_by_prefix(token)
            .await?;
        if res.len() == 0 {
            return Ok(0);
        }
        if res.len() > 1 {
            panic!("Invariant violation. Multiple frequency values found for a token.");
        }
        Ok(res[0].1)
    }
}

pub(crate) fn process_where_document_clause_with_callback<
    F: Fn(&str, WhereDocumentOperator) -> Vec<i32>,
>(
    where_document_clause: &WhereDocument,
    callback: &F,
) -> Result<Vec<usize>, MetadataIndexError> {
    let mut results = vec![];
    match where_document_clause {
        WhereDocument::DirectWhereDocumentComparison(direct_document_comparison) => {
            match &direct_document_comparison.operator {
                WhereDocumentOperator::Contains => {
                    let result = callback(
                        &direct_document_comparison.document,
                        WhereDocumentOperator::Contains,
                    );
                    results = result.iter().map(|x| *x as usize).collect();
                }
                WhereDocumentOperator::NotContains => {
                    todo!();
                }
            }
        }
        WhereDocument::WhereDocumentChildren(where_document_children) => {
            let mut first_iteration = true;
            for child in where_document_children.children.iter() {
                let child_results: Vec<usize> =
                    match process_where_document_clause_with_callback(&child, callback) {
                        Ok(result) => result,
                        Err(_) => vec![],
                    };
                if first_iteration {
                    results = child_results;
                    first_iteration = false;
                } else {
                    match where_document_children.operator {
                        BooleanOperator::And => {
                            results = merge_sorted_vecs_conjunction(&results, &child_results);
                        }
                        BooleanOperator::Or => {
                            results = merge_sorted_vecs_disjunction(&results, &child_results);
                        }
                    }
                }
            }
        }
    }
    results.sort();
    return Ok(results);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::fulltext::tokenizer::TantivyChromaTokenizer;
    use chroma_blockstore::provider::BlockfileProvider;
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
