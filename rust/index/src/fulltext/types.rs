use crate::fulltext::tokenizer::ChromaTokenizer;
use crate::metadata::types::MetadataIndexError;
use crate::utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction};
use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::{BooleanOperator, WhereDocument, WhereDocumentOperator};
use futures::StreamExt;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use tantivy::tokenizer::Token;
use thiserror::Error;
use uuid::Uuid;

use super::tokenizer::ChromaTokenStream;

#[derive(Error, Debug)]
pub enum FullTextIndexError {
    #[error("Empty value in positional posting list")]
    EmptyValueInPositionalPostingList,
    #[error("Invariant violation")]
    InvariantViolation,
    #[error("Blockfile write error: {0}")]
    BlockfileWriteError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for FullTextIndexError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Debug, Default)]
pub struct UncommittedPostings {
    // token -> {doc -> [start positions]}
    positional_postings: HashMap<String, HashMap<u32, Vec<u32>>>,
    // (token, doc) pairs that should be deleted from storage.
    deleted_token_doc_pairs: HashSet<(String, i32)>,
}

impl UncommittedPostings {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Clone)]
pub struct FullTextIndexWriter<'me> {
    full_text_index_reader: Option<FullTextIndexReader<'me>>,
    posting_lists_blockfile_writer: BlockfileWriter,
    frequencies_blockfile_writer: BlockfileWriter,
    tokenizer: Arc<Box<dyn ChromaTokenizer>>,

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
            tokenizer: Arc::new(tokenizer),
            uncommitted_postings: Arc::new(tokio::sync::Mutex::new(UncommittedPostings::new())),
            uncommitted_frequencies: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }

    async fn populate_frequencies_and_posting_lists_from_previous_version(
        &self,
        tokens: &[Token],
    ) -> Result<(), FullTextIndexError> {
        // (Scoped to limit the lifetime of the lock)
        {
            let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
            for token in tokens {
                if uncommitted_frequencies.contains_key(&token.text) {
                    continue;
                }

                let frequency = match &self.full_text_index_reader {
                    // Readers are uninitialized until the first compaction finishes
                    // so there is a case when this is none hence not an error.
                    None => 0,
                    Some(reader) => (reader.get_frequencies_for_token(token.text.as_str()).await)
                        .unwrap_or_default(),
                };
                uncommitted_frequencies
                    .insert(token.text.clone(), (frequency as i32, frequency as i32));
            }
        }

        let mut uncommitted_postings = self.uncommitted_postings.lock().await;
        for token in tokens {
            if uncommitted_postings
                .positional_postings
                .contains_key(&token.text)
            {
                continue;
            }

            let results = match &self.full_text_index_reader {
                // Readers are uninitialized until the first compaction finishes
                // so there is a case when this is none hence not an error.
                None => vec![],
                Some(reader) => {
                    (reader.get_all_results_for_token(&token.text).await).unwrap_or_default()
                }
            };
            let mut doc_and_positions = HashMap::new();
            for result in results {
                doc_and_positions.insert(result.0, result.1);
            }
            uncommitted_postings
                .positional_postings
                .insert(token.text.clone(), doc_and_positions);
        }
        Ok(())
    }

    pub fn encode_tokens(&self, document: &str) -> Box<dyn ChromaTokenStream> {
        self.tokenizer.encode(document)
    }

    pub async fn add_document(
        &self,
        document: &str,
        offset_id: u32,
    ) -> Result<(), FullTextIndexError> {
        let tokens = self.encode_tokens(document);
        let tokens = tokens.get_tokens();
        self.populate_frequencies_and_posting_lists_from_previous_version(tokens)
            .await?;
        let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
        let mut uncommitted_postings = self.uncommitted_postings.lock().await;

        for token in tokens {
            // The entry should always exist because self.populate_frequencies_and_posting_lists_from_previous_version
            // will have created it if this token is new to the system.
            uncommitted_frequencies
                .entry(token.text.clone())
                .and_modify(|e| e.0 += 1);

            // For a new token, the uncommitted list will not contain any entry so insert
            // an empty builder in that case.
            let builder = uncommitted_postings
                .positional_postings
                .entry(token.text.to_string())
                .or_insert(HashMap::new());

            // Store starting positions of tokens. These are NOT affected by token filters.
            // For search, we can use the start and end positions to compute offsets to
            // check full string match.
            //
            // See https://docs.rs/tantivy/latest/tantivy/tokenizer/struct.Token.html
            match builder.entry(offset_id) {
                Entry::Vacant(v) => {
                    v.insert(vec![token.offset_from as u32]);
                }
                Entry::Occupied(mut o) => {
                    o.get_mut().push(token.offset_from as u32);
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
        let tokens = tokens.get_tokens();

        self.populate_frequencies_and_posting_lists_from_previous_version(tokens)
            .await?;
        let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
        let mut uncommitted_postings = self.uncommitted_postings.lock().await;

        for token in tokens {
            match uncommitted_frequencies.get_mut(token.text.as_str()) {
                Some(frequency) => {
                    frequency.0 -= 1;
                }
                None => {
                    // Invariant violation -- we just populated this.
                    tracing::error!("Error decrementing frequency for token: {:?}", token.text);
                    return Err(FullTextIndexError::InvariantViolation);
                }
            }
            if let Some(builder) = uncommitted_postings
                .positional_postings
                .get_mut(token.text.as_str())
            {
                builder.remove(&offset_id);
                if builder.is_empty() {
                    uncommitted_postings
                        .positional_postings
                        .remove(token.text.as_str());
                }
                // Track all the deleted (token, doc) pairs. This is needed
                // to remove the old postings list for this pair from storage.
                uncommitted_postings
                    .deleted_token_doc_pairs
                    .insert((token.text.clone(), offset_id as i32));
            }
            // This is fine since we delete all the positions of a token
            // of a document at once so the next time we encounter this token
            // (at a different position) the map could be empty.
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
        self.add_document(new_document, offset_id).await?;
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
                .delete::<u32, Vec<u32>>(token.as_str(), offset_id as u32)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    return Err(FullTextIndexError::BlockfileWriteError(e));
                }
            }
        }

        for (key, mut value) in uncommitted_postings.positional_postings.drain() {
            for (doc_id, positions) in value.drain() {
                // Don't add if postings list is empty for this (token, doc) combo.
                // This can happen with deletes.
                if !positions.is_empty() {
                    match self
                        .posting_lists_blockfile_writer
                        .set(key.as_str(), doc_id, positions)
                        .await
                    {
                        Ok(_) => {}
                        Err(e) => {
                            return Err(FullTextIndexError::BlockfileWriteError(e));
                        }
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
            .commit::<u32, Vec<u32>>()?;
        let frequencies_blockfile_flusher =
            self.frequencies_blockfile_writer.commit::<u32, String>()?;
        Ok(FullTextIndexFlusher {
            posting_lists_blockfile_flusher,
            frequencies_blockfile_flusher,
        })
    }
}

pub struct FullTextIndexFlusher {
    posting_lists_blockfile_flusher: BlockfileFlusher,
    frequencies_blockfile_flusher: BlockfileFlusher,
}

impl FullTextIndexFlusher {
    pub async fn flush(self) -> Result<(), FullTextIndexError> {
        match self
            .posting_lists_blockfile_flusher
            .flush::<u32, Vec<u32>>()
            .await
        {
            Ok(_) => {}
            Err(e) => {
                return Err(FullTextIndexError::BlockfileWriteError(e));
            }
        };
        match self
            .frequencies_blockfile_flusher
            .flush::<u32, String>()
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
pub struct FullTextIndexReader<'me> {
    posting_lists_blockfile_reader: BlockfileReader<'me, u32, &'me [u32]>,
    frequencies_blockfile_reader: BlockfileReader<'me, u32, u32>,
    tokenizer: Arc<Box<dyn ChromaTokenizer>>,
}

impl<'me> FullTextIndexReader<'me> {
    pub fn new(
        posting_lists_blockfile_reader: BlockfileReader<'me, u32, &'me [u32]>,
        frequencies_blockfile_reader: BlockfileReader<'me, u32, u32>,
        tokenizer: Box<dyn ChromaTokenizer>,
    ) -> Self {
        FullTextIndexReader {
            posting_lists_blockfile_reader,
            frequencies_blockfile_reader,
            tokenizer: Arc::new(tokenizer),
        }
    }

    pub fn encode_tokens(&self, document: &str) -> Box<dyn ChromaTokenStream> {
        self.tokenizer.encode(document)
    }

    pub async fn search(&self, query: &str) -> Result<Vec<i32>, FullTextIndexError> {
        let binding = self.encode_tokens(query);
        let tokens = binding.get_tokens();

        if tokens.is_empty() {
            return Ok(vec![]);
        }

        // Retrieve posting lists for each token.
        let posting_lists = futures::stream::iter(tokens)
            .then(|token| async {
                let positional_posting_list = self
                    .posting_lists_blockfile_reader
                    .get_by_prefix(token.text.as_str())
                    .await?;
                Ok::<_, FullTextIndexError>(positional_posting_list)
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let num_tokens = posting_lists.len();
        let mut pointers = vec![0; num_tokens];
        let mut results = Vec::new();

        loop {
            // Get current doc_ids from each posting list (aka for each token).
            let current_doc_ids: Vec<Option<u32>> = posting_lists
                .iter()
                .enumerate()
                .map(|(i, posting_list)| {
                    if pointers[i] < posting_list.len() {
                        Some(posting_list[pointers[i]].1)
                    } else {
                        None
                    }
                })
                .collect();

            // If any list is exhausted, we're done.
            if current_doc_ids.contains(&None) {
                break;
            }

            // Check if all doc_ids are the same.
            let min_doc_id = current_doc_ids.iter().filter_map(|&id| id).min().unwrap();
            let max_doc_id = current_doc_ids.iter().filter_map(|&id| id).max().unwrap();

            if min_doc_id == max_doc_id {
                // All tokens appear in the same document, so check positional alignment.
                let mut positions_per_posting_list = Vec::with_capacity(num_tokens);
                for (i, posting_list) in posting_lists.iter().enumerate() {
                    let (_, _, positions) = posting_list[pointers[i]];
                    positions_per_posting_list.push(positions);
                }

                // Adjust positions and check for sequential alignment.
                // Imagine you're searching for "brown fox" over the document "the quick brown fox".
                // The positions for "brown" are {2} and for "fox" are {3}. The adjusted positions after subtracting the token's position in the query are {2} for "brown" and 3 - 1 = {2} for "fox".
                // The intersection of these two sets is non-empty, so we know that the two tokens are adjacent.

                // Seed with the positions of the first token.
                let mut adjusted_positions = positions_per_posting_list[0]
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>();

                for (offset, positions_set) in positions_per_posting_list.iter().enumerate().skip(1)
                {
                    let positions_set = positions_set
                        .iter()
                        // (We can discard any positions that the token appears at before the current offset)
                        .filter_map(|&p| p.checked_sub(offset as u32))
                        .collect::<HashSet<_>>();
                    adjusted_positions = &adjusted_positions & &positions_set;

                    if adjusted_positions.is_empty() {
                        break;
                    }
                }

                // All tokens are sequential
                if !adjusted_positions.is_empty() {
                    results.push(min_doc_id as i32);
                }

                // Advance all pointers.
                for pointer in pointers.iter_mut() {
                    *pointer += 1;
                }
            } else {
                // Advance pointers of lists with the minimum doc_id.
                for i in 0..num_tokens {
                    if let Some(doc_id) = current_doc_ids[i] {
                        if doc_id == min_doc_id {
                            pointers[i] += 1;
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    // We use this to implement deletes in the Writer. A delete() is implemented
    // by copying all the data from the old blockfile to a new one but skipping
    // the deleted offset id.
    async fn get_all_results_for_token(
        &self,
        token: &str,
    ) -> Result<Vec<(u32, Vec<u32>)>, FullTextIndexError> {
        let positional_posting_list = self
            .posting_lists_blockfile_reader
            .get_by_prefix(token)
            .await?;
        let mut results = vec![];
        for (_, doc_id, positions) in positional_posting_list.iter() {
            results.push((*doc_id, positions.to_vec()));
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
        if res.is_empty() {
            return Ok(0);
        }
        if res.len() > 1 {
            panic!("Invariant violation. Multiple frequency values found for a token.");
        }
        Ok(res[0].1)
    }
}

pub fn process_where_document_clause_with_callback<
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
                    process_where_document_clause_with_callback(child, callback)
                        .unwrap_or_default();
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
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::tokenizer::TantivyChromaTokenizer;
    use chroma_blockstore::provider::BlockfileProvider;
    use tantivy::tokenizer::NgramTokenizer;

    #[test]
    fn test_new_writer() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let _index =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
    }

    #[tokio::test]
    async fn test_new_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_id = freq_blockfile_writer.id();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let _ = FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);
    }

    #[tokio::test]
    async fn test_index_and_search_single_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("helo", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_query_of_repeating_character() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("aaa", 1).await.unwrap();
        index_writer.add_document("aaaaa", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("aaaa").await.unwrap();
        assert_eq!(res, vec![2]);
    }

    #[tokio::test]
    async fn test_repeating_character_in_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("helo").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_search_absent_token() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("chroma").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_multiple_candidates_within_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.open::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let index_reader =
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
        let pl_blockfile_writer = provider.create::<u32, Vec<u32>>().unwrap();
        let freq_blockfile_writer = provider.create::<u32, String>().unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
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
            .open::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = Box::new(TantivyChromaTokenizer::new(
            NgramTokenizer::new(1, 1, false).unwrap(),
        ));
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, vec![1]);
    }
}
