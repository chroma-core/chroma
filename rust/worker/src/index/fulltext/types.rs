use crate::blockstore::positional_posting_list_value::PositionalPostingListBuilder;
use crate::blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use crate::errors::{ChromaError, ErrorCodes};
use crate::index::fulltext::tokenizer::ChromaTokenizer;
use crate::index::metadata::types::MetadataIndexError;
use crate::types::{BooleanOperator, WhereDocument, WhereDocumentOperator};
use crate::utils::{merge_sorted_vecs_conjunction, merge_sorted_vecs_disjunction};

use arrow::array::Int32Array;
use parking_lot::Mutex;
use roaring::RoaringBitmap;
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
}

impl ChromaError for FullTextIndexError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

pub(crate) struct FullTextIndexWriter {
    posting_lists_blockfile_writer: BlockfileWriter,
    frequencies_blockfile_writer: BlockfileWriter,
    // This is a crime.
    tokenizer: Arc<Mutex<Box<dyn ChromaTokenizer>>>,

    // term -> positional posting list builder for that term
    uncommitted: Arc<Mutex<HashMap<String, PositionalPostingListBuilder>>>,
    uncommitted_frequencies: Arc<Mutex<HashMap<String, i32>>>,
}

impl FullTextIndexWriter {
    pub fn new(
        posting_lists_blockfile_writer: BlockfileWriter,
        frequencies_blockfile_writer: BlockfileWriter,
        tokenizer: Box<dyn ChromaTokenizer>,
    ) -> Self {
        FullTextIndexWriter {
            posting_lists_blockfile_writer,
            frequencies_blockfile_writer,
            tokenizer: Arc::new(Mutex::new(tokenizer)),
            uncommitted: Arc::new(Mutex::new(HashMap::new())),
            uncommitted_frequencies: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn add_document(&self, document: &str, offset_id: i32) -> Result<(), Box<dyn ChromaError>> {
        let tokenizer = self.tokenizer.lock();
        let tokens = tokenizer.encode(document);
        for token in tokens.get_tokens() {
            let mut uncommitted_frequencies = self.uncommitted_frequencies.lock();
            uncommitted_frequencies
                .entry(token.text.to_string())
                .and_modify(|e| *e += 1)
                .or_insert(1);
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
                let res =
                    builder.add_doc_id_and_positions(offset_id, vec![token.offset_from as i32]);
                if res.is_err() {
                    return res;
                }
            } else {
                let res =
                    builder.add_positions_for_doc_id(offset_id, vec![token.offset_from as i32]);
                if res.is_err() {
                    return res;
                }
            }
        }
        Ok(())
    }

    pub async fn write_to_blockfiles(&mut self) -> Result<(), Box<dyn ChromaError>> {
        let mut uncommitted = self.uncommitted.lock();
        for (key, mut value) in uncommitted.drain() {
            let built_list = value.build();
            for doc_id in built_list.doc_ids.iter() {
                match doc_id {
                    Some(doc_id) => {
                        let positional_posting_list =
                            built_list.get_positions_for_doc_id(doc_id).unwrap();
                        let res = self
                            .posting_lists_blockfile_writer
                            .set(key.as_str(), doc_id as u32, &positional_posting_list)
                            .await;
                        if res.is_err() {
                            return res;
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
            let res = self
                .frequencies_blockfile_writer
                .set(key.as_str(), value as u32, 0)
                .await;
            if res.is_err() {
                return res;
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
}

pub(crate) fn process_where_document_clause<F: Fn(&str, WhereDocumentOperator) -> Vec<i32>>(
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
                    match process_where_document_clause(&child, callback) {
                        Ok(result) => result,
                        Err(_) => vec![],
                    };
                if first_iteration {
                    results = child_results;
                    first_iteration = false;
                } else {
                    match where_document_children.operator {
                        BooleanOperator::And => {
                            results = merge_sorted_vecs_conjunction(results, child_results);
                        }
                        BooleanOperator::Or => {
                            results = merge_sorted_vecs_disjunction(results, child_results);
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("helo", 1).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("aaa", 1).unwrap();
        index_writer.add_document("aaaaa", 2).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello", 1).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world hello", 1).unwrap();
        index_writer.add_document("    hello ", 2).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).unwrap();
        index_writer.add_document("hello", 2).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).unwrap();
        index_writer.add_document("hello", 2).unwrap();
        index_writer.add_document("world", 3).unwrap();
        index_writer.add_document("world hello", 4).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("aaa", 1).unwrap();
        index_writer.add_document("aaaa", 2).unwrap();
        index_writer.add_document("bbb", 3).unwrap();
        index_writer.add_document("aaabbb", 4).unwrap();
        index_writer.add_document("aabbbbaaaaabbb", 5).unwrap();
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
            FullTextIndexWriter::new(pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("!!!!!", 1).unwrap();
        index_writer.add_document("hello world!!!", 2).unwrap();
        index_writer.add_document(".!.!.!", 3).unwrap();
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
}
