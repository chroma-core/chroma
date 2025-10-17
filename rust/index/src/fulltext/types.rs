use super::util::TokenInstance;
use super::util::TokenInstanceEncodeError;
use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use chroma_types::regex::literal_expr::NgramLiteralProvider;
use futures::StreamExt;
use itertools::Itertools;
use parking_lot::Mutex;
use roaring::RoaringBitmap;
use std::collections::HashSet;
use std::sync::Arc;
use tantivy::tokenizer::NgramTokenizer;
use tantivy::tokenizer::TokenStream;
use tantivy::tokenizer::Tokenizer;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum FullTextIndexError {
    #[error("Empty value in positional posting list")]
    EmptyValueInPositionalPostingList,
    #[error("Invariant violation")]
    InvariantViolation,
    #[error("Blockfile error: {0}")]
    BlockfileError(#[from] Box<dyn ChromaError>),
}

impl ChromaError for FullTextIndexError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[derive(Debug, Clone, Copy)]
pub enum DocumentMutation<'text> {
    Create {
        offset_id: u32,
        new_document: &'text str,
    },
    Update {
        offset_id: u32,
        old_document: &'text str,
        new_document: &'text str,
    },
    Delete {
        offset_id: u32,
        old_document: &'text str,
    },
}

#[derive(Clone)]
pub struct FullTextIndexWriter {
    tokenizer: NgramTokenizer,
    /// Deletes for a given trigram/offset ID pair are represented by a `None` position on the token instance.
    token_instances: Arc<Mutex<Vec<Vec<TokenInstance>>>>,
    posting_lists_blockfile_writer: BlockfileWriter,
}

impl FullTextIndexWriter {
    pub fn new(posting_lists_blockfile_writer: BlockfileWriter, tokenizer: NgramTokenizer) -> Self {
        FullTextIndexWriter {
            tokenizer,
            posting_lists_blockfile_writer,
            token_instances: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Processes a batch of mutations to the full-text index
    /// This assumes that there will never be mutations with the same offset ID across all calls to `handle_batch()` for the lifetime of a `FullTextIndexWriter` struct.
    ///
    /// Recommended usage is running this in several threads at once, each over a chunk of mutations.
    ///
    /// Note that this is a blocking method and may take on the order of hundreds of milliseconds to complete (depending on the batch size) so be careful when calling this from an async context.
    pub fn handle_batch<'documents, M: IntoIterator<Item = DocumentMutation<'documents>>>(
        &self,
        mutations: M,
    ) -> Result<(), FullTextIndexError> {
        let mut token_instances = vec![];

        for mutation in mutations {
            match mutation {
                DocumentMutation::Create {
                    offset_id,
                    new_document,
                } => {
                    self.tokenizer
                        .clone()
                        .token_stream(new_document)
                        .process(&mut |token| {
                            match TokenInstance::encode(
                                token.text.as_str(),
                                offset_id,
                                Some(token.offset_from as u32),
                            ) {
                                Ok(encoded) => token_instances.push(encoded),
                                Err(TokenInstanceEncodeError::NullTerminator) => {
                                    // ignore
                                }
                            }
                        });
                }

                DocumentMutation::Update {
                    offset_id,
                    old_document,
                    new_document,
                } => {
                    if old_document == new_document {
                        // Don't need to do anything if document is identical
                        continue;
                    }

                    // Remove old version
                    let mut trigrams_to_delete = HashSet::new(); // (need to filter out duplicates, each trigram may appear multiple times in a document)
                    self.tokenizer
                        .clone()
                        .token_stream(old_document)
                        .process(&mut |token| {
                            match TokenInstance::encode(token.text.as_str(), offset_id, None) {
                                Ok(encoded) => {
                                    trigrams_to_delete.insert(encoded);
                                }
                                Err(TokenInstanceEncodeError::NullTerminator) => {
                                    // ignore
                                }
                            }
                        });

                    // Add doc
                    self.tokenizer
                        .clone()
                        .token_stream(new_document)
                        .process(&mut |token| {
                            match TokenInstance::encode(token.text.as_str(), offset_id, None) {
                                Ok(encoded) => {
                                    trigrams_to_delete.remove(&encoded);
                                }
                                Err(TokenInstanceEncodeError::NullTerminator) => {
                                    // ignore
                                }
                            }

                            match TokenInstance::encode(
                                token.text.as_str(),
                                offset_id,
                                Some(token.offset_from as u32),
                            ) {
                                Ok(encoded) => {
                                    token_instances.push(encoded);
                                }
                                Err(TokenInstanceEncodeError::NullTerminator) => {
                                    // ignore
                                }
                            }
                        });

                    token_instances.extend(trigrams_to_delete.into_iter());
                }

                DocumentMutation::Delete {
                    offset_id,
                    old_document,
                } => {
                    let mut trigrams_to_delete = HashSet::new(); // (need to filter out duplicates, each trigram may appear multiple times in a document)

                    // Delete doc
                    self.tokenizer
                        .clone()
                        .token_stream(old_document)
                        .process(&mut |token| {
                            match TokenInstance::encode(token.text.as_str(), offset_id, None) {
                                Ok(encoded) => {
                                    trigrams_to_delete.insert(encoded);
                                }
                                Err(TokenInstanceEncodeError::NullTerminator) => {
                                    // ignore
                                }
                            }
                        });

                    token_instances.extend(trigrams_to_delete.into_iter());
                }
            }
        }

        token_instances.sort_unstable();
        self.token_instances.lock().push(token_instances);

        Ok(())
    }

    pub async fn write_to_blockfiles(&mut self) -> Result<(), FullTextIndexError> {
        let mut last_key = TokenInstance::MAX;
        let mut posting_list: Vec<u32> = vec![];

        let token_instances = std::mem::take(&mut *self.token_instances.lock());

        for encoded_instance in token_instances.into_iter().kmerge() {
            match encoded_instance.get_position() {
                Some(offset) => {
                    let this_key = encoded_instance.omit_position();
                    if last_key != this_key {
                        if last_key != TokenInstance::MAX && !posting_list.is_empty() {
                            let token = last_key.get_token();
                            let document_id = last_key.get_offset_id();
                            self.posting_lists_blockfile_writer
                                .set(&token, document_id, posting_list.clone())
                                .await
                                .unwrap();
                            posting_list.clear();
                        }
                        last_key = this_key;
                    }

                    posting_list.push(offset);
                }
                None => {
                    if last_key != TokenInstance::MAX && !posting_list.is_empty() {
                        let token = last_key.get_token();
                        let document_id = last_key.get_offset_id();
                        self.posting_lists_blockfile_writer
                            .set(&token, document_id, posting_list.clone())
                            .await
                            .unwrap();
                        posting_list.clear();
                        last_key = encoded_instance.omit_position();
                    }

                    // Trigram & offset ID pair is a delete
                    self.posting_lists_blockfile_writer
                        .delete::<u32, Vec<u32>>(
                            &encoded_instance.get_token(),
                            encoded_instance.get_offset_id(),
                        )
                        .await
                        .unwrap();
                }
            }
        }

        if last_key != TokenInstance::MAX && !posting_list.is_empty() {
            let token = last_key.get_token();
            let document_id = last_key.get_offset_id();
            self.posting_lists_blockfile_writer
                .set(&token, document_id, posting_list.clone())
                .await
                .unwrap();
        }

        Ok(())
    }

    pub async fn commit(self) -> Result<FullTextIndexFlusher, FullTextIndexError> {
        let posting_lists_blockfile_flusher = self
            .posting_lists_blockfile_writer
            .commit::<u32, Vec<u32>>()
            .await?;
        Ok(FullTextIndexFlusher {
            posting_lists_blockfile_flusher,
        })
    }
}

pub struct FullTextIndexFlusher {
    posting_lists_blockfile_flusher: BlockfileFlusher,
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
                return Err(FullTextIndexError::BlockfileError(e));
            }
        };

        Ok(())
    }

    pub fn pls_id(&self) -> Uuid {
        self.posting_lists_blockfile_flusher.id()
    }

    pub fn prefix_path(&self) -> &str {
        self.posting_lists_blockfile_flusher.prefix_path()
    }
}

#[derive(Clone)]
pub struct FullTextIndexReader<'me> {
    posting_lists_blockfile_reader: BlockfileReader<'me, u32, &'me [u32]>,
    tokenizer: NgramTokenizer,
}

impl<'me> FullTextIndexReader<'me> {
    pub fn new(
        posting_lists_blockfile_reader: BlockfileReader<'me, u32, &'me [u32]>,
        tokenizer: NgramTokenizer,
    ) -> Self {
        FullTextIndexReader {
            posting_lists_blockfile_reader,
            tokenizer,
        }
    }

    pub async fn search(&self, query: &str) -> Result<RoaringBitmap, FullTextIndexError> {
        let mut tokens = vec![];
        self.tokenizer
            .clone()
            .token_stream(query)
            .process(&mut |token| {
                tokens.push(token.clone());
            });

        if tokens.is_empty() {
            return Ok(RoaringBitmap::new());
        }

        // Retrieve posting lists for each token.
        let posting_lists = futures::stream::iter(&tokens)
            .then(|token| async move {
                let positional_posting_list = self
                    .posting_lists_blockfile_reader
                    .get_prefix(token.text.as_str())
                    .await?
                    .collect::<Vec<_>>();
                Ok::<_, FullTextIndexError>(positional_posting_list)
            })
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        let num_tokens = posting_lists.len();
        let mut pointers = vec![0; num_tokens];
        let mut results = RoaringBitmap::new();

        loop {
            // Get current doc_ids from each posting list (aka for each token).
            let current_doc_ids: Vec<Option<u32>> = posting_lists
                .iter()
                .enumerate()
                .map(|(i, posting_list)| {
                    if pointers[i] < posting_list.len() {
                        Some(posting_list[pointers[i]].0)
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
                // Imagine you're searching for "brown fox" over the document "the quick brown fox".
                // The positions for "brown" are {2} and for "fox" are {3}. The adjusted positions after subtracting the token's position in the query are {2} for "brown" and 3 - 1 = {2} for "fox".
                // The intersection of these two sets is non-empty, so we know that the two tokens are adjacent.
                // In practice, we must adjust the positions by using the byte offset of the token rather than the token's position in the query, because Unicode characters can be variable-length and the posting lists are stored as byte offsets.
                // E.x. imagine we're using a 1-gram tokenizer and processing the string "ém". The token positions are {0, 1}, but the byte offsets are {0, 2} (because é is two bytes long).

                // Seed with the positions of the first token.
                let mut adjusted = posting_lists[0][pointers[0]]
                    .1
                    .iter()
                    .copied()
                    .collect::<HashSet<_>>();

                for i in 1..num_tokens {
                    let byte_delta_from_first_token =
                        tokens[i].offset_from as u32 - tokens[0].offset_from as u32;
                    let positions = &posting_lists[i][pointers[i]].1;

                    let shifted = positions
                        .iter()
                        .filter_map(|&p| p.checked_sub(byte_delta_from_first_token))
                        .collect::<HashSet<_>>();

                    adjusted = &adjusted & &shifted;
                    if adjusted.is_empty() {
                        break;
                    }
                }

                // All tokens are sequential
                if !adjusted.is_empty() {
                    results.insert(min_doc_id);
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

    #[cfg(test)]
    async fn get_all_results_for_token(
        &self,
        token: &str,
    ) -> Result<Vec<(u32, Vec<u32>)>, FullTextIndexError> {
        let positional_posting_list = self
            .posting_lists_blockfile_reader
            .get_prefix(token)
            .await?;
        let mut results = vec![];
        for (doc_id, positions) in positional_posting_list {
            results.push((doc_id, positions.to_vec()));
        }
        Ok(results)
    }
}

#[async_trait::async_trait]
impl NgramLiteralProvider<FullTextIndexError> for FullTextIndexReader<'_> {
    fn maximum_branching_factor(&self) -> usize {
        6
    }

    async fn prefetch_ngrams<'me, Ngrams>(&'me self, ngrams: Ngrams)
    where
        Ngrams: IntoIterator<Item = &'me str> + Send + Sync,
    {
        self.posting_lists_blockfile_reader
            .load_blocks_for_prefixes(ngrams)
            .await
    }

    async fn lookup_ngram<'me>(
        &'me self,
        ngram: &'me str,
    ) -> Result<Box<dyn Iterator<Item = (u32, &'me [u32])> + Send + Sync + 'me>, FullTextIndexError>
    {
        Ok(self
            .posting_lists_blockfile_reader
            .get_prefix(ngram)
            .await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_blockstore::{
        arrow::{config::BlockManagerConfig, provider::BlockfileReaderOptions},
        provider::BlockfileProvider,
        BlockfileWriterOptions,
    };
    use chroma_cache::new_cache_for_test;
    use chroma_storage::{local::LocalStorage, Storage};
    use tantivy::tokenizer::NgramTokenizer;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_new_writer() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path))
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let _index = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
    }

    #[tokio::test]
    async fn test_new_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let _ = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);
    }

    #[tokio::test]
    async fn test_index_and_search_single_document() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "hello world",
            }])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_repeating_character_in_query() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "helo",
            }])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_query_of_repeating_character() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "aaa",
            }])
            .unwrap();
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 2,
                new_document: "aaaaa",
            }])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("aaaa").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([2]));
    }

    #[tokio::test]
    async fn test_repeating_character_in_document() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "hello",
            }])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("helo").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_search_absent_token() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "hello world",
            }])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("chroma").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_multiple_candidates_within_document() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "hello world hello",
            }])
            .unwrap();
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 2,
                new_document: "    hello ",
            }])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let reader_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(reader_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2]));

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_multiple_simple_documents() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([
                DocumentMutation::Create {
                    offset_id: 1,
                    new_document: "hello world",
                },
                DocumentMutation::Create {
                    offset_id: 2,
                    new_document: "hello",
                },
            ])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2]));

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_multiple_complex_documents() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([
                DocumentMutation::Create {
                    offset_id: 1,
                    new_document: "hello world",
                },
                DocumentMutation::Create {
                    offset_id: 2,
                    new_document: "hello",
                },
                DocumentMutation::Create {
                    offset_id: 3,
                    new_document: "world",
                },
                DocumentMutation::Create {
                    offset_id: 4,
                    new_document: "world hello",
                },
            ])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2, 4]));

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 3, 4]));

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));

        let res = index_reader.search("world hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([4]));
    }

    #[tokio::test]
    async fn test_index_multiple_character_repeating() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([
                DocumentMutation::Create {
                    offset_id: 1,
                    new_document: "aaa",
                },
                DocumentMutation::Create {
                    offset_id: 2,
                    new_document: "aaaa",
                },
                DocumentMutation::Create {
                    offset_id: 3,
                    new_document: "bbb",
                },
                DocumentMutation::Create {
                    offset_id: 4,
                    new_document: "aaabbb",
                },
                DocumentMutation::Create {
                    offset_id: 5,
                    new_document: "aabbbbaaaaabbb",
                },
            ])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("aaa").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2, 4, 5]));

        let res = index_reader.search("bbb").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([3, 4, 5]));

        let res = index_reader.search("aaabbb").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([4, 5]));
    }

    #[tokio::test]
    async fn test_index_special_characters() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([
                DocumentMutation::Create {
                    offset_id: 1,
                    new_document: "!!!!!",
                },
                DocumentMutation::Create {
                    offset_id: 2,
                    new_document: "hello world!!!",
                },
                DocumentMutation::Create {
                    offset_id: 3,
                    new_document: ".!.!.!",
                },
            ])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("!!!!!").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));

        let res = index_reader.search("!!!").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2]));

        let res = index_reader.search(".!.").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([3]));
    }

    #[tokio::test]
    async fn test_get_all_results_for_token() {
        let provider = BlockfileProvider::new_memory();
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);

        index_writer
            .handle_batch([
                DocumentMutation::Create {
                    offset_id: 1,
                    new_document: "hello world",
                },
                DocumentMutation::Create {
                    offset_id: 2,
                    new_document: "hello",
                },
                DocumentMutation::Create {
                    offset_id: 3,
                    new_document: "world",
                },
            ])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.get_all_results_for_token("h").await.unwrap();
        assert_eq!(res.len(), 2);

        let res = index_reader.get_all_results_for_token("e").await.unwrap();
        assert_eq!(res.len(), 2);

        let res = index_reader.get_all_results_for_token("l").await.unwrap();
        assert_eq!(res.len(), 3);
    }

    #[tokio::test]
    async fn test_document_with_null_terminators() {
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let provider = BlockfileProvider::new_arrow(
            storage,
            1024 * 1024,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer.clone());

        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "hello \0 wor\0ld",
            }])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_document_with_multibyte_characters() {
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let provider = BlockfileProvider::new_arrow(
            storage,
            1024 * 1024,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer.clone());

        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "pretérito",
            }])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("pretérito").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));

        let res = index_reader.search("bretérito").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([]));
    }

    #[tokio::test]
    async fn test_update_document() {
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let provider = BlockfileProvider::new_arrow(
            storage,
            1024 * 1024,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer.clone());

        index_writer
            .handle_batch([
                DocumentMutation::Create {
                    offset_id: 1,
                    new_document: "hello world",
                },
                DocumentMutation::Create {
                    offset_id: 2,
                    new_document: "hello",
                },
                DocumentMutation::Create {
                    offset_id: 3,
                    new_document: "world",
                },
            ])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        // Update document document 1 with same content, update document 3 with new content
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .ordered_mutations()
                    .fork(pl_blockfile_id),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([
                DocumentMutation::Update {
                    offset_id: 1,
                    old_document: "hello world",
                    new_document: "hello world",
                },
                DocumentMutation::Update {
                    offset_id: 3,
                    old_document: "world",
                    new_document: "hello",
                },
            ])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2, 3]));

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_update_document_noop() {
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let provider = BlockfileProvider::new_arrow(
            storage,
            1024 * 1024,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).ordered_mutations(),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();

        // Create empty index
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer.clone());

        index_writer.handle_batch([]).unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        // Add document to index
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .ordered_mutations()
                    .fork(pl_blockfile_id),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer.clone());
        index_writer
            .handle_batch([DocumentMutation::Create {
                offset_id: 1,
                new_document: "hello world",
            }])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        // Update document with same content, should be a noop
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone())
                    .ordered_mutations()
                    .fork(pl_blockfile_id),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Update {
                offset_id: 1,
                old_document: "hello world",
                new_document: "hello world",
            }])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_delete_document() {
        let tmp_dir = tempdir().unwrap();
        let storage = Storage::Local(LocalStorage::new(tmp_dir.path().to_str().unwrap()));
        let block_cache = new_cache_for_test();
        let root_cache = new_cache_for_test();
        let provider = BlockfileProvider::new_arrow(
            storage,
            1024 * 1024,
            block_cache,
            root_cache,
            BlockManagerConfig::default_num_concurrent_block_flushes(),
        );
        let prefix_path = String::from("");
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::new(prefix_path.clone()))
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer.clone());

        index_writer
            .handle_batch([
                DocumentMutation::Create {
                    offset_id: 1,
                    new_document: "hello world",
                },
                DocumentMutation::Create {
                    offset_id: 2,
                    new_document: "hello",
                },
                DocumentMutation::Create {
                    offset_id: 3,
                    new_document: "world",
                },
            ])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        // Delete document 3
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(
                BlockfileWriterOptions::new(prefix_path.clone()).fork(pl_blockfile_id),
            )
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let mut index_writer = FullTextIndexWriter::new(pl_blockfile_writer, tokenizer);
        index_writer
            .handle_batch([DocumentMutation::Delete {
                offset_id: 3,
                old_document: "world",
            }])
            .unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let read_options = BlockfileReaderOptions::new(pl_blockfile_id, prefix_path);
        let pl_blockfile_reader = provider.read::<u32, &[u32]>(read_options).await.unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader = FullTextIndexReader::new(pl_blockfile_reader, tokenizer);

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }
}
