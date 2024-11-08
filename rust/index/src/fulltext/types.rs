use super::tokenizer::{ChromaTokenStream, ChromaTokenizer};
use chroma_blockstore::{BlockfileFlusher, BlockfileReader, BlockfileWriter};
use chroma_error::{ChromaError, ErrorCodes};
use futures::StreamExt;
use itertools::Itertools;
use parking_lot::Mutex;
use roaring::RoaringBitmap;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::{u128, u32};
use tantivy::tokenizer::NgramTokenizer;
use tantivy::tokenizer::TokenStream;
use tantivy::tokenizer::Tokenizer;
use thiserror::Error;
use uuid::Uuid;

pub type TokenInstance = u128;

// Unicode characters only use 21 bit, so we can encode a trigram in 21 * 3 = 63 bits (a u64).
#[inline(always)]
pub(super) fn pack_trigram(s: &str) -> u64 {
    let mut u = 0u64;
    for (i, c) in s.chars().take(3).enumerate() {
        let shift = (2 - i) * 21;
        u |= (c as u64) << shift;
    }
    u
}

unsafe fn encode_utf8_unchecked(c: u32, buf: &mut [u8]) -> usize {
    if c == 0 {
        0
    } else if c < 0x80 {
        buf[0] = c as u8;
        1
    } else if c < 0x800 {
        buf[0] = (0xC0 | (c >> 6)) as u8;
        buf[1] = (0x80 | (c & 0x3F)) as u8;
        2
    } else if c < 0x10000 {
        buf[0] = (0xE0 | (c >> 12)) as u8;
        buf[1] = (0x80 | ((c >> 6) & 0x3F)) as u8;
        buf[2] = (0x80 | (c & 0x3F)) as u8;
        3
    } else {
        buf[0] = (0xF0 | (c >> 18)) as u8;
        buf[1] = (0x80 | ((c >> 12) & 0x3F)) as u8;
        buf[2] = (0x80 | ((c >> 6) & 0x3F)) as u8;
        buf[3] = (0x80 | (c & 0x3F)) as u8;
        4
    }
}

#[inline(always)]
pub(super) fn unpack_trigram(u: u64) -> String {
    let c0 = ((u >> 42) & 0x1F_FFFF) as u32;
    let c1 = ((u >> 21) & 0x1F_FFFF) as u32;
    let c2 = (u & 0x1F_FFFF) as u32;

    // Preallocate the maximum possible size (3 chars * 4 bytes each)
    let mut s = String::with_capacity(12);

    unsafe {
        // Directly get a mutable reference to the internal buffer
        let v = s.as_mut_vec();
        let len0 = v.len();

        // Ensure the buffer has enough capacity
        v.set_len(len0 + 12);

        // Encode the codepoints directly into the buffer
        let bytes_written_c0 = encode_utf8_unchecked(c0, &mut v[len0..]);
        let bytes_written_c1 = encode_utf8_unchecked(c1, &mut v[len0 + bytes_written_c0..]);
        let bytes_written_c2 =
            encode_utf8_unchecked(c2, &mut v[len0 + bytes_written_c0 + bytes_written_c1..]);

        // Set the correct length after writing
        let total_bytes = bytes_written_c0 + bytes_written_c1 + bytes_written_c2;
        v.set_len(len0 + total_bytes);
    }

    s
}

pub trait TokenContainer {
    fn encode(token: &str, document_id: u32, offset: Option<u32>) -> Self;
    fn get_token(&self) -> String;
    fn get_encoded_token(&self) -> u64;
    fn get_document_id(&self) -> u32;
    fn get_encoded_token_and_document_id(&self) -> u128;
    fn get_offset(&self) -> Option<u32>;
}

impl TokenContainer for TokenInstance {
    #[inline(always)]
    fn encode(token: &str, document_id: u32, offset: Option<u32>) -> Self {
        (pack_trigram(token) as u128) << 64
            | (document_id as u128) << 32
            | offset.map(|o| o | (1 << 31)).unwrap_or(0) as u128
    }

    #[inline(always)]
    fn get_encoded_token_and_document_id(&self) -> u128 {
        *self >> 32
    }

    #[inline(always)]
    fn get_encoded_token(&self) -> u64 {
        (*self >> 64) as u64
    }

    #[inline(always)]
    fn get_token(&self) -> String {
        unpack_trigram((*self >> 64) as u64)
    }

    #[inline(always)]
    fn get_document_id(&self) -> u32 {
        (*self >> 32) as u32
    }

    #[inline(always)]
    fn get_offset(&self) -> Option<u32> {
        let offset = *self as u32;
        if offset & (1 << 31) != 0 {
            return Some(offset & !(1 << 31));
        }

        None
    }
}

// type WorkerResult = Vec<TokenInstance>;

// struct Worker {}
// impl Worker {
//     fn new() -> Self {
//         Worker {}
//     }

//     fn run(rx: Receiver<(u32, String)>) -> WorkerResult {
//         let tokenizer = NgramTokenizer::new(3, 3, false).unwrap();
//         let mut results: WorkerResult = Vec::new();

//         while let Ok((offset_id, document)) = rx.recv_blocking() {
//             tokenizer
//                 .clone()
//                 .token_stream(document.as_str())
//                 .process(&mut |token| {
//                     results.push(TokenInstance::encode(
//                         token.text.as_str(),
//                         offset_id,
//                         token.offset_from as u32,
//                     ));
//                 });
//         }

//         results.sort_unstable();
//         results
//     }
// }

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

#[derive(Debug)]
pub struct UncommittedPostings {
    // token -> {doc -> [start positions]}
    positional_postings: HashMap<String, HashMap<u32, Vec<u32>>>,
    // (token, doc) pairs that should be deleted from storage.
    deleted_token_doc_pairs: HashSet<(String, i32)>,
}

impl Default for UncommittedPostings {
    fn default() -> Self {
        Self::new()
    }
}

impl UncommittedPostings {
    pub fn new() -> Self {
        Self {
            positional_postings: HashMap::new(),
            deleted_token_doc_pairs: HashSet::new(),
        }
    }
}

#[derive(Clone)]
pub struct FullTextIndexWriter {
    tokenizer: NgramTokenizer, // todo
    token_instances: Arc<Mutex<Vec<TokenInstance>>>,
    // full_text_index_reader: Option<FullTextIndexReader<'me>>,
    posting_lists_blockfile_writer: BlockfileWriter,
    frequencies_blockfile_writer: BlockfileWriter,
    // tokenizer: Arc<Box<dyn ChromaTokenizer>>,
    // TODO(Sanket): Move off this tokio::sync::mutex and use
    // a lightweight lock instead. This is needed currently to
    // keep holding the lock across an await point.
    // term -> positional posting list builder for that term
    // uncommitted_postings: Arc<tokio::sync::Mutex<UncommittedPostings>>,
    // TODO(Sanket): Move off this tokio::sync::mutex and use
    // a lightweight lock instead. This is needed currently to
    // keep holding the lock across an await point.
    // Value of this map is a tuple (old freq and new freq)
    // because we also need to keep the old frequency
    // around. The reason is (token, freq) is the key in the blockfile hence
    // when freq changes, we need to delete the old (token, freq) key.
    // uncommitted_frequencies: Arc<tokio::sync::Mutex<HashMap<String, (i32, i32)>>>,
}

impl FullTextIndexWriter {
    pub fn new(
        full_text_index_reader: Option<FullTextIndexReader>,
        posting_lists_blockfile_writer: BlockfileWriter,
        frequencies_blockfile_writer: BlockfileWriter,
        tokenizer: NgramTokenizer,
    ) -> Self {
        FullTextIndexWriter {
            tokenizer,
            // full_text_index_reader,
            posting_lists_blockfile_writer,
            frequencies_blockfile_writer,
            token_instances: Arc::new(Mutex::new(Vec::new())),
            // tokenizer: Arc::new(tokenizer),
        }
    }

    // important: duplicate offset IDs not allowed
    pub fn handle_batch(
        &self,
        previous_doc_and_new_doc: &[(u32, Option<&str>, Option<&str>)],
    ) -> Result<(), FullTextIndexError> {
        let mut token_instances = self.token_instances.lock();

        for pair in previous_doc_and_new_doc {
            match pair {
                (offset_id, Some(previous), Some(new)) => {
                    // Update existing doc

                    // Remove old version
                    let mut trigrams_to_delete = HashSet::new(); // (need to filter out duplicates, each trigram may appear multiple times in a document)
                    self.tokenizer
                        .clone()
                        .token_stream(previous)
                        .process(&mut |token| {
                            trigrams_to_delete.insert(TokenInstance::encode(
                                token.text.as_str(),
                                *offset_id,
                                None,
                            ));
                        });

                    // Add doc
                    self.tokenizer
                        .clone()
                        .token_stream(new)
                        .process(&mut |token| {
                            trigrams_to_delete.remove(&TokenInstance::encode(
                                token.text.as_str(),
                                *offset_id,
                                None,
                            ));

                            token_instances.push(TokenInstance::encode(
                                token.text.as_str(),
                                *offset_id,
                                Some(token.offset_from as u32),
                            ));
                        });

                    token_instances.extend(trigrams_to_delete.into_iter());
                }
                (offset_id, Some(previous), None) => {
                    let mut trigrams_to_delete = HashSet::new(); // (need to filter out duplicates, each trigram may appear multiple times in a document)

                    // Delete doc
                    self.tokenizer
                        .clone()
                        .token_stream(previous)
                        .process(&mut |token| {
                            trigrams_to_delete.insert(TokenInstance::encode(
                                token.text.as_str(),
                                *offset_id,
                                None,
                            ));
                        });

                    token_instances.extend(trigrams_to_delete.into_iter());
                }
                (offset_id, None, Some(new)) => {
                    // Add doc
                    self.tokenizer
                        .clone()
                        .token_stream(new)
                        .process(&mut |token| {
                            token_instances.push(TokenInstance::encode(
                                token.text.as_str(),
                                *offset_id,
                                Some(token.offset_from as u32),
                            ));
                        });
                }
                (_, None, None) => {
                    // todo: error
                }
            }
        }

        Ok(())
    }

    pub async fn add_document(
        &self,
        document: &str,
        offset_id: u32,
    ) -> Result<(), FullTextIndexError> {
        unimplemented!()
        // self.tx
        //     .send((offset_id, document.to_string()))
        //     .await
        //     .unwrap(); // todo
        // Ok(())
    }

    pub async fn delete_document(
        &self,
        document: &str,
        offset_id: u32,
    ) -> Result<(), FullTextIndexError> {
        unimplemented!()
        // let tokens = self.encode_tokens(document);
        // let tokens = tokens.get_tokens();

        // self.populate_frequencies_and_posting_lists_from_previous_version(tokens)
        //     .await?;
        // let mut uncommitted_frequencies = self.uncommitted_frequencies.lock().await;
        // let mut uncommitted_postings = self.uncommitted_postings.lock().await;

        // for token in tokens {
        //     match uncommitted_frequencies.get_mut(token.text.as_str()) {
        //         Some(frequency) => {
        //             frequency.0 -= 1;
        //         }
        //         None => {
        //             // Invariant violation -- we just populated this.
        //             tracing::error!("Error decrementing frequency for token: {:?}", token.text);
        //             return Err(FullTextIndexError::InvariantViolation);
        //         }
        //     }
        //     if let Some(builder) = uncommitted_postings
        //         .positional_postings
        //         .get_mut(token.text.as_str())
        //     {
        //         builder.remove(&offset_id);
        //         if builder.is_empty() {
        //             uncommitted_postings
        //                 .positional_postings
        //                 .remove(token.text.as_str());
        //         }
        //         // Track all the deleted (token, doc) pairs. This is needed
        //         // to remove the old postings list for this pair from storage.
        //         uncommitted_postings
        //             .deleted_token_doc_pairs
        //             .insert((token.text.clone(), offset_id as i32));
        //     }
        // }
        // Ok(())
    }

    pub async fn update_document(
        &self,
        old_document: &str,
        new_document: &str,
        offset_id: u32,
    ) -> Result<(), FullTextIndexError> {
        unimplemented!()
        // self.delete_document(old_document, offset_id).await?;
        // self.add_document(new_document, offset_id).await?;
        // Ok(())
    }

    pub async fn write_to_blockfiles(&mut self) -> Result<(), FullTextIndexError> {
        let mut last_key = TokenInstance::MAX;
        let mut posting_list: Vec<u32> = vec![];

        let mut token_instances = std::mem::take(&mut *self.token_instances.lock());
        token_instances.sort_unstable();

        for encoded_instance in token_instances {
            match encoded_instance.get_offset() {
                Some(offset) => {
                    let this_key = encoded_instance.get_encoded_token_and_document_id();
                    if last_key != this_key {
                        if last_key != TokenInstance::MAX {
                            let token = (last_key << 32).get_token();
                            let document_id = (last_key << 32).get_document_id();
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
                    // Trigram & offset ID pair is a delete
                    self.posting_lists_blockfile_writer
                        .delete::<u32, Vec<u32>>(
                            &encoded_instance.get_token(),
                            encoded_instance.get_document_id(), // todo: rename
                        )
                        .await
                        .unwrap();
                }
            }
        }

        if last_key != TokenInstance::MAX {
            let token = (last_key << 32).get_token();
            let document_id = (last_key << 32).get_document_id();
            self.posting_lists_blockfile_writer
                .set(&token, document_id, posting_list.clone())
                .await
                .unwrap();
        }

        Ok(())
    }

    pub async fn commit(self) -> Result<FullTextIndexFlusher, FullTextIndexError> {
        // let mut worker_handles = Arc::try_unwrap(self.worker_handles).unwrap(); // todo
        // drop(self.tx);
        // let mut worker_results = vec![];
        // while let Some(result) = worker_handles.next().await {
        //     let result = result.unwrap();
        //     worker_results.push(result);
        // }

        // let mut last_key = u128::MAX;
        // let mut posting_list: Vec<u32> = vec![];
        // for encoded_instance in worker_results
        //     .into_iter()
        //     .kmerge_by(|a_instance, b_instance| {
        //         a_instance.get_encoded_token_and_document_id()
        //             < b_instance.get_encoded_token_and_document_id()
        //     })
        // {
        //     let this_key = encoded_instance.get_encoded_token_and_document_id();
        //     if last_key != this_key {
        //         if last_key != u128::MAX {
        //             let token = (last_key << 32).get_token();
        //             let document_id = (last_key << 32).get_document_id();
        //             self.posting_lists_blockfile_writer
        //                 .set(&token, document_id, posting_list.clone())
        //                 .await
        //                 .unwrap();
        //             posting_list.clear();
        //         }
        //         last_key = this_key;
        //     }
        //     posting_list.push(encoded_instance.get_offset());
        // }

        // TODO should we be `await?`ing these? Or can we just return the futures?
        let posting_lists_blockfile_flusher = self
            .posting_lists_blockfile_writer
            .commit::<u32, Vec<u32>>()
            .await?;
        let frequencies_blockfile_flusher = self
            .frequencies_blockfile_writer
            .commit::<u32, String>()
            .await?;
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
    tokenizer: NgramTokenizer,
}

impl<'me> FullTextIndexReader<'me> {
    pub fn new(
        posting_lists_blockfile_reader: BlockfileReader<'me, u32, &'me [u32]>,
        frequencies_blockfile_reader: BlockfileReader<'me, u32, u32>,
        tokenizer: NgramTokenizer, // todo
    ) -> Self {
        FullTextIndexReader {
            posting_lists_blockfile_reader,
            frequencies_blockfile_reader,
            tokenizer,
        }
    }

    // pub fn encode_tokens(&self, document: &str) -> Box<dyn ChromaTokenStream> {
    //     self.tokenizer.encode(document)
    // }

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
        let posting_lists = futures::stream::iter(tokens)
            .then(|token| async move {
                let positional_posting_list = self
                    .posting_lists_blockfile_reader
                    .get_range(token.text.as_str()..=token.text.as_str(), ..)
                    .await?;
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
                let mut positions_per_posting_list = Vec::with_capacity(num_tokens);
                for (i, posting_list) in posting_lists.iter().enumerate() {
                    let (_, positions) = posting_list[pointers[i]];
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

    // We use this to implement deletes in the Writer. A delete() is implemented
    // by copying all the data from the old blockfile to a new one but skipping
    // the deleted offset id.
    async fn get_all_results_for_token(
        &self,
        token: &str,
    ) -> Result<Vec<(u32, Vec<u32>)>, FullTextIndexError> {
        let positional_posting_list = self
            .posting_lists_blockfile_reader
            .get_range(token..=token, ..)
            .await?;
        let mut results = vec![];
        for (doc_id, positions) in positional_posting_list.iter() {
            results.push((*doc_id, positions.to_vec()));
        }
        Ok(results)
    }

    // Also used to implement deletes in the Writer. When we delete a document,
    // we have to decrement the frequencies of all its tokens.
    async fn get_frequencies_for_token(&self, token: &str) -> Result<u32, FullTextIndexError> {
        let res = self
            .frequencies_blockfile_reader
            .get_range(token..=token, ..)
            .await?;
        if res.is_empty() {
            return Ok(0);
        }
        if res.len() > 1 {
            panic!("Invariant violation. Multiple frequency values found for a token.");
        }
        Ok(res[0].0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fulltext::tokenizer::TantivyChromaTokenizer;
    use chroma_blockstore::{provider::BlockfileProvider, BlockfileWriterOptions};
    use tantivy::tokenizer::NgramTokenizer;

    #[tokio::test]
    async fn test_new_writer() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let _index =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
    }

    #[tokio::test]
    async fn test_new_writer_then_reader() {
        let provider = BlockfileProvider::new_memory();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_id = freq_blockfile_writer.id();
        let pl_blockfile_id = pl_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let _ = FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);
    }

    #[tokio::test]
    async fn test_index_and_search_single_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer
            .handle_batch(&[(1, None, Some("hello world"))])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

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
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer
            .handle_batch(&[(1, None, Some("helo"))])
            .unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_query_of_repeating_character() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("aaa", 1).await.unwrap();
        index_writer.add_document("aaaaa", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("aaaa").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([2]));
    }

    #[tokio::test]
    async fn test_repeating_character_in_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("helo").await.unwrap();
        assert!(res.is_empty());
    }

    #[tokio::test]
    async fn test_search_absent_token() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("chroma").await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_multiple_candidates_within_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer
            .add_document("hello world hello", 1)
            .await
            .unwrap();
        index_writer.add_document("    hello ", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2]));

        let res = index_reader.search("hello world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_multiple_simple_documents() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2]));

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_multiple_complex_documents() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();
        index_writer.add_document("world hello", 4).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

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
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
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
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

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
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);
        index_writer.add_document("!!!!!", 1).await.unwrap();
        index_writer
            .add_document("hello world!!!", 2)
            .await
            .unwrap();
        index_writer.add_document(".!.!.!", 3).await.unwrap();
        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("!!!!!").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));

        let res = index_reader.search("!!!").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2]));

        let res = index_reader.search(".!.").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([3]));
    }

    #[tokio::test]
    async fn test_get_frequencies_for_token() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);

        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
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
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);

        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
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
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
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
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("hello").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1, 2, 3]));

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }

    #[tokio::test]
    async fn test_delete_document() {
        let provider = BlockfileProvider::new_memory();
        let pl_blockfile_writer = provider
            .write::<u32, Vec<u32>>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let freq_blockfile_writer = provider
            .write::<u32, String>(BlockfileWriterOptions::default())
            .await
            .unwrap();
        let pl_blockfile_id = pl_blockfile_writer.id();
        let freq_blockfile_id = freq_blockfile_writer.id();

        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let mut index_writer =
            FullTextIndexWriter::new(None, pl_blockfile_writer, freq_blockfile_writer, tokenizer);

        index_writer.add_document("hello world", 1).await.unwrap();
        index_writer.add_document("hello", 2).await.unwrap();
        index_writer.add_document("world", 3).await.unwrap();
        index_writer.delete_document("world", 3).await.unwrap();

        index_writer.write_to_blockfiles().await.unwrap();
        let flusher = index_writer.commit().await.unwrap();
        flusher.flush().await.unwrap();

        let freq_blockfile_reader = provider.read::<u32, u32>(&freq_blockfile_id).await.unwrap();
        let pl_blockfile_reader = provider
            .read::<u32, &[u32]>(&pl_blockfile_id)
            .await
            .unwrap();
        let tokenizer = NgramTokenizer::new(1, 1, false).unwrap();
        let index_reader =
            FullTextIndexReader::new(pl_blockfile_reader, freq_blockfile_reader, tokenizer);

        let res = index_reader.search("world").await.unwrap();
        assert_eq!(res, RoaringBitmap::from([1]));
    }
}
