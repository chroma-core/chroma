use std::collections::HashMap;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_index::sparse::{reader::SparseReaderError, types::encode_u32};
use chroma_segment::{
    blockfile_metadata::{MetadataSegmentError, MetadataSegmentReader},
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    MaterializedLogOperation, MetadataValue, Segment, SignedRoaringBitmap, SparseVector,
};
use thiserror::Error;

use crate::execution::operators::fetch_log::FetchLogOutput;

/// Calculates the inverse document frequency (idf) for the dimensions present in the embedding
/// and scales the embedding correspondingly. The formula is:
///     idf(t) = ln((n - n_t + 0.5) / (n_t + 0.5) + 1)
/// where
///     n: total number of documents in the collection
///     n_t: number of documents with term t

#[derive(Debug)]
pub struct Idf {
    pub query: SparseVector,
    pub key: String,
}

#[derive(Debug)]
pub struct IdfInput {
    pub blockfile_provider: BlockfileProvider,
    pub logs: FetchLogOutput,
    pub mask: SignedRoaringBitmap,
    pub metadata_segment: Segment,
    pub record_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct IdfOutput {
    pub scaled_query: SparseVector,
}

#[derive(Debug, Error)]
pub enum IdfError {
    #[error(transparent)]
    Chroma(#[from] Box<dyn ChromaError>),
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating metadata segment reader: {0}")]
    MetadataReader(#[from] MetadataSegmentError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
    #[error("Error using sparse reader: {0}")]
    SparseReader(#[from] SparseReaderError),
}

impl ChromaError for IdfError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            IdfError::Chroma(err) => err.code(),
            IdfError::LogMaterializer(err) => err.code(),
            IdfError::MetadataReader(err) => err.code(),
            IdfError::RecordReader(err) => err.code(),
            IdfError::SparseReader(err) => err.code(),
        }
    }
}

#[async_trait]
impl Operator<IdfInput, IdfOutput> for Idf {
    type Error = IdfError;

    async fn run(&self, input: &IdfInput) -> Result<IdfOutput, IdfError> {
        let mut n = 0;
        let mut nts = HashMap::new();
        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => {
                n += reader.count().await?;
                Ok(Some(reader))
            }
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        let logs = materialize_logs(&record_segment_reader, input.logs.clone(), None).await?;

        let metadata_segment_reader = Box::pin(MetadataSegmentReader::from_segment(
            &input.metadata_segment,
            &input.blockfile_provider,
        ))
        .await?;

        if let Some(sparse_index_reader) = metadata_segment_reader.sparse_index_reader.as_ref() {
            for &dimension_id in &self.query.indices {
                let encoded_dimension_id = encode_u32(dimension_id);
                let nt = sparse_index_reader
                    .get_dimension_offset_rank(&encoded_dimension_id, u32::MAX)
                    .await?
                    .saturating_sub(
                        sparse_index_reader
                            .get_dimension_offset_rank(&encoded_dimension_id, 0)
                            .await?,
                    );
                nts.insert(dimension_id, nt);
            }
        }

        for log in &logs {
            let log = log
                .hydrate(record_segment_reader.as_ref())
                .await
                .map_err(IdfError::LogMaterializer)?;

            if match log.get_operation() {
                MaterializedLogOperation::Initial | MaterializedLogOperation::AddNew => false,
                MaterializedLogOperation::OverwriteExisting
                | MaterializedLogOperation::DeleteExisting => true,
                MaterializedLogOperation::UpdateExisting => log
                    .get_metadata_to_be_merged()
                    .map(|meta| matches!(meta.get(&self.key), Some(MetadataValue::SparseVector(_))))
                    .unwrap_or_default(),
            } {
                if let Some(MetadataValue::SparseVector(existing_embedding)) = log
                    .get_data_record()
                    .and_then(|record| record.metadata.as_ref())
                    .and_then(|meta| meta.get(&self.key))
                {
                    for index in &existing_embedding.indices {
                        if let Some(nt) = nts.get_mut(index) {
                            *nt = nt.saturating_sub(1);
                        }
                    }
                }
            }

            if let Some(MetadataValue::SparseVector(new_embedding)) = log
                .get_metadata_to_be_merged()
                .and_then(|meta| meta.get(&self.key))
            {
                for index in &new_embedding.indices {
                    if let Some(nt) = nts.get_mut(index) {
                        *nt = nt.saturating_add(1);
                    }
                }
            }

            n = match log.get_operation() {
                MaterializedLogOperation::Initial
                | MaterializedLogOperation::OverwriteExisting
                | MaterializedLogOperation::UpdateExisting => n,
                MaterializedLogOperation::AddNew => n.saturating_add(1),
                MaterializedLogOperation::DeleteExisting => n.saturating_sub(1),
            };
        }

        let scaled_query = SparseVector::from_pairs(self.query.iter().map(|(index, value)| {
            let nt = nts.get(&index).cloned().unwrap_or_default() as f32;
            let scale = ((n as f32 - nt + 0.5) / (nt + 0.5)).ln_1p();
            (index, scale * value)
        }));

        Ok(IdfOutput { scaled_query })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::test::{int_as_id, LogGenerator};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_types::{Chunk, LogRecord, Operation, OperationRecord, UpdateMetadataValue};

    /// Generator for creating log records with sparse vectors representing term frequencies
    fn sparse_term_generator(offset: usize) -> OperationRecord {
        let mut metadata = HashMap::new();

        // Create documents with different term distributions:
        // Docs 1-5: have term 0 (common term, appears in 50% of docs)
        // Docs 1-3: have term 1 (medium frequency, appears in 30% of docs)
        // Doc 1 only: has term 2 (rare term, appears in 10% of docs)
        // Docs 6-10: have term 3 (another common term)
        let (indices, values) = match offset {
            1 => (vec![0, 1, 2], vec![2.0, 1.5, 3.0]), // Has all terms including rare
            2..=3 => (vec![0, 1], vec![2.0, 1.5]),     // Has common and medium terms
            4..=5 => (vec![0], vec![2.0]),             // Has only common term
            6..=8 => (vec![3], vec![1.0]),             // Different common term
            9..=10 => (vec![3, 4], vec![1.0, 0.5]),    // Has term 3 and 4
            _ => (vec![], vec![]),
        };

        metadata.insert(
            "sparse_embedding".to_string(),
            UpdateMetadataValue::SparseVector(SparseVector { indices, values }),
        );

        // Add dummy embedding for materialization (required by TestDistributedSegment)
        let embedding = Some(vec![0.1; 128]); // Default dimension is 128

        OperationRecord {
            id: int_as_id(offset),
            embedding,
            encoding: None,
            metadata: Some(metadata),
            document: Some(format!("Document {}", offset)),
            operation: Operation::Upsert,
        }
    }

    async fn setup_idf_input(
        num_records: usize,
        additional_logs: Vec<OperationRecord>,
    ) -> (TestDistributedSegment, IdfInput) {
        let mut test_segment = TestDistributedSegment::new().await;

        // Generate initial records and compact them into the segment
        if num_records > 0 {
            let initial_logs = sparse_term_generator.generate_chunk(1..=num_records);
            Box::pin(test_segment.compact_log(initial_logs, num_records + 1)).await;
        }

        // Convert additional operation records to log records
        let logs: Vec<LogRecord> = additional_logs
            .into_iter()
            .enumerate()
            .map(|(i, record)| LogRecord {
                log_offset: (num_records + i + 1) as i64,
                record,
            })
            .collect();

        let input = IdfInput {
            blockfile_provider: test_segment.blockfile_provider.clone(),
            logs: Chunk::new(logs.into()),
            mask: SignedRoaringBitmap::full(),
            metadata_segment: test_segment.metadata_segment.clone(),
            record_segment: test_segment.record_segment.clone(),
        };

        (test_segment, input)
    }

    #[tokio::test]
    async fn test_idf_basic_scaling() {
        let (_test_segment, input) = Box::pin(setup_idf_input(10, vec![])).await;

        // Query vector with terms that have different frequencies
        let query_vector = SparseVector {
            indices: vec![0, 1, 2, 3, 4],
            values: vec![1.0, 1.0, 1.0, 1.0, 1.0],
        };

        let idf_operator = Idf {
            query: query_vector,
            key: "sparse_embedding".to_string(),
        };

        let output = idf_operator
            .run(&input)
            .await
            .expect("IDF operator should succeed");

        // Verify IDF scaling:
        // Term 0: appears in 5/10 docs, IDF = ln((10 - 5 + 0.5) / (5 + 0.5) + 1) = ln(2) ≈ 0.693
        // Term 1: appears in 3/10 docs, IDF = ln((10 - 3 + 0.5) / (3 + 0.5) + 1) = ln(3.14) ≈ 1.146
        // Term 2: appears in 1/10 docs, IDF = ln((10 - 1 + 0.5) / (1 + 0.5) + 1) = ln(7.33) ≈ 1.992
        // Term 3: appears in 5/10 docs, IDF = ln((10 - 5 + 0.5) / (5 + 0.5) + 1) = ln(2) ≈ 0.693
        // Term 4: appears in 2/10 docs, IDF = ln((10 - 2 + 0.5) / (2 + 0.5) + 1) = ln(4.4) ≈ 1.482

        let scaled = &output.scaled_query;
        assert_eq!(scaled.indices.len(), 5);

        // Check IDF values with tolerance for floating point
        let expected_idfs = [
            (0, 0.693), // Term 0
            (1, 1.146), // Term 1
            (2, 1.992), // Term 2
            (3, 0.693), // Term 3
            (4, 1.482), // Term 4
        ];

        for (i, (term_id, expected_idf)) in expected_idfs.iter().enumerate() {
            assert_eq!(scaled.indices[i], *term_id);
            assert!(
                (scaled.values[i] - expected_idf).abs() < 0.01,
                "Term {} IDF mismatch: expected {:.3}, got {:.3}",
                term_id,
                expected_idf,
                scaled.values[i]
            );
        }
    }

    #[tokio::test]
    async fn test_idf_with_deletions() {
        // Start with 10 documents, then delete some via logs
        let delete_logs = vec![
            OperationRecord {
                id: int_as_id(1), // Delete doc with rare term 2
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Delete,
            },
            OperationRecord {
                id: int_as_id(4), // Delete doc with term 0
                embedding: None,
                encoding: None,
                metadata: None,
                document: None,
                operation: Operation::Delete,
            },
        ];

        let (_test_segment, input) = Box::pin(setup_idf_input(10, delete_logs)).await;

        let query_vector = SparseVector {
            indices: vec![0, 1, 2],
            values: vec![1.0, 1.0, 1.0],
        };

        let idf_operator = Idf {
            query: query_vector,
            key: "sparse_embedding".to_string(),
        };

        let output = idf_operator
            .run(&input)
            .await
            .expect("IDF operator should succeed");

        // After deletions:
        // Total docs: 8 (10 - 2)
        // Term 0: originally in docs 1,2,3,4,5 (5 docs), after deleting 1,4 -> in docs 2,3,5 (3 docs)
        // Term 1: originally in docs 1,2,3 (3 docs), after deleting 1 -> in docs 2,3 (2 docs)
        // Term 2: originally in doc 1 (1 doc), after deleting 1 -> in 0 docs

        let scaled = &output.scaled_query;

        // Term 0: IDF = ln((8 - 3 + 0.5) / (3 + 0.5) + 1) = ln(2.571) ≈ 0.944
        assert!(
            (scaled.values[0] - 0.944).abs() < 0.01,
            "Term 0 IDF mismatch: expected 0.944, got {}",
            scaled.values[0]
        );

        // Term 1: IDF = ln((8 - 2 + 0.5) / (2 + 0.5) + 1) = ln(3.6) ≈ 1.281
        assert!(
            (scaled.values[1] - 1.281).abs() < 0.01,
            "Term 1 IDF mismatch: expected 1.281, got {}",
            scaled.values[1]
        );

        // Term 2: IDF = ln((8 - 0 + 0.5) / (0 + 0.5) + 1) = ln(18) ≈ 2.890
        assert!(
            (scaled.values[2] - 2.890).abs() < 0.01,
            "Term 2 IDF mismatch: expected 2.890, got {}",
            scaled.values[2]
        );
    }

    #[tokio::test]
    async fn test_idf_with_updates() {
        // Update documents to change their sparse vectors
        let update_logs = vec![
            OperationRecord {
                id: int_as_id(5), // Doc 5 currently has only term 0
                embedding: None,
                encoding: None,
                metadata: Some(HashMap::from([(
                    "sparse_embedding".to_string(),
                    UpdateMetadataValue::SparseVector(SparseVector {
                        indices: vec![1, 2], // Now has terms 1 and 2 instead
                        values: vec![2.0, 3.0],
                    }),
                )])),
                document: None,
                operation: Operation::Update,
            },
            OperationRecord {
                id: int_as_id(6), // Doc 6 currently has term 3
                embedding: None,
                encoding: None,
                metadata: Some(HashMap::from([(
                    "sparse_embedding".to_string(),
                    UpdateMetadataValue::SparseVector(SparseVector {
                        indices: vec![0], // Now has term 0 instead
                        values: vec![1.5],
                    }),
                )])),
                document: None,
                operation: Operation::Update,
            },
        ];

        let (_test_segment, input) = Box::pin(setup_idf_input(10, update_logs)).await;

        let query_vector = SparseVector {
            indices: vec![0, 1, 2, 3],
            values: vec![1.0, 1.0, 1.0, 1.0],
        };

        let idf_operator = Idf {
            query: query_vector,
            key: "sparse_embedding".to_string(),
        };

        let output = idf_operator
            .run(&input)
            .await
            .expect("IDF operator should succeed");

        // After updates:
        // Total docs: 10 (no additions or deletions)
        // Term 0: now in 5/10 docs (was 5, lost doc 5, gained doc 6)
        // Term 1: now in 4/10 docs (was 3, gained doc 5)
        // Term 2: now in 2/10 docs (was 1, gained doc 5)
        // Term 3: now in 4/10 docs (was 5, lost doc 6)

        let scaled = &output.scaled_query;

        // Term 0: IDF = ln((10 - 5 + 0.5) / (5 + 0.5) + 1) = ln(2) ≈ 0.693
        assert!((scaled.values[0] - 0.693).abs() < 0.01);

        // Term 1: IDF = ln((10 - 4 + 0.5) / (4 + 0.5) + 1) = ln(2.44) ≈ 0.893
        assert!((scaled.values[1] - 0.893).abs() < 0.01);

        // Term 2: IDF = ln((10 - 2 + 0.5) / (2 + 0.5) + 1) = ln(4.4) ≈ 1.482
        assert!((scaled.values[2] - 1.482).abs() < 0.01);

        // Term 3: IDF = ln((10 - 4 + 0.5) / (4 + 0.5) + 1) = ln(2.44) ≈ 0.893
        assert!((scaled.values[3] - 0.893).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_idf_with_additions() {
        // Add new documents via logs
        let add_logs = vec![
            OperationRecord {
                id: int_as_id(11),
                embedding: Some(vec![0.1; 128]), // Add dummy embedding
                encoding: None,
                metadata: Some(HashMap::from([(
                    "sparse_embedding".to_string(),
                    UpdateMetadataValue::SparseVector(SparseVector {
                        indices: vec![0, 5], // New term 5
                        values: vec![1.0, 2.0],
                    }),
                )])),
                document: Some("Document 11".to_string()),
                operation: Operation::Add,
            },
            OperationRecord {
                id: int_as_id(12),
                embedding: Some(vec![0.1; 128]), // Add dummy embedding
                encoding: None,
                metadata: Some(HashMap::from([(
                    "sparse_embedding".to_string(),
                    UpdateMetadataValue::SparseVector(SparseVector {
                        indices: vec![5], // Another doc with term 5
                        values: vec![3.0],
                    }),
                )])),
                document: Some("Document 12".to_string()),
                operation: Operation::Add,
            },
        ];

        let (_test_segment, input) = Box::pin(setup_idf_input(10, add_logs)).await;

        let query_vector = SparseVector {
            indices: vec![0, 5],
            values: vec![1.0, 1.0],
        };

        let idf_operator = Idf {
            query: query_vector,
            key: "sparse_embedding".to_string(),
        };

        let output = idf_operator
            .run(&input)
            .await
            .expect("IDF operator should succeed");

        // After additions:
        // Total docs: 12 (10 + 2)
        // Term 0: now in 6/12 docs (was 5, added 1)
        // Term 5: now in 2/12 docs (new term)

        let scaled = &output.scaled_query;

        // Term 0: IDF = ln((12 - 6 + 0.5) / (6 + 0.5) + 1) = ln(2) ≈ 0.693
        assert!(
            (scaled.values[0] - 0.693).abs() < 0.01,
            "Term 0 IDF mismatch: expected 0.693, got {}",
            scaled.values[0]
        );

        // Term 5: IDF = ln((12 - 2 + 0.5) / (2 + 0.5) + 1) = ln(5.2) ≈ 1.649
        assert!(
            (scaled.values[1] - 1.649).abs() < 0.01,
            "Term 5 IDF mismatch: expected 1.649, got {}",
            scaled.values[1]
        );
    }

    #[tokio::test]
    async fn test_idf_empty_query() {
        let (_test_segment, input) = Box::pin(setup_idf_input(10, vec![])).await;

        // Empty query vector
        let query_vector = SparseVector {
            indices: vec![],
            values: vec![],
        };

        let idf_operator = Idf {
            query: query_vector,
            key: "sparse_embedding".to_string(),
        };

        let output = idf_operator
            .run(&input)
            .await
            .expect("IDF operator should succeed");

        // Should return empty scaled embedding
        assert_eq!(output.scaled_query.indices.len(), 0);
        assert_eq!(output.scaled_query.values.len(), 0);
    }

    #[tokio::test]
    async fn test_idf_missing_terms() {
        let (_test_segment, input) = Box::pin(setup_idf_input(10, vec![])).await;

        // Query with terms that don't exist in any document
        let query_vector = SparseVector {
            indices: vec![99, 100],
            values: vec![1.0, 2.0],
        };

        let idf_operator = Idf {
            query: query_vector,
            key: "sparse_embedding".to_string(),
        };

        let output = idf_operator
            .run(&input)
            .await
            .expect("IDF operator should succeed");

        // For terms not in any document, n_t = 0
        // IDF = ln((10 - 0 + 0.5) / (0 + 0.5) + 1) = ln(22) ≈ 3.091
        let scaled = &output.scaled_query;
        assert_eq!(scaled.indices.len(), 2);
        assert!((scaled.values[0] - 3.091).abs() < 0.01);
        assert!((scaled.values[1] - 6.182).abs() < 0.01); // 2.0 * 3.091
    }
}
