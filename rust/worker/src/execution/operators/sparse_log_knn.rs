use std::collections::BinaryHeap;

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::ChromaError;
use chroma_segment::{
    blockfile_record::{RecordSegmentReader, RecordSegmentReaderCreationError},
    types::{materialize_logs, LogMaterializerError},
};
use chroma_system::Operator;
use chroma_types::{
    operator::RecordMeasure, MaterializedLogOperation, MetadataValue, Segment, SignedRoaringBitmap,
    SparseVector,
};
use sprs::CsVec;
use thiserror::Error;

use crate::execution::operators::fetch_log::FetchLogOutput;

#[derive(Clone, Debug)]
pub struct SparseLogKnnInput {
    pub blockfile_provider: BlockfileProvider,
    pub logs: FetchLogOutput,
    pub mask: SignedRoaringBitmap,
    pub record_segment: Segment,
}

#[derive(Clone, Debug)]
pub struct SparseLogKnnOutput {
    pub records: Vec<RecordMeasure>,
}

#[derive(Debug, Error)]
pub enum SparseLogKnnError {
    #[error("Error materializing log: {0}")]
    LogMaterializer(#[from] LogMaterializerError),
    #[error("Error creating record segment reader: {0}")]
    RecordReader(#[from] RecordSegmentReaderCreationError),
}

impl ChromaError for SparseLogKnnError {
    fn code(&self) -> chroma_error::ErrorCodes {
        match self {
            SparseLogKnnError::LogMaterializer(err) => err.code(),
            SparseLogKnnError::RecordReader(err) => err.code(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct SparseLogKnn {
    pub query: SparseVector,
    pub key: String,
    pub limit: u32,
}

#[async_trait]
impl Operator<SparseLogKnnInput, SparseLogKnnOutput> for SparseLogKnn {
    type Error = SparseLogKnnError;

    async fn run(
        &self,
        input: &SparseLogKnnInput,
    ) -> Result<SparseLogKnnOutput, SparseLogKnnError> {
        let query_sparse_vector: CsVec<f32> = (&self.query).into();
        let record_segment_reader = match Box::pin(RecordSegmentReader::from_segment(
            &input.record_segment,
            &input.blockfile_provider,
        ))
        .await
        {
            Ok(reader) => Ok(Some(reader)),
            Err(e) if matches!(*e, RecordSegmentReaderCreationError::UninitializedSegment) => {
                Ok(None)
            }
            Err(e) => Err(*e),
        }?;

        let logs = materialize_logs(&record_segment_reader, input.logs.clone(), None).await?;

        // We need the smallest results, so we keep a max heap to track the largest of them
        // so that it can be replaced if we found a smaller one
        let mut max_heap = BinaryHeap::with_capacity(self.limit as usize);
        for log in &logs {
            if !matches!(
                log.get_operation(),
                MaterializedLogOperation::DeleteExisting
            ) && input.mask.contains(log.get_offset_id())
            {
                let log = log
                    .hydrate(record_segment_reader.as_ref())
                    .await
                    .map_err(SparseLogKnnError::LogMaterializer)?;
                let merged_metadata = log.merged_metadata();
                let Some(MetadataValue::SparseVector(sparse_vector)) =
                    merged_metadata.get(&self.key)
                else {
                    continue;
                };
                let log_sparse_vector: CsVec<f32> = sparse_vector.into();
                // NOTE: We use `1 - query · document` as similarity metrics
                let score = 1.0 - query_sparse_vector.dot(&log_sparse_vector);
                if (max_heap.len() as u32) < self.limit {
                    max_heap.push(RecordMeasure {
                        offset_id: log.get_offset_id(),
                        measure: score,
                    });
                } else if score
                    < max_heap
                        .peek()
                        .map(|record| record.measure)
                        .unwrap_or(f32::MAX)
                {
                    max_heap.pop();
                    max_heap.push(RecordMeasure {
                        offset_id: log.get_offset_id(),
                        measure: score,
                    })
                }
            }
        }
        Ok(SparseLogKnnOutput {
            records: max_heap.into_sorted_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_log::test::{int_as_id, random_embedding, LogGenerator, TEST_EMBEDDING_DIMENSION};
    use chroma_segment::test::TestDistributedSegment;
    use chroma_system::Operator;
    use chroma_types::{Operation, OperationRecord, UpdateMetadataValue};
    use std::collections::HashMap;

    /// Generator for creating log records with sparse vectors in metadata
    fn sparse_vector_generator(offset: usize) -> OperationRecord {
        let mut metadata = HashMap::new();

        // Create sparse vectors with pattern based on offset
        // This creates vectors with increasing values for testing ranking
        metadata.insert(
            "sparse_embedding".to_string(),
            UpdateMetadataValue::SparseVector(SparseVector {
                indices: vec![0, 2, 4],
                values: vec![
                    0.1 * offset as f32,
                    0.2 * offset as f32,
                    0.3 * offset as f32,
                ],
            }),
        );

        // Add some other metadata for completeness
        metadata.insert(
            "category".to_string(),
            UpdateMetadataValue::Str(format!("category_{}", offset % 3)),
        );
        metadata.insert("score".to_string(), UpdateMetadataValue::Int(offset as i64));

        OperationRecord {
            id: int_as_id(offset),
            embedding: Some(random_embedding(TEST_EMBEDDING_DIMENSION)), // Add dummy embedding for materialization
            encoding: None,
            metadata: Some(metadata),
            document: Some(format!("Test document {}", offset)),
            operation: Operation::Upsert,
        }
    }

    async fn setup_sparse_log_input(
        num_records: usize,
        mask: SignedRoaringBitmap,
    ) -> (TestDistributedSegment, SparseLogKnnInput) {
        let test_segment = TestDistributedSegment::new().await;
        let logs = sparse_vector_generator.generate_chunk(1..=num_records);

        let input = SparseLogKnnInput {
            logs,
            blockfile_provider: test_segment.blockfile_provider.clone(),
            record_segment: test_segment.record_segment.clone(),
            mask,
        };

        (test_segment, input)
    }

    #[tokio::test]
    async fn test_sparse_log_knn_simple() {
        let (_test_segment, input) = setup_sparse_log_input(10, SignedRoaringBitmap::full()).await;

        // Query vector that will match with our generated sparse vectors
        let query_vector = SparseVector {
            indices: vec![0, 2, 4],
            values: vec![1.0, 1.0, 1.0],
        };

        let sparse_knn_operator = SparseLogKnn {
            query: query_vector,
            key: "sparse_embedding".to_string(),
            limit: 5,
        };

        // Compute expected scores manually for verification
        // Record i has values [0.1*i, 0.2*i, 0.3*i], dot product with [1, 1, 1] = 0.6*i
        // With new similarity metric: 1.0 - dot_product = 1.0 - 0.6*i
        let mut expected_scores: Vec<(u32, f32)> =
            (1..=10).map(|i| (i as u32, 1.0 - 0.6 * i as f32)).collect();
        expected_scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());

        let output = sparse_knn_operator
            .run(&input)
            .await
            .expect("SparseLogKnn should succeed");

        // Should return top 5 records
        assert_eq!(output.records.len(), 5);

        // Verify the top 5 records are correct (should be records 10, 9, 8, 7, 6 with lowest 1-dot_product scores)
        for (i, record) in output.records.iter().enumerate() {
            let expected = &expected_scores[i];
            assert_eq!(record.offset_id, expected.0);
            assert!((record.measure - expected.1).abs() < 0.001);
        }
    }

    #[tokio::test]
    async fn test_sparse_log_knn_with_filter() {
        // Only include odd offset IDs
        let mask = SignedRoaringBitmap::Include(roaring::RoaringBitmap::from_iter(
            (1..=20).filter(|x| x % 2 == 1),
        ));

        let (_test_segment, input) = setup_sparse_log_input(20, mask).await;

        let query_vector = SparseVector {
            indices: vec![0, 2, 4],
            values: vec![1.0, 1.0, 1.0],
        };

        let sparse_knn_operator = SparseLogKnn {
            query: query_vector,
            key: "sparse_embedding".to_string(),
            limit: 3,
        };

        let output = sparse_knn_operator
            .run(&input)
            .await
            .expect("SparseLogKnn should succeed");

        assert_eq!(output.records.len(), 3);

        // Should only return odd offset IDs, in ascending order of (1 - dot_product)
        // With 1 - dot_product, smaller values are better
        // Records 19, 17, 15 should be the top 3 odd records with most negative (1 - dot_product) scores
        assert_eq!(output.records[0].offset_id, 19);
        assert_eq!(output.records[1].offset_id, 17);
        assert_eq!(output.records[2].offset_id, 15);
    }

    #[tokio::test]
    async fn test_sparse_log_knn_partial_overlap() {
        let (_test_segment, input) = setup_sparse_log_input(5, SignedRoaringBitmap::full()).await;

        // Query vector with different indices - only partial overlap
        let query_vector = SparseVector {
            indices: vec![0, 1, 3], // Only index 0 overlaps with generated vectors
            values: vec![2.0, 1.0, 1.0],
        };

        let sparse_knn_operator = SparseLogKnn {
            query: query_vector,
            key: "sparse_embedding".to_string(),
            limit: 3,
        };

        let output = sparse_knn_operator
            .run(&input)
            .await
            .expect("SparseLogKnn should succeed");

        assert_eq!(output.records.len(), 3);

        // Scores should be based only on index 0 overlap
        // Record i has value 0.1*i at index 0, multiplied by 2.0 from query = 0.2*i
        // With new similarity metric: 1.0 - 0.2*i
        // Record 5: 1.0 - 1.0 = 0.0 (most similar)
        assert_eq!(output.records[0].offset_id, 5);
        assert!((output.records[0].measure - 0.0).abs() < 0.001);
        assert_eq!(output.records[1].offset_id, 4);
        assert!((output.records[1].measure - 0.2).abs() < 0.001);
        assert_eq!(output.records[2].offset_id, 3);
        assert!((output.records[2].measure - 0.4).abs() < 0.001);
    }

    #[tokio::test]
    async fn test_sparse_log_knn_overfetch() {
        let (_test_segment, input) = setup_sparse_log_input(3, SignedRoaringBitmap::full()).await;

        let query_vector = SparseVector {
            indices: vec![0, 2, 4],
            values: vec![1.0, 1.0, 1.0],
        };

        let sparse_knn_operator = SparseLogKnn {
            query: query_vector,
            key: "sparse_embedding".to_string(),
            limit: 10, // Requesting more than available
        };

        let output = sparse_knn_operator
            .run(&input)
            .await
            .expect("SparseLogKnn should succeed");

        // Should only return 3 records even though we asked for 10
        assert_eq!(output.records.len(), 3);

        // Verify they're in ascending order of similarity measure
        // With new metric, record 3 has the most negative score (most similar)
        assert_eq!(output.records[0].offset_id, 3);
        assert_eq!(output.records[1].offset_id, 2);
        assert_eq!(output.records[2].offset_id, 1);
    }

    #[tokio::test]
    async fn test_sparse_log_knn_no_overlap() {
        let (_test_segment, input) = setup_sparse_log_input(5, SignedRoaringBitmap::full()).await;

        // Query vector with completely different indices - no overlap
        let query_vector = SparseVector {
            indices: vec![1, 3, 5], // Generated vectors have indices [0, 2, 4]
            values: vec![1.0, 1.0, 1.0],
        };

        let sparse_knn_operator = SparseLogKnn {
            query: query_vector,
            key: "sparse_embedding".to_string(),
            limit: 3,
        };

        let output = sparse_knn_operator
            .run(&input)
            .await
            .expect("SparseLogKnn should succeed");

        // Should return 3 records but all with score 1.0 (1.0 - 0.0)
        assert_eq!(output.records.len(), 3);

        for record in &output.records {
            assert_eq!(record.measure, 1.0);
        }
    }
}
