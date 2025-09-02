use async_trait::async_trait;

use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::operator::{Merge, RecordMeasure};
use thiserror::Error;

#[derive(Debug)]
pub struct SparseKnnMergeInput {
    pub batch_measures: Vec<Vec<RecordMeasure>>,
}

#[derive(Debug, Default)]
pub struct SparseKnnMergeOutput {
    pub measures: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
#[error("Knn merge error (unreachable)")]
pub struct SparseKnnMergeError;

impl ChromaError for SparseKnnMergeError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<SparseKnnMergeInput, SparseKnnMergeOutput> for Merge {
    type Error = SparseKnnMergeError;

    async fn run(
        &self,
        input: &SparseKnnMergeInput,
    ) -> Result<SparseKnnMergeOutput, SparseKnnMergeError> {
        Ok(SparseKnnMergeOutput {
            measures: self.merge(input.batch_measures.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chroma_system::Operator;

    fn setup_sparse_knn_merge_input() -> SparseKnnMergeInput {
        SparseKnnMergeInput {
            batch_measures: vec![
                vec![
                    RecordMeasure {
                        offset_id: 3,
                        measure: 0.9,
                    },
                    RecordMeasure {
                        offset_id: 7,
                        measure: 0.7,
                    },
                    RecordMeasure {
                        offset_id: 12,
                        measure: 0.5,
                    },
                ],
                vec![
                    RecordMeasure {
                        offset_id: 1,
                        measure: 0.95,
                    },
                    RecordMeasure {
                        offset_id: 4,
                        measure: 0.6,
                    },
                    RecordMeasure {
                        offset_id: 10,
                        measure: 0.4,
                    },
                ],
                vec![
                    RecordMeasure {
                        offset_id: 2,
                        measure: 0.85,
                    },
                    RecordMeasure {
                        offset_id: 13,
                        measure: 0.3,
                    },
                ],
            ],
        }
    }

    #[tokio::test]
    async fn test_sparse_knn_merge() {
        let input = setup_sparse_knn_merge_input();
        let merge_operator = Merge { k: 5 };

        let output = merge_operator
            .run(&input)
            .await
            .expect("SparseKnnMerge should not fail");

        // Should return top 5 records sorted by measure (descending)
        assert_eq!(output.measures.len(), 5);
        assert_eq!(output.measures[0].offset_id, 1);
        assert_eq!(output.measures[0].measure, 0.95);
        assert_eq!(output.measures[1].offset_id, 3);
        assert_eq!(output.measures[1].measure, 0.9);
        assert_eq!(output.measures[2].offset_id, 2);
        assert_eq!(output.measures[2].measure, 0.85);
        assert_eq!(output.measures[3].offset_id, 7);
        assert_eq!(output.measures[3].measure, 0.7);
        assert_eq!(output.measures[4].offset_id, 4);
        assert_eq!(output.measures[4].measure, 0.6);
    }

    #[tokio::test]
    async fn test_sparse_knn_merge_overfetch() {
        let input = setup_sparse_knn_merge_input();
        let merge_operator = Merge { k: 20 };

        let output = merge_operator
            .run(&input)
            .await
            .expect("SparseKnnMerge should not fail");

        // Should return all 8 records when k > total records
        assert_eq!(output.measures.len(), 8);
        // Verify they're sorted by measure
        for i in 1..output.measures.len() {
            assert!(output.measures[i - 1].measure >= output.measures[i].measure);
        }
    }
}
