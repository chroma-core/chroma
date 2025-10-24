use std::{
    collections::{HashMap, HashSet},
    ops::{Add, Div, Mul, Sub},
};

use async_trait::async_trait;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::operator::{RankExpr, RecordMeasure};
use thiserror::Error;

// NOTE: `RankDomain` represents evaluated scores for records
// - `support`: scores of specific records
// - `default`: scores of records not specified in `support`
//    where `None` suggests no other record is considered for evaluation
struct RankDomain {
    support: HashMap<u32, f32>,
    default: Option<f32>,
}

impl RankDomain {
    fn flat(default: f32) -> Self {
        Self {
            support: HashMap::new(),
            default: Some(default),
        }
    }
    fn map(self, op: impl Fn(f32) -> f32) -> Self {
        Self {
            support: self.support.into_iter().map(|(k, v)| (k, op(v))).collect(),
            default: self.default.map(op),
        }
    }
    fn merge(left: Self, right: Self, op: impl Fn(f32, f32) -> f32) -> Self {
        let left_base = left.support.keys().cloned().collect::<HashSet<_>>();
        let right_base = right.support.keys().cloned().collect::<HashSet<_>>();
        match (left.default, right.default) {
            (Some(left_default), Some(right_default)) => RankDomain {
                support: (&left_base | &right_base)
                    .into_iter()
                    .map(|id| {
                        (
                            id,
                            op(
                                left.support.get(&id).cloned().unwrap_or(left_default),
                                right.support.get(&id).cloned().unwrap_or(right_default),
                            ),
                        )
                    })
                    .collect(),
                default: Some(op(left_default, right_default)),
            },
            (Some(left_default), None) => RankDomain {
                support: right
                    .support
                    .into_iter()
                    .map(|(id, right_value)| {
                        (
                            id,
                            op(
                                left.support.get(&id).cloned().unwrap_or(left_default),
                                right_value,
                            ),
                        )
                    })
                    .collect(),
                default: None,
            },
            (None, Some(right_default)) => RankDomain {
                support: left
                    .support
                    .into_iter()
                    .map(|(id, left_value)| {
                        (
                            id,
                            op(
                                left_value,
                                right.support.get(&id).cloned().unwrap_or(right_default),
                            ),
                        )
                    })
                    .collect(),
                default: None,
            },
            (None, None) => RankDomain {
                support: (&left_base & &right_base)
                    .into_iter()
                    .filter_map(|id| {
                        let left_val = left.support.get(&id).cloned()?;
                        let right_val = right.support.get(&id).cloned()?;
                        Some((id, op(left_val, right_val)))
                    })
                    .collect(),
                default: None,
            },
        }
    }
}

struct RankProvider<R> {
    knn_result_iter: R,
}

impl<R> RankProvider<R>
where
    R: Iterator<Item = Vec<RecordMeasure>>,
{
    fn eval(&mut self, expr: RankExpr) -> RankDomain {
        match expr {
            RankExpr::Absolute(expr) => self.eval(*expr).map(f32::abs),
            RankExpr::Division { left, right } => {
                RankDomain::merge(self.eval(*left), self.eval(*right), f32::div)
            }
            RankExpr::Exponentiation(expr) => self.eval(*expr).map(f32::exp),
            RankExpr::Logarithm(expr) => self.eval(*expr).map(f32::ln),
            RankExpr::Maximum(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(f32::MIN), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::max)
                }),
            RankExpr::Minimum(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(f32::MAX), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::min)
                }),
            RankExpr::Multiplication(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(1.0), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::mul)
                }),
            RankExpr::Knn {
                query: _,
                key: _,
                limit: _,
                default,
                return_rank,
            } => {
                let support = self
                    .knn_result_iter
                    .next()
                    .unwrap_or_default()
                    .into_iter()
                    .enumerate()
                    .map(|(index, RecordMeasure { offset_id, measure })| {
                        (offset_id, if return_rank { index as f32 } else { measure })
                    })
                    .collect();
                RankDomain { support, default }
            }
            RankExpr::Subtraction { left, right } => {
                RankDomain::merge(self.eval(*left), self.eval(*right), f32::sub)
            }
            RankExpr::Summation(exprs) => exprs
                .into_iter()
                .map(|expr| self.eval(expr))
                .fold(RankDomain::flat(0.0), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::add)
                }),
            RankExpr::Value(val) => RankDomain::flat(val),
        }
    }
}

// NOTE: We assume that the provided vector of knn results are in the DFS order of Rank expression.
#[derive(Clone, Debug)]
pub struct RankInput {
    pub knn_results: Vec<Vec<RecordMeasure>>,
}

#[derive(Clone, Debug)]
pub struct RankOutput {
    pub ranks: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
#[error("Rank error (unreachable)")]
pub struct RankError;

impl ChromaError for RankError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<RankInput, RankOutput> for RankExpr {
    type Error = RankError;

    async fn run(&self, input: &RankInput) -> Result<RankOutput, RankError> {
        let knn_results = input.knn_results.clone();
        let mut rank_provider = RankProvider {
            knn_result_iter: knn_results.into_iter(),
        };
        let rank_domain = rank_provider.eval(self.clone());
        let mut ranks = rank_domain
            .support
            .into_iter()
            .map(|(offset_id, measure)| RecordMeasure { offset_id, measure })
            .collect::<Vec<_>>();
        ranks.sort_unstable();
        Ok(RankOutput { ranks })
    }
}

#[cfg(test)]
mod tests {
    use chroma_types::operator::{Key, KnnQuery};

    use super::*;

    #[tokio::test]
    async fn test_rank_with_knn_results() {
        let query = KnnQuery {
            query: chroma_types::operator::QueryVector::Dense(vec![0.1, 0.2, 0.3]),
            key: Key::Embedding,
            limit: 3,
        };
        let knn_results = vec![vec![
            RecordMeasure {
                offset_id: 1,
                measure: 0.9,
            },
            RecordMeasure {
                offset_id: 2,
                measure: 0.7,
            },
            RecordMeasure {
                offset_id: 3,
                measure: 0.5,
            },
        ]];

        // Test simple KNN rank
        let expr = RankExpr::Knn {
            query: query.query.clone(),
            key: Key::Embedding,
            limit: query.limit,
            default: None,
            return_rank: false,
        };
        let input = RankInput { knn_results };

        let output = expr.run(&input).await.expect("Rank should succeed");
        assert_eq!(output.ranks.len(), 3);
        // After removing .reverse(), results are in ascending order by measure
        assert_eq!(output.ranks[0].offset_id, 3);
        assert_eq!(output.ranks[0].measure, 0.5);
    }

    #[tokio::test]
    async fn test_rank_arithmetic_operations() {
        let query1 = KnnQuery {
            query: chroma_types::operator::QueryVector::Dense(vec![0.1]),
            key: Key::Embedding,
            limit: 2,
        };
        let query2 = KnnQuery {
            query: chroma_types::operator::QueryVector::Sparse(chroma_types::SparseVector {
                indices: vec![0],
                values: vec![1.0],
            }),
            key: Key::field("sparse"),
            limit: 2,
        };
        let mut knn_results = vec![
            vec![
                RecordMeasure {
                    offset_id: 1,
                    measure: 0.8,
                },
                RecordMeasure {
                    offset_id: 2,
                    measure: 0.6,
                },
            ],
            vec![
                RecordMeasure {
                    offset_id: 1,
                    measure: 0.4,
                },
                RecordMeasure {
                    offset_id: 3,
                    measure: 0.2,
                },
            ],
        ];

        // Test summation
        let expr = RankExpr::Summation(vec![
            RankExpr::Knn {
                query: query1.query.clone(),
                key: Key::Embedding,
                limit: query1.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Knn {
                query: query2.query.clone(),
                key: Key::field("sparse"),
                limit: query2.limit,
                default: None,
                return_rank: false,
            },
        ]);
        let input = RankInput {
            knn_results: knn_results.clone(),
        };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Summation results:
        // Only Record 1 appears in both lists: 0.8 + 0.4 = 1.2
        // Records 2 and 3 are filtered out since they don't appear in both lists
        // and both Knn operations have default: None
        assert_eq!(output.ranks.len(), 1);
        assert_eq!(output.ranks[0].offset_id, 1);
        assert_eq!(output.ranks[0].measure, 1.2);

        // Test multiplication with constant
        knn_results.pop();
        let expr = RankExpr::Multiplication(vec![
            RankExpr::Knn {
                query: query1.query.clone(),
                key: Key::Embedding,
                limit: query1.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Value(0.5),
        ]);
        let input = RankInput { knn_results };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Results are in ascending order, so the record with the lowest measure comes first
        // After multiplication by 0.5: record 1 = 0.8 * 0.5 = 0.4, record 2 = 0.6 * 0.5 = 0.3
        assert_eq!(output.ranks[0].offset_id, 2);
        assert_eq!(output.ranks[0].measure, 0.3); // 0.6 * 0.5
    }

    #[tokio::test]
    async fn test_rank_min_max_functions() {
        let query = KnnQuery {
            query: chroma_types::operator::QueryVector::Dense(vec![0.1]),
            key: Key::Embedding,
            limit: 2,
        };
        let knn_results = vec![vec![
            RecordMeasure {
                offset_id: 1,
                measure: 0.8,
            },
            RecordMeasure {
                offset_id: 2,
                measure: 0.3,
            },
        ]];

        // Test max
        let expr = RankExpr::Maximum(vec![
            RankExpr::Knn {
                query: query.query.clone(),
                key: Key::Embedding,
                limit: query.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Value(0.5),
        ]);
        let input = RankInput {
            knn_results: knn_results.clone(),
        };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Results are in ascending order
        assert_eq!(output.ranks[0].offset_id, 2);
        assert_eq!(output.ranks[0].measure, 0.5); // max(0.3, 0.5) = 0.5
        assert_eq!(output.ranks[1].offset_id, 1);
        assert_eq!(output.ranks[1].measure, 0.8); // max(0.8, 0.5) = 0.8

        // Test min
        let expr = RankExpr::Minimum(vec![
            RankExpr::Knn {
                query: query.query.clone(),
                key: Key::Embedding,
                limit: query.limit,
                default: None,
                return_rank: false,
            },
            RankExpr::Value(0.5),
        ]);
        let input = RankInput { knn_results };

        let output = expr.run(&input).await.expect("Rank should succeed");
        // Results are in ascending order
        assert_eq!(output.ranks[0].offset_id, 2);
        assert_eq!(output.ranks[0].measure, 0.3); // min(0.3, 0.5) = 0.3
        assert_eq!(output.ranks[1].offset_id, 1);
        assert_eq!(output.ranks[1].measure, 0.5); // min(0.8, 0.5) = 0.5
    }
}
