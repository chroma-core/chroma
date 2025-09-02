use std::{
    collections::{HashMap, HashSet},
    ops::{Add, Div, Mul, Sub},
};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::operator::{KnnQuery, Rank, RecordMeasure};
use thiserror::Error;

struct RankProvider<'me> {
    knn_results: &'me HashMap<KnnQuery, Vec<RecordMeasure>>,
}

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

impl RankProvider<'_> {
    fn eval(&self, rank: Rank) -> RankDomain {
        match rank {
            Rank::Absolute(rank) => self.eval(*rank).map(f32::abs),
            Rank::Division { left, right } => {
                RankDomain::merge(self.eval(*left), self.eval(*right), f32::div)
            }
            Rank::Exponentiation(rank) => self.eval(*rank).map(f32::exp),
            Rank::Logarithm(rank) => self.eval(*rank).map(f32::ln),
            Rank::Maximum(ranks) => ranks
                .into_iter()
                .map(|rank| self.eval(rank))
                .fold(RankDomain::flat(f32::MIN), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::max)
                }),
            Rank::Minimum(ranks) => ranks
                .into_iter()
                .map(|rank| self.eval(rank))
                .fold(RankDomain::flat(f32::MAX), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::min)
                }),
            Rank::Multiplication(ranks) => ranks
                .into_iter()
                .map(|rank| self.eval(rank))
                .fold(RankDomain::flat(1.0), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::mul)
                }),
            Rank::Knn {
                embedding,
                key,
                limit,
                default,
                ordinal,
            } => {
                let knn_query = KnnQuery {
                    embedding,
                    key,
                    limit,
                };
                let support = self
                    .knn_results
                    .get(&knn_query)
                    .map(|records| {
                        records
                            .iter()
                            .enumerate()
                            .map(|(index, &RecordMeasure { offset_id, measure })| {
                                (offset_id, if ordinal { index as f32 } else { measure })
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                RankDomain { support, default }
            }
            Rank::Subtraction { left, right } => {
                RankDomain::merge(self.eval(*left), self.eval(*right), f32::sub)
            }
            Rank::Summation(ranks) => ranks
                .into_iter()
                .map(|rank| self.eval(rank))
                .fold(RankDomain::flat(0.0), |accumulate_domain, domain| {
                    RankDomain::merge(accumulate_domain, domain, f32::add)
                }),
            Rank::Value(val) => RankDomain::flat(val),
        }
    }
}

#[derive(Clone, Debug)]
pub struct RankInput {
    pub blockfile_provider: BlockfileProvider,
    pub knn_results: HashMap<KnnQuery, Vec<RecordMeasure>>,
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
impl Operator<RankInput, RankOutput> for Rank {
    type Error = RankError;

    async fn run(&self, input: &RankInput) -> Result<RankOutput, RankError> {
        let rank_provider = RankProvider {
            knn_results: &input.knn_results,
        };
        let rank_domain = rank_provider.eval(self.clone());
        let mut ranks = rank_domain
            .support
            .into_iter()
            .map(|(offset_id, measure)| RecordMeasure { offset_id, measure })
            .collect::<Vec<_>>();
        ranks.sort_unstable();
        ranks.reverse();
        Ok(RankOutput { ranks })
    }
}
