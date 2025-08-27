use std::{
    collections::{HashMap, HashSet},
    ops::{Add, Div, Mul, Sub},
};

use async_trait::async_trait;
use chroma_blockstore::provider::BlockfileProvider;
use chroma_error::{ChromaError, ErrorCodes};
use chroma_system::Operator;
use chroma_types::operator::{Rank, RecordMeasure, Score};
use thiserror::Error;

struct ScoreProvider<'me> {
    ranks: &'me HashMap<Rank, Vec<RecordMeasure>>,
}

struct ScoreDomain {
    support: HashMap<u32, f32>,
    default: Option<f32>,
}

impl ScoreDomain {
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
            (Some(left_default), Some(right_default)) => ScoreDomain {
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
            (Some(left_default), None) => ScoreDomain {
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
            (None, Some(right_default)) => ScoreDomain {
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
            (None, None) => ScoreDomain {
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

impl ScoreProvider<'_> {
    fn eval(&self, score: Score) -> ScoreDomain {
        match score {
            Score::Absolute(score) => self.eval(*score).map(f32::abs),
            Score::Division { left, right } => {
                ScoreDomain::merge(self.eval(*left), self.eval(*right), f32::div)
            }
            Score::Exponentiation(score) => self.eval(*score).map(f32::exp),
            Score::Logarithm(score) => self.eval(*score).map(f32::ln),
            Score::Maximum(scores) => scores
                .into_iter()
                .map(|score| self.eval(score))
                .fold(ScoreDomain::flat(f32::MIN), |accumulate_domain, domain| {
                    ScoreDomain::merge(accumulate_domain, domain, f32::max)
                }),
            Score::Minimum(scores) => scores
                .into_iter()
                .map(|score| self.eval(score))
                .fold(ScoreDomain::flat(f32::MAX), |accumulate_domain, domain| {
                    ScoreDomain::merge(accumulate_domain, domain, f32::min)
                }),
            Score::Multiplication(scores) => scores
                .into_iter()
                .map(|score| self.eval(score))
                .fold(ScoreDomain::flat(1.0), |accumulate_domain, domain| {
                    ScoreDomain::merge(accumulate_domain, domain, f32::mul)
                }),
            Score::Rank {
                default,
                ordinal,
                source,
            } => {
                let records = self.ranks.get(&source).cloned().unwrap_or_default();
                let support = records
                    .into_iter()
                    .enumerate()
                    .map(|(index, RecordMeasure { offset_id, measure })| {
                        (offset_id, if ordinal { index as f32 } else { measure })
                    })
                    .collect();
                ScoreDomain { support, default }
            }
            Score::Subtraction { left, right } => {
                ScoreDomain::merge(self.eval(*left), self.eval(*right), f32::sub)
            }
            Score::Summation(scores) => scores
                .into_iter()
                .map(|score| self.eval(score))
                .fold(ScoreDomain::flat(0.0), |accumulate_domain, domain| {
                    ScoreDomain::merge(accumulate_domain, domain, f32::add)
                }),
            Score::Value(val) => ScoreDomain::flat(val),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScoreInput {
    pub blockfile_provider: BlockfileProvider,
    pub ranks: HashMap<Rank, Vec<RecordMeasure>>,
}

#[derive(Clone, Debug)]
pub struct ScoreOutput {
    pub scores: Vec<RecordMeasure>,
}

#[derive(Error, Debug)]
#[error("Score error (unreachable)")]
pub struct ScoreError;

impl ChromaError for ScoreError {
    fn code(&self) -> ErrorCodes {
        ErrorCodes::Internal
    }
}

#[async_trait]
impl Operator<ScoreInput, ScoreOutput> for Score {
    type Error = ScoreError;

    async fn run(&self, input: &ScoreInput) -> Result<ScoreOutput, ScoreError> {
        let score_provider = ScoreProvider {
            ranks: &input.ranks,
        };
        let score_domain = score_provider.eval(self.clone());
        let mut scores = score_domain
            .support
            .into_iter()
            .map(|(offset_id, measure)| RecordMeasure { offset_id, measure })
            .collect::<Vec<_>>();
        scores.sort_unstable();
        scores.reverse();
        Ok(ScoreOutput { scores })
    }
}
