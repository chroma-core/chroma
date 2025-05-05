use std::{collections::HashMap, ops::RangeBounds};

use regex_syntax::hir::ClassUnicode;
use roaring::RoaringBitmap;

use super::hir::ChromaHir;

#[derive(Clone, Debug)]
pub enum Literal {
    Char(char),
    Class(ClassUnicode),
}

impl Literal {
    pub fn width(&self) -> usize {
        match self {
            Literal::Char(_) => 1,
            Literal::Class(class_unicode) => class_unicode.iter().map(|range| range.len()).sum(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum LiteralExpr {
    Literal(Vec<Literal>),
    Concat(Vec<LiteralExpr>),
    Alternation(Vec<LiteralExpr>),
}

impl From<ChromaHir> for LiteralExpr {
    fn from(value: ChromaHir) -> Self {
        match value {
            ChromaHir::Empty => Self::Literal(Vec::new()),
            ChromaHir::Literal(literal) => {
                Self::Literal(literal.chars().map(Literal::Char).collect())
            }
            ChromaHir::Class(class_unicode) => Self::Literal(vec![Literal::Class(class_unicode)]),
            ChromaHir::Repetition { min, max: _, sub } => {
                let mut repeat = vec![*sub; min as usize];
                // Append a breakpoint Hir to prevent merge with literal on the right
                repeat.push(ChromaHir::Alternation(vec![ChromaHir::Empty]));
                ChromaHir::Concat(repeat).into()
            }
            ChromaHir::Concat(hirs) => {
                let exprs = hirs.into_iter().fold(Vec::new(), |mut exprs, expr| {
                    match (exprs.last_mut(), expr.into()) {
                        (Some(Self::Literal(literal)), Self::Literal(extra_literal)) => {
                            literal.extend(extra_literal)
                        }
                        (_, expr) => exprs.push(expr),
                    }
                    exprs
                });
                Self::Concat(exprs)
            }
            ChromaHir::Alternation(hirs) => {
                Self::Alternation(hirs.into_iter().map(Into::into).collect())
            }
        }
    }
}

#[async_trait::async_trait]
pub trait NgramLiteralProvider<E, const N: usize = 3> {
    // Return the maximum number of ngram to search on start
    fn initial_beam_width(&self) -> usize;

    // Return the documents containing the ngram and the positions of occurences
    async fn lookup_ngram(&self, ngram: &str) -> Result<HashMap<u32, RoaringBitmap>, E>;

    // Return the (ngram, doc_id, positions) for a range of ngrams and documents
    async fn lookup_ngram_document_range<'me, NgramRange, DocRange>(
        &'me self,
        ngram_range: NgramRange,
        doc_range: DocRange,
    ) -> Result<Vec<(&'me str, u32, RoaringBitmap)>, E>
    where
        NgramRange: RangeBounds<&'me str>,
        DocRange: RangeBounds<u32>;

    // Return the documents containing the literals. The search space is restricted to the documents in the mask if specified
    // If all documents could contain the literals, Ok(None) is returned
    async fn match_literal_with_mask(
        &self,
        mut literals: &[Literal],
        mask: Option<&RoaringBitmap>,
    ) -> Result<Option<RoaringBitmap>, E> {
        if mask.is_some_and(|m| m.is_empty()) {
            return Ok(mask.cloned());
        }

        let mut initial_ngrams = Vec::new();
        let mut initial_position = None;
        for (index, initial_literals) in literals.windows(N).enumerate() {
            let volume = initial_literals
                .iter()
                .fold(1, |acc, lit| acc * lit.width());

            // We would like to find a starting position where the space of ngrams to explore is small enough
            if volume > self.initial_beam_width() {
                continue;
            }

            initial_ngrams =
                initial_literals
                    .iter()
                    .fold(
                        Vec::<Vec<char>>::with_capacity(N),
                        |mut acc, lit| match lit {
                            Literal::Char(c) => {
                                acc.iter_mut().for_each(|s| s.push(*c));
                                acc
                            }
                            Literal::Class(class_unicode) => acc
                                .into_iter()
                                .flat_map(|s| {
                                    class_unicode.iter().flat_map(|r| r.start()..=r.end()).map(
                                        move |c| {
                                            let mut sc = s.clone();
                                            sc.push(c);
                                            sc
                                        },
                                    )
                                })
                                .collect(),
                        },
                    );
            initial_position = Some(index);
            break;
        }

        match initial_position {
            // Drain the initial literals
            Some(pos) => {
                literals = &literals[pos..];
            }
            // There is no initial ngrams to explore, by default we assume the all documents could contain these literals
            None => return Ok(mask.cloned()),
        }

        // ngram suffix -> doc_id -> position
        let mut suffix_doc_pos: HashMap<Vec<char>, HashMap<u32, RoaringBitmap>> = HashMap::new();
        for ngram in initial_ngrams {
            let ngram_string = ngram.iter().collect::<String>();
            let mut doc_pos = self.lookup_ngram(&ngram_string).await?;

            if let Some(whitelist) = mask {
                doc_pos.retain(|doc, _| whitelist.contains(*doc));
            }

            if doc_pos.is_empty() {
                continue;
            }

            let suffix = ngram[1..].to_vec();
            suffix_doc_pos
                .entry(suffix)
                .and_modify(|dp| {
                    doc_pos
                        .iter()
                        .for_each(|(doc, pos)| *dp.entry(*doc).or_default() |= pos);
                })
                .or_insert(doc_pos);
        }

        for literal in literals {
            if suffix_doc_pos.is_empty() {
                break;
            }
            let mut new_suffix_doc_pos: HashMap<Vec<char>, HashMap<u32, RoaringBitmap>> =
                HashMap::new();
            for (mut suffix, doc_pos) in suffix_doc_pos {
                let ngram_ranges = match literal {
                    Literal::Char(c) => {
                        suffix.push(*c);
                        vec![(suffix.clone(), suffix)]
                    }
                    Literal::Class(class_unicode) => class_unicode
                        .iter()
                        .map(|r| {
                            let mut start = suffix.clone();
                            start.push(r.start());
                            let mut end = suffix.clone();
                            end.push(r.end());
                            (start, end)
                        })
                        .collect(),
                };
                let (min_doc_id, max_doc_id) = doc_pos
                    .keys()
                    .fold((u32::MAX, u32::MIN), |(min, max), doc_id| {
                        (min.min(*doc_id), max.max(*doc_id))
                    });
                for (min_ngram, max_ngram) in ngram_ranges {
                    let min_ngram_string = min_ngram.iter().collect::<String>();
                    let max_ngram_string = max_ngram.iter().collect::<String>();
                    let mut ngram_doc_pos = self
                        .lookup_ngram_document_range(
                            min_ngram_string.as_str()..=max_ngram_string.as_str(),
                            min_doc_id..=max_doc_id,
                        )
                        .await?;

                    if let Some(whitelist) = mask {
                        ngram_doc_pos.retain(|(_, doc, _)| whitelist.contains(*doc));
                    }

                    for (ngram, doc_id, new_pos) in ngram_doc_pos {
                        if let Some(pos) = doc_pos.get(&doc_id) {
                            // SAFETY(Sicheng): The RoaringBitmap iterator should be sorted
                            let valid_pos = RoaringBitmap::from_sorted_iter(
                                pos.into_iter()
                                    .filter_map(|p| new_pos.contains(p + 1).then_some(p + 1)),
                            )
                            .expect("RoaringBitmap iterator should be sorted");
                            if !valid_pos.is_empty() {
                                let new_suffix = ngram.chars().skip(1).collect();
                                *new_suffix_doc_pos
                                    .entry(new_suffix)
                                    .or_default()
                                    .entry(doc_id)
                                    .or_default() |= valid_pos;
                            }
                        }
                    }
                }
            }
            suffix_doc_pos = new_suffix_doc_pos;
        }

        let result = suffix_doc_pos
            .into_values()
            .flat_map(|doc_pos| doc_pos.into_keys())
            .collect();
        Ok(Some(result))
    }

    // Return the documents matching the literal expression. The search space is restricted to the documents in the mask if specified
    // If all documents could match the literal expression, Ok(None) is returned
    async fn match_literal_expression_with_mask(
        &self,
        literal_expression: &LiteralExpr,
        mask: Option<&RoaringBitmap>,
    ) -> Result<Option<RoaringBitmap>, E> {
        match literal_expression {
            LiteralExpr::Literal(literals) => self.match_literal_with_mask(literals, mask).await,
            LiteralExpr::Concat(literal_exprs) => {
                let mut result = mask.cloned();
                for expr in literal_exprs {
                    result = self
                        .match_literal_expression_with_mask(expr, result.as_ref())
                        .await?;
                }
                Ok(result)
            }
            LiteralExpr::Alternation(literal_exprs) => {
                let mut result = RoaringBitmap::new();
                for expr in literal_exprs {
                    if let Some(matching_docs) =
                        self.match_literal_expression_with_mask(expr, mask).await?
                    {
                        result |= matching_docs;
                    } else {
                        return Ok(mask.cloned());
                    }
                }
                Ok(Some(result))
            }
        }
    }

    // Return the documents matching the literal expression
    // If all documents could match the literal expression, Ok(None) is returned
    async fn match_literal_expression(
        &self,
        literal_expression: &LiteralExpr,
    ) -> Result<Option<RoaringBitmap>, E> {
        self.match_literal_expression_with_mask(literal_expression, None)
            .await
    }

    fn can_match_exactly(&self, literal_expression: &LiteralExpr) -> bool {
        match literal_expression {
            LiteralExpr::Literal(literals) => literals.windows(N).next().is_some_and(|head| {
                head.iter().fold(1, |acc, lit| acc * lit.width()) <= self.initial_beam_width()
            }),
            LiteralExpr::Concat(_) | LiteralExpr::Alternation(_) => false,
        }
    }
}
