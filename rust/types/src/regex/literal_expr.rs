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
    async fn lookup_ngram<'ngram>(
        &self,
        ngram: &'ngram [char; N],
    ) -> Result<HashMap<u32, RoaringBitmap>, E>;

    // Return the positions of occurences of a range of ngram in the document
    async fn lookup_ngram_range_in_document<'ngram, NgramRange>(
        &self,
        ngram_range: NgramRange,
        document_id: u32,
    ) -> Result<Vec<([char; N], RoaringBitmap)>, E>
    where
        NgramRange: RangeBounds<&'ngram [char; N]>;

    // Return the documents containing the literals and the potential matching positions
    // If all documents could contain the literals, Ok(None) is returned
    async fn match_literal(
        &self,
        mut literals: Vec<Literal>,
    ) -> Result<Option<HashMap<u32, RoaringBitmap>>, E> {
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
                    .fold(Vec::<Vec<char>>::new(), |mut acc, lit| match lit {
                        Literal::Char(c) => {
                            acc.iter_mut().for_each(|s| s.push(*c));
                            acc
                        }
                        Literal::Class(class_unicode) => acc
                            .into_iter()
                            .flat_map(|s| {
                                class_unicode.iter().flat_map(|r| r.start()..r.end()).map(
                                    move |c| {
                                        let mut sc = s.clone();
                                        sc.push(c);
                                        sc
                                    },
                                )
                            })
                            .collect(),
                    });
            initial_position = Some(index);
            break;
        }

        match initial_position {
            // Drain the initial literals
            Some(pos) => {
                literals.drain(..pos);
            }
            // There is no initial ngrams to explore, by default we assume the all documents could contain these literals
            None => return Ok(None),
        }

        // ngram suffix -> doc_id -> position
        let mut ngram_suffix_mapping: HashMap<Vec<char>, HashMap<u32, RoaringBitmap>> =
            HashMap::new();
        for ngram in initial_ngrams {
            // SAFETY(sicheng): The window function above guarantees each ngram should have exactly N chars
            let ngram_doc_pos = self
                .lookup_ngram(
                    ngram
                        .as_slice()
                        .try_into()
                        .expect("Ngram should have the right size"),
                )
                .await?;

            if ngram_doc_pos.is_empty() {
                continue;
            }

            let ngram_suffix = ngram[1..].to_vec();
            ngram_suffix_mapping
                .entry(ngram_suffix)
                .and_modify(|doc_pos| {
                    ngram_doc_pos
                        .iter()
                        .for_each(|(doc, pos)| *doc_pos.entry(*doc).or_default() |= pos);
                })
                .or_insert(ngram_doc_pos);
        }

        for literal in &literals {
            let new_ngram_suffix_mapping = HashMap::new();
            for (mut ngram_suffix, doc_pos) in ngram_suffix_mapping {
                let ngram_ranges = match literal {
                    Literal::Char(c) => {
                        ngram_suffix.push(*c);
                        vec![(ngram_suffix.clone(), ngram_suffix)]
                    }
                    Literal::Class(class_unicode) => class_unicode
                        .iter()
                        .map(|r| {
                            let mut start = ngram_suffix.clone();
                            start.push(r.start());
                            let mut end = ngram_suffix.clone();
                            end.push(r.end());
                            (start, end)
                        })
                        .collect(),
                };
                for (start_ngram, end_ngram) in ngram_ranges {
                    // SAFETY(sicheng): The ngram is always constructed from suffix with length N-1 and an additional char
                    let start = <[char; N]>::try_from(start_ngram)
                        .expect("Ngram should have the right size");
                    let end =
                        <[char; N]>::try_from(end_ngram).expect("Ngram should have the right size");
                    for (doc_id, pos) in &doc_pos {
                        for (ngram, doc_pos) in self
                            .lookup_ngram_range_in_document(&start..&end, *doc_id)
                            .await?
                        {}
                    }
                }
            }
            ngram_suffix_mapping = new_ngram_suffix_mapping;
        }

        Ok(None)
    }
}
