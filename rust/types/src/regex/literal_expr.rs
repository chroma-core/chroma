use std::{
    collections::{HashMap, HashSet},
    ops::RangeBounds,
};

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
            ChromaHir::Repetition { min, max, sub } => {
                let mut repeat = vec![*sub; min as usize];
                if max.is_none() || max.is_some_and(|m| m > min) {
                    // Append a breakpoint Hir to prevent merge with literal on the right
                    repeat.push(ChromaHir::Alternation(vec![ChromaHir::Empty]));
                }
                ChromaHir::Concat(repeat).into()
            }
            ChromaHir::Concat(hirs) => {
                let mut exprs = hirs.into_iter().fold(Vec::new(), |mut exprs, expr| {
                    match (exprs.last_mut(), expr.into()) {
                        (Some(Self::Literal(literal)), Self::Literal(extra_literal)) => {
                            literal.extend(extra_literal)
                        }
                        (_, expr) => exprs.push(expr),
                    }
                    exprs
                });
                if exprs.len() > 1 {
                    Self::Concat(exprs)
                } else if let Some(expr) = exprs.pop() {
                    expr
                } else {
                    Self::Literal(Vec::new())
                }
            }
            ChromaHir::Alternation(hirs) => {
                Self::Alternation(hirs.into_iter().map(Into::into).collect())
            }
        }
    }
}

#[async_trait::async_trait]
pub trait NgramLiteralProvider<E, const N: usize = 3> {
    // Return the max branching factor during the search
    fn maximum_branching_factor(&self) -> usize;

    // Return the (ngram, doc_id, positions) for a range of ngrams
    async fn lookup_ngram_range<'me, NgramRange>(
        &'me self,
        ngram_range: NgramRange,
    ) -> Result<Vec<(&'me str, u32, &'me [u32])>, E>
    where
        NgramRange: Clone + RangeBounds<&'me str> + Send + Sync;

    // Return the documents containing the literals. The search space is restricted to the documents in the mask if specified
    // The literal slice must not be shorter than N, else `[...].split_at(N)` will panic
    async fn match_literal_with_mask(
        &self,
        literals: &[Literal],
        mask: Option<&HashSet<u32>>,
    ) -> Result<HashSet<u32>, E> {
        if mask.is_some_and(|m| m.is_empty()) {
            return Ok(HashSet::new());
        }

        let (initial_literals, remaining_literals) = literals.split_at(N);
        let initial_ngrams =
            initial_literals
                .iter()
                .fold(vec![Vec::with_capacity(N)], |mut acc, lit| match lit {
                    Literal::Char(c) => {
                        acc.iter_mut().for_each(|s| s.push(*c));
                        acc
                    }
                    Literal::Class(class_unicode) => {
                        acc.into_iter()
                            .flat_map(|s| {
                                class_unicode.iter().flat_map(|r| r.start()..=r.end()).map(
                                    move |c| {
                                        let mut sc = s.clone();
                                        sc.push(c);
                                        sc
                                    },
                                )
                            })
                            .collect()
                    }
                });

        // ngram suffix -> doc_id -> position
        let mut suffix_doc_pos: HashMap<Vec<char>, HashMap<u32, HashSet<u32>>> = HashMap::new();
        for ngram in initial_ngrams {
            let ngram_string = ngram.iter().collect::<String>();
            let ngram_doc_pos = self
                .lookup_ngram_range(ngram_string.as_str()..=ngram_string.as_str())
                .await?;

            if ngram_doc_pos.is_empty() {
                continue;
            }

            let suffix = ngram[1..].to_vec();
            for (_, doc_id, pos) in ngram_doc_pos {
                if pos.is_empty() {
                    continue;
                }
                if mask.is_none() || mask.is_some_and(|m| m.contains(&doc_id)) {
                    suffix_doc_pos
                        .entry(suffix.clone())
                        .or_default()
                        .entry(doc_id)
                        .or_default()
                        .extend(pos);
                }
            }
        }

        for literal in remaining_literals {
            if suffix_doc_pos.is_empty() {
                break;
            }
            let mut new_suffix_doc_pos: HashMap<Vec<char>, HashMap<u32, HashSet<u32>>> =
                HashMap::new();
            for (mut suffix, doc_pos) in suffix_doc_pos {
                let ngram_ranges = match literal {
                    Literal::Char(literal_char) => {
                        suffix.push(*literal_char);
                        vec![(suffix.clone(), suffix)]
                    }
                    Literal::Class(class_unicode) => class_unicode
                        .iter()
                        .map(|r| {
                            let mut min_ngram = suffix.clone();
                            min_ngram.push(r.start());
                            let mut max_ngram = suffix.clone();
                            max_ngram.push(r.end());
                            (min_ngram, max_ngram)
                        })
                        .collect(),
                };

                for (min_ngram, max_ngram) in ngram_ranges {
                    let min_ngram_string = min_ngram.iter().collect::<String>();
                    let max_ngram_string = max_ngram.iter().collect::<String>();
                    let ngram_doc_pos = self
                        .lookup_ngram_range(min_ngram_string.as_str()..=max_ngram_string.as_str())
                        .await?;
                    for (ngram, doc_id, next_pos) in ngram_doc_pos {
                        if let Some(pos) = doc_pos.get(&doc_id) {
                            let next_pos_set: HashSet<&u32> = HashSet::from_iter(next_pos);
                            let mut valid_next_pos = pos
                                .iter()
                                .filter_map(|p| next_pos_set.contains(&(p + 1)).then_some(p + 1))
                                .peekable();
                            if valid_next_pos.peek().is_some() {
                                let new_suffix = ngram.chars().skip(1).collect();
                                new_suffix_doc_pos
                                    .entry(new_suffix)
                                    .or_default()
                                    .entry(doc_id)
                                    .or_default()
                                    .extend(valid_next_pos);
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
        Ok(result)
    }

    // Return the documents matching the literal expression. The search space is restricted to the documents in the mask if specified
    // If all documents could match the literal expression, Ok(None) is returned
    async fn match_literal_expression_with_mask(
        &self,
        literal_expression: &LiteralExpr,
        mask: Option<&HashSet<u32>>,
    ) -> Result<Option<HashSet<u32>>, E> {
        match literal_expression {
            LiteralExpr::Literal(literals) => {
                let mut result = mask.cloned();
                for query in literals.split(|lit| lit.width() > self.maximum_branching_factor()) {
                    if result.as_ref().is_some_and(|m| m.is_empty()) {
                        break;
                    }
                    if query.len() >= N {
                        result = Some(self.match_literal_with_mask(query, result.as_ref()).await?);
                    }
                }
                Ok(result)
            }
            LiteralExpr::Concat(literal_exprs) => {
                let mut result = mask.cloned();
                for expr in literal_exprs {
                    if result.as_ref().is_some_and(|m| m.is_empty()) {
                        break;
                    }
                    result = self
                        .match_literal_expression_with_mask(expr, result.as_ref())
                        .await?;
                }
                Ok(result)
            }
            LiteralExpr::Alternation(literal_exprs) => {
                let mut result = Vec::new();
                for expr in literal_exprs {
                    if let Some(matching_docs) =
                        self.match_literal_expression_with_mask(expr, mask).await?
                    {
                        result.extend(matching_docs);
                    } else {
                        return Ok(mask.cloned());
                    }
                }
                Ok(Some(HashSet::from_iter(result)))
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
            .map(|res| res.map(RoaringBitmap::from_iter))
    }

    fn can_match_exactly(&self, literal_expression: &LiteralExpr) -> bool {
        match literal_expression {
            LiteralExpr::Literal(literals) => literals
                .iter()
                .all(|c| c.width() <= self.maximum_branching_factor()),
            LiteralExpr::Concat(_) | LiteralExpr::Alternation(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, ops::RangeBounds};

    use regex_syntax::hir::{ClassUnicode, ClassUnicodeRange};
    use roaring::RoaringBitmap;

    use crate::regex::literal_expr::LiteralExpr;

    use super::{Literal, NgramLiteralProvider};

    struct StaticLiteralProvider {
        #[allow(clippy::type_complexity)]
        inverted_literal_index: Vec<(String, Vec<(u32, Vec<u32>)>)>,
    }

    #[async_trait::async_trait]
    impl NgramLiteralProvider<()> for StaticLiteralProvider {
        fn maximum_branching_factor(&self) -> usize {
            6
        }

        async fn lookup_ngram_range<'me, NgramRange>(
            &'me self,
            ngram_range: NgramRange,
        ) -> Result<Vec<(&'me str, u32, &'me [u32])>, ()>
        where
            NgramRange: Clone + RangeBounds<&'me str> + Send + Sync,
        {
            Ok(self
                .inverted_literal_index
                .iter()
                .filter(|(literal, _)| ngram_range.contains(&literal.as_str()))
                .flat_map(|(literal, m)| {
                    m.iter()
                        .map(|(doc, pos)| (literal.as_str(), *doc, pos.as_slice()))
                })
                .collect())
        }
    }

    #[tokio::test]
    async fn test_simple_literal_match() {
        let provider = StaticLiteralProvider {
            inverted_literal_index: vec![
                ("aaa".to_string(), vec![(0, vec![0])]),
                ("aab".to_string(), vec![(0, vec![])]),
            ],
        };

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[Literal::Char('a'), Literal::Char('a'), Literal::Char('a')],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([0])
        );

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[Literal::Char('a'), Literal::Char('a'), Literal::Char('b')],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([])
        );

        let case_insensitive_a = ClassUnicode::new([
            ClassUnicodeRange::new('a', 'a'),
            ClassUnicodeRange::new('A', 'A'),
        ]);
        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[
                        Literal::Class(case_insensitive_a.clone()),
                        Literal::Char('a'),
                        Literal::Char('a')
                    ],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([0])
        );
    }

    #[tokio::test]
    async fn test_long_literal_match() {
        let provider = StaticLiteralProvider {
            inverted_literal_index: vec![
                (
                    "abc".to_string(),
                    vec![(0, vec![0, 6]), (1, vec![10, 16]), (2, vec![3])],
                ),
                (
                    "bcd".to_string(),
                    vec![(0, vec![1, 7]), (1, vec![11, 27]), (3, vec![4])],
                ),
                ("cde".to_string(), vec![(0, vec![8, 20]), (1, vec![12, 28])]),
                ("def".to_string(), vec![(0, vec![9, 21])]),
                ("deF".to_string(), vec![(1, vec![29, 40])]),
            ],
        };

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[Literal::Char('a'), Literal::Char('b'), Literal::Char('c')],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([0, 1, 2])
        );

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[
                        Literal::Char('a'),
                        Literal::Char('b'),
                        Literal::Char('c'),
                        Literal::Char('d'),
                    ],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([0, 1])
        );

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[
                        Literal::Char('a'),
                        Literal::Char('b'),
                        Literal::Char('c'),
                        Literal::Char('d'),
                        Literal::Char('e'),
                    ],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([0, 1])
        );

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[
                        Literal::Char('a'),
                        Literal::Char('b'),
                        Literal::Char('c'),
                        Literal::Char('d'),
                        Literal::Char('e'),
                        Literal::Char('f'),
                    ],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([0])
        );

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[
                        Literal::Char('a'),
                        Literal::Char('b'),
                        Literal::Char('c'),
                        Literal::Char('d'),
                        Literal::Char('e'),
                        Literal::Char('F'),
                    ],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([])
        );

        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[
                        Literal::Char('b'),
                        Literal::Char('c'),
                        Literal::Char('d'),
                        Literal::Char('e'),
                        Literal::Char('F'),
                    ],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([1])
        );

        let case_insensitive_f = ClassUnicode::new([
            ClassUnicodeRange::new('f', 'f'),
            ClassUnicodeRange::new('F', 'F'),
        ]);
        assert_eq!(
            provider
                .match_literal_with_mask(
                    &[
                        Literal::Char('b'),
                        Literal::Char('c'),
                        Literal::Char('d'),
                        Literal::Char('e'),
                        Literal::Class(case_insensitive_f),
                    ],
                    None
                )
                .await
                .unwrap(),
            HashSet::from_iter([0, 1])
        );
    }

    #[tokio::test]
    async fn test_literal_expression_match() {
        let provider = StaticLiteralProvider {
            inverted_literal_index: vec![
                (
                    "abc".to_string(),
                    vec![(0, vec![0, 6]), (1, vec![10, 16]), (2, vec![3])],
                ),
                (
                    "def".to_string(),
                    vec![(0, vec![9, 21]), (2, vec![30]), (3, vec![7])],
                ),
            ],
        };

        assert_eq!(
            provider
                .match_literal_expression(&LiteralExpr::Concat(vec![
                    LiteralExpr::Literal(vec![
                        Literal::Char('a'),
                        Literal::Char('b'),
                        Literal::Char('c'),
                    ]),
                    LiteralExpr::Literal(vec![
                        Literal::Char('d'),
                        Literal::Char('e'),
                        Literal::Char('f'),
                    ])
                ]))
                .await
                .unwrap(),
            Some(RoaringBitmap::from_sorted_iter([0, 2]).unwrap())
        );

        assert_eq!(
            provider
                .match_literal_expression(&LiteralExpr::Alternation(vec![
                    LiteralExpr::Literal(vec![
                        Literal::Char('a'),
                        Literal::Char('b'),
                        Literal::Char('c'),
                    ]),
                    LiteralExpr::Literal(vec![
                        Literal::Char('d'),
                        Literal::Char('e'),
                        Literal::Char('f'),
                    ])
                ]))
                .await
                .unwrap(),
            Some(RoaringBitmap::from_sorted_iter([0, 1, 2, 3]).unwrap())
        );

        // Literal is ignored if it is too wide (i.e. can match too many characters)
        let digit = ClassUnicode::new([ClassUnicodeRange::new('0', '9')]);
        assert_eq!(
            provider
                .match_literal_expression(&LiteralExpr::Literal(vec![
                    Literal::Char('a'),
                    Literal::Char('b'),
                    Literal::Char('c'),
                    Literal::Class(digit),
                    Literal::Char('d'),
                    Literal::Char('e'),
                    Literal::Char('f'),
                ]))
                .await
                .unwrap(),
            Some(RoaringBitmap::from_sorted_iter([0, 2]).unwrap())
        );
    }
}
