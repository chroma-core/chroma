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

#[derive(Debug, Default)]
struct PrefixSuffixLookupTable<'me> {
    prefix: HashMap<&'me str, Vec<usize>>,
    suffix: HashMap<&'me str, Vec<usize>>,
}

impl<'me> PrefixSuffixLookupTable<'me> {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            prefix: HashMap::with_capacity(capacity),
            suffix: HashMap::with_capacity(capacity),
        }
    }
}

#[async_trait::async_trait]
pub trait NgramLiteralProvider<E, const N: usize = 3> {
    // Return the max branching factor during the search
    fn maximum_branching_factor(&self) -> usize;

    async fn prefetch_ngrams<'me, Ngrams>(&'me self, _ngrams: Ngrams)
    where
        Ngrams: IntoIterator<Item = &'me str> + Send + Sync,
    {
    }

    // Return the (ngram, doc_id, positions) for a range of ngrams
    async fn lookup_ngram_range<'me, NgramRange>(
        &'me self,
        ngram_range: NgramRange,
    ) -> Result<Vec<(&'me str, u32, &'me [u32])>, E>
    where
        NgramRange: Clone + RangeBounds<&'me str> + Send + Sync;

    // Return the documents containing the literals. The search space is restricted to the documents in the mask if specified
    //
    // The literal slice should not be shorter than N, or an empty set will be returned to indicate no document contains a
    // ngram sequence that match the literal sequence
    //
    // The high level algorithm can be separated into the following phases:
    // - Calculate all ngrams that could present in the match
    // - Prefetch all relevant blocks for these ngrams
    // - For each sliding window of size N in the literal sequence:
    //   - Fetch all (ngram, doc, pos) tuples from the index where the ngram can match the window of N literals
    //   - Track the sliding window with minimum number of candidate (ngram, doc, pos) tuples
    //   - Reorganize the ngrams by prefix and suffix into a lookup table
    // - Taking the sliding window with minimum number of candidate (ngram, doc, pos) tuples as the pivot:
    //   - Group the (ngram, doc, pos) tuples by document
    //   - For each document, iterate over the candidate (ngram, pos) tuples:
    //     - Repeatedly use the suffix of the ngram and the prefix lookup table to see if there exists a sequence of ngrams
    //       and positions that aligns all the way to the last sliding window
    //     - Repeatedly use the prefix of the ngram and the suffix lookup table to see if there exists a sequence of ngrams
    //       and positions that aligns all the way to the first sliding window
    //     - If there is such an alignment from the start to the end, add the document to the result and skip to the next document
    //
    // An illustrative example (N=3) for one successful iteration of the final step is presented below (irrelevant info is hidden):
    //                ┌─────┐        ┌─────┐
    //                │ ijk │        │ jkl │
    //                │     │        │     │        ┌─────┐
    //                │ 42──┼────────┼►43  │        │ klm │
    // ┌─────┐        │     │        │     │        │     │
    // │ hij │        │ 54──┼────────┼►55──┼────────┼►56  │
    // │     │        │     │        └─────┘        └─────┘
    // │ 71◄─┼────────┼─72──┼────┐
    // │     │        │     │    │   ┌─────┐        ┌─────┐
    // │ 107 │        │ 108 │    │   │ jkL │        │ kLm │
    // └─────┘        └─────┘    │   │     │        │     │
    //                 pivot     └───┼►73──┼────────┼►74  │
    //                               │     │        │     │
    //                               │ 109 │        │ 110 │
    //                               └─────┘        └─────┘
    // In this iteration, we inspect a document that contains the ngrams at the positions specified above. Starting at the pivot:
    // - We check if position `42` could be part of a match. We check the window at right, which contains `jkl` and `jkL` as potential
    //   candidates. Position `43` is present in ngram `jkl` and aligns with `42`, so we proceed to check further to the right. The
    //   next window contains `klm` and `kLm` as potential candidates but there is no aligned position in either. Thus `42` cannot be
    //   part of a match.
    // - We then check if position `54` could be part of a match. `jkl` contains position `55` and `klm` contains position `56`, thus
    //   we successfully find an aligned sequence of ngrams to the last sliding window. However there is no match to the left of the
    //   pivot, thus `54` cannot be part of a match.
    // - We finally check position `72`, and successfully find an alignment to the last and first sliding window. Thus position `72`
    //   is part of a match, indicating this document matches the literal sequence. We proceed to the next document, even if there
    //   could be another match at position `108`.

    async fn match_literal_with_mask(
        &self,
        literals: &[Literal],
        mask: Option<&HashSet<u32>>,
    ) -> Result<HashSet<u32>, E> {
        if mask.is_some_and(|m| m.is_empty()) {
            return Ok(HashSet::new());
        }

        // Derive the full set of ngrams
        let ngram_vec = literals
            .windows(N)
            .map(|ngram_literals| {
                ngram_literals
                    .iter()
                    .fold(vec![String::with_capacity(N)], |mut acc, lit| match lit {
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
                    })
            })
            .collect::<Vec<_>>();

        if ngram_vec.is_empty() {
            return Ok(HashSet::new());
        }

        self.prefetch_ngrams(
            ngram_vec
                .iter()
                .flat_map(|ngrams| ngrams.iter().map(|ngram| ngram.as_str())),
        )
        .await;

        // Retrieve all ngram posting lists
        let mut ngram_doc_pos_vec = Vec::with_capacity(ngram_vec.iter().map(Vec::len).sum());
        let mut lookup_table_vec = Vec::<PrefixSuffixLookupTable>::with_capacity(ngram_vec.len());
        let mut min_lookup_table_size = usize::MAX;
        let mut min_lookup_table_index = 0;
        for ngrams in &ngram_vec {
            let mut lookup_table = PrefixSuffixLookupTable::with_capacity(ngrams.len());
            let mut lookup_table_size = 0;
            for ngram in ngrams {
                let ngram_doc_pos = self
                    .lookup_ngram_range(ngram.as_str()..=ngram.as_str())
                    .await?;

                if ngram_doc_pos.is_empty() {
                    continue;
                }

                let ngram_doc_pos_index = ngram_doc_pos_vec.len();
                lookup_table_size += ngram_doc_pos.len();
                ngram_doc_pos_vec.push(ngram_doc_pos);

                let prefix = &ngram[..ngram.char_indices().next_back().unwrap_or_default().0];
                let suffix = &ngram[ngram.char_indices().nth(1).unwrap_or_default().0..];
                lookup_table
                    .prefix
                    .entry(prefix)
                    .or_insert_with(|| Vec::with_capacity(ngrams.len()))
                    .push(ngram_doc_pos_index);
                lookup_table
                    .suffix
                    .entry(suffix)
                    .or_insert_with(|| Vec::with_capacity(ngrams.len()))
                    .push(ngram_doc_pos_index);
            }
            let lookup_table_index = lookup_table_vec.len();
            lookup_table_vec.push(lookup_table);
            if lookup_table_size < min_lookup_table_size {
                min_lookup_table_size = lookup_table_size;
                min_lookup_table_index = lookup_table_index;
            }
        }

        // Gather candidate documents
        let min_lookup_table = &lookup_table_vec[min_lookup_table_index];
        let min_ngram_doc_pos_iter = min_lookup_table
            .prefix
            .values()
            .flat_map(|idxs| idxs.iter().map(|idx| &ngram_doc_pos_vec[*idx]));
        let mut candidates =
            HashMap::<_, Vec<_>>::with_capacity(min_ngram_doc_pos_iter.clone().map(Vec::len).sum());
        for (ngram, doc, pos) in min_ngram_doc_pos_iter
            .flatten()
            .filter(|(_, d, _)| mask.is_none() || mask.is_some_and(|m| m.contains(d)))
        {
            candidates
                .entry(*doc)
                .or_insert_with(|| Vec::with_capacity(min_lookup_table.prefix.len()))
                .push((*ngram, *pos));
        }

        // Find a valid trace across lookup tables
        let mut result = HashSet::with_capacity(candidates.len());
        for (doc, pivot_ngram_pos) in candidates {
            for (ngram, pos) in pivot_ngram_pos
                .into_iter()
                .flat_map(|(n, ps)| ps.iter().map(move |p| (n, *p)))
            {
                // Trace to the right of pivot
                let mut suffix_pos_idx =
                    Vec::with_capacity(lookup_table_vec.len() - min_lookup_table_index);
                let suffix_offset = ngram.char_indices().nth(1).unwrap_or_default().0;
                suffix_pos_idx.push((&ngram[suffix_offset..], pos + suffix_offset as u32, 0));
                while let Some((suffix, match_pos, ngram_index)) = suffix_pos_idx.pop() {
                    let focus_lookup_table = match lookup_table_vec
                        .get(min_lookup_table_index + suffix_pos_idx.len() + 1)
                    {
                        Some(table) => table,
                        None => {
                            suffix_pos_idx.push((suffix, match_pos, ngram_index));
                            break;
                        }
                    };
                    let focus_ngram_doc_pos = match focus_lookup_table
                        .prefix
                        .get(suffix)
                        .and_then(|idxs| idxs.get(ngram_index))
                    {
                        Some(idx) => &ngram_doc_pos_vec[*idx],
                        None => continue,
                    };
                    suffix_pos_idx.push((suffix, match_pos, ngram_index + 1));
                    let (focus_ngram, _, pos) =
                        match focus_ngram_doc_pos.binary_search_by_key(&doc, |(_, d, _)| *d) {
                            Ok(idx) => focus_ngram_doc_pos[idx],
                            Err(_) => continue,
                        };
                    if pos.binary_search(&match_pos).is_ok() {
                        let suffix_offset = focus_ngram.char_indices().nth(1).unwrap_or_default().0;
                        suffix_pos_idx.push((
                            &focus_ngram[suffix_offset..],
                            match_pos + suffix_offset as u32,
                            0,
                        ));
                    }
                }
                if suffix_pos_idx.is_empty() {
                    continue;
                }

                // Trace to the left of pivot
                let mut prefix_pos_idx = Vec::with_capacity(min_lookup_table_index + 1);
                let prefix_offset = ngram.char_indices().next_back().unwrap_or_default().0;
                prefix_pos_idx.push((&ngram[..prefix_offset], pos, 0));
                while let Some((prefix, match_pos_with_offset, ngram_index)) = prefix_pos_idx.pop()
                {
                    let focus_lookup_table = match min_lookup_table_index
                        .checked_sub(prefix_pos_idx.len() + 1)
                        .and_then(|lookup_index| lookup_table_vec.get(lookup_index))
                    {
                        Some(table) => table,
                        None => {
                            prefix_pos_idx.push((prefix, match_pos_with_offset, ngram_index));
                            break;
                        }
                    };
                    let focus_ngram_doc_pos = match focus_lookup_table
                        .suffix
                        .get(prefix)
                        .and_then(|idxs| idxs.get(ngram_index))
                    {
                        Some(idx) => &ngram_doc_pos_vec[*idx],
                        None => continue,
                    };
                    prefix_pos_idx.push((prefix, match_pos_with_offset, ngram_index + 1));
                    let (focus_ngram, _, pos) =
                        match focus_ngram_doc_pos.binary_search_by_key(&doc, |(_, d, _)| *d) {
                            Ok(idx) => focus_ngram_doc_pos[idx],
                            Err(_) => continue,
                        };
                    let match_pos = match match_pos_with_offset
                        .checked_sub(focus_ngram.char_indices().nth(1).unwrap_or_default().0 as u32)
                    {
                        Some(pos) => pos,
                        None => continue,
                    };
                    if pos.binary_search(&match_pos).is_ok() {
                        let prefix_offset =
                            focus_ngram.char_indices().next_back().unwrap_or_default().0;
                        prefix_pos_idx.push((&focus_ngram[..prefix_offset], match_pos, 0));
                    }
                }
                if !prefix_pos_idx.is_empty() {
                    result.insert(doc);
                    break;
                }
            }
        }
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
