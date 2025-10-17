use std::collections::HashSet;

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
    prefix: Vec<(&'me str, usize)>,
    suffix: Vec<(&'me str, usize)>,
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
    async fn lookup_ngram<'me>(
        &'me self,
        ngram: &'me str,
    ) -> Result<Box<dyn Iterator<Item = (u32, &'me [u32])> + Send + Sync + 'me>, E>;

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
        let ngram_doc_pos_len = ngram_vec.iter().map(Vec::len).sum();
        let mut ngram_doc_pos_vec = Vec::with_capacity(ngram_doc_pos_len);
        let mut lookup_table_vec = Vec::<PrefixSuffixLookupTable>::with_capacity(ngram_vec.len());
        let mut min_lookup_table_size = usize::MAX;
        let mut min_lookup_table_index = 0;
        for ngrams in &ngram_vec {
            let mut lookup_table = PrefixSuffixLookupTable::default();
            let mut lookup_table_size = 0;
            for ngram in ngrams {
                let mut doc_pos = self.lookup_ngram(ngram).await?.peekable();

                if doc_pos.peek().is_none() {
                    continue;
                }

                let ngram_doc_pos_index = ngram_doc_pos_vec.len();
                lookup_table_size += doc_pos.size_hint().0;
                ngram_doc_pos_vec.push((ngram, doc_pos));

                let prefix = &ngram[..ngram.char_indices().next_back().unwrap_or_default().0];
                let suffix = &ngram[ngram.char_indices().nth(1).unwrap_or_default().0..];
                lookup_table.prefix.push((prefix, ngram_doc_pos_index));
                lookup_table.suffix.push((suffix, ngram_doc_pos_index));
            }
            lookup_table.prefix.sort_unstable();
            lookup_table.suffix.sort_unstable();
            let lookup_table_index = lookup_table_vec.len();
            lookup_table_vec.push(lookup_table);
            if lookup_table_size < min_lookup_table_size {
                min_lookup_table_size = lookup_table_size;
                min_lookup_table_index = lookup_table_index;
            }
        }

        // Gather candidate documents
        let mut candidates = Vec::with_capacity(min_lookup_table_size);
        for ngram in &ngram_vec[min_lookup_table_index] {
            candidates.extend(self.lookup_ngram(ngram).await?.filter_map(|(doc, pos)| {
                (mask.is_none() || mask.is_some_and(|m| m.contains(&doc)))
                    .then_some((ngram, doc, pos))
            }));
        }
        candidates.sort_unstable_by_key(|(_, doc, _)| *doc);

        // Find a valid trace across lookup tables
        let mut result = HashSet::with_capacity(
            candidates.len() / lookup_table_vec[min_lookup_table_index].prefix.len().max(1),
        );
        for pivot_ngram_pos_vec in candidates.chunk_by(|(_, left, _), (_, right, _)| left == right)
        {
            for (ngram, doc, pos) in pivot_ngram_pos_vec
                .iter()
                .flat_map(|(ngram, doc, pos)| pos.iter().map(move |p| (ngram, *doc, *p)))
            {
                // Trace to the right of pivot
                // `suffix_pos_idx_stack` stores a stack of (
                //   <suffix of current ngram>,
                //   <expected position of next ngram>,
                //   <index of next ngram to check in the prefix lookup table>,
                // )
                let mut suffix_pos_idx_stack =
                    Vec::with_capacity(lookup_table_vec.len() - min_lookup_table_index);
                let suffix_offset = ngram.char_indices().nth(1).unwrap_or_default().0;
                suffix_pos_idx_stack.push((
                    &ngram[suffix_offset..],
                    pos + suffix_offset as u32,
                    None,
                ));
                while let Some((suffix, match_pos, ngram_index)) = suffix_pos_idx_stack.pop() {
                    // Find the next lookup table to the right
                    let focus_lookup_table = match lookup_table_vec
                        .get(min_lookup_table_index + suffix_pos_idx_stack.len() + 1)
                    {
                        Some(table) => table,
                        None => {
                            // There is no more lookup table on the right
                            // We have found a valid trace to the right
                            suffix_pos_idx_stack.push((suffix, match_pos, ngram_index));
                            break;
                        }
                    };
                    // Find the next ngram to check
                    let focus_ngram_prefix_index = match ngram_index {
                        Some(idx) => idx,
                        None if focus_lookup_table.prefix.len() <= 1 => 0,
                        None => focus_lookup_table
                            .prefix
                            .partition_point(|(prefix, _)| prefix < &suffix),
                    };
                    let focus_ngram_doc_pos_idx = match focus_lookup_table
                        .prefix
                        .get(focus_ngram_prefix_index)
                        .and_then(|(prefix, ngram_index)| {
                            (prefix == &suffix).then_some(*ngram_index)
                        }) {
                        Some(ngram_index) => ngram_index,
                        None => continue,
                    };
                    suffix_pos_idx_stack.push((
                        suffix,
                        match_pos,
                        Some(focus_ngram_prefix_index + 1),
                    ));
                    // Find the document and search for expected position
                    let (focus_ngram, focus_doc_pos_iter) =
                        &mut ngram_doc_pos_vec[focus_ngram_doc_pos_idx];
                    while focus_doc_pos_iter.peek().is_some_and(|(d, _)| *d < doc) {
                        focus_doc_pos_iter.next();
                    }
                    let Some(pos) = focus_doc_pos_iter
                        .peek()
                        .and_then(|(d, p)| (*d == doc).then_some(*p))
                    else {
                        continue;
                    };
                    if pos.binary_search(&match_pos).is_ok() {
                        let suffix_offset = focus_ngram.char_indices().nth(1).unwrap_or_default().0;
                        suffix_pos_idx_stack.push((
                            &focus_ngram[suffix_offset..],
                            match_pos + suffix_offset as u32,
                            None,
                        ));
                    }
                }
                // Try next candidate pivot position if there is no valid trace to the right
                if suffix_pos_idx_stack.is_empty() {
                    continue;
                }

                // Trace to the left of pivot
                // `prefix_pos_idx_stack` stores a stack of (
                //   <prefix of current ngram>,
                //   <position of current ngram>,
                //   <index of next ngram to check in the suffix lookup table>,
                // )
                let mut prefix_pos_idx_stack = Vec::with_capacity(min_lookup_table_index + 1);
                let prefix_offset = ngram.char_indices().next_back().unwrap_or_default().0;
                prefix_pos_idx_stack.push((&ngram[..prefix_offset], pos, None));
                while let Some((prefix, match_pos_with_offset, ngram_index)) =
                    prefix_pos_idx_stack.pop()
                {
                    // Find the next lookup table to the left
                    let focus_lookup_table = match min_lookup_table_index
                        .checked_sub(prefix_pos_idx_stack.len() + 1)
                        .and_then(|lookup_index| lookup_table_vec.get(lookup_index))
                    {
                        Some(table) => table,
                        None => {
                            // There is no more lookup table on the left
                            // We have found a valid trace to the left
                            prefix_pos_idx_stack.push((prefix, match_pos_with_offset, ngram_index));
                            break;
                        }
                    };
                    // Find the next ngram to check
                    let focus_ngram_suffix_index = match ngram_index {
                        Some(idx) => idx,
                        None if focus_lookup_table.suffix.len() <= 1 => 0,
                        None => focus_lookup_table
                            .suffix
                            .partition_point(|(suffix, _)| suffix < &prefix),
                    };
                    let focus_ngram_doc_pos_idx = match focus_lookup_table
                        .suffix
                        .get(focus_ngram_suffix_index)
                        .and_then(|(suffix, ngram_index)| {
                            (suffix == &prefix).then_some(*ngram_index)
                        }) {
                        Some(ngram_index) => ngram_index,
                        None => continue,
                    };
                    prefix_pos_idx_stack.push((
                        prefix,
                        match_pos_with_offset,
                        Some(focus_ngram_suffix_index + 1),
                    ));
                    // Find the document and search for expected position
                    let (focus_ngram, focus_doc_pos_iter) =
                        &mut ngram_doc_pos_vec[focus_ngram_doc_pos_idx];
                    while focus_doc_pos_iter.peek().is_some_and(|(d, _)| *d < doc) {
                        focus_doc_pos_iter.next();
                    }
                    let Some(pos) = focus_doc_pos_iter
                        .peek()
                        .and_then(|(d, p)| (*d == doc).then_some(*p))
                    else {
                        continue;
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
                        prefix_pos_idx_stack.push((&focus_ngram[..prefix_offset], match_pos, None));
                    }
                }
                // Record the candidate if there is a successful trace to the left
                if !prefix_pos_idx_stack.is_empty() {
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
            LiteralExpr::Literal(literals) => {
                N <= literals.len()
                    && literals
                        .iter()
                        .all(|c| c.width() <= self.maximum_branching_factor())
            }
            LiteralExpr::Concat(_) | LiteralExpr::Alternation(_) => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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

        async fn lookup_ngram<'me>(
            &'me self,
            ngram: &'me str,
        ) -> Result<Box<dyn Iterator<Item = (u32, &'me [u32])> + Send + Sync + 'me>, ()> {
            match self
                .inverted_literal_index
                .binary_search_by_key(&ngram, |(n, _)| n)
            {
                Ok(index) => Ok(Box::new(
                    self.inverted_literal_index[index]
                        .1
                        .iter()
                        .map(|(doc, pos)| (*doc, pos.as_slice())),
                )),
                Err(_) => Ok(Box::new(Vec::new().into_iter())),
            }
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
                ("deF".to_string(), vec![(1, vec![29, 40])]),
                ("def".to_string(), vec![(0, vec![9, 21])]),
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
