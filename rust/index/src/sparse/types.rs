use std::cmp::Ordering;
use std::collections::BinaryHeap;

use base64::{prelude::BASE64_STANDARD, DecodeError, Engine};
use thiserror::Error;

pub const DEFAULT_BLOCK_SIZE: u32 = 128;

// NOTE: This is a temporary hack to store dimension id in prefix of blockfile.
// This should be removed once we have generic prefix type.

pub const DIMENSION_PREFIX: &str = "DIM";

#[derive(Debug, Error)]
pub enum Base64DecodeError {
    #[error(transparent)]
    Decode(#[from] DecodeError),
    #[error("Unable to convert bytes to u32")]
    Parse,
}

pub fn encode_u32(value: u32) -> String {
    BASE64_STANDARD.encode(value.to_le_bytes())
}

pub fn decode_u32(code: &str) -> Result<u32, Base64DecodeError> {
    let le_bytes: [u8; 4] = BASE64_STANDARD
        .decode(code)?
        .try_into()
        .map_err(|_| Base64DecodeError::Parse)?;
    Ok(u32::from_le_bytes(le_bytes))
}

// ── Score type ──────────────────────────────────────────────────────

/// A (score, offset) pair with reversed ordering so that `BinaryHeap`
/// acts as a min-heap: the *lowest* score sits at `peek()`, making it
/// cheap to maintain a top-k set.
#[derive(Debug, PartialEq)]
pub struct Score {
    pub score: f32,
    pub offset: u32,
}

impl Eq for Score {}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then(self.offset.cmp(&other.offset))
            .reverse()
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// ── Top-k min-heap ──────────────────────────────────────────────────

/// A fixed-capacity min-heap for top-k score tracking.
///
/// Wraps `BinaryHeap<Score>` (which is a max-heap, but `Score` has
/// reversed `Ord`) so that `peek()` returns the *lowest* score.
/// `push()` inserts a candidate and evicts the minimum if over capacity.
pub struct TopKHeap {
    heap: BinaryHeap<Score>,
    k: usize,
}

impl TopKHeap {
    pub fn new(k: usize) -> Self {
        Self {
            heap: BinaryHeap::with_capacity(k),
            k,
        }
    }

    /// Push a candidate into the heap. If the heap is already at capacity
    /// and the candidate doesn't beat the current minimum, it is ignored.
    /// Returns the current threshold (minimum score in heap, or `f32::MIN`
    /// if the heap isn't full yet).
    pub fn push(&mut self, score: f32, offset: u32) -> f32 {
        if self.heap.len() < self.k || score > self.threshold() {
            self.heap.push(Score { score, offset });
            if self.heap.len() > self.k {
                self.heap.pop();
            }
        }
        self.threshold()
    }

    /// The minimum score in the heap, or `f32::MIN` if not yet at capacity.
    pub fn threshold(&self) -> f32 {
        if self.heap.len() < self.k {
            f32::MIN
        } else {
            self.heap.peek().map(|s| s.score).unwrap_or(f32::MIN)
        }
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    /// Drain the heap into a `Vec<Score>` sorted by descending score,
    /// with ties broken by ascending offset.
    pub fn into_sorted_vec(self) -> Vec<Score> {
        self.heap.into_sorted_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_u32() {
        assert_eq!(
            decode_u32(&encode_u32(42)).expect("Encoding should be valid"),
            42
        );
    }

    #[test]
    fn score_min_heap_ordering() {
        let mut heap = BinaryHeap::new();
        heap.push(Score {
            score: 3.0,
            offset: 1,
        });
        heap.push(Score {
            score: 1.0,
            offset: 2,
        });
        heap.push(Score {
            score: 2.0,
            offset: 3,
        });
        assert_eq!(heap.peek().unwrap().score, 1.0);
        heap.pop();
        assert_eq!(heap.peek().unwrap().score, 2.0);
    }

    #[test]
    fn score_tiebreak_by_offset() {
        let a = Score {
            score: 1.0,
            offset: 10,
        };
        let b = Score {
            score: 1.0,
            offset: 20,
        };
        assert!(a > b); // reversed: higher offset = "lower" priority
    }

    #[test]
    fn topk_heap_basic() {
        let mut heap = TopKHeap::new(2);
        assert_eq!(heap.threshold(), f32::MIN);

        heap.push(1.0, 1);
        assert_eq!(heap.threshold(), f32::MIN); // not full yet

        heap.push(3.0, 2);
        assert_eq!(heap.threshold(), 1.0); // full, min is 1.0

        heap.push(2.0, 3);
        assert_eq!(heap.threshold(), 2.0); // evicted 1.0, min is now 2.0
        assert_eq!(heap.len(), 2);

        let results = heap.into_sorted_vec();
        assert_eq!(results[0].score, 3.0);
        assert_eq!(results[1].score, 2.0);
    }

    #[test]
    fn topk_heap_ignores_below_threshold() {
        let mut heap = TopKHeap::new(2);
        heap.push(5.0, 1);
        heap.push(3.0, 2);
        heap.push(1.0, 3); // below threshold, should be ignored
        assert_eq!(heap.len(), 2);

        let results = heap.into_sorted_vec();
        assert_eq!(results[0].score, 5.0);
        assert_eq!(results[1].score, 3.0);
    }
}
