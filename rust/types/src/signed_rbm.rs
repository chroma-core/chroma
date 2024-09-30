use std::ops::{BitAnd, BitOr};

use roaring::RoaringBitmap;

/// This enum helps to delay the evaluation of set minus in metadata filtering:
/// - `Include(rbm)` suggests the result contains the specified ids in `rbm`.
///   For example, `<k>: {$eq: <v>}` will result in a `Include(<record ids with k=v>)`.
/// - `Exclude(rbm)` suggests the result exludes the specified ids in `rbm`.
///   For example, `<k>: {$ne: <v>}` will result in a `Exclude(<record ids with k=v>)`
///
/// Without this, we need to figure out the set of existing ids when we evaluate `$ne`, `$nin`, and `$not_contains`,
/// but this is not always necessary and may lead to overheads. Let's consider the following where clause as an example:
///
/// `{$and: [{<k0>: {$gt: <v0>}}, {<k1>: {$ne: <v1>}}]}`
///
/// The naive way is to evaluate the `$gt` and `$ne` seperately and take the conjunction, but this requires
/// us to know the full set of existing ids because it is needed to evaluate `$ne`.
/// However, we can first evaluate `$gt` and then exclude the offset ids for records with metadata `k1=v1`.
/// This behavior is captured in the `BitAnd::bitand` operator of `SignedRoaringBitmap`:
/// - A bitand between `Include(...)` and `Exclude(...)` will always result in `Include(...)`
/// - This process does not require the knowledge of the full domain
/// - The domain is needed only when we have to convert an `Exclude(...)` to the actual result in the end.
///
/// In summary, this enum distinguishes the results that depends on the domain and those that do not:
/// - `Include(...)` does not depend on the domain
/// - `Exclude(...)` depends on the domain
/// We only need to figure out the domain (i.e. the set of ids for existing records) when it is necessary to do so.
#[derive(Clone, Debug, PartialEq)]
pub enum SignedRoaringBitmap {
    Include(RoaringBitmap),
    Exclude(RoaringBitmap),
}

impl SignedRoaringBitmap {
    pub fn empty() -> Self {
        Self::Include(RoaringBitmap::new())
    }

    pub fn full() -> Self {
        Self::Exclude(RoaringBitmap::new())
    }

    pub fn flip(self) -> Self {
        use SignedRoaringBitmap::*;
        match self {
            Include(rbm) => Exclude(rbm),
            Exclude(rbm) => Include(rbm),
        }
    }
}

impl BitAnd for SignedRoaringBitmap {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self {
        use SignedRoaringBitmap::*;
        match (self, rhs) {
            (Include(lhs), Include(rhs)) => Include(lhs & rhs),
            (Include(lhs), Exclude(rhs)) => Include(lhs - rhs),
            (Exclude(lhs), Include(rhs)) => Include(rhs - lhs),
            (Exclude(lhs), Exclude(rhs)) => Exclude(lhs | rhs),
        }
    }
}

impl BitOr for SignedRoaringBitmap {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        use SignedRoaringBitmap::*;
        match (self, rhs) {
            (Include(lhs), Include(rhs)) => Include(lhs | rhs),
            (Include(lhs), Exclude(rhs)) => Exclude(rhs - lhs),
            (Exclude(lhs), Include(rhs)) => Exclude(lhs - rhs),
            (Exclude(lhs), Exclude(rhs)) => Exclude(lhs & rhs),
        }
    }
}
