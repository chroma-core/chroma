//! Input identities are checked independently of posting placement. Consecutive
//! IDs compress into ranges; scattered IDs can require one range per vector.
use std::collections::BTreeMap;
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub(super) struct InsertedIds {
    ranges: BTreeMap<u64, u64>,
}
impl InsertedIds {
    pub fn ranges(&self) -> usize {
        self.ranges.len()
    }
    pub fn contains(&self, id: u64) -> bool {
        self.ranges
            .range(..=id)
            .next_back()
            .is_some_and(|(_, end)| id <= *end)
    }
    pub fn admit(&self, ids: &[u64]) -> Result<Self, String> {
        let mut sorted = ids.to_vec();
        sorted.sort_unstable();
        if sorted.windows(2).any(|w| w[0] == w[1]) || sorted.iter().any(|id| self.contains(*id)) {
            return Err("duplicate new vector ID in the ingestion history".into());
        }
        let mut next = self.clone();
        for id in sorted {
            let predecessor = next
                .ranges
                .range(..id)
                .next_back()
                .map(|(start, end)| (*start, *end));
            let successor = next
                .ranges
                .range(id..)
                .next()
                .map(|(start, end)| (*start, *end));
            let mut start = id;
            let mut end = id;
            if let Some((a, b)) = predecessor {
                if b.checked_add(1) == Some(id) {
                    start = a;
                    next.ranges.remove(&a);
                }
            }
            if let Some((a, b)) = successor {
                if id.checked_add(1) == Some(a) {
                    end = b;
                    next.ranges.remove(&a);
                }
            }
            next.ranges.insert(start, end);
        }
        Ok(next)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bridges_intervals_and_rejects_earlier_ids() {
        let a = InsertedIds::default().admit(&[0, 1, 4, 5]).unwrap();
        assert_eq!(a.ranges(), 2);
        let b = a.admit(&[3, 2]).unwrap();
        assert_eq!(b.ranges(), 1);
        assert!(b.admit(&[0]).is_err());
        assert!(b.admit(&[6, 6]).is_err());
        assert!(!b.contains(6));
    }
    #[test]
    fn arbitrary_wide_ids_and_maximum_do_not_wrap() {
        let a = InsertedIds::default()
            .admit(&[u64::MAX, 10, 1 << 63])
            .unwrap();
        assert_eq!(a.ranges(), 3);
        let b = a.admit(&[u64::MAX - 1]).unwrap();
        assert!(b.contains(u64::MAX));
        assert!(!b.contains(0));
        assert!(b.admit(&[1 << 63]).is_err());
    }
}
