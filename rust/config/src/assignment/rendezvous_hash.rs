// This implementation mirrors the rendezvous hash implementation
// in the go and python services.
// The go implementation is located go/internal/utils/rendezvous_hash.go
// The python implementation is located chromadb/utils/rendezvous_hash.py

use chroma_error::{ChromaError, ErrorCodes};
use std::io::Cursor;
use thiserror::Error;

use murmur3::murmur3_x64_128;

/// A trait for hashing a member and a key to a score.
pub trait Hasher {
    fn hash(&self, member: &str, key: &str) -> Result<u64, AssignmentError>;
    /// Assign a key to a collection of members using the rendezvous hash algorithm
    /// # Arguments
    /// - key: The key to assign.
    /// - members: The members to assign to.
    /// # Returns
    /// The members that the key were assigned to.
    /// # Errors
    /// - If the key is empty.
    /// - If there are insufficient members to assign to.
    /// - If there is an error hashing a member.
    /// # Notes
    /// This implementation mirrors the rendezvous hash implementation
    /// in the go and python services.
    fn assign(
        &self,
        members: impl IntoIterator<Item = impl AsRef<str>>,
        key: &str,
        k: usize,
    ) -> Result<Vec<String>, AssignmentError> {
        let mut member_vec = members
            .into_iter()
            .map(|m| {
                self.hash(m.as_ref(), key)
                    .map(|s| (s, m.as_ref().to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if member_vec.len() < k {
            return Err(AssignmentError::InsufficientMember(k, member_vec.len()));
        }

        member_vec.sort_by_key(|(s, _)| *s);
        Ok(member_vec
            .into_iter()
            .rev()
            .take(k)
            .map(|(_, m)| m)
            .collect())
    }

    fn assign_one(
        &self,
        members: impl IntoIterator<Item = impl AsRef<str>>,
        key: &str,
    ) -> Result<String, AssignmentError> {
        self.assign(members, key, 1).map(|mut members| {
            members
                .pop()
                .expect("The key should be assigned to exactly one member")
        })
    }
}

/// Error codes for assignment
#[derive(Error, Debug)]
pub enum AssignmentError {
    #[error("Cannot assign empty key")]
    EmptyKey,
    #[error("Insufficient members: requested {0}, available {1}")]
    InsufficientMember(usize, usize),
    #[error("Error hashing member")]
    HashError,
}

impl ChromaError for AssignmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            AssignmentError::EmptyKey => ErrorCodes::InvalidArgument,
            AssignmentError::InsufficientMember(_, _) => ErrorCodes::InvalidArgument,
            AssignmentError::HashError => ErrorCodes::Internal,
        }
    }
}

fn merge_hashes(x: u64, y: u64) -> u64 {
    let mut acc = x ^ y;
    acc ^= acc >> 33;
    acc = acc.wrapping_mul(0xFF51AFD7ED558CCD);
    acc ^= acc >> 33;
    acc = acc.wrapping_mul(0xC4CEB9FE1A85EC53);
    acc ^= acc >> 33;
    acc
}

#[derive(Clone, Debug)]
pub(crate) struct Murmur3Hasher {}

impl Hasher for Murmur3Hasher {
    fn hash(&self, member: &str, key: &str) -> Result<u64, AssignmentError> {
        let member_hash = murmur3_x64_128(&mut Cursor::new(member), 0);
        let key_hash = murmur3_x64_128(&mut Cursor::new(key), 0);
        // The murmur library returns a 128 bit hash, but we only need 64 bits, grab the first 64 bits
        match (member_hash, key_hash) {
            (Ok(member_hash), Ok(key_hash)) => {
                let member_hash_64 = member_hash as u64;
                let key_hash_64 = key_hash as u64;
                let merged = merge_hashes(member_hash_64, key_hash_64);
                Ok(merged)
            }
            _ => Err(AssignmentError::HashError),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    struct MockHasher {}

    impl Hasher for MockHasher {
        fn hash(&self, member: &str, _key: &str) -> Result<u64, AssignmentError> {
            match member {
                "a" => Ok(1),
                "b" => Ok(2),
                "c" => Ok(3),
                _ => Err(AssignmentError::HashError),
            }
        }
    }

    #[test]
    fn test_assign() {
        let members = vec!["a", "b", "c"];
        let hasher = MockHasher {};
        let key = "key";
        let member = hasher.assign_one(&members, key).unwrap();
        assert_eq!(member, "c".to_string());
    }

    #[test]
    fn test_even_distribution() {
        let key_count = 1000;
        let member_count = 10;
        // Probablity of a key get assigned to a particular member, assuming perfect hashing
        let prob = 1_f64 / member_count as f64;
        // Expected number of keys assigned to a member
        let expected = key_count as f64 * prob;
        // Variance of the total number of keys assigned to a member
        let var = key_count as f64 * prob * (1_f64 - prob);
        let hasher = Murmur3Hasher {};

        let nodes = (0..member_count)
            .map(|i| format!("{i}"))
            .collect::<Vec<_>>();

        let mut counts = vec![0; member_count];
        for i in 0..key_count {
            let key = format!("key_{}", i);
            let member = hasher.assign_one(&nodes, &key).unwrap();
            counts[member.parse::<usize>().unwrap()] += 1;
        }

        for count in counts.iter().take(member_count).copied() {
            let diff = count as f64 - expected;
            // The distribution should be Binomial(key_count, prob)
            // Since key_count is large, this is approximately normal
            // We are confident that number of keys assigned to any member should be within 3 standard deviation of the expected value
            assert!(diff.abs() < var.sqrt() * 3_f64);
        }
    }

    #[test]
    fn test_multi_assign_even_distribution() {
        let k = 3;
        let key_count = 1000;
        let member_count = 10;
        // Probablity of a key get assigned to a particular member, assuming perfect hashing
        let prob = k as f64 / member_count as f64;
        // Expected number of keys assigned to a member
        let expected = key_count as f64 * prob;
        // Estimated variance of the total number of keys assigned to a member
        let var = (k * key_count) as f64 * prob * (1_f64 - prob);
        let hasher = Murmur3Hasher {};

        let nodes = (0..member_count)
            .map(|i| format!("{i}"))
            .collect::<Vec<_>>();

        let mut counts = vec![0; member_count];
        for i in 0..key_count {
            let key = format!("key_{}", i);
            let members = hasher.assign(&nodes, &key, k).unwrap();
            // Assigned members should be unique
            let unique_members: HashSet<_> = HashSet::from_iter(members.iter());
            assert_eq!(unique_members.len(), members.len());
            for member in members {
                counts[member.parse::<usize>().unwrap()] += 1;
            }
        }

        for count in counts.iter().take(member_count).copied() {
            let diff = count as f64 - expected;
            // The number of keys assigned to a member should be approximately normal
            // We are confident that number of keys assigned to any member should be within 3 standard deviation of the expected value
            assert!(diff.abs() < var.sqrt() * 3_f64);
        }
    }
}
