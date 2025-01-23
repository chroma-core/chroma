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
        Ok(member_vec.into_iter().take(k).map(|(_, m)| m).collect())
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
        let member = hasher.assign_one(&members, &key).unwrap();
        assert_eq!(member, "c".to_string());
    }

    #[test]
    fn test_even_distribution() {
        let member_count = 10;
        let tolerance = 25;
        let mut nodes = Vec::with_capacity(member_count);
        let hasher = Murmur3Hasher {};

        for i in 0..member_count {
            let member = format!("member{}", i);
            nodes.push(member);
        }

        let mut counts = vec![0; member_count];
        let num_keys = 1000;
        for i in 0..num_keys {
            let key = format!("key_{}", i);
            let member = hasher.assign_one(&nodes, &key).unwrap();
            let index = nodes.iter().position(|x| *x == member).unwrap();
            counts[index] += 1;
        }

        let expected = num_keys / member_count;
        for count in counts.iter().take(member_count).copied() {
            let diff = count - expected as i32;
            assert!(diff.abs() < tolerance);
        }
    }
}
