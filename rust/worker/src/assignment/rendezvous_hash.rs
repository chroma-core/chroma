// This implementation mirrors the rendezvous hash implementation
// in the go and python services.
// The go implementation is located go/internal/utils/rendezvous_hash.go
// The python implementation is located chromadb/utils/rendezvous_hash.py

use crate::errors::{ChromaError, ErrorCodes};
use std::io::Cursor;
use thiserror::Error;

use murmur3::murmur3_x64_128;

/// A trait for hashing a member and a key to a score.
pub(crate) trait Hasher {
    fn hash(&self, member: &str, key: &str) -> Result<u64, AssignmentError>;
}

/// Error codes for assignment
#[derive(Error, Debug)]
pub(crate) enum AssignmentError {
    #[error("Cannot assign empty key")]
    EmptyKey,
    #[error("No members to assign to")]
    NoMembers,
    #[error("Error hashing member")]
    HashError,
}

impl ChromaError for AssignmentError {
    fn code(&self) -> ErrorCodes {
        match self {
            AssignmentError::EmptyKey => ErrorCodes::InvalidArgument,
            AssignmentError::NoMembers => ErrorCodes::InvalidArgument,
            AssignmentError::HashError => ErrorCodes::Internal,
        }
    }
}

/// Assign a key to a member using the rendezvous hash algorithm.
/// # Arguments
/// - key: The key to assign.
/// - members: The members to assign to.
/// - hasher: The hasher to use.
/// # Returns
/// The member that the key was assigned to.
/// # Errors
/// - If the key is empty.
/// - If there are no members to assign to.
/// - If there is an error hashing a member.
/// # Notes
/// This implementation mirrors the rendezvous hash implementation
/// in the go and python services.
pub(crate) fn assign<H: Hasher>(
    key: &str,
    members: impl IntoIterator<Item = impl AsRef<str>>,
    hasher: &H,
) -> Result<String, AssignmentError> {
    if key.is_empty() {
        return Err(AssignmentError::EmptyKey);
    }

    let mut iterated = false;
    let mut max_score = u64::MIN;
    let mut max_member = None;

    for member in members {
        if !iterated {
            iterated = true;
        }
        let score = hasher.hash(member.as_ref(), key);
        let score = match score {
            Ok(score) => score,
            Err(err) => return Err(AssignmentError::HashError),
        };
        if score > max_score {
            max_score = score;
            max_member = Some(member);
        }
    }

    if !iterated {
        return Err(AssignmentError::NoMembers);
    }

    match max_member {
        Some(max_member) => return Ok(max_member.as_ref().to_string()),
        None => return Err(AssignmentError::NoMembers),
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
                return Ok(merged);
            }
            _ => return Err(AssignmentError::HashError),
        };
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
        let member = assign(key, members, &hasher).unwrap();
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
            let member = assign(&key, &nodes, &hasher).unwrap();
            let index = nodes.iter().position(|x| *x == member).unwrap();
            counts[index] += 1;
        }

        let expected = num_keys / member_count;
        for i in 0..member_count {
            let count = counts[i];
            let diff = count - expected as i32;
            assert!(diff.abs() < tolerance);
        }
    }
}
