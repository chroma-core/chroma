// mirrors the python and go rendezvous hashing
use std::io::Cursor;

use murmur3::murmur3_x64_128;

trait Hasher {
    fn hash(&self, member: &str, key: &str) -> Result<u64, &'static str>;
}

fn assign<H: Hasher>(
    key: &str,
    members: impl IntoIterator<Item = impl AsRef<str>>,
    hasher: &H,
) -> Result<String, &'static str> {
    if key.is_empty() {
        return Err("Cannot assign empty key");
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
            Err(err) => return Err(err),
        };
        if score > max_score {
            max_score = score;
            max_member = Some(member);
        }
    }

    if !iterated {
        return Err("No members to assign to");
    }

    match max_member {
        Some(max_member) => return Ok(max_member.as_ref().to_string()),
        None => return Err("No member to assign to"),
    }
}

fn merge_hashes(x: u64, y: u64) -> u64 {
    let mut acc = x ^ y;
    acc ^= acc >> 33;
    acc = acc.wrapping_mul(0xFF51AFD7ED558CCD); // TODO: is this the correct way to do this?
    acc ^= acc >> 33;
    acc = acc.wrapping_mul(0xC4CEB9FE1A85EC53);
    acc ^= acc >> 33;
    acc
}

struct Murmur3Hasher {}

impl Hasher for Murmur3Hasher {
    fn hash(&self, member: &str, key: &str) -> Result<u64, &'static str> {
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
            _ => return Err("Error in hashing member or key"),
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockHasher {}

    impl Hasher for MockHasher {
        fn hash(&self, member: &str, _key: &str) -> Result<u64, &'static str> {
            match member {
                "a" => Ok(1),
                "b" => Ok(2),
                "c" => Ok(3),
                _ => Err("No such mock member"),
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

        for i in 0..member_count {
            let count = counts[i];
            let expected = num_keys / member_count;
            let diff = count - expected as i32;
            assert!(diff.abs() < tolerance);
        }
    }
}
