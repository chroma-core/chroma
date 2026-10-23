//! Tagged 64-bit vector postings carried by an existing `Vec<u32>` value.
//!
//! Every word and vector ID uses little-endian byte order. The 56-byte header
//! precedes IDs, version bytes, code bytes, and zero padding to a word boundary.
//! This format is distinct from legacy hierarchical posting values.
use anyhow::{ensure, Context, Result};

const MAGIC: [u32; 2] = [0x3436_5053, 0x3154_534c];
const VERSION: u32 = 1;
const HEADER_WORDS: usize = 14;
const HEADER_BYTES: usize = HEADER_WORDS * 4;

#[derive(Debug, PartialEq, Eq)]
pub struct Posting64 {
    pub ids: Vec<u64>,
    pub versions: Vec<u8>,
    pub codes: Vec<u8>,
}

fn pair(words: &[u32], at: usize) -> u64 {
    u64::from(words[at]) | (u64::from(words[at + 1]) << 32)
}
fn append_pair(words: &mut Vec<u32>, value: usize) -> Result<()> {
    let value = u64::try_from(value).context("posting64 size exceeds u64")?;
    words.extend([value as u32, (value >> 32) as u32]);
    Ok(())
}
fn payload_sizes(rows: usize, code_size: usize) -> Result<(usize, usize, usize)> {
    let ids = rows.checked_mul(8).context("posting64 ID length overflow")?;
    let codes = rows.checked_mul(code_size).context("posting64 code length overflow")?;
    let payload = ids.checked_add(rows).and_then(|v| v.checked_add(codes))
        .context("posting64 payload length overflow")?;
    Ok((ids, codes, payload))
}
fn total_words(payload: usize) -> Result<usize> {
    HEADER_BYTES.checked_add(payload).and_then(|v| v.checked_add(3))
        .map(|v| v / 4).context("posting64 total length overflow")
}
fn byte(words: &[u32], offset: usize) -> u8 {
    (words[HEADER_WORDS + offset / 4] >> ((offset % 4) * 8)) as u8
}

/// Preserve physical row order and duplicates. A zero code size is valid only
/// with an empty code payload; callers decide whether their scorer permits it.
pub fn encode(ids: &[u64], versions: &[u8], codes: &[u8], code_size: usize) -> Result<Vec<u32>> {
    let rows = ids.len();
    let (id_bytes, code_bytes, payload) = payload_sizes(rows, code_size)?;
    ensure!(versions.len() == rows && codes.len() == code_bytes, "posting64 row/payload lengths differ");
    let count = total_words(payload)?;
    let mut words = Vec::new();
    words.try_reserve_exact(count).context("posting64 encoded allocation")?;
    words.extend(MAGIC);
    words.extend([VERSION, HEADER_WORDS as u32]);
    for value in [rows, code_size, id_bytes, rows, code_bytes] {
        append_pair(&mut words, value)?;
    }
    words.resize(count, 0);
    for (index, id) in ids.iter().enumerate() {
        words[HEADER_WORDS + index * 2] = *id as u32;
        words[HEADER_WORDS + index * 2 + 1] = (*id >> 32) as u32;
    }
    for (offset, value) in versions.iter().chain(codes).enumerate() {
        let offset = id_bytes + offset;
        words[HEADER_WORDS + offset / 4] |= u32::from(*value) << ((offset % 4) * 8);
    }
    Ok(words)
}

/// Validate the full envelope, derived lengths, caller row budget and padding
/// before reserving any decoded array. The input slice itself is caller-owned.
pub fn decode(words: &[u32], code_size: usize, max_rows: usize) -> Result<Posting64> {
    ensure!(words.len() >= HEADER_WORDS, "posting64 truncated header");
    ensure!(words[..2] == MAGIC && words[2] == VERSION && words[3] == HEADER_WORDS as u32,
        "posting64 tag/version differs");
    let rows = usize::try_from(pair(words, 4)).context("posting64 rows exceed usize")?;
    ensure!(rows <= max_rows, "posting64 row budget exceeded");
    ensure!(pair(words, 6) == u64::try_from(code_size).context("posting64 code size exceeds u64")?,
        "posting64 code size differs");
    let (id_bytes, code_bytes, payload) = payload_sizes(rows, code_size)?;
    ensure!(pair(words, 8) == u64::try_from(id_bytes)?
        && pair(words, 10) == u64::try_from(rows)?
        && pair(words, 12) == u64::try_from(code_bytes)?, "posting64 declared lengths differ");
    ensure!(words.len() == total_words(payload)?, "posting64 truncated or trailing payload");
    let packed_bytes = (words.len() - HEADER_WORDS).checked_mul(4).context("posting64 padding overflow")?;
    for offset in payload..packed_bytes {
        ensure!(byte(words, offset) == 0, "posting64 nonzero padding");
    }
    let mut ids = Vec::new();
    let mut versions = Vec::new();
    let mut codes = Vec::new();
    ids.try_reserve_exact(rows).context("posting64 ID allocation")?;
    versions.try_reserve_exact(rows).context("posting64 version allocation")?;
    codes.try_reserve_exact(code_bytes).context("posting64 code allocation")?;
    for index in 0..rows { ids.push(pair(words, HEADER_WORDS + index * 2)); }
    for offset in id_bytes..id_bytes + rows { versions.push(byte(words, offset)); }
    for offset in id_bytes + rows..payload { codes.push(byte(words, offset)); }
    Ok(Posting64 { ids, versions, codes })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn high_ids_duplicates_all_versions_and_arbitrary_codes_roundtrip() {
        let ids: Vec<_> = (0..256).map(|i| [7, (1u64 << 32) + 7, (2u64 << 32) + 7, u64::MAX][i % 4]).collect();
        let versions: Vec<_> = (0..=255).collect();
        let codes: Vec<_> = (0..256 * 3).map(|i| (i * 73) as u8).collect();
        let words = encode(&ids, &versions, &codes, 3).unwrap();
        assert_eq!(decode(&words, 3, 256).unwrap(), Posting64 { ids, versions, codes });
        assert_eq!(words.len() * 4, HEADER_BYTES + 256 * 12);
    }
    #[test]
    fn empty_and_zero_code_rows_are_explicit() {
        for size in [0, 1, 768] {
            let words = encode(&[], &[], &[], size).unwrap();
            assert_eq!(words.len(), HEADER_WORDS);
            assert_eq!(decode(&words, size, 0).unwrap(), Posting64 { ids: vec![], versions: vec![], codes: vec![] });
        }
        assert_eq!(decode(&encode(&[u64::MAX], &[255], &[], 0).unwrap(), 0, 1).unwrap().ids, vec![u64::MAX]);
    }
    #[test]
    fn byte_order_and_padding_are_deterministic() {
        let words = encode(&[0x0807_0605_0403_0201], &[0xab], &[0xcd, 0xef], 2).unwrap();
        assert_eq!(&words[HEADER_WORDS..], &[0x0403_0201, 0x0807_0605, 0x00ef_cdab]);
        assert_eq!(encode(&[0x0807_0605_0403_0201], &[0xab], &[0xcd, 0xef], 2).unwrap(), words);
        let mut bad = words; *bad.last_mut().unwrap() |= 0xff00_0000;
        assert!(decode(&bad, 2, 1).is_err());
    }
    #[test]
    fn malformed_headers_claims_truncation_and_trailing_words_reject() {
        let good = encode(&[7], &[128], &[1, 2, 3], 3).unwrap();
        for index in 0..HEADER_WORDS {
            let mut bad = good.clone(); bad[index] ^= 1;
            assert!(decode(&bad, 3, 1).is_err(), "header word {index}");
        }
        for len in 0..good.len() { assert!(decode(&good[..len], 3, 1).is_err()); }
        let mut bad = good.clone(); bad.push(0); assert!(decode(&bad, 3, 1).is_err());
        assert!(decode(&good, 2, 1).is_err());
        assert!(decode(&good, 3, 0).is_err());
        assert!(decode(&[0; HEADER_WORDS], 3, 1).is_err());
    }
    #[test]
    fn impossible_lengths_reject_before_decoded_allocation() {
        assert!(encode(&[1], &[], &[], 0).is_err());
        assert!(encode(&[1, 2], &[0, 0], &[], usize::MAX).is_err());
        let mut bad = encode(&[], &[], &[], usize::MAX).unwrap();
        bad[4] = u32::MAX; bad[5] = u32::MAX;
        assert!(decode(&bad, usize::MAX, 10).is_err());
        assert!(decode(&bad, usize::MAX, usize::MAX).is_err());
    }
}
