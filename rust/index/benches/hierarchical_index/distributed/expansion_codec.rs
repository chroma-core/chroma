//! Lossless field encoding for complete expansion queries and scored rows.
//! The JSON envelope and semantic validation stay in the navigation driver.
use super::{NodeRef, ScoredChild};
use base64::{prelude::BASE64_STANDARD, Engine};
use serde::{de::Error, Deserializer, Serializer};

// This field ceiling never enlarges the transport's independent frame limit.
// At the default 16 MiB frame limit, envelope bytes further reduce what fits.
const MAX_PAYLOAD_BYTES: usize = 12 * 1024 * 1024 - 2;
const MAX_ENCODED_BYTES: usize = MAX_PAYLOAD_BYTES.div_ceil(3) * 4;
const HEADER_BYTES: usize = 4;
const CHILD_BYTES: usize = 69;

fn payload_len(count: usize, width: usize) -> Result<usize, &'static str> {
    let length = count
        .checked_mul(width)
        .and_then(|n| n.checked_add(HEADER_BYTES))
        .ok_or("expansion payload size overflow")?;
    if count > u32::MAX as usize || length > MAX_PAYLOAD_BYTES {
        return Err("expansion payload exceeds decoded byte limit");
    }
    Ok(length)
}
fn buffer(count: usize, width: usize) -> Result<Vec<u8>, &'static str> {
    let length = payload_len(count, width)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| "expansion payload allocation failed")?;
    bytes.extend_from_slice(&(count as u32).to_le_bytes());
    Ok(bytes)
}
fn encode<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
    serializer.serialize_str(&BASE64_STANDARD.encode(bytes))
}
fn decode<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
    struct BoundedPayload;
    impl serde::de::Visitor<'_> for BoundedPayload {
        type Value = Vec<u8>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a bounded canonical padded base64 expansion payload")
        }
        fn visit_str<E: Error>(self, encoded: &str) -> Result<Self::Value, E> {
            if encoded.len() > MAX_ENCODED_BYTES {
                return Err(E::custom("expansion payload exceeds encoded byte limit"));
            }
            let bytes = BASE64_STANDARD.decode(encoded).map_err(E::custom)?;
            if bytes.len() > MAX_PAYLOAD_BYTES {
                return Err(E::custom("expansion payload exceeds decoded byte limit"));
            }
            Ok(bytes)
        }
    }
    deserializer.deserialize_str(BoundedPayload)
}
fn count<E: Error>(bytes: &[u8], width: usize) -> Result<usize, E> {
    let header: [u8; 4] = bytes
        .get(..HEADER_BYTES)
        .ok_or_else(|| E::custom("truncated expansion count"))?
        .try_into()
        .map_err(E::custom)?;
    let count = usize::try_from(u32::from_le_bytes(header)).map_err(E::custom)?;
    if payload_len(count, width).map_err(E::custom)? != bytes.len() {
        return Err(E::custom(
            "expansion count does not match exact payload length",
        ));
    }
    Ok(count)
}
fn reserve<T, E: Error>(count: usize) -> Result<Vec<T>, E> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(count)
        .map_err(|_| E::custom("expansion row allocation failed"))?;
    Ok(rows)
}

pub(super) mod query {
    use super::*;
    pub fn serialize<S: Serializer>(query: &[f32], serializer: S) -> Result<S::Ok, S::Error> {
        let mut bytes = buffer(query.len(), 4).map_err(serde::ser::Error::custom)?;
        for coordinate in query {
            if !coordinate.is_finite() {
                return Err(serde::ser::Error::custom(
                    "nonfinite expansion query coordinate",
                ));
            }
            bytes.extend_from_slice(&coordinate.to_bits().to_le_bytes());
        }
        encode(&bytes, serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<f32>, D::Error> {
        let bytes = decode(deserializer)?;
        let count = count::<D::Error>(&bytes, 4)?;
        let mut query = reserve::<_, D::Error>(count)?;
        for raw in bytes[HEADER_BYTES..].chunks_exact(4) {
            let coordinate = f32::from_bits(u32::from_le_bytes(
                raw.try_into().map_err(D::Error::custom)?,
            ));
            if !coordinate.is_finite() {
                return Err(D::Error::custom("nonfinite expansion query coordinate"));
            }
            query.push(coordinate);
        }
        Ok(query)
    }
}

pub(super) mod children {
    use super::*;
    pub fn serialize<S: Serializer>(
        rows: &[ScoredChild],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let mut bytes = buffer(rows.len(), CHILD_BYTES).map_err(serde::ser::Error::custom)?;
        for row in rows {
            if !row.distance.is_finite() {
                return Err(serde::ser::Error::custom("nonfinite expansion child score"));
            }
            bytes.extend_from_slice(&row.parent.to_le_bytes());
            bytes.extend_from_slice(
                &u64::try_from(row.ordinal)
                    .map_err(serde::ser::Error::custom)?
                    .to_le_bytes(),
            );
            bytes.extend_from_slice(&row.target.id.to_le_bytes());
            bytes.extend_from_slice(&row.target.authority.to_le_bytes());
            bytes.extend_from_slice(&row.target.placement.to_le_bytes());
            bytes.push(u8::from(row.leaf));
            bytes.extend_from_slice(&row.distance.to_bits().to_le_bytes());
            bytes.extend_from_slice(&row.observed_revision.to_le_bytes());
        }
        encode(&bytes, serializer)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<ScoredChild>, D::Error> {
        let bytes = decode(deserializer)?;
        let count = count::<D::Error>(&bytes, CHILD_BYTES)?;
        let mut rows = reserve::<_, D::Error>(count)?;
        for raw in bytes[HEADER_BYTES..].chunks_exact(CHILD_BYTES) {
            // Exact length is checked before allocation and these fixed slices.
            let u128_at =
                |offset| u128::from_le_bytes(raw[offset..offset + 16].try_into().unwrap());
            let u64_at = |offset| u64::from_le_bytes(raw[offset..offset + 8].try_into().unwrap());
            let distance = f32::from_bits(u32::from_le_bytes(
                raw[57..61].try_into().map_err(D::Error::custom)?,
            ));
            if !distance.is_finite() {
                return Err(D::Error::custom("nonfinite expansion child score"));
            }
            let leaf = match raw[56] {
                0 => false,
                1 => true,
                _ => return Err(D::Error::custom("invalid expansion leaf flag")),
            };
            rows.push(ScoredChild {
                parent: u128_at(0),
                ordinal: usize::try_from(u64_at(16)).map_err(D::Error::custom)?,
                target: NodeRef {
                    id: u128_at(24),
                    authority: u64_at(40),
                    placement: u64_at(48),
                },
                leaf,
                distance,
                observed_revision: u64_at(61),
            });
        }
        Ok(rows)
    }
}

#[cfg(test)]
#[path = "expansion_codec_tests.rs"]
mod tests;

#[path = "expansion_batch_codec.rs"]
pub(crate) mod batch;
