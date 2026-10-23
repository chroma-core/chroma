//! Response-local exact metadata reuse. Decode back to complete scored replies;
//! the navigation driver still validates identity, coverage and revisions.
use super::super::{ExpandReply, ExpansionStatus, NodeRef, ScoredChild};
use super::{decode, encode, reserve, CHILD_BYTES, HEADER_BYTES, MAX_PAYLOAD_BYTES};
use serde::{
    de::{Error, SeqAccess, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{
    collections::{HashMap, HashSet},
    hash::{BuildHasher, Hash, Hasher},
};

// Bound decoded envelope slots independently of row compression. The sender's
// current64-query policy is not a wire semantic limit.
const MAX_REPLIES: usize = MAX_PAYLOAD_BYTES / std::mem::size_of::<WireReply>();
const MAX_ROWS: usize = (MAX_PAYLOAD_BYTES - HEADER_BYTES) / CHILD_BYTES;
const META_BYTES: usize = 41;
const IMAGE_HEADER: usize = 28;
const SIZE: &str = "expansion payload exceeds decoded byte limit";
fn add(total: usize, bytes: usize) -> Result<usize, &'static str> {
    let n = total
        .checked_add(bytes)
        .ok_or("expansion payload size overflow")?;
    if n > MAX_PAYLOAD_BYTES {
        Err(SIZE)
    } else {
        Ok(n)
    }
}
fn mul(n: usize, width: usize) -> Result<usize, &'static str> {
    n.checked_mul(width)
        .ok_or("expansion payload size overflow")
}
fn grow(bytes: &mut Vec<u8>, extra: usize) -> Result<(), &'static str> {
    add(bytes.len(), extra)?;
    bytes
        .try_reserve(extra)
        .map_err(|_| "expansion payload allocation failed")
}
mod raw {
    use super::*;
    pub fn serialize<S: Serializer>(bytes: &[u8], s: S) -> Result<S::Ok, S::Error> {
        if bytes.len() > MAX_PAYLOAD_BYTES {
            return Err(serde::ser::Error::custom(SIZE));
        }
        encode(bytes, s)
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        decode(d)
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Wire {
    #[serde(rename = "images_le_v2_base64", with = "raw")]
    images: Vec<u8>,
    #[serde(deserialize_with = "bounded_replies")]
    replies: Vec<WireReply>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireReply {
    authority: u64,
    run: u128,
    epoch: u64,
    shared_sequence: u64,
    child_counts: Vec<(u128, usize)>,
    navigation: u128,
    step: u64,
    query_slot: usize,
    nodes: Vec<NodeRef>,
    status: ExpansionStatus,
    #[serde(rename = "runs_le_v2_base64", with = "raw")]
    runs: Vec<u8>,
}
fn bounded_replies<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<WireReply>, D::Error> {
    struct Replies;
    impl<'de> Visitor<'de> for Replies {
        type Value = Vec<WireReply>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a bounded array of expansion reply envelopes")
        }
        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut replies = reserve::<_, A::Error>(MAX_REPLIES.min(64))?;
            for _ in 0..MAX_REPLIES {
                match seq.next_element()? {
                    Some(reply) => {
                        replies
                            .try_reserve(1)
                            .map_err(|_| A::Error::custom("expansion reply allocation failed"))?;
                        replies.push(reply)
                    }
                    None => return Ok(replies),
                }
            }
            if seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(SIZE));
            }
            Ok(replies)
        }
    }
    d.deserialize_seq(Replies)
}
#[derive(Clone, Copy)]
struct Metadata<'a>(&'a [ScoredChild]);
impl PartialEq for Metadata<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.len() == other.0.len()
            && self.0.iter().zip(other.0).all(|(a, b)| {
                a.parent == b.parent
                    && a.observed_revision == b.observed_revision
                    && a.ordinal == b.ordinal
                    && a.target == b.target
                    && a.leaf == b.leaf
            })
    }
}
impl Eq for Metadata<'_> {}
impl Hash for Metadata<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.len().hash(state);
        for row in self.0 {
            row.parent.hash(state);
            row.observed_revision.hash(state);
            row.ordinal.hash(state);
            row.target.hash(state);
            row.leaf.hash(state);
        }
    }
}
fn build<S: BuildHasher>(replies: &[ExpandReply], hasher: S) -> Result<Wire, &'static str> {
    if replies.len() > MAX_REPLIES {
        return Err(SIZE);
    }
    // Retain the old representation's expanded-row budget independently of
    // compression. Repeated references cannot amplify bounded wire bytes into
    // an unbounded allocation of rich decoded rows.
    let mut expanded = 0;
    for reply in replies {
        expanded = add(
            expanded,
            add(HEADER_BYTES, mul(reply.children.len(), CHILD_BYTES)?)?,
        )?;
    }
    let mut table: HashMap<Metadata<'_>, u32, S> = HashMap::with_hasher(hasher);
    table
        .try_reserve(replies.len())
        .map_err(|_| "expansion dictionary allocation failed")?;
    let mut images = vec![0; 4];
    let mut wire_replies = Vec::new();
    wire_replies
        .try_reserve_exact(replies.len())
        .map_err(|_| "expansion reply allocation failed")?;
    let mut image_count = 0u32;
    let mut raw_total = HEADER_BYTES;
    for reply in replies {
        let mut runs = vec![0; 4];
        let mut run_count = 0u32;
        let mut first = 0;
        while first < reply.children.len() {
            let parent = reply.children[first].parent;
            let revision = reply.children[first].observed_revision;
            let mut end = first + 1;
            while end < reply.children.len()
                && reply.children[end].parent == parent
                && reply.children[end].observed_revision == revision
            {
                end += 1;
            }
            let rows = &reply.children[first..end];
            let key = Metadata(rows);
            let index = if let Some(&index) = table.get(&key) {
                index
            } else {
                let extra = add(IMAGE_HEADER, mul(rows.len(), META_BYTES)?)?;
                raw_total = add(raw_total, extra)?;
                grow(&mut images, extra)?;
                images.extend_from_slice(&parent.to_le_bytes());
                images.extend_from_slice(&revision.to_le_bytes());
                images
                    .extend_from_slice(&u32::try_from(rows.len()).map_err(|_| SIZE)?.to_le_bytes());
                for row in rows {
                    images.extend_from_slice(
                        &u64::try_from(row.ordinal)
                            .map_err(|_| "expansion ordinal exceeds u64")?
                            .to_le_bytes(),
                    );
                    images.extend_from_slice(&row.target.id.to_le_bytes());
                    images.extend_from_slice(&row.target.authority.to_le_bytes());
                    images.extend_from_slice(&row.target.placement.to_le_bytes());
                    images.push(u8::from(row.leaf));
                }
                let index = image_count;
                image_count = image_count.checked_add(1).ok_or(SIZE)?;
                table
                    .try_reserve(1)
                    .map_err(|_| "expansion dictionary allocation failed")?;
                table.insert(key, index);
                index
            };
            let extra = add(4, mul(rows.len(), 4)?)?;
            raw_total = add(raw_total, extra)?;
            grow(&mut runs, extra)?;
            runs.extend_from_slice(&index.to_le_bytes());
            for row in rows {
                if !row.distance.is_finite() {
                    return Err("nonfinite expansion child score");
                }
                runs.extend_from_slice(&row.distance.to_bits().to_le_bytes());
            }
            run_count = run_count.checked_add(1).ok_or(SIZE)?;
            first = end;
        }
        raw_total = add(raw_total, HEADER_BYTES)?;
        runs[..4].copy_from_slice(&run_count.to_le_bytes());
        wire_replies.push(WireReply {
            authority: reply.authority,
            run: reply.run,
            epoch: reply.epoch,
            shared_sequence: reply.shared_sequence,
            child_counts: reply.child_counts.clone(),
            navigation: reply.navigation,
            step: reply.step,
            query_slot: reply.query_slot,
            nodes: reply.nodes.clone(),
            status: reply.status.clone(),
            runs,
        });
    }
    images[..4].copy_from_slice(&image_count.to_le_bytes());
    Ok(Wire {
        images,
        replies: wire_replies,
    })
}
pub fn serialize<S: Serializer>(replies: &[ExpandReply], s: S) -> Result<S::Ok, S::Error> {
    build(replies, std::collections::hash_map::RandomState::new())
        .map_err(serde::ser::Error::custom)?
        .serialize(s)
}
struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let end = self
            .at
            .checked_add(n)
            .ok_or("expansion payload size overflow")?;
        let out = self
            .bytes
            .get(self.at..end)
            .ok_or("truncated expansion dictionary or score run")?;
        self.at = end;
        Ok(out)
    }
    fn u32(&mut self) -> Result<usize, &'static str> {
        usize::try_from(u32::from_le_bytes(self.take(4)?.try_into().unwrap())).map_err(|_| SIZE)
    }
    fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128, &'static str> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn finish(self) -> Result<(), &'static str> {
        if self.at == self.bytes.len() {
            Ok(())
        } else {
            Err("trailing expansion dictionary or score bytes")
        }
    }
}
struct Image<'a> {
    parent: u128,
    revision: u64,
    rows: &'a [u8],
    count: usize,
}
fn metadata(raw: &[u8]) -> Result<(usize, NodeRef, bool), &'static str> {
    let mut c = Cursor::new(raw);
    let ordinal = usize::try_from(c.u64()?).map_err(|_| "expansion ordinal exceeds usize")?;
    let target = NodeRef {
        id: c.u128()?,
        authority: c.u64()?,
        placement: c.u64()?,
    };
    let leaf = match c.take(1)?[0] {
        0 => false,
        1 => true,
        _ => return Err("invalid expansion leaf flag"),
    };
    c.finish()?;
    Ok((ordinal, target, leaf))
}
fn images(bytes: &[u8]) -> Result<Vec<Image<'_>>, &'static str> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()?;
    if count > MAX_ROWS || count > bytes.len().saturating_sub(4) / IMAGE_HEADER {
        return Err(SIZE);
    }
    let mut images = Vec::new();
    images
        .try_reserve_exact(count)
        .map_err(|_| "expansion dictionary allocation failed")?;
    let mut unique = HashSet::new();
    unique
        .try_reserve(count)
        .map_err(|_| "expansion dictionary allocation failed")?;
    let mut rows_total = 0usize;
    for _ in 0..count {
        let start = cursor.at;
        let parent = cursor.u128()?;
        let revision = cursor.u64()?;
        let n = cursor.u32()?;
        rows_total = rows_total.checked_add(n).ok_or(SIZE)?;
        if n == 0 || rows_total > MAX_ROWS {
            return Err(SIZE);
        }
        let rows = cursor.take(mul(n, META_BYTES)?)?;
        for row in rows.chunks_exact(META_BYTES) {
            metadata(row)?;
        }
        if !unique.insert(&bytes[start..cursor.at]) {
            return Err("duplicate expansion dictionary entry");
        }
        images.push(Image {
            parent,
            revision,
            rows,
            count: n,
        });
    }
    cursor.finish()?;
    Ok(images)
}
fn visit_runs<'a>(
    bytes: &'a [u8],
    images: &[Image<'_>],
    mut visit: impl FnMut(usize, &'a [u8]) -> Result<(), &'static str>,
) -> Result<(), &'static str> {
    let mut cursor = Cursor::new(bytes);
    let count = cursor.u32()?;
    if count > MAX_ROWS || count > bytes.len().saturating_sub(4) / 4 {
        return Err(SIZE);
    }
    for _ in 0..count {
        let index = cursor.u32()?;
        let image = images
            .get(index)
            .ok_or("unknown expansion dictionary reference")?;
        let scores = cursor.take(mul(image.count, 4)?)?;
        visit(index, scores)?;
    }
    cursor.finish()
}
fn restore(wire: Wire) -> Result<Vec<ExpandReply>, &'static str> {
    if wire.replies.len() > MAX_REPLIES {
        return Err(SIZE);
    }
    let mut raw = add(0, wire.images.len())?;
    for reply in &wire.replies {
        raw = add(raw, reply.runs.len())?;
    }
    let table = images(&wire.images)?;
    let mut used = vec![false; table.len()];
    let mut expanded = 0;
    let mut counts = Vec::with_capacity(wire.replies.len());
    for reply in &wire.replies {
        let mut rows = 0usize;
        visit_runs(&reply.runs, &table, |index, scores| {
            used[index] = true;
            rows = rows.checked_add(table[index].count).ok_or(SIZE)?;
            for score in scores.chunks_exact(4) {
                if !f32::from_bits(u32::from_le_bytes(score.try_into().unwrap())).is_finite() {
                    return Err("nonfinite expansion child score");
                }
            }
            Ok(())
        })?;
        expanded = add(expanded, add(HEADER_BYTES, mul(rows, CHILD_BYTES)?)?)?;
        counts.push(rows);
    }
    if used.iter().any(|used| !*used) {
        return Err("unused expansion dictionary entry");
    }
    let mut replies = Vec::new();
    replies
        .try_reserve_exact(wire.replies.len())
        .map_err(|_| "expansion reply allocation failed")?;
    for (reply, count) in wire.replies.into_iter().zip(counts) {
        let mut children = Vec::new();
        children
            .try_reserve_exact(count)
            .map_err(|_| "expansion row allocation failed")?;
        visit_runs(&reply.runs, &table, |index, scores| {
            let image = &table[index];
            for (row, score) in image
                .rows
                .chunks_exact(META_BYTES)
                .zip(scores.chunks_exact(4))
            {
                let (ordinal, target, leaf) = metadata(row)?;
                children.push(ScoredChild {
                    parent: image.parent,
                    observed_revision: image.revision,
                    ordinal,
                    target,
                    leaf,
                    distance: f32::from_bits(u32::from_le_bytes(score.try_into().unwrap())),
                });
            }
            Ok(())
        })?;
        replies.push(ExpandReply {
            authority: reply.authority,
            run: reply.run,
            epoch: reply.epoch,
            shared_sequence: reply.shared_sequence,
            child_counts: reply.child_counts,
            navigation: reply.navigation,
            step: reply.step,
            query_slot: reply.query_slot,
            nodes: reply.nodes,
            status: reply.status,
            children,
        });
    }
    Ok(replies)
}
pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<ExpandReply>, D::Error> {
    restore(Wire::deserialize(d)?).map_err(D::Error::custom)
}
#[cfg(test)]
#[path = "expansion_batch_codec_tests.rs"]
mod tests;
