//! Lossless bounded wire representation used only when installing a complete view.
use super::super::protocol::AttemptId;
use super::super::routing::{Child, NodeRef, RoutingNode};
use super::{BoundaryView, PublishedView};
use base64::{prelude::BASE64_STANDARD, Engine};
use serde::{de::Error, Deserializer, Serializer};
use std::{collections::BTreeMap, sync::Arc};
const MAX: usize = 12 * 1024 * 1024 - 1024;
const ENCODED_MAX: usize = MAX.div_ceil(3) * 4;
type Result<T> = std::result::Result<T, &'static str>;
struct Writer(Vec<u8>);
impl Writer {
    fn raw(&mut self, bytes: &[u8]) -> Result<()> {
        if bytes.len() > MAX.saturating_sub(self.0.len()) {
            return Err("shared view exceeds decoded byte limit");
        }
        self.0
            .try_reserve(bytes.len())
            .map_err(|_| "shared view allocation failed")?;
        self.0.extend_from_slice(bytes);
        Ok(())
    }
    fn count(&mut self, n: usize) -> Result<()> {
        self.raw(
            &u32::try_from(n)
                .map_err(|_| "shared view count overflow")?
                .to_le_bytes(),
        )
    }
    fn flag(&mut self, v: bool) -> Result<()> {
        self.raw(&[u8::from(v)])
    }
    fn reference(&mut self, n: NodeRef) -> Result<()> {
        self.raw(&n.id.to_le_bytes())?;
        self.raw(&n.authority.to_le_bytes())?;
        self.raw(&n.placement.to_le_bytes())
    }
    fn floats(&mut self, values: &[f32]) -> Result<()> {
        self.count(values.len())?;
        for v in values {
            if !v.is_finite() {
                return Err("nonfinite shared view coordinate");
            }
            self.raw(&v.to_bits().to_le_bytes())?;
        }
        Ok(())
    }
    fn node(&mut self, n: &RoutingNode) -> Result<()> {
        self.reference(n.target)?;
        self.flag(n.parent.is_some())?;
        if let Some(p) = n.parent {
            self.reference(p)?;
        }
        self.floats(&n.centroid)?;
        self.count(n.centroid_code.len())?;
        self.raw(&n.centroid_code)?;
        self.flag(n.leaf)?;
        self.raw(&n.revision.to_le_bytes())?;
        self.count(n.children.len())?;
        for c in &n.children {
            self.reference(c.target)?;
            self.flag(c.leaf)?;
            self.floats(&c.centroid)?;
        }
        Ok(())
    }
}
struct Reader<'a>(&'a [u8]);
impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if n > self.0.len() {
            return Err("truncated shared view payload");
        }
        let (a, b) = self.0.split_at(n);
        self.0 = b;
        Ok(a)
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn u128(&mut self) -> Result<u128> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }
    fn count(&mut self, min_bytes: usize) -> Result<usize> {
        let n = u32::from_le_bytes(self.take(4)?.try_into().unwrap()) as usize;
        if n > self.0.len() / min_bytes {
            return Err("shared view count exceeds remaining payload");
        }
        Ok(n)
    }
    fn flag(&mut self) -> Result<bool> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid shared view flag"),
        }
    }
    fn reference(&mut self) -> Result<NodeRef> {
        Ok(NodeRef {
            id: self.u128()?,
            authority: self.u64()?,
            placement: self.u64()?,
        })
    }
    fn floats(&mut self) -> Result<Vec<f32>> {
        let n = self.count(4)?;
        let bytes = self.take(n * 4)?;
        let mut v = Vec::new();
        v.try_reserve_exact(n)
            .map_err(|_| "shared view allocation failed")?;
        for b in bytes.chunks_exact(4) {
            let x = f32::from_bits(u32::from_le_bytes(b.try_into().unwrap()));
            if !x.is_finite() {
                return Err("nonfinite shared view coordinate");
            }
            v.push(x);
        }
        Ok(v)
    }
    fn node(&mut self) -> Result<RoutingNode> {
        let target = self.reference()?;
        let parent = if self.flag()? {
            Some(self.reference()?)
        } else {
            None
        };
        let centroid = self.floats()?;
        let n = self.count(1)?;
        let centroid_code = self.take(n)?.to_vec();
        let leaf = self.flag()?;
        let revision = self.u64()?;
        let n = self.count(37)?;
        let mut children = Vec::new();
        children
            .try_reserve_exact(n)
            .map_err(|_| "shared view allocation failed")?;
        for _ in 0..n {
            children.push(Child {
                target: self.reference()?,
                leaf: self.flag()?,
                centroid: self.floats()?,
            });
        }
        let mut node = RoutingNode {
            target,
            parent,
            centroid,
            centroid_code,
            children,
            leaf,
            revision,
            navigation: Arc::default(),
        };
        // Validate dimensions before the derived packed buffer reserves memory.
        // Otherwise many empty children and one wide parent could amplify a
        // small malformed payload into a much larger allocation.
        if (!node.children.is_empty() && node.centroid.is_empty())
            || node
                .children
                .iter()
                .any(|child| child.centroid.len() != node.centroid.len())
        {
            return Err("invalid shared view navigation dimensions");
        }
        node.prepare_navigation()
            .map_err(|_| "invalid shared view navigation dimensions")?;
        Ok(node)
    }
}
fn pack(view: &PublishedView<BoundaryView>) -> Result<Vec<u8>> {
    let mut w = Writer(Vec::new());
    w.raw(b"SCV1")?;
    w.raw(&view.sequence.to_le_bytes())?;
    w.flag(view.attempt.is_some())?;
    if let Some(a) = view.attempt {
        w.raw(&a.authority.to_le_bytes())?;
        w.raw(&a.sequence.to_le_bytes())?;
    }
    w.node(&view.value.root)?;
    w.count(view.value.shared.len())?;
    for (id, n) in &view.value.shared {
        w.raw(&id.to_le_bytes())?;
        w.node(n)?;
    }
    w.count(view.value.units.len())?;
    for (id, n) in &view.value.units {
        w.raw(&id.to_le_bytes())?;
        w.reference(*n)?;
    }
    Ok(w.0)
}
fn unpack(bytes: &[u8]) -> Result<PublishedView<BoundaryView>> {
    if bytes.len() > MAX {
        return Err("shared view exceeds decoded byte limit");
    }
    let mut r = Reader(bytes);
    if r.take(4)? != b"SCV1" {
        return Err("unsupported shared view wire version");
    }
    let sequence = r.u64()?;
    let attempt = if r.flag()? {
        Some(AttemptId {
            authority: r.u64()?,
            sequence: r.u64()?,
        })
    } else {
        None
    };
    let root = r.node()?;
    let n = r.count(70)?;
    let mut shared = BTreeMap::new();
    for _ in 0..n {
        let id = r.u128()?;
        let node = r.node()?;
        if shared.insert(id, node).is_some() {
            return Err("duplicate shared view node");
        }
    }
    let n = r.count(48)?;
    let mut units = BTreeMap::new();
    for _ in 0..n {
        let id = r.u128()?;
        let node = r.reference()?;
        if units.insert(id, node).is_some() {
            return Err("duplicate shared view ownership unit");
        }
    }
    if !r.0.is_empty() {
        return Err("trailing shared view payload");
    }
    Ok(PublishedView {
        sequence,
        attempt,
        value: BoundaryView {
            root,
            shared,
            units,
            navigation_topology: None,
        },
    })
}
pub fn serialize<S: Serializer>(
    view: &PublishedView<BoundaryView>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let bytes = pack(view).map_err(serde::ser::Error::custom)?;
    serializer.serialize_str(&BASE64_STANDARD.encode(bytes))
}
pub fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<PublishedView<BoundaryView>, D::Error> {
    struct Payload;
    impl serde::de::Visitor<'_> for Payload {
        type Value = PublishedView<BoundaryView>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a bounded SCV1 base64 shared view")
        }
        fn visit_str<E: Error>(self, s: &str) -> std::result::Result<Self::Value, E> {
            if s.len() > ENCODED_MAX {
                return Err(E::custom("shared view exceeds encoded byte limit"));
            }
            let bytes = BASE64_STANDARD.decode(s).map_err(E::custom)?;
            unpack(&bytes).map_err(E::custom)
        }
    }
    deserializer.deserialize_str(Payload)
}
#[cfg(test)]
#[path = "view_codec_tests.rs"]
mod tests;
