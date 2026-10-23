//! Explicit conversion of versioned checkpoints. The source provider is read
//! only; every output blockfile is created in a separate destination provider.
use std::collections::{HashMap, HashSet};

use chroma_blockstore::{
    arrow::provider::BlockfileReaderOptions, provider::BlockfileProvider, BlockfileReader,
    BlockfileWriterOptions,
};
use chroma_error::ChromaError;
use chroma_index::quantization::Code;
use chroma_types::hierarchical_spann::{
    HierarchicalInternalNode, HierarchicalLeafNode, HierarchicalSpannPostingList,
    LegacyHierarchicalSpannPostingList,
};

use super::super::persistance::*;
use super::HierarchicalSpannIds;

pub const PREFIX_VERSION: &str = "version";

type Error = Box<dyn ChromaError>;
fn invalid(message: impl Into<String>) -> Error {
    Box::new(std::io::Error::new(
        std::io::ErrorKind::InvalidData,
        message.into(),
    ))
}

pub async fn require_version_free(
    reader: &BlockfileReader<'static, u32, u32>,
) -> Result<(), Error> {
    match reader.get(PREFIX_FORMAT, SINGLETON_KEY).await? {
        Some(VERSION_FREE_FORMAT) => Ok(()),
        other => Err(invalid(format!("checkpoint format {other:?} requires explicit --upgrade-legacy-to conversion; the source checkpoint is preserved"))),
    }
}

pub struct UpgradedCheckpoint {
    pub ids: HierarchicalSpannIds,
    pub vector_count: usize,
}

/// Convert one leaf at a time and stream embeddings in ID order. Memory holds
/// topology, a set of accepted IDs, and one leaf's rows, never all embeddings.
/// Multiple authoritative copies are ambiguous and reject the conversion.
pub async fn upgrade_legacy_checkpoint(
    source: &BlockfileProvider,
    destination: &BlockfileProvider,
    ids: &HierarchicalSpannIds,
) -> Result<UpgradedCheckpoint, Error> {
    let sm = source
        .read::<u32, u32>(BlockfileReaderOptions::new(
            ids.scalar_metadata_id,
            "".into(),
        ))
        .await
        .map_err(|err| err as Error)?;
    if sm.get(PREFIX_FORMAT, SINGLETON_KEY).await?.is_some() {
        return Err(invalid("conversion requires an unmarked legacy checkpoint"));
    }
    let root = sm
        .get(PREFIX_ROOT, SINGLETON_KEY)
        .await?
        .ok_or_else(|| invalid("missing root"))?;
    let next = sm
        .get(PREFIX_NEXT_NODE, SINGLETON_KEY)
        .await?
        .ok_or_else(|| invalid("missing next node ID"))?;
    let dim = sm
        .get(PREFIX_DIM, SINGLETON_KEY)
        .await?
        .ok_or_else(|| invalid("missing dimension"))?;
    if dim == 0 {
        return Err(invalid("zero dimension"));
    }
    let leaves = source
        .read::<u32, HierarchicalLeafNode<'static>>(BlockfileReaderOptions::new(
            ids.leaf_node_id,
            "".into(),
        ))
        .await
        .map_err(|err| err as Error)?;
    let internals = source
        .read::<u32, HierarchicalInternalNode<'static>>(BlockfileReaderOptions::new(
            ids.internal_node_id,
            "".into(),
        ))
        .await
        .map_err(|err| err as Error)?;
    let postings = source
        .read::<u32, LegacyHierarchicalSpannPostingList<'static>>(BlockfileReaderOptions::new(
            ids.posting_list_id,
            "".into(),
        ))
        .await
        .map_err(|err| err as Error)?;
    let vectors = source
        .read::<u32, &'static [f32]>(BlockfileReaderOptions::new(ids.vector_data_id, "".into()))
        .await
        .map_err(|err| err as Error)?;
    // Only topology is materialized. Validate parent links and reachability
    // before emitting any destination data.
    let leaf_rows: Vec<_> = leaves
        .get_range(""..="", ..)
        .await?
        .map(|(_, id, leaf)| (id, leaf.parent, leaf.centroid_code.to_vec(), leaf.length))
        .collect();
    let internal_rows: Vec<_> = internals
        .get_range(""..="", ..)
        .await?
        .map(|(_, id, node)| {
            (
                id,
                node.parent,
                node.centroid_code.to_vec(),
                node.children.to_vec(),
            )
        })
        .collect();
    let mut topology = HashMap::new();
    for (id, parent, _, _) in &leaf_rows {
        if topology.insert(*id, (*parent, Vec::new())).is_some() {
            return Err(invalid(format!("duplicate node {id}")));
        }
    }
    for (id, parent, _, children) in &internal_rows {
        if children.is_empty() || topology.insert(*id, (*parent, children.clone())).is_some() {
            return Err(invalid(format!("invalid internal node {id}")));
        }
    }
    let mut seen = HashSet::new();
    let mut stack = vec![(root, NO_PARENT)];
    while let Some((id, parent)) = stack.pop() {
        if !seen.insert(id) {
            return Err(invalid(format!("repeated reachable node {id}")));
        }
        let (stored_parent, children) = topology
            .get(&id)
            .ok_or_else(|| invalid(format!("missing child {id}")))?;
        if *stored_parent != parent || id >= next {
            return Err(invalid(format!("invalid parent or next ID at node {id}")));
        }
        stack.extend(children.iter().map(|child| (*child, id)));
    }
    if seen.len() != topology.len() {
        return Err(invalid("checkpoint contains unreachable nodes"));
    }
    let mut pl = destination
        .write::<u32, HierarchicalSpannPostingList<'_>>(
            BlockfileWriterOptions::new("".into()).ordered_mutations(),
        )
        .await
        .map_err(|err| err as Error)?;
    let ln = destination
        .write::<u32, HierarchicalLeafNode<'_>>(
            BlockfileWriterOptions::new("".into()).ordered_mutations(),
        )
        .await
        .map_err(|err| err as Error)?;
    let int = destination
        .write::<u32, HierarchicalInternalNode<'_>>(
            BlockfileWriterOptions::new("".into()).ordered_mutations(),
        )
        .await
        .map_err(|err| err as Error)?;
    let mut vd = destination
        .write::<u32, Vec<f32>>(BlockfileWriterOptions::new("".into()).ordered_mutations())
        .await
        .map_err(|err| err as Error)?;
    let meta = destination
        .write::<u32, u32>(BlockfileWriterOptions::new("".into()).ordered_mutations())
        .await
        .map_err(|err| err as Error)?;
    let mut node_ids: Vec<_> = topology.keys().copied().collect();
    node_ids.sort_unstable();
    let zero = vec![0.0; dim as usize];
    let mut centroid_codes = HashMap::new();
    for id in node_ids {
        let centroid = vectors
            .get(PREFIX_CENTROID, id)
            .await?
            .ok_or_else(|| invalid(format!("missing centroid {id}")))?;
        if centroid.len() != dim as usize || centroid.iter().any(|x| !x.is_finite()) {
            return Err(invalid(format!("invalid centroid {id}")));
        }
        centroid_codes.insert(id, Code::<1>::quantize(centroid, &zero).as_ref().to_vec());
        vd.set(PREFIX_CENTROID, id, centroid.to_vec()).await?;
    }
    vectors.clear_loaded_blocks();
    let mut live = HashSet::new();
    let code_size = Code::<1>::size(dim as usize);
    let mut posting_bytes = 0usize;
    for (id, parent, _legacy_centroid_code, old_length) in leaf_rows {
        let centroid = vectors
            .get(PREFIX_CENTROID, id)
            .await?
            .ok_or_else(|| invalid(format!("missing centroid {id}")))?
            .to_vec();
        let mut kept = Vec::new();
        let mut codes = Vec::new();
        if let Some(rows) = postings.get("", id).await? {
            if rows.ids.len() != rows.versions.len()
                || rows.codes.len() != rows.ids.len() * code_size
            {
                return Err(invalid(format!("invalid posting layout for leaf {id}")));
            }
            for (&vector_id, &version) in rows.ids.iter().zip(rows.versions) {
                let current = sm
                    .get(PREFIX_VERSION, vector_id)
                    .await?
                    .ok_or_else(|| invalid(format!("missing authoritative version {vector_id}")))?;
                if current > u8::MAX as u32 {
                    return Err(invalid(format!(
                        "invalid authoritative version {vector_id}"
                    )));
                }
                if current & 0x80 != 0 || current as u8 != version {
                    continue;
                }
                if !live.insert(vector_id) {
                    return Err(invalid(format!("multiple live copies of vector {vector_id}; conversion requires a single authoritative copy")));
                }
                let embedding = vectors
                    .get(PREFIX_EMBEDDING, vector_id)
                    .await?
                    .ok_or_else(|| invalid(format!("missing embedding {vector_id}")))?;
                if embedding.len() != dim as usize || embedding.iter().any(|x| !x.is_finite()) {
                    return Err(invalid(format!("invalid embedding {vector_id}")));
                }
                // Recode accepted rows from authoritative embeddings and the
                // persisted centroid, including malformed legacy code bytes.
                codes.extend_from_slice(Code::<1>::quantize(embedding, &centroid).as_ref());
                kept.push(vector_id);
                vectors.clear_loaded_blocks();
            }
        } else if old_length != 0 {
            return Err(invalid(format!("missing legacy posting for leaf {id}")));
        }
        ln.set(
            "",
            id,
            HierarchicalLeafNode {
                parent,
                length: kept.len() as u32,
                centroid_code: &centroid_codes[&id],
            },
        )
        .await?;
        pl.set(
            "",
            id,
            HierarchicalSpannPostingList {
                ids: &kept,
                codes: &codes,
            },
        )
        .await?;
        posting_bytes += codes.len() + kept.len() * 4;
        if posting_bytes >= 32 * 1024 * 1024 {
            let flushed = pl.commit::<u32, HierarchicalSpannPostingList<'_>>().await?;
            let base = flushed.id();
            flushed
                .flush::<u32, HierarchicalSpannPostingList<'_>>()
                .await?;
            pl = destination
                .write::<u32, HierarchicalSpannPostingList<'_>>(
                    BlockfileWriterOptions::new("".into())
                        .ordered_mutations()
                        .fork(base),
                )
                .await
                .map_err(|err| err as Error)?;
            posting_bytes = 0;
        }
        postings.clear_loaded_blocks();
        sm.clear_loaded_blocks();
    }
    for (id, parent, _legacy_centroid_code, children) in internal_rows {
        int.set(
            "",
            id,
            HierarchicalInternalNode {
                parent,
                centroid_code: &centroid_codes[&id],
                children: &children,
            },
        )
        .await?;
    }
    // Numeric windows bound loaded metadata blocks even for old checkpoints
    // without a maximum ID summary. Scan the full namespace to detect live
    // authoritative IDs that have no reachable posting.
    let mut authoritative_count = 0usize;
    for window in 0..4096u32 {
        let start = window << 20;
        let end = start | ((1 << 20) - 1);
        let rows: Vec<_> = sm
            .get_range(PREFIX_VERSION..=PREFIX_VERSION, start..=end)
            .await?
            .map(|(_, id, version)| (id, version))
            .collect();
        for (id, version) in rows {
            if version > u8::MAX as u32 {
                return Err(invalid(format!("invalid authoritative version {id}")));
            }
            if version & 0x80 == 0 {
                if !live.contains(&id) {
                    return Err(invalid(format!(
                        "authoritative live vector {id} has no reachable posting"
                    )));
                }
                authoritative_count += 1;
            }
        }
        sm.clear_loaded_blocks();
    }
    if authoritative_count != live.len() {
        return Err(invalid("authoritative and reachable vector counts differ"));
    }
    let vector_count = live.len();
    let mut ordered_ids: Vec<_> = live.into_iter().collect();
    ordered_ids.sort_unstable();
    let max_id = ordered_ids.last().copied().unwrap_or(0);
    for (i, id) in ordered_ids.into_iter().enumerate() {
        let embedding = vectors
            .get(PREFIX_EMBEDDING, id)
            .await?
            .ok_or_else(|| invalid(format!("missing embedding {id}")))?;
        if embedding.len() != dim as usize || embedding.iter().any(|x| !x.is_finite()) {
            return Err(invalid(format!("invalid embedding {id}")));
        }
        vd.set(PREFIX_EMBEDDING, id, embedding.to_vec()).await?;
        if i % 4096 == 4095 {
            vectors.clear_loaded_blocks();
            if i + 1 < vector_count {
                let flushed = vd.commit::<u32, Vec<f32>>().await?;
                let base = flushed.id();
                flushed.flush::<u32, Vec<f32>>().await?;
                vd = destination
                    .write::<u32, Vec<f32>>(
                        BlockfileWriterOptions::new("".into())
                            .ordered_mutations()
                            .fork(base),
                    )
                    .await
                    .map_err(|err| err as Error)?;
            }
        }
    }
    // Ordered prefix writes; no legacy version metadata is copied.
    for (prefix, value) in [
        (PREFIX_DIM, dim),
        (PREFIX_FORMAT, VERSION_FREE_FORMAT),
        (PREFIX_MAX_VECTOR_ID, max_id),
        (PREFIX_NEXT_NODE, next),
        (PREFIX_ROOT, root),
    ] {
        meta.set(prefix, SINGLETON_KEY, value).await?;
    }
    let pl = pl.commit::<u32, HierarchicalSpannPostingList<'_>>().await?;
    let ln = ln.commit::<u32, HierarchicalLeafNode<'_>>().await?;
    let int = int.commit::<u32, HierarchicalInternalNode<'_>>().await?;
    let vd = vd.commit::<u32, Vec<f32>>().await?;
    let meta = meta.commit::<u32, u32>().await?;
    let ids = HierarchicalSpannIds {
        posting_list_id: pl.id(),
        leaf_node_id: ln.id(),
        internal_node_id: int.id(),
        vector_data_id: vd.id(),
        scalar_metadata_id: meta.id(),
    };
    pl.flush::<u32, HierarchicalSpannPostingList<'_>>().await?;
    ln.flush::<u32, HierarchicalLeafNode<'_>>().await?;
    int.flush::<u32, HierarchicalInternalNode<'_>>().await?;
    vd.flush::<u32, Vec<f32>>().await?;
    meta.flush::<u32, u32>().await?;
    Ok(UpgradedCheckpoint { ids, vector_count })
}
